package raft

import (
	"encoding/json"
	"errors"
	"math"
	"slices"
	"testing"
)

func TestSnapshotMessageJSONV2GoldenAndClone(t *testing.T) {
	t.Parallel()

	message := Message{Type: InstallSnapshot, From: "a", To: "b", Term: math.MaxUint64, Sequence: 7, Snapshot: Snapshot{LastIncludedIndex: math.MaxUint64, LastIncludedTerm: 9, Members: []NodeID{"a", "b"}, Data: []byte{0xff}}}
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"Type":5,"From":"a","To":"b","Term":18446744073709551615,"Sequence":7,"LastLogIndex":0,"LastLogTerm":0,"VoteGranted":false,"PrevLogIndex":0,"PrevLogTerm":0,"Entries":null,"LeaderCommit":0,"Success":false,"MatchIndex":0,"RejectHint":0,"Snapshot":{"LastIncludedIndex":18446744073709551615,"LastIncludedTerm":9,"Members":["a","b"],"Data":"/w=="}}`
	if string(encoded) != want {
		t.Fatalf("codec v2 JSON\n got: %s\nwant: %s", encoded, want)
	}
	clone := CloneMessage(message)
	message.Snapshot.Members[0] = "changed"
	message.Snapshot.Data[0] = 0
	if clone.Snapshot.Members[0] != "a" || clone.Snapshot.Data[0] != 0xff {
		t.Fatalf("clone changed: %+v", clone.Snapshot)
	}
}

func TestElectionPersistsSelfVoteBeforeSending(t *testing.T) {
	t.Parallel()

	node := mustNode(t, "a", []NodeID{"c", "a", "b"}, PersistentState{}, 0)
	persist, afterPersist := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputElectionTimeout}))
	if persist.State.HardState.CurrentTerm != 1 || persist.State.HardState.VotedFor != "a" {
		t.Fatalf("persisted state = %+v", persist.State.HardState)
	}
	wantKinds := []EffectKind{EffectResetElectionTimer, EffectSend, EffectSend}
	if got := effectKinds(afterPersist); !slices.Equal(got, wantKinds) {
		t.Fatalf("post-persist effects = %v, want %v", got, wantKinds)
	}
	if afterPersist[1].Message.To != "b" || afterPersist[2].Message.To != "c" {
		t.Fatalf("request order = %q, %q", afterPersist[1].Message.To, afterPersist[2].Message.To)
	}

	leaderPersist, leaderEffects := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputMessage, Message: Message{
		Type: RequestVoteResponse, From: "b", To: "a", Term: 1, VoteGranted: true,
	}}))
	if len(leaderPersist.State.Log) != 1 || leaderPersist.State.Log[0].Type != EntryNoop {
		t.Fatalf("leader no-op persistence = %+v", leaderPersist.State)
	}
	if got := effectKinds(leaderEffects); !slices.Equal(got, []EffectKind{EffectResetHeartbeatTimer, EffectSend, EffectSend}) {
		t.Fatalf("leader effects = %v", got)
	}
	status := node.Status()
	if status.Role != Leader || status.Term != 1 || !slices.Equal(status.ElectionVotes, []NodeID{"a", "b"}) {
		t.Fatalf("leader status = %+v", status)
	}
}

func TestVoteReplyWaitsForPersistence(t *testing.T) {
	t.Parallel()

	node := mustNode(t, "b", []NodeID{"a", "b", "c"}, PersistentState{}, 0)
	effects := mustStep(t, node, Input{Kind: InputMessage, Message: Message{
		Type: RequestVote, From: "a", To: "b", Term: 4,
	}})
	if len(effects) != 1 || effects[0].Kind != EffectPersist {
		t.Fatalf("pre-persist effects = %+v", effects)
	}
	if _, err := node.Step(Input{Kind: InputElectionTimeout}); !errors.Is(err, ErrAwaitingPersistence) {
		t.Fatalf("input during persistence error = %v", err)
	}
	if _, err := node.Step(Input{Kind: InputPersisted, WriteToken: effects[0].WriteToken + 1}); !errors.Is(err, ErrUnexpectedPersistence) {
		t.Fatalf("wrong token error = %v", err)
	}
	_, afterPersist := acknowledgeOnlyPersist(t, node, effects)
	if got := effectKinds(afterPersist); !slices.Equal(got, []EffectKind{EffectResetElectionTimer, EffectSend}) {
		t.Fatalf("post-persist effects = %v", got)
	}
	reply := afterPersist[1].Message
	if reply.Type != RequestVoteResponse || !reply.VoteGranted || reply.Term != 4 {
		t.Fatalf("vote reply = %+v", reply)
	}
}

func TestSingleNodeProposalPersistsBeforeApply(t *testing.T) {
	t.Parallel()

	node := mustNode(t, "a", []NodeID{"a"}, PersistentState{}, 0)
	_, electionEffects := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputElectionTimeout}))
	if got := effectKinds(electionEffects); !slices.Equal(got, []EffectKind{EffectResetElectionTimer, EffectResetHeartbeatTimer, EffectApply}) {
		t.Fatalf("election effects = %v", got)
	}

	proposal := []byte("set x=1")
	persist, afterPersist := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputProposal, Data: proposal}))
	proposal[0] = 'X'
	if persist.State.HardState.CommitIndex != 2 || len(persist.State.Log) != 2 || string(persist.State.Log[1].Data) != "set x=1" {
		t.Fatalf("persisted proposal = %+v", persist.State)
	}
	if len(afterPersist) != 1 || afterPersist[0].Kind != EffectApply || afterPersist[0].Entry.Index != 2 || string(afterPersist[0].Entry.Data) != "set x=1" {
		t.Fatalf("post-persist effects = %+v", afterPersist)
	}
}

func TestThreeNodeReplicationAndCommit(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a", "b", "c"}
	a := mustNode(t, "a", members, PersistentState{}, 0)
	b := mustNode(t, "b", members, PersistentState{}, 0)

	_, electionEffects := acknowledgeOnlyPersist(t, a, mustStep(t, a, Input{Kind: InputElectionTimeout}))
	voteRequest := findMessage(t, electionEffects, RequestVote, "b")
	_, voteEffects := acknowledgeOnlyPersist(t, b, mustStep(t, b, Input{Kind: InputMessage, Message: voteRequest}))
	voteReply := findMessage(t, voteEffects, RequestVoteResponse, "a")
	_, _ = acknowledgeOnlyPersist(t, a, mustStep(t, a, Input{Kind: InputMessage, Message: voteReply}))
	if a.Status().Role != Leader {
		t.Fatalf("candidate did not become leader: %+v", a.Status())
	}

	leaderPersist, sends := acknowledgeOnlyPersist(t, a, mustStep(t, a, Input{Kind: InputProposal, Data: []byte("command")}))
	appendRequest := findMessage(t, sends, AppendEntries, "b")
	followerPersist, followerEffects := acknowledgeOnlyPersist(t, b, mustStep(t, b, Input{Kind: InputMessage, Message: appendRequest}))
	if len(followerPersist.State.Log) != 2 || followerPersist.State.HardState.CommitIndex != 0 {
		t.Fatalf("follower persistence = %+v", followerPersist.State)
	}
	appendReply := findMessage(t, followerEffects, AppendEntriesResponse, "a")
	commitPersist, committedEffects := acknowledgeOnlyPersist(t, a, mustStep(t, a, Input{Kind: InputMessage, Message: appendReply}))
	if leaderPersist.State.HardState.CommitIndex != 0 || commitPersist.State.HardState.CommitIndex != 2 {
		t.Fatalf("commit transition before=%d after=%d", leaderPersist.State.HardState.CommitIndex, commitPersist.State.HardState.CommitIndex)
	}
	if len(committedEffects) != 2 || committedEffects[0].Kind != EffectApply || committedEffects[0].Entry.Index != 1 || committedEffects[1].Entry.Index != 2 {
		t.Fatalf("commit effects = %+v", committedEffects)
	}

	heartbeatEffects := mustStep(t, a, Input{Kind: InputHeartbeatTimeout})
	commitHeartbeat := findMessage(t, heartbeatEffects, AppendEntries, "b")
	followerCommit, followerApply := acknowledgeOnlyPersist(t, b, mustStep(t, b, Input{Kind: InputMessage, Message: commitHeartbeat}))
	if followerCommit.State.HardState.CommitIndex != 2 || len(followerApply) != 4 || followerApply[1].Kind != EffectApply || followerApply[2].Kind != EffectApply {
		t.Fatalf("follower commit=%+v effects=%+v", followerCommit.State, followerApply)
	}
}

func TestAppendEntriesTruncatesOnlyAfterMatchingPrefix(t *testing.T) {
	t.Parallel()

	state := PersistentState{
		HardState: HardState{CurrentTerm: 2},
		Log: []Entry{
			{Index: 1, Term: 1, Type: EntryCommand, Data: []byte("one")},
			{Index: 2, Term: 2, Type: EntryCommand, Data: []byte("old")},
		},
	}
	node := mustNode(t, "b", []NodeID{"a", "b", "c"}, state, 0)
	persist, _ := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputMessage, Message: Message{
		Type: AppendEntries, From: "a", To: "b", Term: 3,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 3, Type: EntryCommand, Data: []byte("new")}},
	}}))
	if len(persist.State.Log) != 2 || persist.State.Log[1].Term != 3 || string(persist.State.Log[1].Data) != "new" {
		t.Fatalf("replaced log = %+v", persist.State.Log)
	}

	before := node.Status().Log
	effects := mustStep(t, node, Input{Kind: InputMessage, Message: Message{
		Type: AppendEntries, From: "a", To: "b", Term: 3,
		PrevLogIndex: 2, PrevLogTerm: 2,
	}})
	if len(effects) != 2 || effects[0].Kind != EffectResetElectionTimer || effects[1].Message.Success {
		t.Fatalf("mismatch effects = %+v", effects)
	}
	if !entriesEqual(node.Status().Log, before) {
		t.Fatalf("prefix mismatch changed log: before=%+v after=%+v", before, node.Status().Log)
	}
}

func TestElectionRejectsTermOverflow(t *testing.T) {
	t.Parallel()

	node := mustNode(t, "a", []NodeID{"a"}, PersistentState{HardState: HardState{CurrentTerm: math.MaxUint64}}, 0)
	if _, err := node.Step(Input{Kind: InputElectionTimeout}); !errors.Is(err, ErrTermExhausted) {
		t.Fatalf("overflow error = %v", err)
	}
}

func TestLocalSnapshotCompactsAppliedPrefix(t *testing.T) {
	t.Parallel()

	node := mustNode(t, "a", []NodeID{"a"}, PersistentState{}, 0)
	_, _ = acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputElectionTimeout}))
	_, _ = acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputProposal, Data: []byte("one")}))
	persist, after := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputSnapshot, SnapshotIndex: 2, SnapshotData: []byte("state@2")}))
	if len(after) != 0 || persist.State.Snapshot.LastIncludedIndex != 2 || persist.State.Snapshot.LastIncludedTerm != 1 || string(persist.State.Snapshot.Data) != "state@2" || len(persist.State.Log) != 0 {
		t.Fatalf("snapshot persist=%+v after=%+v", persist.State, after)
	}
	persist, applied := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputProposal, Data: []byte("two")}))
	if len(persist.State.Log) != 1 || persist.State.Log[0].Index != 3 || persist.State.HardState.CommitIndex != 3 || len(applied) != 1 || applied[0].Entry.Index != 3 {
		t.Fatalf("post-snapshot persist=%+v effects=%+v", persist.State, applied)
	}
}

func TestAppendEntriesRejectsMalformedBatchBeforeMutation(t *testing.T) {
	t.Parallel()

	node := mustNode(t, "b", []NodeID{"a", "b", "c"}, PersistentState{HardState: HardState{CurrentTerm: 1}}, 0)
	effects := mustStep(t, node, Input{Kind: InputMessage, Message: Message{
		Type: AppendEntries, From: "a", To: "b", Term: 1,
		Entries: []Entry{
			{Index: 1, Term: 1, Type: EntryCommand},
			{Index: 2, Term: 1, Type: EntryType(255)},
		},
	}})
	if len(effects) != 2 || effects[0].Kind != EffectResetElectionTimer || effects[1].Message.Success || len(node.Status().Log) != 0 || node.Status().AwaitingPersistence {
		t.Fatalf("effects=%+v status=%+v", effects, node.Status())
	}
}

func TestAppendEntriesRejectsCompactedPrefix(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a", "b", "c"}
	state := PersistentState{HardState: HardState{CurrentTerm: 2, CommitIndex: 5}, Snapshot: Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 1, Members: members}}
	node := mustNode(t, "b", members, state, 5)
	effects := mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: AppendEntries, From: "a", To: "b", Term: 2, PrevLogIndex: 4}})
	if len(effects) != 2 || effects[1].Message.Success || effects[1].Message.RejectHint != 6 {
		t.Fatalf("effects = %+v", effects)
	}
}

func TestIndexExhaustionDoesNotWrapSnapshotsOrAppendBatches(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a", "b", "c"}
	state := PersistentState{
		HardState: HardState{CurrentTerm: 3, CommitIndex: math.MaxUint64 - 1},
		Snapshot:  Snapshot{LastIncludedIndex: math.MaxUint64 - 1, LastIncludedTerm: 1, Members: members},
		Log:       []Entry{{Index: math.MaxUint64, Term: 2, Type: EntryCommand}},
	}
	node := mustNode(t, "b", members, state, math.MaxUint64-1)
	overflow := mustStep(t, node, Input{Kind: InputMessage, Message: Message{
		Type: AppendEntries, From: "a", To: "b", Term: 3,
		PrevLogIndex: math.MaxUint64 - 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: math.MaxUint64, Term: 2, Type: EntryCommand}, {Index: 0, Term: 3, Type: EntryCommand}},
	}})
	if len(overflow) != 2 || overflow[1].Message.Success || node.Status().LastLogIndex != math.MaxUint64 {
		t.Fatalf("overflow effects=%+v status=%+v", overflow, node.Status())
	}

	snapshot := Snapshot{LastIncludedIndex: math.MaxUint64, LastIncludedTerm: 2, Members: members, Data: []byte("max")}
	persist, _ := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: InstallSnapshot, From: "a", To: "b", Term: 3, Snapshot: snapshot}}))
	if persist.State.Snapshot.LastIncludedIndex != math.MaxUint64 || len(persist.State.Log) != 0 || persist.State.HardState.CommitIndex != math.MaxUint64 {
		t.Fatalf("persisted state = %+v", persist.State)
	}
}

func TestPersistentStateRejectsImpossibleTermsAndEntryTypes(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a"}
	cases := []PersistentState{
		{HardState: HardState{CurrentTerm: 1, CommitIndex: 1}, Snapshot: Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 2, Members: members}},
		{HardState: HardState{CurrentTerm: 1}, Log: []Entry{{Index: 1, Term: 2, Type: EntryCommand}}},
		{HardState: HardState{CurrentTerm: 1}, Log: []Entry{{Index: 1, Term: 1, Type: EntryType(255)}}},
		{HardState: HardState{CurrentTerm: 2}, Log: []Entry{{Index: 1, Term: 2, Type: EntryCommand}, {Index: 2, Term: 1, Type: EntryCommand}}},
	}
	for _, state := range cases {
		if _, err := New(Config{ID: "a", Members: members}, state); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("state=%+v error=%v", state, err)
		}
	}
	installMembers := []NodeID{"a", "b"}
	node := mustNode(t, "a", installMembers, PersistentState{HardState: HardState{CurrentTerm: 2}}, 0)
	effects := mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: InstallSnapshot, From: "b", To: "a", Term: 2, Snapshot: Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 3, Members: installMembers}}})
	if len(effects) != 2 || effects[1].Message.Success {
		t.Fatalf("future-term snapshot effects = %+v", effects)
	}
}

func TestInstallSnapshotWaitsForPersistenceAndRecoversAfterRestart(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a", "b", "c"}
	snapshot := Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 2, Members: members, Data: []byte("checkpoint")}
	node := mustNode(t, "b", members, PersistentState{}, 0)
	effects := mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: InstallSnapshot, From: "a", To: "b", Term: 3, Sequence: 7, Snapshot: snapshot}})
	if len(effects) != 1 || effects[0].Kind != EffectPersist || effects[0].State.Snapshot.LastIncludedIndex != 5 || effects[0].State.HardState.CommitIndex != 5 {
		t.Fatalf("pre-persist effects = %+v", effects)
	}
	persisted := effects[0].State
	_, after := acknowledgeOnlyPersist(t, node, effects)
	if got := effectKinds(after); !slices.Equal(got, []EffectKind{EffectResetElectionTimer, EffectInstallSnapshot, EffectSend}) || !after[2].Message.Success || after[2].Message.MatchIndex != 5 {
		t.Fatalf("post-persist effects = %+v", after)
	}

	restarted := mustNode(t, "b", members, persisted, 0)
	start := restarted.Start()
	if got := effectKinds(start); !slices.Equal(got, []EffectKind{EffectInstallSnapshot, EffectResetElectionTimer}) || start[0].Snapshot.LastIncludedIndex != 5 {
		t.Fatalf("restart effects = %+v", start)
	}
}

func TestInstallSnapshotPreservesMatchingSuffix(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a", "b", "c"}
	state := PersistentState{HardState: HardState{CurrentTerm: 3, CommitIndex: 3}, Log: []Entry{
		{Index: 1, Term: 1, Type: EntryCommand}, {Index: 2, Term: 1, Type: EntryCommand}, {Index: 3, Term: 2, Type: EntryCommand},
		{Index: 4, Term: 2, Type: EntryCommand}, {Index: 5, Term: 3, Type: EntryCommand}, {Index: 6, Term: 3, Type: EntryCommand},
	}}
	node := mustNode(t, "b", members, state, 3)
	snapshot := Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 2, Members: members, Data: []byte("through-four")}
	persist, _ := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: InstallSnapshot, From: "a", To: "b", Term: 3, Sequence: 1, Snapshot: snapshot}}))
	if len(persist.State.Log) != 2 || persist.State.Log[0].Index != 5 || persist.State.Log[1].Index != 6 {
		t.Fatalf("preserved suffix = %+v", persist.State)
	}
}

func TestLeaderSendsSnapshotToFollowerBehindCompaction(t *testing.T) {
	t.Parallel()

	members := []NodeID{"a", "b", "c"}
	state := PersistentState{HardState: HardState{CurrentTerm: 2, CommitIndex: 4}, Snapshot: Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Members: members, Data: []byte("s3")}, Log: []Entry{{Index: 4, Term: 2, Type: EntryCommand}}}
	node := mustNode(t, "a", members, state, 4)
	_, election := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputElectionTimeout}))
	request := findMessage(t, election, RequestVote, "b")
	_, leaderEffects := acknowledgeOnlyPersist(t, node, mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: RequestVoteResponse, From: "b", To: "a", Term: request.Term, VoteGranted: true}}))
	appendMessage := findMessage(t, leaderEffects, AppendEntries, "b")
	retry := mustStep(t, node, Input{Kind: InputMessage, Message: Message{Type: AppendEntriesResponse, From: "b", To: "a", Term: request.Term, Sequence: appendMessage.Sequence, Success: false, RejectHint: 1}})
	snapshotMessage := findMessage(t, retry, InstallSnapshot, "b")
	if snapshotMessage.Snapshot.LastIncludedIndex != 3 || string(snapshotMessage.Snapshot.Data) != "s3" {
		t.Fatalf("snapshot message = %+v", snapshotMessage)
	}
}

func mustNode(t *testing.T, id NodeID, members []NodeID, state PersistentState, applied uint64) *Node {
	t.Helper()
	node, err := New(Config{ID: id, Members: members, AppliedIndex: applied}, state)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return node
}

func mustStep(t *testing.T, node *Node, input Input) []Effect {
	t.Helper()
	effects, err := node.Step(input)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return effects
}

func acknowledgeOnlyPersist(t *testing.T, node *Node, effects []Effect) (Effect, []Effect) {
	t.Helper()
	if len(effects) != 1 || effects[0].Kind != EffectPersist || effects[0].WriteToken == 0 {
		t.Fatalf("effects before persistence = %+v", effects)
	}
	after := mustStep(t, node, Input{Kind: InputPersisted, WriteToken: effects[0].WriteToken})
	return effects[0], after
}

func effectKinds(effects []Effect) []EffectKind {
	result := make([]EffectKind, len(effects))
	for index, effect := range effects {
		result[index] = effect.Kind
	}
	return result
}

func findMessage(t *testing.T, effects []Effect, kind MessageType, to NodeID) Message {
	t.Helper()
	for _, effect := range effects {
		if effect.Kind == EffectSend && effect.Message.Type == kind && effect.Message.To == to {
			return CloneMessage(effect.Message)
		}
	}
	t.Fatalf("message %s to %q not found in %+v", kind, to, effects)
	return Message{}
}

func entriesEqual(left, right []Entry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Index != right[index].Index || left[index].Term != right[index].Term || left[index].Type != right[index].Type || !slices.Equal(left[index].Data, right[index].Data) {
			return false
		}
	}
	return true
}
