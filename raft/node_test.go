package raft

import (
	"errors"
	"math"
	"slices"
	"testing"
)

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
