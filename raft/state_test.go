package raft

import (
	"reflect"
	"slices"
	"testing"
)

func TestNodeStateCanonicalOrderingAndSensitivity(t *testing.T) {
	t.Parallel()

	node := stateTestNode()
	state := node.State()
	if !slices.Equal(state.Votes, []NodeID{"a", "b", "c"}) {
		t.Fatalf("votes = %v", state.Votes)
	}
	wantNext := []NodeIndex{{Node: "a", Value: 7}, {Node: "b", Value: 3}, {Node: "c", Value: 5}}
	if !slices.Equal(state.NextIndex, wantNext) {
		t.Fatalf("next indexes = %+v, want %+v", state.NextIndex, wantNext)
	}
	wantMatch := []NodeIndex{{Node: "a", Value: 6}, {Node: "b", Value: 2}, {Node: "c", Value: 4}}
	if !slices.Equal(state.MatchIndex, wantMatch) {
		t.Fatalf("match indexes = %+v, want %+v", state.MatchIndex, wantMatch)
	}
	wantSequence := []NodeIndex{{Node: "a", Value: 11}, {Node: "b", Value: 13}, {Node: "c", Value: 12}}
	if !slices.Equal(state.AppendSequence, wantSequence) {
		t.Fatalf("append sequences = %+v, want %+v", state.AppendSequence, wantSequence)
	}

	tests := []struct {
		name   string
		mutate func(*Node)
	}{
		{"votes", func(n *Node) { n.votes["d"] = struct{}{} }},
		{"replication", func(n *Node) { n.matchIndex["b"]++ }},
		{"sequence", func(n *Node) { n.appendSequence["c"]++ }},
		{"write token", func(n *Node) { n.nextWriteToken++ }},
		{"pending effects", func(n *Node) { n.pending.effects[0].Entry.Data[0] = 'X' }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			n := stateTestNode()
			before := n.State()
			test.mutate(n)
			if reflect.DeepEqual(before, n.State()) {
				t.Fatal("state export did not reflect future-relevant mutation")
			}
		})
	}
}

func TestNodeStateIsDeepCopy(t *testing.T) {
	t.Parallel()

	node := stateTestNode()
	want := node.State()
	exported := node.State()

	exported.Members[0] = "changed"
	exported.InitialMembership.Voters[0] = "changed"
	exported.Membership.Voters[0] = "changed"
	exported.Persistent.Snapshot.Members[0] = "changed"
	exported.Persistent.Snapshot.Data[0] = 0
	exported.Persistent.Snapshot.Membership.Voters[0] = "changed"
	exported.Persistent.Log[0].Data[0] = 0
	exported.Persistent.Log[0].Membership.Voters[0] = "changed"
	exported.Votes[0] = "changed"
	exported.ElectionVotes[0] = "changed"
	exported.ElectionMembership.Voters[0] = "changed"
	exported.NextIndex[0].Value = 0
	exported.MatchIndex[0].Value = 0
	exported.AppendSequence[0].Value = 0
	exported.Pending.Effects[0].State.Log[0].Data[0] = 0
	exported.Pending.Effects[0].Message.Entries[0].Data[0] = 0
	exported.Pending.Effects[0].Message.Snapshot.Data[0] = 0
	exported.Pending.Effects[0].Entry.Data[0] = 0
	exported.Pending.Effects[0].Snapshot.Data[0] = 0

	if got := node.State(); !reflect.DeepEqual(got, want) {
		t.Fatalf("mutating exported state changed node\n got: %#v\nwant: %#v", got, want)
	}
}

func stateTestNode() *Node {
	membership := stableMembership([]NodeID{"a", "b", "c"}, nil)
	entry := Entry{Index: 3, Term: 2, Type: EntryConfigFinal, Data: []byte("entry"), Membership: membership}
	snapshot := Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Members: []NodeID{"a", "b", "c"}, Data: []byte("snapshot"), Membership: membership}
	persistent := PersistentState{HardState: HardState{CurrentTerm: 2, VotedFor: "a", CommitIndex: 2}, Snapshot: snapshot, Log: []Entry{entry}}
	effect := Effect{
		Kind:       EffectSend,
		WriteToken: 8,
		State:      persistent,
		Message:    Message{Entries: []Entry{entry}, Snapshot: snapshot},
		Entry:      entry,
		Snapshot:   snapshot,
	}
	return &Node{
		id:                 "a",
		members:            []NodeID{"a", "b", "c"},
		initialMembership:  membership,
		membership:         membership,
		role:               Leader,
		leader:             "a",
		state:              persistent,
		applied:            2,
		votes:              map[NodeID]struct{}{"c": {}, "a": {}, "b": {}},
		electionVotes:      []NodeID{"a", "c"},
		electionMembership: membership,
		nextIndex:          map[NodeID]uint64{"c": 5, "a": 7, "b": 3},
		matchIndex:         map[NodeID]uint64{"b": 2, "c": 4, "a": 6},
		appendSequence:     map[NodeID]uint64{"c": 12, "b": 13, "a": 11},
		nextWriteToken:     9,
		pending:            &pendingWrite{token: 8, effects: []Effect{effect}},
	}
}
