package check

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/aminkbi/d-raft/raft"
)

func TestCheckerStateCanonicalOrdering(t *testing.T) {
	t.Parallel()

	state := stateTestChecker().State()
	if !slices.Equal(state.Seen, []string{"fp-a", "fp-b", "fp-c"}) {
		t.Fatalf("seen = %v", state.Seen)
	}
	if !slices.Equal(state.Leaders, []TermLeader{{Term: 2, Leader: "b"}, {Term: 7, Leader: "a"}, {Term: 11, Leader: "c"}}) {
		t.Fatalf("leaders = %+v", state.Leaders)
	}
	if !slices.Equal(state.Votes, []NodeTermVote{
		{Node: "a", Term: 2, Candidate: "b"},
		{Node: "a", Term: 9, Candidate: "a"},
		{Node: "b", Term: 1, Candidate: "a"},
	}) {
		t.Fatalf("votes = %+v", state.Votes)
	}
	if !slices.Equal(state.DurableTerms, []NodeValue{{Node: "a", Value: 4}, {Node: "b", Value: 3}, {Node: "c", Value: 5}}) {
		t.Fatalf("durable terms = %+v", state.DurableTerms)
	}
	if got := indexes(state.Committed); !slices.Equal(got, []uint64{1, 3, 8}) {
		t.Fatalf("committed indexes = %v", got)
	}
	if got := snapshotIndexes(state.Snapshots); !slices.Equal(got, []uint64{2, 7}) {
		t.Fatalf("snapshot indexes = %v", got)
	}
}

func TestCheckerStateSensitiveToRetainedHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Checker)
	}{
		{"seen", func(c *Checker) { c.seen["new"] = struct{}{} }},
		{"leaders", func(c *Checker) { c.leaders[20] = "a" }},
		{"votes", func(c *Checker) { c.votes["a"][20] = "c" }},
		{"durable terms", func(c *Checker) { c.durableTerms["a"]++ }},
		{"commit indexes", func(c *Checker) { c.commitIndexes["a"]++ }},
		{"applied indexes", func(c *Checker) { c.appliedIndexes["a"]++ }},
		{"committed", func(c *Checker) { c.committed[1] = entryWitness{Node: "b", Entry: testWitnessEntry(1, "changed")} }},
		{"applied", func(c *Checker) { c.applied[1] = entryWitness{Node: "b", Entry: testWitnessEntry(1, "changed")} }},
		{"snapshots", func(c *Checker) { witness := c.snapshots[2]; witness.Snapshot.Data[0] = 'X'; c.snapshots[2] = witness }},
		{"violations", func(c *Checker) { c.violations[0].AtNS++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checker := stateTestChecker()
			before := checker.State()
			test.mutate(checker)
			if reflect.DeepEqual(before, checker.State()) {
				t.Fatal("state export did not reflect retained-history mutation")
			}
		})
	}
}

func TestCheckerStateIsDeepCopy(t *testing.T) {
	t.Parallel()

	checker := stateTestChecker()
	want := checker.State()
	exported := checker.State()

	exported.Members[0] = "changed"
	exported.Initial.Voters[0] = "changed"
	exported.Seen[0] = "changed"
	exported.Leaders[0].Leader = "changed"
	exported.Votes[0].Candidate = "changed"
	exported.DurableTerms[0].Value = 0
	exported.CommitIndexes[0].Value = 0
	exported.AppliedIndexes[0].Value = 0
	exported.Committed[0].Entry.Data[0] = 0
	exported.Committed[0].Entry.Membership.Voters[0] = "changed"
	exported.Applied[0].Entry.Data[0] = 0
	exported.Snapshots[0].Snapshot.Members[0] = "changed"
	exported.Snapshots[0].Snapshot.Data[0] = 0
	exported.Snapshots[0].Snapshot.Membership.Voters[0] = "changed"
	exported.Violations[0].Nodes[0] = "changed"
	exported.Violations[0].Evidence[0] = 'X'

	if got := checker.State(); !reflect.DeepEqual(got, want) {
		t.Fatalf("mutating exported state changed checker\n got: %#v\nwant: %#v", got, want)
	}
}

func stateTestChecker() *Checker {
	members := []raft.NodeID{"a", "b", "c"}
	membership := raft.StableMembership(members, nil)
	entries := map[uint64]entryWitness{
		8: {Node: "c", Entry: testWitnessEntry(8, "eight"), CommitTerm: 9},
		1: {Node: "a", Entry: testWitnessEntry(1, "one"), CommitTerm: 2},
		3: {Node: "b", Entry: testWitnessEntry(3, "three"), CommitTerm: 4},
	}
	applied := map[uint64]entryWitness{
		3: {Node: "b", Entry: testWitnessEntry(3, "three")},
		1: {Node: "a", Entry: testWitnessEntry(1, "one")},
	}
	snapshots := map[uint64]snapshotWitness{
		7: {Node: "c", Snapshot: raft.Snapshot{LastIncludedIndex: 7, Members: members, Data: []byte("seven"), Membership: membership}},
		2: {Node: "a", Snapshot: raft.Snapshot{LastIncludedIndex: 2, Members: members, Data: []byte("two"), Membership: membership}},
	}
	return &Checker{
		members:        members,
		initial:        membership,
		seen:           map[string]struct{}{"fp-c": {}, "fp-a": {}, "fp-b": {}},
		leaders:        map[uint64]raft.NodeID{11: "c", 2: "b", 7: "a"},
		votes:          map[raft.NodeID]map[uint64]raft.NodeID{"b": {1: "a"}, "a": {9: "a", 2: "b"}},
		durableTerms:   map[raft.NodeID]uint64{"c": 5, "a": 4, "b": 3},
		commitIndexes:  map[raft.NodeID]uint64{"b": 3, "a": 1},
		appliedIndexes: map[raft.NodeID]uint64{"c": 2, "a": 1},
		committed:      entries,
		applied:        applied,
		snapshots:      snapshots,
		violations:     []Violation{{ID: CommittedConflict, AtNS: 5, Nodes: []raft.NodeID{"a", "b"}, Evidence: json.RawMessage(`{"index":1}`), Fingerprint: "fingerprint"}},
	}
}

func testWitnessEntry(index uint64, data string) raft.Entry {
	return raft.Entry{Index: index, Term: index + 1, Type: raft.EntryCommand, Data: []byte(data), Membership: raft.StableMembership([]raft.NodeID{"a", "b", "c"}, nil)}
}

func indexes(witnesses []IndexedEntryWitness) []uint64 {
	result := make([]uint64, len(witnesses))
	for index, witness := range witnesses {
		result[index] = witness.Index
	}
	return result
}

func snapshotIndexes(witnesses []IndexedSnapshotWitness) []uint64 {
	result := make([]uint64, len(witnesses))
	for index, witness := range witnesses {
		result[index] = witness.Index
	}
	return result
}
