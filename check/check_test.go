package check

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/aminkbi/d-raft/raft"
)

func TestViolationFingerprintValidation(t *testing.T) {
	t.Parallel()

	evidence := json.RawMessage(`{"term":4}`)
	fingerprint, err := Fingerprint(ElectionSafety, []raft.NodeID{"a", "b"}, evidence)
	if err != nil {
		t.Fatal(err)
	}
	violation := Violation{ID: ElectionSafety, Nodes: []raft.NodeID{"a", "b"}, Evidence: json.RawMessage(" { \"term\" : 4 } "), Fingerprint: fingerprint}
	if err := ValidateViolation(violation); err != nil {
		t.Fatalf("ValidateViolation: %v", err)
	}
	violation.Evidence = json.RawMessage(`{"term":5}`)
	if err := ValidateViolation(violation); err == nil {
		t.Fatal("tampered evidence passed validation")
	}
}

func TestViolationSchemaVocabulary(t *testing.T) {
	t.Parallel()

	evidence := json.RawMessage(`{"index":1}`)
	fingerprint, err := Fingerprint(SnapshotConflict, []raft.NodeID{"a"}, evidence)
	if err != nil {
		t.Fatal(err)
	}
	violation := Violation{ID: SnapshotConflict, Nodes: []raft.NodeID{"a"}, Evidence: evidence, Fingerprint: fingerprint}
	if err := ValidateViolationForSchema(SchemaVersion, violation); err != nil {
		t.Fatalf("v2 validation: %v", err)
	}
	if err := ValidateViolationForSchema(SchemaV1, violation); err == nil {
		t.Fatal("snapshot violation passed checker v1 validation")
	}
}

func TestCheckerAcceptsCertifiedLeader(t *testing.T) {
	t.Parallel()

	checker := New([]raft.NodeID{"a", "b", "c"})
	checker.Observe(observation(
		node("a", raft.HardState{CurrentTerm: 1, VotedFor: "a"}, nil),
		node("b", raft.HardState{CurrentTerm: 1, VotedFor: "a"}, nil),
		node("c", raft.HardState{CurrentTerm: 1}, nil),
	))
	leader := raft.Status{ID: "a", Role: raft.Leader, Term: 1, VotedFor: "a"}
	violations := checker.Observe(observation(
		node("a", raft.HardState{CurrentTerm: 1, VotedFor: "a"}, &leader),
		node("b", raft.HardState{CurrentTerm: 1, VotedFor: "a"}, nil),
		node("c", raft.HardState{CurrentTerm: 1}, nil),
	))
	if len(violations) != 0 {
		t.Fatalf("valid election violations = %+v", violations)
	}
}

func TestCheckerFindsElectionSafetyAndMissingCertificate(t *testing.T) {
	t.Parallel()

	checker := New([]raft.NodeID{"a", "b", "c"})
	leaderA := raft.Status{ID: "a", Role: raft.Leader, Term: 3, VotedFor: "a"}
	checker.Observe(observation(
		node("a", raft.HardState{CurrentTerm: 3, VotedFor: "a"}, &leaderA),
		node("b", raft.HardState{CurrentTerm: 3}, nil),
		node("c", raft.HardState{CurrentTerm: 3}, nil),
	))
	leaderB := raft.Status{ID: "b", Role: raft.Leader, Term: 3, VotedFor: "b"}
	violations := checker.Observe(observation(
		node("a", raft.HardState{CurrentTerm: 3, VotedFor: "a"}, nil),
		node("b", raft.HardState{CurrentTerm: 3, VotedFor: "b"}, &leaderB),
		node("c", raft.HardState{CurrentTerm: 3}, nil),
	))
	if !hasViolation(violations, ElectionSafety) || !hasViolation(checker.Violations(), ElectionCertificate) {
		t.Fatalf("violations = %+v", checker.Violations())
	}
}

func TestCheckerFindsDurableDoubleVote(t *testing.T) {
	t.Parallel()

	checker := New([]raft.NodeID{"a", "b", "c"})
	checker.Observe(observation(node("a", raft.HardState{CurrentTerm: 2, VotedFor: "b"}, nil)))
	violations := checker.Observe(observation(node("a", raft.HardState{CurrentTerm: 2, VotedFor: "c"}, nil)))
	if !hasViolation(violations, DurableDoubleVote) {
		t.Fatalf("violations = %+v", violations)
	}
}

func TestCheckerFindsCommittedConflict(t *testing.T) {
	t.Parallel()

	checker := New([]raft.NodeID{"a", "b", "c"})
	left := Entry(1, 1, "left")
	right := Entry(1, 1, "right")
	violations := checker.Observe(observation(
		nodeWithLog("a", raft.HardState{CurrentTerm: 1, CommitIndex: 1}, []raft.Entry{left}),
		nodeWithLog("b", raft.HardState{CurrentTerm: 1, CommitIndex: 1}, []raft.Entry{right}),
	))
	if !hasViolation(violations, CommittedConflict) || !hasViolation(violations, LogMatching) {
		t.Fatalf("violations = %+v", violations)
	}
}

func TestCheckerFindsSnapshotConflict(t *testing.T) {
	t.Parallel()

	checker := New([]raft.NodeID{"a", "b", "c"})
	observation := observation(
		node("a", raft.HardState{CurrentTerm: 2, CommitIndex: 4}, nil),
		node("b", raft.HardState{CurrentTerm: 2, CommitIndex: 4}, nil),
	)
	observation.Nodes[0].Durable.Snapshot = raft.Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 2, Members: []raft.NodeID{"a", "b", "c"}, Data: []byte("left")}
	observation.Nodes[1].Durable.Snapshot = raft.Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 2, Members: []raft.NodeID{"a", "b", "c"}, Data: []byte("right")}
	violations := checker.Observe(observation)
	found := false
	for _, violation := range violations {
		found = found || violation.ID == SnapshotConflict
	}
	if !found {
		t.Fatalf("violations = %+v", violations)
	}
}

func TestCheckerHandlesMaximumLogIndex(t *testing.T) {
	t.Parallel()

	members := []raft.NodeID{"a", "b"}
	snapshot := raft.Snapshot{LastIncludedIndex: math.MaxUint64 - 1, LastIncludedTerm: 1, Members: members, Data: []byte("prefix")}
	state := raft.PersistentState{
		HardState: raft.HardState{CurrentTerm: 2, CommitIndex: math.MaxUint64},
		Snapshot:  snapshot,
		Log:       []raft.Entry{{Index: math.MaxUint64, Term: 2, Type: raft.EntryCommand}},
	}
	checker := New(members)
	violations := checker.Observe(Observation{Members: members, Nodes: []NodeObservation{{ID: "a", Durable: state}, {ID: "b", Durable: raft.ClonePersistentState(state)}}})
	if len(violations) != 0 {
		t.Fatalf("violations = %+v", violations)
	}
}

func observation(nodes ...NodeObservation) Observation {
	return Observation{Members: []raft.NodeID{"a", "b", "c"}, Nodes: nodes}
}

func node(id raft.NodeID, hard raft.HardState, status *raft.Status) NodeObservation {
	return NodeObservation{ID: id, Up: status != nil, Status: status, Durable: raft.PersistentState{HardState: hard}}
}

func nodeWithLog(id raft.NodeID, hard raft.HardState, log []raft.Entry) NodeObservation {
	return NodeObservation{ID: id, Durable: raft.PersistentState{HardState: hard, Log: log}}
}

func Entry(index, term uint64, data string) raft.Entry {
	return raft.Entry{Index: index, Term: term, Type: raft.EntryCommand, Data: []byte(data)}
}

func hasViolation(violations []Violation, id string) bool {
	for _, violation := range violations {
		if violation.ID == id {
			return true
		}
	}
	return false
}
