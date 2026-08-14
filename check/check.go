// Package check implements independent, history-aware Raft safety checkers.
// It consumes canonical observations rather than trusting claims made by the
// protocol implementation under test.
package check

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/aminkbi/d-raft/raft"
)

const (
	SchemaV1      = "d-raft.check/v1"
	SchemaVersion = "d-raft.check/v2"
)

const (
	ElectionSafety       = "raft/election-safety"
	ElectionCertificate  = "raft/election-certificate"
	DurableTermMonotonic = "raft/durable-term-monotonic"
	DurableDoubleVote    = "raft/durable-double-vote"
	VolatileDurableMatch = "raft/volatile-durable-match"
	LogMatching          = "raft/log-matching"
	LeaderCompleteness   = "raft/leader-completeness"
	CommittedConflict    = "raft/committed-conflict"
	CommitMonotonic      = "raft/commit-monotonic"
	AppliedConflict      = "raft/applied-conflict"
	AppliedMonotonic     = "raft/applied-monotonic"
	SnapshotConflict     = "raft/snapshot-conflict"
)

// NodeObservation combines independent durable/application state with an
// optional live protocol status.
type NodeObservation struct {
	ID           raft.NodeID
	Up           bool
	Status       *raft.Status
	Durable      raft.PersistentState
	AppliedIndex uint64
	Applied      []raft.Entry
}

// Observation is one cluster-wide safety-check boundary.
type Observation struct {
	At      time.Duration
	Members []raft.NodeID
	Nodes   []NodeObservation
}

// Violation is a portable safety witness. Fingerprint is stable for equivalent
// canonical evidence and can be used as a shrinker's preservation target.
type Violation struct {
	ID          string          `json:"id"`
	AtNS        int64           `json:"at_ns"`
	Nodes       []raft.NodeID   `json:"nodes,omitempty"`
	Evidence    json.RawMessage `json:"evidence"`
	Fingerprint string          `json:"fingerprint"`
}

func (v Violation) Error() string {
	return fmt.Sprintf("%s at %dns [%s]: %s", v.ID, v.AtNS, v.Fingerprint, v.Evidence)
}

// Fingerprint returns the stable identity of canonical invariant evidence.
func Fingerprint(id string, nodes []raft.NodeID, evidence json.RawMessage) (string, error) {
	if id == "" || !json.Valid(evidence) {
		return "", errors.New("check: invalid violation identity or evidence")
	}
	canonicalNodes := slices.Clone(nodes)
	slices.Sort(canonicalNodes)
	var canonicalEvidence bytes.Buffer
	if err := json.Compact(&canonicalEvidence, evidence); err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte(id))
	hash.Write([]byte{0})
	for _, node := range canonicalNodes {
		hash.Write([]byte(node))
		hash.Write([]byte{0})
	}
	hash.Write(canonicalEvidence.Bytes())
	return hex.EncodeToString(hash.Sum(nil)[:12]), nil
}

// ValidateViolation verifies canonical node order, evidence, and fingerprint.
func ValidateViolation(violation Violation) error {
	if !slices.IsSorted(violation.Nodes) {
		return errors.New("check: violation nodes are not sorted")
	}
	for index, node := range violation.Nodes {
		if node == "" || index > 0 && node == violation.Nodes[index-1] {
			return errors.New("check: violation nodes are empty or duplicated")
		}
	}
	fingerprint, err := Fingerprint(violation.ID, violation.Nodes, violation.Evidence)
	if err != nil {
		return err
	}
	if fingerprint != violation.Fingerprint {
		return errors.New("check: violation fingerprint mismatch")
	}
	return nil
}

// ValidateViolationForSchema additionally verifies that an invariant ID belongs
// to the declared built-in checker vocabulary.
func ValidateViolationForSchema(schema string, violation Violation) error {
	if err := ValidateViolation(violation); err != nil {
		return err
	}
	if !violationSupported(schema, violation.ID) {
		return fmt.Errorf("check: violation %q is not defined by %s", violation.ID, schema)
	}
	return nil
}

func violationSupported(schema, id string) bool {
	switch id {
	case ElectionSafety, ElectionCertificate, DurableTermMonotonic, DurableDoubleVote, VolatileDurableMatch, LogMatching, LeaderCompleteness, CommittedConflict, CommitMonotonic, AppliedConflict, AppliedMonotonic:
		return schema == SchemaV1 || schema == SchemaVersion
	case SnapshotConflict:
		return schema == SchemaVersion
	default:
		return false
	}
}

type entryWitness struct {
	Node       raft.NodeID `json:"node"`
	Entry      raft.Entry  `json:"entry"`
	CommitTerm uint64      `json:"commit_term,omitempty"`
}

// Checker retains only the history needed by Raft safety properties.
type Checker struct {
	members []raft.NodeID
	seen    map[string]struct{}

	leaders        map[uint64]raft.NodeID
	votes          map[raft.NodeID]map[uint64]raft.NodeID
	durableTerms   map[raft.NodeID]uint64
	commitIndexes  map[raft.NodeID]uint64
	appliedIndexes map[raft.NodeID]uint64
	committed      map[uint64]entryWitness
	applied        map[uint64]entryWitness
	snapshots      map[uint64]snapshotWitness
	violations     []Violation
}

type snapshotWitness struct {
	Node     raft.NodeID   `json:"node"`
	Snapshot raft.Snapshot `json:"snapshot"`
}

// New returns a checker for fixed members.
func New(members []raft.NodeID) *Checker {
	canonical := slices.Clone(members)
	slices.Sort(canonical)
	return &Checker{
		members:        canonical,
		seen:           make(map[string]struct{}),
		leaders:        make(map[uint64]raft.NodeID),
		votes:          make(map[raft.NodeID]map[uint64]raft.NodeID),
		durableTerms:   make(map[raft.NodeID]uint64),
		commitIndexes:  make(map[raft.NodeID]uint64),
		appliedIndexes: make(map[raft.NodeID]uint64),
		committed:      make(map[uint64]entryWitness),
		applied:        make(map[uint64]entryWitness),
		snapshots:      make(map[uint64]snapshotWitness),
	}
}

// Observe checks one transition boundary and returns newly discovered
// violations. Repeated equivalent evidence is reported once.
func (c *Checker) Observe(observation Observation) []Violation {
	nodes := slices.Clone(observation.Nodes)
	slices.SortFunc(nodes, func(left, right NodeObservation) int { return stringCompare(left.ID, right.ID) })
	start := len(c.violations)

	for _, node := range nodes {
		c.checkDurableHistory(observation.At, node)
		c.checkCommittedHistory(observation.At, node)
		c.checkAppliedHistory(observation.At, node)
		c.checkSnapshotHistory(observation.At, node)
	}
	c.checkLogs(observation.At, nodes)
	for _, node := range nodes {
		if node.Up && node.Status != nil && node.Status.Role == raft.Leader && !node.Status.AwaitingPersistence {
			c.checkLeader(observation.At, node, nodes)
		}
	}
	return slices.Clone(c.violations[start:])
}

// Violations returns all unique violations observed so far.
func (c *Checker) Violations() []Violation {
	return slices.Clone(c.violations)
}

func (c *Checker) checkDurableHistory(at time.Duration, node NodeObservation) {
	hard := node.Durable.HardState
	if previous, exists := c.durableTerms[node.ID]; exists && hard.CurrentTerm < previous {
		c.add(at, DurableTermMonotonic, []raft.NodeID{node.ID}, map[string]any{"previous": previous, "current": hard.CurrentTerm})
	}
	if hard.CurrentTerm > c.durableTerms[node.ID] {
		c.durableTerms[node.ID] = hard.CurrentTerm
	}
	if hard.VotedFor != "" {
		ledger := c.votes[node.ID]
		if ledger == nil {
			ledger = make(map[uint64]raft.NodeID)
			c.votes[node.ID] = ledger
		}
		if previous, exists := ledger[hard.CurrentTerm]; exists && previous != hard.VotedFor {
			c.add(at, DurableDoubleVote, []raft.NodeID{node.ID, previous, hard.VotedFor}, map[string]any{"term": hard.CurrentTerm, "first": previous, "second": hard.VotedFor})
		} else {
			ledger[hard.CurrentTerm] = hard.VotedFor
		}
	}
	if node.Up && node.Status != nil && !node.Status.AwaitingPersistence {
		if node.Status.Term != hard.CurrentTerm || node.Status.VotedFor != hard.VotedFor || node.Status.CommitIndex != hard.CommitIndex {
			c.add(at, VolatileDurableMatch, []raft.NodeID{node.ID}, map[string]any{"status": node.Status, "hard_state": hard})
		}
	}
}

func (c *Checker) checkCommittedHistory(at time.Duration, node NodeObservation) {
	commit := node.Durable.HardState.CommitIndex
	if previous := c.commitIndexes[node.ID]; commit < previous {
		c.add(at, CommitMonotonic, []raft.NodeID{node.ID}, map[string]any{"previous": previous, "current": commit})
	}
	if commit > c.commitIndexes[node.ID] {
		c.commitIndexes[node.ID] = commit
	}
	for _, entry := range node.Durable.Log {
		index := entry.Index
		if index > commit {
			break
		}
		if previous, exists := c.committed[index]; exists && !entriesEqual(previous.Entry, entry) {
			c.add(at, CommittedConflict, []raft.NodeID{previous.Node, node.ID}, map[string]any{"index": index, "first": previous, "second": entryWitness{Node: node.ID, Entry: entry}})
		} else if !exists {
			c.committed[index] = entryWitness{Node: node.ID, Entry: raft.CloneEntry(entry), CommitTerm: node.Durable.HardState.CurrentTerm}
		}
	}
}

func (c *Checker) checkSnapshotHistory(at time.Duration, node NodeObservation) {
	snapshot := node.Durable.Snapshot
	if snapshot.LastIncludedIndex == 0 {
		return
	}
	if previous, exists := c.snapshots[snapshot.LastIncludedIndex]; exists {
		if previous.Snapshot.LastIncludedTerm != snapshot.LastIncludedTerm || !slices.Equal(previous.Snapshot.Members, snapshot.Members) || !slices.Equal(previous.Snapshot.Data, snapshot.Data) {
			c.add(at, SnapshotConflict, []raft.NodeID{previous.Node, node.ID}, map[string]any{"index": snapshot.LastIncludedIndex, "first": previous, "second": snapshotWitness{Node: node.ID, Snapshot: raft.CloneSnapshot(snapshot)}})
		}
	} else {
		c.snapshots[snapshot.LastIncludedIndex] = snapshotWitness{Node: node.ID, Snapshot: raft.CloneSnapshot(snapshot)}
	}
}

func (c *Checker) checkAppliedHistory(at time.Duration, node NodeObservation) {
	if previous := c.appliedIndexes[node.ID]; node.AppliedIndex < previous {
		c.add(at, AppliedMonotonic, []raft.NodeID{node.ID}, map[string]any{"previous": previous, "current": node.AppliedIndex})
	}
	if node.AppliedIndex > c.appliedIndexes[node.ID] {
		c.appliedIndexes[node.ID] = node.AppliedIndex
	}
	for _, entry := range node.Applied {
		if previous, exists := c.applied[entry.Index]; exists && !entriesEqual(previous.Entry, entry) {
			c.add(at, AppliedConflict, []raft.NodeID{previous.Node, node.ID}, map[string]any{"index": entry.Index, "first": previous, "second": entryWitness{Node: node.ID, Entry: entry}})
		} else if !exists {
			c.applied[entry.Index] = entryWitness{Node: node.ID, Entry: raft.CloneEntry(entry)}
		}
	}
}

func (c *Checker) checkLogs(at time.Duration, nodes []NodeObservation) {
	for leftIndex, left := range nodes {
		for _, right := range nodes[leftIndex+1:] {
			boundary := max(left.Durable.Snapshot.LastIncludedIndex, right.Durable.Snapshot.LastIncludedIndex)
			if boundary == math.MaxUint64 {
				continue
			}
			start := boundary + 1
			limit := min(stateLastIndex(left.Durable), stateLastIndex(right.Durable))
			if start > limit {
				continue
			}
			for index := start; ; index++ {
				leftEntry, leftOK := stateEntryAt(left.Durable, index)
				rightEntry, rightOK := stateEntryAt(right.Durable, index)
				if leftOK && rightOK && leftEntry.Term == rightEntry.Term {
					for prefix := start; ; prefix++ {
						leftPrefix, leftExists := stateEntryAt(left.Durable, prefix)
						rightPrefix, rightExists := stateEntryAt(right.Durable, prefix)
						if !leftExists || !rightExists || !entriesEqual(leftPrefix, rightPrefix) {
							c.add(at, LogMatching, []raft.NodeID{left.ID, right.ID}, map[string]any{"matching_index": index, "conflicting_index": prefix, "term": leftEntry.Term})
							break
						}
						if prefix == index {
							break
						}
					}
				}
				if index == limit {
					break
				}
			}
		}
	}
}

func (c *Checker) checkLeader(at time.Duration, node NodeObservation, nodes []NodeObservation) {
	status := node.Status
	if previous, exists := c.leaders[status.Term]; exists && previous != node.ID {
		c.add(at, ElectionSafety, []raft.NodeID{previous, node.ID}, map[string]any{"term": status.Term, "first": previous, "second": node.ID})
	} else {
		c.leaders[status.Term] = node.ID
	}
	votes := 0
	for _, voter := range c.members {
		if c.votes[voter][status.Term] == node.ID {
			votes++
		}
	}
	if votes < len(c.members)/2+1 {
		c.add(at, ElectionCertificate, []raft.NodeID{node.ID}, map[string]any{"term": status.Term, "votes": votes, "required": len(c.members)/2 + 1})
	}
	for index, committed := range c.committed {
		if status.Term < committed.CommitTerm {
			continue
		}
		if index <= status.Snapshot.LastIncludedIndex {
			continue
		}
		entry, exists := statusEntryAt(status, index)
		if !exists || !entriesEqual(entry, committed.Entry) {
			c.add(at, LeaderCompleteness, []raft.NodeID{node.ID, committed.Node}, map[string]any{"term": status.Term, "index": index, "committed": committed})
		}
	}
	_ = nodes
}

func stateLastIndex(state raft.PersistentState) uint64 {
	boundary := state.Snapshot.LastIncludedIndex
	if uint64(len(state.Log)) > math.MaxUint64-boundary {
		return math.MaxUint64
	}
	return boundary + uint64(len(state.Log))
}

func stateEntryAt(state raft.PersistentState, index uint64) (raft.Entry, bool) {
	boundary := state.Snapshot.LastIncludedIndex
	if index <= boundary {
		return raft.Entry{}, false
	}
	offset := index - boundary - 1
	if offset >= uint64(len(state.Log)) {
		return raft.Entry{}, false
	}
	return state.Log[offset], true
}

func statusEntryAt(status *raft.Status, index uint64) (raft.Entry, bool) {
	boundary := status.Snapshot.LastIncludedIndex
	if index <= boundary {
		return raft.Entry{}, false
	}
	offset := index - boundary - 1
	if offset >= uint64(len(status.Log)) {
		return raft.Entry{}, false
	}
	return status.Log[offset], true
}

func (c *Checker) add(at time.Duration, id string, nodes []raft.NodeID, evidence any) {
	canonicalNodes := slices.Clone(nodes)
	slices.Sort(canonicalNodes)
	raw, err := json.Marshal(evidence)
	if err != nil {
		panic(fmt.Sprintf("check: marshal invariant evidence: %v", err))
	}
	fingerprint, err := Fingerprint(id, canonicalNodes, raw)
	if err != nil {
		panic(fmt.Sprintf("check: fingerprint invariant evidence: %v", err))
	}
	if _, exists := c.seen[fingerprint]; exists {
		return
	}
	c.seen[fingerprint] = struct{}{}
	c.violations = append(c.violations, Violation{ID: id, AtNS: int64(at), Nodes: canonicalNodes, Evidence: raw, Fingerprint: fingerprint})
}

func entriesEqual(left, right raft.Entry) bool {
	return left.Index == right.Index && left.Term == right.Term && left.Type == right.Type && slices.Equal(left.Data, right.Data)
}

func stringCompare(left, right raft.NodeID) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
