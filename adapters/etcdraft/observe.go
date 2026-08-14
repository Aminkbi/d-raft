package etcdraft

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	rootraft "github.com/aminkbi/d-raft/raft"
	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const ObservationSchemaVersion = "d-raft.etcdraft-observation/v1"
const CheckerProfile = "d-raft.etcdraft-check/common-durable-v1"

type nodeSnapshot struct {
	ID           rootraft.NodeID          `json:"id"`
	Up           bool                     `json:"up"`
	Status       *rootraft.Status         `json:"status,omitempty"`
	Durable      rootraft.PersistentState `json:"durable"`
	AppliedIndex uint64                   `json:"applied_index"`
	Applied      []rootraft.Entry         `json:"applied"`
	Chain        []Block                  `json:"chain"`
	Pending      *pendingSnapshot         `json:"pending,omitempty"`
	Mailbox      []inputSnapshot          `json:"mailbox,omitempty"`
}

type pendingSnapshot struct {
	Generation  uint64           `json:"generation"`
	Incarnation uint64           `json:"incarnation"`
	Persisted   bool             `json:"persisted"`
	MustSync    bool             `json:"must_sync"`
	Term        uint64           `json:"term"`
	Vote        rootraft.NodeID  `json:"vote,omitempty"`
	Commit      uint64           `json:"commit"`
	Entries     []rootraft.Entry `json:"entries,omitempty"`
	Committed   []rootraft.Entry `json:"committed,omitempty"`
	Messages    [][]byte         `json:"messages,omitempty"`
	Snapshot    []byte           `json:"snapshot,omitempty"`
}

type inputSnapshot struct {
	Kind        inputKind `json:"kind"`
	Incarnation uint64    `json:"incarnation"`
	Data        []byte    `json:"data,omitempty"`
	Message     []byte    `json:"message,omitempty"`
}

type observationSnapshot struct {
	Schema     string            `json:"schema"`
	Adapter    string            `json:"adapter"`
	Version    string            `json:"version"`
	AtNS       int64             `json:"at_ns"`
	Nodes      []nodeSnapshot    `json:"nodes"`
	Violations []check.Violation `json:"violations,omitempty"`
}

func normalizeEntry(source *pb.Entry) rootraft.Entry {
	entry := rootraft.Entry{Index: source.GetIndex(), Term: source.GetTerm(), Type: rootraft.EntryNoop}
	if source.GetType() == pb.EntryNormal && len(source.GetData()) > 0 {
		entry.Type = rootraft.EntryCommand
		entry.Data = slices.Clone(source.GetData())
	}
	return entry
}

func (c *Cluster) durableState(process *process) (rootraft.PersistentState, error) {
	hard, _, err := process.storage.InitialState()
	if err != nil {
		return rootraft.PersistentState{}, err
	}
	snapshot, err := process.storage.Snapshot()
	if err != nil {
		return rootraft.PersistentState{}, err
	}
	state := rootraft.PersistentState{
		HardState: rootraft.HardState{CurrentTerm: hard.GetTerm(), CommitIndex: hard.GetCommit()},
		Snapshot: rootraft.Snapshot{
			LastIncludedIndex: snapshot.GetMetadata().GetIndex(), LastIncludedTerm: snapshot.GetMetadata().GetTerm(),
			Members: slices.Clone(c.members), Data: slices.Clone(snapshot.GetData()),
			Membership: rootraft.StableMembership(c.members, nil),
		},
	}
	if hard.GetVote() != 0 {
		votedFor, ok := c.names[hard.GetVote()]
		if !ok {
			return rootraft.PersistentState{}, fmt.Errorf("%w: durable vote %d", ErrUnknownNode, hard.GetVote())
		}
		state.HardState.VotedFor = votedFor
	}
	first, err := process.storage.FirstIndex()
	if err != nil {
		return rootraft.PersistentState{}, err
	}
	last, err := process.storage.LastIndex()
	if err != nil {
		return rootraft.PersistentState{}, err
	}
	if first <= last {
		entries, entriesErr := process.storage.Entries(first, last+1, math.MaxUint64)
		if entriesErr != nil {
			return rootraft.PersistentState{}, entriesErr
		}
		state.Log = make([]rootraft.Entry, len(entries))
		for index, entry := range entries {
			state.Log[index] = normalizeEntry(entry)
		}
	}
	return state, nil
}

func (c *Cluster) status(process *process, durable rootraft.PersistentState) (*rootraft.Status, error) {
	if !process.up {
		return nil, nil
	}
	basic := process.raw.BasicStatus()
	role, err := normalizeRole(basic.RaftState)
	if err != nil {
		return nil, err
	}
	status := &rootraft.Status{
		ID: process.name, Role: role, Term: basic.GetTerm(), CommitIndex: basic.GetCommit(), AppliedIndex: process.appliedIndex,
		Snapshot: rootraft.CloneSnapshot(durable.Snapshot), Membership: rootraft.StableMembership(c.members, nil),
		ElectionMembership: rootraft.StableMembership(c.members, nil), AwaitingPersistence: process.pending != nil,
		Log: cloneEntries(durable.Log),
	}
	if basic.GetVote() != 0 {
		vote, ok := c.names[basic.GetVote()]
		if !ok {
			return nil, fmt.Errorf("%w: volatile vote %d", ErrUnknownNode, basic.GetVote())
		}
		status.VotedFor = vote
	}
	if basic.Lead != 0 {
		leader, ok := c.names[basic.Lead]
		if !ok {
			return nil, fmt.Errorf("%w: volatile leader %d", ErrUnknownNode, basic.Lead)
		}
		status.Leader = leader
	}
	status.LastLogIndex = durable.Snapshot.LastIncludedIndex + uint64(len(durable.Log))
	status.LastLogTerm = durable.Snapshot.LastIncludedTerm
	if len(durable.Log) > 0 {
		status.LastLogTerm = durable.Log[len(durable.Log)-1].Term
	}
	return status, nil
}

func normalizeRole(role etcdraft.StateType) (rootraft.Role, error) {
	switch role {
	case etcdraft.StateFollower:
		return rootraft.Follower, nil
	case etcdraft.StateCandidate, etcdraft.StatePreCandidate:
		return rootraft.Candidate, nil
	case etcdraft.StateLeader:
		return rootraft.Leader, nil
	default:
		return 0, fmt.Errorf("etcdraft: unknown role %d", role)
	}
}

func cloneEntries(source []rootraft.Entry) []rootraft.Entry {
	result := make([]rootraft.Entry, len(source))
	for index, entry := range source {
		result[index] = rootraft.CloneEntry(entry)
	}
	return result
}

func (c *Cluster) snapshotPending(process *process) (*pendingSnapshot, error) {
	if process.pending == nil {
		return nil, nil
	}
	ready := process.pending.ready
	snapshot := &pendingSnapshot{
		Generation: process.pending.generation, Incarnation: process.pending.incarnation,
		Persisted: process.pending.persisted, MustSync: ready.MustSync,
		Term: ready.HardState.GetTerm(), Commit: ready.HardState.GetCommit(),
		Entries: make([]rootraft.Entry, len(ready.Entries)), Committed: make([]rootraft.Entry, len(ready.CommittedEntries)),
	}
	if ready.HardState.GetVote() != 0 {
		vote, ok := c.names[ready.HardState.GetVote()]
		if !ok {
			return nil, fmt.Errorf("%w: pending vote %d", ErrUnknownNode, ready.HardState.GetVote())
		}
		snapshot.Vote = vote
	}
	for index, entry := range ready.Entries {
		snapshot.Entries[index] = normalizeEntry(entry)
	}
	for index, entry := range ready.CommittedEntries {
		snapshot.Committed[index] = normalizeEntry(entry)
	}
	for _, message := range ready.Messages {
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
		if err != nil {
			return nil, err
		}
		snapshot.Messages = append(snapshot.Messages, wire)
	}
	if !etcdraft.IsEmptySnap(ready.Snapshot) {
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(ready.Snapshot)
		if err != nil {
			return nil, err
		}
		snapshot.Snapshot = wire
	}
	return snapshot, nil
}

func snapshotMailbox(process *process) ([]inputSnapshot, error) {
	result := make([]inputSnapshot, len(process.mailbox))
	for index, input := range process.mailbox {
		result[index] = inputSnapshot{Kind: input.kind, Incarnation: input.incarnation, Data: slices.Clone(input.data)}
		if input.message != nil {
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(input.message)
			if err != nil {
				return nil, err
			}
			result[index].Message = wire
		}
	}
	return result, nil
}

func (c *Cluster) observation() (check.Observation, error) {
	observation := check.Observation{At: c.simulator.Now(), Members: slices.Clone(c.members)}
	for _, name := range c.members {
		process := c.processes[name]
		durable, err := c.durableState(process)
		if err != nil {
			return check.Observation{}, err
		}
		status, err := c.status(process, durable)
		if err != nil {
			return check.Observation{}, err
		}
		if err := verifyChain(process); err != nil {
			return check.Observation{}, err
		}
		observation.Nodes = append(observation.Nodes, check.NodeObservation{
			ID: name, Up: process.up, Status: status, Durable: durable,
			AppliedIndex: process.appliedIndex, Applied: cloneEntries(process.applied),
		})
	}
	return observation, nil
}

func verifyChain(process *process) error {
	var rebuilt Chain
	if err := rebuilt.Apply(rootraft.Entry{Index: 1, Term: 1, Type: rootraft.EntryNoop}); err != nil {
		return err
	}
	for _, entry := range process.applied {
		if err := rebuilt.Apply(entry); err != nil {
			return err
		}
	}
	if rebuilt.Index() != process.appliedIndex || rebuilt.Digest() != process.chain.Digest() {
		return errors.New("etcdraft: chain-of-blocks commitment mismatch")
	}
	return nil
}

func (c *Cluster) observe() {
	observation, err := c.observation()
	if err != nil {
		c.fail(err)
		return
	}
	checkerObservation := observation
	checkerObservation.Nodes = slices.Clone(observation.Nodes)
	for index := range checkerObservation.Nodes {
		// Public RawNode state is insufficient to reconstruct election
		// certificates or the complete volatile log. The v1 production-core
		// profile therefore enables only the common durable/application checks.
		checkerObservation.Nodes[index].Status = nil
	}
	newViolations := c.checker.Observe(checkerObservation)
	for _, violation := range newViolations {
		violation.Nodes = slices.Clone(violation.Nodes)
		violation.Evidence = slices.Clone(violation.Evidence)
		c.violations = append(c.violations, violation)
	}
	if c.config.StopOnViolation && len(newViolations) > 0 {
		c.fail(newViolations[0])
	}
}

func (c *Cluster) SnapshotDigest() (string, error) {
	snapshot := observationSnapshot{Schema: ObservationSchemaVersion, Adapter: AdapterID, Version: AdapterVersion, AtNS: int64(c.simulator.Now()), Violations: c.Violations()}
	for _, name := range c.members {
		process := c.processes[name]
		durable, err := c.durableState(process)
		if err != nil {
			return "", err
		}
		status, err := c.status(process, durable)
		if err != nil {
			return "", err
		}
		pending, err := c.snapshotPending(process)
		if err != nil {
			return "", err
		}
		mailbox, err := snapshotMailbox(process)
		if err != nil {
			return "", err
		}
		snapshot.Nodes = append(snapshot.Nodes, nodeSnapshot{
			ID: name, Up: process.up, Status: status, Durable: durable, AppliedIndex: process.appliedIndex,
			Applied: cloneEntries(process.applied), Chain: process.chain.Blocks(), Pending: pending, Mailbox: mailbox,
		})
	}
	return artifact.DigestJSON(snapshot)
}

// Observation returns a deep adapter-normalized view. This is broader than
// CheckerProfile: the checker deliberately excludes incomplete volatile
// status evidence, while callers may inspect it with the documented caveats.
func (c *Cluster) Observation() (check.Observation, error) { return c.observation() }

// DurableState returns the normalized durable snapshot and log for one node.
func (c *Cluster) DurableState(name rootraft.NodeID) (rootraft.PersistentState, error) {
	process, ok := c.processes[name]
	if !ok {
		return rootraft.PersistentState{}, fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	return c.durableState(process)
}

// Status returns the normalized live public RawNode status for one up node.
func (c *Cluster) Status(name rootraft.NodeID) (rootraft.Status, error) {
	process, ok := c.processes[name]
	if !ok {
		return rootraft.Status{}, fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	if !process.up {
		return rootraft.Status{}, fmt.Errorf("%w: %q", ErrNodeDown, name)
	}
	durable, err := c.durableState(process)
	if err != nil {
		return rootraft.Status{}, err
	}
	status, err := c.status(process, durable)
	if err != nil {
		return rootraft.Status{}, err
	}
	return *status, nil
}

// AppliedEntries and ChainBlocks return independent application-history views.
func (c *Cluster) AppliedEntries(name rootraft.NodeID) ([]rootraft.Entry, error) {
	process, ok := c.processes[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	return cloneEntries(process.applied), nil
}

func (c *Cluster) ChainBlocks(name rootraft.NodeID) ([]Block, error) {
	process, ok := c.processes[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	return process.chain.Blocks(), nil
}
