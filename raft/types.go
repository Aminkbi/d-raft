// Package raft implements d-raft's deterministic reference Raft state
// machine. It performs no I/O and owns no clocks or random sources.
package raft

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

var (
	ErrInvalidConfig         = errors.New("raft: invalid configuration")
	ErrInvalidState          = errors.New("raft: invalid persistent state")
	ErrInvalidInput          = errors.New("raft: invalid input")
	ErrNotLeader             = errors.New("raft: proposal requires a leader")
	ErrAwaitingPersistence   = errors.New("raft: awaiting persistence acknowledgement")
	ErrUnexpectedPersistence = errors.New("raft: unexpected persistence acknowledgement")
	ErrTermExhausted         = errors.New("raft: term space exhausted")
	ErrIndexExhausted        = errors.New("raft: log index space exhausted")
)

// NodeID identifies a Raft member.
type NodeID string

// Role is a node's volatile Raft role.
type Role uint8

const (
	Follower Role = iota + 1
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// EntryType identifies a replicated log entry.
type EntryType uint8

const (
	EntryNoop EntryType = iota + 1
	EntryCommand
	EntryConfiguration
)

// Entry is one durable Raft log entry.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState is the durable scalar Raft state.
type HardState struct {
	CurrentTerm uint64
	VotedFor    NodeID
	CommitIndex uint64
}

// Snapshot is an atomic application and log-prefix checkpoint. Members is the
// configuration in force at LastIncludedIndex.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Members           []NodeID
	Data              []byte
}

// PersistentState is the atomically persisted reference-model state. The
// current implementation uses a full log image to make persistence ordering
// explicit; production adapters may persist equivalent incremental updates.
type PersistentState struct {
	HardState HardState
	Snapshot  Snapshot
	Log       []Entry
}

// Config describes a fixed-membership reference node. Members are sorted by
// NodeID internally so message emission order does not depend on caller order.
type Config struct {
	ID           NodeID
	Members      []NodeID
	AppliedIndex uint64
}

func (c Config) validate(state PersistentState) error {
	if c.ID == "" || len(c.Members) == 0 {
		return fmt.Errorf("%w: empty node or membership", ErrInvalidConfig)
	}
	members := slices.Clone(c.Members)
	slices.Sort(members)
	if slices.Contains(members, NodeID("")) {
		return fmt.Errorf("%w: empty member", ErrInvalidConfig)
	}
	for index := 1; index < len(members); index++ {
		if members[index] == members[index-1] {
			return fmt.Errorf("%w: duplicate member %q", ErrInvalidConfig, members[index])
		}
	}
	if !slices.Contains(members, c.ID) {
		return fmt.Errorf("%w: local node %q is not a member", ErrInvalidConfig, c.ID)
	}
	if state.Snapshot.LastIncludedIndex == 0 {
		if state.Snapshot.LastIncludedTerm != 0 || len(state.Snapshot.Members) != 0 || len(state.Snapshot.Data) != 0 {
			return fmt.Errorf("%w: zero snapshot carries metadata", ErrInvalidState)
		}
	} else if state.Snapshot.LastIncludedTerm == 0 || state.Snapshot.LastIncludedTerm > state.HardState.CurrentTerm || !slices.Equal(state.Snapshot.Members, members) {
		return fmt.Errorf("%w: invalid snapshot boundary or membership", ErrInvalidState)
	}
	if math.MaxUint64-state.Snapshot.LastIncludedIndex < uint64(len(state.Log)) {
		return fmt.Errorf("%w: log index overflow", ErrInvalidState)
	}
	previousTerm := state.Snapshot.LastIncludedTerm
	for index, entry := range state.Log {
		want := state.Snapshot.LastIncludedIndex + uint64(index) + 1
		if entry.Index != want || entry.Term == 0 || entry.Term > state.HardState.CurrentTerm || entry.Term < previousTerm || entry.Type < EntryNoop || entry.Type > EntryCommand {
			return fmt.Errorf("%w: log entry %d has index=%d term=%d type=%d", ErrInvalidState, index, entry.Index, entry.Term, entry.Type)
		}
		previousTerm = entry.Term
	}
	lastIndex := state.Snapshot.LastIncludedIndex + uint64(len(state.Log))
	if state.HardState.CommitIndex < state.Snapshot.LastIncludedIndex || state.HardState.CommitIndex > lastIndex || c.AppliedIndex > state.HardState.CommitIndex {
		return fmt.Errorf("%w: last=%d commit=%d applied=%d", ErrInvalidState, lastIndex, state.HardState.CommitIndex, c.AppliedIndex)
	}
	return nil
}

// MessageType identifies a Raft RPC.
type MessageType uint8

const (
	RequestVote MessageType = iota + 1
	RequestVoteResponse
	AppendEntries
	AppendEntriesResponse
	InstallSnapshot
	InstallSnapshotResponse
)

func (t MessageType) String() string {
	switch t {
	case RequestVote:
		return "request_vote"
	case RequestVoteResponse:
		return "request_vote_response"
	case AppendEntries:
		return "append_entries"
	case AppendEntriesResponse:
		return "append_entries_response"
	case InstallSnapshot:
		return "install_snapshot"
	case InstallSnapshotResponse:
		return "install_snapshot_response"
	default:
		return "unknown"
	}
}

// Message contains the union of fields used by the reference Raft RPCs.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64
	// Sequence identifies an AppendEntries attempt from one leader to one
	// follower. It lets leaders ignore stale rejection responses.
	Sequence uint64

	LastLogIndex uint64
	LastLogTerm  uint64
	VoteGranted  bool

	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
	Success      bool
	MatchIndex   uint64
	RejectHint   uint64
	Snapshot     Snapshot
}

// InputKind identifies an external stimulus accepted by Node.Step.
type InputKind uint8

const (
	InputElectionTimeout InputKind = iota + 1
	InputHeartbeatTimeout
	InputMessage
	InputProposal
	InputPersisted
	InputSnapshot
)

// Input is one deterministic state-machine stimulus.
type Input struct {
	Kind          InputKind
	Message       Message
	Data          []byte
	WriteToken    uint64
	SnapshotIndex uint64
	SnapshotData  []byte
}

// EffectKind identifies an ordered action requested by a Node. EffectPersist,
// when present, is always the first effect so a harness cannot send messages
// based on state that has not been made durable.
type EffectKind uint8

const (
	EffectPersist EffectKind = iota + 1
	EffectSend
	EffectResetElectionTimer
	EffectResetHeartbeatTimer
	EffectApply
	EffectInstallSnapshot
)

// Effect is one ordered action produced by Node.Step or Node.Start.
type Effect struct {
	Kind       EffectKind
	WriteToken uint64
	State      PersistentState
	Message    Message
	Entry      Entry
	Snapshot   Snapshot
}

// Status is a deterministic, read-only node snapshot for tracing, invariant
// checks, and state hashing.
type Status struct {
	ID                  NodeID
	Role                Role
	Term                uint64
	VotedFor            NodeID
	Leader              NodeID
	CommitIndex         uint64
	AppliedIndex        uint64
	LastLogIndex        uint64
	LastLogTerm         uint64
	Snapshot            Snapshot
	Log                 []Entry
	ElectionVotes       []NodeID
	AwaitingPersistence bool
	WriteToken          uint64
}

func cloneEntry(entry Entry) Entry {
	entry.Data = slices.Clone(entry.Data)
	return entry
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Members = slices.Clone(snapshot.Members)
	snapshot.Data = slices.Clone(snapshot.Data)
	return snapshot
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	for index, entry := range entries {
		result[index] = cloneEntry(entry)
	}
	return result
}

// CloneMessage returns a deep copy suitable for a simulation router snapshot
// function.
func CloneMessage(message Message) Message {
	message.Entries = cloneEntries(message.Entries)
	message.Snapshot = cloneSnapshot(message.Snapshot)
	return message
}

// CloneEntry returns a deep copy of entry.
func CloneEntry(entry Entry) Entry {
	return cloneEntry(entry)
}

// ClonePersistentState returns a deep copy of state.
func ClonePersistentState(state PersistentState) PersistentState {
	return cloneState(state)
}

// CloneSnapshot returns a deep copy.
func CloneSnapshot(snapshot Snapshot) Snapshot { return cloneSnapshot(snapshot) }

func cloneState(state PersistentState) PersistentState {
	state.Snapshot = cloneSnapshot(state.Snapshot)
	state.Log = cloneEntries(state.Log)
	return state
}
