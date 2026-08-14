// Package raft implements d-raft's deterministic reference Raft state
// machine. It performs no I/O and owns no clocks or random sources.
package raft

import (
	"errors"
	"fmt"
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

// PersistentState is the atomically persisted reference-model state. The
// current implementation uses a full log image to make persistence ordering
// explicit; production adapters may persist equivalent incremental updates.
type PersistentState struct {
	HardState HardState
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
	for index, entry := range state.Log {
		want := uint64(index + 1)
		if entry.Index != want || entry.Term == 0 || entry.Type == 0 {
			return fmt.Errorf("%w: log entry %d has index=%d term=%d type=%d", ErrInvalidState, index, entry.Index, entry.Term, entry.Type)
		}
	}
	lastIndex := uint64(len(state.Log))
	if state.HardState.CommitIndex > lastIndex || c.AppliedIndex > state.HardState.CommitIndex {
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
}

// InputKind identifies an external stimulus accepted by Node.Step.
type InputKind uint8

const (
	InputElectionTimeout InputKind = iota + 1
	InputHeartbeatTimeout
	InputMessage
	InputProposal
	InputPersisted
)

// Input is one deterministic state-machine stimulus.
type Input struct {
	Kind       InputKind
	Message    Message
	Data       []byte
	WriteToken uint64
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
)

// Effect is one ordered action produced by Node.Step or Node.Start.
type Effect struct {
	Kind       EffectKind
	WriteToken uint64
	State      PersistentState
	Message    Message
	Entry      Entry
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
	Log                 []Entry
	ElectionVotes       []NodeID
	AwaitingPersistence bool
	WriteToken          uint64
}

func cloneEntry(entry Entry) Entry {
	entry.Data = slices.Clone(entry.Data)
	return entry
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

func cloneState(state PersistentState) PersistentState {
	state.Log = cloneEntries(state.Log)
	return state
}
