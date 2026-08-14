package raft

import (
	"fmt"
	"math"
	"slices"
)

// Node is a deterministic, pure Raft state machine. It is not safe for
// concurrent use. All I/O requested by a transition is returned as Effects.
type Node struct {
	id      NodeID
	members []NodeID
	role    Role
	leader  NodeID
	state   PersistentState
	applied uint64

	votes          map[NodeID]struct{}
	electionVotes  []NodeID
	nextIndex      map[NodeID]uint64
	matchIndex     map[NodeID]uint64
	appendSequence map[NodeID]uint64
	nextWriteToken uint64
	pending        *pendingWrite
}

type pendingWrite struct {
	token   uint64
	effects []Effect
}

// New constructs a node from durable state.
func New(config Config, state PersistentState) (*Node, error) {
	state = cloneState(state)
	if err := config.validate(state); err != nil {
		return nil, err
	}
	members := slices.Clone(config.Members)
	slices.Sort(members)
	return &Node{
		id:             config.ID,
		members:        members,
		role:           Follower,
		state:          state,
		applied:        config.AppliedIndex,
		nextWriteToken: 1,
	}, nil
}

// Start returns the effects needed after construction or restart. Committed
// but not durably applied entries are re-emitted before the election timer is
// armed.
func (n *Node) Start() []Effect {
	effects := n.applyCommitted()
	effects = append(effects, Effect{Kind: EffectResetElectionTimer})
	return effects
}

// Step applies one input and returns ordered effects.
func (n *Node) Step(input Input) ([]Effect, error) {
	if input.Kind == InputPersisted {
		return n.acknowledgePersistence(input.WriteToken)
	}
	if n.pending != nil {
		return nil, ErrAwaitingPersistence
	}

	dirty := false
	var effects []Effect
	var err error

	switch input.Kind {
	case InputElectionTimeout:
		if n.state.HardState.CurrentTerm == math.MaxUint64 {
			err = ErrTermExhausted
		} else {
			dirty, effects = n.startElection()
		}
	case InputHeartbeatTimeout:
		if n.role == Leader {
			effects = append(effects, Effect{Kind: EffectResetHeartbeatTimer})
			effects = append(effects, n.broadcastAppend()...)
		}
	case InputMessage:
		dirty, effects, err = n.stepMessage(input.Message)
	case InputProposal:
		if n.role != Leader {
			return nil, ErrNotLeader
		}
		entry := Entry{Index: n.lastIndex() + 1, Term: n.state.HardState.CurrentTerm, Type: EntryCommand, Data: slices.Clone(input.Data)}
		n.state.Log = append(n.state.Log, entry)
		n.matchIndex[n.id] = entry.Index
		dirty = true
		effects = append(effects, n.advanceCommit()...)
		effects = append(effects, n.broadcastAppend()...)
	default:
		err = fmt.Errorf("%w: kind %d", ErrInvalidInput, input.Kind)
	}
	if err != nil {
		return nil, err
	}
	if dirty {
		token := n.nextWriteToken
		n.nextWriteToken++
		n.pending = &pendingWrite{token: token, effects: effects}
		return []Effect{{Kind: EffectPersist, WriteToken: token, State: cloneState(n.state)}}, nil
	}
	return effects, nil
}

// Status returns a deep copy of the node's observable state.
func (n *Node) Status() Status {
	status := Status{
		ID:                  n.id,
		Role:                n.role,
		Term:                n.state.HardState.CurrentTerm,
		VotedFor:            n.state.HardState.VotedFor,
		Leader:              n.leader,
		CommitIndex:         n.state.HardState.CommitIndex,
		AppliedIndex:        n.applied,
		LastLogIndex:        n.lastIndex(),
		LastLogTerm:         n.lastTerm(),
		Log:                 cloneEntries(n.state.Log),
		ElectionVotes:       slices.Clone(n.electionVotes),
		AwaitingPersistence: n.pending != nil,
	}
	if n.pending != nil {
		status.WriteToken = n.pending.token
	}
	return status
}

func (n *Node) acknowledgePersistence(token uint64) ([]Effect, error) {
	if n.pending == nil || token == 0 || token != n.pending.token {
		return nil, fmt.Errorf("%w: token %d", ErrUnexpectedPersistence, token)
	}
	effects := n.pending.effects
	n.pending = nil
	return effects, nil
}

func (n *Node) startElection() (bool, []Effect) {
	n.role = Candidate
	n.leader = ""
	n.state.HardState.CurrentTerm++
	n.state.HardState.VotedFor = n.id
	n.votes = map[NodeID]struct{}{n.id: {}}
	n.electionVotes = nil
	n.nextIndex = nil
	n.matchIndex = nil
	n.appendSequence = nil
	effects := []Effect{{Kind: EffectResetElectionTimer}}
	if n.hasQuorum(1) {
		effects = append(effects, n.becomeLeader()...)
		return true, effects
	}
	for _, member := range n.members {
		if member == n.id {
			continue
		}
		effects = append(effects, Effect{Kind: EffectSend, Message: Message{
			Type:         RequestVote,
			From:         n.id,
			To:           member,
			Term:         n.state.HardState.CurrentTerm,
			LastLogIndex: n.lastIndex(),
			LastLogTerm:  n.lastTerm(),
		}})
	}
	return true, effects
}

func (n *Node) stepMessage(message Message) (bool, []Effect, error) {
	if message.To != n.id || message.From == "" || message.From == n.id || !slices.Contains(n.members, message.From) {
		return false, nil, fmt.Errorf("%w: invalid route %q -> %q", ErrInvalidInput, message.From, message.To)
	}
	if message.Type < RequestVote || message.Type > AppendEntriesResponse {
		return false, nil, fmt.Errorf("%w: message type %d", ErrInvalidInput, message.Type)
	}

	dirty := false
	if message.Term > n.state.HardState.CurrentTerm {
		n.becomeFollower(message.Term, "")
		dirty = true
	}

	switch message.Type {
	case RequestVote:
		changed, effects := n.handleRequestVote(message)
		return dirty || changed, effects, nil
	case RequestVoteResponse:
		changed, effects := n.handleRequestVoteResponse(message)
		return dirty || changed, effects, nil
	case AppendEntries:
		changed, effects := n.handleAppendEntries(message)
		return dirty || changed, effects, nil
	case AppendEntriesResponse:
		changed, effects := n.handleAppendEntriesResponse(message)
		return dirty || changed, effects, nil
	default:
		return dirty, nil, nil
	}
}

func (n *Node) handleRequestVote(message Message) (bool, []Effect) {
	granted := false
	dirty := false
	if message.Term == n.state.HardState.CurrentTerm &&
		(n.state.HardState.VotedFor == "" || n.state.HardState.VotedFor == message.From) &&
		n.candidateLogIsUpToDate(message.LastLogIndex, message.LastLogTerm) {
		if n.state.HardState.VotedFor != message.From {
			n.state.HardState.VotedFor = message.From
			dirty = true
		}
		granted = true
	}
	effects := []Effect{{Kind: EffectSend, Message: Message{
		Type:        RequestVoteResponse,
		From:        n.id,
		To:          message.From,
		Term:        n.state.HardState.CurrentTerm,
		VoteGranted: granted,
	}}}
	if granted {
		effects = append([]Effect{{Kind: EffectResetElectionTimer}}, effects...)
	}
	return dirty, effects
}

func (n *Node) handleRequestVoteResponse(message Message) (bool, []Effect) {
	if n.role != Candidate || message.Term != n.state.HardState.CurrentTerm || !message.VoteGranted {
		return false, nil
	}
	if _, exists := n.votes[message.From]; exists {
		return false, nil
	}
	n.votes[message.From] = struct{}{}
	if !n.hasQuorum(len(n.votes)) {
		return false, nil
	}
	return true, n.becomeLeader()
}

func (n *Node) becomeLeader() []Effect {
	n.role = Leader
	n.leader = n.id
	n.electionVotes = make([]NodeID, 0, len(n.votes))
	for _, member := range n.members {
		if _, voted := n.votes[member]; voted {
			n.electionVotes = append(n.electionVotes, member)
		}
	}
	n.nextIndex = make(map[NodeID]uint64, len(n.members))
	n.matchIndex = make(map[NodeID]uint64, len(n.members))
	n.appendSequence = make(map[NodeID]uint64, len(n.members))
	last := n.lastIndex()
	for _, member := range n.members {
		n.nextIndex[member] = last + 1
		n.matchIndex[member] = 0
	}
	n.matchIndex[n.id] = last
	noop := Entry{Index: last + 1, Term: n.state.HardState.CurrentTerm, Type: EntryNoop}
	n.state.Log = append(n.state.Log, noop)
	n.matchIndex[n.id] = noop.Index
	effects := []Effect{{Kind: EffectResetHeartbeatTimer}}
	effects = append(effects, n.advanceCommit()...)
	effects = append(effects, n.broadcastAppend()...)
	return effects
}

func (n *Node) handleAppendEntries(message Message) (bool, []Effect) {
	if message.Term < n.state.HardState.CurrentTerm {
		return false, []Effect{{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, n.lastIndex()+1)}}
	}
	if n.role != Follower || n.leader != message.From {
		n.becomeFollower(message.Term, message.From)
	}
	effects := []Effect{{Kind: EffectResetElectionTimer}}
	if n.termAt(message.PrevLogIndex) != message.PrevLogTerm {
		rejectHint := min(message.PrevLogIndex, n.lastIndex()+1)
		if rejectHint == 0 {
			rejectHint = 1
		}
		effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, rejectHint)})
		return false, effects
	}

	dirty := false
	for offset, incoming := range message.Entries {
		index := message.PrevLogIndex + uint64(offset) + 1
		if incoming.Index != index || incoming.Term == 0 || incoming.Type == 0 {
			effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, index)})
			return false, effects
		}
		if index <= n.lastIndex() {
			if n.termAt(index) == incoming.Term {
				continue
			}
			n.state.Log = n.state.Log[:index-1]
		}
		if index > n.lastIndex() {
			n.state.Log = append(n.state.Log, cloneEntry(incoming))
			dirty = true
		}
	}
	if message.LeaderCommit > n.state.HardState.CommitIndex {
		n.state.HardState.CommitIndex = min(message.LeaderCommit, n.lastIndex())
		dirty = true
		effects = append(effects, n.applyCommitted()...)
	}
	match := message.PrevLogIndex + uint64(len(message.Entries))
	effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, true, match, 0)})
	return dirty, effects
}

func (n *Node) handleAppendEntriesResponse(message Message) (bool, []Effect) {
	if n.role != Leader || message.Term != n.state.HardState.CurrentTerm {
		return false, nil
	}
	if !message.Success {
		if message.Sequence != n.appendSequence[message.From] {
			return false, nil
		}
		next := n.nextIndex[message.From]
		if next > 1 {
			next--
		}
		if message.RejectHint > 0 && message.RejectHint < next {
			next = message.RejectHint
		}
		n.nextIndex[message.From] = max(uint64(1), next)
		return false, []Effect{{Kind: EffectSend, Message: n.appendFor(message.From)}}
	}
	if message.MatchIndex > n.matchIndex[message.From] {
		n.matchIndex[message.From] = min(message.MatchIndex, n.lastIndex())
		n.nextIndex[message.From] = n.matchIndex[message.From] + 1
	}
	effects := n.advanceCommit()
	dirty := len(effects) > 0
	if n.nextIndex[message.From] <= n.lastIndex() {
		effects = append(effects, Effect{Kind: EffectSend, Message: n.appendFor(message.From)})
	}
	return dirty, effects
}

func (n *Node) advanceCommit() []Effect {
	oldCommit := n.state.HardState.CommitIndex
	for index := n.lastIndex(); index > oldCommit; index-- {
		if n.termAt(index) != n.state.HardState.CurrentTerm {
			continue
		}
		matched := 0
		for _, member := range n.members {
			if n.matchIndex[member] >= index {
				matched++
			}
		}
		if n.hasQuorum(matched) {
			n.state.HardState.CommitIndex = index
			return n.applyCommitted()
		}
	}
	return nil
}

func (n *Node) applyCommitted() []Effect {
	var effects []Effect
	for n.applied < n.state.HardState.CommitIndex {
		n.applied++
		entry := n.state.Log[n.applied-1]
		effects = append(effects, Effect{Kind: EffectApply, Entry: cloneEntry(entry)})
	}
	return effects
}

func (n *Node) broadcastAppend() []Effect {
	effects := make([]Effect, 0, len(n.members)-1)
	for _, member := range n.members {
		if member != n.id {
			effects = append(effects, Effect{Kind: EffectSend, Message: n.appendFor(member)})
		}
	}
	return effects
}

func (n *Node) appendFor(member NodeID) Message {
	n.appendSequence[member]++
	next := n.nextIndex[member]
	if next == 0 {
		next = n.lastIndex() + 1
	}
	prev := next - 1
	var entries []Entry
	if next <= n.lastIndex() {
		entries = cloneEntries(n.state.Log[next-1:])
	}
	return Message{
		Type:         AppendEntries,
		From:         n.id,
		To:           member,
		Term:         n.state.HardState.CurrentTerm,
		Sequence:     n.appendSequence[member],
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      entries,
		LeaderCommit: n.state.HardState.CommitIndex,
	}
}

func (n *Node) appendResponse(to NodeID, sequence uint64, success bool, matchIndex, rejectHint uint64) Message {
	return Message{
		Type:       AppendEntriesResponse,
		From:       n.id,
		To:         to,
		Term:       n.state.HardState.CurrentTerm,
		Sequence:   sequence,
		Success:    success,
		MatchIndex: matchIndex,
		RejectHint: rejectHint,
	}
}

func (n *Node) becomeFollower(term uint64, leader NodeID) {
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.electionVotes = nil
	n.nextIndex = nil
	n.matchIndex = nil
	n.appendSequence = nil
	if term > n.state.HardState.CurrentTerm {
		n.state.HardState.CurrentTerm = term
		n.state.HardState.VotedFor = ""
	}
}

func (n *Node) candidateLogIsUpToDate(index, term uint64) bool {
	lastTerm := n.lastTerm()
	return term > lastTerm || term == lastTerm && index >= n.lastIndex()
}

func (n *Node) lastIndex() uint64 {
	return uint64(len(n.state.Log))
}

func (n *Node) lastTerm() uint64 {
	return n.termAt(n.lastIndex())
}

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 || index > uint64(len(n.state.Log)) {
		return 0
	}
	return n.state.Log[index-1].Term
}

func (n *Node) hasQuorum(count int) bool {
	return count >= len(n.members)/2+1
}
