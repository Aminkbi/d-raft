package raft

import (
	"fmt"
	"math"
	"slices"
)

const StateSchemaVersion = "d-raft.raft-node-state/v1"

// Node is a deterministic, pure Raft state machine. It is not safe for
// concurrent use. All I/O requested by a transition is returned as Effects.
type Node struct {
	id                NodeID
	members           []NodeID
	initialMembership Membership
	membership        Membership
	role              Role
	leader            NodeID
	state             PersistentState
	applied           uint64

	votes              map[NodeID]struct{}
	electionVotes      []NodeID
	electionMembership Membership
	nextIndex          map[NodeID]uint64
	matchIndex         map[NodeID]uint64
	appendSequence     map[NodeID]uint64
	nextWriteToken     uint64
	pending            *pendingWrite
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
	initial, _ := initialMembership(config, members)
	node := &Node{
		id:                config.ID,
		members:           members,
		initialMembership: initial,
		role:              Follower,
		state:             state,
		applied:           config.AppliedIndex,
		nextWriteToken:    1,
	}
	node.refreshMembership()
	return node, nil
}

// Start returns the effects needed after construction or restart. Committed
// but not durably applied entries are re-emitted before the election timer is
// armed.
func (n *Node) Start() []Effect {
	var effects []Effect
	if n.applied < n.state.Snapshot.LastIncludedIndex {
		n.applied = n.state.Snapshot.LastIncludedIndex
		effects = append(effects, Effect{Kind: EffectInstallSnapshot, Snapshot: cloneSnapshot(n.state.Snapshot)})
	}
	effects = append(effects, n.applyCommitted()...)
	if n.membership.isVoter(n.id) {
		effects = append(effects, Effect{Kind: EffectResetElectionTimer})
	}
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
		if !n.membership.isVoter(n.id) {
			return nil, nil
		}
		if n.state.HardState.CurrentTerm == math.MaxUint64 {
			err = ErrTermExhausted
		} else {
			dirty, effects, err = n.startElection()
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
		if !n.membership.isVoter(n.id) {
			return nil, ErrNotVoter
		}
		if n.hasUncommittedConfiguration() {
			return nil, ErrMembershipInProgress
		}
		if n.lastIndex() == math.MaxUint64 {
			return nil, ErrIndexExhausted
		}
		entry := Entry{Index: n.lastIndex() + 1, Term: n.state.HardState.CurrentTerm, Type: EntryCommand, Data: slices.Clone(input.Data)}
		n.state.Log = append(n.state.Log, entry)
		n.matchIndex[n.id] = entry.Index
		dirty = true
		effects = append(effects, n.advanceCommit()...)
		effects = append(effects, n.broadcastAppend()...)
	case InputSnapshot:
		index := input.SnapshotIndex
		if index == 0 || index <= n.state.Snapshot.LastIncludedIndex || index > n.applied || index > n.state.HardState.CommitIndex {
			return nil, fmt.Errorf("%w: snapshot index %d", ErrInvalidInput, index)
		}
		term := n.termAt(index)
		if term == 0 {
			return nil, fmt.Errorf("%w: snapshot boundary %d has no term", ErrInvalidInput, index)
		}
		var suffix []Entry
		if index < math.MaxUint64 {
			suffix = n.entriesFrom(index + 1)
		}
		n.state.Snapshot = Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Members: slices.Clone(n.members), Data: slices.Clone(input.SnapshotData), Membership: CloneMembership(n.membershipAt(index))}
		n.state.Log = suffix
		n.refreshMembership()
		dirty = true
	case InputBeginMembership:
		dirty, effects, err = n.beginMembership(input.Voters, input.Learners)
	case InputFinalizeMembership:
		dirty, effects, err = n.finalizeMembership()
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
		Snapshot:            cloneSnapshot(n.state.Snapshot),
		Membership:          CloneMembership(n.membership),
		Log:                 cloneEntries(n.state.Log),
		ElectionVotes:       slices.Clone(n.electionVotes),
		ElectionMembership:  CloneMembership(n.electionMembership),
		AwaitingPersistence: n.pending != nil,
	}
	if n.pending != nil {
		status.WriteToken = n.pending.token
	}
	return status
}

// State returns a complete canonical deep copy of the node state. Map-backed
// fields are represented as slices sorted by node ID.
func (n *Node) State() NodeState {
	state := NodeState{
		Schema:             StateSchemaVersion,
		ID:                 n.id,
		Members:            slices.Clone(n.members),
		InitialMembership:  CloneMembership(n.initialMembership),
		Membership:         CloneMembership(n.membership),
		Role:               n.role,
		Leader:             n.leader,
		Persistent:         cloneState(n.state),
		Applied:            n.applied,
		ElectionVotes:      slices.Clone(n.electionVotes),
		ElectionMembership: CloneMembership(n.electionMembership),
		NextWriteToken:     n.nextWriteToken,
	}
	state.Votes = sortedNodeSet(n.votes)
	state.NextIndex = sortedNodeIndexes(n.nextIndex)
	state.MatchIndex = sortedNodeIndexes(n.matchIndex)
	state.AppendSequence = sortedNodeIndexes(n.appendSequence)
	if n.pending != nil {
		state.Pending = &PendingWriteState{Token: n.pending.token, Effects: cloneEffects(n.pending.effects)}
	}
	return state
}

func sortedNodeSet(values map[NodeID]struct{}) []NodeID {
	result := make([]NodeID, 0, len(values))
	for node := range values {
		result = append(result, node)
	}
	slices.Sort(result)
	return result
}

func sortedNodeIndexes(values map[NodeID]uint64) []NodeIndex {
	result := make([]NodeIndex, 0, len(values))
	for node, value := range values {
		result = append(result, NodeIndex{Node: node, Value: value})
	}
	slices.SortFunc(result, func(left, right NodeIndex) int {
		if left.Node < right.Node {
			return -1
		}
		if left.Node > right.Node {
			return 1
		}
		return 0
	})
	return result
}

func (n *Node) acknowledgePersistence(token uint64) ([]Effect, error) {
	if n.pending == nil || token == 0 || token != n.pending.token {
		return nil, fmt.Errorf("%w: token %d", ErrUnexpectedPersistence, token)
	}
	effects := n.pending.effects
	n.pending = nil
	return effects, nil
}

func (n *Node) beginMembership(voters, learners []NodeID) (bool, []Effect, error) {
	if n.role != Leader {
		return false, nil, ErrNotLeader
	}
	if !n.membership.isVoter(n.id) {
		return false, nil, ErrNotVoter
	}
	if n.membership.Joint() || n.hasUncommittedConfiguration() {
		return false, nil, ErrMembershipInProgress
	}
	if n.lastIndex() == math.MaxUint64 {
		return false, nil, ErrIndexExhausted
	}
	target := stableMembership(voters, learners)
	if !validateMembership(target, n.members) || membershipsEqual(target, n.membership) {
		return false, nil, fmt.Errorf("%w: invalid or unchanged target membership", ErrInvalidInput)
	}
	jointLearners := make([]NodeID, 0, len(target.Learners))
	for _, learner := range target.Learners {
		if !slices.Contains(n.membership.Voters, learner) {
			jointLearners = append(jointLearners, learner)
		}
	}
	joint := Membership{
		Voters:         slices.Clone(target.Voters),
		VotersOutgoing: slices.Clone(n.membership.Voters),
		Learners:       jointLearners,
		LearnersNext:   slices.Clone(target.Learners),
	}
	return n.appendMembershipEntry(EntryConfigJoint, joint)
}

func (n *Node) finalizeMembership() (bool, []Effect, error) {
	if n.role != Leader {
		return false, nil, ErrNotLeader
	}
	if !n.membership.Joint() || n.hasUncommittedConfiguration() || n.state.HardState.CommitIndex != n.lastIndex() {
		return false, nil, ErrMembershipInProgress
	}
	if n.lastIndex() == math.MaxUint64 {
		return false, nil, ErrIndexExhausted
	}
	final := stableMembership(n.membership.Voters, n.membership.LearnersNext)
	return n.appendMembershipEntry(EntryConfigFinal, final)
}

func (n *Node) appendMembershipEntry(entryType EntryType, membership Membership) (bool, []Effect, error) {
	entry := Entry{Index: n.lastIndex() + 1, Term: n.state.HardState.CurrentTerm, Type: entryType, Membership: CloneMembership(membership)}
	if _, ok := transitionMembership(n.membership, entry, n.members); !ok {
		return false, nil, fmt.Errorf("%w: invalid membership transition", ErrInvalidInput)
	}
	n.state.Log = append(n.state.Log, entry)
	n.refreshMembership()
	n.matchIndex[n.id] = entry.Index
	effects := n.advanceCommit()
	if n.role == Leader {
		effects = append(effects, n.broadcastAppend()...)
	}
	return true, effects, nil
}

func (n *Node) startElection() (bool, []Effect, error) {
	if !n.membership.isVoter(n.id) {
		return false, nil, ErrNotVoter
	}
	if n.membership.quorum(func(id NodeID) bool { return id == n.id }) && n.lastIndex() == math.MaxUint64 {
		return false, nil, ErrIndexExhausted
	}
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
	if n.membership.quorum(func(id NodeID) bool { _, ok := n.votes[id]; return ok }) {
		leaderEffects, err := n.becomeLeader()
		if err != nil {
			return false, nil, err
		}
		effects = append(effects, leaderEffects...)
		return true, effects, nil
	}
	for _, member := range voterUnion(n.membership) {
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
	return true, effects, nil
}

func (n *Node) stepMessage(message Message) (bool, []Effect, error) {
	if message.To != n.id || message.From == "" || message.From == n.id || !slices.Contains(n.members, message.From) {
		return false, nil, fmt.Errorf("%w: invalid route %q -> %q", ErrInvalidInput, message.From, message.To)
	}
	if message.Type < RequestVote || message.Type > InstallSnapshotResponse {
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
		changed, effects, err := n.handleRequestVoteResponse(message)
		return dirty || changed, effects, err
	case AppendEntries:
		changed, effects := n.handleAppendEntries(message)
		return dirty || changed, effects, nil
	case AppendEntriesResponse:
		changed, effects := n.handleAppendEntriesResponse(message)
		return dirty || changed, effects, nil
	case InstallSnapshot:
		changed, effects := n.handleInstallSnapshot(message)
		return dirty || changed, effects, nil
	case InstallSnapshotResponse:
		changed, effects := n.handleInstallSnapshotResponse(message)
		return dirty || changed, effects, nil
	default:
		return dirty, nil, nil
	}
}

func (n *Node) handleRequestVote(message Message) (bool, []Effect) {
	granted := false
	dirty := false
	if message.Term == n.state.HardState.CurrentTerm &&
		n.membership.isVoter(n.id) && n.membership.isVoter(message.From) &&
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

func (n *Node) handleRequestVoteResponse(message Message) (bool, []Effect, error) {
	if n.role != Candidate || message.Term != n.state.HardState.CurrentTerm || !message.VoteGranted {
		return false, nil, nil
	}
	if _, exists := n.votes[message.From]; exists {
		return false, nil, nil
	}
	n.votes[message.From] = struct{}{}
	if !n.membership.quorum(func(id NodeID) bool { _, ok := n.votes[id]; return ok }) {
		return false, nil, nil
	}
	effects, err := n.becomeLeader()
	return err == nil, effects, err
}

func (n *Node) becomeLeader() ([]Effect, error) {
	if n.lastIndex() == math.MaxUint64 {
		return nil, ErrIndexExhausted
	}
	n.role = Leader
	n.leader = n.id
	n.electionVotes = make([]NodeID, 0, len(n.votes))
	n.electionMembership = CloneMembership(n.membership)
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
	return effects, nil
}

func (n *Node) handleAppendEntries(message Message) (bool, []Effect) {
	if message.Term < n.state.HardState.CurrentTerm {
		return false, []Effect{{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, nextIndexAfter(n.lastIndex()))}}
	}
	if n.role != Follower || n.leader != message.From {
		n.becomeFollower(message.Term, message.From)
	}
	effects := []Effect{{Kind: EffectResetElectionTimer}}
	if message.PrevLogIndex < n.state.Snapshot.LastIncludedIndex || n.termAt(message.PrevLogIndex) != message.PrevLogTerm {
		rejectHint := min(message.PrevLogIndex, nextIndexAfter(n.lastIndex()))
		if message.PrevLogIndex < n.state.Snapshot.LastIncludedIndex {
			rejectHint = nextIndexAfter(n.state.Snapshot.LastIncludedIndex)
		}
		if rejectHint == 0 {
			rejectHint = 1
		}
		effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, rejectHint)})
		return false, effects
	}
	if uint64(len(message.Entries)) > math.MaxUint64-message.PrevLogIndex {
		effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, nextIndexAfter(n.lastIndex()))})
		return false, effects
	}
	previousTerm := message.PrevLogTerm
	membership := n.membershipAt(message.PrevLogIndex)
	for offset, incoming := range message.Entries {
		index := message.PrevLogIndex + uint64(offset) + 1
		if incoming.Index != index || incoming.Term == 0 || incoming.Term > message.Term || incoming.Term < previousTerm || incoming.Type < EntryNoop || incoming.Type > EntryConfigFinal {
			effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, index)})
			return false, effects
		}
		var transitionOK bool
		membership, transitionOK = transitionMembership(membership, incoming, n.members)
		if !transitionOK {
			effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, index)})
			return false, effects
		}
		previousTerm = incoming.Term
	}

	dirty := false
	for offset, incoming := range message.Entries {
		index := message.PrevLogIndex + uint64(offset) + 1
		if index <= n.lastIndex() {
			if n.termAt(index) == incoming.Term {
				continue
			}
			n.truncateFrom(index)
		}
		if index > n.lastIndex() {
			if index != nextIndexAfter(n.lastIndex()) {
				effects = append(effects, Effect{Kind: EffectSend, Message: n.appendResponse(message.From, message.Sequence, false, 0, nextIndexAfter(n.lastIndex()))})
				return dirty, effects
			}
			n.state.Log = append(n.state.Log, cloneEntry(incoming))
			dirty = true
		}
	}
	if dirty {
		n.refreshMembership()
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

func (n *Node) handleInstallSnapshot(message Message) (bool, []Effect) {
	if message.Term < n.state.HardState.CurrentTerm {
		return false, []Effect{{Kind: EffectSend, Message: n.snapshotResponse(message.From, message.Sequence, false, n.state.Snapshot.LastIncludedIndex)}}
	}
	if n.role != Follower || n.leader != message.From {
		n.becomeFollower(message.Term, message.From)
	}
	effects := []Effect{{Kind: EffectResetElectionTimer}}
	snapshot := message.Snapshot
	snapshotMembership := snapshot.Membership
	if membershipIsZero(snapshotMembership) {
		snapshotMembership = stableMembership(snapshot.Members, nil)
	}
	legacyMembershipMismatch := membershipIsZero(snapshot.Membership) && !membershipsEqual(n.initialMembership, stableMembership(n.members, nil))
	if snapshot.LastIncludedIndex == 0 || snapshot.LastIncludedTerm == 0 || snapshot.LastIncludedTerm > message.Term || !slices.Equal(snapshot.Members, n.members) || !validateMembership(snapshotMembership, n.members) || legacyMembershipMismatch {
		effects = append(effects, Effect{Kind: EffectSend, Message: n.snapshotResponse(message.From, message.Sequence, false, n.state.Snapshot.LastIncludedIndex)})
		return false, effects
	}
	if snapshot.LastIncludedIndex <= n.state.HardState.CommitIndex {
		effects = append(effects, Effect{Kind: EffectSend, Message: n.snapshotResponse(message.From, message.Sequence, true, snapshot.LastIncludedIndex)})
		return false, effects
	}
	var suffix []Entry
	if n.termAt(snapshot.LastIncludedIndex) == snapshot.LastIncludedTerm {
		if snapshot.LastIncludedIndex < math.MaxUint64 {
			suffix = n.entriesFrom(snapshot.LastIncludedIndex + 1)
		}
	}
	candidateMembership := CloneMembership(snapshotMembership)
	for _, entry := range suffix {
		var transitionOK bool
		candidateMembership, transitionOK = transitionMembership(candidateMembership, entry, n.members)
		if !transitionOK {
			effects = append(effects, Effect{Kind: EffectSend, Message: n.snapshotResponse(message.From, message.Sequence, false, n.state.Snapshot.LastIncludedIndex)})
			return false, effects
		}
	}
	n.state.Snapshot = cloneSnapshot(snapshot)
	n.state.Log = suffix
	n.state.HardState.CommitIndex = snapshot.LastIncludedIndex
	n.applied = snapshot.LastIncludedIndex
	n.refreshMembership()
	effects = append(effects,
		Effect{Kind: EffectInstallSnapshot, Snapshot: cloneSnapshot(snapshot)},
		Effect{Kind: EffectSend, Message: n.snapshotResponse(message.From, message.Sequence, true, snapshot.LastIncludedIndex)},
	)
	return true, effects
}

func (n *Node) handleInstallSnapshotResponse(message Message) (bool, []Effect) {
	if n.role != Leader || message.Term != n.state.HardState.CurrentTerm || !message.Success {
		return false, nil
	}
	if message.MatchIndex > n.matchIndex[message.From] {
		n.matchIndex[message.From] = min(message.MatchIndex, n.lastIndex())
		n.nextIndex[message.From] = nextIndexAfter(n.matchIndex[message.From])
	}
	effects := n.advanceCommit()
	dirty := len(effects) > 0
	if n.role != Leader {
		return dirty, effects
	}
	if n.matchIndex[message.From] < n.lastIndex() {
		effects = append(effects, Effect{Kind: EffectSend, Message: n.appendFor(message.From)})
	}
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
		n.nextIndex[message.From] = nextIndexAfter(n.matchIndex[message.From])
	}
	effects := n.advanceCommit()
	dirty := len(effects) > 0
	if n.role != Leader {
		return dirty, effects
	}
	if n.matchIndex[message.From] < n.lastIndex() {
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
		membership := n.membershipAt(index)
		if membership.quorum(func(member NodeID) bool { return n.matchIndex[member] >= index }) {
			n.state.HardState.CommitIndex = index
			effects := n.applyCommitted()
			if n.role == Leader && !n.membership.isVoter(n.id) {
				n.becomeFollower(n.state.HardState.CurrentTerm, "")
			}
			return effects
		}
	}
	return nil
}

func (n *Node) applyCommitted() []Effect {
	var effects []Effect
	for n.applied < n.state.HardState.CommitIndex {
		n.applied++
		entry, exists := n.entryAt(n.applied)
		if !exists {
			panic(fmt.Sprintf("raft: committed entry %d is unavailable after snapshot %d", n.applied, n.state.Snapshot.LastIncludedIndex))
		}
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
	if n.matchIndex[member] == math.MaxUint64 {
		return Message{
			Type:         AppendEntries,
			From:         n.id,
			To:           member,
			Term:         n.state.HardState.CurrentTerm,
			Sequence:     n.appendSequence[member],
			PrevLogIndex: math.MaxUint64,
			PrevLogTerm:  n.termAt(math.MaxUint64),
			LeaderCommit: n.state.HardState.CommitIndex,
		}
	}
	next := n.nextIndex[member]
	if next == 0 {
		next = nextIndexAfter(n.lastIndex())
	}
	if next <= n.state.Snapshot.LastIncludedIndex {
		return Message{Type: InstallSnapshot, From: n.id, To: member, Term: n.state.HardState.CurrentTerm, Sequence: n.appendSequence[member], Snapshot: cloneSnapshot(n.state.Snapshot)}
	}
	prev := next - 1
	var entries []Entry
	if next <= n.lastIndex() {
		entries = n.entriesFrom(next)
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

func (n *Node) snapshotResponse(to NodeID, sequence uint64, success bool, matchIndex uint64) Message {
	return Message{Type: InstallSnapshotResponse, From: n.id, To: to, Term: n.state.HardState.CurrentTerm, Sequence: sequence, Success: success, MatchIndex: matchIndex}
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
	n.electionMembership = Membership{}
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
	return n.state.Snapshot.LastIncludedIndex + uint64(len(n.state.Log))
}

func (n *Node) lastTerm() uint64 {
	return n.termAt(n.lastIndex())
}

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == n.state.Snapshot.LastIncludedIndex {
		return n.state.Snapshot.LastIncludedTerm
	}
	entry, exists := n.entryAt(index)
	if !exists {
		return 0
	}
	return entry.Term
}

func (n *Node) entryAt(index uint64) (Entry, bool) {
	boundary := n.state.Snapshot.LastIncludedIndex
	if index <= boundary || index > n.lastIndex() {
		return Entry{}, false
	}
	return n.state.Log[index-boundary-1], true
}

func (n *Node) entriesFrom(index uint64) []Entry {
	boundary := n.state.Snapshot.LastIncludedIndex
	if index <= boundary {
		return cloneEntries(n.state.Log)
	}
	if index > n.lastIndex() {
		return nil
	}
	return cloneEntries(n.state.Log[index-boundary-1:])
}

func (n *Node) truncateFrom(index uint64) {
	boundary := n.state.Snapshot.LastIncludedIndex
	if index <= boundary {
		n.state.Log = nil
		return
	}
	if index <= n.lastIndex() {
		n.state.Log = n.state.Log[:index-boundary-1]
	}
}

func (n *Node) membershipAt(index uint64) Membership {
	membership := CloneMembership(n.initialMembership)
	if n.state.Snapshot.LastIncludedIndex > 0 && index >= n.state.Snapshot.LastIncludedIndex {
		if membershipIsZero(n.state.Snapshot.Membership) {
			membership = stableMembership(n.state.Snapshot.Members, nil)
		} else {
			membership = CloneMembership(n.state.Snapshot.Membership)
		}
	}
	for _, entry := range n.state.Log {
		if entry.Index > index {
			break
		}
		if entry.Type != EntryConfigJoint && entry.Type != EntryConfigFinal {
			continue
		}
		next, ok := transitionMembership(membership, entry, n.members)
		if !ok {
			panic(fmt.Sprintf("raft: invalid membership transition at index %d", entry.Index))
		}
		membership = next
	}
	return membership
}

func (n *Node) refreshMembership() {
	n.membership = n.membershipAt(n.lastIndex())
}

func (n *Node) hasUncommittedConfiguration() bool {
	for _, entry := range n.state.Log {
		if entry.Index > n.state.HardState.CommitIndex && (entry.Type == EntryConfigJoint || entry.Type == EntryConfigFinal) {
			return true
		}
	}
	return false
}

func nextIndexAfter(index uint64) uint64 {
	if index == math.MaxUint64 {
		return math.MaxUint64
	}
	return index + 1
}
