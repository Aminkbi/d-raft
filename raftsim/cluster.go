// Package raftsim integrates the pure Raft reference model with d-raft's
// deterministic scheduler, network, storage, timers, and process lifecycle.
package raftsim

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
)

var (
	ErrInvalidConfig     = errors.New("raftsim: invalid configuration")
	ErrUnknownNode       = errors.New("raftsim: unknown node")
	ErrNodeDown          = errors.New("raftsim: node is down")
	ErrNodeUp            = errors.New("raftsim: node is already up")
	ErrNoLeader          = errors.New("raftsim: cluster has no unique leader")
	ErrApplyOrder        = errors.New("raftsim: applied entry is out of order")
	ErrTransportIdentity = errors.New("raftsim: transport identity mismatch")
)

// Config defines a deterministic Raft cluster over a fixed provisioned node
// universe. Voting and learner roles may change through joint consensus.
type Config struct {
	Members            []raft.NodeID
	Voters             []raft.NodeID
	Learners           []raft.NodeID
	Seed               uint64
	Network            sim.LinkConfig
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	StorageLatency     time.Duration
	Trace              sim.TraceSink
	StopOnViolation    bool
	Decider            decision.Decider
}

// Envelope carries harness-owned causal identity separately from a pure Raft
// protocol message. Packet.From and Packet.To remain the authoritative
// transport endpoints and are validated at delivery.
type Envelope struct {
	SenderIncarnation uint64       `json:"sender_incarnation"`
	SendSequence      uint64       `json:"send_sequence"`
	Message           raft.Message `json:"message"`
}

func cloneEnvelope(envelope Envelope) Envelope {
	envelope.Message = raft.CloneMessage(envelope.Message)
	return envelope
}

// DefaultConfig returns a useful low-latency configuration for members.
func DefaultConfig(members ...raft.NodeID) Config {
	return Config{
		Members:            slices.Clone(members),
		Network:            sim.LinkConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 25 * time.Millisecond},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		StorageLatency:     time.Millisecond,
		StopOnViolation:    true,
	}
}

func (c Config) validate() error {
	if len(c.Members) == 0 {
		return fmt.Errorf("%w: membership is empty", ErrInvalidConfig)
	}
	members := slices.Clone(c.Members)
	slices.Sort(members)
	for index, member := range members {
		if member == "" {
			return fmt.Errorf("%w: empty member", ErrInvalidConfig)
		}
		if index > 0 && member == members[index-1] {
			return fmt.Errorf("%w: duplicate member %q", ErrInvalidConfig, member)
		}
	}
	if len(c.Voters) > 0 || len(c.Learners) > 0 {
		membership := raft.StableMembership(c.Voters, c.Learners)
		if !raft.ValidateMembership(membership, members) {
			return fmt.Errorf("%w: invalid voter or learner sets", ErrInvalidConfig)
		}
	}
	if err := c.Network.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return fmt.Errorf("%w: election timeout range [%s, %s]", ErrInvalidConfig, c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("%w: heartbeat %s must be below minimum election timeout %s", ErrInvalidConfig, c.HeartbeatInterval, c.ElectionTimeoutMin)
	}
	if c.StorageLatency < 0 {
		return fmt.Errorf("%w: negative storage latency", ErrInvalidConfig)
	}
	return nil
}

// Store is the durable state retained across process crashes.
type Store struct {
	State             raft.PersistentState
	AppliedIndex      uint64
	Applied           []raft.Entry
	InstalledSnapshot raft.Snapshot
}

func (s Store) clone() Store {
	result := Store{State: raft.ClonePersistentState(s.State), AppliedIndex: s.AppliedIndex, InstalledSnapshot: raft.CloneSnapshot(s.InstalledSnapshot)}
	result.Applied = make([]raft.Entry, len(s.Applied))
	for index, entry := range s.Applied {
		result.Applied[index] = raft.CloneEntry(entry)
	}
	return result
}

type timerKind uint8

const (
	electionTimer timerKind = iota + 1
	heartbeatTimer
)

type queuedInput struct {
	input       raft.Input
	incarnation uint64
	timer       timerKind
	generation  uint64
}

type process struct {
	node        *raft.Node
	up          bool
	incarnation uint64
	store       Store
	random      *sim.Rand
	mailbox     []queuedInput

	electionEvent       sim.EventID
	electionGeneration  uint64
	heartbeatEvent      sim.EventID
	heartbeatGeneration uint64
	persistEvent        sim.EventID
	persistAckEvent     sim.EventID
	persistGeneration   uint64
	crashAfterPersist   bool
	sendSequence        uint64
}

// Cluster owns a deterministic reference Raft execution.
type Cluster struct {
	config     Config
	members    []raft.NodeID
	simulator  *sim.Simulator
	router     *sim.Router[Envelope]
	processes  map[raft.NodeID]*process
	checker    *check.Checker
	violations []check.Violation
	err        error
}

// New creates and starts every configured process.
func New(config Config) (*Cluster, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	members := slices.Clone(config.Members)
	slices.Sort(members)
	config.Members = slices.Clone(members)
	config.Voters = slices.Clone(config.Voters)
	config.Learners = slices.Clone(config.Learners)
	initialMembership := raft.StableMembership(config.Voters, config.Learners)
	if len(config.Voters) == 0 && len(config.Learners) == 0 {
		initialMembership = raft.StableMembership(members, nil)
	}
	simulator := sim.New()
	rootRandom := sim.NewRand(config.Seed)
	if config.Trace != nil {
		simulator.SetTraceSink(config.Trace)
		if config.Decider == nil {
			rootRandom.SetTraceSink(config.Trace, "cluster")
		}
	}
	networkRandom := rootRandom.Split()
	router, err := sim.NewRouter(simulator, networkRandom, config.Network, cloneEnvelope)
	if err != nil {
		return nil, err
	}
	if config.Trace != nil {
		router.SetTraceSink(config.Trace)
	}
	if config.Decider != nil {
		router.SetDecisionSource(raftNetworkDecisions{decider: config.Decider})
	}
	cluster := &Cluster{
		config:    config,
		members:   members,
		simulator: simulator,
		router:    router,
		processes: make(map[raft.NodeID]*process, len(members)),
		checker:   check.NewWithMembership(members, initialMembership),
	}
	for _, id := range members {
		cluster.processes[id] = &process{up: true, incarnation: 1, random: rootRandom.Split()}
	}
	for _, id := range members {
		id := id
		if err := router.Register(sim.NodeID(id), func(packet sim.Packet[Envelope]) {
			cluster.receive(id, packet)
		}); err != nil {
			return nil, err
		}
	}
	for _, id := range members {
		if err := cluster.start(id); err != nil {
			return nil, err
		}
	}
	cluster.observe()
	return cluster, nil
}

// Simulator returns the cluster's virtual-time scheduler.
func (c *Cluster) Simulator() *sim.Simulator {
	return c.simulator
}

// Router returns the cluster's deterministic network.
func (c *Cluster) Router() *sim.Router[Envelope] {
	return c.router
}

// Members returns the canonical membership order.
func (c *Cluster) Members() []raft.NodeID {
	return slices.Clone(c.members)
}

// Status returns one live node's state.
func (c *Cluster) Status(id raft.NodeID) (raft.Status, error) {
	process, ok := c.processes[id]
	if !ok {
		return raft.Status{}, fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return raft.Status{}, fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	return process.node.Status(), nil
}

// Statuses returns live node states in canonical membership order.
func (c *Cluster) Statuses() []raft.Status {
	result := make([]raft.Status, 0, len(c.members))
	for _, id := range c.members {
		if process := c.processes[id]; process.up {
			result = append(result, process.node.Status())
		}
	}
	return result
}

// Store returns a deep copy of a node's durable store, whether it is up or
// down.
func (c *Cluster) Store(id raft.NodeID) (Store, error) {
	process, ok := c.processes[id]
	if !ok {
		return Store{}, fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	return process.store.clone(), nil
}

// Leader returns the unique live leader. Multiple simultaneous leaders are
// reported as no unique leader rather than silently selecting one.
func (c *Cluster) Leader() (raft.NodeID, bool) {
	var leader raft.NodeID
	for _, status := range c.Statuses() {
		if status.Role != raft.Leader {
			continue
		}
		if leader != "" {
			return "", false
		}
		leader = status.ID
	}
	return leader, leader != ""
}

// Propose submits a command to the unique live leader.
func (c *Cluster) Propose(data []byte) error {
	leader, ok := c.Leader()
	if !ok {
		return ErrNoLeader
	}
	return c.ProposeTo(leader, data)
}

// ProposeTo submits a command to a specific live leader. It is useful during
// partitions where leaders from different terms can coexist temporarily.
func (c *Cluster) ProposeTo(id raft.NodeID, data []byte) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	if process.node.Status().Role != raft.Leader {
		return raft.ErrNotLeader
	}
	if err := c.submit(id, queuedInput{input: raft.Input{Kind: raft.InputProposal, Data: slices.Clone(data)}, incarnation: process.incarnation}); err != nil {
		return err
	}
	c.observe()
	return c.err
}

// Snapshot compacts a live node through its current applied index. Data is the
// application checkpoint corresponding to that index.
func (c *Cluster) Snapshot(id raft.NodeID, data []byte) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	index := process.store.AppliedIndex
	if err := c.submit(id, queuedInput{input: raft.Input{Kind: raft.InputSnapshot, SnapshotIndex: index, SnapshotData: slices.Clone(data)}, incarnation: process.incarnation}); err != nil {
		return err
	}
	c.observe()
	return c.err
}

// BeginMembershipChange appends a joint configuration through the current
// leader. Every node must already belong to the pre-provisioned Members
// universe.
func (c *Cluster) BeginMembershipChange(voters, learners []raft.NodeID) error {
	leader, ok := c.Leader()
	if !ok {
		return ErrNoLeader
	}
	return c.BeginMembershipChangeTo(leader, voters, learners)
}

// BeginMembershipChangeTo targets a specific live leader.
func (c *Cluster) BeginMembershipChangeTo(id raft.NodeID, voters, learners []raft.NodeID) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	if process.node.Status().Role != raft.Leader {
		return raft.ErrNotLeader
	}
	if err := c.submit(id, queuedInput{input: raft.Input{Kind: raft.InputBeginMembership, Voters: slices.Clone(voters), Learners: slices.Clone(learners)}, incarnation: process.incarnation}); err != nil {
		return err
	}
	c.observe()
	return c.err
}

// FinalizeMembershipChange appends the stable incoming configuration through
// the current leader after the joint entry has committed.
func (c *Cluster) FinalizeMembershipChange() error {
	leader, ok := c.Leader()
	if !ok {
		return ErrNoLeader
	}
	return c.FinalizeMembershipChangeTo(leader)
}

// FinalizeMembershipChangeTo targets a specific live leader.
func (c *Cluster) FinalizeMembershipChangeTo(id raft.NodeID) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	if process.node.Status().Role != raft.Leader {
		return raft.ErrNotLeader
	}
	if err := c.submit(id, queuedInput{input: raft.Input{Kind: raft.InputFinalizeMembership}, incarnation: process.incarnation}); err != nil {
		return err
	}
	c.observe()
	return c.err
}

// Violations returns independent safety violations observed so far.
func (c *Cluster) Violations() []check.Violation {
	return slices.Clone(c.violations)
}

// Step executes one simulator event. A callback failure is returned after the
// event so errors are never hidden inside Action closures.
func (c *Cluster) Step() (bool, error) {
	if c.err != nil {
		return false, c.err
	}
	ran := c.simulator.Step()
	if ran {
		c.observe()
	}
	if c.err != nil {
		return ran, c.err
	}
	return ran, nil
}

// RunSteps executes at most limit events.
func (c *Cluster) RunSteps(limit int) (int, error) {
	count := 0
	for count < limit {
		ran, err := c.Step()
		if err != nil || !ran {
			return count, err
		}
		count++
	}
	return count, nil
}

// RunUntil executes through the inclusive virtual-time boundary.
func (c *Cluster) RunUntil(end time.Duration) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if end < c.simulator.Now() {
		_, err := c.simulator.RunUntil(end)
		return 0, err
	}
	count := 0
	for {
		next, exists := c.simulator.NextEventTime()
		if !exists || next > end {
			break
		}
		ran, err := c.Step()
		if err != nil || !ran {
			return count, err
		}
		count++
	}
	_, err := c.simulator.RunUntil(end)
	return count, err
}

// Crash destroys a process's volatile state while preserving its durable
// store and already-sent network packets.
func (c *Cluster) Crash(id raft.NodeID) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	process.up = false
	process.incarnation++
	process.node = nil
	process.mailbox = nil
	process.crashAfterPersist = false
	process.sendSequence = 0
	c.cancelProcessEvents(process)
	c.trace(id, sim.TraceProcessLifecycle, "crashed", map[string]any{"incarnation": process.incarnation})
	c.observe()
	return c.err
}

// Restart constructs a fresh follower solely from durable storage.
func (c *Cluster) Restart(id raft.NodeID) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if process.up {
		return fmt.Errorf("%w: %q", ErrNodeUp, id)
	}
	process.up = true
	process.mailbox = nil
	if err := c.start(id); err != nil {
		process.up = false
		return err
	}
	c.trace(id, sim.TraceProcessLifecycle, "restarted", map[string]any{"incarnation": process.incarnation})
	c.observe()
	return c.err
}

// Partition activates symmetric connectivity groups.
func (c *Cluster) Partition(groups ...[]raft.NodeID) error {
	converted := make([][]sim.NodeID, len(groups))
	for groupIndex, group := range groups {
		converted[groupIndex] = make([]sim.NodeID, len(group))
		for index, id := range group {
			converted[groupIndex][index] = sim.NodeID(id)
		}
	}
	matrix, err := sim.NewPartitions(converted...)
	if err != nil {
		return err
	}
	c.router.SetPartition(matrix)
	return nil
}

// Heal restores full network connectivity.
func (c *Cluster) Heal() {
	c.router.SetPartition(nil)
}

// ScheduleCrash schedules a crash relative to the current virtual time.
func (c *Cluster) ScheduleCrash(delay time.Duration, id raft.NodeID) (sim.EventID, error) {
	return c.simulator.Schedule(delay, func(*sim.Simulator) {
		if err := c.Crash(id); err != nil {
			c.fail(err)
		}
	})
}

// ScheduleRestart schedules a restart relative to the current virtual time.
func (c *Cluster) ScheduleRestart(delay time.Duration, id raft.NodeID) (sim.EventID, error) {
	return c.simulator.Schedule(delay, func(*sim.Simulator) {
		if err := c.Restart(id); err != nil {
			c.fail(err)
		}
	})
}

// CrashAfterNextPersist arms a deterministic crash after the next durable
// write completes but before its acknowledgement releases dependent effects.
func (c *Cluster) CrashAfterNextPersist(id raft.NodeID) error {
	process, ok := c.processes[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, id)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, id)
	}
	process.crashAfterPersist = true
	return nil
}

func (c *Cluster) start(id raft.NodeID) error {
	process := c.processes[id]
	node, err := raft.New(raft.Config{ID: id, Members: c.members, Voters: c.config.Voters, Learners: c.config.Learners, AppliedIndex: process.store.AppliedIndex}, process.store.State)
	if err != nil {
		return err
	}
	process.node = node
	return c.processEffects(id, node.Start())
}

func (c *Cluster) receive(id raft.NodeID, packet sim.Packet[Envelope]) {
	envelope := packet.Message
	message := envelope.Message
	if envelope.SenderIncarnation == 0 || envelope.SendSequence == 0 || packet.From != sim.NodeID(message.From) || packet.To != sim.NodeID(message.To) || message.To != id {
		c.fail(fmt.Errorf("%w: packet=%s->%s message=%s->%s", ErrTransportIdentity, packet.From, packet.To, message.From, message.To))
		return
	}
	process := c.processes[id]
	if !process.up {
		c.trace(id, sim.TraceProtocolDrop, "process_down", message)
		return
	}
	queued := queuedInput{input: raft.Input{Kind: raft.InputMessage, Message: raft.CloneMessage(message)}, incarnation: process.incarnation}
	if err := c.submit(id, queued); err != nil {
		c.fail(err)
	}
}

func (c *Cluster) submit(id raft.NodeID, queued queuedInput) error {
	process := c.processes[id]
	if !process.up || queued.incarnation != process.incarnation {
		return nil
	}
	if process.node.Status().AwaitingPersistence {
		process.mailbox = append(process.mailbox, queued)
		return nil
	}
	if !c.queuedInputIsCurrent(process, queued) {
		return nil
	}
	before := process.node.Status()
	c.trace(id, sim.TraceProtocolInput, inputAction(queued.input), queued.input)
	effects, err := process.node.Step(queued.input)
	if err != nil {
		return err
	}
	if err := c.processEffects(id, effects); err != nil {
		return err
	}
	after := process.node.Status()
	c.trace(id, sim.TraceProtocolState, "transition", map[string]any{"before": before, "after": after})
	c.syncRoleTimers(process, before.Role, after.Role)
	return nil
}

func (c *Cluster) processEffects(id raft.NodeID, effects []raft.Effect) error {
	process := c.processes[id]
	for _, effect := range effects {
		switch effect.Kind {
		case raft.EffectPersist:
			if process.persistEvent != 0 || process.persistAckEvent != 0 {
				return fmt.Errorf("raftsim: node %q requested overlapping persistence", id)
			}
			process.persistGeneration++
			generation := process.persistGeneration
			incarnation := process.incarnation
			state := raft.ClonePersistentState(effect.State)
			token := effect.WriteToken
			delay, err := c.chooseDuration(
				fmt.Sprintf("storage/%s/%d/%d", id, incarnation, generation),
				decision.StorageLatency,
				c.config.StorageLatency,
				c.config.StorageLatency,
				map[string]any{"node": id, "incarnation": incarnation, "generation": generation, "token": token},
			)
			if err != nil {
				return err
			}
			eventID, err := c.simulator.Schedule(delay, func(*sim.Simulator) {
				if !process.up || process.incarnation != incarnation || process.persistGeneration != generation {
					return
				}
				process.persistEvent = 0
				process.store.State = raft.ClonePersistentState(state)
				c.trace(id, sim.TracePersistence, "completed", map[string]any{"token": token, "state": state})
				if process.crashAfterPersist {
					process.crashAfterPersist = false
					if crashErr := c.Crash(id); crashErr != nil {
						c.fail(crashErr)
					}
					return
				}
				ackID, scheduleErr := c.simulator.Schedule(0, func(*sim.Simulator) {
					if !process.up || process.incarnation != incarnation || process.persistGeneration != generation {
						return
					}
					process.persistAckEvent = 0
					after, stepErr := process.node.Step(raft.Input{Kind: raft.InputPersisted, WriteToken: token})
					if stepErr != nil {
						c.fail(stepErr)
						return
					}
					if processErr := c.processEffects(id, after); processErr != nil {
						c.fail(processErr)
						return
					}
					c.drain(id)
				})
				if scheduleErr != nil {
					c.fail(scheduleErr)
					return
				}
				process.persistAckEvent = ackID
			})
			if err != nil {
				return err
			}
			process.persistEvent = eventID
			c.trace(id, sim.TracePersistence, "scheduled", map[string]any{"token": token, "completion_at_ns": int64(c.simulator.Now() + delay)})
		case raft.EffectSend:
			process.sendSequence++
			message := raft.CloneMessage(effect.Message)
			envelope := Envelope{SenderIncarnation: process.incarnation, SendSequence: process.sendSequence, Message: message}
			if _, err := c.router.Send(sim.NodeID(id), sim.NodeID(message.To), envelope); err != nil {
				return err
			}
		case raft.EffectResetElectionTimer:
			if err := c.resetElectionTimer(id); err != nil {
				return err
			}
		case raft.EffectResetHeartbeatTimer:
			if err := c.resetHeartbeatTimer(id); err != nil {
				return err
			}
		case raft.EffectApply:
			if effect.Entry.Index != process.store.AppliedIndex+1 {
				return fmt.Errorf("%w: node=%q got=%d want=%d", ErrApplyOrder, id, effect.Entry.Index, process.store.AppliedIndex+1)
			}
			process.store.AppliedIndex = effect.Entry.Index
			process.store.Applied = append(process.store.Applied, raft.CloneEntry(effect.Entry))
		case raft.EffectInstallSnapshot:
			if effect.Snapshot.LastIncludedIndex < process.store.AppliedIndex {
				return fmt.Errorf("%w: node=%q snapshot=%d applied=%d", ErrApplyOrder, id, effect.Snapshot.LastIncludedIndex, process.store.AppliedIndex)
			}
			process.store.AppliedIndex = effect.Snapshot.LastIncludedIndex
			process.store.InstalledSnapshot = raft.CloneSnapshot(effect.Snapshot)
			process.store.Applied = slices.DeleteFunc(process.store.Applied, func(entry raft.Entry) bool {
				return entry.Index <= effect.Snapshot.LastIncludedIndex
			})
			c.trace(id, sim.TracePersistence, "snapshot_installed", effect.Snapshot)
		default:
			return fmt.Errorf("raftsim: unknown effect kind %d", effect.Kind)
		}
	}
	return nil
}

func (c *Cluster) drain(id raft.NodeID) {
	process := c.processes[id]
	for process.up && !process.node.Status().AwaitingPersistence && len(process.mailbox) > 0 {
		queued := process.mailbox[0]
		copy(process.mailbox, process.mailbox[1:])
		process.mailbox[len(process.mailbox)-1] = queuedInput{}
		process.mailbox = process.mailbox[:len(process.mailbox)-1]
		if err := c.submit(id, queued); err != nil {
			c.fail(err)
			return
		}
	}
}

func (c *Cluster) resetElectionTimer(id raft.NodeID) error {
	process := c.processes[id]
	if process.electionEvent != 0 {
		c.simulator.Cancel(process.electionEvent)
		process.electionEvent = 0
	}
	if !process.node.Status().Membership.IsVoter(id) {
		return nil
	}
	process.electionGeneration++
	generation := process.electionGeneration
	incarnation := process.incarnation
	var delay time.Duration
	if c.config.Decider == nil {
		delay = process.random.Duration(c.config.ElectionTimeoutMin, c.config.ElectionTimeoutMax)
	} else {
		var err error
		delay, err = c.chooseDuration(
			fmt.Sprintf("timer/%s/%d/election/%d", id, incarnation, generation),
			decision.ElectionTimeout,
			c.config.ElectionTimeoutMin,
			c.config.ElectionTimeoutMax,
			map[string]any{"node": id, "incarnation": incarnation, "generation": generation},
		)
		if err != nil {
			return err
		}
	}
	eventID, err := c.simulator.Schedule(delay, func(*sim.Simulator) {
		if !process.up || process.incarnation != incarnation || process.electionGeneration != generation || process.node.Status().Role == raft.Leader {
			return
		}
		process.electionEvent = 0
		if submitErr := c.submit(id, queuedInput{input: raft.Input{Kind: raft.InputElectionTimeout}, incarnation: incarnation, timer: electionTimer, generation: generation}); submitErr != nil {
			c.fail(submitErr)
		}
	})
	if err != nil {
		return err
	}
	process.electionEvent = eventID
	return nil
}

func (c *Cluster) resetHeartbeatTimer(id raft.NodeID) error {
	process := c.processes[id]
	if process.heartbeatEvent != 0 {
		c.simulator.Cancel(process.heartbeatEvent)
	}
	process.heartbeatGeneration++
	generation := process.heartbeatGeneration
	incarnation := process.incarnation
	eventID, err := c.simulator.Schedule(c.config.HeartbeatInterval, func(*sim.Simulator) {
		if !process.up || process.incarnation != incarnation || process.heartbeatGeneration != generation || process.node.Status().Role != raft.Leader {
			return
		}
		process.heartbeatEvent = 0
		if submitErr := c.submit(id, queuedInput{input: raft.Input{Kind: raft.InputHeartbeatTimeout}, incarnation: incarnation, timer: heartbeatTimer, generation: generation}); submitErr != nil {
			c.fail(submitErr)
		}
	})
	if err != nil {
		return err
	}
	process.heartbeatEvent = eventID
	return nil
}

func (c *Cluster) queuedInputIsCurrent(process *process, queued queuedInput) bool {
	if queued.incarnation != process.incarnation {
		return false
	}
	switch queued.timer {
	case electionTimer:
		return queued.generation == process.electionGeneration && process.node.Status().Role != raft.Leader
	case heartbeatTimer:
		return queued.generation == process.heartbeatGeneration && process.node.Status().Role == raft.Leader
	default:
		return true
	}
}

func (c *Cluster) syncRoleTimers(process *process, before, after raft.Role) {
	if before != raft.Leader && after == raft.Leader && process.electionEvent != 0 {
		c.simulator.Cancel(process.electionEvent)
		process.electionEvent = 0
		process.electionGeneration++
	}
	if before == raft.Leader && after != raft.Leader && process.heartbeatEvent != 0 {
		c.simulator.Cancel(process.heartbeatEvent)
		process.heartbeatEvent = 0
		process.heartbeatGeneration++
	}
}

func (c *Cluster) cancelProcessEvents(process *process) {
	for _, eventID := range []sim.EventID{process.electionEvent, process.heartbeatEvent, process.persistEvent, process.persistAckEvent} {
		if eventID != 0 {
			c.simulator.Cancel(eventID)
		}
	}
	process.electionEvent = 0
	process.heartbeatEvent = 0
	process.persistEvent = 0
	process.persistAckEvent = 0
	process.electionGeneration++
	process.heartbeatGeneration++
	process.persistGeneration++
}

func (c *Cluster) trace(id raft.NodeID, kind sim.TraceEventKind, action string, details any) {
	if c.config.Trace == nil {
		return
	}
	at := int64(c.simulator.Now())
	c.config.Trace.RecordTrace(sim.TraceEvent{
		Kind:      kind,
		AtNS:      &at,
		Component: "raft/" + string(id),
		Action:    action,
		Details:   details,
	})
}

func (c *Cluster) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}

func (c *Cluster) observe() {
	observation := check.Observation{At: c.simulator.Now(), Members: slices.Clone(c.members)}
	for _, id := range c.members {
		process := c.processes[id]
		store := process.store.clone()
		node := check.NodeObservation{
			ID:           id,
			Up:           process.up,
			Durable:      store.State,
			AppliedIndex: store.AppliedIndex,
			Applied:      store.Applied,
		}
		if process.up {
			status := process.node.Status()
			node.Status = &status
		}
		observation.Nodes = append(observation.Nodes, node)
	}
	violations := c.checker.Observe(observation)
	for _, violation := range violations {
		c.violations = append(c.violations, violation)
		c.trace("", sim.TraceProtocolState, "invariant_violation", violation)
	}
	if c.config.StopOnViolation && len(violations) > 0 {
		c.fail(violations[0])
	}
}

func (c *Cluster) chooseDuration(id string, kind decision.Kind, minimum, maximum time.Duration, context any) (time.Duration, error) {
	if c.config.Decider == nil {
		return minimum, nil
	}
	minNS, maxNS := int64(minimum), int64(maximum)
	contextJSON, err := json.Marshal(context)
	if err != nil {
		return 0, err
	}
	choice := decision.Choice{ID: id, Kind: kind, Min: &minNS, Max: &maxNS, Context: contextJSON}
	selection, err := c.config.Decider.Choose(choice)
	if err != nil {
		return 0, err
	}
	if err := decision.ValidateSelection(choice, selection); err != nil {
		return 0, fmt.Errorf("raftsim: duration choice %q: %w", id, err)
	}
	return time.Duration(*selection.Number), nil
}

type raftNetworkDecisions struct {
	decider decision.Decider
}

type networkDecisionContext struct {
	From              sim.NodeID   `json:"from"`
	To                sim.NodeID   `json:"to"`
	SenderIncarnation uint64       `json:"sender_incarnation"`
	SendSequence      uint64       `json:"send_sequence"`
	Message           raft.Message `json:"message"`
	MinLatencyNS      int64        `json:"min_latency_ns"`
	MaxLatencyNS      int64        `json:"max_latency_ns"`
	LossProbability   float64      `json:"loss_probability"`
}

func semanticNetworkContext(packet sim.Packet[Envelope], link sim.LinkConfig) networkDecisionContext {
	return networkDecisionContext{
		From: packet.From, To: packet.To,
		SenderIncarnation: packet.Message.SenderIncarnation,
		SendSequence:      packet.Message.SendSequence,
		Message:           raft.CloneMessage(packet.Message.Message),
		MinLatencyNS:      int64(link.MinLatency), MaxLatencyNS: int64(link.MaxLatency),
		LossProbability: link.LossProbability,
	}
}

func (d raftNetworkDecisions) Drop(packet sim.Packet[Envelope], link sim.LinkConfig) (bool, error) {
	options := lossOptions(link.LossProbability)
	context, err := json.Marshal(semanticNetworkContext(packet, link))
	if err != nil {
		return false, err
	}
	choice := decision.Choice{
		ID:      fmt.Sprintf("network/%s/%d/%d/loss", packet.From, packet.Message.SenderIncarnation, packet.Message.SendSequence),
		Kind:    decision.NetworkLoss,
		Options: options,
		Context: context,
	}
	selection, err := d.decider.Choose(choice)
	if err != nil {
		return false, err
	}
	if err := decision.ValidateSelection(choice, selection); err != nil {
		return false, fmt.Errorf("raftsim: network loss choice: %w", err)
	}
	return selection.Option == "drop", nil
}

func (d raftNetworkDecisions) Latency(packet sim.Packet[Envelope], link sim.LinkConfig) (time.Duration, error) {
	minimum, maximum := int64(link.MinLatency), int64(link.MaxLatency)
	context, err := json.Marshal(semanticNetworkContext(packet, link))
	if err != nil {
		return 0, err
	}
	choice := decision.Choice{
		ID:      fmt.Sprintf("network/%s/%d/%d/latency", packet.From, packet.Message.SenderIncarnation, packet.Message.SendSequence),
		Kind:    decision.NetworkLatency,
		Min:     &minimum,
		Max:     &maximum,
		Context: context,
	}
	selection, err := d.decider.Choose(choice)
	if err != nil {
		return 0, err
	}
	if err := decision.ValidateSelection(choice, selection); err != nil {
		return 0, fmt.Errorf("raftsim: network latency choice: %w", err)
	}
	return time.Duration(*selection.Number), nil
}

func lossOptions(probability float64) []decision.Option {
	if probability <= 0 {
		return []decision.Option{{ID: "deliver", Weight: 1}}
	}
	if probability >= 1 {
		return []decision.Option{{ID: "drop", Weight: 1}}
	}
	const scale = uint64(1) << 53
	dropWeight := uint64(math.Ceil(probability * float64(scale)))
	return []decision.Option{{ID: "drop", Weight: dropWeight}, {ID: "deliver", Weight: scale - dropWeight}}
}

func inputAction(input raft.Input) string {
	switch input.Kind {
	case raft.InputElectionTimeout:
		return "election_timeout"
	case raft.InputHeartbeatTimeout:
		return "heartbeat_timeout"
	case raft.InputMessage:
		return "message/" + input.Message.Type.String()
	case raft.InputProposal:
		return "proposal"
	case raft.InputPersisted:
		return "persisted"
	case raft.InputSnapshot:
		return "snapshot"
	case raft.InputBeginMembership:
		return "begin_membership"
	case raft.InputFinalizeMembership:
		return "finalize_membership"
	default:
		return "unknown"
	}
}
