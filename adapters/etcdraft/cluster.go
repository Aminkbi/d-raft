package etcdraft

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type inputKind uint8

const (
	inputMessage inputKind = iota + 1
	inputCampaign
	inputHeartbeat
	inputProposal
)

type queuedInput struct {
	kind        inputKind
	message     *pb.Message
	data        []byte
	incarnation uint64
}

type pendingReady struct {
	ready       etcdraft.Ready
	generation  uint64
	incarnation uint64
	event       sim.EventID
	persisted   bool
}

type process struct {
	name        rootraft.NodeID
	id          uint64
	raw         *etcdraft.RawNode
	storage     *etcdraft.MemoryStorage
	up          bool
	incarnation uint64

	appliedIndex uint64
	applied      []rootraft.Entry
	chain        Chain
	application  *apporacle.Machine
	mailbox      []queuedInput
	pending      *pendingReady
	persistGen   uint64

	electionEvent   sim.EventID
	electionGen     uint64
	heartbeatEvent  sim.EventID
	heartbeatGen    uint64
	crashAfterWrite bool
	sendSequence    uint64
	campaignQueued  bool
	heartbeatQueued bool
}

// Cluster is a deterministic environment around the unmodified production
// etcd/raft RawNode implementation.
type Cluster struct {
	config     Config
	members    []rootraft.NodeID
	ids        map[rootraft.NodeID]uint64
	names      map[uint64]rootraft.NodeID
	simulator  *sim.Simulator
	router     *sim.Router[envelope]
	processes  map[rootraft.NodeID]*process
	checker    *check.Checker
	violations []check.Violation
	err        error
}

func New(config Config) (*Cluster, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if config.Decider == nil {
		config.Decider = decision.NewSeedDecider(config.Seed)
	}
	if config.Application != nil {
		application := *config.Application
		config.Application = &application
	}
	members := slices.Clone(config.Members)
	slices.Sort(members)
	config.Members = slices.Clone(members)
	simulator := sim.New()
	router, err := sim.NewRouter(simulator, sim.NewRand(config.Seed), sim.LinkConfig{
		MinLatency: config.Network.MinLatency, MaxLatency: config.Network.MaxLatency, LossProbability: config.Network.LossProbability,
	}, cloneEnvelope)
	if err != nil {
		return nil, err
	}
	router.SetDecisionSource(networkDecisions{decider: config.Decider})
	cluster := &Cluster{
		config: config, members: members, simulator: simulator, router: router,
		ids: make(map[rootraft.NodeID]uint64, len(members)), names: make(map[uint64]rootraft.NodeID, len(members)),
		processes: make(map[rootraft.NodeID]*process, len(members)), checker: check.New(members),
	}
	for index, name := range members {
		id := uint64(index + 1)
		cluster.ids[name], cluster.names[id] = id, name
	}
	for _, name := range members {
		process, err := cluster.newProcess(name, nil)
		if err != nil {
			return nil, err
		}
		cluster.processes[name] = process
		name := name
		if err := router.Register(sim.NodeID(name), func(packet sim.Packet[envelope]) { cluster.receive(name, packet) }); err != nil {
			return nil, err
		}
	}
	for _, name := range members {
		if err := cluster.resetElectionTimer(name); err != nil {
			return nil, err
		}
	}
	cluster.observe()
	if cluster.err != nil {
		return nil, cluster.err
	}
	return cluster, nil
}

func (c *Cluster) newProcess(name rootraft.NodeID, prior *process) (*process, error) {
	id := c.ids[name]
	if prior == nil {
		storage := etcdraft.NewMemoryStorage()
		voters := make([]uint64, 0, len(c.members))
		for _, member := range c.members {
			voters = append(voters, c.ids[member])
		}
		snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: &pb.ConfState{Voters: voters}}}
		if err := storage.ApplySnapshot(snapshot); err != nil {
			return nil, err
		}
		if err := storage.SetHardState(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}); err != nil {
			return nil, err
		}
		prior = &process{name: name, id: id, storage: storage, up: true, incarnation: 1, appliedIndex: 1}
		if c.config.Application != nil {
			prior.application = apporacle.New()
		}
		if err := prior.chain.Apply(rootraft.Entry{Index: 1, Term: 1, Type: rootraft.EntryNoop}); err != nil {
			return nil, err
		}
	}
	if c.config.Application != nil {
		application, err := recoverApplication(prior)
		if err != nil {
			return nil, fmt.Errorf("etcdraft: recover portable application: %w", err)
		}
		prior.application = application
	}
	raw, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID: id, ElectionTick: 1_000_000_000, HeartbeatTick: 1, Storage: prior.storage, Applied: prior.appliedIndex,
		MaxSizePerMsg: math.MaxUint64, MaxCommittedSizePerReady: math.MaxUint64, MaxInflightMsgs: 256,
		CheckQuorum: false, PreVote: false, AsyncStorageWrites: false, StepDownOnRemoval: true, Logger: adapterLogger{},
	})
	if err != nil {
		return nil, err
	}
	prior.raw, prior.up = raw, true
	return prior, nil
}

func (c *Cluster) Simulator() *sim.Simulator { return c.simulator }

func (c *Cluster) Members() []rootraft.NodeID { return slices.Clone(c.members) }

func (c *Cluster) Violations() []check.Violation {
	result := make([]check.Violation, len(c.violations))
	for index, violation := range c.violations {
		violation.Nodes = slices.Clone(violation.Nodes)
		violation.Evidence = slices.Clone(violation.Evidence)
		result[index] = violation
	}
	return result
}

func (c *Cluster) Leader() (rootraft.NodeID, bool) {
	var leader rootraft.NodeID
	for _, name := range c.members {
		process := c.processes[name]
		if !process.up || process.raw.BasicStatus().RaftState != etcdraft.StateLeader {
			continue
		}
		if leader != "" {
			return "", false
		}
		leader = name
	}
	return leader, leader != ""
}

func (c *Cluster) Propose(data []byte) error {
	leader, ok := c.Leader()
	if !ok {
		return ErrNoLeader
	}
	return c.ProposeTo(leader, data)
}

func (c *Cluster) ProposeTo(name rootraft.NodeID, data []byte) error {
	process, ok := c.processes[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, name)
	}
	if len(data) == 0 {
		return ErrInvalidProposal
	}
	if c.config.Application != nil {
		if _, err := apporacle.DecodeCommand(data); err != nil {
			return fmt.Errorf("etcdraft: invalid portable proposal: %w", err)
		}
	}
	if process.raw.BasicStatus().RaftState != etcdraft.StateLeader {
		return etcdraft.ErrProposalDropped
	}
	return c.submit(name, queuedInput{kind: inputProposal, data: slices.Clone(data), incarnation: process.incarnation})
}

func (c *Cluster) Crash(name rootraft.NodeID) error {
	process, ok := c.processes[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, name)
	}
	c.cancelProcessEvents(process)
	process.up = false
	process.incarnation++
	process.raw = nil
	process.application = nil
	process.mailbox = nil
	process.pending = nil
	process.crashAfterWrite = false
	process.sendSequence = 0
	process.campaignQueued = false
	process.heartbeatQueued = false
	c.observe()
	return c.err
}

func (c *Cluster) Restart(name rootraft.NodeID) error {
	process, ok := c.processes[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	if process.up {
		return fmt.Errorf("%w: %q", ErrNodeUp, name)
	}
	if _, err := c.newProcess(name, process); err != nil {
		return err
	}
	if err := c.resetElectionTimer(name); err != nil {
		return err
	}
	if err := c.pump(name); err != nil {
		return err
	}
	c.observe()
	return c.err
}

func (c *Cluster) CrashAfterNextPersist(name rootraft.NodeID) error {
	process, ok := c.processes[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, name)
	}
	if !process.up {
		return fmt.Errorf("%w: %q", ErrNodeDown, name)
	}
	process.crashAfterWrite = true
	return nil
}

func (c *Cluster) Partition(groups ...[]rootraft.NodeID) error {
	converted := make([][]sim.NodeID, len(groups))
	for groupIndex, group := range groups {
		converted[groupIndex] = make([]sim.NodeID, len(group))
		for index, member := range group {
			converted[groupIndex][index] = sim.NodeID(member)
		}
	}
	matrix, err := sim.NewPartitions(converted...)
	if err != nil {
		return err
	}
	c.router.SetPartition(matrix)
	return nil
}

func (c *Cluster) Heal() { c.router.SetPartition(nil) }

func (c *Cluster) Step() (bool, error) {
	if c.err != nil {
		return false, c.err
	}
	ran := c.simulator.Step()
	if ran {
		c.observe()
	}
	return ran, c.err
}

func (c *Cluster) submit(name rootraft.NodeID, input queuedInput) error {
	process := c.processes[name]
	if !process.up || input.incarnation != process.incarnation {
		return nil
	}
	if process.pending != nil {
		switch input.kind {
		case inputCampaign:
			if process.campaignQueued {
				return nil
			}
			process.campaignQueued = true
		case inputHeartbeat:
			if process.heartbeatQueued {
				return nil
			}
			process.heartbeatQueued = true
		}
		process.mailbox = append(process.mailbox, cloneInput(input))
		return nil
	}
	before := process.raw.BasicStatus()
	if input.kind == inputCampaign {
		process.campaignQueued = false
		if before.RaftState == etcdraft.StateLeader {
			return nil
		}
	}
	if input.kind == inputHeartbeat {
		process.heartbeatQueued = false
		if before.RaftState != etcdraft.StateLeader {
			return nil
		}
	}
	var err error
	switch input.kind {
	case inputMessage:
		err = process.raw.Step(proto.Clone(input.message).(*pb.Message))
	case inputCampaign:
		err = process.raw.Campaign()
	case inputHeartbeat:
		process.raw.Tick()
	case inputProposal:
		err = process.raw.Propose(slices.Clone(input.data))
	default:
		err = errors.New("etcdraft: unknown input")
	}
	if err != nil {
		return err
	}
	if err := c.pump(name); err != nil {
		return err
	}
	after := process.raw.BasicStatus()
	if err := c.syncRoleTimers(name, before.RaftState, after.RaftState); err != nil {
		return err
	}
	switch input.kind {
	case inputCampaign:
		if after.RaftState != etcdraft.StateLeader {
			if err := c.resetElectionTimer(name); err != nil {
				return err
			}
		}
	case inputHeartbeat:
		if after.RaftState == etcdraft.StateLeader {
			if err := c.resetHeartbeatTimer(name); err != nil {
				return err
			}
		}
	case inputMessage:
		if shouldResetElectionTimer(before, after, input.message) {
			if err := c.resetElectionTimer(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func shouldResetElectionTimer(before, after etcdraft.BasicStatus, message *pb.Message) bool {
	if message == nil || after.RaftState == etcdraft.StateLeader {
		return false
	}
	if after.GetTerm() > before.GetTerm() || before.RaftState != etcdraft.StateFollower && after.RaftState == etcdraft.StateFollower {
		return true
	}
	leaderMessage := message.GetType() == pb.MsgApp || message.GetType() == pb.MsgHeartbeat || message.GetType() == pb.MsgSnap
	if leaderMessage && message.GetTerm() >= before.GetTerm() && after.RaftState == etcdraft.StateFollower {
		return true
	}
	return message.GetType() == pb.MsgVote && after.GetTerm() == message.GetTerm() && after.GetVote() == message.GetFrom()
}

func cloneInput(input queuedInput) queuedInput {
	input.data = slices.Clone(input.data)
	if input.message != nil {
		input.message = proto.Clone(input.message).(*pb.Message)
	}
	return input
}

func (c *Cluster) pump(name rootraft.NodeID) error {
	process := c.processes[name]
	for process.up && process.pending == nil && process.raw.HasReady() {
		ready := process.raw.Ready()
		if readyNeedsPersistence(ready) {
			process.persistGen++
			generation, incarnation := process.persistGen, process.incarnation
			delay, err := c.chooseDuration(fmt.Sprintf("storage/%s/%d/%d", name, incarnation, generation), decision.StorageLatency, c.config.StorageLatency, c.config.StorageLatency, map[string]any{"node": name, "incarnation": incarnation, "generation": generation})
			if err != nil {
				return err
			}
			pending := &pendingReady{ready: ready, generation: generation, incarnation: incarnation}
			event, err := c.simulator.Schedule(delay, func(*sim.Simulator) {
				if !process.up || process.incarnation != incarnation || process.pending != pending {
					return
				}
				if persistErr := c.persistReady(process, ready); persistErr != nil {
					c.fail(persistErr)
					return
				}
				pending.persisted = true
				if process.crashAfterWrite {
					process.crashAfterWrite = false
					if crashErr := c.Crash(name); crashErr != nil {
						c.fail(crashErr)
					}
					return
				}
				ack, scheduleErr := c.simulator.Schedule(0, func(*sim.Simulator) {
					if !process.up || process.incarnation != incarnation || process.pending != pending || !pending.persisted {
						return
					}
					if releaseErr := c.releaseReady(process, ready); releaseErr != nil {
						c.fail(releaseErr)
						return
					}
					process.pending = nil
					if pumpErr := c.pump(name); pumpErr != nil {
						c.fail(pumpErr)
						return
					}
					c.drain(name)
				})
				if scheduleErr != nil {
					c.fail(scheduleErr)
					return
				}
				pending.event = ack
			})
			if err != nil {
				return err
			}
			pending.event = event
			process.pending = pending
			return nil
		}
		if err := c.releaseReady(process, ready); err != nil {
			return err
		}
	}
	return nil
}

func readyNeedsPersistence(ready etcdraft.Ready) bool {
	return len(ready.Entries) > 0 || !etcdraft.IsEmptyHardState(ready.HardState) || !etcdraft.IsEmptySnap(ready.Snapshot)
}

func (c *Cluster) persistReady(process *process, ready etcdraft.Ready) error {
	if !readyNeedsPersistence(ready) {
		return ErrPersistenceOrder
	}
	if !etcdraft.IsEmptySnap(ready.Snapshot) {
		if err := process.storage.ApplySnapshot(ready.Snapshot); err != nil {
			return err
		}
	}
	if !etcdraft.IsEmptyHardState(ready.HardState) {
		if err := process.storage.SetHardState(ready.HardState); err != nil {
			return err
		}
	}
	return process.storage.Append(ready.Entries)
}

func (c *Cluster) releaseReady(process *process, ready etcdraft.Ready) error {
	if readyNeedsPersistence(ready) && (process.pending == nil || !process.pending.persisted) {
		return ErrPersistenceOrder
	}
	for _, entry := range ready.CommittedEntries {
		if err := c.applyEntry(process, entry); err != nil {
			return err
		}
	}
	for _, message := range ready.Messages {
		if err := c.send(process, message); err != nil {
			return err
		}
	}
	process.raw.Advance(ready)
	return nil
}

func (c *Cluster) applyEntry(process *process, source *pb.Entry) error {
	entry := normalizeEntry(source)
	if entry.Index <= process.appliedIndex {
		return nil
	}
	if entry.Index != process.appliedIndex+1 {
		return fmt.Errorf("%w: applied index %d after %d", ErrPersistenceOrder, entry.Index, process.appliedIndex)
	}
	if process.chain.Index() != process.appliedIndex {
		return ErrOracleOrder
	}
	application := process.application
	if c.config.Application != nil && application == nil {
		return errors.New("etcdraft: live portable application state is unavailable")
	}
	if application != nil && entry.Type == rootraft.EntryCommand {
		if _, err := application.ApplyEncoded(entry.Data); err != nil {
			return fmt.Errorf("etcdraft: apply portable command: %w", err)
		}
	}
	if err := process.chain.Apply(entry); err != nil {
		return err
	}
	process.appliedIndex = entry.Index
	process.applied = append(process.applied, entry)
	process.application = application
	return nil
}

func recoverApplication(process *process) (*apporacle.Machine, error) {
	application := apporacle.New()
	last := uint64(1)
	for _, entry := range process.applied {
		if last == math.MaxUint64 || entry.Index != last+1 || entry.Index > process.appliedIndex {
			return nil, ErrPersistenceOrder
		}
		if entry.Type == rootraft.EntryCommand {
			if _, err := application.ApplyEncoded(entry.Data); err != nil {
				return nil, err
			}
		} else if entry.Type != rootraft.EntryNoop {
			return nil, fmt.Errorf("%w: portable application entry type %d", apporacle.ErrInvalidEntry, entry.Type)
		}
		last = entry.Index
	}
	if last != process.appliedIndex {
		return nil, ErrPersistenceOrder
	}
	return application, nil
}

func (c *Cluster) send(process *process, source *pb.Message) error {
	to, ok := c.names[source.GetTo()]
	if !ok {
		return fmt.Errorf("%w: numeric target %d", ErrUnknownNode, source.GetTo())
	}
	process.sendSequence++
	message := proto.Clone(source).(*pb.Message)
	_, err := c.router.Send(sim.NodeID(process.name), sim.NodeID(to), envelope{
		SenderIncarnation: process.incarnation, SendSequence: process.sendSequence,
		From: process.name, To: to, Message: message,
	})
	return err
}

func (c *Cluster) receive(name rootraft.NodeID, packet sim.Packet[envelope]) {
	envelope := packet.Message
	process := c.processes[name]
	if envelope.SenderIncarnation == 0 || envelope.SendSequence == 0 || envelope.From != rootraft.NodeID(packet.From) || envelope.To != name || envelope.Message.GetFrom() != c.ids[envelope.From] || envelope.Message.GetTo() != process.id {
		c.fail(errors.New("etcdraft: transport identity mismatch"))
		return
	}
	if !process.up {
		return
	}
	if err := c.submit(name, queuedInput{kind: inputMessage, message: envelope.Message, incarnation: process.incarnation}); err != nil {
		c.fail(err)
		return
	}
}

func (c *Cluster) drain(name rootraft.NodeID) {
	process := c.processes[name]
	for process.up && process.pending == nil && len(process.mailbox) > 0 {
		input := process.mailbox[0]
		process.mailbox = process.mailbox[1:]
		if err := c.submit(name, input); err != nil {
			c.fail(err)
			return
		}
	}
}

func (c *Cluster) resetElectionTimer(name rootraft.NodeID) error {
	process := c.processes[name]
	if process.electionEvent != 0 {
		c.simulator.Cancel(process.electionEvent)
		process.electionEvent = 0
	}
	if !process.up || process.raw.BasicStatus().RaftState == etcdraft.StateLeader {
		return nil
	}
	process.electionGen++
	generation, incarnation := process.electionGen, process.incarnation
	delay, err := c.chooseDuration(fmt.Sprintf("timer/%s/%d/election/%d", name, incarnation, generation), decision.ElectionTimeout, c.config.ElectionTimeoutMin, c.config.ElectionTimeoutMax, map[string]any{"node": name, "incarnation": incarnation, "generation": generation})
	if err != nil {
		return err
	}
	event, err := c.simulator.Schedule(delay, func(*sim.Simulator) {
		if !process.up || process.incarnation != incarnation || process.electionGen != generation || process.raw.BasicStatus().RaftState == etcdraft.StateLeader {
			return
		}
		process.electionEvent = 0
		if submitErr := c.submit(name, queuedInput{kind: inputCampaign, incarnation: incarnation}); submitErr != nil {
			c.fail(submitErr)
		}
	})
	if err != nil {
		return err
	}
	process.electionEvent = event
	return nil
}

func (c *Cluster) resetHeartbeatTimer(name rootraft.NodeID) error {
	process := c.processes[name]
	if process.heartbeatEvent != 0 {
		c.simulator.Cancel(process.heartbeatEvent)
		process.heartbeatEvent = 0
	}
	if !process.up || process.raw.BasicStatus().RaftState != etcdraft.StateLeader {
		return nil
	}
	process.heartbeatGen++
	generation, incarnation := process.heartbeatGen, process.incarnation
	event, err := c.simulator.Schedule(c.config.HeartbeatInterval, func(*sim.Simulator) {
		if !process.up || process.incarnation != incarnation || process.heartbeatGen != generation || process.raw.BasicStatus().RaftState != etcdraft.StateLeader {
			return
		}
		process.heartbeatEvent = 0
		if submitErr := c.submit(name, queuedInput{kind: inputHeartbeat, incarnation: incarnation}); submitErr != nil {
			c.fail(submitErr)
			return
		}
	})
	if err != nil {
		return err
	}
	process.heartbeatEvent = event
	return nil
}

func (c *Cluster) syncRoleTimers(name rootraft.NodeID, before, after etcdraft.StateType) error {
	process := c.processes[name]
	if after == etcdraft.StateLeader {
		if process.electionEvent != 0 {
			c.simulator.Cancel(process.electionEvent)
			process.electionEvent = 0
			process.electionGen++
		}
		if before != etcdraft.StateLeader {
			return c.resetHeartbeatTimer(name)
		}
		return nil
	}
	if process.heartbeatEvent != 0 {
		c.simulator.Cancel(process.heartbeatEvent)
		process.heartbeatEvent = 0
		process.heartbeatGen++
	}
	return nil
}

func (c *Cluster) cancelProcessEvents(process *process) {
	for _, event := range []sim.EventID{process.electionEvent, process.heartbeatEvent} {
		if event != 0 {
			c.simulator.Cancel(event)
		}
	}
	if process.pending != nil && process.pending.event != 0 {
		c.simulator.Cancel(process.pending.event)
	}
	process.electionEvent, process.heartbeatEvent = 0, 0
	process.electionGen++
	process.heartbeatGen++
	process.persistGen++
}

func (c *Cluster) chooseDuration(id string, kind decision.Kind, minimum, maximum time.Duration, context any) (time.Duration, error) {
	if c.config.Decider == nil {
		return minimum, nil
	}
	raw, err := json.Marshal(context)
	if err != nil {
		return 0, err
	}
	minNS, maxNS := int64(minimum), int64(maximum)
	choice := decision.Choice{ID: id, Kind: kind, Min: &minNS, Max: &maxNS, Context: raw}
	selection, err := c.config.Decider.Choose(choice)
	if err != nil {
		return 0, err
	}
	if err := decision.ValidateSelection(choice, selection); err != nil {
		return 0, err
	}
	return time.Duration(*selection.Number), nil
}

func (c *Cluster) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}
