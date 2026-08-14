package etcdraft

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testConfig(members ...rootraft.NodeID) Config {
	return Config{
		Members: members, Seed: 17,
		Network:            NetworkConfig{MinLatency: time.Millisecond, MaxLatency: 3 * time.Millisecond},
		ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second,
		HeartbeatInterval: 20 * time.Millisecond, StorageLatency: 5 * time.Millisecond,
		MaxSteps: 100_000, StopOnViolation: true,
	}
}

func stepUntil(t *testing.T, cluster *Cluster, limit int, condition func() bool) {
	t.Helper()
	for step := 0; step < limit; step++ {
		if condition() {
			return
		}
		ran, err := cluster.Step()
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if !ran {
			t.Fatalf("event queue exhausted after %d steps", step)
		}
	}
	t.Fatalf("condition not reached after %d steps", limit)
}

func TestReadyPersistenceAndAcknowledgementAreSeparate(t *testing.T) {
	cluster, err := New(testConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["a"]
	if err := cluster.submit("a", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	if process.pending == nil || process.pending.persisted {
		t.Fatal("campaign did not create an unpersisted Ready barrier")
	}
	before, err := cluster.durableState(process)
	if err != nil {
		t.Fatal(err)
	}
	if before.HardState.CurrentTerm != 1 || before.HardState.VotedFor != "" {
		t.Fatalf("unexpected genesis hard state: %+v", before.HardState)
	}

	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("persist step: ran=%t err=%v", ran, err)
	}
	if process.pending == nil || !process.pending.persisted {
		t.Fatal("persistence completion released the Ready barrier")
	}
	afterWrite, err := cluster.durableState(process)
	if err != nil {
		t.Fatal(err)
	}
	if afterWrite.HardState.CurrentTerm != 2 || afterWrite.HardState.VotedFor != "a" {
		t.Fatalf("campaign was not durable: %+v", afterWrite.HardState)
	}
	if process.sendSequence != 0 || process.appliedIndex != 1 {
		t.Fatalf("effects escaped before ack: sends=%d applied=%d", process.sendSequence, process.appliedIndex)
	}

	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("ack step: ran=%t err=%v", ran, err)
	}
	if process.pending != nil {
		t.Fatal("ack did not release the Ready barrier")
	}
	if process.sendSequence != 2 {
		t.Fatalf("vote requests after ack = %d, want 2", process.sendSequence)
	}
}

func TestVoteResponseWaitsForPersistenceAcknowledgement(t *testing.T) {
	cluster, err := New(testConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["b"]
	from, to, term, index, logTerm := cluster.ids["a"], process.id, uint64(2), uint64(1), uint64(1)
	message := &pb.Message{Type: pb.MsgVote.Enum(), From: &from, To: &to, Term: &term, Index: &index, LogTerm: &logTerm}
	if err := cluster.submit("b", queuedInput{kind: inputMessage, message: message, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	if process.pending == nil || process.sendSequence != 0 {
		t.Fatal("vote response escaped its Ready barrier")
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("persist step: ran=%t err=%v", ran, err)
	}
	if process.pending == nil || !process.pending.persisted || process.sendSequence != 0 {
		t.Fatal("vote response escaped before acknowledgement")
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("ack step: ran=%t err=%v", ran, err)
	}
	if process.sendSequence != 1 {
		t.Fatalf("vote responses after ack = %d, want 1", process.sendSequence)
	}
}

func TestObservationDigestIncludesReadyPhaseAndMailbox(t *testing.T) {
	cluster, err := New(testConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["a"]
	if err := cluster.submit("a", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	unpersisted, err := cluster.SnapshotDigest()
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.submit("a", queuedInput{kind: inputProposal, data: []byte("queued"), incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	withMailbox, err := cluster.SnapshotDigest()
	if err != nil {
		t.Fatal(err)
	}
	if withMailbox == unpersisted {
		t.Fatal("mailbox did not affect observation digest")
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("persist step: ran=%t err=%v", ran, err)
	}
	persisted, err := cluster.SnapshotDigest()
	if err != nil {
		t.Fatal(err)
	}
	if persisted == withMailbox {
		t.Fatal("persistence phase did not affect observation digest")
	}
}

func TestTimerInputsQueueWithoutEarlyResetAndCoalesce(t *testing.T) {
	cluster, err := New(testConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["a"]
	if err := cluster.submit("a", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	generation, event := process.electionGen, process.electionEvent
	for range 3 {
		if err := cluster.submit("a", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
			t.Fatal(err)
		}
	}
	if process.electionGen != generation || process.electionEvent != event {
		t.Fatal("queued campaign reset the semantic election timer")
	}
	if len(process.mailbox) != 1 || !process.campaignQueued {
		t.Fatalf("campaigns were not coalesced: mailbox=%d queued=%t", len(process.mailbox), process.campaignQueued)
	}
}

func TestQueuedLeaderMessageResetsTimerExactlyWhenProcessed(t *testing.T) {
	cluster, err := New(testConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["b"]
	if err := cluster.submit("b", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	generation := process.electionGen
	from, to, term, index, logTerm, commit := cluster.ids["a"], process.id, uint64(2), uint64(1), uint64(1), uint64(1)
	message := &pb.Message{Type: pb.MsgApp.Enum(), From: &from, To: &to, Term: &term, Index: &index, LogTerm: &logTerm, Commit: &commit}
	if err := cluster.submit("b", queuedInput{kind: inputMessage, message: message, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	if process.electionGen != generation {
		t.Fatal("queued leader message reset timer before processing")
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("persist step: ran=%t err=%v", ran, err)
	}
	if process.electionGen != generation {
		t.Fatal("persistence completion reset timer before queued input processing")
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("ack/drain step: ran=%t err=%v", ran, err)
	}
	if process.electionGen != generation+1 {
		t.Fatalf("processed leader message reset count = %d, want one", process.electionGen-generation)
	}
}

func TestStaleLeaderMessageDoesNotResetElectionTimer(t *testing.T) {
	cluster, err := New(testConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["b"]
	from, to := cluster.ids["a"], process.id
	vote := &pb.Message{Type: pb.MsgVote.Enum(), From: &from, To: &to, Term: new(uint64), Index: new(uint64), LogTerm: new(uint64)}
	*vote.Term = 2
	*vote.Index = 1
	*vote.LogTerm = 1
	if err := cluster.submit("b", queuedInput{kind: inputMessage, message: vote, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 100, func() bool { return process.pending == nil && process.raw.BasicStatus().GetTerm() == 2 })
	generation, event := process.electionGen, process.electionEvent
	stale := &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: &from, To: &to, Term: new(uint64)}
	*stale.Term = 1
	if err := cluster.submit("b", queuedInput{kind: inputMessage, message: stale, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	if process.electionGen != generation || process.electionEvent != event {
		t.Fatalf("stale heartbeat reset election timer: generation %d -> %d", generation, process.electionGen)
	}
}

func TestCrashBeforePersistenceLosesPendingWrite(t *testing.T) {
	cluster, err := New(testConfig("a"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["a"]
	if err := cluster.submit("a", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Crash("a"); err != nil {
		t.Fatal(err)
	}
	durable, err := cluster.durableState(process)
	if err != nil {
		t.Fatal(err)
	}
	if durable.HardState.CurrentTerm != 1 || durable.HardState.VotedFor != "" || len(durable.Log) != 0 {
		t.Fatalf("pending write survived crash: %+v", durable)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatal(err)
	}
	if process.appliedIndex != 1 || process.chain.Index() != 1 {
		t.Fatalf("restart applied lost data: applied=%d chain=%d", process.appliedIndex, process.chain.Index())
	}
}

func TestCrashAfterWriteBeforeAckRecoversDurableEntry(t *testing.T) {
	cluster, err := New(testConfig("a"))
	if err != nil {
		t.Fatal(err)
	}
	process := cluster.processes["a"]
	if err := cluster.submit("a", queuedInput{kind: inputCampaign, incarnation: process.incarnation}); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 100, func() bool {
		leader, ok := cluster.Leader()
		return ok && leader == "a" && process.pending == nil && process.appliedIndex == 2
	})
	if err := cluster.Propose([]byte("survives-crash")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.CrashAfterNextPersist("a"); err != nil {
		t.Fatal(err)
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("persist/crash step: ran=%t err=%v", ran, err)
	}
	if process.up {
		t.Fatal("node did not crash after durable write")
	}
	if process.appliedIndex != 2 || process.chain.Index() != 2 || process.sendSequence != 0 {
		t.Fatalf("effects escaped before crash: applied=%d chain=%d sends=%d", process.appliedIndex, process.chain.Index(), process.sendSequence)
	}
	durable, err := cluster.durableState(process)
	if err != nil {
		t.Fatal(err)
	}
	if durable.HardState.CurrentTerm != 2 || durable.HardState.VotedFor != "a" || durable.HardState.CommitIndex != 2 || len(durable.Log) != 2 {
		t.Fatalf("durable command missing: %+v", durable)
	}

	if err := cluster.Restart("a"); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		for _, entry := range process.applied {
			if entry.Type == rootraft.EntryCommand && slices.Equal(entry.Data, []byte("survives-crash")) {
				return true
			}
		}
		return false
	})
	commands := 0
	for _, entry := range process.applied {
		if entry.Type == rootraft.EntryCommand && slices.Equal(entry.Data, []byte("survives-crash")) {
			commands++
		}
	}
	if commands != 1 {
		t.Fatalf("recovered command applications = %d, want 1", commands)
	}
	if err := verifyChain(process); err != nil {
		t.Fatal(err)
	}
}

func TestElectionProposalReplicationAndChainAgreement(t *testing.T) {
	config := testConfig("a", "b", "c")
	config.ElectionTimeoutMin = 100 * time.Millisecond
	config.ElectionTimeoutMax = 250 * time.Millisecond
	cluster, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	var leader rootraft.NodeID
	stepUntil(t, cluster, 10_000, func() bool {
		var ok bool
		leader, ok = cluster.Leader()
		return ok && cluster.processes[leader].pending == nil
	})
	if err := cluster.Propose([]byte("command-1")); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 10_000, func() bool {
		for _, name := range cluster.members {
			found := false
			for _, entry := range cluster.processes[name].applied {
				if entry.Type == rootraft.EntryCommand && slices.Equal(entry.Data, []byte("command-1")) {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
	digest := cluster.processes[cluster.members[0]].chain.Digest()
	for _, name := range cluster.members {
		process := cluster.processes[name]
		if process.chain.Digest() != digest {
			t.Fatalf("chain mismatch on %s", name)
		}
		if err := verifyChain(process); err != nil {
			t.Fatalf("chain verification on %s: %v", name, err)
		}
	}
	if violations := cluster.Violations(); len(violations) != 0 {
		t.Fatalf("unexpected violations: %+v", violations)
	}
}

func TestInspectionAPIsReturnDeepCopies(t *testing.T) {
	config := testConfig("a", "b", "c")
	config.ElectionTimeoutMin = 100 * time.Millisecond
	config.ElectionTimeoutMax = 250 * time.Millisecond
	cluster, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	var leader rootraft.NodeID
	stepUntil(t, cluster, 10_000, func() bool {
		var ok bool
		leader, ok = cluster.Leader()
		return ok && cluster.processes[leader].pending == nil
	})
	if err := cluster.Propose([]byte("immutable")); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 10_000, func() bool {
		entries, _ := cluster.AppliedEntries(leader)
		for _, entry := range entries {
			if entry.Type == rootraft.EntryCommand {
				return true
			}
		}
		return false
	})
	state, err := cluster.DurableState(leader)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := cluster.AppliedEntries(leader)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := cluster.ChainBlocks(leader)
	if err != nil {
		t.Fatal(err)
	}
	state.Log[len(state.Log)-1].Data[0] = 'X'
	entries[len(entries)-1].Data[0] = 'X'
	blocks[len(blocks)-1].Digest = "mutated"
	againState, _ := cluster.DurableState(leader)
	againEntries, _ := cluster.AppliedEntries(leader)
	againBlocks, _ := cluster.ChainBlocks(leader)
	if slices.Equal(againState.Log[len(againState.Log)-1].Data, state.Log[len(state.Log)-1].Data) || slices.Equal(againEntries[len(againEntries)-1].Data, entries[len(entries)-1].Data) || againBlocks[len(againBlocks)-1].Digest == "mutated" {
		t.Fatal("inspection mutation leaked into cluster state")
	}
	status, err := cluster.Status(leader)
	if err != nil {
		t.Fatal(err)
	}
	status.Log[len(status.Log)-1].Data[0] = 'Y'
	againStatus, _ := cluster.Status(leader)
	if againStatus.Log[len(againStatus.Log)-1].Data[0] == 'Y' {
		t.Fatal("status mutation leaked into cluster state")
	}
}

func TestEmptyProposalFailsClosed(t *testing.T) {
	cluster, err := New(testConfig("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ProposeTo("a", nil); !errors.Is(err, ErrInvalidProposal) {
		t.Fatalf("ProposeTo error = %v, want ErrInvalidProposal", err)
	}
}

func TestPartitionHealCatchesFollowerUp(t *testing.T) {
	config := testConfig("a", "b", "c")
	config.ElectionTimeoutMin = 100 * time.Millisecond
	config.ElectionTimeoutMax = 250 * time.Millisecond
	cluster, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	var leader rootraft.NodeID
	stepUntil(t, cluster, 10_000, func() bool {
		var ok bool
		leader, ok = cluster.Leader()
		return ok && cluster.processes[leader].pending == nil
	})
	var isolated, majorityPeer rootraft.NodeID
	for _, name := range cluster.members {
		if name == leader {
			continue
		}
		if isolated == "" {
			isolated = name
		} else {
			majorityPeer = name
		}
	}
	if err := cluster.Partition([]rootraft.NodeID{isolated}, []rootraft.NodeID{leader, majorityPeer}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.ProposeTo(leader, []byte("during-partition")); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 10_000, func() bool {
		for _, entry := range cluster.processes[leader].applied {
			if entry.Type == rootraft.EntryCommand && slices.Equal(entry.Data, []byte("during-partition")) {
				return true
			}
		}
		return false
	})
	for _, entry := range cluster.processes[isolated].applied {
		if entry.Type == rootraft.EntryCommand && slices.Equal(entry.Data, []byte("during-partition")) {
			t.Fatal("isolated follower applied command before heal")
		}
	}
	cluster.Heal()
	stepUntil(t, cluster, 20_000, func() bool {
		for _, entry := range cluster.processes[isolated].applied {
			if entry.Type == rootraft.EntryCommand && slices.Equal(entry.Data, []byte("during-partition")) {
				return true
			}
		}
		return false
	})
	if violations := cluster.Violations(); len(violations) != 0 {
		t.Fatalf("unexpected violations: %+v", violations)
	}
}

func TestExecuteIsExactlyReplayable(t *testing.T) {
	configuration := artifact.Configuration{
		Members: []rootraft.NodeID{"a", "b", "c"}, InfrastructureSeed: 23,
		NetworkMinLatencyNS: int64(time.Millisecond), NetworkMaxLatencyNS: int64(3 * time.Millisecond),
		ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(250 * time.Millisecond),
		HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: int64(5 * time.Millisecond), StopOnViolation: true,
	}
	scenario := artifact.Scenario{ID: "etcdraft-replay", Version: "1", DurationNS: int64(time.Second), MaxSteps: 100_000}
	recorder := decision.NewRecorder(decision.NewSeedDecider(41))
	first, err := Execute(scenario, configuration, recorder)
	if err != nil {
		t.Fatal(err)
	}
	if recorder.Err() != nil {
		t.Fatal(recorder.Err())
	}
	replay, err := decision.NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatal(err)
	}
	second, err := Execute(scenario, configuration, replay)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(first, second) {
		t.Fatalf("replay mismatch:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if first.Status != artifact.OutcomeCompleted || len(first.Violations) != 0 {
		t.Fatalf("unexpected outcome: %+v", first)
	}

	recorderAgain := decision.NewRecorder(decision.NewSeedDecider(41))
	third, err := Execute(scenario, configuration, recorderAgain)
	if err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(first, third) || !reflect.DeepEqual(recorder.Tape(), recorderAgain.Tape()) {
		t.Fatal("same seed did not reproduce tape and outcome")
	}
}

func TestExecuteRejectsUnsupportedCapabilitiesBeforeChoices(t *testing.T) {
	configuration := artifact.Configuration{
		Members: []rootraft.NodeID{"a"}, InfrastructureSeed: 1,
		NetworkMinLatencyNS: 0, NetworkMaxLatencyNS: 0,
		ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
		HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: 0,
	}
	scenario := artifact.Scenario{ID: "unsupported", Version: "1", DurationNS: int64(time.Second), MaxSteps: 100, Actions: []artifact.Action{{AtNS: int64(500 * time.Millisecond), Kind: artifact.ActionSnapshot, Node: "a"}}}
	recorder := decision.NewRecorder(decision.NewSeedDecider(2))
	_, err := Execute(scenario, configuration, recorder)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Execute error = %v, want ErrUnsupported", err)
	}
	if len(recorder.Tape().Entries) != 0 {
		t.Fatal("unsupported scenario consumed decisions")
	}
}
