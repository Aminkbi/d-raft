package raftsim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
)

func TestCanonicalStateIsDeterministicAndMapOrderIndependent(t *testing.T) {
	t.Parallel()

	build := func(reverse bool) []byte {
		t.Helper()
		config := DefaultConfig("a", "b", "c")
		config.Decider = decision.NewSeedDecider(7)
		cluster, err := NewPaused(config)
		if err != nil {
			t.Fatal(err)
		}
		links := [][2]sim.NodeID{{"a", "b"}, {"b", "c"}}
		if reverse {
			links[0], links[1] = links[1], links[0]
		}
		for _, link := range links {
			if err := cluster.Router().SetLink(link[0], link[1], sim.LinkConfig{MinLatency: time.Millisecond, MaxLatency: 2 * time.Millisecond, LossProbability: .25}); err != nil {
				t.Fatal(err)
			}
		}
		if err := cluster.Bootstrap(); err != nil {
			t.Fatal(err)
		}
		raw, err := cluster.CanonicalState()
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(raw) {
			t.Fatal("canonical state is not JSON")
		}
		return raw
	}
	left := build(false)
	if !bytes.Equal(left, build(true)) {
		t.Fatal("map insertion order changed canonical bytes")
	}
}

func TestCanonicalStateDistinguishesPendingEventOrder(t *testing.T) {
	t.Parallel()

	build := func(reverse bool) []byte {
		t.Helper()
		config := DefaultConfig("a")
		config.Decider = decision.NewSeedDecider(1)
		cluster, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		if reverse {
			if _, err := cluster.ScheduleRestart(time.Second, "a"); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.ScheduleCrash(time.Second, "a"); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := cluster.ScheduleCrash(time.Second, "a"); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.ScheduleRestart(time.Second, "a"); err != nil {
				t.Fatal(err)
			}
		}
		raw, err := cluster.CanonicalState()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if bytes.Equal(build(false), build(true)) {
		t.Fatal("future event order was erased")
	}
}

func TestViolationsAreDeepCopiedAndMatchCanonicalState(t *testing.T) {
	t.Parallel()

	config := DefaultConfig("a")
	config.Decider = decision.NewSeedDecider(1)
	cluster, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	cluster.violations = []check.Violation{{ID: check.ElectionSafety, Nodes: []raft.NodeID{"a"}, Evidence: []byte(`{"term":1}`), Fingerprint: "test"}}
	before, err := cluster.CanonicalState()
	if err != nil {
		t.Fatal(err)
	}
	copy := cluster.Violations()
	copy[0].Nodes[0] = "mutated"
	copy[0].Evidence[0] = 'x'
	after, err := cluster.CanonicalState()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || cluster.violations[0].Nodes[0] != "a" || cluster.violations[0].Evidence[0] != '{' {
		t.Fatal("caller mutation escaped violation snapshot")
	}
}

func TestClusterElectsAndReplicates(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, testConfig("a", "b", "c"))
	runUntil(t, cluster, 2*time.Second)
	leader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no unique leader: %+v", cluster.Statuses())
	}
	if err := cluster.Propose([]byte("set x=1")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+time.Second)

	for _, id := range cluster.Members() {
		store, err := cluster.Store(id)
		if err != nil {
			t.Fatalf("Store(%q): %v", id, err)
		}
		if store.State.HardState.CommitIndex < 2 || store.AppliedIndex < 2 {
			t.Fatalf("node %q did not commit/apply: %+v", id, store)
		}
		if len(store.Applied) < 2 || store.Applied[0].Type != raft.EntryNoop || string(store.Applied[1].Data) != "set x=1" {
			t.Fatalf("node %q applied entries = %+v", id, store.Applied)
		}
	}
	if status, err := cluster.Status(leader); err != nil || status.Role != raft.Leader {
		t.Fatalf("leader %q status=%+v err=%v", leader, status, err)
	}
}

func TestPortableApplicationIsOptInAndPreservesLegacyPayloads(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, singleNodeCrashConfig())
	runUntil(t, cluster, 40*time.Millisecond)
	if _, err := cluster.ApplicationCommitment("a"); !errors.Is(err, ErrApplicationDisabled) {
		t.Fatalf("ApplicationCommitment error = %v", err)
	}
	if err := cluster.SnapshotApplication("a"); !errors.Is(err, ErrApplicationDisabled) {
		t.Fatalf("SnapshotApplication error = %v", err)
	}
	proposal := []byte("legacy arbitrary proposal")
	if err := cluster.Propose(proposal); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	checkpoint := []byte("legacy arbitrary snapshot")
	if err := cluster.Snapshot("a", checkpoint); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+20*time.Millisecond)
	store, err := cluster.Store("a")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(store.State.Snapshot.Data, checkpoint) || len(store.Applied) < 2 || !bytes.Equal(store.Applied[len(store.Applied)-1].Data, proposal) {
		t.Fatalf("legacy snapshot/proposal changed semantics: %+v", store)
	}
}

func TestPortableApplicationRejectsMalformedProposalWithoutMutation(t *testing.T) {
	t.Parallel()

	config := applicationConfig(singleNodeCrashConfig())
	cluster := mustCluster(t, config)
	runUntil(t, cluster, 40*time.Millisecond)
	beforeCommitment, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	beforeStore, err := cluster.Store("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Propose([]byte("not a portable command")); !errors.Is(err, apporacle.ErrInvalidCommand) {
		t.Fatalf("Propose error = %v", err)
	}
	afterCommitment, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	afterStore, err := cluster.Store("a")
	if err != nil {
		t.Fatal(err)
	}
	if afterCommitment != beforeCommitment || !entriesEqual(afterStore.State.Log, beforeStore.State.Log) || afterStore.AppliedIndex != beforeStore.AppliedIndex {
		t.Fatalf("malformed proposal mutated state: before=%+v/%+v after=%+v/%+v", beforeCommitment, beforeStore, afterCommitment, afterStore)
	}
	if err := cluster.Snapshot("a", []byte("opaque")); !errors.Is(err, ErrApplicationSnapshot) {
		t.Fatalf("opaque Snapshot error = %v", err)
	}
}

func TestPortableApplicationCommandsConverge(t *testing.T) {
	t.Parallel()

	config := applicationConfig(testConfig("a", "b", "c"))
	cluster := mustCluster(t, config)
	runUntil(t, cluster, 500*time.Millisecond)
	commands := [][]byte{
		portablePut(t, 1, []byte("x"), []byte("1")),
		portablePut(t, 17, []byte("y"), []byte("2")),
	}
	for _, command := range commands {
		if err := cluster.Propose(command); err != nil {
			t.Fatal(err)
		}
		runUntil(t, cluster, cluster.Simulator().Now()+200*time.Millisecond)
	}

	var want apporacle.Commitment
	for index, id := range cluster.Members() {
		commitment, err := cluster.ApplicationCommitment(id)
		if err != nil {
			t.Fatalf("ApplicationCommitment(%q): %v", id, err)
		}
		if commitment.Schema != apporacle.CommitmentSchema || commitment.Commands != 2 {
			t.Fatalf("node %q commitment = %+v", id, commitment)
		}
		if index == 0 {
			want = commitment
		} else if commitment != want {
			t.Fatalf("application commitments did not converge: node %q=%+v want=%+v", id, commitment, want)
		}
	}
	if _, err := cluster.ApplicationCommitment("unknown"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("unknown node commitment error = %v", err)
	}
}

func TestPortableApplicationSurvivesCrashAndRestart(t *testing.T) {
	t.Parallel()

	config := applicationConfig(singleNodeCrashConfig())
	cluster := mustCluster(t, config)
	runUntil(t, cluster, 40*time.Millisecond)
	if err := cluster.Propose(portablePut(t, 1, []byte("x"), []byte("1"))); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	before, err := cluster.ApplicationCommitment("a")
	if err != nil || before.Commands != 1 {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	if err := cluster.Crash("a"); err != nil {
		t.Fatal(err)
	}
	if cluster.processes["a"].application != nil {
		t.Fatal("crash retained volatile portable application pointer")
	}
	down, err := cluster.ApplicationCommitment("a")
	if err != nil || down != before {
		t.Fatalf("down commitment=%+v err=%v, want %+v", down, err, before)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatal(err)
	}
	afterRestart, err := cluster.ApplicationCommitment("a")
	if err != nil || afterRestart != before {
		t.Fatalf("restart commitment=%+v err=%v, want %+v", afterRestart, err, before)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+50*time.Millisecond)
	if err := cluster.Propose(portablePut(t, 17, []byte("y"), []byte("2"))); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	continued, err := cluster.ApplicationCommitment("a")
	if err != nil || continued.Commands != 2 || continued.ChainDigest == before.ChainDigest {
		t.Fatalf("continued=%+v err=%v before=%+v", continued, err, before)
	}
}

func TestPortableApplicationSnapshotInstallsAndRestores(t *testing.T) {
	t.Parallel()

	config := applicationConfig(testConfig("a", "b", "c"))
	cluster := mustCluster(t, config)
	runUntil(t, cluster, 500*time.Millisecond)
	leader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no leader: %+v", cluster.Statuses())
	}
	var lagger raft.NodeID
	for _, id := range cluster.Members() {
		if id != leader {
			lagger = id
			break
		}
	}
	if err := cluster.Crash(lagger); err != nil {
		t.Fatal(err)
	}
	for index, value := range []string{"one", "two", "three"} {
		if err := cluster.Propose(portablePut(t, byte(index*16+1), []byte{byte('a' + index)}, []byte(value))); err != nil {
			t.Fatal(err)
		}
		runUntil(t, cluster, cluster.Simulator().Now()+150*time.Millisecond)
	}
	if err := cluster.SnapshotApplication(leader); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	leaderCommitment, err := cluster.ApplicationCommitment(leader)
	if err != nil || leaderCommitment.Commands != 3 {
		t.Fatalf("leader commitment=%+v err=%v", leaderCommitment, err)
	}
	if err := cluster.Restart(lagger); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+2*time.Second)
	laggerCommitment, err := cluster.ApplicationCommitment(lagger)
	if err != nil || laggerCommitment != leaderCommitment {
		t.Fatalf("lagger commitment=%+v err=%v, leader=%+v", laggerCommitment, err, leaderCommitment)
	}
	store, err := cluster.Store(lagger)
	if err != nil {
		t.Fatal(err)
	}
	if store.InstalledSnapshot.LastIncludedIndex == 0 || len(store.InstalledSnapshot.Data) == 0 {
		t.Fatalf("portable snapshot was not installed: %+v", store.InstalledSnapshot)
	}
	checkpoint, err := apporacle.DecodeCheckpoint(store.InstalledSnapshot.Data)
	if err != nil {
		t.Fatalf("DecodeCheckpoint: %v", err)
	}
	restored, err := apporacle.Restore(checkpoint)
	if err != nil || restored.Commitment() != leaderCommitment {
		t.Fatalf("restored commitment=%+v err=%v, want %+v", restored.Commitment(), err, leaderCommitment)
	}
	if err := cluster.Propose(portablePut(t, 65, []byte("suffix"), []byte("after-snapshot"))); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+300*time.Millisecond)
	wantAfterSuffix, err := cluster.ApplicationCommitment(leader)
	if err != nil || wantAfterSuffix.Commands != 4 {
		t.Fatalf("suffix commitment=%+v err=%v", wantAfterSuffix, err)
	}
	if err := cluster.Crash(lagger); err != nil {
		t.Fatal(err)
	}
	if cluster.processes[lagger].application != nil {
		t.Fatal("crash retained snapshot-restored application pointer")
	}
	if err := cluster.Restart(lagger); err != nil {
		t.Fatal(err)
	}
	gotAfterRestart, err := cluster.ApplicationCommitment(lagger)
	if err != nil || gotAfterRestart != wantAfterSuffix {
		t.Fatalf("checkpoint+suffix recovery=%+v err=%v, want %+v", gotAfterRestart, err, wantAfterSuffix)
	}
}

func TestPortableApplicationRecoversSnapshotPersistedBeforeAcknowledgement(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, applicationConfig(singleNodeCrashConfig()))
	runUntil(t, cluster, 40*time.Millisecond)
	if err := cluster.Propose(portablePut(t, 1, []byte("durable"), []byte("snapshot"))); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	want, err := cluster.ApplicationCommitment("a")
	if err != nil || want.Commands != 1 {
		t.Fatalf("before snapshot=%+v err=%v", want, err)
	}
	if err := cluster.CrashAfterNextPersist("a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SnapshotApplication("a"); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+15*time.Millisecond)
	if cluster.processes["a"].up || cluster.processes["a"].application != nil {
		t.Fatal("snapshot persistence boundary did not crash and clear volatile application state")
	}
	store, err := cluster.Store("a")
	if err != nil || store.State.Snapshot.LastIncludedIndex != store.AppliedIndex || len(store.State.Snapshot.Data) == 0 {
		t.Fatalf("durable snapshot store=%+v err=%v", store, err)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatal(err)
	}
	got, err := cluster.ApplicationCommitment("a")
	if err != nil || got != want {
		t.Fatalf("snapshot restart commitment=%+v err=%v, want %+v", got, err, want)
	}
}

func TestPortableApplicationDisablesCanonicalState(t *testing.T) {
	t.Parallel()

	config := applicationConfig(testConfig("a", "b", "c"))
	cluster, err := NewPaused(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CanonicalState(); !errors.Is(err, ErrUncacheableState) {
		t.Fatalf("CanonicalState before bootstrap error = %v", err)
	}
	if err := cluster.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CanonicalState(); !errors.Is(err, ErrUncacheableState) {
		t.Fatalf("CanonicalState after bootstrap error = %v", err)
	}
}

func TestPartitionElectsMajorityLeaderAndHeals(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, testConfig("a", "b", "c"))
	runUntil(t, cluster, 2*time.Second)
	oldLeader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no initial leader: %+v", cluster.Statuses())
	}
	majority := make([]raft.NodeID, 0, 2)
	for _, id := range cluster.Members() {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}
	if err := cluster.Partition([]raft.NodeID{oldLeader}, majority); err != nil {
		t.Fatalf("Partition: %v", err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+2*time.Second)

	majorityHasLeader := false
	for _, id := range majority {
		status, err := cluster.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		majorityHasLeader = majorityHasLeader || status.Role == raft.Leader
	}
	if !majorityHasLeader {
		t.Fatalf("majority failed to elect: %+v", cluster.Statuses())
	}

	cluster.Heal()
	runUntil(t, cluster, cluster.Simulator().Now()+time.Second)
	if _, ok := cluster.Leader(); !ok {
		t.Fatalf("cluster did not converge after healing: %+v", cluster.Statuses())
	}
}

func TestCrashBeforePersistenceLosesTransition(t *testing.T) {
	t.Parallel()

	config := singleNodeCrashConfig()
	cluster := mustCluster(t, config)
	if _, err := cluster.ScheduleCrash(15*time.Millisecond, "a"); err != nil {
		t.Fatalf("ScheduleCrash: %v", err)
	}
	runUntil(t, cluster, 15*time.Millisecond)
	store, _ := cluster.Store("a")
	if store.State.HardState.CurrentTerm != 0 || len(store.State.Log) != 0 {
		t.Fatalf("uncompleted transition became durable: %+v", store)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	runUntil(t, cluster, 45*time.Millisecond)
	status, err := cluster.Status("a")
	if err != nil || status.Role != raft.Leader || status.Term != 1 {
		t.Fatalf("restarted status=%+v err=%v", status, err)
	}
}

func TestCrashAfterPersistenceRetainsTransitionButReleasesNoEffects(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, singleNodeCrashConfig())
	if err := cluster.CrashAfterNextPersist("a"); err != nil {
		t.Fatalf("CrashAfterNextPersist: %v", err)
	}
	runUntil(t, cluster, 25*time.Millisecond)
	store, _ := cluster.Store("a")
	if store.State.HardState.CurrentTerm != 1 || store.State.HardState.VotedFor != "a" || len(store.State.Log) != 1 {
		t.Fatalf("completed transition was lost: %+v", store)
	}
	if store.AppliedIndex != 0 {
		t.Fatalf("post-persist effect escaped before acknowledgement: %+v", store)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	store, _ = cluster.Store("a")
	if store.AppliedIndex != 1 || len(store.Applied) != 1 || store.Applied[0].Type != raft.EntryNoop {
		t.Fatalf("restart did not recover committed apply: %+v", store)
	}
}

func TestClusterSeedReproducesStateAndTrace(t *testing.T) {
	t.Parallel()

	run := func() ([]raft.Status, []byte) {
		var output bytes.Buffer
		config := testConfig("a", "b", "c")
		recorder := sim.NewJSONLRecorder(&output)
		config.Trace = recorder
		cluster := mustCluster(t, config)
		runUntil(t, cluster, 2*time.Second)
		if err := recorder.Err(); err != nil {
			t.Fatalf("trace: %v", err)
		}
		return cluster.Statuses(), slices.Clone(output.Bytes())
	}
	leftStatus, leftTrace := run()
	rightStatus, rightTrace := run()
	if !statusesEqual(leftStatus, rightStatus) || !bytes.Equal(leftTrace, rightTrace) {
		t.Fatal("equal seeds produced different cluster executions")
	}
}

func TestDecisionTapeReplaysIndependentlyOfClusterSeed(t *testing.T) {
	t.Parallel()

	var originalTrace bytes.Buffer
	recorder := decision.NewRecorder(decision.NewSeedDecider(12345))
	config := testConfig("a", "b", "c")
	config.Decider = recorder
	config.Trace = sim.NewJSONLRecorder(&originalTrace)
	original := mustCluster(t, config)
	runUntil(t, original, 2*time.Second)
	if err := recorder.Err(); err != nil {
		t.Fatalf("record decisions: %v", err)
	}

	replay, err := decision.NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatalf("NewTapeDecider: %v", err)
	}
	replayConfig := testConfig("a", "b", "c")
	replayConfig.Seed = 999999 // Infrastructure RNG draws must not drive this run.
	replayConfig.Decider = replay
	var replayTrace bytes.Buffer
	replayConfig.Trace = sim.NewJSONLRecorder(&replayTrace)
	replayed := mustCluster(t, replayConfig)
	runUntil(t, replayed, 2*time.Second)
	if err := replay.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if !statusesEqual(original.Statuses(), replayed.Statuses()) {
		t.Fatalf("original=%+v replayed=%+v", original.Statuses(), replayed.Statuses())
	}
	if !bytes.Equal(originalTrace.Bytes(), replayTrace.Bytes()) {
		t.Fatal("tape replay produced a different observational trace")
	}
	for _, id := range original.Members() {
		left, _ := original.Store(id)
		right, _ := replayed.Store(id)
		if left.AppliedIndex != right.AppliedIndex || !entriesEqual(left.State.Log, right.State.Log) || !entriesEqual(left.Applied, right.Applied) {
			t.Fatalf("node %q original=%+v replayed=%+v", id, left, right)
		}
	}
}

func TestNetworkDecisionContextExcludesIncidentalPacketState(t *testing.T) {
	t.Parallel()

	link := sim.LinkConfig{MinLatency: time.Millisecond, MaxLatency: 2 * time.Millisecond, LossProbability: 0.5}
	envelope := Envelope{SenderIncarnation: 3, SendSequence: 7, Message: raft.Message{Type: raft.AppendEntries, From: "a", To: "b", Term: 4}}
	first := sim.Packet[Envelope]{ID: 1, From: "a", To: "b", Message: envelope, SentAt: time.Second}
	second := first
	second.ID = 999
	second.SentAt = 42 * time.Second

	recorder := decision.NewRecorder(decision.NewSeedDecider(5))
	if _, err := (raftNetworkDecisions{decider: recorder}).Drop(first, link); err != nil {
		t.Fatal(err)
	}
	replay, err := decision.NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (raftNetworkDecisions{decider: replay}).Drop(second, link); err != nil {
		t.Fatalf("incidental packet state changed context: %v", err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterRejectsSpoofedTransportIdentity(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, testConfig("a", "b", "c"))
	_, err := cluster.Router().Send("a", "c", Envelope{Message: raft.Message{Type: raft.RequestVote, From: "b", To: "c", Term: 1}})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := cluster.RunUntil(10 * time.Millisecond); !errors.Is(err, ErrTransportIdentity) {
		t.Fatalf("transport error = %v", err)
	}
}

func TestClusterPersistsLocalSnapshotAndCompacts(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, singleNodeCrashConfig())
	runUntil(t, cluster, 40*time.Millisecond)
	if err := cluster.Propose([]byte("one")); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	if err := cluster.Snapshot("a", []byte("state@2")); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+20*time.Millisecond)
	store, err := cluster.Store("a")
	if err != nil {
		t.Fatal(err)
	}
	if store.State.Snapshot.LastIncludedIndex != 2 || len(store.State.Log) != 0 || store.AppliedIndex != 2 || string(store.State.Snapshot.Data) != "state@2" {
		t.Fatalf("store = %+v", store)
	}
	if err := cluster.Crash("a"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatal(err)
	}
	status, err := cluster.Status("a")
	if err != nil || status.Snapshot.LastIncludedIndex != 2 || status.LastLogIndex != 2 {
		t.Fatalf("restart status=%+v err=%v", status, err)
	}
}

func TestQueuedSnapshotKeepsCheckpointBoundary(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, singleNodeCrashConfig())
	runUntil(t, cluster, 40*time.Millisecond)
	before, err := cluster.Store("a")
	if err != nil || before.AppliedIndex != 1 {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	if err := cluster.Propose([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Snapshot("a", []byte("state@1")); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+30*time.Millisecond)
	store, err := cluster.Store("a")
	if err != nil {
		t.Fatal(err)
	}
	if store.State.Snapshot.LastIncludedIndex != 1 || string(store.State.Snapshot.Data) != "state@1" || len(store.State.Log) != 1 || store.State.Log[0].Index != 2 || store.AppliedIndex != 2 {
		t.Fatalf("store = %+v", store)
	}
}

func TestSnapshotCrashBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("before durable completion", func(t *testing.T) {
		cluster := mustCluster(t, singleNodeCrashConfig())
		runUntil(t, cluster, 40*time.Millisecond)
		if err := cluster.Snapshot("a", []byte("state@1")); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Crash("a"); err != nil {
			t.Fatal(err)
		}
		store, _ := cluster.Store("a")
		if store.State.Snapshot.LastIncludedIndex != 0 {
			t.Fatalf("snapshot persisted before completion: %+v", store.State.Snapshot)
		}
	})

	t.Run("after persistence before acknowledgement", func(t *testing.T) {
		cluster := mustCluster(t, singleNodeCrashConfig())
		runUntil(t, cluster, 40*time.Millisecond)
		if err := cluster.CrashAfterNextPersist("a"); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Snapshot("a", []byte("state@1")); err != nil {
			t.Fatal(err)
		}
		runUntil(t, cluster, cluster.Simulator().Now()+15*time.Millisecond)
		store, _ := cluster.Store("a")
		if store.State.Snapshot.LastIncludedIndex != 1 || string(store.State.Snapshot.Data) != "state@1" {
			t.Fatalf("durable snapshot = %+v", store.State.Snapshot)
		}
		if err := cluster.Restart("a"); err != nil {
			t.Fatal(err)
		}
		status, err := cluster.Status("a")
		if err != nil || status.Snapshot.LastIncludedIndex != 1 {
			t.Fatalf("status=%+v err=%v", status, err)
		}
	})
}

func TestLaggingFollowerInstallsLeaderSnapshot(t *testing.T) {
	t.Parallel()

	cluster := mustCluster(t, testConfig("a", "b", "c"))
	runUntil(t, cluster, 500*time.Millisecond)
	leader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no leader: %+v", cluster.Statuses())
	}
	var lagger raft.NodeID
	for _, id := range cluster.Members() {
		if id != leader {
			lagger = id
			break
		}
	}
	if err := cluster.Crash(lagger); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"one", "two", "three"} {
		if err := cluster.Propose([]byte(command)); err != nil {
			t.Fatal(err)
		}
		runUntil(t, cluster, cluster.Simulator().Now()+100*time.Millisecond)
	}
	leaderStatus, err := cluster.Status(leader)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Snapshot(leader, []byte("chain@"+fmt.Sprint(leaderStatus.AppliedIndex))); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+20*time.Millisecond)
	leaderStore, _ := cluster.Store(leader)
	if leaderStore.State.Snapshot.LastIncludedIndex == 0 {
		t.Fatal("leader did not compact")
	}
	if err := cluster.Restart(lagger); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+2*time.Second)
	laggerStore, _ := cluster.Store(lagger)
	if laggerStore.State.Snapshot.LastIncludedIndex != leaderStore.State.Snapshot.LastIncludedIndex || laggerStore.AppliedIndex < leaderStore.State.Snapshot.LastIncludedIndex || laggerStore.InstalledSnapshot.LastIncludedIndex != leaderStore.State.Snapshot.LastIncludedIndex {
		t.Fatalf("leader=%+v lagger=%+v", leaderStore, laggerStore)
	}
	if violations := cluster.Violations(); len(violations) != 0 {
		t.Fatalf("violations = %+v", violations)
	}
	laggerStore.InstalledSnapshot.Members[0] = "mutated"
	laggerStore.InstalledSnapshot.Data[0] ^= 0xff
	laggerStore.InstalledSnapshot.Membership.Voters[0] = "mutated"
	again, _ := cluster.Store(lagger)
	if again.InstalledSnapshot.Members[0] == "mutated" || again.InstalledSnapshot.Membership.Voters[0] == "mutated" || slices.Equal(again.InstalledSnapshot.Data, laggerStore.InstalledSnapshot.Data) {
		t.Fatal("Store did not deep-clone installed snapshot")
	}
}

func TestClusterJointConsensusPromotesLearnerAndRemovesLeader(t *testing.T) {
	t.Parallel()

	config := testConfig("a", "b", "c", "d")
	config.Voters = []raft.NodeID{"a", "b", "c"}
	config.Learners = []raft.NodeID{"d"}
	cluster := mustCluster(t, config)
	runUntil(t, cluster, 500*time.Millisecond)
	oldLeader, ok := cluster.Leader()
	if !ok || oldLeader == "d" {
		t.Fatalf("initial leader=%q statuses=%+v", oldLeader, cluster.Statuses())
	}
	newVoters := []raft.NodeID{"b", "c", "d"}
	newLearners := []raft.NodeID{oldLeader}
	if oldLeader != "a" {
		newVoters = make([]raft.NodeID, 0, 3)
		for _, id := range []raft.NodeID{"a", "b", "c", "d"} {
			if id != oldLeader && len(newVoters) < 3 {
				newVoters = append(newVoters, id)
			}
		}
		slices.Sort(newVoters)
	}
	if err := cluster.BeginMembershipChange(newVoters, newLearners); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+500*time.Millisecond)
	jointLeader, ok := cluster.Leader()
	if !ok {
		t.Fatalf("no joint leader: %+v", cluster.Statuses())
	}
	jointStatus, _ := cluster.Status(jointLeader)
	if !jointStatus.Membership.Joint() || jointStatus.CommitIndex < 2 {
		t.Fatalf("joint status = %+v", jointStatus)
	}
	if err := cluster.Snapshot(jointLeader, []byte("joint-checkpoint")); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+50*time.Millisecond)
	jointStore, _ := cluster.Store(jointLeader)
	if !jointStore.State.Snapshot.Membership.Joint() {
		t.Fatalf("joint snapshot = %+v", jointStore.State.Snapshot)
	}
	if err := cluster.FinalizeMembershipChange(); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+time.Second)
	newLeader, ok := cluster.Leader()
	if !ok || newLeader == oldLeader || !slices.Contains(newVoters, newLeader) {
		t.Fatalf("final leader=%q old=%q statuses=%+v", newLeader, oldLeader, cluster.Statuses())
	}
	for _, status := range cluster.Statuses() {
		if status.Membership.Joint() || !slices.Equal(status.Membership.Voters, newVoters) || !slices.Equal(status.Membership.Learners, newLearners) {
			t.Fatalf("node %q membership = %+v", status.ID, status.Membership)
		}
	}
	for _, id := range cluster.Members() {
		if err := cluster.Crash(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range cluster.Members() {
		if err := cluster.Restart(id); err != nil {
			t.Fatal(err)
		}
		status, err := cluster.Status(id)
		if err != nil || status.Membership.Joint() || !slices.Equal(status.Membership.Voters, newVoters) || !slices.Equal(status.Membership.Learners, newLearners) {
			t.Fatalf("restart status=%+v err=%v", status, err)
		}
	}
	runUntil(t, cluster, cluster.Simulator().Now()+time.Second)
	newLeader, ok = cluster.Leader()
	if !ok || !slices.Contains(newVoters, newLeader) {
		t.Fatalf("post-restart leader=%q statuses=%+v", newLeader, cluster.Statuses())
	}
	if err := cluster.Propose([]byte("after-reconfiguration")); err != nil {
		t.Fatal(err)
	}
	runUntil(t, cluster, cluster.Simulator().Now()+200*time.Millisecond)
	if violations := cluster.Violations(); len(violations) != 0 {
		t.Fatalf("violations = %+v", violations)
	}
}

func TestClusterOwnsInitialRoleConfigurationAcrossRestart(t *testing.T) {
	t.Parallel()

	config := testConfig("a", "b", "c")
	config.Voters = []raft.NodeID{"a", "b"}
	config.Learners = []raft.NodeID{"c"}
	cluster := mustCluster(t, config)
	if err := cluster.Crash("c"); err != nil {
		t.Fatal(err)
	}
	config.Voters[0] = "c"
	config.Learners[0] = "a"
	if err := cluster.Restart("c"); err != nil {
		t.Fatal(err)
	}
	status, err := cluster.Status("c")
	if err != nil || !slices.Equal(status.Membership.Voters, []raft.NodeID{"a", "b"}) || !slices.Equal(status.Membership.Learners, []raft.NodeID{"c"}) {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	status.Membership.Voters[0] = "mutated"
	again, err := cluster.Status("c")
	if err != nil || again.Membership.Voters[0] != "a" {
		t.Fatalf("status membership was not cloned: status=%+v err=%v", again, err)
	}
}

func testConfig(members ...raft.NodeID) Config {
	config := DefaultConfig(members...)
	config.Seed = 0x5eed
	config.Network = sim.LinkConfig{MinLatency: time.Millisecond, MaxLatency: 5 * time.Millisecond}
	config.ElectionTimeoutMin = 40 * time.Millisecond
	config.ElectionTimeoutMax = 80 * time.Millisecond
	config.HeartbeatInterval = 10 * time.Millisecond
	config.StorageLatency = time.Millisecond
	return config
}

func singleNodeCrashConfig() Config {
	config := DefaultConfig("a")
	config.ElectionTimeoutMin = 10 * time.Millisecond
	config.ElectionTimeoutMax = 10 * time.Millisecond
	config.HeartbeatInterval = 2 * time.Millisecond
	config.StorageLatency = 10 * time.Millisecond
	return config
}

func applicationConfig(config Config) Config {
	application := apporacle.KVConfig()
	config.Application = &application
	return config
}

func portablePut(t testing.TB, start byte, key, value []byte) []byte {
	t.Helper()
	var id apporacle.CommandID
	for index := range id {
		id[index] = start + byte(index)
	}
	encoded, err := apporacle.EncodeCommand(apporacle.Command{ID: id, Operation: apporacle.Put, Key: slices.Clone(key), Value: slices.Clone(value)})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustCluster(t *testing.T, config Config) *Cluster {
	t.Helper()
	cluster, err := New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cluster
}

func runUntil(t *testing.T, cluster *Cluster, end time.Duration) {
	t.Helper()
	if _, err := cluster.RunUntil(end); err != nil {
		t.Fatalf("RunUntil(%s): %v", end, err)
	}
}

func statusesEqual(left, right []raft.Status) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].ID != right[index].ID || left[index].Role != right[index].Role || left[index].Term != right[index].Term || left[index].CommitIndex != right[index].CommitIndex || left[index].AppliedIndex != right[index].AppliedIndex || !entriesEqual(left[index].Log, right[index].Log) {
			return false
		}
	}
	return true
}

func entriesEqual(left, right []raft.Entry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Index != right[index].Index || left[index].Term != right[index].Term || left[index].Type != right[index].Type || !bytes.Equal(left[index].Data, right[index].Data) {
			return false
		}
	}
	return true
}
