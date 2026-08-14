package experiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/explore"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

func TestReferenceFrontierIsStableAndIncludesScenarioNamespace(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a", "b")
	configuration := artifact.ConfigurationFrom(config)
	scenario := artifact.Scenario{ID: "frontier", Version: "1", DurationNS: int64(time.Second), MaxSteps: 100,
		Actions: []artifact.Action{{AtNS: int64(500 * time.Millisecond), Kind: artifact.ActionCrash, Node: "a"}},
	}
	run := func(source artifact.Scenario) []byte {
		t.Helper()
		prefix := decision.Tape{Schema: decision.SchemaVersion}
		decider, err := decision.NewPrefixDecider(prefix)
		if err != nil {
			t.Fatal(err)
		}
		_, frontier, err := ExecuteWithFrontier(source, configuration, decider)
		if !errors.Is(err, decision.ErrOpenChoice) {
			t.Fatalf("error = %v", err)
		}
		if !json.Valid(frontier) {
			t.Fatalf("invalid frontier %q", frontier)
		}
		return frontier
	}
	first := run(scenario)
	if !bytes.Equal(first, run(scenario)) {
		t.Fatal("identical clean reruns produced different frontiers")
	}
	changed := scenario
	changed.Actions = []artifact.Action{{AtNS: int64(500 * time.Millisecond), Kind: artifact.ActionCrash, Node: "b"}}
	if bytes.Equal(first, run(changed)) {
		t.Fatal("scenario namespace was omitted")
	}
}

func TestReferenceFrontierCapturesBootstrapContinuation(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a", "b")
	configuration := artifact.ConfigurationFrom(config)
	scenario := artifact.Scenario{ID: "bootstrap-frontier", Version: "1", DurationNS: int64(time.Second), MaxSteps: 100}
	empty, err := decision.NewPrefixDecider(decision.Tape{Schema: decision.SchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	_, _, runErr := ExecuteWithFrontier(scenario, configuration, empty)
	var firstOpen *decision.OpenChoiceError
	if !errors.As(runErr, &firstOpen) {
		t.Fatalf("first error = %v", runErr)
	}
	selection := decision.Selection{Number: firstOpen.Choice.Min}
	entry, err := decision.NewEntry(firstOpen.Choice, selection)
	if err != nil {
		t.Fatal(err)
	}
	prefix := decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{entry}}
	decider, err := decision.NewPrefixDecider(prefix)
	if err != nil {
		t.Fatal(err)
	}
	_, frontier, runErr := ExecuteWithFrontier(scenario, configuration, decider)
	var secondOpen *decision.OpenChoiceError
	if !errors.As(runErr, &secondOpen) {
		t.Fatalf("second error = %v", runErr)
	}
	var decoded frontierState
	if err := json.Unmarshal(frontier, &decoded); err != nil {
		t.Fatal(err)
	}
	if secondOpen.Choice.ID == firstOpen.Choice.ID || len(decoded.InEvent) != 1 || decoded.InEvent[0].Choice.ID != firstOpen.Choice.ID || decoded.StepsUsed != 0 {
		t.Fatalf("open=%+v frontier=%+v", secondOpen, decoded)
	}
}

func TestReferenceFrontierCapturesMidCallbackOpenChoice(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a")
	config.ElectionTimeoutMin = 10 * time.Millisecond
	config.ElectionTimeoutMax = 10 * time.Millisecond
	config.HeartbeatInterval = 2 * time.Millisecond
	configuration := artifact.ConfigurationFrom(config)
	scenario := artifact.Scenario{ID: "mid-callback", Version: "1", DurationNS: int64(20 * time.Millisecond), MaxSteps: 100}
	empty, err := decision.NewPrefixDecider(decision.Tape{Schema: decision.SchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	_, _, runErr := ExecuteWithFrontier(scenario, configuration, empty)
	var timerOpen *decision.OpenChoiceError
	if !errors.As(runErr, &timerOpen) {
		t.Fatalf("timer error = %v", runErr)
	}
	entry, err := decision.NewEntry(timerOpen.Choice, decision.Selection{Number: timerOpen.Choice.Min})
	if err != nil {
		t.Fatal(err)
	}
	decider, err := decision.NewPrefixDecider(decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{entry}})
	if err != nil {
		t.Fatal(err)
	}
	_, frontier, runErr := ExecuteWithFrontier(scenario, configuration, decider)
	var storageOpen *decision.OpenChoiceError
	if !errors.As(runErr, &storageOpen) || storageOpen.Choice.Kind != decision.StorageLatency {
		t.Fatalf("storage error = %v", runErr)
	}
	var decoded frontierState
	if err := json.Unmarshal(frontier, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.InEvent) != 0 || !bytes.Contains(decoded.PreEvent, []byte(`"kind":"election_timer"`)) {
		t.Fatalf("frontier does not identify active election event: %s", frontier)
	}
}

func TestCachedAndUncachedReferenceExplorationAgree(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a")
	config.ElectionTimeoutMin = 10 * time.Millisecond
	config.ElectionTimeoutMax = 11 * time.Millisecond
	config.HeartbeatInterval = 2 * time.Millisecond
	configuration := artifact.ConfigurationFrom(config)
	scenario := artifact.Scenario{ID: "cache-parity", Version: "1", DurationNS: int64(25 * time.Millisecond), MaxSteps: 100}
	bounds := explore.Bounds{MaxRuns: 100, MaxDepth: 2, MaxBranchesPerChoice: 2, RangeSamples: 3}
	plain, err := explore.DFS(func(decider decision.Decider) (artifact.Outcome, error) {
		return Execute(scenario, configuration, decider)
	}, bounds)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := explore.DFSWithCache(func(decider decision.Decider) (artifact.Outcome, []byte, error) {
		return ExecuteWithFrontier(scenario, configuration, decider)
	}, bounds, explore.CacheBounds{MaxEntries: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if plain.ViolatingRuns != cached.ViolatingRuns || (plain.FirstViolation == nil) != (cached.FirstViolation == nil) || plain.Truncated != cached.Truncated {
		t.Fatalf("plain=%+v cached=%+v", plain, cached)
	}
}

func TestScenarioRecordAndReplay(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a", "b", "c")
	config.Seed = 9
	config.Network = sim.LinkConfig{MinLatency: time.Millisecond, MaxLatency: 5 * time.Millisecond, LossProbability: 0.05}
	config.ElectionTimeoutMin = 40 * time.Millisecond
	config.ElectionTimeoutMax = 80 * time.Millisecond
	config.HeartbeatInterval = 10 * time.Millisecond
	scenario := artifact.Scenario{
		ID: "fault-cycle", Version: "1", DurationNS: int64(time.Second), MaxSteps: 100_000,
		Actions: []artifact.Action{
			{AtNS: int64(250 * time.Millisecond), Kind: artifact.ActionPropose, Data: []byte("x=1")},
			{AtNS: int64(350 * time.Millisecond), Kind: artifact.ActionPartition, Groups: [][]raft.NodeID{{"a"}, {"b", "c"}}},
			{AtNS: int64(550 * time.Millisecond), Kind: artifact.ActionHeal},
			{AtNS: int64(650 * time.Millisecond), Kind: artifact.ActionCrash, Node: "a"},
			{AtNS: int64(750 * time.Millisecond), Kind: artifact.ActionRestart, Node: "a"},
		},
	}

	recorder := decision.NewRecorder(decision.NewSeedDecider(123))
	original, err := Execute(scenario, artifact.ConfigurationFrom(config), recorder)
	if err != nil || recorder.Err() != nil {
		t.Fatalf("original outcome=%+v err=%v recorder=%v", original, err, recorder.Err())
	}
	run := artifact.Run{
		Schema:          artifact.SchemaVersion,
		Scenario:        scenario,
		Adapter:         artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		Configuration:   artifact.ConfigurationFrom(config),
		Reproducibility: artifact.NewReproducibility(123),
		Decisions:       recorder.Tape(),
		Outcome:         original,
	}
	var encoded bytes.Buffer
	if err := artifact.Encode(&encoded, run); err != nil {
		t.Fatal(err)
	}
	decoded, err := artifact.Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := decision.NewTapeDecider(decoded.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Execute(decoded.Scenario, decoded.Configuration, replay)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(original, replayed) {
		t.Fatalf("original=%+v replayed=%+v", original, replayed)
	}
}

func TestCrashAfterPersistScenarioRecordAndReplay(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a")
	config.ElectionTimeoutMin = 10 * time.Millisecond
	config.ElectionTimeoutMax = 10 * time.Millisecond
	config.HeartbeatInterval = 2 * time.Millisecond
	config.StorageLatency = 10 * time.Millisecond
	scenario := artifact.Scenario{ID: "persist-boundary", Version: "1", DurationNS: int64(50 * time.Millisecond), MaxSteps: 100, Actions: []artifact.Action{{AtNS: int64(5 * time.Millisecond), Kind: artifact.ActionCrashAfterNextPersist, Node: "a"}}}
	recorder := decision.NewRecorder(decision.NewSeedDecider(7))
	original, err := Execute(scenario, artifact.ConfigurationFrom(config), recorder)
	if err != nil || original.Status != artifact.OutcomeCompleted {
		t.Fatalf("original=%+v err=%v", original, err)
	}
	run := artifact.Run{
		Schema:          artifact.SchemaVersion,
		Scenario:        scenario,
		Adapter:         artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		Configuration:   artifact.ConfigurationFrom(config),
		Reproducibility: artifact.NewReproducibility(91),
		Decisions:       recorder.Tape(),
		Outcome:         original,
	}
	var encoded bytes.Buffer
	if err := artifact.Encode(&encoded, run); err != nil {
		t.Fatal(err)
	}
	decoded, err := artifact.Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := decision.NewTapeDecider(decoded.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Execute(decoded.Scenario, decoded.Configuration, replay)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(original, replayed) {
		t.Fatalf("original=%+v replayed=%+v", original, replayed)
	}
}

func TestScenarioStepBudget(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a")
	scenario := artifact.Scenario{ID: "budget", Version: "1", DurationNS: int64(time.Second), MaxSteps: 1}
	outcome, err := Execute(scenario, artifact.ConfigurationFrom(config), decision.NewSeedDecider(1))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != artifact.OutcomeBudgetExhausted || outcome.Steps != 1 || outcome.EndNS >= scenario.DurationNS {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestSnapshotScenarioActionCompactsReferenceNode(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a")
	config.ElectionTimeoutMin = 10 * time.Millisecond
	config.ElectionTimeoutMax = 10 * time.Millisecond
	config.HeartbeatInterval = 2 * time.Millisecond
	config.StorageLatency = time.Millisecond
	cluster, err := raftsim.New(config)
	if err != nil {
		t.Fatal(err)
	}
	scenario := artifact.Scenario{
		ID: "snapshot", Version: "1", DurationNS: int64(60 * time.Millisecond), MaxSteps: 1_000,
		Actions: []artifact.Action{
			{AtNS: int64(25 * time.Millisecond), Kind: artifact.ActionPropose, Node: "a", Data: []byte("one")},
			{AtNS: int64(35 * time.Millisecond), Kind: artifact.ActionSnapshot, Node: "a", Data: []byte("state@2")},
		},
	}
	outcome, err := executeScheduled(cluster, scenario)
	if err != nil || outcome.Status != artifact.OutcomeCompleted {
		t.Fatalf("outcome=%+v err=%v", outcome, err)
	}
	store, err := cluster.Store("a")
	if err != nil || store.State.Snapshot.LastIncludedIndex != 2 || string(store.State.Snapshot.Data) != "state@2" {
		t.Fatalf("store=%+v err=%v", store, err)
	}
}

func TestObservationV3DigestIncludesSnapshotAndMembershipState(t *testing.T) {
	t.Parallel()

	base := observationSnapshot{Nodes: []nodeSnapshot{{ID: "a", Up: true, Store: raftsim.Store{}}}}
	withSnapshot := base
	withSnapshot.Nodes = append([]nodeSnapshot(nil), base.Nodes...)
	withSnapshot.Nodes[0].Store.State = raft.PersistentState{
		HardState: raft.HardState{CurrentTerm: 1, CommitIndex: 1},
		Snapshot:  raft.Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Members: []raft.NodeID{"a"}, Data: []byte("state"), Membership: raft.StableMembership([]raft.NodeID{"a"}, nil)},
	}
	left, err := artifact.DigestJSON(base)
	if err != nil {
		t.Fatal(err)
	}
	right, err := artifact.DigestJSON(withSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("snapshot state did not change observation-v3 digest")
	}
}

func TestMembershipScenarioRecordAndReplay(t *testing.T) {
	t.Parallel()

	config := raftsim.DefaultConfig("a", "b", "c", "d")
	config.Voters = []raft.NodeID{"a", "b", "c"}
	config.Learners = []raft.NodeID{"d"}
	config.Seed = 17
	config.Network = sim.LinkConfig{MinLatency: time.Millisecond, MaxLatency: 3 * time.Millisecond}
	config.ElectionTimeoutMin = 40 * time.Millisecond
	config.ElectionTimeoutMax = 80 * time.Millisecond
	config.HeartbeatInterval = 10 * time.Millisecond
	scenario := artifact.Scenario{
		ID: "membership-cycle", Version: "1", DurationNS: int64(2 * time.Second), MaxSteps: 100_000,
		Actions: []artifact.Action{
			{AtNS: int64(400 * time.Millisecond), Kind: artifact.ActionBeginMembership, Voters: []raft.NodeID{"b", "c", "d"}, Learners: []raft.NodeID{"a"}},
			{AtNS: int64(900 * time.Millisecond), Kind: artifact.ActionFinalizeMembership},
			{AtNS: int64(1500 * time.Millisecond), Kind: artifact.ActionPropose, Data: []byte("after-membership")},
		},
	}
	recorder := decision.NewRecorder(decision.NewSeedDecider(91))
	original, err := Execute(scenario, artifact.ConfigurationFrom(config), recorder)
	if err != nil || recorder.Err() != nil || original.Status != artifact.OutcomeCompleted {
		t.Fatalf("original=%+v err=%v recorder=%v", original, err, recorder.Err())
	}
	run := artifact.Run{
		Schema:          artifact.SchemaVersion,
		Scenario:        scenario,
		Adapter:         artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		Configuration:   artifact.ConfigurationFrom(config),
		Reproducibility: artifact.NewReproducibility(91),
		Decisions:       recorder.Tape(),
		Outcome:         original,
	}
	var encoded bytes.Buffer
	if err := artifact.Encode(&encoded, run); err != nil {
		t.Fatal(err)
	}
	decoded, err := artifact.Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := decision.NewTapeDecider(decoded.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Execute(decoded.Scenario, decoded.Configuration, replay)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(original, replayed) {
		t.Fatalf("original=%+v replayed=%+v", original, replayed)
	}
}
