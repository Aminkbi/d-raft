package experiment

import (
	"testing"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

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
	replay, err := decision.NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Execute(scenario, artifact.ConfigurationFrom(config), replay)
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
	replay, err := decision.NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Execute(scenario, artifact.ConfigurationFrom(config), replay)
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
