package etcdraft

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
)

func portableCommand(t *testing.T, id byte, operation apporacle.Operation, key, value string) []byte {
	t.Helper()
	var commandID apporacle.CommandID
	commandID[len(commandID)-1] = id
	encoded, err := apporacle.EncodeCommand(apporacle.Command{
		ID: commandID, Operation: operation, Key: []byte(key), Value: []byte(value),
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func portableConfig(members ...rootraft.NodeID) Config {
	config := testConfig(members...)
	application := apporacle.KVConfig()
	config.Application = &application
	config.ElectionTimeoutMin = 100 * time.Millisecond
	config.ElectionTimeoutMax = 250 * time.Millisecond
	return config
}

func TestApplicationModeRejectsMalformedProposalBeforeMutation(t *testing.T) {
	recorder := decision.NewRecorder(decision.NewSeedDecider(29))
	config := portableConfig("a")
	config.Decider = recorder
	cluster, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		leader, ok := cluster.Leader()
		return ok && leader == "a" && cluster.processes["a"].pending == nil
	})
	beforeDigest, err := cluster.SnapshotDigest()
	if err != nil {
		t.Fatal(err)
	}
	beforeTape := recorder.Tape()
	beforeStatus := cluster.processes["a"].raw.BasicStatus()

	if err := cluster.ProposeTo("a", []byte("opaque-legacy-payload")); !errors.Is(err, apporacle.ErrInvalidCommand) {
		t.Fatalf("ProposeTo error = %v, want ErrInvalidCommand", err)
	}
	afterDigest, err := cluster.SnapshotDigest()
	if err != nil {
		t.Fatal(err)
	}
	if beforeDigest != afterDigest || !reflect.DeepEqual(beforeStatus, cluster.processes["a"].raw.BasicStatus()) {
		t.Fatal("malformed portable proposal mutated adapter state")
	}
	if !reflect.DeepEqual(beforeTape, recorder.Tape()) {
		t.Fatal("malformed portable proposal consumed a decision")
	}
}

func TestLegacyModeStillAcceptsOpaqueProposal(t *testing.T) {
	cluster, err := New(testConfig("a"))
	if err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		leader, ok := cluster.Leader()
		return ok && leader == "a" && cluster.processes["a"].pending == nil
	})
	if err := cluster.Propose([]byte("opaque-legacy-payload")); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		return len(cluster.processes["a"].applied) == 2
	})
	if _, err := cluster.ApplicationCommitment("a"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ApplicationCommitment error = %v, want ErrUnsupported", err)
	}
}

func TestApplicationCommitmentsConvergeAndSurviveRestart(t *testing.T) {
	cluster, err := New(portableConfig("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 10_000, func() bool {
		leader, ok := cluster.Leader()
		return ok && cluster.processes[leader].pending == nil
	})
	if err := cluster.Propose(portableCommand(t, 1, apporacle.Put, "key", "value")); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 20_000, func() bool {
		for _, name := range cluster.members {
			commitment, commitmentErr := cluster.ApplicationCommitment(name)
			if commitmentErr != nil || commitment.Commands != 1 {
				return false
			}
		}
		return true
	})
	want, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range cluster.members {
		got, commitmentErr := cluster.ApplicationCommitment(name)
		if commitmentErr != nil {
			t.Fatal(commitmentErr)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("application commitment on %s = %+v, want %+v", name, got, want)
		}
	}

	if err := cluster.Crash("b"); err != nil {
		t.Fatal(err)
	}
	if cluster.processes["b"].application != nil {
		t.Fatal("crash retained volatile portable application pointer")
	}
	if err := cluster.Restart("b"); err != nil {
		t.Fatal(err)
	}
	got, err := cluster.ApplicationCommitment("b")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restarted commitment = %+v, want %+v", got, want)
	}
}

func TestApplicationRecoversCommandPersistedBeforeCrash(t *testing.T) {
	cluster, err := New(portableConfig("a"))
	if err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		leader, ok := cluster.Leader()
		return ok && leader == "a" && cluster.processes["a"].pending == nil
	})
	command := portableCommand(t, 2, apporacle.Put, "durable", "yes")
	if err := cluster.Propose(command); err != nil {
		t.Fatal(err)
	}
	if err := cluster.CrashAfterNextPersist("a"); err != nil {
		t.Fatal(err)
	}
	if ran, err := cluster.Step(); err != nil || !ran {
		t.Fatalf("persist/crash step: ran=%t err=%v", ran, err)
	}
	if cluster.processes["a"].application != nil {
		t.Fatal("crash-after-write retained volatile portable application pointer")
	}
	beforeRestart, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	if beforeRestart.Commands != 0 {
		t.Fatalf("command applied before persistence acknowledgement: %+v", beforeRestart)
	}
	if err := cluster.Restart("a"); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		commitment, commitmentErr := cluster.ApplicationCommitment("a")
		return commitmentErr == nil && commitment.Commands == 1
	})

	wantMachine := apporacle.New()
	if _, err := wantMachine.ApplyEncoded(command); err != nil {
		t.Fatal(err)
	}
	got, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	if want := wantMachine.Commitment(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered commitment = %+v, want %+v", got, want)
	}
}

func TestDuplicateApplicationCommandFailsWithoutChangingCommitment(t *testing.T) {
	cluster, err := New(portableConfig("a"))
	if err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		leader, ok := cluster.Leader()
		return ok && leader == "a" && cluster.processes["a"].pending == nil
	})
	command := portableCommand(t, 3, apporacle.Put, "key", "first")
	if err := cluster.Propose(command); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, cluster, 1_000, func() bool {
		commitment, commitmentErr := cluster.ApplicationCommitment("a")
		return commitmentErr == nil && commitment.Commands == 1 && cluster.processes["a"].pending == nil
	})
	want, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	beforeApplied := len(cluster.processes["a"].applied)
	if err := cluster.Propose(command); err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 1_000; step++ {
		ran, stepErr := cluster.Step()
		if stepErr != nil {
			if !errors.Is(stepErr, apporacle.ErrDuplicateCommand) {
				t.Fatalf("duplicate apply error = %v, want ErrDuplicateCommand", stepErr)
			}
			break
		}
		if !ran {
			t.Fatal("event queue exhausted before duplicate command failed")
		}
		if step == 999 {
			t.Fatal("duplicate command did not fail")
		}
	}
	got, err := cluster.ApplicationCommitment("a")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || len(cluster.processes["a"].applied) != beforeApplied {
		t.Fatal("duplicate command changed application or applied-entry history")
	}
}

func TestExecuteWithApplicationPrevalidatesAllProposalsBeforeChoices(t *testing.T) {
	configuration := artifact.Configuration{
		Members: []rootraft.NodeID{"a"}, InfrastructureSeed: 1,
		NetworkMinLatencyNS: 0, NetworkMaxLatencyNS: 0,
		ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
		HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: 0,
	}
	valid := portableCommand(t, 4, apporacle.Put, "key", "value")
	tests := []struct {
		name    string
		actions []artifact.Action
		want    error
	}{
		{name: "malformed", actions: []artifact.Action{{AtNS: int64(500 * time.Millisecond), Kind: artifact.ActionPropose, Data: []byte("malformed")}}, want: apporacle.ErrInvalidCommand},
		{name: "duplicate ID", actions: []artifact.Action{
			{AtNS: int64(500 * time.Millisecond), Kind: artifact.ActionPropose, Data: valid},
			{AtNS: int64(600 * time.Millisecond), Kind: artifact.ActionPropose, Data: valid},
		}, want: apporacle.ErrDuplicateCommand},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := artifact.Scenario{ID: "portable-prevalidation", Version: "1", DurationNS: int64(time.Second), MaxSteps: 10_000, Actions: test.actions}
			recorder := decision.NewRecorder(decision.NewSeedDecider(5))
			_, err := ExecuteWithApplication(scenario, configuration, recorder, apporacle.KVConfig())
			if !errors.Is(err, test.want) {
				t.Fatalf("ExecuteWithApplication error = %v, want %v", err, test.want)
			}
			if len(recorder.Tape().Entries) != 0 {
				t.Fatal("invalid proposal set consumed decisions")
			}
		})
	}
}
