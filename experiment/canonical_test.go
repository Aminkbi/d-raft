package experiment

import (
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
)

func TestPortableFaultsV1DefinitionAndReplay(t *testing.T) {
	scenario, configuration, err := Canonical(PortableFaultsV1)
	if err != nil {
		t.Fatal(err)
	}
	inputDigest, err := canonicalInputDigest(scenario, configuration, PortableFaultsV1DecisionSeed)
	if err != nil {
		t.Fatal(err)
	}
	if inputDigest != "3d6b249ba7c015451a6ec03285e5ba26f6e8f5d9a6e6aa9123b8fc37b52e1f1e" {
		t.Fatalf("canonical input digest = %s", inputDigest)
	}
	if scenario.ID != "semantic/portable-faults" || scenario.Version != "1" || scenario.DurationNS != int64(5*time.Second) || scenario.MaxSteps != 500_000 {
		t.Fatalf("scenario = %+v", scenario)
	}
	if configuration.InfrastructureSeed != 19 || configuration.NetworkLossProbability != 0.02 || !configuration.StopOnViolation {
		t.Fatalf("configuration = %+v", configuration)
	}
	if len(scenario.Actions) != 8 {
		t.Fatalf("actions = %d, want 8", len(scenario.Actions))
	}
	wantTimes := []time.Duration{600 * time.Millisecond, 800 * time.Millisecond, time.Second, 1400 * time.Millisecond, 1600 * time.Millisecond, 1800 * time.Millisecond, 2200 * time.Millisecond, 2400 * time.Millisecond}
	wantKinds := []artifact.ActionKind{artifact.ActionPropose, artifact.ActionPartition, artifact.ActionPropose, artifact.ActionHeal, artifact.ActionCrash, artifact.ActionPropose, artifact.ActionRestart, artifact.ActionPropose}
	commandOrdinal := byte(0)
	for index, action := range scenario.Actions {
		if action.AtNS != int64(wantTimes[index]) || action.Kind != wantKinds[index] {
			t.Fatalf("action %d = %+v", index, action)
		}
		if action.Kind == artifact.ActionPropose {
			commandOrdinal++
			command, decodeErr := apporacle.DecodeCommand(action.Data)
			if decodeErr != nil {
				t.Fatalf("action %d command: %v", index, decodeErr)
			}
			if command.ID != (apporacle.CommandID{15: commandOrdinal}) || command.Operation != apporacle.Put {
				t.Fatalf("action %d command = %+v", index, command)
			}
		}
	}

	recorder := decision.NewRecorder(decision.NewSeedDecider(PortableFaultsV1DecisionSeed))
	outcome, err := Execute(scenario, configuration, recorder)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Err(); err != nil {
		t.Fatal(err)
	}
	if outcome.Status != artifact.OutcomeCompleted || outcome.EndNS != scenario.DurationNS {
		t.Fatalf("outcome = %+v", outcome)
	}
	replay, err := decision.NewTapeDecider(recorder.Tape())
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Execute(scenario, configuration, replay)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(outcome, replayed) {
		t.Fatalf("replayed outcome = %+v, want %+v", replayed, outcome)
	}
}

func TestCanonicalRejectsUnknownName(t *testing.T) {
	_, _, err := Canonical("unknown")
	if err == nil || !strings.Contains(err.Error(), "unknown canonical scenario") {
		t.Fatalf("error = %v", err)
	}
}

func TestVerifyCanonicalRejectsInputDrift(t *testing.T) {
	scenario, configuration, err := Canonical(PortableFaultsV1)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCanonical(PortableFaultsV1, scenario, configuration, PortableFaultsV1DecisionSeed); err != nil {
		t.Fatal(err)
	}
	scenario.Actions[0].Data[0] ^= 1
	if err := VerifyCanonical(PortableFaultsV1, scenario, configuration, PortableFaultsV1DecisionSeed); err == nil {
		t.Fatal("changed command bytes accepted")
	}
	scenario, configuration, err = Canonical(PortableFaultsV1)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCanonical(PortableFaultsV1, scenario, configuration, PortableFaultsV1DecisionSeed+1); err == nil {
		t.Fatal("changed decision seed accepted")
	}
}
