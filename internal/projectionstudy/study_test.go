package projectionstudy

import (
	"bytes"
	"os"
	"testing"

	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/semanticplan"
)

func TestProjectionRobustnessMatrix(t *testing.T) {
	report, err := Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Observations) != 18 {
		t.Fatalf("observations = %d", len(report.Observations))
	}
	for _, observation := range report.Observations {
		t.Run(observation.Case+"/"+observation.Method, func(t *testing.T) {
			if observation.Method == "causal-operation-prototype" && observation.Case == "merge-conflicting-batch" {
				if observation.Status != "ineligible" || observation.Reason == "" || len(observation.Deliveries) != 0 || len(observation.Tape.Entries) != 0 || observation.Witness != nil || observation.LocalReplayVerified {
					t.Fatalf("batch was approximated or executed: %+v", observation)
				}
				return
			}
			wantFailure := observation.Case == "control" || observation.Case == "merge-conflicting-batch" || observation.Method == "causal-operation-prototype" || observation.Case == "shift-time" && observation.Method == "occurrence-v1"
			if observation.Status != "completed" || !observation.LocalReplayVerified || observation.FailurePreserved != wantFailure {
				t.Fatalf("failure preservation want %v: %+v", wantFailure, observation)
			}
			if observation.Method == "causal-operation-prototype" && observation.MisalignedOperations != 0 {
				t.Fatalf("causal policy changed a source operation disposition: %+v", observation)
			}
			if observation.Case == "reorder" && observation.Method == "occurrence-v1" {
				if observation.Projection.Fidelity != semanticplan.ProjectionExact || observation.MisalignedOperations != 2 || observation.Witness != nil {
					t.Fatalf("expected exact coverage with causal mismatch: %+v", observation)
				}
			}
		})
	}
}

func TestPerturbationsPreserveFaultFreeToyBehavior(t *testing.T) {
	for _, fixture := range Fixtures() {
		var deliveries []Delivery
		for _, message := range fixture.Messages {
			deliveries = append(deliveries, Delivery{Message: message, Selection: "deliver"})
		}
		_, _, witness := assess(deliveries)
		if witness != nil {
			t.Fatalf("%s changes fault-free survival of x", fixture.ID)
		}
	}
}

func TestExactTapeRejectsReordering(t *testing.T) {
	report, err := Run()
	if err != nil {
		t.Fatal(err)
	}
	replay, err := decision.NewTapeDecider(report.SourceTape)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay.Choose(choice(report.Cases[2].Messages[0])); err == nil {
		t.Fatal("exact local tape accepted different operation context")
	}
}

func TestPublishedStudyRegeneratesAndRejectsTampering(t *testing.T) {
	raw, err := os.ReadFile("../../evaluation/results/projection-v1/result.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(raw); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		bytes.Replace(raw, []byte(`"failure_preserved": true`), []byte(`"failure_preserved": false`), 1),
		append(bytes.Clone(raw), []byte("{}")...),
		bytes.Replace(raw, []byte(`"schema":`), []byte(`"unknown": 1, "schema":`), 1),
		bytes.Replace(raw, []byte(`"schema":`), []byte(`"schema": "duplicate", "schema":`), 1),
	} {
		if err := Verify(changed); err == nil {
			t.Fatal("accepted changed study")
		}
	}
}
