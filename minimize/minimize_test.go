package minimize

import (
	"testing"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
)

func TestMinimizeRemovesIrrelevantActionsAndGuidance(t *testing.T) {
	t.Parallel()

	choice := func(id string) decision.Entry {
		entry, err := decision.NewEntry(decision.Choice{ID: id, Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "drop", Weight: 1}, {ID: "deliver", Weight: 1}}}, decision.Selection{Option: "deliver"})
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	input := artifact.Run{
		Scenario:  artifact.Scenario{Actions: []artifact.Action{{Data: []byte("noise-1")}, {Data: []byte("keep")}, {Data: []byte("noise-2")}}},
		Decisions: decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{choice("noise-a"), choice("keep-choice"), choice("noise-b")}},
	}
	evaluate := func(scenario artifact.Scenario, guide decision.Tape) (artifact.Run, bool, error) {
		keepsAction := false
		for _, action := range scenario.Actions {
			keepsAction = keepsAction || string(action.Data) == "keep"
		}
		keepsChoice := false
		for _, entry := range guide.Entries {
			keepsChoice = keepsChoice || entry.Choice.ID == "keep-choice" && entry.Selection.Option == "deliver"
		}
		candidate := input
		candidate.Scenario = cloneScenario(scenario)
		candidate.Decisions = decision.CloneTape(guide)
		if keepsAction && keepsChoice {
			candidate.Outcome = artifact.Outcome{Status: artifact.OutcomeViolation, Violations: []check.Violation{{Fingerprint: "target"}}}
		}
		return candidate, keepsAction && keepsChoice, nil
	}
	result, err := minimize(input, Bounds{MaxRuns: 100}, "target", evaluate)
	if err != nil {
		t.Fatal(err)
	}
	if result.ActionsRemoved != 2 || len(result.Run.Scenario.Actions) != 1 || result.GuidanceEntriesRemoved != 2 || len(result.Run.Decisions.Entries) != 1 {
		t.Fatalf("result = %+v", result)
	}
}
