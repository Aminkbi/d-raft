package explore

import (
	"testing"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
)

func TestDFSFindsBoundedSemanticFailure(t *testing.T) {
	t.Parallel()

	runner := func(decider decision.Decider) (artifact.Outcome, error) {
		choice := func(id string) (decision.Selection, error) {
			return decider.Choose(decision.Choice{ID: id, Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}, {ID: "drop", Weight: 1}}})
		}
		first, err := choice("first")
		if err != nil {
			return artifact.Outcome{}, err
		}
		second, err := choice("second")
		if err != nil {
			return artifact.Outcome{}, err
		}
		outcome := artifact.Outcome{Status: artifact.OutcomeCompleted}
		if first.Option == "drop" && second.Option == "drop" {
			outcome.Status = artifact.OutcomeViolation
			outcome.Violations = []check.Violation{{Fingerprint: "target"}}
		}
		return outcome, nil
	}
	result, err := DFS(runner, Bounds{MaxRuns: 20, MaxDepth: 2, MaxBranchesPerChoice: 2, RangeSamples: 3, StopOnViolation: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstViolation == nil || result.FirstViolation.Outcome.Violations[0].Fingerprint != "target" || len(result.FirstViolation.Tape.Entries) != 2 {
		t.Fatalf("result = %+v", result)
	}
}

func TestDFSFillsSuffixAtDepthBound(t *testing.T) {
	t.Parallel()

	runner := func(decider decision.Decider) (artifact.Outcome, error) {
		for _, id := range []string{"a", "b", "c"} {
			if _, err := decider.Choose(decision.Choice{ID: id, Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}, {ID: "drop", Weight: 1}}}); err != nil {
				return artifact.Outcome{}, err
			}
		}
		return artifact.Outcome{Status: artifact.OutcomeCompleted}, nil
	}
	result, err := DFS(runner, Bounds{MaxRuns: 20, MaxDepth: 1, MaxBranchesPerChoice: 2, RangeSamples: 3})
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 2 || result.OpenChoices != 1 {
		t.Fatalf("result = %+v", result)
	}
}
