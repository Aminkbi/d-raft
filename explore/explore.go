// Package explore performs bounded depth-first exploration by clean prefix
// reruns over semantic choices.
package explore

import (
	"errors"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
)

var ErrInvalidBounds = errors.New("explore: invalid bounds")

const (
	MaxRunsLimit     = 100_000
	MaxDepthLimit    = 64
	MaxBranchesLimit = 64
)

// Runner executes one clean run with the supplied decider.
type Runner func(decision.Decider) (artifact.Outcome, error)

type Bounds struct {
	MaxRuns              int
	MaxDepth             int
	MaxBranchesPerChoice int
	RangeSamples         int
	FallbackSeed         uint64
	StopOnViolation      bool
	TargetFingerprint    string
}

type Execution struct {
	Tape    decision.Tape
	Outcome artifact.Outcome
}

type Result struct {
	Runs           int
	OpenChoices    int
	Completed      int
	PrunedPrefixes int
	DepthBoundHits int
	SampledDomains int
	ViolatingRuns  int
	Truncated      bool
	FirstViolation *Execution
}

// DFS explores choice prefixes in deterministic domain order. At MaxDepth it
// completes the suffix with a seeded decider so every explored leaf can yield
// a concrete run artifact.
func DFS(run Runner, bounds Bounds) (Result, error) {
	if run == nil || bounds.MaxRuns <= 0 || bounds.MaxRuns > MaxRunsLimit || bounds.MaxDepth < 0 || bounds.MaxDepth > MaxDepthLimit || bounds.MaxBranchesPerChoice <= 0 || bounds.MaxBranchesPerChoice > MaxBranchesLimit || bounds.RangeSamples <= 0 || bounds.RangeSamples > 3 {
		return Result{}, ErrInvalidBounds
	}
	stack := []decision.Tape{{Schema: decision.SchemaVersion}}
	var result Result
	for len(stack) > 0 && result.Runs < bounds.MaxRuns {
		prefix := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if len(prefix.Entries) >= bounds.MaxDepth {
			result.DepthBoundHits++
			execution, pruned, err := complete(run, prefix, bounds.FallbackSeed)
			result.Runs++
			if err != nil {
				return result, err
			}
			if pruned {
				result.PrunedPrefixes++
				continue
			}
			result.Completed++
			if outcomeMatches(execution.Outcome, bounds.TargetFingerprint) {
				result.ViolatingRuns++
				if result.FirstViolation == nil {
					copy := execution
					result.FirstViolation = &copy
				}
				if bounds.StopOnViolation {
					result.Truncated = len(stack) > 0
					return result, nil
				}
			}
			continue
		}

		decider, err := decision.NewPrefixDecider(prefix)
		if err != nil {
			return result, err
		}
		outcome, runErr := run(decider)
		result.Runs++
		var open *decision.OpenChoiceError
		switch {
		case errors.As(runErr, &open):
			if err := decider.Finish(); err != nil {
				return result, err
			}
			result.OpenChoices++
			selections, sampled, err := candidates(open.Choice, bounds)
			if err != nil {
				return result, err
			}
			if sampled {
				result.SampledDomains++
			}
			for index := len(selections) - 1; index >= 0; index-- {
				entry, err := decision.NewEntry(open.Choice, selections[index])
				if err != nil {
					return result, err
				}
				child := decision.CloneTape(prefix)
				child.Entries = append(child.Entries, entry)
				stack = append(stack, child)
			}
		case runErr != nil:
			return result, runErr
		default:
			if err := decider.Finish(); err != nil {
				if errors.Is(err, decision.ErrTapeNotConsumed) {
					result.PrunedPrefixes++
					continue
				}
				return result, err
			}
			result.Completed++
			execution := Execution{Tape: decision.CloneTape(prefix), Outcome: outcome}
			if outcomeMatches(outcome, bounds.TargetFingerprint) {
				result.ViolatingRuns++
				if result.FirstViolation == nil {
					result.FirstViolation = &execution
				}
				if bounds.StopOnViolation {
					result.Truncated = len(stack) > 0
					return result, nil
				}
			}
		}
	}
	result.Truncated = len(stack) > 0
	return result, nil
}

func complete(run Runner, prefix decision.Tape, seed uint64) (Execution, bool, error) {
	combined, err := decision.NewPrefixThenDecider(prefix, decision.NewSeedDecider(seed))
	if err != nil {
		return Execution{}, false, err
	}
	recorder := decision.NewRecorder(combined)
	outcome, runErr := run(recorder)
	if runErr != nil {
		return Execution{}, false, runErr
	}
	if err := combined.Finish(); err != nil {
		if errors.Is(err, decision.ErrTapeNotConsumed) {
			return Execution{}, true, nil
		}
		return Execution{}, false, err
	}
	if err := recorder.Err(); err != nil {
		return Execution{}, false, err
	}
	return Execution{Tape: recorder.Tape(), Outcome: outcome}, false, nil
}

func candidates(choice decision.Choice, bounds Bounds) ([]decision.Selection, bool, error) {
	if err := decision.ValidateChoice(choice); err != nil {
		return nil, false, err
	}
	if len(choice.Options) > 0 {
		limit := min(len(choice.Options), bounds.MaxBranchesPerChoice)
		result := make([]decision.Selection, limit)
		for index := range limit {
			result[index] = decision.Selection{Option: choice.Options[index].ID}
		}
		return result, limit < len(choice.Options), nil
	}
	minimum, maximum := *choice.Min, *choice.Max
	span := uint64(maximum-minimum) + 1
	count := min(bounds.RangeSamples, bounds.MaxBranchesPerChoice)
	if span <= uint64(count) {
		result := make([]decision.Selection, int(span))
		for index := range result {
			value := minimum + int64(index)
			result[index].Number = &value
		}
		return result, false, nil
	}
	values := []int64{minimum}
	if count >= 3 {
		values = append(values, minimum+(maximum-minimum)/2)
	}
	if count >= 2 {
		values = append(values, maximum)
	}
	result := make([]decision.Selection, len(values))
	for index, source := range values {
		value := source
		result[index].Number = &value
	}
	return result, true, nil
}

func outcomeMatches(outcome artifact.Outcome, target string) bool {
	for _, violation := range outcome.Violations {
		if target == "" || violation.Fingerprint == target {
			return true
		}
	}
	return false
}
