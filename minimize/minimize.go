// Package minimize reduces scenarios and semantic guidance while preserving a
// specific independently checked violation fingerprint.
package minimize

import (
	"errors"
	"fmt"
	"slices"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	"github.com/aminkbi/d-raft/raft"
)

var (
	ErrInvalidBounds   = errors.New("minimize: invalid bounds")
	ErrNoViolation     = errors.New("minimize: artifact has no target violation")
	ErrNotReproducible = errors.New("minimize: input artifact does not replay exactly")
)

const MaxRunsLimit = 100_000

type Bounds struct {
	MaxRuns           int
	FallbackSeed      uint64
	TargetFingerprint string
}

type Result struct {
	Run                    artifact.Run
	Runs                   int
	ActionsRemoved         int
	GuidanceEntriesRemoved int
	SelectionsShrunk       int
	Truncated              bool
}

type evaluator func(artifact.Scenario, decision.Tape) (artifact.Run, bool, error)

// Artifact verifies and minimizes a reference-adapter violation artifact.
func Artifact(input artifact.Run, bounds Bounds) (Result, error) {
	if err := input.Validate(); err != nil {
		return Result{}, err
	}
	if bounds.MaxRuns <= 0 || bounds.MaxRuns > MaxRunsLimit {
		return Result{}, ErrInvalidBounds
	}
	target := bounds.TargetFingerprint
	if target == "" && len(input.Outcome.Violations) > 0 {
		target = input.Outcome.Violations[0].Fingerprint
	}
	if target == "" || !hasViolation(input.Outcome, target) {
		return Result{}, ErrNoViolation
	}
	if input.Adapter.ID != artifact.ReferenceAdapterID || input.Adapter.Version != artifact.ReferenceAdapterCurrent {
		return Result{}, fmt.Errorf("minimize: unsupported adapter %s@%s", input.Adapter.ID, input.Adapter.Version)
	}

	replay, err := decision.NewTapeDecider(input.Decisions)
	if err != nil {
		return Result{}, err
	}
	outcome, err := experiment.Execute(input.Scenario, input.Configuration, replay)
	if err != nil {
		return Result{}, err
	}
	if err := replay.Finish(); err != nil || !artifact.OutcomesEqual(input.Outcome, outcome) {
		return Result{}, ErrNotReproducible
	}

	evaluate := func(scenario artifact.Scenario, guide decision.Tape) (artifact.Run, bool, error) {
		guided, err := decision.NewGuidedDecider(guide, decision.NewSeedDecider(bounds.FallbackSeed))
		if err != nil {
			return artifact.Run{}, false, err
		}
		recorder := decision.NewRecorder(guided)
		outcome, err := experiment.Execute(scenario, input.Configuration, recorder)
		if err != nil {
			return artifact.Run{}, false, err
		}
		if err := recorder.Err(); err != nil {
			return artifact.Run{}, false, err
		}
		candidate := cloneRun(input)
		candidate.Scenario = cloneScenario(scenario)
		candidate.Decisions = recorder.Tape()
		candidate.Outcome = outcome
		candidate.Reproducibility = artifact.NewReproducibility(bounds.FallbackSeed)
		if err := candidate.Validate(); err != nil {
			return artifact.Run{}, false, err
		}
		return candidate, hasViolation(outcome, target), nil
	}

	remaining := bounds
	remaining.MaxRuns--
	if remaining.MaxRuns == 0 {
		return Result{Run: cloneRun(input), Runs: 1, Truncated: true}, nil
	}
	result, err := minimize(input, remaining, target, evaluate)
	if err != nil {
		return Result{}, err
	}
	result.Runs++ // exact input replay
	return result, nil
}

func minimize(input artifact.Run, bounds Bounds, target string, evaluate evaluator) (Result, error) {
	current := cloneRun(input)
	guide := decision.CloneTape(input.Decisions)
	runs := 0
	originalActions := len(input.Scenario.Actions)

	granularity := 2
	for len(current.Scenario.Actions) > 0 && runs < bounds.MaxRuns {
		length := len(current.Scenario.Actions)
		chunk := (length + granularity - 1) / granularity
		accepted := false
		for start := 0; start < length && runs < bounds.MaxRuns; start += chunk {
			end := min(start+chunk, length)
			scenario := cloneScenario(current.Scenario)
			scenario.Actions = append(scenario.Actions[:start:start], scenario.Actions[end:]...)
			candidate, matches, err := evaluate(scenario, guide)
			runs++
			if err != nil {
				return Result{}, err
			}
			if matches {
				current = cloneRun(candidate)
				guide = decision.CloneTape(candidate.Decisions)
				granularity = max(2, granularity-1)
				accepted = true
				break
			}
		}
		if accepted {
			continue
		}
		if granularity >= length {
			break
		}
		granularity = min(length, granularity*2)
	}

	guideStart := len(guide.Entries)
	granularity = 2
	for len(guide.Entries) > 0 && runs < bounds.MaxRuns {
		length := len(guide.Entries)
		chunk := (length + granularity - 1) / granularity
		accepted := false
		for start := 0; start < length && runs < bounds.MaxRuns; start += chunk {
			end := min(start+chunk, length)
			candidateGuide := decision.CloneTape(guide)
			candidateGuide.Entries = append(candidateGuide.Entries[:start:start], candidateGuide.Entries[end:]...)
			candidate, matches, err := evaluate(current.Scenario, candidateGuide)
			runs++
			if err != nil {
				return Result{}, err
			}
			if matches {
				current = cloneRun(candidate)
				guide = candidateGuide
				granularity = max(2, granularity-1)
				accepted = true
				break
			}
		}
		if accepted {
			continue
		}
		if granularity >= length {
			break
		}
		granularity = min(length, granularity*2)
	}

	shrunk := 0
	for index := range guide.Entries {
		if runs >= bounds.MaxRuns {
			break
		}
		for _, selection := range simplerSelections(guide.Entries[index]) {
			candidateGuide := decision.CloneTape(guide)
			candidateGuide.Entries[index].Selection = selection
			candidate, matches, err := evaluate(current.Scenario, candidateGuide)
			runs++
			if err != nil {
				return Result{}, err
			}
			if matches {
				current = cloneRun(candidate)
				guide = candidateGuide
				shrunk++
				break
			}
			if runs >= bounds.MaxRuns {
				break
			}
		}
	}

	return Result{
		Run: cloneRun(current), Runs: runs,
		ActionsRemoved:         originalActions - len(current.Scenario.Actions),
		GuidanceEntriesRemoved: guideStart - len(guide.Entries),
		SelectionsShrunk:       shrunk,
		Truncated:              runs >= bounds.MaxRuns,
	}, nil
}

func simplerSelections(entry decision.Entry) []decision.Selection {
	choice, current := entry.Choice, entry.Selection
	if len(choice.Options) > 0 {
		var result []decision.Selection
		for _, preferred := range []string{"drop", choice.Options[0].ID} {
			if preferred != current.Option && optionExists(choice, preferred) && !selectionExists(result, decision.Selection{Option: preferred}) {
				result = append(result, decision.Selection{Option: preferred})
			}
		}
		return result
	}
	if current.Number == nil || *current.Number <= *choice.Min {
		return nil
	}
	minimum, original := *choice.Min, *current.Number
	values := []int64{minimum}
	for cursor := minimum + (original-minimum)/2; cursor > minimum && cursor < original; cursor = cursor + (original-cursor)/2 {
		values = append(values, cursor)
		if original-cursor <= 1 {
			break
		}
	}
	result := make([]decision.Selection, len(values))
	for index, source := range values {
		value := source
		result[index].Number = &value
	}
	return result
}

func optionExists(choice decision.Choice, id string) bool {
	for _, option := range choice.Options {
		if option.ID == id {
			return true
		}
	}
	return false
}

func selectionExists(selections []decision.Selection, target decision.Selection) bool {
	for _, selection := range selections {
		if selection.Option == target.Option {
			return true
		}
	}
	return false
}

func hasViolation(outcome artifact.Outcome, target string) bool {
	for _, violation := range outcome.Violations {
		if violation.Fingerprint == target {
			return true
		}
	}
	return false
}

func cloneScenario(scenario artifact.Scenario) artifact.Scenario {
	source := scenario.Actions
	scenario.Actions = make([]artifact.Action, len(source))
	for index, action := range source {
		action.Data = slices.Clone(action.Data)
		action.Voters = slices.Clone(action.Voters)
		action.Learners = slices.Clone(action.Learners)
		groups := action.Groups
		action.Groups = make([][]raft.NodeID, len(groups))
		for group := range groups {
			action.Groups[group] = slices.Clone(groups[group])
		}
		scenario.Actions[index] = action
	}
	return scenario
}

func cloneRun(run artifact.Run) artifact.Run {
	run.Scenario = cloneScenario(run.Scenario)
	run.Configuration.Members = slices.Clone(run.Configuration.Members)
	run.Configuration.Voters = slices.Clone(run.Configuration.Voters)
	run.Configuration.Learners = slices.Clone(run.Configuration.Learners)
	run.Decisions = decision.CloneTape(run.Decisions)
	sourceViolations := run.Outcome.Violations
	run.Outcome.Violations = make([]check.Violation, len(sourceViolations))
	for index, violation := range sourceViolations {
		violation.Nodes = slices.Clone(violation.Nodes)
		violation.Evidence = slices.Clone(violation.Evidence)
		run.Outcome.Violations[index] = violation
	}
	return run
}
