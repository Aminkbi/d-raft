package minimize

import (
	"slices"
	"testing"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
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

func TestMinimizedResultOwnsMembershipSlices(t *testing.T) {
	t.Parallel()

	entry, err := decision.NewEntry(decision.Choice{ID: "keep", Kind: decision.NetworkLoss, Options: []decision.Option{{ID: "deliver", Weight: 1}}}, decision.Selection{Option: "deliver"})
	if err != nil {
		t.Fatal(err)
	}
	input := artifact.Run{
		Configuration: artifact.Configuration{Members: []raft.NodeID{"a", "b", "c"}, Voters: []raft.NodeID{"a", "b"}, Learners: []raft.NodeID{"c"}},
		Scenario: artifact.Scenario{Actions: []artifact.Action{
			{Kind: artifact.ActionBeginMembership, Voters: []raft.NodeID{"b", "c"}, Learners: []raft.NodeID{"a"}},
			{Kind: artifact.ActionPropose, Data: []byte("irrelevant")},
		}},
		Decisions: decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{entry}},
	}
	var returnedConfigurationVoters, returnedActionVoters, returnedActionLearners []raft.NodeID
	observedAcceptedAlias := false
	evaluate := func(scenario artifact.Scenario, guide decision.Tape) (artifact.Run, bool, error) {
		if returnedConfigurationVoters != nil {
			returnedConfigurationVoters[0] = "mutated"
			returnedActionVoters[0] = "mutated"
			returnedActionLearners[0] = "mutated"
			if len(scenario.Actions) > 0 && len(scenario.Actions[0].Voters) > 0 && len(scenario.Actions[0].Learners) > 0 {
				observedAcceptedAlias = observedAcceptedAlias || scenario.Actions[0].Voters[0] == "mutated" || scenario.Actions[0].Learners[0] == "mutated"
			}
			return artifact.Run{}, false, nil
		}
		hasMembership := false
		for _, action := range scenario.Actions {
			hasMembership = hasMembership || action.Kind == artifact.ActionBeginMembership
		}
		candidate := input
		candidate.Scenario = scenario
		if hasMembership {
			candidate.Scenario.Actions[0].Voters = input.Scenario.Actions[0].Voters
			candidate.Scenario.Actions[0].Learners = input.Scenario.Actions[0].Learners
		}
		candidate.Decisions = guide
		candidate.Outcome = artifact.Outcome{Status: artifact.OutcomeViolation, Violations: []check.Violation{{Fingerprint: "target"}}}
		if hasMembership && len(scenario.Actions) == 1 {
			returnedConfigurationVoters = candidate.Configuration.Voters
			returnedActionVoters = candidate.Scenario.Actions[0].Voters
			returnedActionLearners = candidate.Scenario.Actions[0].Learners
		}
		return candidate, hasMembership, nil
	}
	result, err := minimize(input, Bounds{MaxRuns: 10}, "target", evaluate)
	if err != nil {
		t.Fatal(err)
	}
	if observedAcceptedAlias {
		t.Fatal("accepted candidate remained aliased during subsequent evaluation")
	}
	input.Configuration.Voters[0] = "mutated"
	input.Configuration.Learners[0] = "mutated"
	input.Scenario.Actions[0].Voters[0] = "mutated"
	input.Scenario.Actions[0].Learners[0] = "mutated"
	if !slices.Equal(result.Run.Configuration.Voters, []raft.NodeID{"a", "b"}) || !slices.Equal(result.Run.Configuration.Learners, []raft.NodeID{"c"}) {
		t.Fatalf("configuration aliases input: %+v", result.Run.Configuration)
	}
	if result.ActionsRemoved != 1 || len(result.Run.Scenario.Actions) != 1 || result.Run.Scenario.Actions[0].Kind != artifact.ActionBeginMembership || !slices.Equal(result.Run.Scenario.Actions[0].Voters, []raft.NodeID{"b", "c"}) || !slices.Equal(result.Run.Scenario.Actions[0].Learners, []raft.NodeID{"a"}) {
		t.Fatalf("action aliases input: %+v", result.Run.Scenario.Actions[0])
	}
}
