package etcdraft

import (
	"fmt"
	"slices"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	rootraft "github.com/aminkbi/d-raft/raft"
)

// Execute runs a portable d-raft scenario against the production-used
// go.etcd.io/raft/v3 core under this adapter's explicit capability boundary.
func Execute(scenario artifact.Scenario, configuration artifact.Configuration, decider decision.Decider) (artifact.Outcome, error) {
	if err := artifact.ValidateExperiment(scenario, configuration); err != nil {
		return artifact.Outcome{}, err
	}
	if err := validateScenarioCapabilities(scenario); err != nil {
		return artifact.Outcome{}, err
	}
	config, err := ConfigurationFrom(configuration, decider)
	if err != nil {
		return artifact.Outcome{}, err
	}
	cluster, err := New(config)
	if err != nil {
		return artifact.Outcome{}, err
	}
	return executeScheduled(cluster, scenario)
}

// ExecuteWithApplication runs the same adapter under the explicit portable KV
// profile. Existing Execute retains opaque proposal semantics and digest bytes.
func ExecuteWithApplication(scenario artifact.Scenario, configuration artifact.Configuration, decider decision.Decider, application apporacle.Config) (artifact.Outcome, error) {
	if err := artifact.ValidateExperiment(scenario, configuration); err != nil {
		return artifact.Outcome{}, err
	}
	if err := validateScenarioCapabilities(scenario); err != nil {
		return artifact.Outcome{}, err
	}
	if err := application.Validate(); err != nil {
		return artifact.Outcome{}, err
	}
	if err := validateApplicationProposals(scenario); err != nil {
		return artifact.Outcome{}, err
	}
	config, err := ConfigurationFrom(configuration, decider)
	if err != nil {
		return artifact.Outcome{}, err
	}
	config.Application = &application
	cluster, err := New(config)
	if err != nil {
		return artifact.Outcome{}, err
	}
	return executeScheduled(cluster, scenario)
}

func validateApplicationProposals(scenario artifact.Scenario) error {
	seen := make(map[apporacle.CommandID]struct{})
	for _, action := range scenario.Actions {
		if action.Kind != artifact.ActionPropose {
			continue
		}
		command, err := apporacle.DecodeCommand(action.Data)
		if err != nil {
			return fmt.Errorf("etcdraft: invalid portable proposal: %w", err)
		}
		if _, exists := seen[command.ID]; exists {
			return fmt.Errorf("etcdraft: %w: %s", apporacle.ErrDuplicateCommand, command.ID)
		}
		seen[command.ID] = struct{}{}
	}
	return nil
}

func validateScenarioCapabilities(scenario artifact.Scenario) error {
	for _, action := range scenario.Actions {
		switch action.Kind {
		case artifact.ActionSnapshot, artifact.ActionBeginMembership, artifact.ActionFinalizeMembership:
			return fmt.Errorf("%w: action %q", ErrUnsupported, action.Kind)
		case artifact.ActionPropose:
			if len(action.Data) == 0 {
				return ErrInvalidProposal
			}
		}
	}
	return nil
}

func executeScheduled(cluster *Cluster, scenario artifact.Scenario) (artifact.Outcome, error) {
	var actionErr error
	for _, source := range scenario.Actions {
		action := cloneAction(source)
		if _, err := cluster.simulator.ScheduleAt(time.Duration(action.AtNS), func(*sim.Simulator) {
			if actionErr == nil {
				actionErr = applyAction(cluster, action)
			}
		}); err != nil {
			return artifact.Outcome{}, err
		}
	}

	var steps uint64
	end := time.Duration(scenario.DurationNS)
	var runErr error
	budgetExhausted := false
	for {
		if actionErr != nil {
			break
		}
		next, exists := cluster.simulator.NextEventTime()
		if !exists || next > end {
			break
		}
		if steps == scenario.MaxSteps {
			budgetExhausted = true
			break
		}
		ran, err := cluster.Step()
		if ran {
			steps++
		}
		if err != nil {
			runErr = err
			break
		}
	}
	if actionErr != nil {
		runErr = actionErr
	}
	if runErr == nil && !budgetExhausted {
		if _, err := cluster.simulator.RunUntil(end); err != nil {
			runErr = err
		}
	}

	violations := cluster.Violations()
	digest, err := cluster.SnapshotDigest()
	if err != nil {
		return artifact.Outcome{}, err
	}
	outcome := artifact.Outcome{Status: artifact.OutcomeCompleted, Steps: steps, EndNS: int64(cluster.simulator.Now()), ObservationDigest: digest, Violations: violations}
	if len(violations) > 0 {
		outcome.Status = artifact.OutcomeViolation
		return outcome, nil
	}
	if runErr != nil {
		outcome.Status = artifact.OutcomeError
		outcome.Error = runErr.Error()
		return outcome, nil
	}
	if budgetExhausted {
		outcome.Status = artifact.OutcomeBudgetExhausted
	}
	return outcome, nil
}

func applyAction(cluster *Cluster, action artifact.Action) error {
	switch action.Kind {
	case artifact.ActionPropose:
		if action.Node == "" {
			return cluster.Propose(action.Data)
		}
		return cluster.ProposeTo(action.Node, action.Data)
	case artifact.ActionCrash:
		return cluster.Crash(action.Node)
	case artifact.ActionRestart:
		return cluster.Restart(action.Node)
	case artifact.ActionCrashAfterNextPersist:
		return cluster.CrashAfterNextPersist(action.Node)
	case artifact.ActionPartition:
		return cluster.Partition(action.Groups...)
	case artifact.ActionHeal:
		cluster.Heal()
		return nil
	case artifact.ActionSnapshot, artifact.ActionBeginMembership, artifact.ActionFinalizeMembership:
		return fmt.Errorf("%w: action %q", ErrUnsupported, action.Kind)
	default:
		return fmt.Errorf("etcdraft: unknown action %q", action.Kind)
	}
}

func cloneAction(action artifact.Action) artifact.Action {
	action.Data = slices.Clone(action.Data)
	action.Voters = slices.Clone(action.Voters)
	action.Learners = slices.Clone(action.Learners)
	action.Groups = make([][]rootraft.NodeID, len(action.Groups))
	for index := range action.Groups {
		action.Groups[index] = slices.Clone(action.Groups[index])
	}
	return action
}
