// Package experiment executes versioned scenarios against d-raft adapters.
package experiment

import (
	"errors"
	"fmt"
	"slices"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

type nodeSnapshot struct {
	ID     raft.NodeID   `json:"id"`
	Up     bool          `json:"up"`
	Status *raft.Status  `json:"status,omitempty"`
	Store  raftsim.Store `json:"store"`
}

type observationSnapshot struct {
	AtNS       int64             `json:"at_ns"`
	Nodes      []nodeSnapshot    `json:"nodes"`
	Violations []check.Violation `json:"violations,omitempty"`
}

// Execute reruns a scenario from a clean cluster using decider.
func Execute(scenario artifact.Scenario, configuration artifact.Configuration, decider decision.Decider) (artifact.Outcome, error) {
	if err := artifact.ValidateExperiment(scenario, configuration); err != nil {
		return artifact.Outcome{}, err
	}

	cluster, err := raftsim.New(configuration.ClusterConfig(decider, nil))
	if err != nil {
		return artifact.Outcome{}, err
	}
	return executeScheduled(cluster, scenario)
}

func executeScheduled(cluster *raftsim.Cluster, scenario artifact.Scenario) (artifact.Outcome, error) {
	var actionErr error
	for _, source := range scenario.Actions {
		action := cloneAction(source)
		if _, err := cluster.Simulator().Schedule(time.Duration(action.AtNS), func(_ *sim.Simulator) {
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
		next, exists := cluster.Simulator().NextEventTime()
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
	if runErr == nil && actionErr == nil && !budgetExhausted {
		additional, err := cluster.RunUntil(end)
		steps += uint64(additional)
		runErr = err
	}
	if actionErr != nil {
		runErr = actionErr
	}
	if errors.Is(runErr, decision.ErrOpenChoice) {
		return artifact.Outcome{}, runErr
	}

	violations := cluster.Violations()
	digest, digestErr := snapshotDigest(cluster, violations)
	if digestErr != nil {
		return artifact.Outcome{}, digestErr
	}
	outcome := artifact.Outcome{Status: artifact.OutcomeCompleted, Steps: steps, EndNS: int64(cluster.Simulator().Now()), ObservationDigest: digest, Violations: violations}
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
		return outcome, nil
	}
	return outcome, nil
}

func applyAction(cluster *raftsim.Cluster, action artifact.Action) error {
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
	case artifact.ActionSnapshot:
		return cluster.Snapshot(action.Node, action.Data)
	case artifact.ActionPartition:
		return cluster.Partition(action.Groups...)
	case artifact.ActionHeal:
		cluster.Heal()
		return nil
	default:
		return fmt.Errorf("experiment: unknown action %q", action.Kind)
	}
}

func snapshotDigest(cluster *raftsim.Cluster, violations []check.Violation) (string, error) {
	snapshot := observationSnapshot{AtNS: int64(cluster.Simulator().Now()), Violations: slices.Clone(violations)}
	for _, id := range cluster.Members() {
		store, err := cluster.Store(id)
		if err != nil {
			return "", err
		}
		node := nodeSnapshot{ID: id, Up: true, Store: store}
		status, err := cluster.Status(id)
		if errors.Is(err, raftsim.ErrNodeDown) {
			node.Up = false
		} else if err != nil {
			return "", err
		} else {
			node.Status = &status
		}
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	return artifact.DigestJSON(snapshot)
}

func cloneAction(action artifact.Action) artifact.Action {
	action.Data = slices.Clone(action.Data)
	action.Groups = make([][]raft.NodeID, len(action.Groups))
	for index := range action.Groups {
		action.Groups[index] = slices.Clone(action.Groups[index])
	}
	return action
}
