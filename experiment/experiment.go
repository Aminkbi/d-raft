// Package experiment executes versioned scenarios against d-raft adapters.
package experiment

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

const FrontierSchemaVersion = "d-raft.reference-frontier/v1"
const ApplicationObservationSchemaVersion = "d-raft.reference-app-observation/v1"

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

type applicationNodeSnapshot struct {
	ID          raft.NodeID          `json:"id"`
	Up          bool                 `json:"up"`
	Status      *raft.Status         `json:"status,omitempty"`
	Store       raftsim.Store        `json:"store"`
	Application apporacle.Commitment `json:"application"`
}

type applicationObservationSnapshot struct {
	Schema            string                    `json:"schema"`
	ApplicationSchema string                    `json:"application_schema"`
	AtNS              int64                     `json:"at_ns"`
	Nodes             []applicationNodeSnapshot `json:"nodes"`
	Violations        []check.Violation         `json:"violations,omitempty"`
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

// ExecuteWithApplication runs the reference adapter under an explicit
// portable KV profile. Legacy Execute continues to treat proposal bytes as
// opaque and retains its existing observation schema.
func ExecuteWithApplication(scenario artifact.Scenario, configuration artifact.Configuration, decider decision.Decider, application apporacle.Config) (artifact.Outcome, error) {
	if err := artifact.ValidateExperiment(scenario, configuration); err != nil {
		return artifact.Outcome{}, err
	}
	if err := application.Validate(); err != nil {
		return artifact.Outcome{}, err
	}
	if err := validateApplicationScenario(scenario, true); err != nil {
		return artifact.Outcome{}, err
	}
	config := configuration.ClusterConfig(decider, nil)
	config.Application = &application
	cluster, err := raftsim.New(config)
	if err != nil {
		return artifact.Outcome{}, err
	}
	return executeScheduledApplication(cluster, scenario)
}

// ExecuteWithFrontier reruns the controlled reference adapter and, when the
// decider opens a choice, returns canonical bytes for the stable boundary
// immediately before the active event plus choices already consumed inside
// that event. Generic runners cannot use this contract implicitly.
func ExecuteWithFrontier(scenario artifact.Scenario, configuration artifact.Configuration, decider decision.Decider) (artifact.Outcome, []byte, error) {
	if err := artifact.ValidateExperiment(scenario, configuration); err != nil {
		return artifact.Outcome{}, nil, err
	}
	if decider == nil {
		return artifact.Outcome{}, nil, errors.New("experiment: frontier execution requires a decider")
	}
	recorder := decision.NewRecorder(decider)
	cluster, err := raftsim.NewPaused(configuration.ClusterConfig(recorder, nil))
	if err != nil {
		return artifact.Outcome{}, nil, err
	}
	preEvent, err := cluster.CanonicalState()
	if err != nil {
		return artifact.Outcome{}, nil, err
	}
	before := len(recorder.Tape().Entries)
	err = cluster.Bootstrap()
	if errors.Is(err, decision.ErrOpenChoice) {
		frontier, encodeErr := encodeFrontier(scenario, configuration, 0, preEvent, decisionsSince(recorder, before))
		if encodeErr != nil {
			return artifact.Outcome{}, nil, encodeErr
		}
		return artifact.Outcome{}, frontier, err
	}
	if err != nil {
		return artifact.Outcome{}, nil, err
	}
	return executeScheduledControlled(cluster, scenario, configuration, recorder)
}

func executeScheduled(cluster *raftsim.Cluster, scenario artifact.Scenario) (artifact.Outcome, error) {
	outcome, _, err := executeScheduledCore(cluster, scenario, artifact.Configuration{}, nil, false)
	return outcome, err
}

func executeScheduledApplication(cluster *raftsim.Cluster, scenario artifact.Scenario) (artifact.Outcome, error) {
	outcome, _, err := executeScheduledCore(cluster, scenario, artifact.Configuration{}, nil, true)
	return outcome, err
}

func executeScheduledControlled(cluster *raftsim.Cluster, scenario artifact.Scenario, configuration artifact.Configuration, recorder *decision.Recorder) (artifact.Outcome, []byte, error) {
	return executeScheduledCore(cluster, scenario, configuration, recorder, false)
}

func executeScheduledCore(cluster *raftsim.Cluster, scenario artifact.Scenario, configuration artifact.Configuration, recorder *decision.Recorder, application bool) (artifact.Outcome, []byte, error) {
	var actionErr error
	for _, source := range scenario.Actions {
		action := cloneAction(source)
		data, err := json.Marshal(action)
		if err != nil {
			return artifact.Outcome{}, nil, err
		}
		tag := sim.EventTag{Kind: sim.EventExternalAction, Data: data}
		if _, err := cluster.Simulator().ScheduleAtTagged(time.Duration(action.AtNS), tag, func(_ *sim.Simulator) {
			if actionErr == nil {
				if application && action.Kind == artifact.ActionSnapshot {
					actionErr = cluster.SnapshotApplication(action.Node)
				} else {
					actionErr = applyAction(cluster, action)
				}
			}
		}); err != nil {
			return artifact.Outcome{}, nil, err
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
		var preEvent []byte
		before := 0
		stepsBefore := steps
		if recorder != nil {
			var stateErr error
			preEvent, stateErr = cluster.CanonicalState()
			if stateErr != nil {
				return artifact.Outcome{}, nil, stateErr
			}
			before = len(recorder.Tape().Entries)
		}
		ran, err := cluster.Step()
		if ran {
			steps++
		}
		openErr := err
		if openErr == nil && errors.Is(actionErr, decision.ErrOpenChoice) {
			openErr = actionErr
		}
		if recorder != nil && errors.Is(openErr, decision.ErrOpenChoice) {
			frontier, encodeErr := encodeFrontier(scenario, configuration, stepsBefore, preEvent, decisionsSince(recorder, before))
			if encodeErr != nil {
				return artifact.Outcome{}, nil, encodeErr
			}
			return artifact.Outcome{}, frontier, openErr
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
		return artifact.Outcome{}, nil, runErr
	}

	violations := cluster.Violations()
	var digest string
	var digestErr error
	if application {
		digest, digestErr = applicationSnapshotDigest(cluster, violations)
	} else {
		digest, digestErr = snapshotDigest(cluster, violations)
	}
	if digestErr != nil {
		return artifact.Outcome{}, nil, digestErr
	}
	outcome := artifact.Outcome{Status: artifact.OutcomeCompleted, Steps: steps, EndNS: int64(cluster.Simulator().Now()), ObservationDigest: digest, Violations: violations}
	if len(violations) > 0 {
		outcome.Status = artifact.OutcomeViolation
		return outcome, nil, nil
	}
	if runErr != nil {
		outcome.Status = artifact.OutcomeError
		outcome.Error = runErr.Error()
		return outcome, nil, nil
	}
	if budgetExhausted {
		outcome.Status = artifact.OutcomeBudgetExhausted
		return outcome, nil, nil
	}
	return outcome, nil, nil
}

type frontierState struct {
	Schema         string                 `json:"schema"`
	Adapter        artifact.Adapter       `json:"adapter"`
	DecisionSchema string                 `json:"decision_schema"`
	CheckerSchema  string                 `json:"checker_schema"`
	MessageCodec   string                 `json:"message_codec"`
	Scenario       artifact.Scenario      `json:"scenario"`
	Configuration  artifact.Configuration `json:"configuration"`
	StepsUsed      uint64                 `json:"steps_used"`
	PreEvent       json.RawMessage        `json:"pre_event"`
	InEvent        []decision.Entry       `json:"in_event"`
}

func encodeFrontier(scenario artifact.Scenario, configuration artifact.Configuration, steps uint64, preEvent []byte, inEvent []decision.Entry) ([]byte, error) {
	if !json.Valid(preEvent) {
		return nil, errors.New("experiment: invalid canonical pre-event state")
	}
	return json.Marshal(frontierState{
		Schema:         FrontierSchemaVersion,
		Adapter:        artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		DecisionSchema: decision.SchemaVersion, CheckerSchema: check.SchemaVersion, MessageCodec: artifact.MessageCodecCurrent,
		Scenario: scenario, Configuration: configuration, StepsUsed: steps,
		PreEvent: append(json.RawMessage(nil), preEvent...), InEvent: inEvent,
	})
}

func decisionsSince(recorder *decision.Recorder, before int) []decision.Entry {
	tape := recorder.Tape()
	if before < 0 || before > len(tape.Entries) {
		return nil
	}
	return decision.CloneTape(decision.Tape{Schema: decision.SchemaVersion, Entries: tape.Entries[before:]}).Entries
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
	case artifact.ActionBeginMembership:
		if action.Node == "" {
			return cluster.BeginMembershipChange(action.Voters, action.Learners)
		}
		return cluster.BeginMembershipChangeTo(action.Node, action.Voters, action.Learners)
	case artifact.ActionFinalizeMembership:
		if action.Node == "" {
			return cluster.FinalizeMembershipChange()
		}
		return cluster.FinalizeMembershipChangeTo(action.Node)
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

func applicationSnapshotDigest(cluster *raftsim.Cluster, violations []check.Violation) (string, error) {
	snapshot := applicationObservationSnapshot{
		Schema: ApplicationObservationSchemaVersion, ApplicationSchema: apporacle.CommandSchema,
		AtNS: int64(cluster.Simulator().Now()), Violations: slices.Clone(violations),
	}
	for _, id := range cluster.Members() {
		store, err := cluster.Store(id)
		if err != nil {
			return "", err
		}
		commitment, err := cluster.ApplicationCommitment(id)
		if err != nil {
			return "", err
		}
		node := applicationNodeSnapshot{ID: id, Up: true, Store: store, Application: commitment}
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

func validateApplicationScenario(scenario artifact.Scenario, snapshots bool) error {
	seen := make(map[apporacle.CommandID]struct{})
	for _, action := range scenario.Actions {
		switch action.Kind {
		case artifact.ActionPropose:
			command, err := apporacle.DecodeCommand(action.Data)
			if err != nil {
				return fmt.Errorf("experiment: invalid portable proposal: %w", err)
			}
			if _, exists := seen[command.ID]; exists {
				return fmt.Errorf("experiment: %w: %s", apporacle.ErrDuplicateCommand, command.ID)
			}
			seen[command.ID] = struct{}{}
		case artifact.ActionSnapshot:
			if !snapshots || len(action.Data) != 0 {
				return fmt.Errorf("experiment: portable snapshot must be generated by the adapter")
			}
		}
	}
	return nil
}

func cloneAction(action artifact.Action) artifact.Action {
	action.Data = slices.Clone(action.Data)
	action.Voters = slices.Clone(action.Voters)
	action.Learners = slices.Clone(action.Learners)
	action.Groups = make([][]raft.NodeID, len(action.Groups))
	for index := range action.Groups {
		action.Groups[index] = slices.Clone(action.Groups[index])
	}
	return action
}
