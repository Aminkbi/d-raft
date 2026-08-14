package experiment

import (
	"fmt"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/raft"
)

// PortableFaultsV1 is the stable public name of the first canonical
// cross-adapter workload. Its timing and payloads are part of the scenario
// version and must not be changed in place.
const PortableFaultsV1 = "portable-faults-v1"

// PortableFaultsV1DecisionSeed is part of the immutable v1 experiment. A
// later corpus that changes the semantic choice stream needs a new name.
const PortableFaultsV1DecisionSeed uint64 = 1

// Canonical returns one immutable, named experiment input. The caller owns the
// returned values and must obtain the matching semantic seed from
// CanonicalDecisionSeed.
func Canonical(name string) (artifact.Scenario, artifact.Configuration, error) {
	switch name {
	case PortableFaultsV1:
		return portableFaultsV1()
	default:
		return artifact.Scenario{}, artifact.Configuration{}, fmt.Errorf("experiment: unknown canonical scenario %q", name)
	}
}

// CanonicalDecisionSeed returns the decision seed fixed by a named experiment.
func CanonicalDecisionSeed(name string) (uint64, error) {
	switch name {
	case PortableFaultsV1:
		return PortableFaultsV1DecisionSeed, nil
	default:
		return 0, fmt.Errorf("experiment: unknown canonical scenario %q", name)
	}
}

// CanonicalName recognizes an immutable scenario identity. The caller can use
// VerifyCanonical to reject an artifact that claims the identity but changes
// its inputs.
func CanonicalName(scenario artifact.Scenario) (string, bool) {
	if scenario.ID == "semantic/portable-faults" && scenario.Version == "1" {
		return PortableFaultsV1, true
	}
	return "", false
}

// VerifyCanonical binds a public name to its complete scenario,
// configuration, and semantic choice seed.
func VerifyCanonical(name string, scenario artifact.Scenario, configuration artifact.Configuration, seed uint64) error {
	expectedScenario, expectedConfiguration, err := Canonical(name)
	if err != nil {
		return err
	}
	expectedSeed, err := CanonicalDecisionSeed(name)
	if err != nil {
		return err
	}
	expectedDigest, err := canonicalInputDigest(expectedScenario, expectedConfiguration, expectedSeed)
	if err != nil {
		return err
	}
	actualDigest, err := canonicalInputDigest(scenario, configuration, seed)
	if err != nil {
		return err
	}
	if actualDigest != expectedDigest {
		return fmt.Errorf("experiment: canonical scenario %q input mismatch", name)
	}
	return nil
}

func canonicalInputDigest(scenario artifact.Scenario, configuration artifact.Configuration, seed uint64) (string, error) {
	return artifact.DigestJSON(struct {
		Scenario      artifact.Scenario      `json:"scenario"`
		Configuration artifact.Configuration `json:"configuration"`
		DecisionSeed  artifact.Uint64        `json:"decision_seed"`
	}{Scenario: scenario, Configuration: configuration, DecisionSeed: artifact.Uint64(seed)})
}

func portableFaultsV1() (artifact.Scenario, artifact.Configuration, error) {
	actions := make([]artifact.Action, 0, 8)
	appendPut := func(at time.Duration, ordinal byte) error {
		encoded, err := apporacle.EncodeCommand(apporacle.Command{
			ID:        apporacle.CommandID{15: ordinal},
			Operation: apporacle.Put,
			Key:       []byte(fmt.Sprintf("key-%d", ordinal)),
			Value:     []byte(fmt.Sprintf("value-%d", ordinal)),
		})
		if err != nil {
			return err
		}
		actions = append(actions, artifact.Action{AtNS: int64(at), Kind: artifact.ActionPropose, Data: encoded})
		return nil
	}

	if err := appendPut(600*time.Millisecond, 1); err != nil {
		return artifact.Scenario{}, artifact.Configuration{}, err
	}
	actions = append(actions, artifact.Action{
		AtNS: int64(800 * time.Millisecond), Kind: artifact.ActionPartition,
		Groups: [][]raft.NodeID{{"a", "b"}, {"c"}},
	})
	if err := appendPut(time.Second, 2); err != nil {
		return artifact.Scenario{}, artifact.Configuration{}, err
	}
	actions = append(actions, artifact.Action{AtNS: int64(1400 * time.Millisecond), Kind: artifact.ActionHeal})
	actions = append(actions, artifact.Action{AtNS: int64(1600 * time.Millisecond), Kind: artifact.ActionCrash, Node: "c"})
	if err := appendPut(1800*time.Millisecond, 3); err != nil {
		return artifact.Scenario{}, artifact.Configuration{}, err
	}
	actions = append(actions, artifact.Action{AtNS: int64(2200 * time.Millisecond), Kind: artifact.ActionRestart, Node: "c"})
	if err := appendPut(2400*time.Millisecond, 4); err != nil {
		return artifact.Scenario{}, artifact.Configuration{}, err
	}

	scenario := artifact.Scenario{
		ID: "semantic/portable-faults", Version: "1",
		DurationNS: int64(5 * time.Second), MaxSteps: 500_000,
		Actions: actions,
	}
	configuration := artifact.Configuration{
		Members: []raft.NodeID{"a", "b", "c"}, InfrastructureSeed: 19,
		NetworkMinLatencyNS: int64(time.Millisecond), NetworkMaxLatencyNS: int64(5 * time.Millisecond),
		NetworkLossProbability: 0.02,
		ElectionTimeoutMinNS:   int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
		HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: int64(time.Millisecond),
		StopOnViolation: true,
	}
	if err := artifact.ValidateExperiment(scenario, configuration); err != nil {
		return artifact.Scenario{}, artifact.Configuration{}, fmt.Errorf("experiment: invalid canonical scenario %q: %w", PortableFaultsV1, err)
	}
	return scenario, configuration, nil
}
