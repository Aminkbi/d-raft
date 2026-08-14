// Package artifact defines d-raft's self-describing, versioned run artifact.
package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"time"

	sim "github.com/aminkbi/d-raft"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
)

const (
	SchemaV1                  = "d-raft.run/v1"
	SchemaVersion             = "d-raft.run/v2"
	ReferenceAdapterID        = "d-raft/reference"
	ReferenceAdapterV1        = "1"
	ReferenceAdapterV2        = "2"
	ReferenceAdapterCurrent   = ReferenceAdapterV2
	MessageCodecV1            = "d-raft.raft-message/json-v1"
	MessageCodecV2            = "d-raft.raft-message/json-v2"
	MessageCodecCurrent       = MessageCodecV2
	ObservationSchemaV1       = "d-raft.observation/v1"
	ObservationSchemaV2       = "d-raft.observation/v2"
	ObservationSchemaCurrent  = ObservationSchemaV2
	DefaultMaxArtifactBytes   = 64 << 20
	MaxMembers                = 31
	MaxActions                = 10_000
	MaxDecisions              = 100_000
	MaxActionPayloadBytes     = 1 << 20
	MaxDecisionContextBytes   = 1 << 20
	MaxDecisionOptions        = 1_024
	MaxTotalDecisionOptions   = 1_000_000
	MaxDecisionTextBytes      = 4 << 10
	MaxViolations             = 1_024
	MaxViolationEvidenceBytes = 1 << 20
	MaxOutcomeErrorBytes      = 4 << 10
	MaxScenarioSteps          = 1_000_000
	MaxVirtualDurationNS      = int64(24 * time.Hour)
)

var (
	ErrInvalidArtifact  = errors.New("artifact: invalid run artifact")
	ErrArtifactTooLarge = errors.New("artifact: run artifact exceeds size limit")
)

// Uint64 encodes a full-width unsigned value as a decimal JSON string so
// generic JSON tooling cannot silently round it through float64.
type Uint64 uint64

func (value Uint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatUint(uint64(value), 10))
}

func (value *Uint64) UnmarshalJSON(data []byte) error {
	if value == nil {
		return errors.New("artifact: nil Uint64 receiver")
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return fmt.Errorf("%w: uint64 must be a decimal string", ErrInvalidArtifact)
	}
	parsed, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != encoded {
		return fmt.Errorf("%w: invalid canonical uint64 %q", ErrInvalidArtifact, encoded)
	}
	*value = Uint64(parsed)
	return nil
}

// ActionKind identifies an external scenario operation.
type ActionKind string

const (
	ActionPropose               ActionKind = "propose"
	ActionCrash                 ActionKind = "crash"
	ActionRestart               ActionKind = "restart"
	ActionCrashAfterNextPersist ActionKind = "crash_after_next_persist"
	ActionSnapshot              ActionKind = "snapshot"
	ActionPartition             ActionKind = "partition"
	ActionHeal                  ActionKind = "heal"
)

// Action is one externally supplied scenario operation at virtual time AtNS.
// Equal-time actions execute in their listed order after events already armed
// while the cluster was constructed.
type Action struct {
	AtNS   int64           `json:"at_ns"`
	Kind   ActionKind      `json:"kind"`
	Node   raft.NodeID     `json:"node,omitempty"`
	Data   []byte          `json:"data,omitempty"`
	Groups [][]raft.NodeID `json:"groups,omitempty"`
}

// Scenario fixes a named action sequence and virtual run horizon.
type Scenario struct {
	ID         string   `json:"id"`
	Version    string   `json:"version"`
	DurationNS int64    `json:"duration_ns"`
	MaxSteps   uint64   `json:"max_steps"`
	Actions    []Action `json:"actions,omitempty"`
}

// Adapter identifies the protocol implementation driven by a run.
type Adapter struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// Configuration is the stable JSON form of raftsim.Config.
type Configuration struct {
	Members                []raft.NodeID `json:"members"`
	InfrastructureSeed     Uint64        `json:"infrastructure_seed"`
	NetworkMinLatencyNS    int64         `json:"network_min_latency_ns"`
	NetworkMaxLatencyNS    int64         `json:"network_max_latency_ns"`
	NetworkLossProbability float64       `json:"network_loss_probability"`
	ElectionTimeoutMinNS   int64         `json:"election_timeout_min_ns"`
	ElectionTimeoutMaxNS   int64         `json:"election_timeout_max_ns"`
	HeartbeatIntervalNS    int64         `json:"heartbeat_interval_ns"`
	StorageLatencyNS       int64         `json:"storage_latency_ns"`
	StopOnViolation        bool          `json:"stop_on_violation"`
}

// ConfigurationFrom converts a cluster configuration to its stable form.
func ConfigurationFrom(config raftsim.Config) Configuration {
	members := slices.Clone(config.Members)
	slices.Sort(members)
	return Configuration{
		Members: members, InfrastructureSeed: Uint64(config.Seed),
		NetworkMinLatencyNS: int64(config.Network.MinLatency), NetworkMaxLatencyNS: int64(config.Network.MaxLatency),
		NetworkLossProbability: config.Network.LossProbability,
		ElectionTimeoutMinNS:   int64(config.ElectionTimeoutMin), ElectionTimeoutMaxNS: int64(config.ElectionTimeoutMax),
		HeartbeatIntervalNS: int64(config.HeartbeatInterval), StorageLatencyNS: int64(config.StorageLatency),
		StopOnViolation: config.StopOnViolation,
	}
}

// ClusterConfig reconstructs the executable reference-harness configuration.
func (c Configuration) ClusterConfig(decider decision.Decider, trace sim.TraceSink) raftsim.Config {
	return raftsim.Config{
		Members: slices.Clone(c.Members), Seed: uint64(c.InfrastructureSeed),
		Network:            sim.LinkConfig{MinLatency: time.Duration(c.NetworkMinLatencyNS), MaxLatency: time.Duration(c.NetworkMaxLatencyNS), LossProbability: c.NetworkLossProbability},
		ElectionTimeoutMin: time.Duration(c.ElectionTimeoutMinNS), ElectionTimeoutMax: time.Duration(c.ElectionTimeoutMaxNS),
		HeartbeatInterval: time.Duration(c.HeartbeatIntervalNS), StorageLatency: time.Duration(c.StorageLatencyNS),
		StopOnViolation: c.StopOnViolation, Decider: decider, Trace: trace,
	}
}

// Reproducibility identifies the decision stream and build environment.
type Reproducibility struct {
	DecisionSeed      Uint64 `json:"decision_seed"`
	GitRevision       string `json:"git_revision"`
	GitModified       bool   `json:"git_modified"`
	GoVersion         string `json:"go_version"`
	DecisionSchema    string `json:"decision_schema"`
	CheckerSchema     string `json:"checker_schema"`
	MessageCodec      string `json:"message_codec"`
	ObservationSchema string `json:"observation_schema"`
}

// OutcomeStatus classifies a completed, violating, or operationally failed run.
type OutcomeStatus string

const (
	OutcomeCompleted       OutcomeStatus = "completed"
	OutcomeViolation       OutcomeStatus = "violation"
	OutcomeError           OutcomeStatus = "error"
	OutcomeBudgetExhausted OutcomeStatus = "budget_exhausted"
)

// Outcome is the independently verifiable result embedded in an artifact.
type Outcome struct {
	Status            OutcomeStatus     `json:"status"`
	Steps             uint64            `json:"steps"`
	EndNS             int64             `json:"end_ns"`
	ObservationDigest string            `json:"observation_digest"`
	Error             string            `json:"error,omitempty"`
	Violations        []check.Violation `json:"violations,omitempty"`
}

// Run is one complete versioned d-raft run artifact.
type Run struct {
	Schema          string          `json:"schema"`
	Scenario        Scenario        `json:"scenario"`
	Adapter         Adapter         `json:"adapter"`
	Configuration   Configuration   `json:"configuration"`
	Reproducibility Reproducibility `json:"reproducibility"`
	Decisions       decision.Tape   `json:"decisions"`
	Outcome         Outcome         `json:"outcome"`
}

// NewReproducibility captures deterministic inputs and available build data.
func NewReproducibility(seed uint64) Reproducibility {
	revision, modified := buildRevision()
	return Reproducibility{DecisionSeed: Uint64(seed), GitRevision: revision, GitModified: modified, GoVersion: runtime.Version(), DecisionSchema: decision.SchemaVersion, CheckerSchema: check.SchemaVersion, MessageCodec: MessageCodecCurrent, ObservationSchema: ObservationSchemaCurrent}
}

func (r Run) Validate() error {
	if r.Schema != SchemaV1 && r.Schema != SchemaVersion {
		return fmt.Errorf("%w: schema %q", ErrInvalidArtifact, r.Schema)
	}
	if r.Adapter.ID == "" || r.Adapter.Version == "" {
		return fmt.Errorf("%w: empty adapter identity", ErrInvalidArtifact)
	}
	if !validIdentifier(r.Adapter.ID, true, 128) || !validIdentifier(r.Adapter.Version, false, 64) {
		return fmt.Errorf("%w: invalid adapter identity", ErrInvalidArtifact)
	}
	if err := validateConfiguration(r.Configuration); err != nil {
		return err
	}
	if err := validateScenario(r.Scenario, r.Configuration.Members, r.Schema == SchemaVersion); err != nil {
		return err
	}
	if r.Reproducibility.DecisionSchema != decision.SchemaVersion ||
		!validMetadata(r.Reproducibility.CheckerSchema, 128) ||
		!validMetadata(r.Reproducibility.MessageCodec, 128) ||
		!validMetadata(r.Reproducibility.ObservationSchema, 128) ||
		!validMetadata(r.Reproducibility.GoVersion, 128) ||
		!validMetadata(r.Reproducibility.GitRevision, 256) {
		return fmt.Errorf("%w: incomplete reproducibility metadata", ErrInvalidArtifact)
	}
	if r.Adapter.ID == ReferenceAdapterID {
		switch r.Adapter.Version {
		case ReferenceAdapterV1:
			if r.Schema != SchemaV1 || r.Reproducibility.MessageCodec != MessageCodecV1 || r.Reproducibility.CheckerSchema != check.SchemaV1 || r.Reproducibility.ObservationSchema != ObservationSchemaV1 {
				return fmt.Errorf("%w: inconsistent reference adapter v1 schemas", ErrInvalidArtifact)
			}
		case ReferenceAdapterV2:
			if r.Schema != SchemaVersion || r.Reproducibility.MessageCodec != MessageCodecV2 || r.Reproducibility.CheckerSchema != check.SchemaVersion || r.Reproducibility.ObservationSchema != ObservationSchemaV2 {
				return fmt.Errorf("%w: inconsistent reference adapter v2 schemas", ErrInvalidArtifact)
			}
		default:
			return fmt.Errorf("%w: unsupported reference adapter version %q", ErrInvalidArtifact, r.Adapter.Version)
		}
	}
	if len(r.Decisions.Entries) > MaxDecisions {
		return fmt.Errorf("%w: too many decisions", ErrInvalidArtifact)
	}
	if r.Schema == SchemaVersion {
		if err := validateResourceBudget(r, DefaultMaxArtifactBytes); err != nil {
			return err
		}
	}
	if _, err := decision.NewTapeDecider(r.Decisions); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArtifact, err)
	}
	if r.Outcome.Steps > r.Scenario.MaxSteps || r.Outcome.EndNS < 0 || r.Outcome.EndNS > r.Scenario.DurationNS || !validDigest(r.Outcome.ObservationDigest) || len(r.Outcome.Error) > MaxOutcomeErrorBytes || len(r.Outcome.Violations) > MaxViolations {
		return fmt.Errorf("%w: invalid outcome counters or digest", ErrInvalidArtifact)
	}
	memberSet := make(map[raft.NodeID]struct{}, len(r.Configuration.Members))
	for _, member := range r.Configuration.Members {
		memberSet[member] = struct{}{}
	}
	for index, violation := range r.Outcome.Violations {
		validationErr := check.ValidateViolation(violation)
		if validationErr == nil && r.Adapter.ID == ReferenceAdapterID {
			validationErr = check.ValidateViolationForSchema(r.Reproducibility.CheckerSchema, violation)
		}
		if !validIdentifier(violation.ID, true, 128) || violation.AtNS < 0 || violation.AtNS > r.Outcome.EndNS || len(violation.Evidence) > MaxViolationEvidenceBytes || validationErr != nil {
			return fmt.Errorf("%w: invalid violation witness %d", ErrInvalidArtifact, index)
		}
		for _, node := range violation.Nodes {
			if _, exists := memberSet[node]; !exists {
				return fmt.Errorf("%w: violation witness %d names unknown node %q", ErrInvalidArtifact, index, node)
			}
		}
	}
	switch r.Outcome.Status {
	case OutcomeCompleted:
		if r.Outcome.EndNS != r.Scenario.DurationNS || r.Outcome.Error != "" || len(r.Outcome.Violations) != 0 {
			return fmt.Errorf("%w: completed outcome carries failure data", ErrInvalidArtifact)
		}
	case OutcomeViolation:
		if r.Outcome.Error != "" || len(r.Outcome.Violations) == 0 {
			return fmt.Errorf("%w: violation outcome has no witness", ErrInvalidArtifact)
		}
	case OutcomeError:
		if r.Outcome.Error == "" || len(r.Outcome.Violations) != 0 {
			return fmt.Errorf("%w: error outcome has no error", ErrInvalidArtifact)
		}
	case OutcomeBudgetExhausted:
		if r.Outcome.Steps != r.Scenario.MaxSteps || r.Outcome.Error != "" || len(r.Outcome.Violations) != 0 {
			return fmt.Errorf("%w: inconsistent budget-exhausted outcome", ErrInvalidArtifact)
		}
	default:
		return fmt.Errorf("%w: unknown outcome %q", ErrInvalidArtifact, r.Outcome.Status)
	}
	return nil
}

// ValidateExperiment checks the portable inputs needed to start a clean run.
func ValidateExperiment(scenario Scenario, configuration Configuration) error {
	if err := validateConfiguration(configuration); err != nil {
		return err
	}
	return validateScenario(scenario, configuration.Members, true)
}

// Encode validates and writes one bounded JSON artifact.
func Encode(writer io.Writer, run Run) error {
	return encodeWithLimit(writer, run, DefaultMaxArtifactBytes)
}

func encodeWithLimit(writer io.Writer, run Run, maximum int) error {
	if writer == nil {
		return fmt.Errorf("%w: nil writer", ErrInvalidArtifact)
	}
	if maximum <= 0 {
		return fmt.Errorf("%w: non-positive size limit", ErrInvalidArtifact)
	}
	if err := run.Validate(); err != nil {
		return err
	}
	if run.Schema == SchemaVersion {
		if err := validateResourceBudget(run, maximum); err != nil {
			return err
		}
	}
	encoded := boundedBuffer{maximum: maximum}
	if err := encodeRun(&encoded, run); err != nil {
		return err
	}
	written, err := writer.Write(encoded.buffer.Bytes())
	if err != nil {
		return err
	}
	if written != encoded.buffer.Len() {
		return io.ErrShortWrite
	}
	return nil
}

type boundedBuffer struct {
	buffer  bytes.Buffer
	maximum int
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	if len(data) > b.maximum-b.buffer.Len() {
		return 0, ErrArtifactTooLarge
	}
	return b.buffer.Write(data)
}

type jsonLiteral string

func encodeRun(writer io.Writer, run Run) error {
	if err := writeJSONParts(writer, jsonLiteral(`{"schema":`), run.Schema, jsonLiteral(`,"scenario":`)); err != nil {
		return err
	}
	if err := encodeScenario(writer, run.Scenario); err != nil {
		return err
	}
	if err := writeJSONParts(writer, jsonLiteral(`,"adapter":`), run.Adapter, jsonLiteral(`,"configuration":`), run.Configuration, jsonLiteral(`,"reproducibility":`), run.Reproducibility, jsonLiteral(`,"decisions":`)); err != nil {
		return err
	}
	if err := encodeTape(writer, run.Decisions); err != nil {
		return err
	}
	if err := writeLiteral(writer, `,"outcome":`); err != nil {
		return err
	}
	if err := encodeOutcome(writer, run.Outcome); err != nil {
		return err
	}
	return writeLiteral(writer, "}\n")
}

func encodeScenario(writer io.Writer, scenario Scenario) error {
	if err := writeJSONParts(writer, jsonLiteral(`{"id":`), scenario.ID, jsonLiteral(`,"version":`), scenario.Version, jsonLiteral(`,"duration_ns":`), scenario.DurationNS, jsonLiteral(`,"max_steps":`), scenario.MaxSteps); err != nil {
		return err
	}
	if len(scenario.Actions) > 0 {
		if err := writeLiteral(writer, `,"actions":[`); err != nil {
			return err
		}
		for index, action := range scenario.Actions {
			if index > 0 {
				if err := writeLiteral(writer, ","); err != nil {
					return err
				}
			}
			if err := writeJSONValue(writer, action); err != nil {
				return err
			}
		}
		if err := writeLiteral(writer, "]"); err != nil {
			return err
		}
	}
	return writeLiteral(writer, "}")
}

func encodeTape(writer io.Writer, tape decision.Tape) error {
	if err := writeJSONParts(writer, jsonLiteral(`{"schema":`), tape.Schema, jsonLiteral(`,"entries":`)); err != nil {
		return err
	}
	if tape.Entries == nil {
		return writeLiteral(writer, "null}")
	}
	if err := writeLiteral(writer, "["); err != nil {
		return err
	}
	for index, entry := range tape.Entries {
		if index > 0 {
			if err := writeLiteral(writer, ","); err != nil {
				return err
			}
		}
		if err := writeJSONValue(writer, entry); err != nil {
			return err
		}
	}
	return writeLiteral(writer, "]}")
}

func encodeOutcome(writer io.Writer, outcome Outcome) error {
	if err := writeJSONParts(writer, jsonLiteral(`{"status":`), outcome.Status, jsonLiteral(`,"steps":`), outcome.Steps, jsonLiteral(`,"end_ns":`), outcome.EndNS, jsonLiteral(`,"observation_digest":`), outcome.ObservationDigest); err != nil {
		return err
	}
	if outcome.Error != "" {
		if err := writeJSONParts(writer, jsonLiteral(`,"error":`), outcome.Error); err != nil {
			return err
		}
	}
	if len(outcome.Violations) > 0 {
		if err := writeLiteral(writer, `,"violations":[`); err != nil {
			return err
		}
		for index, violation := range outcome.Violations {
			if index > 0 {
				if err := writeLiteral(writer, ","); err != nil {
					return err
				}
			}
			if err := writeJSONValue(writer, violation); err != nil {
				return err
			}
		}
		if err := writeLiteral(writer, "]"); err != nil {
			return err
		}
	}
	return writeLiteral(writer, "}")
}

func writeJSONParts(writer io.Writer, parts ...any) error {
	for _, part := range parts {
		if literal, ok := part.(jsonLiteral); ok {
			if err := writeLiteral(writer, string(literal)); err != nil {
				return err
			}
			continue
		}
		if err := writeJSONValue(writer, part); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONValue(writer io.Writer, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	written, err := writer.Write(encoded)
	if err != nil {
		return err
	}
	if written != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}

func writeLiteral(writer io.Writer, literal string) error {
	written, err := io.WriteString(writer, literal)
	if err != nil {
		return err
	}
	if written != len(literal) {
		return io.ErrShortWrite
	}
	return nil
}

// Decode strictly reads and validates one bounded JSON artifact.
func Decode(reader io.Reader) (Run, error) {
	return decodeWithLimit(reader, DefaultMaxArtifactBytes)
}

func decodeWithLimit(reader io.Reader, maximum int) (Run, error) {
	if reader == nil {
		return Run{}, fmt.Errorf("%w: nil reader", ErrInvalidArtifact)
	}
	if maximum <= 0 {
		return Run{}, fmt.Errorf("%w: non-positive size limit", ErrInvalidArtifact)
	}
	data, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		return Run{}, err
	}
	if len(data) > maximum {
		return Run{}, ErrArtifactTooLarge
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var run Run
	if err := decoder.Decode(&run); err != nil {
		return Run{}, fmt.Errorf("%w: %v", ErrInvalidArtifact, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Run{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidArtifact)
	}
	if err := run.Validate(); err != nil {
		return Run{}, err
	}
	return run, nil
}

// OutcomesEqual compares every result field that exact replay must preserve.
func OutcomesEqual(left, right Outcome) bool {
	if left.Status != right.Status || left.Steps != right.Steps || left.EndNS != right.EndNS || left.ObservationDigest != right.ObservationDigest || left.Error != right.Error || len(left.Violations) != len(right.Violations) {
		return false
	}
	for index := range left.Violations {
		if left.Violations[index].Fingerprint != right.Violations[index].Fingerprint {
			return false
		}
	}
	return true
}

// DigestJSON returns a full SHA-256 digest of Go's canonical JSON encoding.
func DigestJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validateScenario(scenario Scenario, members []raft.NodeID, allowSnapshots bool) error {
	if !validIdentifier(scenario.ID, true, 128) || !validIdentifier(scenario.Version, false, 64) || scenario.DurationNS < 0 || scenario.DurationNS > MaxVirtualDurationNS || scenario.MaxSteps == 0 || scenario.MaxSteps > MaxScenarioSteps || len(scenario.Actions) > MaxActions {
		return fmt.Errorf("%w: invalid scenario identity or duration", ErrInvalidArtifact)
	}
	memberSet := make(map[raft.NodeID]struct{}, len(members))
	for _, member := range members {
		memberSet[member] = struct{}{}
	}
	previous := int64(-1)
	for index, action := range scenario.Actions {
		if action.AtNS < previous || action.AtNS < 0 || action.AtNS > scenario.DurationNS || len(action.Data) > MaxActionPayloadBytes {
			return fmt.Errorf("%w: action %d is out of order or range", ErrInvalidArtifact, index)
		}
		previous = action.AtNS
		switch action.Kind {
		case ActionPropose:
			if len(action.Groups) != 0 {
				return fmt.Errorf("%w: proposal action %d carries partition groups", ErrInvalidArtifact, index)
			}
			if action.Node != "" {
				if _, ok := memberSet[action.Node]; !ok {
					return fmt.Errorf("%w: action %d has unknown node", ErrInvalidArtifact, index)
				}
			}
		case ActionCrash, ActionRestart, ActionCrashAfterNextPersist:
			if len(action.Data) != 0 || len(action.Groups) != 0 {
				return fmt.Errorf("%w: process action %d carries unrelated fields", ErrInvalidArtifact, index)
			}
			if _, ok := memberSet[action.Node]; !ok {
				return fmt.Errorf("%w: action %d has unknown node", ErrInvalidArtifact, index)
			}
		case ActionSnapshot:
			if !allowSnapshots || len(action.Groups) != 0 {
				return fmt.Errorf("%w: snapshot action %d is unsupported or carries partition groups", ErrInvalidArtifact, index)
			}
			if _, ok := memberSet[action.Node]; !ok {
				return fmt.Errorf("%w: action %d has unknown node", ErrInvalidArtifact, index)
			}
		case ActionPartition:
			if action.Node != "" || len(action.Data) != 0 || len(action.Groups) == 0 {
				return fmt.Errorf("%w: action %d has no partition groups", ErrInvalidArtifact, index)
			}
			partitioned := make(map[raft.NodeID]struct{}, len(members))
			for groupIndex, group := range action.Groups {
				if len(group) == 0 {
					return fmt.Errorf("%w: action %d has an empty partition group", ErrInvalidArtifact, index)
				}
				if !slices.IsSorted(group) {
					return fmt.Errorf("%w: action %d partition group %d is not canonical", ErrInvalidArtifact, index, groupIndex)
				}
				for _, node := range group {
					if _, ok := memberSet[node]; !ok {
						return fmt.Errorf("%w: action %d has unknown partition node", ErrInvalidArtifact, index)
					}
					if _, exists := partitioned[node]; exists {
						return fmt.Errorf("%w: action %d repeats partition node %q", ErrInvalidArtifact, index, node)
					}
					partitioned[node] = struct{}{}
				}
			}
			if !partitionGroupsSorted(action.Groups) {
				return fmt.Errorf("%w: action %d partition groups are not canonical", ErrInvalidArtifact, index)
			}
		case ActionHeal:
			if action.Node != "" || len(action.Data) != 0 || len(action.Groups) != 0 {
				return fmt.Errorf("%w: heal action %d carries unrelated fields", ErrInvalidArtifact, index)
			}
		default:
			return fmt.Errorf("%w: action %d has unknown kind %q", ErrInvalidArtifact, index, action.Kind)
		}
	}
	return nil
}

func validateConfiguration(config Configuration) error {
	if len(config.Members) == 0 || len(config.Members) > MaxMembers || !slices.IsSorted(config.Members) || config.NetworkMinLatencyNS < 0 || config.NetworkMaxLatencyNS < config.NetworkMinLatencyNS || config.NetworkMaxLatencyNS > MaxVirtualDurationNS || config.ElectionTimeoutMinNS <= 0 || config.ElectionTimeoutMaxNS < config.ElectionTimeoutMinNS || config.ElectionTimeoutMaxNS > MaxVirtualDurationNS || config.HeartbeatIntervalNS <= 0 || config.HeartbeatIntervalNS >= config.ElectionTimeoutMinNS || config.StorageLatencyNS < 0 || config.StorageLatencyNS > MaxVirtualDurationNS || math.IsNaN(config.NetworkLossProbability) || config.NetworkLossProbability < 0 || config.NetworkLossProbability > 1 {
		return fmt.Errorf("%w: invalid cluster configuration", ErrInvalidArtifact)
	}
	canonical := slices.Clone(config.Members)
	slices.Sort(canonical)
	for index, member := range canonical {
		if !validIdentifier(string(member), false, 64) || index > 0 && member == canonical[index-1] {
			return fmt.Errorf("%w: invalid membership", ErrInvalidArtifact)
		}
	}
	return nil
}

func validIdentifier(value string, allowSlash bool, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		if index > 0 && (character == '-' || character == '_' || character == '.' || character == '+' || allowSlash && character == '/') {
			continue
		}
		return false
	}
	return true
}

func validMetadata(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character < 0x20 || character > 0x7e {
			return false
		}
	}
	return true
}

func validateResourceBudget(run Run, maximum int) error {
	if maximum <= 0 {
		return fmt.Errorf("%w: non-positive size limit", ErrInvalidArtifact)
	}
	budget := maximum / 2
	used := 0
	add := func(size int) error {
		if size < 0 || size > budget-used {
			return ErrArtifactTooLarge
		}
		used += size
		return nil
	}
	stringsToCount := []string{
		run.Schema, run.Scenario.ID, run.Scenario.Version, run.Adapter.ID, run.Adapter.Version,
		run.Reproducibility.GitRevision, run.Reproducibility.GoVersion, run.Reproducibility.DecisionSchema,
		run.Reproducibility.CheckerSchema, run.Reproducibility.MessageCodec, run.Reproducibility.ObservationSchema,
		run.Decisions.Schema, string(run.Outcome.Status), run.Outcome.Error, run.Outcome.ObservationDigest,
	}
	for _, value := range stringsToCount {
		if err := add(len(value)); err != nil {
			return err
		}
	}
	for _, member := range run.Configuration.Members {
		if err := add(len(member)); err != nil {
			return err
		}
	}
	for _, action := range run.Scenario.Actions {
		if err := add(len(action.Kind) + len(action.Node) + len(action.Data)); err != nil {
			return err
		}
		for _, group := range action.Groups {
			for _, member := range group {
				if err := add(len(member)); err != nil {
					return err
				}
			}
		}
	}
	totalOptions := 0
	for _, entry := range run.Decisions.Entries {
		choice := entry.Choice
		if len(choice.Context) > MaxDecisionContextBytes || len(choice.Options) > MaxDecisionOptions || len(choice.ID) > MaxDecisionTextBytes || len(choice.Kind) > MaxDecisionTextBytes || len(entry.Selection.Option) > MaxDecisionTextBytes {
			return fmt.Errorf("%w: decision exceeds resource limit", ErrInvalidArtifact)
		}
		if len(choice.Options) > MaxTotalDecisionOptions-totalOptions {
			return fmt.Errorf("%w: too many decision options", ErrInvalidArtifact)
		}
		totalOptions += len(choice.Options)
		if err := add(len(choice.ID) + len(choice.Kind) + len(choice.Context) + len(entry.DomainDigest) + len(entry.ContextDigest) + len(entry.Selection.Option)); err != nil {
			return err
		}
		for _, option := range choice.Options {
			if len(option.ID) > MaxDecisionTextBytes {
				return fmt.Errorf("%w: decision option exceeds resource limit", ErrInvalidArtifact)
			}
			if err := add(len(option.ID)); err != nil {
				return err
			}
		}
	}
	for _, violation := range run.Outcome.Violations {
		if err := add(len(violation.ID) + len(violation.Evidence) + len(violation.Fingerprint)); err != nil {
			return err
		}
		for _, node := range violation.Nodes {
			if err := add(len(node)); err != nil {
				return err
			}
		}
	}
	return nil
}

func partitionGroupsSorted(groups [][]raft.NodeID) bool {
	for index := 1; index < len(groups); index++ {
		left, right := groups[index-1], groups[index]
		limit := min(len(left), len(right))
		comparison := 0
		for item := 0; item < limit; item++ {
			if left[item] < right[item] {
				comparison = -1
				break
			}
			if left[item] > right[item] {
				comparison = 1
				break
			}
		}
		if comparison == 0 {
			if len(left) < len(right) {
				comparison = -1
			} else if len(left) > len(right) {
				comparison = 1
			}
		}
		if comparison >= 0 {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func buildRevision() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown", false
	}
	revision := "unknown"
	modified := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return revision, modified
}
