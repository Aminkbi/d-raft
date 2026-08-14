package semanticplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/internal/strictjson"
	"github.com/aminkbi/d-raft/raft"
)

const (
	// SemanticPlanSchema is the first adapter-neutral experiment-plan schema.
	SemanticPlanSchema = "d-raft.semantic-plan/v1"
	// AdapterCapabilitiesSchema is the matching adapter capability declaration.
	AdapterCapabilitiesSchema = "d-raft.adapter-capabilities/v1"

	// MembershipFixedAllVoters is v1's only portable membership profile.
	MembershipFixedAllVoters MembershipProfile = "fixed_all_voters"

	maxCapabilitiesDocumentBytes = 1 << 20
	maxCapabilityValues          = 1_024
	maxCapabilityValueBytes      = 256
)

var (
	ErrInvalidPlan         = errors.New("semanticplan: invalid semantic plan")
	ErrInvalidCapabilities = errors.New("semanticplan: invalid adapter capabilities")
	ErrDocumentTooLarge    = errors.New("semanticplan: document exceeds size limit")

	portableActions = []artifact.ActionKind{
		artifact.ActionCrash,
		artifact.ActionHeal,
		artifact.ActionPartition,
		artifact.ActionPropose,
		artifact.ActionRestart,
	}
	portableProjectionKinds = []decision.Kind{
		decision.ElectionTimeout,
		decision.NetworkLatency,
		decision.NetworkLoss,
		decision.StorageLatency,
	}
)

// V1Actions returns the closed portable workload action vocabulary.
func V1Actions() []artifact.ActionKind { return slices.Clone(portableActions) }

// V1ProjectionKinds returns the closed portable choice vocabulary.
func V1ProjectionKinds() []decision.Kind { return slices.Clone(portableProjectionKinds) }

// AdapterID uses the repository's existing stable adapter identity shape.
// The defined type prevents two subtly different wire identities from
// emerging while retaining explicit semantic-plan API ownership.
type AdapterID artifact.Adapter

// Convergence separates the last external stimulus from the comparison
// boundary. V1 requires the boundary to equal the scenario duration.
type Convergence struct {
	WorkloadEndNS        int64 `json:"workload_end_ns"`
	ComparisonBoundaryNS int64 `json:"comparison_boundary_ns"`
}

// Source binds a plan to the exact run from which its directives were
// projected without embedding adapter-local observations in the plan.
type Source struct {
	Adapter   artifact.Adapter `json:"adapter"`
	RunSHA256 string           `json:"run_sha256"`
}

// Plan is one bounded adapter-neutral workload and semantic-choice projection.
type Plan struct {
	Schema        string                 `json:"schema"`
	Scenario      artifact.Scenario      `json:"scenario"`
	Configuration artifact.Configuration `json:"configuration"`
	Application   apporacle.Config       `json:"application"`
	Convergence   Convergence            `json:"convergence"`
	Source        Source                 `json:"source"`
	FallbackSeed  artifact.Uint64        `json:"fallback_seed"`
	Directives    []Directive            `json:"directives"`
}

// MembershipProfile identifies the portable initial-membership contract.
type MembershipProfile string

// Capabilities is a closed, canonical adapter capability declaration. Each
// list is a sorted set; empty capability sets are encoded as [] rather than
// null so hashes do not depend on Go nil-slice representation.
type Capabilities struct {
	Schema              string                `json:"schema"`
	Adapter             artifact.Adapter      `json:"adapter"`
	MembershipProfiles  []MembershipProfile   `json:"membership_profiles"`
	Actions             []artifact.ActionKind `json:"actions"`
	ApplicationProfiles []string              `json:"application_profiles"`
	ProjectionKinds     []decision.Kind       `json:"projection_kinds"`
	InvariantIDs        []string              `json:"invariant_ids"`
}

// RejectionCode is a stable machine-readable v1 preflight failure.
type RejectionCode string

const (
	RejectFixedAllVoters      RejectionCode = "plan/fixed_all_voters_required"
	RejectUnsupportedAction   RejectionCode = "plan/unsupported_action"
	RejectInvalidCommand      RejectionCode = "plan/invalid_portable_command"
	RejectDuplicateCommand    RejectionCode = "plan/duplicate_command_id"
	RejectUnbalancedLifecycle RejectionCode = "plan/unbalanced_lifecycle"
	RejectUnhealedNetwork     RejectionCode = "plan/unhealed_network"
	RejectSourceAdapter       RejectionCode = "capability/left/source_adapter"
	RejectLeftMembership      RejectionCode = "capability/left/membership_profile"
	RejectRightMembership     RejectionCode = "capability/right/membership_profile"
	RejectLeftAction          RejectionCode = "capability/left/action"
	RejectRightAction         RejectionCode = "capability/right/action"
	RejectLeftApplication     RejectionCode = "capability/left/application_profile"
	RejectRightApplication    RejectionCode = "capability/right/application_profile"
	RejectLeftProjectionKind  RejectionCode = "capability/left/projection_kind"
	RejectRightProjectionKind RejectionCode = "capability/right/projection_kind"
)

// Eligibility is deliberately separate from malformed-input errors. A valid
// plan can be ineligible for one or both declared adapters.
type Eligibility struct {
	Eligible   bool            `json:"eligible"`
	Rejections []RejectionCode `json:"rejections"`
}

// Validate checks the bounded, canonical structure shared by every v1
// projection. Capability and v1 eligibility checks belong to Preflight.
func (plan Plan) Validate() error {
	if plan.Schema != SemanticPlanSchema {
		return fmt.Errorf("%w: unsupported schema %q", ErrInvalidPlan, plan.Schema)
	}
	if err := artifact.ValidateExperiment(plan.Scenario, plan.Configuration); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPlan, err)
	}
	if !planFitsResourceBudget(plan) {
		return fmt.Errorf("%w: plan exceeds %d-byte resource budget", ErrInvalidPlan, artifact.DefaultMaxArtifactBytes)
	}
	if err := plan.Application.Validate(); err != nil {
		return fmt.Errorf("%w: application profile: %v", ErrInvalidPlan, err)
	}
	if err := validateAdapterID(plan.Source.Adapter); err != nil {
		return fmt.Errorf("%w: source adapter: %v", ErrInvalidPlan, err)
	}
	if !digestPattern.MatchString(plan.Source.RunSHA256) {
		return fmt.Errorf("%w: source run_sha256 must be lowercase SHA-256", ErrInvalidPlan)
	}
	if plan.Convergence.ComparisonBoundaryNS != plan.Scenario.DurationNS ||
		plan.Convergence.WorkloadEndNS < 0 ||
		plan.Convergence.WorkloadEndNS >= plan.Convergence.ComparisonBoundaryNS {
		return fmt.Errorf("%w: convergence boundaries must satisfy 0 <= workload_end_ns < comparison_boundary_ns == scenario.duration_ns", ErrInvalidPlan)
	}
	for index, action := range plan.Scenario.Actions {
		if action.AtNS > plan.Convergence.WorkloadEndNS {
			return fmt.Errorf("%w: scenario action %d occurs after workload_end_ns", ErrInvalidPlan, index)
		}
	}
	if plan.Directives == nil || len(plan.Directives) > artifact.MaxDecisions {
		return fmt.Errorf("%w: directives must be a non-null list of at most %d entries", ErrInvalidPlan, artifact.MaxDecisions)
	}
	if err := ValidateDirectives(plan.Directives); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPlan, err)
	}
	members := make(map[raft.NodeID]struct{}, len(plan.Configuration.Members))
	for _, member := range plan.Configuration.Members {
		members[member] = struct{}{}
	}
	for index, directive := range plan.Directives {
		for _, node := range []raft.NodeID{directive.Key.Node, directive.Key.From, directive.Key.To} {
			if node == "" {
				continue
			}
			if _, exists := members[node]; !exists {
				return fmt.Errorf("%w: directive %d names unknown node %q", ErrInvalidPlan, index, node)
			}
		}
	}
	return nil
}

// Validate rejects non-canonical or unbounded capability declarations.
func (capabilities Capabilities) Validate() error {
	if capabilities.Schema != AdapterCapabilitiesSchema {
		return fmt.Errorf("%w: unsupported schema %q", ErrInvalidCapabilities, capabilities.Schema)
	}
	if err := validateAdapterID(capabilities.Adapter); err != nil {
		return fmt.Errorf("%w: adapter: %v", ErrInvalidCapabilities, err)
	}
	if err := validateMembershipProfiles(capabilities.MembershipProfiles); err != nil {
		return err
	}
	if err := validateActions(capabilities.Actions); err != nil {
		return err
	}
	if err := validateApplicationProfiles(capabilities.ApplicationProfiles); err != nil {
		return err
	}
	if err := validateProjectionKinds(capabilities.ProjectionKinds); err != nil {
		return err
	}
	if err := validateCapabilityStrings("invariant_ids", capabilities.InvariantIDs, invariantPattern.MatchString); err != nil {
		return err
	}
	return nil
}

// DecodePlan strictly decodes one bounded plan and validates its structure.
func DecodePlan(reader io.Reader) (Plan, error) {
	var plan Plan
	if err := decodePlanDocument(reader, artifact.DefaultMaxArtifactBytes, &plan); err != nil {
		return Plan{}, fmt.Errorf("%w: %v", ErrInvalidPlan, err)
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// DecodeCapabilities strictly decodes one bounded capability declaration.
func DecodeCapabilities(reader io.Reader) (Capabilities, error) {
	var capabilities Capabilities
	if err := decodePlanDocument(reader, maxCapabilitiesDocumentBytes, &capabilities); err != nil {
		return Capabilities{}, fmt.Errorf("%w: %v", ErrInvalidCapabilities, err)
	}
	if err := capabilities.Validate(); err != nil {
		return Capabilities{}, err
	}
	return capabilities, nil
}

// DigestPlan returns the lowercase SHA-256 of Go's stable JSON encoding after
// full structural validation. It hashes the semantic document, not source-file
// whitespace.
func DigestPlan(plan Plan) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("%w: encode for digest: %v", ErrInvalidPlan, err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// Preflight validates both documents, then computes every stable v1
// eligibility rejection before a projector or fallback decider is created.
func Preflight(plan Plan, left, right Capabilities) (Eligibility, error) {
	if err := plan.Validate(); err != nil {
		return Eligibility{}, err
	}
	if err := left.Validate(); err != nil {
		return Eligibility{}, fmt.Errorf("semanticplan: left capabilities: %w", err)
	}
	if err := right.Validate(); err != nil {
		return Eligibility{}, fmt.Errorf("semanticplan: right capabilities: %w", err)
	}

	rejections := make([]RejectionCode, 0)
	configurationEligible := fixedAllVoters(plan.Configuration)
	if !configurationEligible {
		rejections = append(rejections, RejectFixedAllVoters)
	}
	if plan.Source.Adapter != left.Adapter {
		rejections = append(rejections, RejectSourceAdapter)
	}
	if !slices.Contains(left.MembershipProfiles, MembershipFixedAllVoters) {
		rejections = append(rejections, RejectLeftMembership)
	}
	if !slices.Contains(right.MembershipProfiles, MembershipFixedAllVoters) {
		rejections = append(rejections, RejectRightMembership)
	}
	if !slices.Contains(left.ApplicationProfiles, plan.Application.Schema) {
		rejections = append(rejections, RejectLeftApplication)
	}
	if !slices.Contains(right.ApplicationProfiles, plan.Application.Schema) {
		rejections = append(rejections, RejectRightApplication)
	}

	requiredActions := make(map[artifact.ActionKind]struct{})
	commands := make(map[apporacle.CommandID]struct{})
	up := make(map[raft.NodeID]bool, len(plan.Configuration.Members))
	for _, member := range plan.Configuration.Members {
		up[member] = true
	}
	partitioned := false
	lifecycleValid := true
	for _, action := range plan.Scenario.Actions {
		requiredActions[action.Kind] = struct{}{}
		switch action.Kind {
		case artifact.ActionPropose:
			command, err := apporacle.DecodeCommand(action.Data)
			if err != nil {
				rejections = append(rejections, RejectInvalidCommand)
				continue
			}
			if _, duplicate := commands[command.ID]; duplicate {
				rejections = append(rejections, RejectDuplicateCommand)
			} else {
				commands[command.ID] = struct{}{}
			}
		case artifact.ActionCrash:
			if !up[action.Node] {
				lifecycleValid = false
			} else {
				up[action.Node] = false
			}
		case artifact.ActionRestart:
			if up[action.Node] {
				lifecycleValid = false
			} else {
				up[action.Node] = true
			}
		case artifact.ActionPartition:
			partitioned = true
		case artifact.ActionHeal:
			partitioned = false
		default:
			rejections = append(rejections, RejectUnsupportedAction)
		}
	}
	for _, isUp := range up {
		lifecycleValid = lifecycleValid && isUp
	}
	if !lifecycleValid {
		rejections = append(rejections, RejectUnbalancedLifecycle)
	}
	if partitioned {
		rejections = append(rejections, RejectUnhealedNetwork)
	}
	for action := range requiredActions {
		if !slices.Contains(left.Actions, action) {
			rejections = append(rejections, RejectLeftAction)
		}
		if !slices.Contains(right.Actions, action) {
			rejections = append(rejections, RejectRightAction)
		}
	}
	// A target may introduce a portable choice absent from the source tape.
	// Consequently v1 negotiates its complete closed projection vocabulary,
	// not merely the kinds that happened to produce source directives.
	for _, kind := range portableProjectionKinds {
		if !slices.Contains(left.ProjectionKinds, kind) {
			rejections = append(rejections, RejectLeftProjectionKind)
		}
		if !slices.Contains(right.ProjectionKinds, kind) {
			rejections = append(rejections, RejectRightProjectionKind)
		}
	}
	slices.Sort(rejections)
	rejections = slices.Compact(rejections)
	if rejections == nil {
		rejections = []RejectionCode{}
	}
	return Eligibility{Eligible: len(rejections) == 0, Rejections: rejections}, nil
}

func decodePlanDocument(reader io.Reader, maximum int64, target any) error {
	if reader == nil {
		return errors.New("nil reader")
	}
	data, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > maximum {
		return ErrDocumentTooLarge
	}
	if err := strictjson.RejectDuplicateNames(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func fixedAllVoters(configuration artifact.Configuration) bool {
	if len(configuration.Learners) != 0 {
		return false
	}
	if len(configuration.Voters) == 0 {
		return true
	}
	return slices.Equal(configuration.Members, configuration.Voters)
}

// planFitsResourceBudget prevents callers that construct Plan values directly
// from bypassing DecodePlan's encoded-size limit. The accounting is
// intentionally conservative and includes base64 expansion of action data.
func planFitsResourceBudget(plan Plan) bool {
	remaining := artifact.DefaultMaxArtifactBytes - 4_096
	consume := func(amount int) bool {
		if amount < 0 || amount > remaining {
			return false
		}
		remaining -= amount
		return true
	}
	for _, action := range plan.Scenario.Actions {
		encodedDataBytes := ((len(action.Data) + 2) / 3) * 4
		if !consume(512 + encodedDataBytes + len(action.Node)) {
			return false
		}
		for _, group := range action.Groups {
			for _, node := range group {
				if !consume(len(node) + 4) {
					return false
				}
			}
		}
	}
	for _, directive := range plan.Directives {
		if !consume(512 + len(directive.Key.Node) + len(directive.Key.From) + len(directive.Key.To)) {
			return false
		}
	}
	return true
}

func validateMembershipProfiles(values []MembershipProfile) error {
	if values == nil || len(values) > maxCapabilityValues || !strictlySorted(values) {
		return fmt.Errorf("%w: membership_profiles must be a non-null sorted unique bounded set", ErrInvalidCapabilities)
	}
	for _, value := range values {
		if value != MembershipFixedAllVoters {
			return fmt.Errorf("%w: unknown membership profile %q", ErrInvalidCapabilities, value)
		}
	}
	return nil
}

func validateActions(values []artifact.ActionKind) error {
	if values == nil || len(values) > maxCapabilityValues || !strictlySorted(values) {
		return fmt.Errorf("%w: actions must be a non-null sorted unique bounded set", ErrInvalidCapabilities)
	}
	for _, value := range values {
		switch value {
		case artifact.ActionPropose, artifact.ActionCrash, artifact.ActionRestart,
			artifact.ActionCrashAfterNextPersist, artifact.ActionSnapshot,
			artifact.ActionBeginMembership, artifact.ActionFinalizeMembership,
			artifact.ActionPartition, artifact.ActionHeal:
		default:
			return fmt.Errorf("%w: unknown action %q", ErrInvalidCapabilities, value)
		}
	}
	return nil
}

func validateApplicationProfiles(values []string) error {
	return validateCapabilityStrings("application_profiles", values, func(value string) bool {
		return value == apporacle.CommandSchema
	})
}

func validateProjectionKinds(values []decision.Kind) error {
	if values == nil || len(values) > maxCapabilityValues || !strictlySorted(values) {
		return fmt.Errorf("%w: projection_kinds must be a non-null sorted unique bounded set", ErrInvalidCapabilities)
	}
	for _, value := range values {
		if !supportedKind(value) {
			return fmt.Errorf("%w: unknown projection kind %q", ErrInvalidCapabilities, value)
		}
	}
	return nil
}

func validateCapabilityStrings(name string, values []string, valid func(string) bool) error {
	if values == nil || len(values) > maxCapabilityValues || !strictlySorted(values) {
		return fmt.Errorf("%w: %s must be a non-null sorted unique bounded set", ErrInvalidCapabilities, name)
	}
	for _, value := range values {
		if len(value) == 0 || len(value) > maxCapabilityValueBytes || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 || !valid(value) {
			return fmt.Errorf("%w: invalid %s value %q", ErrInvalidCapabilities, name, value)
		}
	}
	return nil
}

func strictlySorted[S ~[]E, E ~string](values S) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] >= values[index] {
			return false
		}
	}
	return true
}
