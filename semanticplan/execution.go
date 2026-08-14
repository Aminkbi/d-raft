package semanticplan

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
)

const (
	// SemanticExecutionSchema is the adapter-local evidence produced by one
	// attempted projection of a semantic plan.
	SemanticExecutionSchema = "d-raft.semantic-execution/v1"
	maxExecutionErrorBytes  = 4 << 10
)

var ErrInvalidExecution = errors.New("semanticplan: invalid semantic execution")

// SemanticExecution retains an exact target-local tape for successful
// projections, or the exact successful prefix for a failed projection,
// alongside portable accounting. Outcome is absent only when preflight made
// the adapter ineligible or execution failed before a cluster outcome existed.
type SemanticExecution struct {
	Schema             string                   `json:"schema"`
	PlanSHA256         string                   `json:"plan_sha256"`
	CapabilitiesSHA256 string                   `json:"capabilities_sha256"`
	Adapter            AdapterID                `json:"adapter"`
	Reproducibility    artifact.Reproducibility `json:"reproducibility"`
	Eligibility        EligibilityStatus        `json:"eligibility"`
	Rejections         []RejectionCode          `json:"rejections"`
	Projection         ProjectionReport         `json:"projection"`
	Decisions          decision.Tape            `json:"decisions"`
	Outcome            *artifact.Outcome        `json:"outcome,omitempty"`
	OperationalError   string                   `json:"operational_error,omitempty"`
	NodeCommitments    []NodeCommitment         `json:"node_commitments"`
}

// Validate rejects malformed, unbounded, or internally inconsistent
// execution evidence without needing the referenced plan document.
func (execution SemanticExecution) Validate() error {
	if execution.Schema != SemanticExecutionSchema {
		return fmt.Errorf("%w: unsupported schema %q", ErrInvalidExecution, execution.Schema)
	}
	if !digestPattern.MatchString(execution.PlanSHA256) || !digestPattern.MatchString(execution.CapabilitiesSHA256) {
		return fmt.Errorf("%w: plan and capability hashes must be lowercase SHA-256", ErrInvalidExecution)
	}
	if err := validateAdapterID(artifact.Adapter(execution.Adapter)); err != nil {
		return fmt.Errorf("%w: adapter: %v", ErrInvalidExecution, err)
	}
	if err := validateExecutionReproducibility(execution.Reproducibility); err != nil {
		return fmt.Errorf("%w: reproducibility: %v", ErrInvalidExecution, err)
	}
	if execution.Rejections == nil || !strictRejectionSet(execution.Rejections) {
		return fmt.Errorf("%w: rejections must be a non-null sorted unique set", ErrInvalidExecution)
	}
	if execution.Decisions.Schema != decision.SchemaVersion || execution.Decisions.Entries == nil {
		return fmt.Errorf("%w: decisions must be a canonical non-null v1 tape", ErrInvalidExecution)
	}
	if len(execution.Decisions.Entries) > artifact.MaxDecisions {
		return fmt.Errorf("%w: decisions exceed %d entries", ErrInvalidExecution, artifact.MaxDecisions)
	}
	if _, err := decision.NewTapeDecider(execution.Decisions); err != nil {
		return fmt.Errorf("%w: decisions: %v", ErrInvalidExecution, err)
	}
	if execution.NodeCommitments == nil {
		return fmt.Errorf("%w: node commitments must be a non-null list", ErrInvalidExecution)
	}
	if err := validateNodeCommitmentList(execution.NodeCommitments); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExecution, err)
	}
	if err := validateProjectionReport(execution.Projection, len(execution.Decisions.Entries)); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExecution, err)
	}
	if len(execution.OperationalError) > maxExecutionErrorBytes || strings.IndexByte(execution.OperationalError, 0) >= 0 {
		return fmt.Errorf("%w: invalid operational error", ErrInvalidExecution)
	}

	switch execution.Eligibility {
	case EligibilityIneligible:
		if len(execution.Rejections) == 0 || execution.Projection.Fidelity != ProjectionFailed ||
			len(execution.Decisions.Entries) != 0 || execution.Outcome != nil || execution.OperationalError != "" || len(execution.NodeCommitments) != 0 {
			return fmt.Errorf("%w: ineligible execution contains execution evidence", ErrInvalidExecution)
		}
	case EligibilityEligible:
		if len(execution.Rejections) != 0 {
			return fmt.Errorf("%w: eligible execution has rejections", ErrInvalidExecution)
		}
		if execution.Outcome == nil && execution.OperationalError == "" {
			return fmt.Errorf("%w: eligible execution has neither outcome nor operational error", ErrInvalidExecution)
		}
		if execution.Outcome != nil && execution.OperationalError != "" {
			return fmt.Errorf("%w: execution has both outcome and operational error", ErrInvalidExecution)
		}
	default:
		return fmt.Errorf("%w: unknown eligibility %q", ErrInvalidExecution, execution.Eligibility)
	}
	if execution.Outcome != nil {
		if err := validateRawOutcome(*execution.Outcome); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidExecution, err)
		}
		if execution.Projection.Fidelity == ProjectionFailed && execution.Outcome.Status != artifact.OutcomeError {
			return fmt.Errorf("%w: failed projection requires an error outcome", ErrInvalidExecution)
		}
	}
	encoded, err := json.Marshal(execution)
	if err != nil || len(encoded) > artifact.DefaultMaxArtifactBytes {
		return fmt.Errorf("%w: encoded execution exceeds its resource budget", ErrInvalidExecution)
	}
	return nil
}

// DecodeSemanticExecution strictly decodes one bounded execution document.
func DecodeSemanticExecution(reader io.Reader) (SemanticExecution, error) {
	var execution SemanticExecution
	if err := decodePlanDocument(reader, artifact.DefaultMaxArtifactBytes, &execution); err != nil {
		return SemanticExecution{}, fmt.Errorf("%w: %v", ErrInvalidExecution, err)
	}
	if err := execution.Validate(); err != nil {
		return SemanticExecution{}, err
	}
	return execution, nil
}

// DigestCapabilities returns the stable digest of a canonical declaration.
func DigestCapabilities(capabilities Capabilities) (string, error) {
	if err := capabilities.Validate(); err != nil {
		return "", err
	}
	return digestCanonicalJSON(capabilities)
}

// NegotiateInvariantIDs returns the canonical intersection of two validated
// checker vocabularies. Only this set may participate in safety agreement.
func NegotiateInvariantIDs(left, right Capabilities) ([]string, error) {
	if err := left.Validate(); err != nil {
		return nil, err
	}
	if err := right.Validate(); err != nil {
		return nil, err
	}
	common := make([]string, 0, min(len(left.InvariantIDs), len(right.InvariantIDs)))
	leftIndex, rightIndex := 0, 0
	for leftIndex < len(left.InvariantIDs) && rightIndex < len(right.InvariantIDs) {
		switch strings.Compare(left.InvariantIDs[leftIndex], right.InvariantIDs[rightIndex]) {
		case -1:
			leftIndex++
		case 1:
			rightIndex++
		default:
			common = append(common, left.InvariantIDs[leftIndex])
			leftIndex++
			rightIndex++
		}
	}
	if common == nil {
		common = []string{}
	}
	return common, nil
}

// NewEligibleExecution constructs canonical evidence for one adapter attempt.
// It defensively clones the exact tape, outcome, projection report, and
// commitments before validating the complete document.
func NewEligibleExecution(plan Plan, capabilities Capabilities, reproducibility artifact.Reproducibility, projection ProjectionReport, tape decision.Tape, outcome *artifact.Outcome, operationalError string, nodes []NodeCommitment) (SemanticExecution, error) {
	planDigest, err := DigestPlan(plan)
	if err != nil {
		return SemanticExecution{}, err
	}
	capabilitiesDigest, err := DigestCapabilities(capabilities)
	if err != nil {
		return SemanticExecution{}, err
	}
	tape = decision.CloneTape(tape)
	if tape.Entries == nil {
		tape.Entries = []decision.Entry{}
	}
	execution := SemanticExecution{
		Schema: SemanticExecutionSchema, PlanSHA256: planDigest,
		CapabilitiesSHA256: capabilitiesDigest, Adapter: AdapterID(capabilities.Adapter),
		Reproducibility: reproducibility,
		Eligibility:     EligibilityEligible, Rejections: []RejectionCode{},
		Projection: cloneProjectionReport(projection), Decisions: tape,
		Outcome: cloneRawOutcome(outcome), OperationalError: operationalError,
		NodeCommitments: cloneNodeCommitments(nodes),
	}
	if err := execution.Validate(); err != nil {
		return SemanticExecution{}, err
	}
	if err := VerifyExecutionProjection(plan, execution); err != nil {
		return SemanticExecution{}, err
	}
	return execution, nil
}

// VerifyExecutionProjection independently reconstructs projection accounting
// and selections from the semantic plan and the exact successful target-local
// tape. For failed projections, the tape is necessarily a successful prefix;
// the failed choice itself is not claimed to be exact replay evidence.
func VerifyExecutionProjection(plan Plan, execution SemanticExecution) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if err := execution.Validate(); err != nil {
		return err
	}
	if execution.Reproducibility.DecisionSeed != plan.FallbackSeed {
		return fmt.Errorf("%w: execution fallback seed does not match plan", ErrInvalidExecution)
	}
	if execution.Eligibility == EligibilityIneligible {
		want := ProjectionReport{
			Fidelity: ProjectionFailed, Directives: len(plan.Directives),
			Additional: []PortableKey{}, Unmatched: cloneDirectives(plan.Directives),
		}
		if !reflect.DeepEqual(want, execution.Projection) {
			return fmt.Errorf("%w: ineligible projection report does not match the unexecuted plan", ErrInvalidExecution)
		}
		return nil
	}
	projector, err := NewProjector(plan.Directives, plan.FallbackSeed)
	if err != nil {
		return err
	}
	for index, entry := range execution.Decisions.Entries {
		selection, chooseErr := projector.Choose(entry.Choice)
		if chooseErr != nil {
			return fmt.Errorf("%w: decision %d cannot be projected: %v", ErrInvalidExecution, index, chooseErr)
		}
		if !reflect.DeepEqual(selection, entry.Selection) {
			return fmt.Errorf("%w: decision %d selection does not match plan projection", ErrInvalidExecution, index)
		}
	}
	want := projector.Finish()
	if execution.Projection.Fidelity == ProjectionFailed {
		want.Fidelity = ProjectionFailed
	}
	if !reflect.DeepEqual(want, execution.Projection) {
		return fmt.Errorf("%w: projection report does not match plan and local tape evidence", ErrInvalidExecution)
	}
	return nil
}

func validateExecutionReproducibility(reproducibility artifact.Reproducibility) error {
	if reproducibility.DecisionSchema != decision.SchemaVersion {
		return errors.New("decision schema does not match the exact local tape")
	}
	values := []struct {
		name    string
		value   string
		maximum int
	}{
		{"git_revision", reproducibility.GitRevision, 256},
		{"go_version", reproducibility.GoVersion, 128},
		{"checker_schema", reproducibility.CheckerSchema, 256},
		{"message_codec", reproducibility.MessageCodec, 256},
		{"observation_schema", reproducibility.ObservationSchema, 256},
	}
	for _, field := range values {
		if field.value == "" || len(field.value) > field.maximum || !utf8.ValidString(field.value) ||
			strings.TrimSpace(field.value) != field.value || strings.IndexFunc(field.value, unicode.IsControl) >= 0 {
			return fmt.Errorf("invalid %s", field.name)
		}
	}
	return nil
}

// DigestSemanticExecution returns the stable digest of validated evidence.
func DigestSemanticExecution(execution SemanticExecution) (string, error) {
	if err := execution.Validate(); err != nil {
		return "", err
	}
	return digestCanonicalJSON(execution)
}

// NormalizeExecution verifies all plan/execution cross-references and removes
// adapter-local evidence from the comparison surface.
func NormalizeExecution(plan Plan, capabilities, peerCapabilities Capabilities, execution SemanticExecution) (NormalizedOutcome, error) {
	planDigest, err := DigestPlan(plan)
	if err != nil {
		return NormalizedOutcome{}, err
	}
	capabilitiesDigest, err := DigestCapabilities(capabilities)
	if err != nil {
		return NormalizedOutcome{}, err
	}
	negotiatedCommon, err := NegotiateInvariantIDs(capabilities, peerCapabilities)
	if err != nil {
		return NormalizedOutcome{}, err
	}
	eligibility, err := bilateralPreflight(plan, capabilities, peerCapabilities)
	if err != nil {
		return NormalizedOutcome{}, err
	}
	if err := execution.Validate(); err != nil {
		return NormalizedOutcome{}, err
	}
	if execution.PlanSHA256 != planDigest {
		return NormalizedOutcome{}, fmt.Errorf("%w: execution refers to a different plan", ErrInvalidExecution)
	}
	if execution.CapabilitiesSHA256 != capabilitiesDigest || execution.Adapter != AdapterID(capabilities.Adapter) {
		return NormalizedOutcome{}, fmt.Errorf("%w: execution refers to different adapter capabilities", ErrInvalidExecution)
	}
	wantEligibility := EligibilityEligible
	if !eligibility.Eligible {
		wantEligibility = EligibilityIneligible
	}
	if execution.Eligibility != wantEligibility || !slices.Equal(execution.Rejections, eligibility.Rejections) {
		return NormalizedOutcome{}, fmt.Errorf("%w: execution eligibility does not match bilateral preflight", ErrInvalidExecution)
	}
	if err := VerifyExecutionProjection(plan, execution); err != nil {
		return NormalizedOutcome{}, err
	}
	executionDigest, err := DigestSemanticExecution(execution)
	if err != nil {
		return NormalizedOutcome{}, err
	}
	normalized := NormalizedOutcome{
		Schema: NormalizedOutcomeSchema, PlanSHA256: planDigest,
		ExecutionSHA256: executionDigest, Adapter: execution.Adapter,
		Eligibility: execution.Eligibility, Projection: execution.Projection.Fidelity,
		Completion: CompletionNotExecuted, NodeCommitments: []NodeCommitment{},
		NegotiatedCommonIDs: slices.Clone(negotiatedCommon),
		CommonInvariantIDs:  []string{}, AdapterSpecificInvariantIDs: []string{},
	}
	if execution.Eligibility == EligibilityIneligible {
		return CanonicalizeOutcome(normalized)
	}
	normalized.NodeCommitments = cloneNodeCommitments(execution.NodeCommitments)
	if execution.Outcome == nil {
		normalized.Completion = CompletionExecutionError
		return CanonicalizeOutcome(normalized)
	}
	if err := validateOutcomeAgainstPlan(plan, *execution.Outcome); err != nil {
		return NormalizedOutcome{}, err
	}
	normalized.Completion = classifyCompletion(*execution.Outcome, plan.Convergence.ComparisonBoundaryNS, execution.Projection.Fidelity)
	observed := make([]string, 0, len(execution.Outcome.Violations))
	for _, violation := range execution.Outcome.Violations {
		observed = append(observed, violation.ID)
	}
	for _, id := range observed {
		if !slices.Contains(capabilities.InvariantIDs, id) {
			return NormalizedOutcome{}, fmt.Errorf("%w: adapter emitted undeclared invariant %q", ErrInvalidExecution, id)
		}
	}
	normalized.CommonInvariantIDs, normalized.AdapterSpecificInvariantIDs, err = NormalizeInvariantIDs(observed, negotiatedCommon)
	if err != nil {
		return NormalizedOutcome{}, err
	}
	if completionReachedBoundary(normalized.Completion) {
		if err := validatePlanCommitmentNodes(plan, normalized.NodeCommitments); err != nil {
			return NormalizedOutcome{}, err
		}
	}
	return CanonicalizeOutcome(normalized)
}

func bilateralPreflight(plan Plan, local, peer Capabilities) (Eligibility, error) {
	switch {
	case local.Adapter == plan.Source.Adapter:
		return Preflight(plan, local, peer)
	case peer.Adapter == plan.Source.Adapter:
		return Preflight(plan, peer, local)
	default:
		return Eligibility{}, fmt.Errorf("%w: neither capability document identifies the source adapter", ErrInvalidExecution)
	}
}

func validateProjectionReport(report ProjectionReport, recorded int) error {
	if report.Directives < 0 || report.Projected < 0 || report.Fixed < 0 || report.Projected > report.Directives || report.Additional == nil || report.Unmatched == nil {
		return errors.New("semanticplan: invalid projection counters or null sets")
	}
	if report.Directives > artifact.MaxDecisions || report.Projected > artifact.MaxDecisions || report.Fixed > artifact.MaxDecisions || len(report.Additional) > artifact.MaxDecisions || len(report.Unmatched) > artifact.MaxDecisions {
		return fmt.Errorf("semanticplan: projection accounting exceeds %d entries", artifact.MaxDecisions)
	}
	for index, key := range report.Additional {
		if err := key.Validate(); err != nil {
			return fmt.Errorf("semanticplan: additional[%d]: %w", index, err)
		}
		if index > 0 && compareKeys(report.Additional[index-1], key) >= 0 {
			return errors.New("semanticplan: additional keys must be sorted and unique")
		}
	}
	if err := ValidateDirectives(report.Unmatched); err != nil {
		return fmt.Errorf("semanticplan: unmatched directives: %w", err)
	}
	if recorded != report.Projected+report.Fixed+len(report.Additional) {
		return errors.New("semanticplan: projection accounting does not match the recorded local tape")
	}
	switch report.Fidelity {
	case ProjectionExact:
		if len(report.Additional) != 0 || len(report.Unmatched) != 0 || report.Projected != report.Directives {
			return errors.New("semanticplan: exact projection has incomplete accounting")
		}
	case ProjectionPartial:
		if len(report.Additional)+len(report.Unmatched) == 0 || report.Projected+len(report.Unmatched) != report.Directives {
			return errors.New("semanticplan: partial projection has inconsistent accounting")
		}
	case ProjectionFailed:
		if report.Projected+len(report.Unmatched) > report.Directives {
			return errors.New("semanticplan: failed projection overcounts directives")
		}
	default:
		return fmt.Errorf("semanticplan: unknown projection fidelity %q", report.Fidelity)
	}
	return nil
}

func validateRawOutcome(outcome artifact.Outcome) error {
	if outcome.EndNS < 0 || outcome.Steps > artifact.MaxScenarioSteps || !digestPattern.MatchString(outcome.ObservationDigest) || len(outcome.Error) > maxExecutionErrorBytes || len(outcome.Violations) > artifact.MaxViolations {
		return errors.New("semanticplan: invalid raw outcome counters, digest, or bounds")
	}
	seen := make(map[string]struct{}, len(outcome.Violations))
	for index, violation := range outcome.Violations {
		if violation.AtNS < 0 || violation.AtNS > outcome.EndNS || len(violation.Evidence) > artifact.MaxViolationEvidenceBytes {
			return fmt.Errorf("semanticplan: violation %d is out of bounds", index)
		}
		if err := check.ValidateViolation(violation); err != nil {
			return fmt.Errorf("semanticplan: violation %d: %w", index, err)
		}
		if _, duplicate := seen[violation.Fingerprint]; duplicate {
			return fmt.Errorf("semanticplan: duplicate violation fingerprint %q", violation.Fingerprint)
		}
		seen[violation.Fingerprint] = struct{}{}
	}
	switch outcome.Status {
	case artifact.OutcomeCompleted:
		if outcome.Error != "" || len(outcome.Violations) != 0 {
			return errors.New("semanticplan: completed raw outcome has error or violations")
		}
	case artifact.OutcomeViolation:
		if outcome.Error != "" || len(outcome.Violations) == 0 {
			return errors.New("semanticplan: violating raw outcome lacks canonical violations")
		}
	case artifact.OutcomeError:
		if outcome.Error == "" || len(outcome.Violations) != 0 {
			return errors.New("semanticplan: error raw outcome lacks an error or contains violations")
		}
	case artifact.OutcomeBudgetExhausted:
		if outcome.Error != "" || len(outcome.Violations) != 0 {
			return errors.New("semanticplan: exhausted raw outcome has error or violations")
		}
	default:
		return fmt.Errorf("semanticplan: unknown raw outcome status %q", outcome.Status)
	}
	return nil
}

func classifyCompletion(outcome artifact.Outcome, boundary int64, fidelity ProjectionFidelity) CompletionStatus {
	if fidelity == ProjectionFailed || outcome.Status == artifact.OutcomeError {
		return CompletionExecutionError
	}
	if outcome.EndNS == boundary && (outcome.Status == artifact.OutcomeCompleted || outcome.Status == artifact.OutcomeViolation) {
		return CompletionCompleted
	}
	switch outcome.Status {
	case artifact.OutcomeViolation:
		return CompletionSafetyViolation
	case artifact.OutcomeBudgetExhausted:
		return CompletionStepLimit
	case artifact.OutcomeCompleted:
		return CompletionTimeLimit
	default:
		return CompletionExecutionError
	}
}

func validatePlanCommitmentNodes(plan Plan, nodes []NodeCommitment) error {
	if len(nodes) != len(plan.Configuration.Members) {
		return fmt.Errorf("%w: boundary outcome has %d commitments for %d members", ErrInvalidExecution, len(nodes), len(plan.Configuration.Members))
	}
	for index, member := range plan.Configuration.Members {
		if nodes[index].Node != member {
			return fmt.Errorf("%w: boundary commitment members do not match the plan", ErrInvalidExecution)
		}
	}
	return nil
}

func validateNodeCommitmentList(nodes []NodeCommitment) error {
	values := make(map[raft.NodeID]struct{}, len(nodes))
	for index, node := range nodes {
		if err := validateNodeID(node.Node); err != nil {
			return err
		}
		if err := validateCommitment(node.Commitment); err != nil {
			return fmt.Errorf("semanticplan: node commitments[%d]: %w", index, err)
		}
		if _, duplicate := values[node.Node]; duplicate {
			return fmt.Errorf("semanticplan: duplicate node commitment %q", node.Node)
		}
		values[node.Node] = struct{}{}
		if index > 0 && nodes[index-1].Node >= node.Node {
			return errors.New("semanticplan: node commitments must be sorted by node")
		}
	}
	return nil
}

func cloneNodeCommitments(nodes []NodeCommitment) []NodeCommitment {
	result := slices.Clone(nodes)
	if result == nil {
		return []NodeCommitment{}
	}
	return result
}

func strictRejectionSet(values []RejectionCode) bool {
	for index, value := range values {
		if !validRejectionCode(value) || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

func validRejectionCode(value RejectionCode) bool {
	switch value {
	case RejectFixedAllVoters, RejectUnsupportedAction, RejectInvalidCommand,
		RejectDuplicateCommand, RejectUnbalancedLifecycle, RejectUnhealedNetwork,
		RejectSourceAdapter, RejectLeftMembership, RejectRightMembership,
		RejectLeftAction, RejectRightAction, RejectLeftApplication,
		RejectRightApplication, RejectLeftProjectionKind, RejectRightProjectionKind:
		return true
	default:
		return false
	}
}

func validateOutcomeAgainstPlan(plan Plan, outcome artifact.Outcome) error {
	if outcome.EndNS > plan.Convergence.ComparisonBoundaryNS || outcome.Steps > plan.Scenario.MaxSteps {
		return fmt.Errorf("%w: raw outcome exceeds the plan boundary or step budget", ErrInvalidExecution)
	}
	if outcome.Status == artifact.OutcomeBudgetExhausted && outcome.Steps != plan.Scenario.MaxSteps {
		return fmt.Errorf("%w: budget-exhausted outcome did not consume the plan step budget", ErrInvalidExecution)
	}
	members := make(map[raft.NodeID]struct{}, len(plan.Configuration.Members))
	for _, member := range plan.Configuration.Members {
		members[member] = struct{}{}
	}
	for _, violation := range outcome.Violations {
		for _, node := range violation.Nodes {
			if _, exists := members[node]; !exists {
				return fmt.Errorf("%w: violation names unknown node %q", ErrInvalidExecution, node)
			}
		}
	}
	return nil
}

func cloneProjectionReport(report ProjectionReport) ProjectionReport {
	report.Additional = slices.Clone(report.Additional)
	report.Unmatched = slices.Clone(report.Unmatched)
	for index := range report.Unmatched {
		report.Unmatched[index].Selection = cloneSelection(report.Unmatched[index].Selection)
	}
	if report.Additional == nil {
		report.Additional = []PortableKey{}
	}
	if report.Unmatched == nil {
		report.Unmatched = []Directive{}
	}
	return report
}

func cloneDirectives(directives []Directive) []Directive {
	result := slices.Clone(directives)
	if result == nil {
		return []Directive{}
	}
	for index := range result {
		result[index].Selection = cloneSelection(result[index].Selection)
	}
	return result
}

func cloneRawOutcome(outcome *artifact.Outcome) *artifact.Outcome {
	if outcome == nil {
		return nil
	}
	clone := *outcome
	clone.Violations = slices.Clone(outcome.Violations)
	for index := range clone.Violations {
		clone.Violations[index].Nodes = slices.Clone(clone.Violations[index].Nodes)
		clone.Violations[index].Evidence = slices.Clone(clone.Violations[index].Evidence)
	}
	return &clone
}
