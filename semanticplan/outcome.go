package semanticplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/internal/strictjson"
	"github.com/aminkbi/d-raft/raft"
)

const (
	// NormalizedOutcomeSchema is the stable, adapter-neutral result surface.
	NormalizedOutcomeSchema = "d-raft.normalized-outcome/v1"
	// NormalizedComparisonSchema is the stable pairwise comparison surface.
	NormalizedComparisonSchema = "d-raft.normalized-comparison/v1"

	maxNormalizedDocumentBytes = 8 << 20
	maxNormalizedNodes         = 1_024
	maxNormalizedInvariants    = 1_024
)

var (
	digestPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	invariantPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*$`)
)

// EligibilityStatus records whether an adapter could execute the semantic
// plan. Projection fidelity and execution completion are deliberately
// separate axes.
type EligibilityStatus string

const (
	EligibilityEligible   EligibilityStatus = "eligible"
	EligibilityIneligible EligibilityStatus = "ineligible"
)

// CompletionStatus is the closed execution-completion vocabulary. It says
// where execution stopped, without exposing adapter-local terms, leaders,
// indexes, observation digests, or error strings.
type CompletionStatus string

const (
	CompletionNotExecuted       CompletionStatus = "not_executed"
	CompletionCompleted         CompletionStatus = "completed"
	CompletionSafetyViolation   CompletionStatus = "safety_violation"
	CompletionDecisionExhausted CompletionStatus = "decision_exhausted"
	CompletionStepLimit         CompletionStatus = "step_limit"
	CompletionTimeLimit         CompletionStatus = "time_limit"
	CompletionExecutionError    CompletionStatus = "execution_error"
)

var completionStatuses = []CompletionStatus{
	CompletionNotExecuted, CompletionCompleted, CompletionSafetyViolation,
	CompletionDecisionExhausted, CompletionStepLimit, CompletionTimeLimit,
	CompletionExecutionError,
}

// NodeCommitment is one node's portable application state. Node is the
// semantic-plan member ID, not an adapter's local numeric identity.
type NodeCommitment struct {
	Node       raft.NodeID          `json:"node"`
	Commitment apporacle.Commitment `json:"commitment"`
}

// NormalizedOutcome contains only portable experiment evidence. Common
// invariant IDs belong to the shared cross-adapter checker vocabulary;
// AdapterSpecificInvariantIDs retain findings that cannot be compared as common
// safety claims.
type NormalizedOutcome struct {
	Schema                          string             `json:"schema"`
	PlanSHA256                      string             `json:"plan_sha256"`
	ExecutionSHA256                 string             `json:"execution_sha256"`
	Adapter                         AdapterID          `json:"adapter"`
	Eligibility                     EligibilityStatus  `json:"eligibility"`
	Projection                      ProjectionFidelity `json:"projection"`
	Completion                      CompletionStatus   `json:"completion"`
	ApplicationNodesAgreeAtBoundary bool               `json:"application_nodes_agree_at_boundary"`
	NodeCommitments                 []NodeCommitment   `json:"node_commitments"`
	NegotiatedCommonIDs             []string           `json:"negotiated_common_ids"`
	CommonInvariantIDs              []string           `json:"common_invariant_ids"`
	AdapterSpecificInvariantIDs     []string           `json:"adapter_specific_invariant_ids"`
}

// EligibilityComparison preserves which side could execute instead of
// collapsing eligibility into a generic agreement bit.
type EligibilityComparison string

const (
	EligibilityBothEligible    EligibilityComparison = "both_eligible"
	EligibilityLeftIneligible  EligibilityComparison = "left_ineligible"
	EligibilityRightIneligible EligibilityComparison = "right_ineligible"
	EligibilityBothIneligible  EligibilityComparison = "both_ineligible"
)

var eligibilityComparisons = []EligibilityComparison{
	EligibilityBothEligible, EligibilityLeftIneligible,
	EligibilityRightIneligible, EligibilityBothIneligible,
}

// AgreementStatus is used only for common safety-ID agreement. Projection and
// execution boundary have richer vocabularies that avoid false equivalence.
type AgreementStatus string

const (
	AgreementAgree         AgreementStatus = "agree"
	AgreementDisagree      AgreementStatus = "disagree"
	AgreementNotComparable AgreementStatus = "not_comparable"
)

var agreementStatuses = []AgreementStatus{
	AgreementAgree, AgreementDisagree, AgreementNotComparable,
}

// ProjectionComparison reports both adapters' projection classes without
// treating two partial projections as semantic agreement.
type ProjectionComparison string

const (
	ProjectionBothExact     ProjectionComparison = "both_exact"
	ProjectionLeftPartial   ProjectionComparison = "left_partial"
	ProjectionRightPartial  ProjectionComparison = "right_partial"
	ProjectionBothPartial   ProjectionComparison = "both_partial"
	ProjectionNotComparable ProjectionComparison = "not_comparable"
)

var projectionComparisons = []ProjectionComparison{
	ProjectionBothExact, ProjectionLeftPartial, ProjectionRightPartial,
	ProjectionBothPartial, ProjectionNotComparable,
}

// ExecutionBoundaryComparison reports which executions reached the plan's
// comparison boundary. Equal error labels are never treated as agreement.
type ExecutionBoundaryComparison string

const (
	ExecutionBothReached     ExecutionBoundaryComparison = "both_reached"
	ExecutionLeftNotReached  ExecutionBoundaryComparison = "left_not_reached"
	ExecutionRightNotReached ExecutionBoundaryComparison = "right_not_reached"
	ExecutionNeitherReached  ExecutionBoundaryComparison = "neither_reached"
	ExecutionNotComparable   ExecutionBoundaryComparison = "not_comparable"
)

var executionBoundaryComparisons = []ExecutionBoundaryComparison{
	ExecutionBothReached, ExecutionLeftNotReached, ExecutionRightNotReached,
	ExecutionNeitherReached, ExecutionNotComparable,
}

// ApplicationComparison distinguishes workload/history divergence from a
// state-machine divergence after the same committed workload.
type ApplicationComparison string

const (
	ApplicationAgree                        ApplicationComparison = "agree"
	ApplicationWorkloadDivergence           ApplicationComparison = "workload_divergence"
	ApplicationStateDivergence              ApplicationComparison = "state_divergence"
	ApplicationLeftNodesDisagreeAtBoundary  ApplicationComparison = "left_nodes_disagree_at_boundary"
	ApplicationRightNodesDisagreeAtBoundary ApplicationComparison = "right_nodes_disagree_at_boundary"
	ApplicationNotComparable                ApplicationComparison = "not_comparable"
)

var applicationComparisons = []ApplicationComparison{
	ApplicationAgree, ApplicationWorkloadDivergence,
	ApplicationStateDivergence, ApplicationLeftNodesDisagreeAtBoundary,
	ApplicationRightNodesDisagreeAtBoundary, ApplicationNotComparable,
}

// NormalizedComparison compares independent axes. Adapter-specific invariant
// IDs never participate in Safety; only the common sorted sets do.
type NormalizedComparison struct {
	Schema               string                      `json:"schema"`
	PlanSHA256           string                      `json:"plan_sha256"`
	LeftExecutionSHA256  string                      `json:"left_execution_sha256"`
	RightExecutionSHA256 string                      `json:"right_execution_sha256"`
	LeftOutcomeSHA256    string                      `json:"left_outcome_sha256"`
	RightOutcomeSHA256   string                      `json:"right_outcome_sha256"`
	LeftAdapter          AdapterID                   `json:"left_adapter"`
	RightAdapter         AdapterID                   `json:"right_adapter"`
	Eligibility          EligibilityComparison       `json:"eligibility"`
	LeftProjection       ProjectionFidelity          `json:"left_projection"`
	RightProjection      ProjectionFidelity          `json:"right_projection"`
	Projection           ProjectionComparison        `json:"projection"`
	LeftCompletion       CompletionStatus            `json:"left_completion"`
	RightCompletion      CompletionStatus            `json:"right_completion"`
	ExecutionBoundary    ExecutionBoundaryComparison `json:"execution_boundary"`
	Safety               AgreementStatus             `json:"safety"`
	NegotiatedCommonIDs  []string                    `json:"negotiated_common_ids"`
	SharedCommonIDs      []string                    `json:"shared_common_ids"`
	LeftOnlyCommonIDs    []string                    `json:"left_only_common_ids"`
	RightOnlyCommonIDs   []string                    `json:"right_only_common_ids"`
	Application          ApplicationComparison       `json:"application"`
}

// NormalizeNodeCommitments converts an adapter's node-keyed portable results
// into the schema's canonical order and derives agreement at this boundary. It accepts no
// adapter-local protocol status.
func NormalizeNodeCommitments(commitments map[raft.NodeID]apporacle.Commitment) ([]NodeCommitment, bool, error) {
	if commitments == nil || len(commitments) > maxNormalizedNodes {
		return nil, false, errors.New("semanticplan: node commitment map is nil or exceeds its bound")
	}
	nodes := make([]NodeCommitment, 0, len(commitments))
	for node, commitment := range commitments {
		if err := validateNodeID(node); err != nil {
			return nil, false, err
		}
		if err := validateCommitment(commitment); err != nil {
			return nil, false, fmt.Errorf("semanticplan: node %q: %w", node, err)
		}
		nodes = append(nodes, NodeCommitment{Node: node, Commitment: commitment})
	}
	slices.SortFunc(nodes, func(left, right NodeCommitment) int {
		return strings.Compare(string(left.Node), string(right.Node))
	})
	return nodes, commitmentsAgree(nodes), nil
}

// NormalizeInvariantIDs splits observed invariant IDs by the negotiated
// common checker vocabulary. An ID not negotiated by both adapters remains
// adapter-specific and cannot affect the common safety-agreement axis.
func NormalizeInvariantIDs(observed, negotiatedCommon []string) (common, adapterSpecific []string, err error) {
	observed = canonicalStrings(observed)
	negotiatedCommon = canonicalStrings(negotiatedCommon)
	if err := validateInvariantSet("observed invariant IDs", observed); err != nil {
		return nil, nil, err
	}
	if err := validateInvariantSet("negotiated common invariant IDs", negotiatedCommon); err != nil {
		return nil, nil, err
	}
	commonSet := make(map[string]struct{}, len(negotiatedCommon))
	for _, id := range negotiatedCommon {
		commonSet[id] = struct{}{}
	}
	common, adapterSpecific = []string{}, []string{}
	for _, id := range observed {
		if _, negotiated := commonSet[id]; negotiated {
			common = append(common, id)
		} else {
			adapterSpecific = append(adapterSpecific, id)
		}
	}
	return common, adapterSpecific, nil
}

// CanonicalizeOutcome clones and orders set-like fields and derives whether
// application commitments agree at the boundary. It is the writer-side counterpart to strict
// validation, which rejects non-canonical input rather than silently fixing it.
func CanonicalizeOutcome(outcome NormalizedOutcome) (NormalizedOutcome, error) {
	outcome.NodeCommitments = slices.Clone(outcome.NodeCommitments)
	slices.SortFunc(outcome.NodeCommitments, func(left, right NodeCommitment) int {
		return strings.Compare(string(left.Node), string(right.Node))
	})
	outcome.CommonInvariantIDs = canonicalStrings(outcome.CommonInvariantIDs)
	outcome.AdapterSpecificInvariantIDs = canonicalStrings(outcome.AdapterSpecificInvariantIDs)
	outcome.NegotiatedCommonIDs = canonicalStrings(outcome.NegotiatedCommonIDs)
	outcome.ApplicationNodesAgreeAtBoundary = commitmentsAgree(outcome.NodeCommitments)
	if err := outcome.Validate(); err != nil {
		return NormalizedOutcome{}, err
	}
	return outcome, nil
}

// DecodeNormalizedOutcome strictly decodes one bounded normalized outcome.
func DecodeNormalizedOutcome(r io.Reader) (NormalizedOutcome, error) {
	var outcome NormalizedOutcome
	if err := decodeNormalized(r, &outcome); err != nil {
		return NormalizedOutcome{}, fmt.Errorf("decode normalized outcome: %w", err)
	}
	if err := outcome.Validate(); err != nil {
		return NormalizedOutcome{}, err
	}
	return outcome, nil
}

// Validate rejects unsupported, unbounded, or non-canonical outcomes.
func (outcome NormalizedOutcome) Validate() error {
	if outcome.Schema != NormalizedOutcomeSchema {
		return fmt.Errorf("semanticplan: unsupported normalized outcome schema %q", outcome.Schema)
	}
	if !digestPattern.MatchString(outcome.PlanSHA256) || !digestPattern.MatchString(outcome.ExecutionSHA256) {
		return errors.New("semanticplan: normalized outcome requires lowercase SHA-256 plan and execution hashes")
	}
	if err := validateAdapterID(artifact.Adapter(outcome.Adapter)); err != nil {
		return fmt.Errorf("semanticplan: normalized outcome adapter: %w", err)
	}
	if outcome.Eligibility != EligibilityEligible && outcome.Eligibility != EligibilityIneligible {
		return fmt.Errorf("semanticplan: invalid eligibility %q", outcome.Eligibility)
	}
	if outcome.Projection != ProjectionExact && outcome.Projection != ProjectionPartial && outcome.Projection != ProjectionFailed {
		return fmt.Errorf("semanticplan: invalid projection fidelity %q", outcome.Projection)
	}
	if !slices.Contains(completionStatuses, outcome.Completion) {
		return fmt.Errorf("semanticplan: invalid completion %q", outcome.Completion)
	}
	if outcome.Eligibility == EligibilityIneligible {
		if outcome.Projection != ProjectionFailed || outcome.Completion != CompletionNotExecuted || len(outcome.NodeCommitments) != 0 || outcome.ApplicationNodesAgreeAtBoundary || len(outcome.CommonInvariantIDs) != 0 || len(outcome.AdapterSpecificInvariantIDs) != 0 {
			return errors.New("semanticplan: ineligible outcome must be failed, not executed, and contain no execution evidence")
		}
	} else if outcome.Completion == CompletionNotExecuted {
		return errors.New("semanticplan: eligible outcome cannot have not_executed completion")
	} else if outcome.Projection == ProjectionFailed && outcome.Completion != CompletionExecutionError {
		return errors.New("semanticplan: eligible failed projection must complete as execution_error")
	}
	if outcome.NodeCommitments == nil || len(outcome.NodeCommitments) > maxNormalizedNodes {
		if outcome.NodeCommitments == nil {
			return errors.New("semanticplan: node commitments must be a non-nil canonical list")
		}
		return fmt.Errorf("semanticplan: node commitments exceed %d", maxNormalizedNodes)
	}
	for index, node := range outcome.NodeCommitments {
		if err := validateNodeID(node.Node); err != nil {
			return fmt.Errorf("semanticplan: node_commitments[%d]: %w", index, err)
		}
		if index > 0 && outcome.NodeCommitments[index-1].Node >= node.Node {
			return errors.New("semanticplan: node commitments must be sorted by strictly increasing node ID")
		}
		if err := validateCommitment(node.Commitment); err != nil {
			return fmt.Errorf("semanticplan: node_commitments[%d]: %w", index, err)
		}
	}
	if outcome.ApplicationNodesAgreeAtBoundary != commitmentsAgree(outcome.NodeCommitments) {
		return errors.New("semanticplan: application_nodes_agree_at_boundary does not match node commitments")
	}
	if err := validateInvariantSet("negotiated_common_ids", outcome.NegotiatedCommonIDs); err != nil {
		return err
	}
	if err := validateInvariantSet("common_invariant_ids", outcome.CommonInvariantIDs); err != nil {
		return err
	}
	if err := validateInvariantSet("adapter_specific_invariant_ids", outcome.AdapterSpecificInvariantIDs); err != nil {
		return err
	}
	if intersects(outcome.CommonInvariantIDs, outcome.AdapterSpecificInvariantIDs) {
		return errors.New("semanticplan: common and adapter-specific invariant ID sets overlap")
	}
	if !isSubset(outcome.CommonInvariantIDs, outcome.NegotiatedCommonIDs) || intersects(outcome.AdapterSpecificInvariantIDs, outcome.NegotiatedCommonIDs) {
		return errors.New("semanticplan: observed invariant IDs do not match the negotiated common universe")
	}
	return nil
}

// CompareNormalized validates and compares two outcomes for the same semantic
// plan. Adapter-local metadata cannot affect this operation because it is not
// represented by NormalizedOutcome.
func CompareNormalized(left, right NormalizedOutcome) (NormalizedComparison, error) {
	if err := left.Validate(); err != nil {
		return NormalizedComparison{}, fmt.Errorf("semanticplan: left outcome: %w", err)
	}
	if err := right.Validate(); err != nil {
		return NormalizedComparison{}, fmt.Errorf("semanticplan: right outcome: %w", err)
	}
	if left.PlanSHA256 != right.PlanSHA256 {
		return NormalizedComparison{}, errors.New("semanticplan: outcomes refer to different semantic plans")
	}
	if !slices.Equal(left.NegotiatedCommonIDs, right.NegotiatedCommonIDs) {
		return NormalizedComparison{}, errors.New("semanticplan: outcomes use different negotiated common invariant universes")
	}
	leftOutcomeDigest, err := DigestNormalizedOutcome(left)
	if err != nil {
		return NormalizedComparison{}, err
	}
	rightOutcomeDigest, err := DigestNormalizedOutcome(right)
	if err != nil {
		return NormalizedComparison{}, err
	}
	comparison := NormalizedComparison{
		Schema: NormalizedComparisonSchema, PlanSHA256: left.PlanSHA256,
		LeftExecutionSHA256: left.ExecutionSHA256, RightExecutionSHA256: right.ExecutionSHA256,
		LeftOutcomeSHA256: leftOutcomeDigest, RightOutcomeSHA256: rightOutcomeDigest,
		LeftAdapter: left.Adapter, RightAdapter: right.Adapter,
		Eligibility:    compareEligibility(left.Eligibility, right.Eligibility),
		LeftProjection: left.Projection, RightProjection: right.Projection,
		Projection:     compareProjection(left.Projection, right.Projection, left.Eligibility, right.Eligibility),
		LeftCompletion: left.Completion, RightCompletion: right.Completion,
		ExecutionBoundary: compareExecutionBoundary(left.Completion, right.Completion, left.Eligibility, right.Eligibility),
		Safety:            AgreementNotComparable, Application: ApplicationNotComparable,
		NegotiatedCommonIDs: slices.Clone(left.NegotiatedCommonIDs),
		SharedCommonIDs:     []string{}, LeftOnlyCommonIDs: []string{}, RightOnlyCommonIDs: []string{},
	}
	if comparison.ExecutionBoundary == ExecutionBothReached {
		comparison.SharedCommonIDs, comparison.LeftOnlyCommonIDs, comparison.RightOnlyCommonIDs = partitionSets(left.CommonInvariantIDs, right.CommonInvariantIDs)
		comparison.Safety = agreement(len(comparison.LeftOnlyCommonIDs) == 0 && len(comparison.RightOnlyCommonIDs) == 0)
		comparison.Application = compareApplication(left, right)
	}
	if err := comparison.Validate(); err != nil {
		return NormalizedComparison{}, err
	}
	return comparison, nil
}

// DecodeNormalizedComparison strictly decodes one bounded comparison.
func DecodeNormalizedComparison(r io.Reader) (NormalizedComparison, error) {
	var comparison NormalizedComparison
	if err := decodeNormalized(r, &comparison); err != nil {
		return NormalizedComparison{}, fmt.Errorf("decode normalized comparison: %w", err)
	}
	if err := comparison.Validate(); err != nil {
		return NormalizedComparison{}, err
	}
	return comparison, nil
}

// Validate rejects structurally inconsistent standalone comparisons.
func (comparison NormalizedComparison) Validate() error {
	if comparison.Schema != NormalizedComparisonSchema {
		return fmt.Errorf("semanticplan: unsupported normalized comparison schema %q", comparison.Schema)
	}
	if !digestPattern.MatchString(comparison.PlanSHA256) || !digestPattern.MatchString(comparison.LeftExecutionSHA256) || !digestPattern.MatchString(comparison.RightExecutionSHA256) || !digestPattern.MatchString(comparison.LeftOutcomeSHA256) || !digestPattern.MatchString(comparison.RightOutcomeSHA256) {
		return errors.New("semanticplan: normalized comparison requires lowercase SHA-256 plan, execution, and outcome hashes")
	}
	if err := validateAdapterID(artifact.Adapter(comparison.LeftAdapter)); err != nil {
		return fmt.Errorf("semanticplan: left adapter: %w", err)
	}
	if err := validateAdapterID(artifact.Adapter(comparison.RightAdapter)); err != nil {
		return fmt.Errorf("semanticplan: right adapter: %w", err)
	}
	if !slices.Contains(eligibilityComparisons, comparison.Eligibility) || !slices.Contains(projectionComparisons, comparison.Projection) || !slices.Contains(executionBoundaryComparisons, comparison.ExecutionBoundary) || !slices.Contains(completionStatuses, comparison.LeftCompletion) || !slices.Contains(completionStatuses, comparison.RightCompletion) || !slices.Contains(agreementStatuses, comparison.Safety) || !slices.Contains(applicationComparisons, comparison.Application) {
		return errors.New("semanticplan: normalized comparison contains an unknown status")
	}
	leftEligibility := eligibilityFromComparison(comparison.Eligibility, true)
	rightEligibility := eligibilityFromComparison(comparison.Eligibility, false)
	if err := validateProjectionCompletionPair("left", leftEligibility, comparison.LeftProjection, comparison.LeftCompletion); err != nil {
		return err
	}
	if err := validateProjectionCompletionPair("right", rightEligibility, comparison.RightProjection, comparison.RightCompletion); err != nil {
		return err
	}
	wantProjection := compareProjection(comparison.LeftProjection, comparison.RightProjection, leftEligibility, rightEligibility)
	if comparison.Projection != wantProjection {
		return errors.New("semanticplan: projection comparison does not match the two fidelities and eligibility")
	}
	wantBoundary := compareExecutionBoundary(comparison.LeftCompletion, comparison.RightCompletion, leftEligibility, rightEligibility)
	if comparison.ExecutionBoundary != wantBoundary {
		return errors.New("semanticplan: execution boundary does not match the two completion statuses and eligibility")
	}
	if err := validateInvariantSet("shared_common_ids", comparison.SharedCommonIDs); err != nil {
		return err
	}
	if err := validateInvariantSet("negotiated_common_ids", comparison.NegotiatedCommonIDs); err != nil {
		return err
	}
	if err := validateInvariantSet("left_only_common_ids", comparison.LeftOnlyCommonIDs); err != nil {
		return err
	}
	if err := validateInvariantSet("right_only_common_ids", comparison.RightOnlyCommonIDs); err != nil {
		return err
	}
	if intersects(comparison.SharedCommonIDs, comparison.LeftOnlyCommonIDs) || intersects(comparison.SharedCommonIDs, comparison.RightOnlyCommonIDs) || intersects(comparison.LeftOnlyCommonIDs, comparison.RightOnlyCommonIDs) {
		return errors.New("semanticplan: comparison common-ID partitions overlap")
	}
	if !isSubset(comparison.SharedCommonIDs, comparison.NegotiatedCommonIDs) || !isSubset(comparison.LeftOnlyCommonIDs, comparison.NegotiatedCommonIDs) || !isSubset(comparison.RightOnlyCommonIDs, comparison.NegotiatedCommonIDs) {
		return errors.New("semanticplan: comparison names IDs outside the negotiated common universe")
	}
	if comparison.ExecutionBoundary != ExecutionBothReached && (comparison.Safety != AgreementNotComparable || comparison.Application != ApplicationNotComparable) {
		return errors.New("semanticplan: safety and application require both executions to reach the comparison boundary")
	}
	if comparison.ExecutionBoundary == ExecutionBothReached && comparison.Safety == AgreementNotComparable {
		return errors.New("semanticplan: common safety must be compared when both executions reach the boundary")
	}
	if comparison.Safety == AgreementAgree && (len(comparison.LeftOnlyCommonIDs) != 0 || len(comparison.RightOnlyCommonIDs) != 0) {
		return errors.New("semanticplan: agreeing safety comparison has side-only common IDs")
	}
	if comparison.Safety == AgreementDisagree && len(comparison.LeftOnlyCommonIDs)+len(comparison.RightOnlyCommonIDs) == 0 {
		return errors.New("semanticplan: disagreeing safety comparison requires a side-only common ID")
	}
	if comparison.Safety == AgreementNotComparable && len(comparison.SharedCommonIDs)+len(comparison.LeftOnlyCommonIDs)+len(comparison.RightOnlyCommonIDs) != 0 {
		return errors.New("semanticplan: non-comparable safety axis cannot carry common-ID partitions")
	}
	return nil
}

func validateProjectionCompletionPair(side string, eligibility EligibilityStatus, projection ProjectionFidelity, completion CompletionStatus) error {
	if projection != ProjectionExact && projection != ProjectionPartial && projection != ProjectionFailed {
		return fmt.Errorf("semanticplan: %s projection fidelity %q is invalid", side, projection)
	}
	if eligibility == EligibilityIneligible {
		if projection != ProjectionFailed || completion != CompletionNotExecuted {
			return fmt.Errorf("semanticplan: %s ineligible execution must have failed projection and not_executed completion", side)
		}
		return nil
	}
	if completion == CompletionNotExecuted {
		return fmt.Errorf("semanticplan: %s eligible execution cannot be not_executed", side)
	}
	if projection == ProjectionFailed && completion != CompletionExecutionError {
		return fmt.Errorf("semanticplan: %s eligible failed projection must have execution_error completion", side)
	}
	return nil
}

func compareEligibility(left, right EligibilityStatus) EligibilityComparison {
	switch {
	case left == EligibilityEligible && right == EligibilityEligible:
		return EligibilityBothEligible
	case left == EligibilityIneligible && right == EligibilityEligible:
		return EligibilityLeftIneligible
	case left == EligibilityEligible && right == EligibilityIneligible:
		return EligibilityRightIneligible
	default:
		return EligibilityBothIneligible
	}
}

func eligibilityFromComparison(comparison EligibilityComparison, left bool) EligibilityStatus {
	switch comparison {
	case EligibilityBothEligible:
		return EligibilityEligible
	case EligibilityLeftIneligible:
		if left {
			return EligibilityIneligible
		}
		return EligibilityEligible
	case EligibilityRightIneligible:
		if left {
			return EligibilityEligible
		}
		return EligibilityIneligible
	default:
		return EligibilityIneligible
	}
}

func compareProjection(left, right ProjectionFidelity, leftEligibility, rightEligibility EligibilityStatus) ProjectionComparison {
	if leftEligibility != EligibilityEligible || rightEligibility != EligibilityEligible {
		return ProjectionNotComparable
	}
	if left == ProjectionFailed || right == ProjectionFailed {
		return ProjectionNotComparable
	}
	switch {
	case left == ProjectionExact && right == ProjectionExact:
		return ProjectionBothExact
	case left == ProjectionPartial && right == ProjectionExact:
		return ProjectionLeftPartial
	case left == ProjectionExact && right == ProjectionPartial:
		return ProjectionRightPartial
	default:
		return ProjectionBothPartial
	}
}

func compareExecutionBoundary(left, right CompletionStatus, leftEligibility, rightEligibility EligibilityStatus) ExecutionBoundaryComparison {
	if leftEligibility != EligibilityEligible || rightEligibility != EligibilityEligible {
		return ExecutionNotComparable
	}
	leftReached := completionReachedBoundary(left)
	rightReached := completionReachedBoundary(right)
	switch {
	case leftReached && rightReached:
		return ExecutionBothReached
	case !leftReached && rightReached:
		return ExecutionLeftNotReached
	case leftReached && !rightReached:
		return ExecutionRightNotReached
	default:
		return ExecutionNeitherReached
	}
}

func completionReachedBoundary(status CompletionStatus) bool {
	return status == CompletionCompleted
}

func agreement(equal bool) AgreementStatus {
	if equal {
		return AgreementAgree
	}
	return AgreementDisagree
}

func compareApplication(left, right NormalizedOutcome) ApplicationComparison {
	if len(left.NodeCommitments) == 0 || len(right.NodeCommitments) == 0 {
		return ApplicationNotComparable
	}
	if len(left.NodeCommitments) != len(right.NodeCommitments) {
		return ApplicationNotComparable
	}
	for index := range left.NodeCommitments {
		if left.NodeCommitments[index].Node != right.NodeCommitments[index].Node {
			return ApplicationNotComparable
		}
	}
	if !left.ApplicationNodesAgreeAtBoundary {
		return ApplicationLeftNodesDisagreeAtBoundary
	}
	if !right.ApplicationNodesAgreeAtBoundary {
		return ApplicationRightNodesDisagreeAtBoundary
	}
	leftCommitment := left.NodeCommitments[0].Commitment
	rightCommitment := right.NodeCommitments[0].Commitment
	if leftCommitment.Commands != rightCommitment.Commands || leftCommitment.ChainDigest != rightCommitment.ChainDigest {
		return ApplicationWorkloadDivergence
	}
	if leftCommitment.StateDigest != rightCommitment.StateDigest {
		return ApplicationStateDivergence
	}
	return ApplicationAgree
}

func commitmentsAgree(nodes []NodeCommitment) bool {
	if len(nodes) == 0 {
		return false
	}
	want := nodes[0].Commitment
	for _, node := range nodes[1:] {
		if node.Commitment != want {
			return false
		}
	}
	return true
}

func validateCommitment(commitment apporacle.Commitment) error {
	if commitment.Schema != apporacle.CommitmentSchema || uint64(commitment.Commands) > apporacle.MaxCommands || !digestPattern.MatchString(commitment.ChainDigest) || !digestPattern.MatchString(commitment.StateDigest) {
		return errors.New("invalid portable application commitment")
	}
	return nil
}

func validateNodeID(id raft.NodeID) error {
	value := string(id)
	if value == "" || len(value) > 256 || !utf8.ValidString(value) || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("invalid semantic node ID %q", id)
	}
	return nil
}

func validateAdapterID(adapter artifact.Adapter) error {
	if adapter.ID == "" || len(adapter.ID) > 256 || adapter.Version == "" || len(adapter.Version) > 256 || !utf8.ValidString(adapter.ID) || !utf8.ValidString(adapter.Version) || strings.TrimSpace(adapter.ID) != adapter.ID || strings.TrimSpace(adapter.Version) != adapter.Version || strings.IndexFunc(adapter.ID+adapter.Version, unicode.IsControl) >= 0 {
		return errors.New("adapter ID and version must be bounded nonempty canonical strings")
	}
	return nil
}

func validateInvariantSet(name string, values []string) error {
	if values == nil || len(values) > maxNormalizedInvariants {
		return fmt.Errorf("semanticplan: %s must be a non-nil set of at most %d IDs", name, maxNormalizedInvariants)
	}
	for index, value := range values {
		if !invariantPattern.MatchString(value) {
			return fmt.Errorf("semanticplan: %s[%d] is not a valid invariant ID", name, index)
		}
		if index > 0 && values[index-1] >= value {
			return fmt.Errorf("semanticplan: %s must be sorted and unique", name)
		}
	}
	return nil
}

func canonicalStrings(values []string) []string {
	result := slices.Clone(values)
	if result == nil {
		result = []string{}
	}
	slices.Sort(result)
	return slices.Compact(result)
}

func intersects(left, right []string) bool {
	leftIndex, rightIndex := 0, 0
	for leftIndex < len(left) && rightIndex < len(right) {
		switch strings.Compare(left[leftIndex], right[rightIndex]) {
		case -1:
			leftIndex++
		case 1:
			rightIndex++
		default:
			return true
		}
	}
	return false
}

func isSubset(subset, set []string) bool {
	setIndex := 0
	for _, value := range subset {
		for setIndex < len(set) && set[setIndex] < value {
			setIndex++
		}
		if setIndex == len(set) || set[setIndex] != value {
			return false
		}
	}
	return true
}

func partitionSets(left, right []string) (shared, leftOnly, rightOnly []string) {
	shared, leftOnly, rightOnly = []string{}, []string{}, []string{}
	leftIndex, rightIndex := 0, 0
	for leftIndex < len(left) && rightIndex < len(right) {
		switch strings.Compare(left[leftIndex], right[rightIndex]) {
		case -1:
			leftOnly = append(leftOnly, left[leftIndex])
			leftIndex++
		case 1:
			rightOnly = append(rightOnly, right[rightIndex])
			rightIndex++
		default:
			shared = append(shared, left[leftIndex])
			leftIndex++
			rightIndex++
		}
	}
	leftOnly = append(leftOnly, left[leftIndex:]...)
	rightOnly = append(rightOnly, right[rightIndex:]...)
	return shared, leftOnly, rightOnly
}

func decodeNormalized(r io.Reader, target any) error {
	if r == nil {
		return errors.New("nil reader")
	}
	data, err := io.ReadAll(io.LimitReader(r, maxNormalizedDocumentBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxNormalizedDocumentBytes {
		return fmt.Errorf("document exceeds %d bytes", maxNormalizedDocumentBytes)
	}
	if err := strictjson.RejectDuplicateNames(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

// DigestNormalizedOutcome returns SHA-256 of the validated canonical JSON
// representation. Because validation requires canonical ordering, the digest
// is stable across encoders and map iteration orders.
func DigestNormalizedOutcome(outcome NormalizedOutcome) (string, error) {
	if err := outcome.Validate(); err != nil {
		return "", err
	}
	return digestCanonicalJSON(outcome)
}

// DigestNormalizedComparison returns SHA-256 of the validated canonical JSON
// representation.
func DigestNormalizedComparison(comparison NormalizedComparison) (string, error) {
	if err := comparison.Validate(); err != nil {
		return "", err
	}
	return digestCanonicalJSON(comparison)
}

func digestCanonicalJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:]), nil
}
