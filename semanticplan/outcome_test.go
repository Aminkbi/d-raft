package semanticplan

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/raft"
)

func TestCanonicalizeOutcomeOrdersPortableSetsAndDerivesBoundaryAgreement(t *testing.T) {
	commitment := apporacle.New().Commitment()
	outcome := validNormalizedOutcome("reference", digestOf('a'), []NodeCommitment{
		{Node: "c", Commitment: commitment}, {Node: "a", Commitment: commitment}, {Node: "b", Commitment: commitment},
	})
	outcome.CommonInvariantIDs = []string{"raft/log-matching", "raft/election-safety", "raft/log-matching"}
	outcome.AdapterSpecificInvariantIDs = []string{"reference/storage-order", "reference/message-shape"}
	outcome.ApplicationNodesAgreeAtBoundary = false

	canonical, err := CanonicalizeOutcome(outcome)
	if err != nil {
		t.Fatalf("CanonicalizeOutcome: %v", err)
	}
	gotNodes := []raft.NodeID{canonical.NodeCommitments[0].Node, canonical.NodeCommitments[1].Node, canonical.NodeCommitments[2].Node}
	if !slices.Equal(gotNodes, []raft.NodeID{"a", "b", "c"}) || !canonical.ApplicationNodesAgreeAtBoundary {
		t.Fatalf("canonical nodes/boundary agreement = %v/%v", gotNodes, canonical.ApplicationNodesAgreeAtBoundary)
	}
	if !slices.Equal(canonical.CommonInvariantIDs, []string{"raft/election-safety", "raft/log-matching"}) || !slices.Equal(canonical.AdapterSpecificInvariantIDs, []string{"reference/message-shape", "reference/storage-order"}) {
		t.Fatalf("canonical invariant sets = %v / %v", canonical.CommonInvariantIDs, canonical.AdapterSpecificInvariantIDs)
	}

	canonical.NodeCommitments[2].Commitment.StateDigest = digestOf('f')
	canonical.ApplicationNodesAgreeAtBoundary = true
	if canonical.Validate() == nil {
		t.Fatal("Validate accepted false boundary-agreement claim")
	}
}

func TestNormalizedConversionHelpers(t *testing.T) {
	commitment := apporacle.New().Commitment()
	nodes, converged, err := NormalizeNodeCommitments(map[raft.NodeID]apporacle.Commitment{
		"b": commitment,
		"a": commitment,
	})
	if err != nil || !converged || len(nodes) != 2 || nodes[0].Node != "a" || nodes[1].Node != "b" {
		t.Fatalf("NormalizeNodeCommitments = %+v, %v, %v", nodes, converged, err)
	}
	common, specific, err := NormalizeInvariantIDs(
		[]string{"adapter/private", "raft/log-matching", "raft/election-safety", "raft/log-matching"},
		[]string{"raft/election-safety", "raft/log-matching", "raft/snapshot-conflict"},
	)
	if err != nil || !slices.Equal(common, []string{"raft/election-safety", "raft/log-matching"}) || !slices.Equal(specific, []string{"adapter/private"}) {
		t.Fatalf("NormalizeInvariantIDs = %v / %v, %v", common, specific, err)
	}
}

func TestNormalizedOutcomeCompletionVocabulary(t *testing.T) {
	statuses := []CompletionStatus{
		CompletionCompleted, CompletionSafetyViolation, CompletionDecisionExhausted,
		CompletionStepLimit, CompletionTimeLimit, CompletionExecutionError,
	}
	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			outcome := validNormalizedOutcome("reference", digestOf('a'), nil)
			outcome.Completion = status
			if err := outcome.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}

	ineligible := ineligibleNormalizedOutcome("reference")
	if err := ineligible.Validate(); err != nil {
		t.Fatalf("ineligible Validate: %v", err)
	}
	bad := validNormalizedOutcome("reference", digestOf('a'), nil)
	bad.Completion = CompletionNotExecuted
	if bad.Validate() == nil {
		t.Fatal("eligible not_executed outcome accepted")
	}
	failed := validNormalizedOutcome("reference", digestOf('a'), nil)
	failed.Projection = ProjectionFailed
	failed.Completion = CompletionExecutionError
	if err := failed.Validate(); err != nil {
		t.Fatalf("eligible projection failure was not representable: %v", err)
	}
}

func TestDecodeNormalizedOutcomeIsStrictAndBounded(t *testing.T) {
	outcome := validNormalizedOutcome("reference", digestOf('a'), nil)
	encoded, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeNormalizedOutcome(bytes.NewReader(encoded))
	if err != nil || !reflect.DeepEqual(decoded, outcome) {
		t.Fatalf("DecodeNormalizedOutcome = %+v, %v", decoded, err)
	}
	for name, document := range map[string]string{
		"unknown":  strings.TrimSuffix(string(encoded), "}") + `,"term":7}`,
		"trailing": string(encoded) + `{}`,
		"bad hash": strings.Replace(string(encoded), digestOf('a'), "ABC", 1),
		"duplicate top-level": strings.Replace(string(encoded),
			`"schema":"`+NormalizedOutcomeSchema+`"`,
			`"schema":"`+NormalizedOutcomeSchema+`","schema":"`+NormalizedOutcomeSchema+`"`, 1),
		"duplicate nested": strings.Replace(string(encoded),
			`"adapter":{"id":"d-raft/reference"`,
			`"adapter":{"id":"shadow","id":"d-raft/reference"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeNormalizedOutcome(strings.NewReader(document)); err == nil {
				t.Fatal("invalid document accepted")
			}
		})
	}

	unsorted := outcome
	unsorted.CommonInvariantIDs = []string{"raft/z", "raft/a"}
	encoded, _ = json.Marshal(unsorted)
	if _, err := DecodeNormalizedOutcome(bytes.NewReader(encoded)); err == nil {
		t.Fatal("unsorted invariant set accepted")
	}
}

func TestCompareNormalizedProjectionAndExecutionAxes(t *testing.T) {
	testCases := []struct {
		name            string
		leftProjection  ProjectionFidelity
		rightProjection ProjectionFidelity
		leftCompletion  CompletionStatus
		rightCompletion CompletionStatus
		wantProjection  ProjectionComparison
		wantBoundary    ExecutionBoundaryComparison
	}{
		{"both exact reached", ProjectionExact, ProjectionExact, CompletionCompleted, CompletionCompleted, ProjectionBothExact, ExecutionBothReached},
		{"left partial", ProjectionPartial, ProjectionExact, CompletionCompleted, CompletionCompleted, ProjectionLeftPartial, ExecutionBothReached},
		{"right partial", ProjectionExact, ProjectionPartial, CompletionCompleted, CompletionCompleted, ProjectionRightPartial, ExecutionBothReached},
		{"both partial", ProjectionPartial, ProjectionPartial, CompletionCompleted, CompletionCompleted, ProjectionBothPartial, ExecutionBothReached},
		{"left not reached", ProjectionExact, ProjectionExact, CompletionExecutionError, CompletionCompleted, ProjectionBothExact, ExecutionLeftNotReached},
		{"right not reached", ProjectionExact, ProjectionExact, CompletionCompleted, CompletionStepLimit, ProjectionBothExact, ExecutionRightNotReached},
		{"neither reached", ProjectionExact, ProjectionExact, CompletionTimeLimit, CompletionSafetyViolation, ProjectionBothExact, ExecutionNeitherReached},
		{"projection failed", ProjectionFailed, ProjectionExact, CompletionExecutionError, CompletionCompleted, ProjectionNotComparable, ExecutionLeftNotReached},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			left := validNormalizedOutcome("left", digestOf('1'), nil)
			right := validNormalizedOutcome("right", digestOf('2'), nil)
			left.Projection, right.Projection = test.leftProjection, test.rightProjection
			left.Completion, right.Completion = test.leftCompletion, test.rightCompletion
			comparison, err := CompareNormalized(left, right)
			if err != nil {
				t.Fatalf("CompareNormalized: %v", err)
			}
			if comparison.Projection != test.wantProjection || comparison.ExecutionBoundary != test.wantBoundary || comparison.LeftExecutionSHA256 == comparison.RightExecutionSHA256 {
				t.Fatalf("projection/boundary/hashes = %q/%q/%q/%q", comparison.Projection, comparison.ExecutionBoundary, comparison.LeftExecutionSHA256, comparison.RightExecutionSHA256)
			}
		})
	}
}

func TestCompareNormalizedEligibilityStatuses(t *testing.T) {
	testCases := []struct {
		name        string
		left, right NormalizedOutcome
		want        EligibilityComparison
	}{
		{"both eligible", validNormalizedOutcome("left", digestOf('1'), nil), validNormalizedOutcome("right", digestOf('2'), nil), EligibilityBothEligible},
		{"left ineligible", ineligibleNormalizedOutcome("left"), validNormalizedOutcome("right", digestOf('2'), nil), EligibilityLeftIneligible},
		{"right ineligible", validNormalizedOutcome("left", digestOf('1'), nil), ineligibleNormalizedOutcome("right"), EligibilityRightIneligible},
		{"both ineligible", ineligibleNormalizedOutcome("left"), ineligibleNormalizedOutcome("right"), EligibilityBothIneligible},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			comparison, err := CompareNormalized(test.left, test.right)
			if err != nil {
				t.Fatalf("CompareNormalized: %v", err)
			}
			if comparison.Eligibility != test.want {
				t.Fatalf("eligibility = %q, want %q", comparison.Eligibility, test.want)
			}
			if test.want != EligibilityBothEligible && (comparison.Projection != ProjectionNotComparable || comparison.ExecutionBoundary != ExecutionNotComparable || comparison.Safety != AgreementNotComparable || comparison.Application != ApplicationNotComparable) {
				t.Fatalf("ineligible axes were overclaimed: %+v", comparison)
			}
		})
	}
}

func TestCompareNormalizedApplicationStatuses(t *testing.T) {
	empty := apporacle.New().Commitment()
	changedWorkload := empty
	changedWorkload.Commands++
	changedWorkload.ChainDigest = digestOf('c')
	changedState := empty
	changedState.StateDigest = digestOf('d')

	testCases := []struct {
		name  string
		left  []NodeCommitment
		right []NodeCommitment
		want  ApplicationComparison
	}{
		{"agree", nodes(empty), nodes(empty), ApplicationAgree},
		{"workload divergence", nodes(empty), nodes(changedWorkload), ApplicationWorkloadDivergence},
		{"state divergence", nodes(empty), nodes(changedState), ApplicationStateDivergence},
		{"left nodes disagree at boundary", []NodeCommitment{{Node: "a", Commitment: empty}, {Node: "b", Commitment: changedState}}, nodes(empty), ApplicationLeftNodesDisagreeAtBoundary},
		{"right nodes disagree at boundary", nodes(empty), []NodeCommitment{{Node: "a", Commitment: empty}, {Node: "b", Commitment: changedState}}, ApplicationRightNodesDisagreeAtBoundary},
		{"different members", nodes(empty), []NodeCommitment{{Node: "a", Commitment: empty}}, ApplicationNotComparable},
		{"not comparable", nil, nil, ApplicationNotComparable},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			left := mustCanonicalOutcome(t, validNormalizedOutcome("left", digestOf('1'), test.left))
			right := mustCanonicalOutcome(t, validNormalizedOutcome("right", digestOf('2'), test.right))
			comparison, err := CompareNormalized(left, right)
			if err != nil {
				t.Fatalf("CompareNormalized: %v", err)
			}
			if comparison.Application != test.want {
				t.Fatalf("application = %q, want %q", comparison.Application, test.want)
			}
		})
	}
}

func TestCompareNormalizedUsesOnlyNegotiatedCommonInvariantIDs(t *testing.T) {
	left := validNormalizedOutcome("left", digestOf('1'), nil)
	right := validNormalizedOutcome("right", digestOf('2'), nil)
	left.CommonInvariantIDs = []string{"raft/election-safety", "raft/log-matching"}
	right.CommonInvariantIDs = []string{"raft/election-safety", "raft/snapshot-conflict"}
	left.AdapterSpecificInvariantIDs = []string{"left/private-check"}
	right.AdapterSpecificInvariantIDs = []string{"right/entirely-different-check"}

	comparison, err := CompareNormalized(left, right)
	if err != nil {
		t.Fatalf("CompareNormalized: %v", err)
	}
	if comparison.Safety != AgreementDisagree || !slices.Equal(comparison.SharedCommonIDs, []string{"raft/election-safety"}) || !slices.Equal(comparison.LeftOnlyCommonIDs, []string{"raft/log-matching"}) || !slices.Equal(comparison.RightOnlyCommonIDs, []string{"raft/snapshot-conflict"}) {
		t.Fatalf("common safety partition = %+v", comparison)
	}

	right.CommonInvariantIDs = slices.Clone(left.CommonInvariantIDs)
	comparison, err = CompareNormalized(left, right)
	if err != nil || comparison.Safety != AgreementAgree {
		t.Fatalf("adapter-specific IDs affected common safety: %+v, %v", comparison, err)
	}
}

func TestCompareNormalizedBindsOutcomesAndNegotiatedUniverse(t *testing.T) {
	left := validNormalizedOutcome("left", digestOf('1'), nil)
	right := validNormalizedOutcome("right", digestOf('2'), nil)
	comparison, err := CompareNormalized(left, right)
	if err != nil {
		t.Fatal(err)
	}
	leftDigest, _ := DigestNormalizedOutcome(left)
	rightDigest, _ := DigestNormalizedOutcome(right)
	if comparison.LeftOutcomeSHA256 != leftDigest || comparison.RightOutcomeSHA256 != rightDigest ||
		!slices.Equal(comparison.NegotiatedCommonIDs, left.NegotiatedCommonIDs) {
		t.Fatalf("comparison bindings = %#v", comparison)
	}

	right.NegotiatedCommonIDs = []string{"raft/log-matching"}
	if _, err := CompareNormalized(left, right); err == nil {
		t.Fatal("different negotiated invariant universes were compared")
	}

	comparison.LeftOutcomeSHA256 = "bad"
	if err := comparison.Validate(); err == nil {
		t.Fatal("invalid normalized-outcome binding accepted")
	}
}

func TestNormalizedComparisonExcludesAdapterLocalRaftState(t *testing.T) {
	left := validNormalizedOutcome("left", digestOf('1'), nodes(apporacle.New().Commitment()))
	right := validNormalizedOutcome("right", digestOf('2'), nodes(apporacle.New().Commitment()))
	comparison, err := CompareNormalized(left, right)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(comparison)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{`"term"`, `"leader"`, `"commit_index"`, `"applied_index"`, `"observation_digest"`, `"local_digest"`} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("adapter-local field %s leaked into %s", forbidden, encoded)
		}
	}

	digest, err := DigestNormalizedComparison(comparison)
	if err != nil || len(digest) != 64 {
		t.Fatalf("DigestNormalizedComparison = %q, %v", digest, err)
	}
	decoded, err := DecodeNormalizedComparison(bytes.NewReader(encoded))
	if err != nil || !reflect.DeepEqual(decoded, comparison) {
		t.Fatalf("DecodeNormalizedComparison = %+v, %v", decoded, err)
	}
	unknown := strings.TrimSuffix(string(encoded), "}") + `,"leader":"a"}`
	if _, err := DecodeNormalizedComparison(strings.NewReader(unknown)); err == nil {
		t.Fatal("comparison decoder accepted adapter-local unknown field")
	}
	if _, err := DecodeNormalizedComparison(strings.NewReader(string(encoded) + `{}`)); err == nil {
		t.Fatal("comparison decoder accepted a second JSON value")
	}
	for name, document := range map[string][]byte{
		"top level": bytes.Replace(encoded,
			[]byte(`"schema":"`+NormalizedComparisonSchema+`"`),
			[]byte(`"schema":"`+NormalizedComparisonSchema+`","schema":"`+NormalizedComparisonSchema+`"`), 1),
		"nested": bytes.Replace(encoded,
			[]byte(`"left_adapter":{"id":"d-raft/left"`),
			[]byte(`"left_adapter":{"id":"shadow","id":"d-raft/left"`), 1),
	} {
		t.Run("duplicate "+name, func(t *testing.T) {
			if _, err := DecodeNormalizedComparison(bytes.NewReader(document)); err == nil {
				t.Fatal("comparison decoder accepted a duplicate field")
			}
		})
	}

	outcomeDigest, err := DigestNormalizedOutcome(left)
	if err != nil || len(outcomeDigest) != 64 {
		t.Fatalf("DigestNormalizedOutcome = %q, %v", outcomeDigest, err)
	}
}

func validNormalizedOutcome(adapter string, executionHash string, nodeCommitments []NodeCommitment) NormalizedOutcome {
	if nodeCommitments == nil {
		nodeCommitments = []NodeCommitment{}
	}
	return NormalizedOutcome{
		Schema: NormalizedOutcomeSchema, PlanSHA256: digestOf('a'), ExecutionSHA256: executionHash,
		Adapter: AdapterID{ID: "d-raft/" + adapter, Version: "1"}, Eligibility: EligibilityEligible,
		Projection: ProjectionExact, Completion: CompletionCompleted,
		ApplicationNodesAgreeAtBoundary: commitmentsAgree(nodeCommitments), NodeCommitments: nodeCommitments,
		NegotiatedCommonIDs: []string{"raft/election-safety", "raft/log-matching", "raft/snapshot-conflict"},
		CommonInvariantIDs:  []string{}, AdapterSpecificInvariantIDs: []string{},
	}
}

func ineligibleNormalizedOutcome(adapter string) NormalizedOutcome {
	return NormalizedOutcome{
		Schema: NormalizedOutcomeSchema, PlanSHA256: digestOf('a'), ExecutionSHA256: digestOf('0'),
		Adapter: AdapterID{ID: "d-raft/" + adapter, Version: "1"}, Eligibility: EligibilityIneligible,
		Projection: ProjectionFailed, Completion: CompletionNotExecuted, ApplicationNodesAgreeAtBoundary: false,
		NodeCommitments: []NodeCommitment{}, NegotiatedCommonIDs: []string{"raft/election-safety", "raft/log-matching", "raft/snapshot-conflict"},
		CommonInvariantIDs: []string{}, AdapterSpecificInvariantIDs: []string{},
	}
}

func nodes(commitment apporacle.Commitment) []NodeCommitment {
	return []NodeCommitment{{Node: "a", Commitment: commitment}, {Node: "b", Commitment: commitment}}
}

func mustCanonicalOutcome(t *testing.T, outcome NormalizedOutcome) NormalizedOutcome {
	t.Helper()
	canonical, err := CanonicalizeOutcome(outcome)
	if err != nil {
		t.Fatalf("CanonicalizeOutcome: %v", err)
	}
	return canonical
}

func digestOf(character byte) string { return strings.Repeat(string(character), 64) }
