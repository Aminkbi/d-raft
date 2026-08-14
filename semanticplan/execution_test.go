package semanticplan

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
)

func TestSemanticExecutionRoundTripDigestAndNormalization(t *testing.T) {
	plan := testPlan(t)
	plan.Directives = []Directive{}
	capabilities := testCapabilities(plan.Source.Adapter)
	capabilities.ProjectionKinds = allProjectionKinds()
	execution := validSemanticExecution(t, plan, capabilities)

	digest, err := DigestSemanticExecution(execution)
	if err != nil || len(digest) != 64 {
		t.Fatalf("DigestSemanticExecution = %q, %v", digest, err)
	}
	encoded, err := json.Marshal(execution)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSemanticExecution(bytes.NewReader(encoded))
	if err != nil || !reflect.DeepEqual(decoded, execution) {
		t.Fatalf("DecodeSemanticExecution = %#v, %v", decoded, err)
	}
	normalized, err := NormalizeExecution(plan, capabilities, capabilities, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.ExecutionSHA256 != digest || normalized.Completion != CompletionCompleted || !normalized.ApplicationNodesAgreeAtBoundary || len(normalized.NodeCommitments) != 3 {
		t.Fatalf("NormalizeExecution = %#v", normalized)
	}
	if err := normalized.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSemanticExecutionStrictFailureShapes(t *testing.T) {
	plan := testPlan(t)
	plan.Directives = []Directive{}
	capabilities := testCapabilities(plan.Source.Adapter)
	capabilities.ProjectionKinds = allProjectionKinds()
	valid := validSemanticExecution(t, plan, capabilities)

	tests := map[string]func(*SemanticExecution){
		"null decisions":   func(execution *SemanticExecution) { execution.Decisions.Entries = nil },
		"null rejections":  func(execution *SemanticExecution) { execution.Rejections = nil },
		"null commitments": func(execution *SemanticExecution) { execution.NodeCommitments = nil },
		"bad projection accounting": func(execution *SemanticExecution) {
			execution.Projection.Fixed = 1
		},
		"eligible rejection": func(execution *SemanticExecution) {
			execution.Rejections = []RejectionCode{RejectUnsupportedAction}
		},
		"bad raw status": func(execution *SemanticExecution) {
			execution.Outcome.Status = artifact.OutcomeCompleted
			execution.Outcome.Error = "unexpected"
		},
		"missing build provenance": func(execution *SemanticExecution) {
			execution.Reproducibility.GoVersion = ""
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			execution := valid
			execution.Decisions = decision.CloneTape(valid.Decisions)
			execution.NodeCommitments = cloneNodeCommitments(valid.NodeCommitments)
			outcome := *valid.Outcome
			execution.Outcome = &outcome
			mutate(&execution)
			if err := execution.Validate(); err == nil {
				t.Fatal("invalid execution accepted")
			}
		})
	}

	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	unknown := strings.TrimSuffix(string(encoded), "}") + `,"term":7}`
	if _, err := DecodeSemanticExecution(strings.NewReader(unknown)); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := DecodeSemanticExecution(strings.NewReader(string(encoded) + `{}`)); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	for name, document := range map[string][]byte{
		"top level": bytes.Replace(encoded,
			[]byte(`"schema":"`+SemanticExecutionSchema+`"`),
			[]byte(`"schema":"`+SemanticExecutionSchema+`","schema":"`+SemanticExecutionSchema+`"`), 1),
		"nested": bytes.Replace(encoded,
			[]byte(`"adapter":{"id":"`+capabilities.Adapter.ID+`"`),
			[]byte(`"adapter":{"id":"shadow","id":"`+capabilities.Adapter.ID+`"`), 1),
	} {
		t.Run("duplicate "+name, func(t *testing.T) {
			if _, err := DecodeSemanticExecution(bytes.NewReader(document)); err == nil {
				t.Fatal("duplicate field accepted")
			}
		})
	}
}

func TestSemanticExecutionIneligibleAndCrossReferences(t *testing.T) {
	plan := testPlan(t)
	plan.Directives = []Directive{}
	capabilities := testCapabilities(plan.Source.Adapter)
	capabilities.ProjectionKinds = allProjectionKinds()
	ineligiblePlan := plan
	ineligiblePlan.Scenario.Actions = []artifact.Action{{AtNS: 1, Kind: artifact.ActionSnapshot, Node: "a"}}
	ineligiblePlan.Convergence.WorkloadEndNS = 1
	planDigest, _ := DigestPlan(ineligiblePlan)
	capabilitiesDigest, _ := DigestCapabilities(capabilities)
	eligibility, err := Preflight(ineligiblePlan, capabilities, capabilities)
	if err != nil || eligibility.Eligible {
		t.Fatalf("Preflight = %#v, %v", eligibility, err)
	}
	ineligible := SemanticExecution{
		Schema: SemanticExecutionSchema, PlanSHA256: planDigest,
		CapabilitiesSHA256: capabilitiesDigest, Adapter: AdapterID(capabilities.Adapter),
		Reproducibility: artifact.NewReproducibility(uint64(ineligiblePlan.FallbackSeed)),
		Eligibility:     EligibilityIneligible, Rejections: eligibility.Rejections,
		Projection: ProjectionReport{
			Fidelity: ProjectionFailed, Directives: len(ineligiblePlan.Directives),
			Additional: []PortableKey{}, Unmatched: cloneDirectives(ineligiblePlan.Directives),
		},
		Decisions:       decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{}},
		NodeCommitments: []NodeCommitment{},
	}
	if err := ineligible.Validate(); err != nil {
		t.Fatal(err)
	}
	normalized, err := NormalizeExecution(ineligiblePlan, capabilities, capabilities, ineligible)
	if err != nil || normalized.Eligibility != EligibilityIneligible || normalized.Completion != CompletionNotExecuted {
		t.Fatalf("NormalizeExecution = %#v, %v", normalized, err)
	}

	valid := validSemanticExecution(t, plan, capabilities)
	valid.PlanSHA256 = digestOf('f')
	if _, err := NormalizeExecution(plan, capabilities, capabilities, valid); err == nil {
		t.Fatal("plan hash mismatch accepted")
	}
	valid = validSemanticExecution(t, plan, capabilities)
	valid.CapabilitiesSHA256 = digestOf('e')
	if _, err := NormalizeExecution(plan, capabilities, capabilities, valid); err == nil {
		t.Fatal("capability hash mismatch accepted")
	}
	valid = validSemanticExecution(t, plan, capabilities)
	valid.Outcome.EndNS = plan.Convergence.ComparisonBoundaryNS + 1
	if _, err := NormalizeExecution(plan, capabilities, capabilities, valid); err == nil {
		t.Fatal("outcome beyond comparison boundary accepted")
	}
	valid = validSemanticExecution(t, plan, capabilities)
	valid.Reproducibility.DecisionSeed++
	if _, err := NormalizeExecution(plan, capabilities, capabilities, valid); err == nil {
		t.Fatal("mismatched fallback seed provenance accepted")
	}
	valid = validSemanticExecution(t, plan, capabilities)
	valid.Outcome.Status = artifact.OutcomeBudgetExhausted
	valid.Outcome.EndNS = plan.Convergence.ComparisonBoundaryNS - 1
	if _, err := NormalizeExecution(plan, capabilities, capabilities, valid); err == nil {
		t.Fatal("premature budget exhaustion accepted")
	}
}

func TestNormalizeExecutionDerivesNegotiatedInvariantUniverse(t *testing.T) {
	plan := testPlan(t)
	plan.Directives = []Directive{}
	capabilities := testCapabilities(plan.Source.Adapter)
	capabilities.InvariantIDs = []string{"raft/election-safety", "raft/log-matching"}
	peer := testCapabilities(artifact.Adapter{ID: "peer", Version: "1"})
	peer.InvariantIDs = []string{"raft/log-matching", "raft/snapshot-conflict"}
	execution := validSemanticExecution(t, plan, capabilities)

	normalized, err := NormalizeExecution(plan, capabilities, peer, execution)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(normalized.NegotiatedCommonIDs, []string{"raft/log-matching"}) {
		t.Fatalf("negotiated universe = %v", normalized.NegotiatedCommonIDs)
	}
}

func TestVerifyExecutionProjectionRejectsForgedAccountingAndTape(t *testing.T) {
	plan := testPlan(t)
	capabilities := testCapabilities(plan.Source.Adapter)
	capabilities.ProjectionKinds = allProjectionKinds()
	choiceA := choiceFor(decision.ElectionTimeout, `{"node":"a","incarnation":1,"generation":1}`, 1, 9)
	choiceB := choiceFor(decision.ElectionTimeout, `{"node":"b","incarnation":1,"generation":1}`, 1, 9)
	keyA, err := PortableKeyForChoice(choiceA)
	if err != nil {
		t.Fatal(err)
	}
	plan.Directives = []Directive{{Key: keyA, SourceIndex: 0, Selection: numberSelection(5)}}

	valid := validSemanticExecution(t, plan, capabilities)
	valid.Decisions.Entries = []decision.Entry{mustEntry(t, choiceA, numberSelection(5))}
	valid.Projection = ProjectionReport{
		Fidelity: ProjectionExact, Directives: 1, Projected: 1,
		Additional: []PortableKey{}, Unmatched: []Directive{},
	}
	if err := VerifyExecutionProjection(plan, valid); err != nil {
		t.Fatalf("valid projection rejected: %v", err)
	}

	tests := map[string]func(*SemanticExecution, *Plan){
		"variable reported fixed": func(execution *SemanticExecution, plan *Plan) {
			plan.Directives = []Directive{}
			execution.Projection = ProjectionReport{
				Fidelity: ProjectionExact, Fixed: 1,
				Additional: []PortableKey{}, Unmatched: []Directive{},
			}
		},
		"swapped portable key": func(execution *SemanticExecution, _ *Plan) {
			execution.Decisions.Entries = []decision.Entry{mustEntry(t, choiceB, numberSelection(5))}
		},
		"mismatched directive selection": func(execution *SemanticExecution, _ *Plan) {
			execution.Decisions.Entries = []decision.Entry{mustEntry(t, choiceA, numberSelection(6))}
		},
		"unsupported choice kind": func(execution *SemanticExecution, plan *Plan) {
			plan.Directives = []Directive{}
			unsupported := choiceFor(decision.FaultAction, `{}`, 1, 9)
			execution.Decisions.Entries = []decision.Entry{mustEntry(t, unsupported, numberSelection(5))}
			execution.Projection = ProjectionReport{
				Fidelity: ProjectionExact, Fixed: 1,
				Additional: []PortableKey{}, Unmatched: []Directive{},
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidatePlan := plan
			candidate := valid
			candidate.Decisions = decision.CloneTape(valid.Decisions)
			candidate.Projection = cloneProjectionReport(valid.Projection)
			mutate(&candidate, &candidatePlan)
			planDigest, digestErr := DigestPlan(candidatePlan)
			if digestErr != nil {
				t.Fatal(digestErr)
			}
			candidate.PlanSHA256 = planDigest
			if err := candidate.Validate(); err != nil {
				t.Fatalf("forgery should be structurally valid for this test: %v", err)
			}
			if err := VerifyExecutionProjection(candidatePlan, candidate); err == nil {
				t.Fatal("projection forgery accepted")
			}
		})
	}
}

func validSemanticExecution(t *testing.T, plan Plan, capabilities Capabilities) SemanticExecution {
	t.Helper()
	planDigest, err := DigestPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	capabilitiesDigest, err := DigestCapabilities(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	commitment := apporacle.New().Commitment()
	return SemanticExecution{
		Schema: SemanticExecutionSchema, PlanSHA256: planDigest,
		CapabilitiesSHA256: capabilitiesDigest, Adapter: AdapterID(capabilities.Adapter),
		Reproducibility: artifact.NewReproducibility(uint64(plan.FallbackSeed)),
		Eligibility:     EligibilityEligible, Rejections: []RejectionCode{},
		Projection: ProjectionReport{Fidelity: ProjectionExact, Additional: []PortableKey{}, Unmatched: []Directive{}},
		Decisions:  decision.Tape{Schema: decision.SchemaVersion, Entries: []decision.Entry{}},
		Outcome: &artifact.Outcome{
			Status: artifact.OutcomeCompleted, EndNS: plan.Convergence.ComparisonBoundaryNS,
			ObservationDigest: digestOf('b'),
		},
		NodeCommitments: []NodeCommitment{
			{Node: "a", Commitment: commitment},
			{Node: "b", Commitment: commitment},
			{Node: "c", Commitment: commitment},
		},
	}
}

func allProjectionKinds() []decision.Kind {
	return []decision.Kind{
		decision.ElectionTimeout, decision.NetworkLatency,
		decision.NetworkLoss, decision.StorageLatency,
	}
}
