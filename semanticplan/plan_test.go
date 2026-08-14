package semanticplan

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
)

func TestPlanStrictRoundTripAndCanonicalCounters(t *testing.T) {
	plan := testPlan(t)
	plan.FallbackSeed = artifact.Uint64(math.MaxUint64)
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"fallback_seed":"18446744073709551615"`,
		`"incarnation":"1"`, `"source_index":"0"`,
	} {
		if !bytes.Contains(raw, []byte(fragment)) {
			t.Fatalf("plan JSON lacks %s: %s", fragment, raw)
		}
	}
	decoded, err := DecodePlan(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("decoded plan differs:\n got: %#v\nwant: %#v", decoded, plan)
	}
	first, err := DigestPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DigestPlan(decoded)
	if err != nil || first != second || len(first) != 64 {
		t.Fatalf("DigestPlan = %q, %q, %v", first, second, err)
	}

	invalid := map[string][]byte{
		"unknown":      bytes.Replace(raw, []byte(`"schema":`), []byte(`"unknown":true,"schema":`), 1),
		"trailing":     append(slices.Clone(raw), []byte(` {}`)...),
		"numeric seed": bytes.Replace(raw, []byte(`"18446744073709551615"`), []byte(`18446744073709551615`), 1),
		"leading zero": bytes.Replace(raw, []byte(`"18446744073709551615"`), []byte(`"01"`), 1),
		"duplicate top-level": bytes.Replace(raw,
			[]byte(`"schema":"`+SemanticPlanSchema+`"`),
			[]byte(`"schema":"`+SemanticPlanSchema+`","schema":"`+SemanticPlanSchema+`"`), 1),
		"duplicate nested": bytes.Replace(raw,
			[]byte(`"source":{"adapter":{"id":"`+artifact.ReferenceAdapterID+`"`),
			[]byte(`"source":{"adapter":{"id":"shadow","id":"`+artifact.ReferenceAdapterID+`"`), 1),
	}
	for name, encoded := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, decodeErr := DecodePlan(bytes.NewReader(encoded)); !errors.Is(decodeErr, ErrInvalidPlan) {
				t.Fatalf("DecodePlan error = %v", decodeErr)
			}
		})
	}
}

func TestPlanValidateConvergenceAndDirectiveStructure(t *testing.T) {
	tests := map[string]func(*Plan){
		"wrong boundary": func(plan *Plan) { plan.Convergence.ComparisonBoundaryNS-- },
		"no quiet tail": func(plan *Plan) {
			plan.Convergence.WorkloadEndNS = plan.Convergence.ComparisonBoundaryNS
		},
		"action after workload": func(plan *Plan) { plan.Convergence.WorkloadEndNS = 499 },
		"null directives":       func(plan *Plan) { plan.Directives = nil },
		"unknown directive node": func(plan *Plan) {
			plan.Directives[0].Key.Node = "unknown"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			plan := testPlan(t)
			mutate(&plan)
			if err := plan.Validate(); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("Validate error = %v", err)
			}
		})
	}
}

func TestCapabilitiesStrictCanonicalDecode(t *testing.T) {
	capabilities := testCapabilities(artifact.Adapter{ID: "adapter", Version: "1"})
	raw, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCapabilities(bytes.NewReader(raw))
	if err != nil || !reflect.DeepEqual(decoded, capabilities) {
		t.Fatalf("DecodeCapabilities = %#v, %v", decoded, err)
	}

	unsorted := capabilities
	unsorted.Actions = []artifact.ActionKind{artifact.ActionRestart, artifact.ActionCrash}
	duplicate := capabilities
	duplicate.ApplicationProfiles = []string{apporacle.CommandSchema, apporacle.CommandSchema}
	nullSet := capabilities
	nullSet.InvariantIDs = nil
	for name, value := range map[string]Capabilities{"unsorted": unsorted, "duplicate": duplicate, "null": nullSet} {
		t.Run(name, func(t *testing.T) {
			encoded, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if _, decodeErr := DecodeCapabilities(bytes.NewReader(encoded)); !errors.Is(decodeErr, ErrInvalidCapabilities) {
				t.Fatalf("DecodeCapabilities error = %v", decodeErr)
			}
		})
	}
	unknown := bytes.Replace(raw, []byte(`"schema":`), []byte(`"unknown":true,"schema":`), 1)
	if _, err := DecodeCapabilities(bytes.NewReader(unknown)); !errors.Is(err, ErrInvalidCapabilities) {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := DecodeCapabilities(bytes.NewReader(append(raw, []byte(` {}`)...))); !errors.Is(err, ErrInvalidCapabilities) {
		t.Fatalf("trailing value error = %v", err)
	}
	for name, document := range map[string][]byte{
		"top level": bytes.Replace(raw,
			[]byte(`"schema":"`+AdapterCapabilitiesSchema+`"`),
			[]byte(`"schema":"`+AdapterCapabilitiesSchema+`","schema":"`+AdapterCapabilitiesSchema+`"`), 1),
		"nested": bytes.Replace(raw,
			[]byte(`"adapter":{"id":"adapter"`),
			[]byte(`"adapter":{"id":"shadow","id":"adapter"`), 1),
	} {
		t.Run("duplicate "+name, func(t *testing.T) {
			if _, err := DecodeCapabilities(bytes.NewReader(document)); !errors.Is(err, ErrInvalidCapabilities) {
				t.Fatalf("duplicate field error = %v", err)
			}
		})
	}
}

func TestPreflightEligibleAndRejectsBeforeProjection(t *testing.T) {
	plan := testPlan(t)
	left := testCapabilities(plan.Source.Adapter)
	right := testCapabilities(artifact.Adapter{ID: "target", Version: "2"})
	eligibility, err := Preflight(plan, left, right)
	if err != nil || !eligibility.Eligible || len(eligibility.Rejections) != 0 || eligibility.Rejections == nil {
		t.Fatalf("Preflight = %#v, %v", eligibility, err)
	}

	tests := []struct {
		name   string
		mutate func(*Plan, *Capabilities, *Capabilities)
		want   []RejectionCode
	}{
		{"membership", func(plan *Plan, _, _ *Capabilities) {
			plan.Configuration.Voters = []raft.NodeID{"a", "b"}
			plan.Configuration.Learners = []raft.NodeID{"c"}
		}, []RejectionCode{RejectFixedAllVoters}},
		{"invalid command", func(plan *Plan, _, _ *Capabilities) {
			plan.Scenario.Actions[0].Data = []byte("invalid")
		}, []RejectionCode{RejectInvalidCommand}},
		{"duplicate command", func(plan *Plan, _, _ *Capabilities) {
			duplicate := plan.Scenario.Actions[0]
			duplicate.AtNS++
			plan.Scenario.Actions = slices.Insert(plan.Scenario.Actions, 1, duplicate)
		}, []RejectionCode{RejectDuplicateCommand}},
		{"crash without restart", func(plan *Plan, _, _ *Capabilities) {
			plan.Scenario.Actions = slices.Delete(plan.Scenario.Actions, 2, 3)
		}, []RejectionCode{RejectUnbalancedLifecycle}},
		{"partition without heal", func(plan *Plan, _, _ *Capabilities) {
			plan.Scenario.Actions = slices.Delete(plan.Scenario.Actions, 4, 5)
		}, []RejectionCode{RejectUnhealedNetwork}},
		{"unsupported snapshot", func(plan *Plan, _, _ *Capabilities) {
			plan.Scenario.Actions[0] = artifact.Action{AtNS: 100, Kind: artifact.ActionSnapshot, Node: "a"}
		}, []RejectionCode{RejectLeftAction, RejectRightAction, RejectUnsupportedAction}},
		{"capability intersection", func(_ *Plan, left, right *Capabilities) {
			left.Adapter = artifact.Adapter{ID: "other", Version: "1"}
			left.MembershipProfiles, right.MembershipProfiles = []MembershipProfile{}, []MembershipProfile{}
			left.Actions, right.Actions = []artifact.ActionKind{}, []artifact.ActionKind{}
			left.ApplicationProfiles, right.ApplicationProfiles = []string{}, []string{}
			left.ProjectionKinds, right.ProjectionKinds = []decision.Kind{}, []decision.Kind{}
		}, []RejectionCode{
			RejectSourceAdapter, RejectLeftMembership, RejectRightMembership,
			RejectLeftAction, RejectRightAction, RejectLeftApplication,
			RejectRightApplication, RejectLeftProjectionKind, RejectRightProjectionKind,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := testPlan(t)
			left := testCapabilities(plan.Source.Adapter)
			right := testCapabilities(artifact.Adapter{ID: "target", Version: "2"})
			test.mutate(&plan, &left, &right)
			got, preflightErr := Preflight(plan, left, right)
			if preflightErr != nil {
				t.Fatal(preflightErr)
			}
			slices.Sort(test.want)
			if got.Eligible || !reflect.DeepEqual(got.Rejections, test.want) || !slices.IsSorted(got.Rejections) {
				t.Fatalf("Preflight = %#v, want %v", got, test.want)
			}
		})
	}
}

func TestPreflightMalformedInputReturnsError(t *testing.T) {
	plan := testPlan(t)
	left := testCapabilities(plan.Source.Adapter)
	right := testCapabilities(artifact.Adapter{ID: "target", Version: "2"})
	plan.Convergence.ComparisonBoundaryNS--
	eligibility, err := Preflight(plan, left, right)
	if !errors.Is(err, ErrInvalidPlan) || eligibility.Rejections != nil {
		t.Fatalf("Preflight = %#v, %v", eligibility, err)
	}
}

func TestPreflightAllowsPartitionReplacementBeforeFinalHeal(t *testing.T) {
	plan := testPlan(t)
	replacement := artifact.Action{
		AtNS: 450, Kind: artifact.ActionPartition,
		Groups: [][]raft.NodeID{{"a", "b"}, {"c"}},
	}
	plan.Scenario.Actions = slices.Insert(plan.Scenario.Actions, 4, replacement)
	left := testCapabilities(plan.Source.Adapter)
	right := testCapabilities(artifact.Adapter{ID: "target", Version: "2"})
	eligibility, err := Preflight(plan, left, right)
	if err != nil || !eligibility.Eligible {
		t.Fatalf("Preflight = %#v, %v", eligibility, err)
	}
}

func testPlan(t *testing.T) Plan {
	t.Helper()
	command, err := apporacle.EncodeCommand(apporacle.Command{
		ID: apporacle.CommandID{15: 1}, Operation: apporacle.Put,
		Key: []byte("key"), Value: []byte("value"),
	})
	if err != nil {
		t.Fatal(err)
	}
	selection := int64(15)
	return Plan{
		Schema: SemanticPlanSchema,
		Scenario: artifact.Scenario{
			ID: "portable/convergence", Version: "1", DurationNS: 1_000, MaxSteps: 10_000,
			Actions: []artifact.Action{
				{AtNS: 100, Kind: artifact.ActionPropose, Data: command},
				{AtNS: 200, Kind: artifact.ActionCrash, Node: "b"},
				{AtNS: 300, Kind: artifact.ActionRestart, Node: "b"},
				{AtNS: 400, Kind: artifact.ActionPartition, Groups: [][]raft.NodeID{{"a"}, {"b", "c"}}},
				{AtNS: 500, Kind: artifact.ActionHeal},
			},
		},
		Configuration: artifact.Configuration{
			Members: []raft.NodeID{"a", "b", "c"}, InfrastructureSeed: 7,
			NetworkMinLatencyNS: 1, NetworkMaxLatencyNS: 2,
			ElectionTimeoutMinNS: 20, ElectionTimeoutMaxNS: 30,
			HeartbeatIntervalNS: 5, StorageLatencyNS: 1,
		},
		Application: apporacle.KVConfig(),
		Convergence: Convergence{WorkloadEndNS: 500, ComparisonBoundaryNS: 1_000},
		Source: Source{
			Adapter:   artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
			RunSHA256: strings.Repeat("a", 64),
		},
		FallbackSeed: 9,
		Directives: []Directive{{
			Key:         PortableKey{Kind: decision.ElectionTimeout, Node: "a", Incarnation: 1, Occurrence: 1},
			SourceIndex: 0, Selection: decision.Selection{Number: &selection},
		}},
	}
}

func testCapabilities(adapter artifact.Adapter) Capabilities {
	return Capabilities{
		Schema: AdapterCapabilitiesSchema, Adapter: adapter,
		MembershipProfiles: []MembershipProfile{MembershipFixedAllVoters},
		Actions: []artifact.ActionKind{
			artifact.ActionCrash, artifact.ActionHeal, artifact.ActionPartition,
			artifact.ActionPropose, artifact.ActionRestart,
		},
		ApplicationProfiles: []string{apporacle.CommandSchema},
		ProjectionKinds: []decision.Kind{
			decision.ElectionTimeout, decision.NetworkLatency,
			decision.NetworkLoss, decision.StorageLatency,
		},
		InvariantIDs: []string{},
	}
}
