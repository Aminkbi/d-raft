package experiment

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/semanticplan"
)

func TestReferenceSemanticExecutionIsLocallyReplayable(t *testing.T) {
	plan := referenceSemanticTestPlan()
	capabilities := ReferenceSemanticCapabilities()
	if err := capabilities.Validate(); err != nil {
		t.Fatal(err)
	}
	execution, err := ExecuteSemanticPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Projection.Fidelity != semanticplan.ProjectionPartial || len(execution.Decisions.Entries) == 0 || execution.Outcome == nil {
		t.Fatalf("semantic execution = %#v", execution)
	}
	normalized, err := semanticplan.NormalizeExecution(plan, capabilities, capabilities, execution)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Completion != semanticplan.CompletionCompleted || !normalized.ApplicationNodesAgreeAtBoundary || len(normalized.NodeCommitments) != len(plan.Configuration.Members) {
		t.Fatalf("normalized outcome = %#v", normalized)
	}

	replay, err := decision.NewTapeDecider(execution.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := ExecuteWithApplication(plan.Scenario, plan.Configuration, replay, plan.Application)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(*execution.Outcome, replayed) {
		t.Fatalf("local exact replay changed outcome:\n got %#v\nwant %#v", replayed, *execution.Outcome)
	}
}

func TestReferenceSemanticExecutionRejectsIneligiblePlan(t *testing.T) {
	plan := referenceSemanticTestPlan()
	plan.Scenario.Actions = []artifact.Action{{
		AtNS: int64(100 * time.Millisecond), Kind: artifact.ActionSnapshot,
		Node: "a", Data: []byte("opaque"),
	}}
	plan.Convergence.WorkloadEndNS = int64(100 * time.Millisecond)
	if _, err := ExecuteSemanticPlan(plan); !errors.Is(err, ErrSemanticIneligible) {
		t.Fatalf("ExecuteSemanticPlan error = %v, want ErrSemanticIneligible", err)
	}
}

func referenceSemanticTestPlan() semanticplan.Plan {
	return semanticplan.Plan{
		Schema: semanticplan.SemanticPlanSchema,
		Scenario: artifact.Scenario{
			ID: "semantic/reference-steady", Version: "1",
			DurationNS: int64(time.Second), MaxSteps: 100_000,
			Actions: []artifact.Action{},
		},
		Configuration: artifact.Configuration{
			Members: []raft.NodeID{"a", "b", "c"}, InfrastructureSeed: 17,
			NetworkMinLatencyNS: int64(time.Millisecond), NetworkMaxLatencyNS: int64(5 * time.Millisecond),
			ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
			HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: int64(time.Millisecond),
			StopOnViolation: false,
		},
		Application: apporacle.KVConfig(),
		Convergence: semanticplan.Convergence{WorkloadEndNS: 0, ComparisonBoundaryNS: int64(time.Second)},
		Source: semanticplan.Source{
			Adapter:   ReferenceSemanticCapabilities().Adapter,
			RunSHA256: strings.Repeat("a", 64),
		},
		FallbackSeed: 23,
		Directives:   []semanticplan.Directive{},
	}
}
