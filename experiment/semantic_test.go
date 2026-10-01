package experiment

import (
	"bytes"
	"encoding/json"
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

func TestReferenceCausalExecutionIncludesOperationIdentity(t *testing.T) {
	plan := referenceSemanticTestPlan()
	command, err := apporacle.EncodeCommand(apporacle.Command{ID: apporacle.CommandID{15: 1}, Operation: apporacle.Put, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.Actions = []artifact.Action{{AtNS: int64(400 * time.Millisecond), Kind: artifact.ActionPropose, Data: command}}
	plan.Convergence.WorkloadEndNS = int64(400 * time.Millisecond)
	_, source, err := ExecuteCausalSourcePlan(plan, 29)
	if err != nil {
		t.Fatal(err)
	}
	outcome, report, tape, err := ExecuteCausalPlan(plan, source)
	if err != nil || outcome.Status == artifact.OutcomeError || report.Schema != semanticplan.CausalReplaySchema {
		t.Fatalf("causal execution = %#v, %#v, %v", outcome, report, err)
	}
	found := false
	for _, entry := range tape.Entries {
		if entry.Choice.Kind != decision.NetworkLoss {
			continue
		}
		var context struct {
			OperationIDs []string `json:"operation_ids"`
		}
		if err := json.Unmarshal(entry.Choice.Context, &context); err != nil {
			t.Fatal(err)
		}
		if len(context.OperationIDs) > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("causal tape omitted operation identity")
	}
	replay, err := decision.NewCausalTapeDecider(tape)
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
	if !artifact.OutcomesEqual(outcome, replayed) {
		t.Fatalf("causal local replay changed outcome: got %#v want %#v", replayed, outcome)
	}
}

func TestReferenceCausalSourceFeedsCausalProjection(t *testing.T) {
	plan := referenceSemanticTestPlan()
	command, err := apporacle.EncodeCommand(apporacle.Command{ID: apporacle.CommandID{15: 1}, Operation: apporacle.Put, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.Actions = []artifact.Action{{AtNS: int64(400 * time.Millisecond), Kind: artifact.ActionPropose, Data: command}}
	plan.Convergence.WorkloadEndNS = int64(400 * time.Millisecond)
	sourceOutcome, sourceTape, err := ExecuteCausalSourcePlan(plan, 29)
	if err != nil || sourceOutcome.Status == artifact.OutcomeError {
		t.Fatalf("causal source = %#v, %v", sourceOutcome, err)
	}
	directives, err := semanticplan.CausalDirectivesFromTape(sourceTape)
	if err != nil {
		t.Fatal(err)
	}
	if len(directives) == 0 {
		t.Fatal("causal source produced no operation directives")
	}
	outcome, report, targetTape, err := ExecuteCausalPlan(plan, sourceTape)
	if err != nil || outcome.Status == artifact.OutcomeError {
		t.Fatalf("causal target = %#v, %#v, %v", outcome, report, err)
	}
	if report.Directives != len(directives) || report.Projected == 0 || report.Fidelity == semanticplan.ProjectionFailed {
		t.Fatalf("causal report = %#v, directives=%#v", report, directives)
	}
	replay, err := decision.NewCausalTapeDecider(targetTape)
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
	if !artifact.OutcomesEqual(outcome, replayed) {
		t.Fatalf("causal projected replay changed outcome: got %#v want %#v", replayed, outcome)
	}
	planDigest, err := semanticplan.DigestPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	evidence := semanticplan.CausalReplayEvidence{
		Schema: semanticplan.CausalEvidenceSchema, PlanSHA256: planDigest,
		SourceAdapter: ReferenceSemanticCapabilities().Adapter, TargetAdapter: ReferenceSemanticCapabilities().Adapter,
		FallbackSeed: plan.FallbackSeed,
		SourceTape:   sourceTape, TargetTape: targetTape,
		SourceOutcome: sourceOutcome, TargetOutcome: outcome, Report: report,
	}
	if err := evidence.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := semanticplan.DecodeCausalReplayEvidence(bytes.NewReader(raw))
	if err != nil || decoded.Schema != semanticplan.CausalEvidenceSchema {
		t.Fatalf("decoded causal evidence = %#v, %v", decoded, err)
	}
	if _, err := semanticplan.DigestCausalReplayEvidence(decoded); err != nil {
		t.Fatal(err)
	}
	if _, err := semanticplan.DecodeCausalReplayEvidence(bytes.NewReader(append(raw, []byte(`{}`)...))); !errors.Is(err, semanticplan.ErrInvalidCausal) {
		t.Fatalf("trailing causal evidence error = %v", err)
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
