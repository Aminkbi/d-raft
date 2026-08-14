package etcdraft

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	rootraft "github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/semanticplan"
)

func TestPortableSemanticPlanExecutesAndComparesAcrossAdapters(t *testing.T) {
	plan := crossSemanticTestPlan(t)
	leftCapabilities := experiment.ReferenceSemanticCapabilities()
	rightCapabilities := SemanticCapabilities()
	eligibility, err := semanticplan.Preflight(plan, leftCapabilities, rightCapabilities)
	if err != nil || !eligibility.Eligible {
		t.Fatalf("Preflight = %#v, %v", eligibility, err)
	}

	leftExecution, err := experiment.ExecuteSemanticPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	rightExecution, err := ExecuteSemanticPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if leftExecution.Projection.Fidelity != semanticplan.ProjectionExact {
		t.Fatalf("source projection = %s, want exact", leftExecution.Projection.Fidelity)
	}
	common, err := semanticplan.NegotiateInvariantIDs(leftCapabilities, rightCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	left, err := semanticplan.NormalizeExecution(plan, leftCapabilities, rightCapabilities, leftExecution)
	if err != nil {
		t.Fatal(err)
	}
	right, err := semanticplan.NormalizeExecution(plan, rightCapabilities, leftCapabilities, rightExecution)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := semanticplan.CompareNormalized(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.ExecutionBoundary != semanticplan.ExecutionBothReached || comparison.Safety != semanticplan.AgreementAgree || comparison.Application != semanticplan.ApplicationAgree {
		t.Fatalf("comparison = %#v", comparison)
	}
	for _, outcome := range []semanticplan.NormalizedOutcome{left, right} {
		if !outcome.ApplicationNodesAgreeAtBoundary || !slices.Equal(outcome.NegotiatedCommonIDs, common) || len(outcome.NodeCommitments) != 3 || outcome.NodeCommitments[0].Commitment.Commands != 1 {
			t.Fatalf("portable commitments = %#v", outcome.NodeCommitments)
		}
	}

	replay, err := decision.NewTapeDecider(rightExecution.Decisions)
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
	if rightExecution.Outcome == nil || !artifact.OutcomesEqual(*rightExecution.Outcome, replayed) {
		t.Fatalf("etcd target-local replay changed outcome:\n got %#v\nwant %#v", replayed, rightExecution.Outcome)
	}
}

func TestSemanticCapabilitiesAreCanonicalAndNarrow(t *testing.T) {
	capabilities := SemanticCapabilities()
	if err := capabilities.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, unsupported := range []artifact.ActionKind{
		artifact.ActionCrashAfterNextPersist, artifact.ActionSnapshot,
		artifact.ActionBeginMembership, artifact.ActionFinalizeMembership,
	} {
		for _, supported := range capabilities.Actions {
			if supported == unsupported {
				t.Fatalf("semantic capabilities advertise unsupported action %q", unsupported)
			}
		}
	}
}

func TestPortableSemanticPlanDeterministicMatrix(t *testing.T) {
	tests := []struct {
		name         string
		id           string
		configure    func(*artifact.Configuration)
		actions      []artifact.Action
		duration     time.Duration
		wantCommands uint64
		wantPartial  bool
	}{
		{
			name: "nonzero packet loss", id: "loss",
			configure: func(configuration *artifact.Configuration) {
				configuration.NetworkLossProbability = 0.02
			},
			actions: []artifact.Action{
				portablePutAction(t, 700*time.Millisecond, 1),
				portablePutAction(t, time.Second, 2),
			},
			duration: 4 * time.Second, wantCommands: 2,
		},
		{
			name: "crash and restart", id: "crash-restart",
			actions: []artifact.Action{
				portablePutAction(t, 700*time.Millisecond, 1),
				{AtNS: int64(900 * time.Millisecond), Kind: artifact.ActionCrash, Node: "c"},
				portablePutAction(t, 1100*time.Millisecond, 2),
				{AtNS: int64(1400 * time.Millisecond), Kind: artifact.ActionRestart, Node: "c"},
				portablePutAction(t, 1700*time.Millisecond, 3),
			},
			duration: 4 * time.Second, wantCommands: 3,
		},
		{
			name: "partition and heal", id: "partition-heal",
			actions: []artifact.Action{
				portablePutAction(t, 500*time.Millisecond, 1),
				{AtNS: int64(700 * time.Millisecond), Kind: artifact.ActionPartition, Groups: [][]rootraft.NodeID{{"a", "b"}, {"c"}}},
				{AtNS: int64(1500 * time.Millisecond), Kind: artifact.ActionHeal},
			},
			duration: 4 * time.Second, wantCommands: 1,
		},
		{
			name: "multiple portable commands and partial target projection", id: "multiple-commands",
			actions: []artifact.Action{
				portablePutAction(t, 600*time.Millisecond, 1),
				portablePutAction(t, 800*time.Millisecond, 2),
				portablePutAction(t, time.Second, 3),
				portablePutAction(t, 1200*time.Millisecond, 4),
			},
			duration: 3 * time.Second, wantCommands: 4, wantPartial: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := crossSemanticTestConfiguration()
			if test.configure != nil {
				test.configure(&configuration)
			}
			scenario := artifact.Scenario{
				ID: "semantic/matrix-" + test.id, Version: "1",
				DurationNS: int64(test.duration), MaxSteps: 500_000, Actions: test.actions,
			}
			plan := semanticPlanFromReferenceRun(t, scenario, configuration, 71, 73)
			left, right, comparison := executeAndNormalizeSemanticPair(t, plan)

			if left.Projection != semanticplan.ProjectionExact {
				t.Fatalf("source projection = %s, want exact", left.Projection)
			}
			if test.wantPartial && right.Projection != semanticplan.ProjectionPartial {
				t.Fatalf("target projection = %s, want partial", right.Projection)
			}
			if comparison.ExecutionBoundary != semanticplan.ExecutionBothReached {
				t.Fatalf("execution boundary = %s (left completion=%s projection=%s, right completion=%s projection=%s)",
					comparison.ExecutionBoundary, left.Completion, left.Projection, right.Completion, right.Projection)
			}
			if comparison.Safety != semanticplan.AgreementAgree {
				t.Fatalf("safety comparison = %s", comparison.Safety)
			}
			if comparison.Application != semanticplan.ApplicationAgree {
				t.Fatalf("application comparison = %s", comparison.Application)
			}
			for _, outcome := range []semanticplan.NormalizedOutcome{left, right} {
				if !outcome.ApplicationNodesAgreeAtBoundary || len(outcome.NodeCommitments) != len(configuration.Members) {
					t.Fatalf("application commitments did not agree at boundary: %#v", outcome.NodeCommitments)
				}
				if got := uint64(outcome.NodeCommitments[0].Commitment.Commands); got != test.wantCommands {
					t.Fatalf("committed commands = %d, want %d", got, test.wantCommands)
				}
			}
		})
	}
}

func TestSemanticBoundaryCommitmentAgreementIsNotQuiescenceProof(t *testing.T) {
	configuration := crossSemanticTestConfiguration()
	const duration = 2 * time.Second
	scenario := artifact.Scenario{
		ID: "semantic/boundary-is-not-quiescence", Version: "1",
		DurationNS: int64(duration), MaxSteps: 300_000,
		Actions: []artifact.Action{portablePutAction(t, duration-time.Nanosecond, 1)},
	}
	plan := semanticPlanFromReferenceRun(t, scenario, configuration, 79, 83)
	left, right, comparison := executeAndNormalizeSemanticPair(t, plan)
	if comparison.ExecutionBoundary != semanticplan.ExecutionBothReached || comparison.Application != semanticplan.ApplicationAgree {
		t.Fatalf("comparison = %#v", comparison)
	}
	for _, outcome := range []semanticplan.NormalizedOutcome{left, right} {
		if !outcome.ApplicationNodesAgreeAtBoundary {
			t.Fatalf("equal boundary commitments were not recorded: %#v", outcome.NodeCommitments)
		}
		for _, node := range outcome.NodeCommitments {
			if node.Commitment.Commands != 0 {
				t.Fatalf("late proposal unexpectedly committed at boundary: %#v", outcome.NodeCommitments)
			}
		}
	}
	// The plan contains a valid command, but the equality surface is still the
	// empty application history because the boundary arrives before persistence
	// and replication can finish. This is equality-at-the-boundary evidence, not
	// a protocol-quiescence or future-stability proof.
}

func executeAndNormalizeSemanticPair(t *testing.T, plan semanticplan.Plan) (semanticplan.NormalizedOutcome, semanticplan.NormalizedOutcome, semanticplan.NormalizedComparison) {
	t.Helper()
	leftCapabilities := experiment.ReferenceSemanticCapabilities()
	rightCapabilities := SemanticCapabilities()
	eligibility, err := semanticplan.Preflight(plan, leftCapabilities, rightCapabilities)
	if err != nil || !eligibility.Eligible {
		t.Fatalf("Preflight = %#v, %v", eligibility, err)
	}
	leftExecution, err := experiment.ExecuteSemanticPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	rightExecution, err := ExecuteSemanticPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	assertSemanticLocalReplay(t, plan, leftExecution, true)
	assertSemanticLocalReplay(t, plan, rightExecution, false)
	common, err := semanticplan.NegotiateInvariantIDs(leftCapabilities, rightCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	left, err := semanticplan.NormalizeExecution(plan, leftCapabilities, rightCapabilities, leftExecution)
	if err != nil {
		t.Fatal(err)
	}
	right, err := semanticplan.NormalizeExecution(plan, rightCapabilities, leftCapabilities, rightExecution)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(left.NegotiatedCommonIDs, common) || !slices.Equal(right.NegotiatedCommonIDs, common) {
		t.Fatalf("negotiated invariant IDs differ: want=%v reference=%v etcdraft=%v", common, left.NegotiatedCommonIDs, right.NegotiatedCommonIDs)
	}
	comparison, err := semanticplan.CompareNormalized(left, right)
	if err != nil {
		t.Fatal(err)
	}
	return left, right, comparison
}

func assertSemanticLocalReplay(t *testing.T, plan semanticplan.Plan, execution semanticplan.SemanticExecution, reference bool) {
	t.Helper()
	if execution.Outcome == nil {
		t.Fatal("semantic execution has no raw outcome")
	}
	replay, err := decision.NewTapeDecider(execution.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	var replayed artifact.Outcome
	if reference {
		replayed, err = experiment.ExecuteWithApplication(plan.Scenario, plan.Configuration, replay, plan.Application)
	} else {
		replayed, err = ExecuteWithApplication(plan.Scenario, plan.Configuration, replay, plan.Application)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Finish(); err != nil {
		t.Fatal(err)
	}
	if !artifact.OutcomesEqual(*execution.Outcome, replayed) {
		t.Fatalf("target-local replay changed outcome:\n got %#v\nwant %#v", replayed, *execution.Outcome)
	}
}

func semanticPlanFromReferenceRun(t *testing.T, scenario artifact.Scenario, configuration artifact.Configuration, sourceSeed, fallbackSeed uint64) semanticplan.Plan {
	t.Helper()
	application := apporacle.KVConfig()
	recorder := decision.NewRecorder(decision.NewSeedDecider(sourceSeed))
	sourceOutcome, err := experiment.ExecuteWithApplication(scenario, configuration, recorder, application)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Err(); err != nil {
		t.Fatal(err)
	}
	directives, err := semanticplan.DirectivesFromTape(recorder.Tape())
	if err != nil {
		t.Fatal(err)
	}
	sourceDigest, err := artifact.DigestJSON(struct {
		Scenario      artifact.Scenario      `json:"scenario"`
		Configuration artifact.Configuration `json:"configuration"`
		Decisions     decision.Tape          `json:"decisions"`
		Outcome       artifact.Outcome       `json:"outcome"`
	}{Scenario: scenario, Configuration: configuration, Decisions: recorder.Tape(), Outcome: sourceOutcome})
	if err != nil {
		t.Fatal(err)
	}
	workloadEnd := int64(0)
	for _, action := range scenario.Actions {
		workloadEnd = action.AtNS
	}
	return semanticplan.Plan{
		Schema: semanticplan.SemanticPlanSchema, Scenario: scenario,
		Configuration: configuration, Application: application,
		Convergence:  semanticplan.Convergence{WorkloadEndNS: workloadEnd, ComparisonBoundaryNS: scenario.DurationNS},
		Source:       semanticplan.Source{Adapter: experiment.ReferenceSemanticCapabilities().Adapter, RunSHA256: sourceDigest},
		FallbackSeed: artifact.Uint64(fallbackSeed), Directives: directives,
	}
}

func crossSemanticTestConfiguration() artifact.Configuration {
	return artifact.Configuration{
		Members: []rootraft.NodeID{"a", "b", "c"}, InfrastructureSeed: 19,
		NetworkMinLatencyNS: int64(time.Millisecond), NetworkMaxLatencyNS: int64(5 * time.Millisecond),
		ElectionTimeoutMinNS: int64(100 * time.Millisecond), ElectionTimeoutMaxNS: int64(200 * time.Millisecond),
		HeartbeatIntervalNS: int64(20 * time.Millisecond), StorageLatencyNS: int64(time.Millisecond),
		StopOnViolation: false,
	}
}

func portablePutAction(t *testing.T, at time.Duration, ordinal byte) artifact.Action {
	t.Helper()
	command, err := apporacle.EncodeCommand(apporacle.Command{
		ID: apporacle.CommandID{15: ordinal}, Operation: apporacle.Put,
		Key: []byte(fmt.Sprintf("key-%d", ordinal)), Value: []byte(fmt.Sprintf("value-%d", ordinal)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Action{AtNS: int64(at), Kind: artifact.ActionPropose, Data: command}
}

func crossSemanticTestPlan(t *testing.T) semanticplan.Plan {
	t.Helper()
	command, err := apporacle.EncodeCommand(apporacle.Command{
		ID: apporacle.CommandID{15: 1}, Operation: apporacle.Put,
		Key: []byte("portable"), Value: []byte("agreement"),
	})
	if err != nil {
		t.Fatal(err)
	}
	scenario := artifact.Scenario{
		ID: "semantic/cross-adapter", Version: "1",
		DurationNS: int64(2 * time.Second), MaxSteps: 200_000,
		Actions: []artifact.Action{{AtNS: int64(500 * time.Millisecond), Kind: artifact.ActionPropose, Data: command}},
	}
	return semanticPlanFromReferenceRun(t, scenario, crossSemanticTestConfiguration(), 31, 37)
}
