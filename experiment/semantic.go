package experiment

import (
	"errors"
	"fmt"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/raftsim"
	"github.com/aminkbi/d-raft/semanticplan"
)

var ErrSemanticIneligible = errors.New("experiment: semantic plan is ineligible for the reference adapter")

// ReferenceSemanticCapabilities declares the portable v1 surface implemented
// by the pure reference model. The sets are canonical and safe for hashing.
func ReferenceSemanticCapabilities() semanticplan.Capabilities {
	return semanticplan.Capabilities{
		Schema:  semanticplan.AdapterCapabilitiesSchema,
		Adapter: artifact.Adapter{ID: artifact.ReferenceAdapterID, Version: artifact.ReferenceAdapterCurrent},
		MembershipProfiles: []semanticplan.MembershipProfile{
			semanticplan.MembershipFixedAllVoters,
		},
		Actions:             semanticplan.V1Actions(),
		ApplicationProfiles: []string{apporacle.CommandSchema},
		ProjectionKinds:     semanticplan.V1ProjectionKinds(),
		InvariantIDs: []string{
			check.AppliedConflict, check.AppliedMonotonic, check.CommitMonotonic,
			check.CommittedConflict, check.DurableDoubleVote, check.DurableTermMonotonic,
			check.ElectionCertificate, check.ElectionSafety, check.LeaderCompleteness,
			check.LogMatching, check.MembershipTransition, check.SnapshotConflict,
			check.VolatileDurableMatch,
		},
	}
}

// ExecuteSemanticPlan projects one eligible portable plan onto the reference
// adapter and retains exact local replay evidence for successful projections,
// or the exact successful prefix on projection failure, plus normalized
// application commitments. Callers comparing two adapters should run the
// bilateral semanticplan.Preflight before invoking either executor.
func ExecuteSemanticPlan(plan semanticplan.Plan) (semanticplan.SemanticExecution, error) {
	capabilities := ReferenceSemanticCapabilities()
	eligibility, err := semanticplan.Preflight(plan, capabilities, capabilities)
	if err != nil {
		return semanticplan.SemanticExecution{}, err
	}
	if !eligibility.Eligible {
		return semanticplan.SemanticExecution{}, fmt.Errorf("%w: %v", ErrSemanticIneligible, eligibility.Rejections)
	}
	projector, err := semanticplan.NewProjector(plan.Directives, plan.FallbackSeed)
	if err != nil {
		return semanticplan.SemanticExecution{}, err
	}
	recorder := decision.NewRecorder(projector)
	reproducibility := artifact.NewReproducibility(uint64(plan.FallbackSeed))
	config := plan.Configuration.ClusterConfig(recorder, nil)
	application := plan.Application
	config.Application = &application
	cluster, err := raftsim.New(config)
	if err != nil {
		return semanticplan.NewEligibleExecution(
			plan, capabilities, reproducibility, projector.Finish(), recorder.Tape(), nil, err.Error(),
			[]semanticplan.NodeCommitment{},
		)
	}
	outcome, err := executeScheduledApplication(cluster, plan.Scenario)
	if err != nil {
		return semanticplan.SemanticExecution{}, err
	}
	commitments := make(map[raft.NodeID]apporacle.Commitment, len(cluster.Members()))
	for _, member := range cluster.Members() {
		commitment, commitmentErr := cluster.ApplicationCommitment(member)
		if commitmentErr != nil {
			return semanticplan.SemanticExecution{}, commitmentErr
		}
		commitments[member] = commitment
	}
	nodes, _, err := semanticplan.NormalizeNodeCommitments(commitments)
	if err != nil {
		return semanticplan.SemanticExecution{}, err
	}
	return semanticplan.NewEligibleExecution(
		plan, capabilities, reproducibility, projector.Finish(), recorder.Tape(), &outcome, "", nodes,
	)
}
