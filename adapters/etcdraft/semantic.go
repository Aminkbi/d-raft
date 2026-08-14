package etcdraft

import (
	"errors"
	"fmt"

	"github.com/aminkbi/d-raft/apporacle"
	"github.com/aminkbi/d-raft/artifact"
	"github.com/aminkbi/d-raft/check"
	"github.com/aminkbi/d-raft/decision"
	"github.com/aminkbi/d-raft/experiment"
	rootraft "github.com/aminkbi/d-raft/raft"
	"github.com/aminkbi/d-raft/semanticplan"
)

var ErrSemanticIneligible = errors.New("etcdraft: semantic plan is ineligible")

// SemanticCapabilities declares the portable v1 surface implemented by this
// production-core adapter. InvariantIDs deliberately excludes properties for
// which public RawNode state does not provide the necessary evidence.
func SemanticCapabilities() semanticplan.Capabilities {
	return semanticplan.Capabilities{
		Schema:  semanticplan.AdapterCapabilitiesSchema,
		Adapter: artifact.Adapter{ID: AdapterID, Version: AdapterVersion},
		MembershipProfiles: []semanticplan.MembershipProfile{
			semanticplan.MembershipFixedAllVoters,
		},
		Actions:             semanticplan.V1Actions(),
		ApplicationProfiles: []string{apporacle.CommandSchema},
		ProjectionKinds:     semanticplan.V1ProjectionKinds(),
		InvariantIDs: []string{
			check.AppliedConflict, check.AppliedMonotonic, check.CommitMonotonic,
			check.CommittedConflict, check.DurableDoubleVote, check.DurableTermMonotonic,
			check.LogMatching, check.MembershipTransition, check.SnapshotConflict,
		},
	}
}

// ExecuteSemanticPlan projects one bilaterally eligible reference-sourced v1
// plan onto etcd/raft. Successful projections record an exact local decision
// tape; failed projections retain the exact successful prefix.
func ExecuteSemanticPlan(plan semanticplan.Plan) (semanticplan.SemanticExecution, error) {
	capabilities := SemanticCapabilities()
	eligibility, err := semanticplan.Preflight(plan, experiment.ReferenceSemanticCapabilities(), capabilities)
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
	reproducibility.CheckerSchema = CheckerProfile
	reproducibility.MessageCodec = MessageCodecVersion
	reproducibility.ObservationSchema = ObservationSchemaVersion
	config, err := ConfigurationFrom(plan.Configuration, recorder)
	if err != nil {
		return semanticplan.SemanticExecution{}, err
	}
	application := plan.Application
	config.Application = &application
	cluster, err := New(config)
	if err != nil {
		return semanticplan.NewEligibleExecution(
			plan, capabilities, reproducibility, projector.Finish(), recorder.Tape(), nil, err.Error(),
			[]semanticplan.NodeCommitment{},
		)
	}
	outcome, err := executeScheduled(cluster, plan.Scenario)
	if err != nil {
		return semanticplan.SemanticExecution{}, err
	}
	commitments := make(map[rootraft.NodeID]apporacle.Commitment, len(cluster.Members()))
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
