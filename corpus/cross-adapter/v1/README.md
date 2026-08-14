# Cross-adapter corpus v1

This immutable corpus contains public `d-raft.semantic-plan/v1` experiments
executed by both the pure reference model and the unmodified
`go.etcd.io/raft/v3` core adapter. Each case retains its exact reference source
run, portable plan, two capability declarations, two build-provenanced semantic
executions, two normalized outcomes, outcome-bound comparison, and the
manifest that commits the seven derived documents.

The corpus is comparative evidence, not a claim of protocol equivalence. An
`exact` projection means the target consumed the complete portable directive
set without extra variable choices. A `partial` projection remains a different
choice stream and must be reported as such. Agreement covers only the
negotiated common invariant IDs and the bounded portable application
commitments at the declared boundary.

## Published cases

| Case | Source condition | Reference | etcd/raft | Boundary | Safety | Application |
| --- | --- | --- | --- | --- | --- | --- |
| [`loss-2pct-seed-20260814`](loss-2pct-seed-20260814/) | 3 nodes, 2 s, 2% loss, no workload actions | exact | partial; all 360 directives projected plus 10 target choices | both reached | agree | agree on the empty history/state |
| [`portable-faults-v1-seed-1`](portable-faults-v1-seed-1/) | 3 nodes, 5 s, 2% loss, 4 puts, partition/heal, crash/restart | exact | partial; 1,412/2,163 directives projected, 751 unmatched, 792 target choices | both reached | agree | all nodes agree on the same 4-command history/state |

The first case is deliberately a transport/projection control. Its application
agreement is empty-history evidence and must not be presented as workload or
linearizability evidence. The second exercises a portable application across
scheduled network and process faults, but its partial etcd/raft projection is
not exact cross-adapter replay. Fault-bearing seeded counterexamples remain in
the separate [`corpus/mutants/v1`](../../mutants/v1/) corpus, where safety and
conformance classifications are reported independently.

Every case is generated from a clean, explicit binary build. Run the case's
documented `draft-cross verify` command before analysis; the verifier checks
source replay, exact-byte hashes, capabilities, projection evidence, successful
local replay or failed-prefix semantics, deterministic re-execution,
normalization, and the final comparison.
