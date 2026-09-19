# Projection robustness development study v1

This deterministic synthetic experiment demonstrates that occurrence coverage
does not establish causal preservation. It uses the actual
`semanticplan.Projector` for the occurrence baseline. The target model is a
deliberately faulty two-replica toy, not the reference Raft implementation or
etcd/raft. These are designed development fixtures, not a sampled benchmark.

## Model and source counterexample

A primary acknowledges logical operation `x` while it is still volatile.
Network envelopes replicate logical operations to a durable backup. The
primary crashes after all listed envelopes; an acknowledged write is lost if
no delivered envelope carried `x`. The toy predicate is
`toy/acknowledged-write-lost(operation=x)`. Auxiliary `y` is not acknowledged;
`noise` is irrelevant to the target predicate. Replication is idempotent.
Every transformation preserves survival of `x` when all envelopes are delivered.

The source sends `x` at abstract time 10 and `y` at time 20. Its explicit
counterexample drops `x` and delivers `y`; it is constructed, not discovered.
All messages have sender `a`, receiver `b`, and sender incarnation 1. The
occurrence baseline uses source directives extracted from a valid decision
tape, with fallback seed 1. Actual selections and tapes are in the raw result.

Three policies see the same target envelopes:

- **Fault interval:** drop messages sent in `[10, 11)`; deliver the rest.
- **Occurrence v1:** project the source tape through the existing portable
  send-sequence keys; use its pinned fallback for additional target choices.
- **Causal operation prototype:** bind source dispositions to logical operation
  IDs. New operations are delivered. All attempts of the same operation get
  its disposition. A batch with conflicting dispositions is ineligible before
  any target execution. These fixture-supplied identities are ground truth;
  extracting trustworthy identities from production code remains future work.

The causal policy intentionally defines operation-level faults, including
retries; it is not equivalent to preserving one packet drop. Its retry result
must not be described as preserving the original packet-level fault budget.
The temporal baseline is also a different fault policy. The comparison asks
which policies retain this witness under each transformation, not which
policies implement identical interventions.

## Raw outcomes

“Preserved” means that the toy witness is reproduced. “Mismatch” counts
operation occurrences whose drop/deliver outcome differs from the source
logical policy (new operations should be delivered). Repeated operations count
per attempt. This metric requires fixture ground truth and is not currently
available from production projection reports.

| Target transformation | Fault interval | Occurrence v1 | Causal prototype |
| --- | --- | --- | --- |
| Unchanged control | preserved, 0 mismatches | exact; preserved, 0 mismatches | preserved, 0 mismatches |
| Insert noise before x | lost, 2 mismatches | partial; lost, 2 mismatches | preserved, 0 mismatches |
| Swap x and y | lost, 2 mismatches | **exact; lost, 2 mismatches** | preserved, 0 mismatches |
| Shift both sends by +1 | lost, 1 mismatch | exact; preserved, 0 mismatches | preserved, 0 mismatches |
| Merge x and y in one envelope | preserved, 1 mismatch | partial; preserved, 1 mismatch | ineligible: conflicting dispositions |
| Retry x before y | lost, 1 mismatch | partial; lost, 1 mismatch | preserved, 0 mismatches |

The reordered case is a counterexample to the implication “exact projection
coverage implies causal alignment”: both directives are consumed, neither is
unmatched, and there are no additional choices, yet the failure disappears.
The merged case also shows that reproducing a failure need not preserve all
source dispositions. Report coverage, causal alignment, and failure identity
as separate axes.

These results do not establish the causal prototype's superiority on real
defects, seed-only replay, general batching, timer changes, or upgrades. There
are no throughput or statistical effectiveness claims for six hand-designed
cases. The next production study follows [RESEARCH_PROTOCOL.md](RESEARCH_PROTOCOL.md).

## Relationship to the production-core corpus

The existing [portable-faults case](corpus/cross-adapter/v1/portable-faults-v1-seed-1/)
contains 2,163 source directives. Its etcd execution projects 1,412, leaves 751
unmatched, and introduces 792 target choices. Both adapters reach their
boundary with agreeing application commitments. Those counts establish partial
coverage, not a measured causal mismatch rate: that corpus has no independent
operation-to-network-event mapping. It also has no source violation to preserve.
The toy study supplies the missing ground truth for a controlled counterexample;
it does not retroactively classify the production corpus's matched messages.

## Reproduce and verify

Use the repository's declared Go 1.26.6 toolchain. From the root:

```sh
go run ./cmd/draft-projection-study > /tmp/projection-study.json
go run ./cmd/draft-projection-study --verify /tmp/projection-study.json
go run ./cmd/draft-projection-study --verify evaluation/results/projection-v1/result.json
go test ./internal/projectionstudy ./cmd/draft-projection-study
(cd evaluation/results/projection-v1 && sha256sum -c SHA256SUMS)
```

The raw artifact includes every target envelope, source tape, actual delivery,
target-local tape, projection accounting, durable state, and witness. Every
completed case exactly replays its target-local tape and recomputes the model
outcome. Ineligible cases contain no target decisions. The verifier regenerates
all 18 observations and compares canonical bytes; changed fields, duplicate or
unknown fields, trailing data, and changed formatting are rejected. This is
deterministic regeneration with the same model, not an independent proof of
model correctness. Tests separately cover fault-free controls, the exact
coverage counterexample, conflicting-batch preflight, and tampered artifacts.

The version fixes the inputs and methods; do not update published v1 bytes to
accommodate a changed experiment. Host-independent output contains no timing or
producer revision claim. Record the checkout commit and `go version` alongside
external replications; the committed source, result, and checksum bind this
development artifact. Existing production plan schemas and corpus bytes are
unchanged.
