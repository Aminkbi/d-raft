# Portable semantic plans

d-raft semantic plans are bounded, adapter-neutral experiment descriptions.
They do not claim that one implementation's exact decision tape can be replayed
by another implementation. Instead, a plan projects choices through portable
identities and records where the target execution diverges from that projection.

## Versioned artifacts

| Artifact | Schema | Purpose |
| --- | --- | --- |
| Semantic plan | `d-raft.semantic-plan/v1` | Workload, convergence boundary, source provenance, portable directives, and fallback seed |
| Adapter capabilities | `d-raft.adapter-capabilities/v1` | Canonical declaration of supported workload, application, projection, membership, and invariant profiles |
| Semantic execution | `d-raft.semantic-execution/v1` | Build provenance, adapter-local tape evidence, projection accounting, raw outcome, and portable application commitments |
| Normalized outcome | `d-raft.normalized-outcome/v1` | Adapter-neutral execution, safety, and application evidence |
| Normalized comparison | `d-raft.normalized-comparison/v1` | Pairwise eligibility, projection, completion, common-invariant, and application axes |
| Cross bundle manifest | `d-raft.cross-bundle/v1` | Exact-byte hashes and sizes for one committed seven-document result set |

All documents are strict and bounded. Unknown or duplicate object fields,
trailing JSON values, non-canonical sets, duplicate semantic identities,
unsupported schema versions, and inconsistent derived fields fail closed.
Full-width unsigned identities are encoded as canonical decimal JSON strings.

The plan's `source.run_sha256` is SHA-256 over the exact source run artifact
bytes. A cross execution verifies that hash, source adapter, complete scenario,
configuration, and directives extracted from the exact source decision tape
before bilateral preflight. This distinguishes source provenance from the
target-local execution hashes.

## Eligibility boundary

The first schema deliberately supports the common subset implemented by both
the reference model and the etcd/raft adapter:

- fixed membership in which every provisioned node is a voter;
- portable KV commands with globally unique command IDs;
- propose, crash, restart, partition, and heal actions;
- election-timeout, storage-latency, network-loss, and network-latency choices;
- every crashed node restarted by the end of the workload;
- a healed network and no more actions during a positive quiet convergence
  tail; and
- one explicit comparison boundary equal to the scenario duration.

Snapshots, learners, membership transitions, crash-after-persist, malformed
commands, and silently deleted actions are not approximated. Preflight checks
the plan against both capability declarations before either cluster is
constructed or any semantic choice is consumed.

## Projection identity

A portable choice key excludes protocol terms, indexes, leaders, message
types, encoded message bytes, adapter-local choice IDs, and domain or context
digests. It contains only:

- election and storage choices: kind, node, process incarnation, generation;
- network choices: kind, sender, receiver, sender incarnation, send sequence.

Each directive also retains its index in the complete source tape. A
one-value target domain is fixed and consumes no directive. A matching
directive must belong to the target domain. A missing directive uses the
plan's pinned fallback seed and is reported as an additional target choice.
Repeated target keys, duplicate source keys, malformed contexts, unsupported
kinds, and out-of-domain selections fail projection.

Projection fidelity is `exact` only when every directive is consumed and the
target introduces no additional variable choice. Successful execution with
either unmatched source directives or additional target choices is `partial`.
For `exact` and `partial` projections, the target-local recorder emits an exact
decision tape, so that adapter's execution remains independently replayable.
For a `failed` projection, the tape contains only the exact successful prefix;
the rejected choice is not misrepresented as replayable evidence. Bundle
verification checks that prefix against the plan and deterministically
regenerates the complete failed execution with the declared build.

## Comparison discipline

Normalized comparisons keep these questions separate:

1. Were both adapters eligible for the plan?
2. Was each projection exact, partial, or failed?
3. Did each execution reach the declared comparison boundary?
4. Do violations agree within the negotiated common invariant-ID set?
5. Do each adapter's node application commitments agree at the comparison
   boundary, and do those commitments agree across adapters?

Normalization intentionally excludes terms, leaders, log indexes, message
batching, raw observation fingerprints, and simulator step counts. Equal KV
commitments establish equality of the bounded encoded command history and
resulting KV state under the shared oracle and SHA-256 assumption. They do not
establish protocol equivalence, linearizability, or correctness.
Boundary agreement is not a quiescence, liveness, or future-stability claim;
messages or uncommitted proposals may still be pending.

## Cross-adapter command

The command is built from the nested production-adapter module:

```text
cd adapters/etcdraft
go run ./cmd/draft-cross derive \
  --source-run ../../source-run.json \
  --fallback-seed 1 \
  --out ../../semantic-plan.json

go run ./cmd/draft-cross run \
  --plan ../../semantic-plan.json \
  --source-run ../../source-run.json \
  --out ../../cross-result

go run ./cmd/draft-cross verify \
  --plan ../../semantic-plan.json \
  --source-run ../../source-run.json \
  --in ../../cross-result
```

`derive` accepts only a strict, replayable reference run and extracts variable
portable directives while retaining their complete-tape indexes. It infers the
workload end from the last action and rejects a run without a positive quiet
tail or outside the bilateral v1 capability intersection. Both `derive` and
`run` execute the complete reference-v3 source tape from a clean cluster,
require every entry to be consumed, and require exact outcome equality before
trusting its directives or provenance.

`run` verifies source provenance and performs bilateral preflight before
constructing either cluster. It then publishes seven result documents plus one
commit manifest, all private and no-clobber:

```text
cross-result.reference.capabilities.json
cross-result.etcdraft.capabilities.json
cross-result.reference.execution.json
cross-result.etcdraft.execution.json
cross-result.reference.outcome.json
cross-result.etcdraft.outcome.json
cross-result.comparison.json
cross-result.manifest.json
```

Each execution records the exact target-local tape for a successful projection,
or the exact successful prefix for a failed projection, together with Git/Go
build provenance. The normalized outcomes reference the execution digests and
persist the exact negotiated common-invariant universe. The comparison binds
both execution and normalized-outcome digests. Publication stages and syncs every
document, links the seven data files, and links the manifest last as the commit
marker; existing targets are never overwritten. All files use mode `0600`.
An absent manifest unambiguously marks an interrupted bundle. After inspecting
the exact prefix, `draft-cross recover --in ../../cross-result` removes only its
seven uncommitted regular files so the experiment can be rerun; it refuses a
committed bundle.

`verify` first checks the manifest's exact byte sizes and SHA-256 hashes. It
then replay-verifies the source, checks plan and capability bindings, validates
projection accounting against each target-local tape, exactly replays
successful local tapes, deterministically regenerates both semantic
executions, computes
the invariant intersection from the two capability documents, renormalizes the
outcomes, and recomputes the comparison. Missing files, stale outcomes, unused
decisions, forged derived fields, and any manifest or document tampering fail
closed. A failed projection is checked by consuming its exact successful tape
prefix, requiring exhaustion at the next unrecorded choice, and matching a
fresh deterministic failed semantic execution.
