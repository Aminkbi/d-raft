# Research direction

d-raft investigates whether a distributed-systems failure can be represented
as a small semantic artifact that remains useful outside the simulator that
found it. Raft is the first target because its safety properties are precise,
its persistence boundaries are operationally important, and mature independent
implementations are available for comparison.

## Primary claim to test

For a predeclared set of supported implementation changes, semantic
counterexamples preserve reproduction of the same defect more reliably than
seed-only replay; semantic reduction decreases the execution evidence needed
to reproduce that defect at a measured cost.

This is a falsifiable hypothesis, not a measured result. The first paper is
scoped to failure-preserving replay and reduction. Search efficiency, cache
performance, general protocol equivalence, and human diagnosis time are
separate workstreams rather than additional primary claims.

[RESEARCH_PROTOCOL.md](RESEARCH_PROTOCOL.md) specifies the comparison units,
failure predicate, baselines, exclusions, and decision gates. Changes of
interest are message insertion/reordering, batching, timer scheduling, and
supported version upgrades. Preservation is conditional on a stated mapping
of causal events; equal occurrence keys alone do not supply that mapping.

Three outcomes must stay separate:

1. Reproducing a defect on another vulnerable implementation version.
2. Replaying its scenario on a fixed version without the target failure.
3. Applying the scenario to an independent implementation and independently
   assessing its outcome. A correct implementation need not fail.

The executable [projection study](PROJECTION_STUDY.md) establishes a narrower
negative result: v1 projection can consume every directive exactly while
moving faults to different logical operations. Its causal prototype is a toy
operation-level policy, not an implemented production-adapter causal schema.
Production-defect effectiveness and a comparative reduction benefit remain
unmeasured.

The project does **not** claim novelty for deterministic simulation, a pure
state-machine interface, seed replay, trace minimization, or implementation
trace validation in isolation.

## Current foundation

The repository currently contains:

1. A protocol-neutral, single-threaded discrete-event runtime with stable
   random streams, virtual time, network faults, and ordered observations.
2. A pure Raft reference state machine implementing elections, heartbeats, log
   replication, current-term commit, durable snapshots, safe compaction,
   joint-consensus membership changes, learners, and leader no-op entries.
3. A cluster harness with configurable initial voters and learners, explicit
   begin/finalize membership actions, durable stores, persistence
   acknowledgement, input barriers, partitions, process incarnations, and
   crash/restart.
4. A package-separated checker for election safety, membership-aware election
   certificates, durable votes, term monotonicity, log matching, leader
   completeness, membership-transition history, and committed/applied conflicts.
5. A semantic decision schema with seeded recording, exact tape replay, stable
   causal identities, and domain-drift rejection.
6. A bounded, payload-lossless decoder for the known fields of the separate
   observational trace schema.
7. A strict run-artifact schema and CLI that bundle scenario, configuration,
   environment, semantic tape, outcome, observation digest, and witnesses.
8. A versioned, event-boundary canonical state for the reference runner and a
   collision-safe, capacity-bounded exploration cache with exact-byte equality.
9. An experimental adapter for the unmodified `go.etcd.io/raft/v3` v3.7.0
   `RawNode` core, with explicit fixed-membership capabilities, persistence
   barriers, conservative checking, and adapter-local exact replay.
10. A versioned binary KV application oracle with strict canonical commands,
    self-verifying checkpoints, reference snapshot continuity, and compact
    adapter-neutral history/state commitments in both adapters.
11. Strict adapter-neutral semantic plans with source-tape provenance,
    bilateral capability negotiation, explicit projection fidelity, exact
    target-local replay tapes for successful projections, successful-prefix
    evidence for failed projections, and axis-separated normalized comparisons.
12. A versioned canonical portable workload with proposals, loss,
    partition/heal, crash/restart, a convergence tail, and an end-to-end
    cross-adapter commitment regression.

The reference model is a fixture and oracle for experiments, not itself the
claimed research novelty.

## Primary research questions

1. Under which declared implementation changes does a semantic counterexample
   preserve a specified defect, compared with seed-only and interval replay?
2. When does occurrence correspondence differ from causal correspondence, and
   which target mappings must be rejected rather than approximated?
3. How much does semantic reduction reduce actions, executed choices, artifact
   bytes, and execution length compared with flat ddmin under the same budget
   and preservation predicate?

Cross-machine exact local replay is a reproducibility prerequisite. The v1
cache/accounting study below is supporting infrastructure evidence. Diagnosis
time remains outside the first-paper claim and requires a separate user study.

## Artifact pipeline

```text
versioned scenario
  -> random or bounded systematic execution
  -> package-separated invariant witness
  -> self-describing run artifact
  -> exact replay
  -> fingerprint-preserving semantic minimization
  -> adapter replay
  -> regression corpus
```

Every artifact should include scenario and adapter identifiers and versions,
membership and timing configuration, root seed, repository revision, Go
version, decision schema and tape, outcome, observation digest, and any
violation witness.

## Implementation roadmap

- [x] Deterministic virtual-time runtime and faultable network
- [x] Pure durable Raft elections and log replication
- [x] Package-separated safety checker and structured fingerprints
- [x] Semantic decision recording and exact replay
- [x] Payload-lossless observational trace decoding
- [x] Versioned, self-describing run artifacts and `run`/`replay`/`inspect` CLI
- [x] Prefix replay and bounded depth-first choice exploration
- [x] Fingerprint-preserving semantic delta debugging and domain shrinkers
- [x] Snapshot installation and safe log compaction
- [x] Joint-consensus membership changes and learners
- [x] Canonical reference frontiers and collision-safe bounded state caching
- [x] Experimental production-core Raft adapter with declared capability boundaries
- [x] Portable application-state oracle and cross-adapter commitment surface
- [x] Versioned seeded mutant corpus and isolated execution harness
- [x] Adapter-neutral semantic plans and normalized cross-adapter execution
- [x] Bounded comparative evaluation and public counterexample corpus
- [x] Reproducible v0.1 archival release package

The completed membership milestone is deliberately scoped to role changes over
a pre-provisioned universe with explicit two-phase finalization. It does not
provide dynamic transport membership, automatic finalization, or an automatic
learner catch-up/readiness gate.

## Evaluation design

Experiments report the repository commit, Go version, hardware, search
budget, cluster size, scenario and adapter versions, fault policy, seed,
decision schema, invariant, and raw/minimized artifact sizes. Performance
comparisons require repeated trials and uncertainty intervals.

The initial comparative study is deliberately a bounded harness/accounting
study, not a bug-finding-effectiveness experiment. Its single common ceiling is
runner invocations. A random invocation is a terminal full run, while a DFS
invocation may stop at an open choice, so equal invocation counts are not equal
computational work. Random-versus-DFS elapsed time and event-attempt throughput
are descriptive marginals only. The pre-specified paired contrast is cache-on
minus cache-off within the same trial; it measures the end-to-end effect of
enabling pruning under matched DFS bounds, including cache lookup/retention
overhead and any downstream runner or canonicalization work avoided, against a
cache-off baseline that retains frontier-capture overhead. If no repeated exact
cache identity occurs, that contrast is a null-cache-hit/overhead result and is
not evidence of pruning efficacy.

Primary measurements for this bounded study are runner invocations, accepted
terminal executions by closed outcome status, processed simulator event
attempts, explored/open/pruned prefixes, retained unique frontier identities,
cache accounting, wall time, and event-attempt throughput. Exact local replay
success, semantic-plan acceptance, normalized-outcome agreement, reduction
ratio/cost, and bounded mutant kills come from their separate versioned corpora.
Time to first real failure, detection probability, distinct production-defect
yield, and diagnosis time remain unmeasured; those require seeded-fault or
real-defect workloads, time/transition-matched designs, and in the diagnosis
case a preregistered user study.

Planned baselines include:

- matched cache-off/cache-on runs with identical scenario, seed, bounds,
  branch order, capacity, sampling, and truncation reporting;
- seed-only randomized replay;
- ordinary delta debugging over a flat stimulus list;
- a DEMi-inspired distributed-trace reduction strategy; and
- descriptive random full runs and bounded prefix exploration under the same
  runner-invocation ceiling, with their unequal work semantics reported.

Diagnosis-time claims require a preregistered small user study or should remain
clearly labeled qualitative.

## Prior art

The design and evaluation must compare against, and avoid overstating novelty
relative to:

- [etcd/raft](https://github.com/etcd-io/raft), including its deterministic core
  and [TLA+ trace validation](https://github.com/etcd-io/raft/tree/main/tla);
- [FoundationDB deterministic simulation](https://apple.github.io/foundationdb/testing.html);
- [DEMi (NSDI 2016)](https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/scott);
- [MadRaft/MadSim](https://github.com/madsim-rs/madraft);
- systematic and interactive systems such as SAMC, Oddity, Coyote, Turmoil,
  and VOPR; and
- Antithesis, including its published
  [Raft findings](https://antithesis.com/blog/2026/finding-bugs-in-raft-implementations/).

A formal related-work matrix and pinned citations accompany the evaluation
artifact in [RELATED_WORK.md](RELATED_WORK.md).

## Threats to validity

- The single-threaded simulator cannot expose production data races.
- A virtual network and atomic storage model omit kernel, filesystem, torn-write,
  corruption, and hardware behavior unless modeled explicitly.
- Bounded exploration cannot establish unbounded safety or liveness.
- A non-Markov-complete state abstraction can unsafely merge distinct futures;
  exploration therefore uses SHA-256 only as a bucket index and requires exact
  canonical-byte equality before pruning.
- Reference-model and checker defects can be correlated despite package
  separation; mutants, independent adapters, and external validation mitigate
  but do not eliminate this risk.
- Membership checks validate transition history and quorum-certificate evidence,
  but the election certificate contains the implementation-reported election
  membership. This is structured evidence, not a mechanized proof of the
  implementation's membership semantics.
- Cross-implementation semantics may be narrower than any one implementation's
  feature set.
- Results from one production adapter or one mutant corpus may not generalize.
- The bounded evaluator's processed-event counter includes a scheduled event
  that opens a choice after partial processing, but excludes bootstrap choices
  and canonicalization work. It is an event-attempt accounting measure, not an
  implementation-independent transition or equal-work unit.
- Its fixed cyclic order balances method positions but does not randomize away
  temporal drift or autocorrelation. Student-t intervals are conditional,
  descriptive summaries of these serial trials rather than population-level
  coverage guarantees.
- Opaque legacy application snapshots cannot prove leader completeness for
  entries below their boundary. The portable KV profile supplies independently
  comparable bounded state/history commitments, but that shared oracle has
  common-mode risk and does not itself prove Raft safety or linearizability.

These limitations must remain explicit in papers, talks, release notes, and
artifact documentation.
