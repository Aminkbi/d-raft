# Research direction

d-raft investigates whether a distributed-systems failure can be represented
as a small semantic artifact that remains useful outside the simulator that
found it. Raft is the first target because its safety properties are precise,
its persistence boundaries are operationally important, and mature independent
implementations are available for comparison.

## Working thesis

Given a Raft safety failure found by randomized or systematic execution,
d-raft should produce a versioned counterexample that is:

- replayable without depending on incidental random-number consumption;
- minimized in terms of semantic choices rather than raw log lines;
- independently checkable from a structured invariant witness; and
- portable across supported implementations and versions through adapters.

The intended research contribution is the combination of portability,
semantic reduction, explicit evidence, and cross-implementation replay. The
project does **not** claim novelty for deterministic simulation, a pure
state-machine interface, seed replay, trace minimization, or implementation
trace validation in isolation.

## Current foundation

The repository currently contains:

1. A protocol-neutral, single-threaded discrete-event runtime with stable
   random streams, virtual time, network faults, and ordered observations.
2. A pure Raft reference state machine implementing elections, heartbeats, log
   replication, current-term commit, durable snapshots, safe compaction, and
   leader no-op entries.
3. A cluster harness with durable stores, explicit persistence acknowledgement,
   input barriers, partitions, process incarnations, and crash/restart.
4. An independent checker for election safety, certificates, durable votes,
   term monotonicity, log matching, leader completeness, and committed/applied
   conflicts.
5. A semantic decision schema with seeded recording, exact tape replay, stable
   causal identities, and domain-drift rejection.
6. A bounded, payload-lossless decoder for the known fields of the separate
   observational trace schema.
7. A strict run-artifact schema and CLI that bundle scenario, configuration,
   environment, semantic tape, outcome, observation digest, and witnesses.

The reference model is a fixture and oracle for experiments, not itself the
claimed research novelty.

## Research questions

1. How reliably do semantic counterexamples replay across machines, supported
   Go toolchains, implementation versions, and independent Raft adapters?
2. Under equal transition and wall-clock budgets, how do randomized and bounded
   systematic search compare in time to first failure and distinct failures?
3. How much do semantic, dependency-aware reduction and generic delta debugging
   reduce a failing execution while preserving the same violation fingerprint?
4. Does a minimized counterexample with a structured witness reduce diagnosis
   time compared with a seed and an unminimized event trace?
5. Which choices form a portable core across implementations with different
   batching, ticking, pre-vote, and storage APIs?

## Artifact pipeline

```text
versioned scenario
  -> random or bounded systematic execution
  -> independent invariant witness
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
- [x] Independent safety checker and structured fingerprints
- [x] Semantic decision recording and exact replay
- [x] Payload-lossless observational trace decoding
- [x] Versioned, self-describing run artifacts and `run`/`replay`/`inspect` CLI
- [x] Prefix replay and bounded depth-first choice exploration
- [x] Fingerprint-preserving semantic delta debugging and domain shrinkers
- [x] Snapshot installation and safe log compaction
- [ ] Joint-consensus membership changes and learners
- [ ] Production Raft adapter with declared capability boundaries
- [ ] Chain-of-Blocks state-machine oracle and seeded mutant corpus
- [ ] Comparative evaluation, public counterexample corpus, and archival release

## Evaluation design

Experiments will report the repository commit, Go version, hardware, search
budget, cluster size, scenario and adapter versions, fault policy, seed,
decision schema, invariant, and raw/minimized artifact sizes. Performance
comparisons require repeated trials and uncertainty intervals.

Primary measurements are executions and transitions per second, explored
prefixes and unique canonical states, time to first failure, distinct violation
fingerprints, replay success rate, cross-adapter replay rate, reduction ratio,
reduction cost, and mutant kill rate.

Planned baselines include:

- seed-only randomized replay;
- ordinary delta debugging over a flat stimulus list;
- a DEMi-inspired distributed-trace reduction strategy; and
- random search versus bounded prefix exploration under equal budgets.

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

A formal related-work matrix and pinned citations will accompany the evaluation
artifact.

## Threats to validity

- The single-threaded simulator cannot expose production data races.
- A virtual network and atomic storage model omit kernel, filesystem, torn-write,
  corruption, and hardware behavior unless modeled explicitly.
- Bounded exploration cannot establish unbounded safety or liveness.
- A non-Markov-complete state abstraction can unsafely merge distinct futures;
  early exploration must compare canonical state encodings as well as hashes.
- Reference-model and checker defects can be correlated despite package
  separation; mutants, independent adapters, and external validation mitigate
  but do not eliminate this risk.
- Cross-implementation semantics may be narrower than any one implementation's
  feature set.
- Results from one production adapter or one mutant corpus may not generalize.
- Opaque application snapshots cannot prove leader completeness for entries
  below their boundary. The current checker detects conflicting snapshots at
  an equal boundary but reports no stronger compacted-prefix claim; the planned
  Chain-of-Blocks oracle supplies independently comparable state commitments.

These limitations must remain explicit in papers, talks, release notes, and
artifact documentation.
