# Research direction

d-raft is investigating whether deterministic simulation can turn failures in
consensus protocols into small, portable, and understandable engineering
artifacts. Raft is the first target because its safety properties are precise,
its failure modes are operationally important, and several mature Go
implementations are available for comparison.

## Working thesis

Given a consensus failure discovered by randomized or systematic simulation,
d-raft should produce a versioned execution trace that can be replayed,
automatically minimized, and explained in terms of a violated protocol
invariant.

The current repository establishes the deterministic scheduler, stable random
streams, faultable network, and ordered trace schema needed to test that
thesis. It does not claim that deterministic simulation alone proves protocol
correctness.

## Research questions

1. How reliably can a discovered failure be reproduced across machines and
   supported Go toolchains?
2. How many distinct failures does guided schedule exploration find compared
   with ordinary seeded randomized testing under an equal compute budget?
3. How much can a failing execution be reduced while preserving its violated
   invariant?
4. Does a minimized semantic trace reduce the time required to diagnose a
   consensus defect?
5. Can one harness drive both a reference model and production-grade Raft
   implementations without weakening the fault model?

## Planned experimental artifact

The intended end-to-end workflow is:

```text
scenario -> explore -> detect invariant violation -> save trace
         -> replay -> minimize -> explain -> regression corpus
```

The next milestones are:

1. A pure election-only Raft state machine with explicit inputs and effects.
2. Safety and bounded-liveness invariant checks.
3. Trace-driven replay independent of random-stream consumption.
4. Delta-debugging of messages, faults, timers, and client operations.
5. State hashing and bounded systematic schedule exploration.
6. An adapter for at least one established Go Raft implementation.
7. A public corpus of minimized failures and reproducibility packages.

## Evaluation and reporting

Experiments should report the repository commit, Go version, hardware, search
budget, cluster size, scenario, fault policy, seed, trace schema, invariant,
and raw/minimized trace sizes. When performance is compared, benchmarks should
include repeated trials and uncertainty rather than only a single throughput
number.

Useful measurements include executions per second, unique abstract states,
time to first failure, distinct invariant violations, replay success rate,
trace reduction ratio, and diagnosis time in a small user study.

## Threats to validity

- A single-threaded simulator cannot expose implementation data races.
- A virtual network and storage model omit some kernel, filesystem, and
  hardware behaviors.
- Bounded exploration cannot establish unbounded liveness.
- State hashing may merge executions that differ in a behaviorally important
  way if the abstraction is too coarse.
- A reference Raft model can share mistakes with its invariants.
- Results from one production adapter may not generalize to other designs.

These limitations should remain explicit in papers, talks, and release notes.
