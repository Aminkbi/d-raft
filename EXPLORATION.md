# Bounded semantic exploration

`draft explore` performs depth-first search over semantic choices by rerunning
each prefix from a clean initial cluster. It never clones scheduler closures or
mutable simulator internals.

## Algorithm

For each prefix, a `decision.PrefixDecider` validates every stored choice and
stops at the first open choice. The explorer creates one child prefix per
selected domain candidate and reruns. At the configured depth boundary, a
`PrefixThenDecider` holds the prefix fixed and completes the suffix with a
seeded decider; a recorder captures the resulting exact tape.

Discrete options are visited in schema order up to the branch limit. Small
integer ranges are enumerated; larger ranges sample minimum, maximum, and
optionally midpoint. Results separately report:

- clean reruns;
- open choices;
- completed suffixes;
- infeasible prefixes that terminated before consuming their tape;
- depth-bound suffix completions;
- sampled rather than exhaustively enumerated domains;
- violating runs; and
- truncation by the run budget.

The first matching violation can be emitted directly as a normal
`d-raft.run/v3` artifact and replayed with `draft replay`.

The built-in reference command branches over semantic timer, network, and
storage choices; it does not synthesize action schedules or target
memberships. The general `explore.DFS` API accepts every valid choice kind and
can execute a caller-constructed v3 membership scenario, while `draft explore`
generates a steady all-voter scenario.

## Bounds and interpretation

Run count, prefix depth, branches per choice, range samples, per-scenario event
count, virtual time, and artifact resource limits are all explicit. A search
that samples a range, limits branches, completes a seeded suffix, or exhausts
its run budget is not an exhaustive proof. The CLI reports those conditions
instead of calling the search complete.

`explore.DFSWithCache` adds opt-in, bounded canonical-state pruning;
`draft explore` enables it by default and `--cache=false` provides the matched
uncached baseline. Partial-order reduction is not implemented.

## Canonical frontier

The cache compares a stable state immediately before an event, the exact
semantic selections consumed inside that event, and the next open choice.
This tuple reconstructs an in-callback continuation through a clean rerun;
Go stack frames and closures are never serialized.

The canonical encoding covers protocol and durable state, process lifecycle,
timers, semantic in-flight messages, topology, pending persistence, queued
inputs, remaining external actions, checker history, and exploration
continuation. Its Markov-completeness contract is limited to the built-in
reference adapter and named schema/configuration versions. Arbitrary runner
closures remain uncached unless they provide an explicit complete-state
contract.

SHA-256 may select a cache bucket, but pruning requires exact equality of the
full canonical bytes stored in that bucket. Cache capacity is bounded and
deterministic; a full cache bypasses admission but cannot justify a merge.
Remaining exploration depth or equivalent coverage information is part of the
bounded-search comparison.

Converging synthetic models and a small reference scenario currently test
cache-off/cache-on parity. The broader evaluation matrix remains to be run.
Benchmark runs should report lookups, exact hits, collisions, retained unique
identities, pruned prefixes, encoding and memory cost, avoided work, wall time,
and all sampling or truncation flags.
State caching is established model-checking practice; this work applies it to
d-raft's semantic explorer rather than claiming a novel reduction method.

## Reproducibility

Every branch is a clean rerun. Choice identities include semantic timer,
storage, and sender-incarnation/send-sequence context, while excluding
incidental scheduler event IDs, router packet IDs, and send timestamps. A
context mismatch fails closed.
