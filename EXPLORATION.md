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
`d-raft.run/v2` artifact and replayed with `draft replay`.

## Bounds and interpretation

Run count, prefix depth, branches per choice, range samples, per-scenario event
count, virtual time, and artifact resource limits are all explicit. A search
that samples a range, limits branches, completes a seeded suffix, or exhausts
its run budget is not an exhaustive proof. The CLI reports those conditions
instead of calling the search complete.

The current implementation intentionally has no state cache or partial-order
reduction. Those optimizations can silently miss failures unless the state
encoding is Markov-complete. A later milestone will define and test a canonical
state containing protocol and durable state, process incarnation, topology,
timers, in-flight semantic messages, remaining actions, and decider state.

## Reproducibility

Every branch is a clean rerun. Choice identities include semantic timer,
storage, and sender-incarnation/send-sequence context, while excluding
incidental scheduler event IDs, router packet IDs, and send timestamps. A
context mismatch fails closed.
