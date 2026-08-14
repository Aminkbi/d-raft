# Run artifacts

`d-raft.run/v1` is the portable unit produced by the research CLI. It bundles
the inputs required to reconstruct a run with the evidence required to verify
its result.

## Contents

Each artifact records:

- scenario ID, version, virtual duration, and ordered external actions;
- adapter ID and version;
- canonical membership, timing, network, storage, and stop policy;
- semantic and infrastructure seeds;
- source revision and dirty state, Go version, decision/checker/observation
  schemas, and message codec;
- the complete `d-raft.decisions/v1` choice tape;
- outcome status, executed steps, virtual end time, and error when applicable;
- a SHA-256 digest of canonical live protocol state, durable stores, applied
  entries, process availability, virtual time, and violations; and
- structured invariant witnesses with stable fingerprints.

The format intentionally does not embed the much larger observational JSONL
trace. A trace explains detailed event order; the semantic tape drives replay;
the observation digest and witnesses verify the result.

## Scenario actions

The v1 schema supports `propose`, `crash`, `restart`,
`crash_after_next_persist`, `partition`, and `heal`. The persistence-boundary
action arms a crash after the next durable write completes but before its
acknowledgement releases dependent effects. Actions are sorted by nondecreasing
virtual time. Actions at the same time run
in their listed order, after events armed during initial cluster construction.
Invalid node references, duplicate partition membership, unrelated action
fields, out-of-range times, and unknown action kinds are rejected before a run.

## Validation and replay

Artifact encoding and decoding are strict and bounded to 64 MiB. The schema
also caps a run at 31 members, 10,000 actions, 100,000 decisions, 1 MiB per
action payload, 1,024 violations, 1 MiB per witness, 4 KiB of outcome error
text, 1,000,000 simulator events, and 24 hours of virtual time. Every scenario
carries a nonzero maximum event count. Exhausting it
produces the deterministic `budget_exhausted` outcome rather than an unbounded
run. Validation rejects unknown fields, trailing JSON values, invalid timing
and membership, malformed tapes,
inconsistent outcomes, and invalid observation digests.

Replay always creates a fresh cluster. It succeeds only when:

1. every encountered choice matches the stored semantic identity, kind,
   domain, and context;
2. every stored decision is consumed exactly once; and
3. status, step count, virtual end time, error, violation fingerprints, and
   canonical observation digest match the recorded outcome.

This is deliberately fail-closed. A changed message, storage token, topology,
or scenario action must not silently consume a decision intended for something
else.

## Security and privacy

Decision contexts contain protocol messages and client proposal bytes. Treat
artifacts as potentially sensitive. Inspect them before publishing and never
record production secrets. CLI writes are private (`0600`), use atomic
no-clobber publication, and refuse to replace an existing artifact. The current
schema provides integrity checks for replay drift, not cryptographic
authenticity; sign release artifacts and checksums separately.

## Current portability boundary

The only adapter in v1 is `d-raft/reference@1`. Cross-implementation replay
will require an adapter to map its batching, timer, storage, and membership
semantics onto the common choice model. A single-threaded artifact cannot
represent production data races, and the current atomic store does not model
torn writes or corruption.

Seeds in the run header are decimal JSON strings so full-width values survive
common JSON tooling. The embedded, already-published decision-v1 schema and
Raft message codec use JSON integer numbers; consumers must parse those fields
with 64-bit or arbitrary-precision integers and must never route them through
binary floating point.

The final-state digest is defined by `d-raft.observation/v1`: virtual time;
canonical member order; process availability; the full `raft.Status`; durable
and applied store state; and ordered violation witnesses. Changing those fields
or their JSON representation requires a new observation schema identifier.
