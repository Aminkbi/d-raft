# Run artifacts

`d-raft.run/v3` is the versioned replay artifact produced by the research CLI
and the candidate portability unit for adapter evaluation. It bundles
the inputs required to reconstruct a run with the evidence required to verify
its result.

## Contents

Each artifact records:

- scenario ID, version, virtual duration, and ordered external actions;
- adapter ID and version;
- a canonical pre-provisioned node universe, initial voter/learner roles,
  timing, network, storage, and stop policy;
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

`members` is the fixed provisioned universe. `voters` and `learners` assign its
initial roles; when both role arrays are absent, all provisioned members are
voters.

## Scenario actions

The v3 schema supports `propose`, `snapshot`, `begin_membership`,
`finalize_membership`, `crash`, `restart`, `crash_after_next_persist`,
`partition`, and `heal`. `begin_membership` carries canonical target voter and
learner sets; `finalize_membership` requests the final stable-configuration
entry, whose commitment happens later through normal replication.
Either action may target a named node, or use the unique current leader when
the node is omitted. The persistence-boundary action arms a crash after the
next durable write completes but before its
acknowledgement releases dependent effects. Actions are sorted by nondecreasing
virtual time. Actions at the same time run
in their listed order, after events armed during initial cluster construction.
`snapshot` binds its bytes to the target node's application index when the
action executes, so a queued persistence acknowledgement cannot move the
checkpoint boundary underneath already-captured bytes. Invalid node
references, duplicate partition membership, unrelated action
fields, out-of-range times, and unknown action kinds are rejected before a run.
Finalization is not an automatic wait or retry: scheduling it before the joint
entry and every preceding entry are committed produces an error outcome.

## Validation and replay

Artifact encoding and decoding are strict and bounded to 64 MiB. The v3 encoder
preflights aggregate variable-size content and per-choice option/context limits,
then streams bounded components into an exact-size capped buffer before
publishing any caller-visible bytes. JSON escaping and base64 expansion cannot
bypass the output cap. The schema
also caps a run at 31 provisioned members, 10,000 actions, 100,000 decisions, 1 MiB per
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

An exact `d-raft.run/v3` tape remains local to its adapter and version. The
separate `d-raft.semantic-plan/v1` schema maps the portable fixed-membership
subset onto both `d-raft/reference@3` and `go.etcd.io/raft/v3@3.7.0+d-raft.1`.
It reports exact, partial, or failed projection. Successful projections record
a fresh exact local tape; failed projections retain the exact successful
prefix and require deterministic re-execution to reproduce the rejected
choice. It never claims that batching, messages, terms, indexes, or
adapter-local observation digests are identical.

The associated capability, semantic-execution, normalized-outcome, and
normalized-comparison schemas are documented in [SEMANTIC_PLANS.md](SEMANTIC_PLANS.md).
An immutable, manifest-bound example is published in the
[cross-adapter corpus](corpus/cross-adapter/v1/).
A single-threaded artifact still cannot represent production data races, and
the current atomic store does not model torn writes or corruption.

Seeds in the run header are decimal JSON strings so full-width values survive
common JSON tooling. The embedded, already-published decision-v1 schema and
Raft message codec use JSON integer numbers; consumers must parse those fields
with 64-bit or arbitrary-precision integers and must never route them through
binary floating point.

The final-state digest is defined by `d-raft.observation/v3`: virtual time;
canonical member order; process availability; the full `raft.Status`; durable
and applied store state, including installed snapshots; and ordered violation
witnesses. Status, log entries, and snapshots include membership role sets.
Changing those fields
or their JSON representation requires a new observation schema identifier.

## Legacy v1 and v2 boundaries

A coherent published v1 artifact uses `d-raft/reference@1`, message codec v1,
checker v1, and observation v1. The library still strictly decodes, validates,
and re-encodes that tuple. `draft inspect` accepts it; current `draft replay`
and `draft minimize` reject it explicitly because the execution adapter is v3.
Snapshot actions and snapshot-conflict witnesses are invalid in v1. This avoids
silently redefining any published v1 identifier.

A coherent published v2 artifact uses `d-raft/reference@2`, message codec v2,
checker v2, and observation v2. It supports snapshot actions and snapshot
witnesses, but has no voter/learner configuration fields or membership actions.
The library continues to decode, validate, re-encode, and inspect that exact
tuple; current replay and minimization reject it explicitly.

The per-choice and aggregate producer limits introduced by v2 continue to
apply to v2 and v3, but are not retroactively applied to v1. Legacy input is
still bounded by the 64 MiB decoder and exact encoder output caps, preserving
historically valid v1 tapes with larger individual contexts.
