# Compatibility and determinism

This document defines the stability boundary of d-raft's simulation kernel.
It distinguishes reproducible behavior from behavior that merely happens to
be consistent in the current implementation.

## Supported Go version

The module follows the current stable Go release and currently declares Go
1.26 with toolchain 1.26.6. CI resolves the version from `go.mod` and requests
the latest available matching toolchain. A future minor release of d-raft may
raise the required stable Go version.

## Go API compatibility

d-raft follows semantic versioning. Before v1, exported APIs may change in a
minor release when the change materially improves the research interface.
Patch releases must remain source compatible. Deprecations will be documented
before removal whenever practical.

## Reproducibility guarantee

For a fixed d-raft version, starting state, sequence of API calls, seeds, and
deterministic callbacks, d-raft guarantees the same:

- random results;
- event identifiers and event execution order;
- packet identifiers, loss decisions, sampled latencies, and delivery order;
- partition and endpoint lifecycle outcomes; and
- `d-raft.trace/v1` record order and values.

Event ties are resolved by scheduling order. Random ranges are inclusive where
the API says they are inclusive. Partition connectivity is evaluated both at
send time and delivery time.

The SplitMix64 `Uint64` bit stream for a seed is a permanent compatibility
surface. A change to derived operations such as bounded integers, probability
draws, or duration sampling will be treated as a determinism-breaking change
and called out prominently in release notes.

The following are outside the guarantee:

- decisions made by unordered map iteration in application callbacks;
- wall-clock reads, global or external random sources, concurrent callbacks,
  and external I/O;
- mutation of a message after `Send` when no adequate `CloneFunc` was given;
- side effects performed by observers or trace sinks; and
- executions moved between different d-raft versions unless their release
  notes explicitly state trace compatibility.

## Trace schema compatibility

The JSON Lines schema is versioned independently through the `schema` field.
The current value is `d-raft.trace/v1`. Within v1:

- existing fields retain their meaning and JSON type;
- new optional fields and new event kinds may be added;
- sequence numbers start at one and increase by one per successfully written
  record;
- virtual times and durations are integer nanoseconds;
- full-width unsigned random values use strings to avoid JSON precision loss;
  and
- packet messages use their ordinary Go JSON representation.

Consumers must ignore unknown fields and may reject unknown event kinds when
strict validation is required. Renaming or removing a field, changing a field
type or unit, or changing an existing event's meaning requires a new schema
major version.

`JSONLRecorder.Err` must be checked after a run. Unsupported message values or
writer failures stop further recording but do not alter simulation control
flow.

## Semantic decision and run artifacts

Semantic choices use `d-raft.decisions/v1`; current self-describing executions
use `d-raft.run/v2`. Published `d-raft.run/v1` artifacts remain strictly
decodable and inspectable. These schemas are independent from the observational
trace.
A v1 decision entry records the full choice, SHA-256 identities of its legal
domain and semantic context, and the selected alternative. Exact replay stops
at the first identity, kind, domain, context, or selection mismatch and also
requires the tape to be fully consumed.

A run artifact additionally fixes the scenario identifier and version,
external action order and virtual times, adapter identity, cluster
configuration, seeds, codec, decision/checker/observation schemas, toolchain and source revision,
outcome, canonical observation digest, and invariant witnesses. The strict v1
decoder rejects unknown fields. Additive schema changes therefore require a
new reader mode or schema version rather than being silently ignored.

Cross-version replay is an evaluated capability, not a blanket guarantee. A
successful replay means the target adapter consumed the semantic tape and
reproduced the recorded outcome; a rejected semantic context is useful drift
evidence rather than a replay failure hidden by best-effort defaults.

Run-header seeds are canonical decimal strings. Decision-v1 option weights,
ranges, selections, and Raft message integers remain JSON numbers for
compatibility with the published schema; consumers must use lossless integer
decoding rather than `float64`.

The built-in reference compatibility tuple is atomic:

- run v1: `d-raft/reference@1`, message codec v1, checker v1, observation v1;
- run v2: `d-raft/reference@2`, message codec v2, checker v2, observation v2.

The current CLI executes and minimizes only the v2 reference adapter. It can
decode and inspect a coherent v1 artifact, but rejects replay rather than
pretending that snapshot-aware semantics reproduced a v1 observation digest.
Run v2 adds the scheduled `snapshot` action and snapshot-bearing protocol,
durable, application-store, checker, and observation state.
