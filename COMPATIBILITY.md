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
