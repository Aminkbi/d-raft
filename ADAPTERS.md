# Adapter contracts

d-raft adapters translate one portable external scenario into an
implementation-specific deterministic execution. They must declare and reject
unsupported capabilities, preserve persistence ordering, normalize deep-copied
observations, and version every replay-relevant boundary.

There are four distinct compatibility levels:

1. **Adapter-local replay:** the same adapter version consumes the exact tape.
2. **Portable scenario:** adapters receive the same external actions and fault
   intervals within their declared capability intersection.
3. **Semantic plan projection:** an adapter-neutral plan is translated while
   unmatched source choices and additional target choices are reported.
4. **Normalized comparison:** common invariant IDs, versioned adapter-neutral
   witness projections, and application commitments are compared after an
   explicit convergence tail.

Cross-adapter work must not require equal leaders, terms, log indexes, internal
no-ops, message counts, simulator steps, timings, or adapter-specific digests.
Evaluation reports eligibility, translation/choice-consumption, deterministic
completion, invariant-ID agreement, and neutral-witness agreement separately.

## Available adapters

| Adapter | Status | Documentation |
| --- | --- | --- |
| `d-raft/reference@3` | Research reference model; full current feature surface and canonical cache | [README](README.md) |
| `go.etcd.io/raft/v3@3.7.0+d-raft.1` | Experimental production-core adapter; fixed-membership capability subset | [adapter README](adapters/etcdraft/README.md) |
