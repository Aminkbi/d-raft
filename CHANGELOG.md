# Changelog

All notable changes to d-raft are documented here. The project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A focused failure-preserving replay research protocol with explicit defect
  predicates, baselines, exclusions, and production-evaluation decision gates.
- A deterministic synthetic projection study comparing fault intervals,
  existing occurrence-based projection, and an operation-identity prototype;
  raw outcomes, exact local replay, tamper verification, and CI checks.

### Clarified

- Exact portable projection describes choice coverage, not causal equivalence.
  A reordered-message fixture demonstrates exact coverage that loses the
  source failure. Conflicting batches are rejected by the experimental causal
  policy; existing v1 schemas and production adapters are unchanged.

### Changed

- Modernized safe internal paths for Go 1.27.1 while retaining Go 1.26 as the
  minimum module language/runtime target.
- Reduced redundant state, message, and decision-tape copies without changing
  ownership boundaries or deterministic output.
- Added explicit Go 1.26 compatibility CI coverage and read-only module,
  formatting, and published-artifact checks.

### Fixed

- Made checker log and leader-completeness witnesses deterministic and removed
  repeated prefix scans.
- Hardened trace and mutant decoding against duplicate JSON names and nil
  readers, and rejected noncanonical artifact digests and role fields.
- Corrected virtual-clock snapshot ownership and publication short-write and
  directory-durability handling.

## [0.1.0] - 2026-08-17

First archival research release by Mohammadamin Khanbabaei (`aminkbi`).

### Added

- A deterministic, single-threaded simulation kernel with virtual time,
  stable random streams, a faultable typed network, and ordered observations.
- A durable reference Raft implementation with elections, replication,
  snapshots, safe compaction, learners, and joint-consensus membership changes.
- Package-separated safety checking with structured witnesses and stable
  violation fingerprints.
- Versioned semantic decision tapes with exact replay and explicit identity,
  kind, domain, and selection drift detection.
- Strict self-describing run artifacts and a research CLI for execution,
  inspection, replay, bounded exploration, and minimization.
- Bounded depth-first exploration, collision-safe exact frontier caching, and
  fingerprint-preserving scenario and semantic-choice minimization.
- An experimental adapter for the unmodified `go.etcd.io/raft/v3` v3.7.0
  `RawNode` core and an adapter-local exact-replay workflow.
- A portable binary KV application oracle with independently checkable state
  and history commitments.
- A versioned six-mutant corpus and isolated runner. The published bounded run
  records three safety-checker kills and three separately classified
  conformance kills.
- Capability-negotiated semantic plans with source provenance, explicit
  exact/partial/failed projection evidence, normalized outcomes, and verified
  reference-versus-etcd corpus cases.
- A canonical faulted workload covering loss, partition/heal, crash/restart,
  application proposals, and a quiet convergence tail.
- A clean-provenance, balanced 21-trial evaluation harness and immutable raw
  result. Its paired cache study observed zero exact cache hits and therefore
  reports a null-hit overhead result, not pruning efficacy.
- Reproducibility, related-work, evaluation, citation, governance, security,
  and contribution documentation suitable for public research reuse.

### Fixed

- Bound canonical workload source identity across reference and production-core
  adapters so a source cannot silently drift during projection.

### Research boundaries

- The release does not claim that bounded executions prove Raft safety.
- Mutant results are known-fault harness evidence, not an estimate of
  production-defect detection probability.
- Cross-adapter agreement is capability-negotiated and partially projected;
  it is not protocol equivalence or a linearizability proof.
- Real-bug effectiveness, comparative reduction quality, and diagnosis-time
  benefit remain unmeasured.

[Unreleased]: https://github.com/aminkbi/d-raft/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/aminkbi/d-raft/releases/tag/v0.1.0
