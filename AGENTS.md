# Working in d-raft

d-raft is a deterministic Raft research platform. The root Go package is named
`sim`; the reference protocol, harness, checker, replay, and research tooling
live in separate packages. This guide is a navigation aid; detailed contracts
remain in the linked documents and executable tests.

## Start with the task's slice

- Check `git status --short` and preserve existing user changes.
- Read the relevant implementation and adjacent tests, then only the documents
  needed for the task. Avoid loading all root Markdown files at once.
- Use targeted `rg` searches. `corpus/` and `evaluation/results/` contain large,
  versioned evidence; inspect their README or a specific JSON field first.
- Keep transient binaries and outputs in ignored `.research-bin/` or a temporary
  directory. Keep credentials, editor settings, and local agent state out of
  project context.
- Local tool preferences and machine-specific settings may guide local
  execution, but do not copy their contents or promote their assumptions into
  code, documentation, or `AGENTS.md`. Shared guidance must be grounded in
  project contracts or explicit repository-wide user instructions.

| Task | Implementation | Contract to read |
| --- | --- | --- |
| Virtual time, event ordering, RNG, network | Root `*.go` | `COMPATIBILITY.md` |
| Protocol, persistence, crash/restart | `raft/`, `raftsim/` | `COMPATIBILITY.md`; `SNAPSHOTS.md` or `MEMBERSHIP.md` as needed |
| Safety witnesses and fingerprints | `check/` | `MEMBERSHIP.md`, `ARTIFACTS.md` |
| Decisions, exact replay, run artifacts | `decision/`, `artifact/`, `experiment/`, `cmd/draft/` | `ARTIFACTS.md` |
| Observational JSONL decoding | `trace/`, root `trace.go` | `COMPATIBILITY.md` |
| Exploration and canonical frontiers | `explore/`, `experiment/`, `raftsim/` | `EXPLORATION.md`, `CANONICAL_STATE.md` |
| Counterexample reduction | `minimize/` | `MINIMIZATION.md` |
| Portable KV commands and commitments | `apporacle/`, `tools/verify_apporacle.py` | `APPLICATION_ORACLE.md` |
| Semantic projection and comparisons | `semanticplan/`, `experiment/semantic.go` | `SEMANTIC_PLANS.md`, `PROJECTION_STUDY.md` |
| Production-core adapter | `adapters/etcdraft/` | Its scoped `AGENTS.md` and `README.md`, root `ADAPTERS.md` |
| Mutant execution | `mutant/`, `cmd/draft-mutants/` | `MUTANTS.md` |
| Evaluation or research claims | `evaluation/`, `internal/projectionstudy/`, corresponding commands | `RESEARCH_PROTOCOL.md`, then `EVALUATION.md` or `PROJECTION_STUDY.md` |

## Verification

Both modules declare their supported Go version and suggested toolchain in
their `go.mod`. Root `go test ./...` **does not test the nested adapter module**.
From any working directory:

```bash
bash /path/to/d-raft/tools/check.sh quick
bash /path/to/d-raft/tools/check.sh evidence
```

`quick` checks formatting, module integrity/tidiness, tests, and vet in both
modules. `evidence` verifies published bundles/checksums and independent KV
vectors without generating new publication results. It requires Python 3 and
`sha256sum` as well as Go. Dependencies must already be cached for offline use.

During implementation, start with affected packages, e.g. `go test ./raft
./raftsim ./check`. Before finishing a code change, run the applicable module
checks; replay/projection changes also need the evidence checks. Full CI adds
race detection, repeated adapter determinism tests, and minimum-Go coverage;
see `.github/workflows/ci.yml`. Documentation-only edits need link/command
review rather than repeated protocol tests.

Canonical-state/cache changes need the enumerated parity and divergent-future
checks described in `CANONICAL_STATE.md`'s validation gate. Do not describe a
sampled, depth-limited, or budget-truncated search as exhaustive verification.

If the default build cache is read-only, set `GOCACHE` to a writable temporary
directory for the whole check command. Do not change module/toolchain pins to
work around a local environment problem.

## Contracts to preserve

- Protocol execution uses virtual time and deterministic callbacks. Keep
  wall-clock measurement and external processes at research-runner boundaries.
- Order map-derived state explicitly and clone mutable messages/state at API
  boundaries. Event IDs break equal-time ties; RNG consumption and decision
  identity changes can change replay compatibility.
- Preserve persist-before-dependent-effects barriers and both crash boundaries:
  before durable completion and after completion but before acknowledgement.
- Decision tapes drive exact local replay; observational traces explain a run.
  Projection coverage alone does not establish causal fault correspondence.
- Keep strict schema/resource validation, full-width integer encoding, outcome
  verification, and witness fingerprints. Changes need the relevant version
  and compatibility review plus focused regression tests.
- Published corpora and results are historical evidence. Preserve their bytes
  and producer provenance; use a new version/path for a new experiment.
- Evaluation publication needs a clean, VCS-stamped Linux build. Follow
  `REPRODUCIBILITY.md` and use a separate checkout for historical revisions.

## Implementation quality

Prefer small, direct implementations with clear ownership and error handling.
Use a Go 1.27+ development toolchain and modern language/standard-library
features when they simplify the solution, respecting the compatibility floor
declared in `go.mod`. Avoid abstractions, dependencies, and configuration for
hypothetical needs. Remove obsolete code/tests/docs in the affected area and
test meaningful behavior and failure boundaries. Validate the finished change;
performance or correctness claims need evidence, not stylistic complexity.

## Maintain these guides as part of every task

Updating affected agent guidance is part of completing a task; do it in the
same change without waiting for a separate documentation request.

- Before finishing, compare the relevant `AGENTS.md` guidance with the final
  implementation, adjacent tests, module files, check scripts, and owning
  contracts. Correct stale guidance encountered within the task's scope.
- Update guidance when a task changes package/file locations, entry points,
  commands, required checks, dependencies/toolchain requirements, capability or
  schema boundaries, architectural contracts, or which document owns a topic.
  Also capture a verified, reusable gotcha when it will save later agents work.
- Put each update in the nearest scoped `AGENTS.md`; update this root guide
  for repository-wide workflow or routing changes. When moving guidance to a
  scoped file, replace the root detail with a pointer instead of duplicating it.
- Replace or remove obsolete instructions rather than appending corrections.
  Link to the owning code/document for detailed or frequently changing facts;
  avoid copying version pins, schema inventories, and research status here.
- Record only verified, durable guidance. Keep task logs, speculative findings,
  temporary workarounds, host-specific paths, secrets, and dated test results
  out of these files. Before recording a lesson, check that it applies across
  supported project environments rather than only the current machine or its
  local preferences. A code/document mismatch is a problem to reconcile, not
  a reason to silently weaken an intentional contract.
- Check updated paths, links, and command syntax; run changed commands when
  practical. If guidance cannot be verified, make the uncertainty explicit
  rather than presenting it as established behavior. Mention material guide
  updates or unresolved stale guidance in the task's final handoff.

If a task changes none of these facts and reveals no reusable guidance, leave
the guides unchanged. These maintenance instructions also apply to future
edits of the guides themselves; preserve the maintenance rule when reorganizing
them and follow explicit user instructions when they change its scope.

Update the owning contract when behavior changes; keep the README an overview
and this file a short routing guide. Put task-specific findings in an explicit
review or issue, not in automatically loaded instructions. Distinguish shipped
behavior, measured evidence, and prospective claims. Current research priorities
belong in `RESEARCH_PROTOCOL.md`; dated reviews are historical snapshots.
