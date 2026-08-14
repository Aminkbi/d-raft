# Seeded mutant evaluation

d-raft evaluates whether a fixed deterministic probe detects deliberately
seeded faults in the reference Raft implementation. The mutant runner is a
research harness, not a general-purpose command executor or a hostile-code
sandbox.

## Reproducibility contract

Corpus manifests use `d-raft.mutant/v1`; result documents use
`d-raft.mutant-result/v1`. A manifest pins:

- the exact 40-character Git commit used as the clean baseline;
- SHA-256 digests for the activation and mutation patches;
- one closed package and exact Go test name; and
- the expected invariant ID and whether it is a safety or conformance claim.

Entries must be sorted by ID. Unknown JSON fields, trailing JSON values,
unbounded collections, path traversal, symlinks, checksum mismatches, binary
patches, whitespace errors, and unsupported patch operations are rejected.
The manifest contains no commands, flags, environment variables, budgets, or
shell fragments.

The activation patch may only add the fixed
`<package>/mutant_activation_test.go` file. Mutation patches may modify only
the allowlisted production files `raft/node.go`, `raft/types.go`, and
`raft/membership.go`; they cannot change tests, checkers, module metadata,
file modes, or the file set. The runner checks this policy both before and
after applying each patch.

For every mutant the runner:

1. creates a fresh detached worktree at the pinned baseline;
2. applies the activation patch and runs the exact baseline test;
3. requires the test to emit `DRAFT_MUTANT_ACTIVATED:<id>`;
4. applies one mutation patch and runs the same test again;
5. attributes a targeted detection only when the exact
   `DRAFT_MUTANT_INVARIANT:<invariant-id>` marker is present; and
6. removes the registered worktree and its private runtime directories.

The Go subprocess receives a private `HOME`, `TMPDIR`, and `GOCACHE`, with
the exact runner toolchain selected through `runtime.GOROOT`, workspace and
module behavior fixed locally, and proxy, credential, and common secret
variables removed. Commands are invoked directly without a shell. Build
failures, panics, timeouts, malformed patches, and infrastructure errors are
never counted as safety kills.

## Running a corpus

```sh
mkdir -p .research-bin
go build -buildvcs=true -o .research-bin/draft-mutants ./cmd/draft-mutants
.research-bin/draft-mutants \
  -repo . \
  -manifest corpus/mutants/v1/corpus.json \
  -out corpus/mutants/v1/results/linux-amd64.json
```

The explicit build is part of the evidence contract: it embeds the executable
Git revision and dirty bit. `go run` does not preserve that VCS information on
all supported Go invocations and is therefore rejected by the runner.

Output files are published atomically and never overwrite an existing result.
Omit `-out` (or use `-out -`) for standard output. Exit status `0` means every
entry completed with a detection or survival result, `1` means at least one
entry was not evaluable, and `2` is a manifest, runner, or output error.

Each result carries the canonical manifest digest, base commit and tree,
activation and mutation digests, executable and target-checkout provenance, Go
version, GOOS, and GOARCH. Baseline and mutant output are bounded to 64 KiB
apiece.

## Classification

| Classification | Meaning |
| --- | --- |
| `safety_kill` | The activated test failed with the declared safety-invariant marker. |
| `conformance_kill` | The activated test failed with the declared conformance marker. |
| `non_safety_detection` | The activated test failed without the expected marker. |
| `survived` | The activated test passed within the fixed probe. |
| `not_activated` | The exact activation marker was absent. |
| `baseline_failed` | The clean baseline probe failed. |
| `ineligible` | Reserved for a future declared capability mismatch. |
| `operational_error` | Patch, build, panic, timeout, or runner failure. |

Report each category separately. A survivor is only “not killed by this fixed
bounded probe”; it is not evidence that the mutant is correct. Conformance
kills are not safety-checker findings. Seeded faults are controlled test cases,
not a representative distribution of production Raft bugs.

## Trust boundary

Detached worktrees protect the caller's checkout from ordinary patch changes,
but the patched Go source still executes with the caller's operating-system
identity. Only run reviewed, trusted corpora. The environment isolation and
closed command vocabulary reduce accidental exposure; they do not provide a
kernel security boundary.
