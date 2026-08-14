# d-raft seeded mutant corpus v1

This corpus contains six reviewed, synthetic faults against reference commit
`9304bcd0ba7f5cc470ac67149bb9b96bc1be6523`. Each fault is a standalone patch;
the reference implementation and checker contain no runtime mutation switches.

| Mutant | Fault | Expected classification | Expected property |
| --- | --- | --- | --- |
| `duplicate-vote-count` | Count one granted response twice in a five-voter election | Safety kill | `raft/election-certificate` |
| `ignored-higher-term` | Ignore a higher-term vote request while leader | Conformance kill | `raft/higher-term-stepdown` |
| `non-persisted-vote` | Release a granted vote response before its write is acknowledged | Safety kill | `raft/election-certificate` |
| `snapshot-before-persistence` | Install snapshot state before its durable write is acknowledged | Conformance kill | `raft/snapshot-persistence-order` |
| `stale-heartbeat-acceptance` | Accept a stale-term heartbeat and reset the election timer | Conformance kill | `raft/stale-heartbeat-rejection` |
| `unsafe-one-step-reconfiguration` | Replace a stable voter set without entering joint consensus | Safety kill | `raft/membership-transition` |

`activation.patch` adds one external `raft_test` probe file. A clean baseline
must pass each exact probe. Every targeted safety probe requires a real
package-separated checker witness; the three transition-level assertions are
explicitly classified as conformance-only. The probes also require the exact
returned effects and pending persistence boundary, so a compound or unrelated
failure is not silently credited to the expected mutant.

Run from the repository root:

```sh
mkdir -p .research-bin
go build -buildvcs=true -o .research-bin/draft-mutants ./cmd/draft-mutants
.research-bin/draft-mutants \
  -repo . \
  -manifest corpus/mutants/v1/corpus.json \
  -out corpus/mutants/v1/results/linux-amd64.json
```

The manifest and every patch are SHA-256 pinned. See
[`../../../MUTANTS.md`](../../../MUTANTS.md) for runner semantics, result
classification, and the trust boundary.

The checked-in [`results/linux-amd64.json`](results/linux-amd64.json) was
produced from a clean checkout with the clean committed runner under Go 1.26.6.
It records three safety kills and three conformance kills.

This is a deliberately seeded fault set used to test activation and attribution.
It is not sampled from production incidents and must not be presented as a
representative mutation score for Raft implementations in general.
