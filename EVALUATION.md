# Bounded comparative evaluation (v1)

**Author:** Mohammadamin Khanbabaei (`aminkbi`)  
**Producer revision:** `6a685e251794ed8344342bea626e0a4a0942da2f`  
**Raw artifact:** [`evaluation/results/v1/steady-cluster-6a685e251794/result.json`](evaluation/results/v1/steady-cluster-6a685e251794/result.json)

## Claim and scope

This study characterizes d-raft's bounded evaluation harness and the
end-to-end effect of enabling its exact frontier cache. It does **not** estimate
real-bug detection probability, time to first real failure, distinct defect
yield, or diagnostic benefit. It also does not treat a random full run and a
DFS frontier probe as equal computational work merely because both consume one
runner invocation.

The pre-specified paired comparison is:

```text
bounded_dfs_frontier_cache_on - bounded_dfs_frontier_cache_off
```

Both DFS methods capture the same canonical frontier bytes. The contrast thus
includes cache lookup/retention overhead and any downstream runner or
canonicalization work avoided by exact-identity pruning; it is not a comparison
against the ordinary `draft explore --cache=false` path without frontier
capture.

## Protocol

The fixed `evaluation/steady-cluster` version `1` workload uses three reference
Raft nodes, a 300 ms virtual horizon, 1–5 ms network latency, 2% packet loss,
100–200 ms election timeouts, 20 ms heartbeats, and 1 ms storage latency. Each
method receives a ceiling of 244 runner invocations per trial. DFS uses maximum
depth 6, at most three branches per choice, and three range samples. Cache-on is
bounded to 100,000 identities and 256 MiB.

There are 21 measured trials per method and one excluded warm-up per method.
Method order rotates cyclically; because 21 is divisible by three, every method
occupies every position exactly seven times. Cache-on and cache-off are paired
by trial index. The report retains all 63 measured observations and uses
two-sided Student-t intervals over the 21 trial values or paired differences.

“Processed simulator events” is an event-attempt counter. An event that runs
until opening a semantic choice counts once, while bootstrap choices and
canonical-state construction do not. Events per second is therefore an
internal harness throughput measure, not an implementation-independent Raft
transition rate.

## Environment

| Field | Recorded value |
| --- | --- |
| CPU | 12th Gen Intel(R) Core(TM) i7-1255U |
| Logical CPUs / `GOMAXPROCS` | 12 / 12 |
| Memory visible to host | 15,937,572,864 bytes |
| Kernel | Linux 7.0.0-29-generic |
| Go / target | go1.26.6 / linux-amd64 (`GOAMD64=v1`) |
| CGO | enabled |
| Reference adapter | `d-raft/reference` version `3` |
| Decision / checker schemas | `d-raft.decisions/v1` / `d-raft.check/v3` |
| Observation / message schemas | `d-raft.observation/v3` / `d-raft.raft-message/json-v3` |
| Git state | exact revision above, `vcs.modified=false` |

The raw JSON contains the full sorted build settings. `BUILDINFO.txt` preserves
the independent `go version -m` view of the producer binary.

## Results

### Marginal descriptive summaries

| Method | Mean elapsed ms (95% t interval) | Mean processed events | Mean events/s (95% t interval) | Mean terminal / open executions |
| --- | ---: | ---: | ---: | ---: |
| Random full runs | 283.425 (275.247–291.603) | 15,758.381 | 55,814.9 (54,183.3–57,446.5) | 244 / 0 |
| DFS frontier, cache off | 788.577 (732.138–845.016) | 8,191.762 | 10,541.4 (10,063.1–11,019.7) | 120 / 124 |
| DFS frontier, cache on | 793.433 (767.084–819.782) | 8,191.762 | 10,364.2 (10,083.5–10,644.8) | 120 / 124 |

All three methods used exactly 244 runner invocations per trial. The bounded DFS
tree also ended without a run-budget-truncated flag: each trial contained 120
accepted terminal executions and 124 open-choice stops. All 5,124 random
terminal executions and all 2,520 terminal executions per DFS method completed
without a reported safety violation, operational error, or simulator-step
budget exhaustion. This bounded absence of violations does not establish
safety.

Random and DFS values above are deliberately not given a between-method
inferential contrast. Their runner calls have different semantics, and DFS also
pays canonical frontier construction costs.

### Paired cache contrast

| Paired difference: cache-on minus cache-off | Mean (95% t interval) | Approximate percentage of cache-off mean |
| --- | ---: | ---: |
| Elapsed time | +4.856 ms (−39.573 to +49.285 ms) | +0.616% (−5.018% to +6.250%) |
| Processed events/s | −177.3 (−581.0 to +226.5) | −1.681% (−5.511% to +2.148%) |

Both intervals include zero. More importantly, the cache-on trials recorded:

- 2,604 cache lookups and 2,604 misses;
- zero cache hits and zero state-pruned prefixes;
- 124 retained exact identities per trial on average;
- 1,141,299 retained bytes per trial on average; and
- zero cache-budget skips and zero hash collisions.

Therefore this is a **null exact-cache-hit result**. It characterizes the
effect of enabling lookup and retention on this non-repeating bounded tree; it does not
demonstrate pruning efficacy and cannot estimate the benefit a reconvergent
workload would receive.

### Order and drift diagnostic

Every cell below contains seven trials. Mean elapsed time varied with serial
position and across trial thirds, which is why the t intervals are treated as
conditional descriptive summaries rather than population-level guarantees.

| Method | Position 1 mean ms | Position 2 mean ms | Position 3 mean ms | First / middle / last seven mean ms |
| --- | ---: | ---: | ---: | ---: |
| Random full runs | 287.541 | 284.617 | 278.116 | 281.135 / 280.256 / 288.883 |
| DFS frontier, cache off | 810.703 | 786.554 | 768.475 | 764.069 / 833.887 / 767.775 |
| DFS frontier, cache on | 789.670 | 782.763 | 807.867 | 781.665 / 828.096 / 770.538 |

Rotation balances position counts but does not randomize away thermal effects,
background load, or autocorrelation.

## Evidence kept separate from this study

The repository contains two other bounded evidence sets that answer different
questions and are not pooled with these timing trials:

- the six-mutant corpus records three checker-backed safety kills and three
  separately classified conformance kills; this is a known-fault harness check,
  not a representative production-defect detection rate;
- the portable-fault cross-adapter corpus reaches its negotiated boundary in
  the reference and etcd/raft adapters and agrees over negotiated invariants and
  four application commitments. The etcd projection is partial, so this is not
  exact cross-implementation replay, protocol equivalence, or a linearizability
  proof.

## Reproduce and verify

Build from the recorded producer revision and keep the tree clean until the
binary is created:

```bash
git checkout 6a685e251794ed8344342bea626e0a4a0942da2f
go version  # go1.26.6
go test ./...
go vet ./...
go build -buildvcs=true \
  -ldflags=-X=main.version=6a685e251794 \
  -o /tmp/draft-eval-6a685e251794 ./cmd/draft-eval
/tmp/draft-eval-6a685e251794 --trials 21 --out /tmp/result.json
/tmp/draft-eval-6a685e251794 --verify /tmp/result.json
```

Wall-clock fields are expected to change across machines and runs. Raw search
accounting should remain deterministic for the recorded code, configuration,
and seeds. Verify the published copy with:

```bash
(cd evaluation/results/v1/steady-cluster-6a685e251794 && \
  sha256sum -c SHA256SUMS)
go run ./cmd/draft-eval --verify \
  evaluation/results/v1/steady-cluster-6a685e251794/result.json
```

## Threats to validity

- One unmutated reference-model workload cannot measure real bug finding or
  generalize to production failures.
- No repeated exact cache identity occurred, so cache pruning itself is
  unobserved.
- The reference model and checker may share mistaken assumptions despite their
  package separation.
- Serial wall-clock measurements are sensitive to the host, scheduler, power
  state, thermal state, and other workload.
- Student-t intervals summarize these 21 serial blocks; they do not by
  themselves prove independent sampling or nominal population coverage.
- Event attempts omit canonicalization and are not matched implementation work
  across random and DFS methods.
- Bounded execution cannot establish unbounded safety or liveness.

The positioning against prior systems and the limits of d-raft's novelty claim
are recorded in [`RELATED_WORK.md`](RELATED_WORK.md).
