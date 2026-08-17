# Steady-cluster bounded evaluation

This directory preserves the first published `d-raft.evaluation/v1` study.
The immutable raw trial observations are in `result.json`; derived prose
belongs in the repository-level
[`EVALUATION.md`](../../../../EVALUATION.md).

## Identity

- Producer commit: `6a685e251794ed8344342bea626e0a4a0942da2f`
- Producer toolchain: Go 1.26.6
- Scenario: `evaluation/steady-cluster` version `1`
- Adapter: `d-raft/reference` version `3`
- Trials: 21 measured trials per method, plus one excluded warm-up per method
- Budget: 244 runner invocations per method and trial
- Release study date: 2026-08-17; producer platform: Linux/amd64

The producer was built from a clean checkout:

```bash
go build -buildvcs=true \
  -ldflags=-X=main.version=6a685e251794 \
  -o /tmp/draft-eval-6a685e251794 ./cmd/draft-eval

/tmp/draft-eval-6a685e251794 \
  --trials 21 \
  --out /tmp/d-raft-evaluation-6a685e251794.json
```

## Verification

From a checkout containing `draft-eval`:

```bash
(cd evaluation/results/v1/steady-cluster-6a685e251794 && \
  sha256sum -c SHA256SUMS)
go run ./cmd/draft-eval --verify \
  evaluation/results/v1/steady-cluster-6a685e251794/result.json
```

Successful verification recomputes all summaries and paired contrasts from the
raw trials, checks counter identities and resource bounds, rejects unknown JSON
fields and duplicate keys, and requires a clean 40-character Git revision with
matching embedded VCS settings.

## Result boundary

The two DFS methods processed identical runner invocations and simulator event
attempts. Cache-on observed 2,604 misses, zero hits, and zero state-pruned
prefixes across all trials. Therefore this case measures the overhead/null-hit
effect of enabling the exact-identity cache; it does not measure pruning
efficacy. Random full runs and DFS frontier probes have different invocation
semantics, so their marginal wall times and event rates are descriptive and are
not an equal-work comparison. Zero violating runs in this bounded unmutated
reference model do not establish Raft safety.
