# Loss 2% / seed 20260814

This case is a deterministic cross-adapter control generated from clean commit
`781e6546a9793b58c2960ec67743a4875cc454dc` with Go 1.26.6 on Linux/amd64.
It runs a three-voter cluster for two seconds with 1–5 ms network latency,
2% packet loss, 150–300 ms election timeouts, 50 ms heartbeats, 1 ms storage
latency, and no external workload actions.

Result:

- source run: completed, 369 exact reference decisions;
- portable plan: 360 variable directives;
- reference projection: exact, with 360 projected and 9 fixed choices;
- etcd/raft projection: partial, with all 360 directives projected, 9 fixed
  choices, 10 additional target choices, and no unmatched source directive;
- execution boundary: both reached;
- negotiated common safety findings: both empty (`agree`);
- portable application commitments: every node agrees on the empty history and
  state in both adapters (`agree`).

The last point is intentionally only an empty-workload control. It is not
evidence of application behavior, quiescence, linearizability, protocol
equivalence, or general Raft correctness.

## Reproduce and verify

Build from the named clean commit:

```sh
mkdir -p .research-bin
go build -buildvcs=true -trimpath \
  -ldflags=-X=main.version=781e654 \
  -o .research-bin/draft ./cmd/draft

(cd adapters/etcdraft && \
  go build -buildvcs=true -trimpath \
    -ldflags=-X=main.version=781e654 \
    -o ../../.research-bin/draft-cross ./cmd/draft-cross)
```

The checked-in files were generated with:

```sh
.research-bin/draft run \
  --duration 2s --seed 20260814 --loss 0.02 \
  --out corpus/cross-adapter/v1/loss-2pct-seed-20260814/source.run.json

.research-bin/draft-cross derive \
  --source-run corpus/cross-adapter/v1/loss-2pct-seed-20260814/source.run.json \
  --fallback-seed 20260815 \
  --out corpus/cross-adapter/v1/loss-2pct-seed-20260814/semantic.plan.json

.research-bin/draft-cross run \
  --plan corpus/cross-adapter/v1/loss-2pct-seed-20260814/semantic.plan.json \
  --source-run corpus/cross-adapter/v1/loss-2pct-seed-20260814/source.run.json \
  --out corpus/cross-adapter/v1/loss-2pct-seed-20260814/cross
```

Verify the immutable bundle:

```sh
.research-bin/draft-cross verify \
  --plan corpus/cross-adapter/v1/loss-2pct-seed-20260814/semantic.plan.json \
  --source-run corpus/cross-adapter/v1/loss-2pct-seed-20260814/source.run.json \
  --in corpus/cross-adapter/v1/loss-2pct-seed-20260814/cross

sha256sum -c \
  corpus/cross-adapter/v1/loss-2pct-seed-20260814/SHA256SUMS
```

`cross.manifest.json` is the bundle commit marker. `SHA256SUMS` additionally
binds the source run, portable plan, and manifest itself.
