# Portable faults v1 / seed 1

This case is the first workload-bearing cross-adapter experiment generated
from clean commit `93389f7ff3b07c8a2aee180c3b53b6efe6b90398` with Go 1.26.6 on
Linux/amd64. The immutable `portable-faults-v1` input runs three voters for
five virtual seconds with 2% packet loss, 1–5 ms latency, 100–200 ms election
timeouts, 20 ms heartbeats, and 1 ms storage latency.

The workload submits four uniquely identified portable KV puts around these
faults:

- at 800 ms, partition `{a,b}` from `{c}`;
- at 1.4 s, heal the network;
- at 1.6 s, crash `c`;
- at 2.2 s, restart `c`; and
- after the final proposal at 2.4 s, run a 2.6-second quiet convergence tail.

Result:

- source run: completed in 1,199 steps with 2,208 exact reference decisions;
- source/reference transport: 13 drops among 875 network-loss choices;
- etcd/raft transport: 14 drops among 891 network-loss choices;
- portable plan: 2,163 variable directives;
- reference projection: exact, with all 2,163 directives projected and 45
  fixed choices;
- etcd/raft projection: partial, with 1,412 directives projected, 751
  unmatched source directives, 43 fixed choices, and 792 additional target
  choices;
- execution boundary: both reached;
- negotiated common safety findings: both empty (`agree`); and
- application: every node in both adapters commits the same four-command
  history (`chain_digest` `1d0ecb29931d1050e1cce02989bedcd8fd57516f208aeea7daa656932caf96d9`)
  and KV state (`state_digest` `5d9a0f3bb047e93e88a40108649c908f5a611948644c2859ae552e2b53a89d0e`).

The etcd/raft execution is explicitly **partial**, not exact replay. Equal
commitments at the declared boundary do not prove linearizability, protocol
equivalence, quiescence, future stability, or general Raft correctness. This
case contains real scheduled faults but no safety violation; seeded
checker/conformance counterexamples are published separately in the mutant
corpus.

Commit/toolchain fields are unsigned producer attestations bound into the
bundle's derived evidence; verification proves internal consistency and
deterministic replay, not the historical truth of those attestations. The
repository commit, checksums, and eventual signed/tagged archival release are
the external provenance anchors. See [ENVIRONMENT.md](ENVIRONMENT.md) for the
producer environment. This case is a functional witness, not a performance
trial.

## Reproduce and verify

Build from the named clean commit:

```sh
mkdir -p .research-bin
go build -buildvcs=true -trimpath \
  -ldflags=-X=main.version=93389f7 \
  -o .research-bin/draft ./cmd/draft

(cd adapters/etcdraft && \
  go build -buildvcs=true -trimpath \
    -ldflags=-X=main.version=93389f7 \
    -o ../../.research-bin/draft-cross ./cmd/draft-cross)
```

The checked-in files were generated with:

```sh
.research-bin/draft canonical \
  --seed 1 \
  --out corpus/cross-adapter/v1/portable-faults-v1-seed-1/source.run.json \
  portable-faults-v1

.research-bin/draft-cross derive \
  --source-run corpus/cross-adapter/v1/portable-faults-v1-seed-1/source.run.json \
  --fallback-seed 2 \
  --out corpus/cross-adapter/v1/portable-faults-v1-seed-1/semantic.plan.json

.research-bin/draft-cross run \
  --plan corpus/cross-adapter/v1/portable-faults-v1-seed-1/semantic.plan.json \
  --source-run corpus/cross-adapter/v1/portable-faults-v1-seed-1/source.run.json \
  --out corpus/cross-adapter/v1/portable-faults-v1-seed-1/cross
```

Verify the immutable bundle:

```sh
.research-bin/draft-cross verify \
  --plan corpus/cross-adapter/v1/portable-faults-v1-seed-1/semantic.plan.json \
  --source-run corpus/cross-adapter/v1/portable-faults-v1-seed-1/source.run.json \
  --in corpus/cross-adapter/v1/portable-faults-v1-seed-1/cross

(cd corpus/cross-adapter/v1/portable-faults-v1-seed-1 && \
  sha256sum -c SHA256SUMS)
```

`cross.manifest.json` is the bundle commit marker. `SHA256SUMS` additionally
binds the source run, portable plan, manifest, producer environment, and every
published result.
