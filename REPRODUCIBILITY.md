# Reproducibility and archival guide

**Maintainer:** Mohammadamin Khanbabaei (`aminkbi`)

This guide separates deterministic protocol evidence from host-dependent
performance observations and defines the verification path for the v0.1
research release.

## Toolchains and modules

- Root module: Go 1.26, toolchain Go 1.26.6, no third-party runtime dependency.
- Nested production adapter: Go 1.26.6, `go.etcd.io/raft/v3` v3.7.0 and
  `google.golang.org/protobuf` v1.36.11, pinned by `go.mod` and `go.sum`.
- Supported evaluation-publication platform: Linux. Artifact decoding and the
  rest of the root library are not restricted to Linux.

Start from a tagged checkout with a clean working tree:

```bash
git status --porcelain=v1
go version
go env GOOS GOARCH
```

The first command must print nothing when producing a publication-stamped
evaluation binary.

## Verification tiers

Run each command block below from the repository root; directory changes in
one block do not carry into the next.

### Root deterministic implementation

```bash
gofmt -d .
go test ./...
go vet ./...
go test -race ./...
```

### etcd/raft production-core adapter

```bash
cd adapters/etcdraft
go test ./...
go test -count=20 ./...
go vet ./...
go test -race ./...
```

### Independently specified application vectors

```bash
python3 tools/verify_apporacle.py --self-test
```

### Published cross-adapter cases

```bash
cd adapters/etcdraft
go run ./cmd/draft-cross verify \
  --plan ../../corpus/cross-adapter/v1/loss-2pct-seed-20260814/semantic.plan.json \
  --source-run ../../corpus/cross-adapter/v1/loss-2pct-seed-20260814/source.run.json \
  --in ../../corpus/cross-adapter/v1/loss-2pct-seed-20260814/cross
go run ./cmd/draft-cross verify \
  --plan ../../corpus/cross-adapter/v1/portable-faults-v1-seed-1/semantic.plan.json \
  --source-run ../../corpus/cross-adapter/v1/portable-faults-v1-seed-1/source.run.json \
  --in ../../corpus/cross-adapter/v1/portable-faults-v1-seed-1/cross

cd ../../corpus/cross-adapter/v1/loss-2pct-seed-20260814
sha256sum -c SHA256SUMS
cd ../portable-faults-v1-seed-1
sha256sum -c SHA256SUMS
```

### Published bounded evaluation

```bash
cd evaluation/results/v1/steady-cluster-6a685e251794
sha256sum -c SHA256SUMS
env GOCACHE=/tmp/draft-go-cache \
  go run ../../../../cmd/draft-eval --verify result.json
```

`--verify` uses the environment embedded in the result. It does not require the
verifier's checkout to be the producer commit and does not mistake the
verifier's current Git state for the producer's provenance.

## Regenerating the bounded evaluation

Use the exact producer revision rather than the later report commit:

```bash
git checkout 6a685e251794ed8344342bea626e0a4a0942da2f
test -z "$(git status --porcelain=v1)"
go build -buildvcs=true \
  -ldflags=-X=main.version=6a685e251794 \
  -o /tmp/draft-eval-6a685e251794 ./cmd/draft-eval
go version -m /tmp/draft-eval-6a685e251794
/tmp/draft-eval-6a685e251794 --trials 21 --out /tmp/result.json
/tmp/draft-eval-6a685e251794 --verify /tmp/result.json
```

The producer refuses an unknown, malformed, or dirty Git revision before the
study begins. It also preflights the no-clobber output path, `0600` mode,
hard-link support, and directory synchronization. Successful publication uses
file sync, a same-directory hard link, directory sync, staging unlink, and a
second directory sync.

Search-accounting fields are expected to reproduce for the same code,
configuration, and seeds. Elapsed time, event-attempt throughput, CPU model,
memory, kernel, and build settings are observations of the producer machine and
will legitimately differ elsewhere.

## Artifact trust boundaries

- A valid run artifact proves that it is internally consistent with its schema
  and checker contract; it does not prove that the model or checker is correct.
- Exact reference or adapter-local replay is stronger than a successful
  adapter-neutral projection. Partial projection is never described as exact
  replay.
- Application state/history commitments compare the bounded portable KV
  profile. They do not establish linearizability or general protocol
  equivalence.
- Mutant kills are known-fault harness evidence, not a representative estimate
  of production defect detection.
- Zero violations in a bounded unmutated reference-model execution do not
  establish safety.

## Network and caches

Verification uses no network after Go dependencies are present. On a fresh
machine, standard `GOPROXY`, `HTTP_PROXY`, and `HTTPS_PROXY` settings may be used
to populate the module cache. Dependency versions and checksums remain governed
by the committed module files. Build caches may be redirected with `GOCACHE`;
they are not part of any artifact identity.

## Release archive

The release attaches a source archive produced deterministically from the
release tag and a separate SHA-256 checksum file:

```bash
git archive --format=tar --prefix=d-raft-v0.1.0/ v0.1.0 | \
  gzip -n > d-raft-v0.1.0.tar.gz
sha256sum d-raft-v0.1.0.tar.gz > d-raft-v0.1.0.tar.gz.sha256
```

Because `git archive` records commit timestamps and `gzip -n` omits gzip
timestamps and original names, this recipe is byte-reproducible for the tag.
A verifier should:

1. verify the archive checksum;
2. extract into a new directory;
3. confirm the archived commit/tag documented in the release notes;
4. run the relevant verification tier above; and
5. retain the raw JSON and checksum manifests unchanged when citing results.

GitHub-generated source archives are convenient mirrors, but the attached
checksummed archive is the canonical v0.1 source snapshot.
