# etcd/raft production-core adapter

This nested module is d-raft's **experimental production-core adapter** for the
unmodified [`go.etcd.io/raft/v3`](https://pkg.go.dev/go.etcd.io/raft/v3)
`RawNode` core. It pins upstream **v3.7.0** and adapter schema **1**.

It is not an etcd server integration. d-raft supplies virtual semantic timers,
a simulated network, atomic in-memory storage, faults, process lifecycle, and a
single-threaded event loop. Consequently, this adapter does not exercise etcd
server code, real disks or networks, goroutine interleavings, data races, torn
writes, corruption, or operator behavior.

## Pinned provenance

| Item | Pin |
| --- | --- |
| Module | `go.etcd.io/raft/v3 v3.7.0` |
| Upstream commit | `b867cf13f6bc0dae21204302df97bc2355c3af55` |
| Module checksum | `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=` |
| Protobuf runtime | `google.golang.org/protobuf v1.36.11` |
| Adapter identity | `go.etcd.io/raft/v3@3.7.0+d-raft.1` |

## Capability boundary

| Capability | Schema 1 |
| --- | --- |
| Fixed, all-voter membership | supported |
| Non-empty proposals | supported |
| Latency, loss, and partitions | supported |
| Crash and restart | supported |
| Crash after durable write, before acknowledgement | supported |
| Durable/application common safety checks | supported |
| Portable KV state/history commitment | opt-in |
| Adapter-local exact decision replay | supported |
| Snapshots and compaction | rejected |
| Learners or membership changes | rejected |
| Empty command payloads | rejected |
| `PreVote`, `CheckQuorum`, lease reads, transfer | rejected |
| Asynchronous storage writes | rejected |
| Canonical exploration cache | unsupported |
| Portable semantic-plan projection | supported with explicit exact/partial/failed accounting |
| Cross-adapter exact decision tapes | unsupported; successful targets record local exact tapes and failures record exact successful prefixes |

Unsupported scenarios are rejected before cluster construction and before any
semantic decision is consumed. `SupportedCapabilities()` is the machine-readable
summary; the table above also records intentional configuration restrictions.

## Determinism and timer policy

etcd/raft v3.7.0 uses process-global `crypto/rand` for its internal randomized
election timeout and does not expose an injectable RNG. The adapter therefore
does not tick followers or candidates. A d-raft semantic election timer invokes
`RawNode.Campaign()`; only leaders receive `Tick()` calls, with
`HeartbeatTick=1`. Expired campaign and heartbeat inputs are coalesced while a
`Ready` barrier is active.

Every synchronous `Ready` follows this modeled sequence:

```text
RawNode.Ready
  -> virtual storage delay
  -> persist Snapshot / HardState / Entries
  -> optional crash-after-write
  -> zero-delay acknowledgement
  -> apply CommittedEntries / send Messages / RawNode.Advance
```

The barrier remains active through acknowledgement. A crash before the write
loses it; a crash after the write retains `MemoryStorage` but discards the
accepted `Ready` and volatile `RawNode`. Restart constructs a new `RawNode` from
the retained storage and last applied index.

The initial fixed membership is encoded as a synthetic snapshot at index 1,
term 1, with commit/applied index 1. The adapter-side chain represents that
genesis boundary as a no-op block. This convention avoids upstream bootstrap
configuration entries and is part of observation schema
`d-raft.etcdraft-observation/v1`.

## Checking and observations

Checker profile `d-raft.etcdraft-check/common-durable-v1` intentionally passes
only normalized durable and applied state to the general checker. It checks
durable term/vote history, log matching, committed/applied conflicts and
monotonicity, and snapshot/membership history. It does **not** claim election
certificates, volatile/durable equality, election safety, or leader completeness
from incomplete public `RawNode` evidence.

`Cluster.Observation`, `DurableState`, `Status`, `AppliedEntries`,
`ChainBlocks`, and `ApplicationCommitment` return deep-copied or immutable
adapter-normalized views. The adapter-specific outcome
digest additionally commits to the pending `Ready`, its persisted/ack phase,
and queued inputs. Public RawNode state is not Markov-complete, so the canonical
reference-state cache remains disabled for this adapter.

The existing Chain-of-Blocks is an adapter-local commitment to ordered applied
protocol-entry history. It includes Raft indexes, terms, types, and internal
no-ops. The separate opt-in `d-raft.kv-command/v1` profile omits those protocol
details and publishes `d-raft.kv-commitment/v1` state/history commitments. It
is specified in the root [application-oracle document](../../APPLICATION_ORACLE.md).
Neither commitment is proof of Raft safety, linearizability, or application
correctness.

## Exact replay and portability

Exact tapes are local to this adapter and upstream version. Network choice
contexts contain deterministic protobuf bytes and sender-local sequence
numbers, so a reference-adapter tape is not directly portable here. The
`d-raft.semantic-plan/v1` projection instead uses node/incarnation/generation
or endpoint/incarnation/send-sequence keys and reports unmatched source and
additional target choices. Cross-adapter evaluation compares negotiated common
invariant IDs and normalized application outcomes,
not identical leaders, terms, timings, message counts, steps, or observation
digests. Current checker fingerprints are adapter-local because their canonical
evidence can include implementation-specific terms and indexes.

See the root [portable semantic-plan contract](../../SEMANTIC_PLANS.md) for
strict schemas, eligibility rules, source provenance, and comparison claims.

## Use

From this directory:

```bash
go test ./...
go test -race ./...
go vet ./...

go build -o draft-etcd ./cmd/draft-etcd
./draft-etcd run --seed 42 --duration 2s --out run.json
./draft-etcd replay run.json

go build -o draft-cross ./cmd/draft-cross
./draft-cross derive --source-run source-run.json --out semantic-plan.json
./draft-cross run --plan semantic-plan.json --source-run source-run.json --out cross-result
./draft-cross verify --plan semantic-plan.json --source-run source-run.json --in cross-result
```

The cross runner replay-verifies the source before deriving or running. A run
publishes seven private result files and a `cross-result.manifest.json` commit
marker last. `verify` checks every exact-byte hash and binding, exactly replays
successful adapter-local tapes, validates failed projections as exact prefixes
plus deterministic re-execution, and recomputes both normalized outcomes and
the final comparison. If a crash leaves data files without the manifest,
inspect the prefix and use `./draft-cross recover --in cross-result`; recovery
refuses a committed bundle.

Build the binary from a Git checkout before publishing artifacts so Go embeds
the VCS revision and dirty-tree flag. `go run` from the nested module can report
an unknown revision and is therefore unsuitable for research artifacts.

The nested `go.mod` keeps the production dependency out of the dependency-free
reference module. Its local `replace ../..` means this module is developed and
tested as part of the repository; it is not published as a standalone module.
