# Snapshots and log compaction

d-raft models a snapshot as an atomic checkpoint containing a last-included
log index and term, the membership at that boundary, and opaque application
bytes. A compacted log stores only the contiguous suffix after that boundary.

## Durability protocol

Local checkpoint creation and remote installation use the same persistence
barrier as votes and log entries:

```text
InputSnapshot(index, data) or InstallSnapshot RPC
  -> EffectPersist(token, snapshot + suffix + hard state)
  -> durable completion
  -> InputPersisted(token)
  -> application installation and acknowledgement effects
```

The harness binds local checkpoint bytes to its application `AppliedIndex` at
the instant `Cluster.Snapshot` is called. If another write is pending, the
request may execute later but retains that explicit target and compacts only
through it. A crash before durable completion leaves the old state intact. A
crash after completion but before acknowledgement is recovered by `Node.Start`,
which re-emits the application installation before arming the election timer.

A leader sends `InstallSnapshot` when a follower's next index is at or before
the leader's compacted boundary. A follower retains a suffix only when its
entry at the boundary has the snapshot's term. It persists the snapshot, new
commit index, and retained suffix atomically before installing application
bytes or replying. The harness retains the installed application checkpoint
separately from Raft's durable protocol state.

All index arithmetic is checked through `MaxUint64`; malformed append batches
are validated in full before any log mutation. Snapshot and suffix terms must
be possible under the durable current term, and unknown entry types are
rejected.

## Artifact and checker boundary

Run v2 adds the scheduled `snapshot` action and uses the coherent reference-v2,
message-codec-v2, checker-v2, and observation-v2 tuple. Snapshot members, bytes,
protocol state, durable state, and installed application state are deep-cloned
and affect the final observation digest. Published run v1 remains a separate,
snapshot-free compatibility surface.

The independent checker detects different term, membership, or bytes for two
snapshots at the same index. It also checks all still-visible suffix history.
Application bytes are intentionally opaque, so a later snapshot alone cannot
prove which command occupied every earlier compacted index. Leader completeness
below an opaque boundary is therefore not claimed. The planned Chain-of-Blocks
oracle will add a canonical state commitment suitable for stronger
cross-boundary and cross-adapter checks.

## Current limitations

- Membership is static; joint consensus and learners are the next protocol
  milestone.
- Snapshots are whole and atomic. There is no chunked transfer, resume, torn
  write, corruption, or filesystem model.
- The direct in-process Go API trusts snapshot byte slices supplied by the
  caller. Run artifacts cap each action payload and the aggregate encoded
  artifact, but the harness does not impose an independent byte limit.
- The simulator is single-threaded and cannot expose production data races.
