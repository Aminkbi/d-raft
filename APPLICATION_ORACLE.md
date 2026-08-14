# Portable application oracle

d-raft's portable application profile is a deliberately small deterministic
key/value state machine. It gives independent adapters a common comparison
surface without requiring equal Raft terms, indexes, leaders, no-op entries,
message batches, timings, or simulator steps.

The profile is explicit. A nil application configuration preserves the legacy
behavior in which proposal and snapshot bytes are opaque. Adapters never infer
the profile from a magic prefix.

## Versioned formats

| Purpose | Schema |
| --- | --- |
| Command | `d-raft.kv-command/v1` |
| State digest | `d-raft.kv-state/v1` |
| History chain | `d-raft.kv-chain/v1` |
| Checkpoint | `d-raft.kv-checkpoint/v1` |
| Commitment | `d-raft.kv-commitment/v1` |

A command has one nonzero 128-bit client ID, a binary nonempty key, and either
`put` or `delete`. Put permits an empty value; delete requires an empty value.
Duplicate IDs fail closed. Client retry/idempotence is outside v1.

The unique command encoding is:

```text
"DRAFTKV1" |
operation:u8 |
command_id:[16]byte |
key_length:u32be |
value_length:u32be |
key |
value
```

Operation 1 is put and operation 2 is delete. Decoding rejects truncation,
trailing bytes, unknown operations, an all-zero ID, empty or oversized keys,
oversized values, and nonempty delete values.

## Commitments

State is hashed independently of Go map iteration:

```text
SHA256(
  "d-raft.kv-state/v1\0" |
  entry_count:u64be |
  sorted(key_length:u32be | key | value_length:u32be | value)*
)
```

The genesis history head is:

```text
H0 = SHA256("d-raft.kv-chain/v1\0")
```

Each applied command advances it:

```text
Hi = SHA256(
  "d-raft.kv-chain/v1\0" |
  H(i-1) |
  command_count:u64be |
  command_length:u32be |
  canonical_command |
  post_state_digest
)
```

The compact comparison value is `{schema, commands, chain_digest,
state_digest}`. Raft term, index, node, leader, membership, internal no-op, and
configuration entries never enter these hashes. No-op and configuration
entries are ignored; unknown entry types fail closed.

Equal commitments are evidence that the encoded bounded command history and KV
state agree under the SHA-256 assumption. They are not proof of linearizability,
Raft safety, application correctness, or unbounded execution equivalence.

## Checkpoints and limits

Checkpoints contain canonical sorted state and the complete ordered block
history. Restore replays every command from genesis, reconstructs duplicate-ID
state, and verifies each state digest and chain link before returning a machine.
This makes v1 independently checkable but gives it O(history) snapshot size.

Canonical checkpoint JSON rejects unknown fields, whitespace variants, trailing
values, `null` state/history/value arrays, unsorted or duplicate keys, altered
digests, and altered commands.

The JSON field order is fixed as `schema`, `commands`, `chain_digest`,
`state_digest`, `state`, `history`. A block uses `ordinal`, `command_id`,
`command`, `command_digest`, `state_digest`, `digest`; a state pair uses `key`,
`value`. Full-width counters are canonical decimal JSON strings. Byte strings
use Go/JSON's padded RFC 4648 base64 encoding. The exact empty and one-command
checkpoint documents are pinned as known-answer constants in
`apporacle/apporacle_test.go`; decoding requires byte-for-byte equality with
the canonical re-encoding.

Current bounds are:

- 1 MiB per command;
- 64 KiB per key;
- 50,000 commands;
- 12 MiB of retained canonical command bytes;
- 8 MiB of current key/value state; and
- 64 MiB per encoded checkpoint.

Aggregate limits are checked before application mutation and before checkpoint
replay.

## Adapter integration

`raftsim.Config.Application` and `etcdraft.Config.Application` opt into the
profile with `apporacle.KVConfig()`. Both adapters validate proposal bytes
before protocol submission, apply commands transactionally before advancing
their applied index, and publish `ApplicationCommitment` accessors.

The reference adapter can generate a checkpoint-bearing Raft snapshot through
`SnapshotApplication`. Opaque snapshots are rejected while the profile is
enabled. Snapshot installation validates the complete checkpoint before
changing the applied index, and crash-after-snapshot-persistence recovery
restores it during node startup. Canonical frontier caching is deliberately
unsupported for this first application profile.

On restart, the reference adapter reconstructs application state from a
persisted portable checkpoint plus its durable applied suffix. The etcd/raft
adapter reconstructs it from its retained applied-entry history. Crash clears
the live machine pointer in both adapters, so recovery tests do not rely on
accidental in-memory survival. The etcd/raft adapter still rejects snapshots
because its declared adapter capability does not include them.

`experiment.ExecuteWithApplication` and `etcdraft.ExecuteWithApplication`
prevalidate all proposal encodings and command-ID uniqueness before cluster
construction or semantic decision consumption. Existing `Execute` functions
remain opaque-command paths.

## Fixed known-answer vectors

The test suite pins the command bytes and SHA-256 results. Two core vectors are:

```text
put x=1 command:
44524146544b563101000102030405060708090a0b0c0d0e0f00000001000000017831

command digest:
a4af80c9764356340696c115937255fd4157e4900d57859758706e8e79f8d62a

state {x:1}:
5afa47ab2fcf92ba11bc6cb680aee8049d589f848614af90bbcf34f9bc1b4c00

genesis chain:
1b62430e166c36ce68764959cd66890a6d2b2be80f1c1555ce851f7825b795b3

chain after put:
a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd
```

Fixed vectors and strict restore tests reduce, but do not eliminate, the
common-mode risk of both adapters using the same oracle package. The
standard-library Python verifier in `tools/verify_apporacle.py` independently
decodes the canonical format and recomputes every transition and digest without
importing the Go implementation. Run `python3 tools/verify_apporacle.py
--self-test`, or pass a canonical checkpoint file to print its verified compact
commitment. The public corpus will include these independently verifiable
checkpoints and checksums.
