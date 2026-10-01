# Working on the etcd/raft adapter

This directory is a separate Go module using the root module through
`replace ../..`. Run its checks here even when root tests pass. The root
`AGENTS.md` still applies.

## Entry points

- `cluster.go`: RawNode lifecycle, virtual timers, durable storage, Ready barrier.
- `execute.go`, `semantic.go`, `decisions.go`: scenario execution and projection.
- `observe.go`, `oracle.go`: normalized observations and application commitments.
- `cmd/draft-etcd/`: adapter-local run/replay.
- `cmd/draft-cross/`: plan derivation, bundle publication, verification/recovery.
- `README.md`: upstream pin, supported capabilities, timer policy, checker scope.

## Adapter contracts

- Preserve the declared fixed-membership capability subset. Snapshots, learners,
  membership changes, and reference canonical caching are unsupported here.
  Reject unsupported input before construction or decision consumption.
- Followers/candidates use semantic timers and `Campaign()`. Only leaders are
  ticked; changing this can invoke upstream process-global election randomness.
- Ready processing persists before acknowledgement, apply, send, and Advance.
  Keep crash-after-write-before-ack recovery distinct from crash-before-write.
- Check only invariants supported by exposed durable/applied evidence; the
  common checker profile does not prove every reference-model invariant.
- Exact tapes are adapter/version local. Cross-adapter comparison uses semantic
  projection, negotiated capabilities, and normalized evidence.
- Bundle publication commits its manifest last. Preserve private, no-clobber
  writes and recovery behavior; read `SEMANTIC_PLANS.md` before changing it.

## Checks

From this directory, use `go test ./...` and `go vet ./...`; changes to timer,
Ready, observation, or decision behavior also warrant `go test -count=20 ./...`.
From the repository root, `bash tools/check.sh evidence` verifies both published
cross-adapter cases with their source provenance and checksums. Keep historical
case bytes and upstream pins unchanged unless the task explicitly updates them.

## Maintain this scoped guide

Apply the root guide's maintenance rule before completing adapter tasks. Update
this file in the same change when entry points, module/check commands, supported
capabilities, timer/Ready policy, observation/checker boundaries, or bundle
publication behavior change. Verify guidance against the implementation, tests,
`go.mod`, and adapter `README.md`; keep version pins in their owning sources.
Update the root routing table or shared check guidance if the change affects
them. Replace obsolete guidance and keep adapter-specific details here, linking
to the owning contracts instead of growing a second specification.
