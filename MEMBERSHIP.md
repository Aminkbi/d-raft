# Joint consensus and learners

d-raft models membership changes over a fixed, pre-provisioned universe of
processes. `Config.Members` names every node that may participate during the
run. `Config.Voters` and `Config.Learners` define the initial roles; when both
are empty, every member is initially a voter for backward compatibility.

Provisioning and consensus membership are intentionally separate. Removing a
voter makes its process dormant for elections, but does not delete its durable
store, transport endpoint, or process identity. This keeps crash/restart and
artifact replay deterministic while still exercising Raft's quorum changes.

## Stable and joint configurations

A stable configuration contains one non-empty voter set and an optional,
disjoint learner set. A membership change is encoded as two replicated log
entries:

| Field | Meaning |
| --- | --- |
| `Voters` | Incoming voters during joint consensus, or current voters when stable |
| `VotersOutgoing` | Old voters; non-empty only while joint |
| `Learners` | Active joint learners, excluding demoted outgoing voters |
| `LearnersNext` | Desired learner set after finalization |

1. `EntryConfigJoint` activates an incoming voter set alongside the outgoing
   voters. Elections and commits in this state require a majority of both sets.
2. `EntryConfigFinal` removes the outgoing set and activates the final learner
   roles. Entries governed by this configuration require only the incoming
   stable majority.

Each node derives its local active configuration from its log tail rather than
waiting for application. The leader evaluates each candidate commit index
against the membership represented at that index. A joint entry therefore needs
both old and new majorities; a final entry uses the incoming stable majority. A
leader excluded by an appended final entry continues leading until that entry
commits, then steps down.

The harness exposes `BeginMembershipChange` and `FinalizeMembershipChange`,
plus variants that target a specific node. Run-v3 artifacts represent the same
operations as `begin_membership` and `finalize_membership` actions.

```go
config := raftsim.DefaultConfig("a", "b", "c", "d")
config.Voters = []raft.NodeID{"a", "b", "c"}
config.Learners = []raft.NodeID{"d"}

cluster, err := raftsim.New(config)
if err != nil {
    log.Fatal(err)
}
if _, err := cluster.RunUntil(2 * time.Second); err != nil {
    log.Fatal(err)
}
if err := cluster.BeginMembershipChange(
    []raft.NodeID{"b", "c", "d"},
    []raft.NodeID{"a"},
); err != nil {
    log.Fatal(err)
}
// Run until the joint entry and all earlier entries commit.
if _, err := cluster.RunUntil(cluster.Simulator().Now() + time.Second); err != nil {
    log.Fatal(err)
}
if err := cluster.FinalizeMembershipChange(); err != nil {
    log.Fatal(err)
}
```

The equivalent scheduled actions carry the target role sets on
`begin_membership`; `finalize_membership` carries no target sets. Scheduling
finalization too early yields an error outcome rather than waiting or retrying.

## Promotion and demotion

A learner receives replicated log entries and snapshots but never campaigns,
votes, or contributes to a quorum. Promotion places the learner in the incoming
voter set of the joint entry, where it immediately participates in the new
majority.

A voter being demoted remains an outgoing voter throughout joint consensus and
does not simultaneously act as a learner. It moves into the stable learner set
only when the final entry takes effect. A leader removed from the voter set
steps down when the final configuration commits.

Only one configuration change may be in progress. Normal proposals are blocked
while a configuration entry is uncommitted. Finalization additionally requires
the joint configuration to be committed and the leader's commit index to equal
its last log index. This prevents a final entry committed by the new majority
from carrying intervening commands past the joint quorum requirement.

## Durability, truncation, and snapshots

Membership is reconstructed from the initial roles, the durable snapshot, and
the contiguous configuration entries in the retained log. No volatile side
table is required. A crash before a configuration write completes loses that
transition; a crash after durable completion recovers it even if the
acknowledgement and dependent sends were not released.

Log conflict repair re-derives membership after truncation. Snapshot creation
stores both the provisioned universe and the exact stable or joint membership
at its boundary. Snapshot installation validates that any retained suffix forms
legal transitions from the installed membership before mutating durable state.

## Checker and schema boundary

Run v3 uses the coherent reference@3, message-codec-v3, checker-v3, and
observation-v3 tuple. The checker validates configuration-transition history
and joint election certificates and emits `raft/membership-transition`
witnesses. Election membership is structured evidence reported by the protocol
status, not a mechanized proof of the implementation's membership semantics.
Run v1 is snapshot-free; run v2 supports snapshots but retains static all-voter
membership.

## Scope and limitations

- The provisioned universe is fixed; there is no dynamic process creation,
  endpoint discovery, or automatic transport reconfiguration.
- Changes are explicit two-step operations; automatic finalization and queued
  configuration changes are not implemented.
- There is no automatic learner catch-up or readiness gate. The caller can
  promote a lagging learner, which can prevent the incoming quorum from forming
  and therefore affect liveness.
- Learners do not have independent replication-flow-control policy.
- The single-threaded simulator cannot expose production concurrency races.
- The current production-adapter milestone is still pending, so portability of
  membership actions across external implementations has not yet been measured.
