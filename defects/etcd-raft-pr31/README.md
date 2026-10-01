# etcd/raft PR #31: log-truncation panic

This is the first pinned production-core replay case. The upstream report and
fix are [PR #31](https://github.com/etcd-io/raft/pull/31), “raft: fix panic on
MsgApp after log truncation”. The vulnerable revision is
`42419da55f51f7a0f0fb22b0e15892fb5657d191`; the fixed merge revision is
`d086538f5647fb518b8dbba26a424aea9d782c78`.

`slow_follower_after_compaction.txt` is the upstream regression interaction
fixture. It fills the follower in-flight window, drops the two replication
messages carrying entries 15 and 16, compacts the leader through entry 17, and
triggers an empty probe. At the vulnerable revision the fixture panics while
processing the resulting stale append; at the fixed revision it reaches the
recorded boundary.

The fixture bytes are bound by `bdf15b0b725fca9114922c7f5d0e56aa3005f6eea2555c9edd14efe3142fb4a9`.
The replay command fetches both pinned revisions into temporary worktrees,
copies this fixture into each checkout, and treats the vulnerable panic and
fixed success as separate outcomes:

```bash
bash tools/replay-etcdraft-defect.sh
```

The script uses external Git and Go processes at the research-runner boundary;
it does not change the current checkout or the pinned fixture. This case is
production-core evidence for the replay workflow, not a claim that the
reference model proves the upstream implementation safe. The fixture's
compaction and `MsgApp` scheduling are outside the portable semantic-plan
capability set, so this runner is the pinned vulnerable/fixed defect gate; the
operation-level causal APIs are validated separately with d-raft source tapes
and retain their own causal coverage report.
