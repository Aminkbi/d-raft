# Working on pinned production-defect replay

`defect/` owns strict metadata and decoding for reviewed upstream defect cases. The
fixture and human-readable provenance live under `defects/`; the runner lives
in `tools/`. Keep vulnerable and fixed revisions as full Git object IDs and
bind fixture bytes with SHA-256. A replay result must distinguish vulnerable
failure preservation from fixed-version absence of the predicate; a successful
local tape replay or a fixed run alone is not a production-effectiveness claim.
The PR #31 fixture is an adapter-boundary gate; compaction is outside the
portable semantic-plan capability set, so it is not presented as a portable
causal plan execution.

Run the pinned PR #31 case with:

```bash
UPSTREAM_REPO=/path/to/etcd-io/raft \
DEFECT_GOMODCACHE=/path/to/writable/modcache \
bash tools/replay-etcdraft-defect.sh
```

The script uses external Git and Go processes at the research-runner boundary.
Do not add upstream source or generated worktrees to the repository.
