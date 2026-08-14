# Published v1 results

Result documents in this directory are immutable outputs of
`d-raft.mutant-result/v1`. The filename identifies the execution platform;
each document carries the exact Go version, GOOS, GOARCH, executable revision,
target checkout state, base tree, manifest digest, patch digests, fixed test
output, and classification.

Do not overwrite an existing result. Repetitions or another platform should
use a distinct filename and retain all outcome categories, including
operational errors and survivors.

Published result:

- `linux-amd64.json` — SHA-256
  `2e3a978c2dde791a50eeef17a7a047eec458b42fb095fd6be76c8a81e18823f9`;
  3 safety kills, 3 conformance kills, and no other classifications.
