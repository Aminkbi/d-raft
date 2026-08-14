# Published v1 results

Result documents in this directory are immutable outputs of
`d-raft.mutant-result/v1`. The filename identifies the execution platform;
each document carries the exact Go version, GOOS, GOARCH, executable revision,
target checkout state, base tree, manifest digest, patch digests, fixed test
output, and classification.

Do not overwrite an existing result. Repetitions or another platform should
use a distinct filename and retain all outcome categories, including
operational errors and survivors.
