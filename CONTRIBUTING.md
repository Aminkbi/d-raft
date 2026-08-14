# Contributing

Thank you for helping make d-raft a rigorous and useful research artifact.
Bug reports, trace-schema review, fault-model critique, documentation, and
small focused implementations are welcome.

## Before opening a change

For substantial API, trace-schema, or research-method changes, open an issue
first. Describe the problem, the observable behavior, alternatives considered,
and any effect on determinism or trace compatibility.

## Local checks

Use the toolchain declared by `go.mod`, then run:

```bash
gofmt -w .
go test ./...
go vet ./...
go test -bench . -benchmem ./...
```

New behavior needs focused tests. Any change to random sampling, event order,
packet lifecycle, or trace output also needs a reproducibility or golden test.
Benchmarks should avoid external I/O and report allocations.

## Pull requests

- Keep changes narrow and explain their user or research value.
- Preserve the deterministic contract in `COMPATIBILITY.md`.
- Document exported APIs and update relevant examples.
- Call out schema additions and every potential compatibility break.
- Do not include generated traces containing private application data.

Contributions are accepted under the repository's Apache 2.0 license.
Participation is governed by `CODE_OF_CONDUCT.md`.
