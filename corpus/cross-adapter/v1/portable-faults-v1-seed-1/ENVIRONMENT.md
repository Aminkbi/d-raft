# Producer environment

This functional replay witness was generated on 2026-08-15 (Asia/Tehran) with:

- OS/kernel: Linux 7.0.0-29-generic, amd64;
- Go: 1.26.6, `GOOS=linux`, `GOARCH=amd64`;
- CPU: 12th Gen Intel Core i7-1255U, 1 socket, 10 cores, 12 logical CPUs,
  2 threads per core; and
- memory visible to the producer environment: 15,937,572,864 bytes.

The observations came from `uname -srm`, selected `lscpu` fields, `free -b`,
`go version`, and `go env GOOS GOARCH`. This case reports no wall-clock or
throughput measurement, so it is a functional reproducibility witness rather
than a performance trial. Hardware and runner contention must be controlled
and reported separately for comparative performance results.
