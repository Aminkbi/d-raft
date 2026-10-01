#!/usr/bin/env bash
# Read-only repository checks; caller-supplied Go cache/toolchain settings apply.
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-quick}"
if [[ $# -gt 1 || ( "$mode" != quick && "$mode" != evidence ) ]]; then
  printf 'usage: bash tools/check.sh [quick|evidence]\n' >&2
  exit 2
fi
cd -- "$repository_root"

if [[ "$mode" == quick ]]; then
  unformatted="$(gofmt -l .)"
  if [[ -n "$unformatted" ]]; then
    printf 'Unformatted Go files:\n%s\n' "$unformatted" >&2
    exit 1
  fi
  for module_directory in . adapters/etcdraft; do
    (
      cd -- "$module_directory"
      printf 'Checking module: %s\n' "$module_directory"
      go mod verify
      go mod tidy -diff
      go test ./...
      go vet ./...
    )
  done
else
  python3 tools/verify_apporacle.py --self-test
  go run ./cmd/draft-projection-study --verify evaluation/results/projection-v1/result.json
  go run ./cmd/draft-eval --verify evaluation/results/v1/steady-cluster-6a685e251794/result.json
  for case_name in loss-2pct-seed-20260814 portable-faults-v1-seed-1; do
    (
      cd adapters/etcdraft
      case_directory="../../corpus/cross-adapter/v1/$case_name"
      go run ./cmd/draft-cross verify \
        --plan "$case_directory/semantic.plan.json" \
        --source-run "$case_directory/source.run.json" \
        --in "$case_directory/cross"
    )
  done
  for evidence_directory in \
    evaluation/results/projection-v1 \
    evaluation/results/v1/steady-cluster-6a685e251794 \
    corpus/cross-adapter/v1/loss-2pct-seed-20260814 \
    corpus/cross-adapter/v1/portable-faults-v1-seed-1; do
    (cd -- "$evidence_directory" && sha256sum -c SHA256SUMS)
  done
fi
printf '%s checks passed\n' "$mode"
