#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
repo="${UPSTREAM_REPO:-https://github.com/etcd-io/raft.git}"
if [[ -n "${DEFECT_WORKDIR:-}" ]]; then
	work="$DEFECT_WORKDIR"
	cleanup=0
else
	work="$(mktemp -d "${TMPDIR:-/tmp}/d-raft-pr31.XXXXXX")"
	cleanup=1
fi
trap 'if [[ "$cleanup" == 1 ]]; then rm -rf -- "$work"; fi' EXIT

fixture="$root/defects/etcd-raft-pr31/slow_follower_after_compaction.txt"
manifest="$root/defects/etcd-raft-pr31/manifest.json"
fixture_sha="bdf15b0b725fca9114922c7f5d0e56aa3005f6eea2555c9edd14efe3142fb4a9"
vulnerable="42419da55f51f7a0f0fb22b0e15892fb5657d191"
fixed="d086538f5647fb518b8dbba26a424aea9d782c78"

[[ -f "$fixture" ]]
[[ -f "$manifest" ]]
[[ "$(sha256sum "$fixture" | awk '{print $1}')" == "$fixture_sha" ]]
python3 - "$manifest" "$fixture" "$fixture_sha" "$vulnerable" "$fixed" <<'PY'
import hashlib
import json
import sys

manifest_path, fixture_path, fixture_sha, vulnerable, fixed = sys.argv[1:]
with open(manifest_path, "r", encoding="utf-8") as stream:
    manifest = json.load(stream)
expected = {
    "fixture": "defects/etcd-raft-pr31/slow_follower_after_compaction.txt",
    "fixture_sha256": fixture_sha,
    "vulnerable_revision": vulnerable,
    "fixed_revision": fixed,
}
for key, value in expected.items():
    if manifest.get(key) != value:
        raise SystemExit(f"manifest mismatch for {key}")
with open(fixture_path, "rb") as fixture_stream:
    if hashlib.sha256(fixture_stream.read()).hexdigest() != manifest["fixture_sha256"]:
        raise SystemExit("manifest fixture digest mismatch")
PY
[[ ! -e "$work" || -z "$(find "$work" -mindepth 1 -maxdepth 1 -print -quit)" ]]
mkdir -p -- "$work"
source="$work/source"
git clone --no-checkout "$repo" "$source" >/dev/null
git -C "$source" fetch --quiet origin "$vulnerable" "$fixed"

run_case() {
	label="$1"
	revision="$2"
	checkout="$work/$label"
	git -C "$source" worktree add --detach "$checkout" "$revision" >/dev/null
	cp -- "$fixture" "$checkout/testdata/slow_follower_after_compaction.txt"
	cache="$work/cache-$label"
	gopath="$work/gopath-$label"
	modcache="${DEFECT_GOMODCACHE:-$work/modcache-$label}"
	set +e
	output="$(GOCACHE="$cache" GOPATH="$gopath" GOMODCACHE="$modcache" go -C "$checkout" test . -run '^TestInteraction$' -count=1 2>&1)"
	status=$?
	set -e
	if [[ "$label" == vulnerable ]]; then
		[[ "$status" -ne 0 && "$output" == *panic* ]] || { printf '%s\n' "$output" >&2; return 1; }
		echo "vulnerable $revision: panic predicate preserved"
	else
		[[ "$status" -eq 0 ]] || { printf '%s\n' "$output" >&2; return 1; }
		echo "fixed $revision: fixture boundary reached without panic"
	fi
}

run_case vulnerable "$vulnerable"
run_case fixed "$fixed"
