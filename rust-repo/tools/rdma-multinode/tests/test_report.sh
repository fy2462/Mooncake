#!/usr/bin/env bash
set -Eeuo pipefail
suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
printf 'PASS\ntool=ib_write_bw\n' >"$tmp_dir/verbs.result"
printf 'status=FAIL\nprotocol=rdma\n' >"$tmp_dir/te.result"
printf '{"status": "PASS"}\n' >"$tmp_dir/store.result"
printf 'status=PASS\n' >"$tmp_dir/open-rdma.result"
printf 'deadbeef\n' >"$tmp_dir/open-rdma-revision.txt"
if RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/render-report.sh"; then
    printf 'report accepted Store PASS with TE FAIL\n' >&2
    exit 1
fi
printf 'status=PASS\nprotocol=rdma\n' >"$tmp_dir/te.result"
RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/render-report.sh"
grep -q 'mock-only API/driver evidence' "$tmp_dir/report.md"
grep -q 'shared-rdma-device' "$tmp_dir/report.md"
