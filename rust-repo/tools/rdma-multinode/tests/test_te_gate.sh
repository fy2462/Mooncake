#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
printf 'PASS\n' >"$tmp_dir/verbs.result"
cat >"$tmp_dir/te-initiator.log" <<'EOF'
Remote segment protocol: rdma
Stage 1: Write Data
Stage 2: Read Data
RDMA compare: OK
EOF

RDMA_ARTIFACT_ROOT="$tmp_dir" TE_CLASSIFY_ONLY=1 \
    bash "$suite_dir/run-te-gate.sh"
grep -q '^status=PASS$' "$tmp_dir/te.result"

sed -i '/RDMA compare/d' "$tmp_dir/te-initiator.log"
if RDMA_ARTIFACT_ROOT="$tmp_dir" TE_CLASSIFY_ONLY=1 \
    bash "$suite_dir/run-te-gate.sh"; then
    printf 'accepted a missing compare marker\n' >&2
    exit 1
fi
grep -q '^status=FAIL$' "$tmp_dir/te.result"
if RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/run-store-gate.sh"; then
    printf 'ran Store after a failed TE compare\n' >&2
    exit 1
fi
grep -q '"status":"BLOCKED"' "$tmp_dir/store.result"

printf 'FAIL\n' >"$tmp_dir/verbs.result"
if RDMA_ARTIFACT_ROOT="$tmp_dir" TE_CLASSIFY_ONLY=1 \
    bash "$suite_dir/run-te-gate.sh"; then
    printf 'ran TE after a failed verbs gate\n' >&2
    exit 1
fi
grep -q '^status=BLOCKED$' "$tmp_dir/te.result"
