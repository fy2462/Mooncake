#!/usr/bin/env bash
set -Eeuo pipefail
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
repo=${OPEN_RDMA_ROOT:-/home/fy2462/workspace/PFS/open-rdma-driver}
before="$artifact_root/open-rdma-status-before.txt"
after="$artifact_root/open-rdma-status-after.txt"
git -C "$repo" status --short >"$before"
git -C "$repo" rev-parse HEAD >"$artifact_root/open-rdma-revision.txt"
set +e
(cd "$repo/rust-driver" && CARGO_BUILD_JOBS=5 \
 CARGO_TARGET_DIR="$artifact_root/open-rdma-target" \
 cargo test --no-default-features --features mock --lib -- --nocapture) \
 >"$artifact_root/open-rdma.log" 2>&1
rc=$?
set -e
git -C "$repo" status --short >"$after"
cmp -s "$before" "$after" || { printf 'Open-RDMA checkout changed\n' >&2; exit 1; }
printf 'status=%s\ncheckout_unchanged=true\nclassification=mock-only\n' \
 "$([[ $rc == 0 ]] && printf PASS || printf FAIL)" >"$artifact_root/open-rdma.result"
exit "$rc"
