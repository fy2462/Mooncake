#!/usr/bin/env bash
set -Eeuo pipefail
suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}

cleanup() {
    docker compose -f "$suite_dir/compose.yaml" down --remove-orphans >/dev/null 2>&1 || true
    for name in mc-rdma-fallback-a mc-rdma-fallback-b mc-rdma-master-fallback mc-rdma-client-fallback; do
        docker rm -f "$name" >/dev/null 2>&1 || true
    done
    docker network rm mc-rdma-net >/dev/null 2>&1 || true
    if rdma link show 2>/dev/null | grep -q 'mc-rdma-rxe-a/'; then sudo rdma link delete mc-rdma-rxe-a; fi
    if rdma link show 2>/dev/null | grep -q 'mc-rdma-rxe-b/'; then sudo rdma link delete mc-rdma-rxe-b; fi
    if ip link show mc-rdma-net-a >/dev/null 2>&1; then sudo ip link delete mc-rdma-net-a; fi
}

case ${1:-all} in
    preflight) command -v docker rdma >/dev/null; test -d "$artifact_root" ;;
    build) "$suite_dir/build-runtime.sh" ;;
    verbs) "$suite_dir/run-verbs-gate.sh" ;;
    te) "$suite_dir/run-te-gate.sh" ;;
    store) "$suite_dir/run-store-gate.sh" ;;
    open-rdma) "$suite_dir/run-open-rdma-mock.sh" ;;
    report) "$suite_dir/render-report.sh" ;;
    cleanup) cleanup ;;
    all)
        trap cleanup EXIT INT TERM
        "$0" preflight
        "$0" build
        "$0" verbs
        "$0" te
        "$0" store
        "$0" open-rdma || true
        "$0" report
        ;;
    *) printf 'usage: %s [preflight|build|verbs|te|store|open-rdma|report|all|cleanup]\n' "$0" >&2; exit 2 ;;
esac
