#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}

stage_variable() {
    local stage=${1^^}
    stage=${stage//-/_}
    printf 'RDMA_%s_COMMAND' "$stage"
}

run_stage_command() {
    local stage=$1
    shift
    local variable override
    variable=$(stage_variable "$stage")
    override=${!variable:-}
    if [[ -n $override ]]; then
        bash -c "$override"
    else
        "$@"
    fi
}

preflight() {
    mkdir -p -- "$artifact_root"
    run_stage_command preflight bash -c 'command -v docker >/dev/null && command -v rdma >/dev/null'
}

host_rdma_setup() {
    run_stage_command host-rdma-setup "$suite_dir/setup-host-rdma.sh"
}

build() {
    run_stage_command build "$suite_dir/build-runtime.sh"
}

compose_up() {
    run_stage_command compose-up docker compose -f "$suite_dir/compose.yaml" up -d --wait
}

verbs() {
    run_stage_command verbs "$suite_dir/run-verbs-gate.sh"
}

te() {
    run_stage_command te "$suite_dir/run-te-gate.sh"
}

store_standard() {
    local result="$artifact_root/store-standard.result"
    local rc=0
    if run_stage_command store-standard "$suite_dir/run-store-gate.sh"; then
        :
    else
        rc=$?
    fi

    if [[ $rc -eq 0 && -f $artifact_root/store.result ]] &&
       grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"' "$artifact_root/store.result"; then
        cp -- "$artifact_root/store.result" "$result"
        return 0
    fi

    if [[ -f $artifact_root/store.result ]]; then
        cp -- "$artifact_root/store.result" "$result"
    fi
    printf '{"status":"FAIL","reason":"runner-exit"}\n' >"$result"
    [[ $rc -ne 0 ]] && return "$rc"
    return 1
}

store_resilience() {
    local standard_result="$artifact_root/store-standard.result"
    local result="$artifact_root/store-resilience.result"
    local rc=0

    if ! grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"' "$standard_result" 2>/dev/null; then
        printf 'status=BLOCKED\nreason=store-standard-gate\n' >"$result"
        return 1
    fi

    if [[ -z ${RDMA_STORE_RESILIENCE_COMMAND:-} ]]; then
        printf 'status=NOT_IMPLEMENTED\nreason=task-5\n' >"$result"
        return 1
    fi

    if run_stage_command store-resilience false; then
        :
    else
        rc=$?
    fi
    if [[ $rc -eq 0 ]] &&
       grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"|^status=PASS$' "$result" 2>/dev/null; then
        return 0
    fi
    if [[ $rc -ne 0 ]] || [[ ! -f $result ]]; then
        printf 'status=FAIL\nreason=runner-exit\n' >"$result"
    fi
    [[ $rc -ne 0 ]] && return "$rc"
    return 1
}

open_rdma() {
    run_stage_command open-rdma "$suite_dir/run-open-rdma-mock.sh"
}

report() {
    run_stage_command report "$suite_dir/render-report.sh"
}

compose_down() {
    run_stage_command compose-down docker compose -f "$suite_dir/compose.yaml" down --remove-orphans
}

host_rdma_cleanup() {
    run_stage_command host-rdma-cleanup "$suite_dir/cleanup-host-rdma.sh"
}

cleanup() {
    compose_down || true
    host_rdma_cleanup || true
}

first_failure=0
record_failure() {
    local rc=$1
    [[ $first_failure -eq 0 ]] && first_failure=$rc
}

try_stage() {
    if "$@"; then
        return 0
    fi
    local rc=$?
    record_failure "$rc"
    return "$rc"
}

cleanup_all() {
    local status=$?
    trap - EXIT INT TERM
    cleanup
    exit "$status"
}

all() {
    local ready=true

    trap cleanup_all EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    if ! try_stage preflight; then ready=false; fi
    if "$ready" && ! try_stage host_rdma_setup; then ready=false; fi
    if "$ready" && ! try_stage build; then ready=false; fi
    if "$ready" && ! try_stage compose_up; then ready=false; fi

    if "$ready" && try_stage verbs; then
        if try_stage te; then
            if try_stage store_standard; then
                try_stage store_resilience || true
            else
                store_resilience || true
            fi
        fi
    fi

    try_stage open_rdma || true
    try_stage report || true
    return "$first_failure"
}

case ${1:-all} in
    preflight) preflight ;;
    host-rdma-setup) host_rdma_setup ;;
    build) build ;;
    compose-up) compose_up ;;
    verbs) verbs ;;
    te) te ;;
    store|store-standard) store_standard ;;
    store-resilience) store_resilience ;;
    open-rdma) open_rdma ;;
    report) report ;;
    compose-down) compose_down ;;
    host-rdma-cleanup) host_rdma_cleanup ;;
    cleanup) cleanup ;;
    all) all ;;
    *)
        printf 'usage: %s [preflight|host-rdma-setup|build|compose-up|verbs|te|store-standard|store-resilience|open-rdma|report|compose-down|host-rdma-cleanup|cleanup|all]\n' "$0" >&2
        exit 2
        ;;
esac
