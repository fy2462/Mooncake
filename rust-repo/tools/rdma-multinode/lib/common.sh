#!/usr/bin/env bash

common_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
MOONCAKE_ROOT=${MOONCAKE_ROOT:-$(cd "$common_dir/../../../.." && pwd -P)}
RDMA_ARTIFACT_ROOT=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
evidence_file=${evidence_file:-$RDMA_ARTIFACT_ROOT/evidence.tsv}

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        printf 'required command is missing: %s\n' "$1" >&2
        return 1
    }
}

require_path() {
    [[ -e "$1" ]] || {
        printf 'required path is missing: %s\n' "$1" >&2
        return 1
    }
}

json_status_is() {
    local path=$1 expected=$2
    python3 - "$path" "$expected" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        value = json.load(stream)
except (OSError, ValueError):
    raise SystemExit(1)
raise SystemExit(
    0
    if isinstance(value, dict) and value.get("status") == sys.argv[2]
    else 1
)
PY
}

owned_name() {
    [[ ${1:-} == mc-rdma-* && ${1:-} != *'/'* && ${1:-} != *'..'* ]]
}

record() {
    [[ $# -eq 2 ]] || return 2
    mkdir -p "$(dirname "$evidence_file")"
    printf '%s\t%s\n' "$1" "$2" >>"$evidence_file"
}

wait_for_log() {
    local container=$1
    local pattern=$2
    local timeout_seconds=$3
    local deadline=$((SECONDS + timeout_seconds))

    owned_name "$container" || return 2
    while ((SECONDS <= deadline)); do
        if docker logs "$container" 2>&1 | grep -Eq -- "$pattern"; then
            return 0
        fi
        sleep 0.1
    done
    printf 'timed out waiting for %s in %s logs\n' "$pattern" "$container" >&2
    return 1
}

run_if_te_passed() {
    local result_file=$1
    shift

    [[ -f "$result_file" ]] || return 1
    grep -Fxq 'status=PASS' "$result_file" || return 1
    grep -Fxq 'protocol=rdma' "$result_file" || return 1
    "$@"
}
