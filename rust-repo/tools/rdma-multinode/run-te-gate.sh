#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=lib/common.sh
source "$suite_dir/lib/common.sh"
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
verbs_result=${VERBS_RESULT:-$artifact_root/verbs.result}
result=${TE_RESULT:-$artifact_root/te.result}

log_passes() {
    local log=$1 rc=$2
    [[ $rc == 0 ]] &&
        grep -q 'Remote segment protocol: rdma' "$log" &&
        grep -q 'Stage 1: Write Data' "$log" &&
        grep -q 'Stage 2: Read Data' "$log" &&
        grep -q 'RDMA compare: OK' "$log" &&
        ! grep -Eqi 'timeout|transfer failed|protocol: tcp' "$log"
}

classify() {
    local initiator_log=${TE_INITIATOR_LOG:-$artifact_root/te-initiator.log}
    local initiator_rc=${TE_INITIATOR_RC:-0} status=FAIL
    log_passes "$initiator_log" "$initiator_rc" && status=PASS
    printf 'status=%s\nprotocol=rdma\ntarget_device=%s\ninitiator_device=%s\ncompare=%s\n' \
        "$status" "${TE_TARGET_DEVICE:-mc-rdma-rxe-a}" \
        "${TE_INITIATOR_DEVICE:-mc-rdma-rxe-b}" \
        "$([[ $status == PASS ]] && printf OK || printf MISSING)" >"$result"
    [[ $status == PASS ]]
}

if ! grep -q '^PASS$' "$verbs_result" 2>/dev/null; then
    printf 'status=BLOCKED\nreason=verbs-gate\n' >"$result"
    exit 1
fi
if [[ ${TE_CLASSIFY_ONLY:-0} == 1 ]]; then
    classify
    exit
fi

metadata=${TE_METADATA_SERVER:-127.0.0.1:2379}
gid_index=${TE_GID_INDEX:-0}
declare -A containers=(
    [a]=mc-rdma-te-node-a [b]=mc-rdma-te-node-b [c]=mc-rdma-te-node-c
)
declare -A devices=(
    [a]=mc-rdma-rxe-a [b]=mc-rdma-rxe-b [c]=mc-rdma-rxe-c
)
directions=(a:b b:a b:c c:b c:a a:c)
aggregate=$(mktemp "$artifact_root/.te.result.XXXXXX")
target_pid=
active_target=
active_target_port=
stop_target() {
    if [[ -n $active_target && -n $active_target_port ]]; then
        docker exec "$active_target" pkill -TERM -f -- \
            "--local_server_name=127.0.0.1:$active_target_port" \
            >/dev/null 2>&1 || true
    fi
    if [[ -n $target_pid ]]; then
        kill "$target_pid" 2>/dev/null || true
        wait "$target_pid" 2>/dev/null || true
    fi
    target_pid= active_target= active_target_port=
}
trap 'stop_target; rm -f -- "$aggregate"' EXIT
printf 'protocol=rdma\ndirections=a-b,b-a,b-c,c-b,c-a,a-c\n' >"$aggregate"
overall=PASS

for pair in "${directions[@]}"; do
    target_name=${pair%%:*}
    initiator_name=${pair##*:}
    target=${containers[$target_name]}
    initiator=${containers[$initiator_name]}
    target_device=${devices[$target_name]}
    initiator_device=${devices[$initiator_name]}
    target_port=$((12340 + ${#aggregate} + ${#pair}))
    initiator_port=$((target_port + 100))
    target_log="$artifact_root/te-$target_name-$initiator_name-target.log"
    initiator_log="$artifact_root/te-$target_name-$initiator_name-initiator.log"
    active_target=$target
    active_target_port=$target_port
    docker exec "$target" sh -lc "MC_GID_INDEX='$gid_index' timeout 60 /opt/mooncake-rdma/te/rdma_transport_test \
 --mode=target --protocol=rdma --mem_backend=cpu --metadata_server='$metadata' \
 --local_server_name='127.0.0.1:$target_port' --device_name='$target_device' \
 --buffer_size=67108864 --data_length=4194304 --logtostderr=1" >"$target_log" 2>&1 &
    target_pid=$!
    sleep 2
    set +e
    docker exec "$initiator" sh -lc "MC_GID_INDEX='$gid_index' timeout 45 /opt/mooncake-rdma/te/rdma_transport_test \
 --mode=initiator --protocol=rdma --mem_backend=cpu --metadata_server='$metadata' \
 --local_server_name='127.0.0.1:$initiator_port' --segment_id='127.0.0.1:$target_port' \
 --device_name='$initiator_device' --expect_remote_location=cpu:0 \
 --buffer_size=67108864 --data_length=4194304 --logtostderr=1" >"$initiator_log" 2>&1
    initiator_rc=$?
    set -e
    stop_target
    direction_status=FAIL
    log_passes "$initiator_log" "$initiator_rc" && direction_status=PASS
    [[ $direction_status == PASS ]] || overall=FAIL
    printf 'direction_%s_%s=%s,target_device=%s,initiator_device=%s,compare=%s\n' \
        "$target_name" "$initiator_name" "$direction_status" "$target_device" \
        "$initiator_device" "$([[ $direction_status == PASS ]] && printf OK || printf MISSING)" \
        >>"$aggregate"
done
{
    printf 'status=%s\n' "$overall"
    cat "$aggregate"
} >"$result"
[[ $overall == PASS ]]
