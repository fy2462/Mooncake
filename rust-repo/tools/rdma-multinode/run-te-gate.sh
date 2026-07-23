#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=lib/common.sh
source "$suite_dir/lib/common.sh"

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
verbs_result=${VERBS_RESULT:-$artifact_root/verbs.result}
target_log=${TE_TARGET_LOG:-$artifact_root/te-target.log}
initiator_log=${TE_INITIATOR_LOG:-$artifact_root/te-initiator.log}
result=${TE_RESULT:-$artifact_root/te.result}

classify() {
    local initiator_rc=${TE_INITIATOR_RC:-0}
    local status=FAIL
    if [[ $initiator_rc == 0 ]] &&
       grep -q 'Remote segment protocol: rdma' "$initiator_log" &&
       grep -q 'Stage 1: Write Data' "$initiator_log" &&
       grep -q 'Stage 2: Read Data' "$initiator_log" &&
       grep -q 'RDMA compare: OK' "$initiator_log" &&
       ! grep -Eqi 'timeout|transfer failed|protocol: tcp' "$initiator_log"; then
        status=PASS
    fi
    printf 'status=%s\nprotocol=rdma\ntarget_device=%s\ninitiator_device=%s\ncompare=%s\n' \
        "$status" "${TE_TARGET_DEVICE:-mc-rdma-rxe}" "${TE_INITIATOR_DEVICE:-mc-rdma-rxe}" \
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

target=${TE_TARGET_CONTAINER:-mc-rdma-te-node-a}
initiator=${TE_INITIATOR_CONTAINER:-mc-rdma-te-node-b}
target_ip=${TE_TARGET_IP:-127.0.0.1}
initiator_ip=${TE_INITIATOR_IP:-127.0.0.1}
target_device=${TE_TARGET_DEVICE:-mc-rdma-rxe}
initiator_device=${TE_INITIATOR_DEVICE:-mc-rdma-rxe}
metadata=${TE_METADATA_SERVER:-127.0.0.1:2379}

docker exec "$target" sh -lc "timeout 60 /opt/mooncake-rdma/te/rdma_transport_test \
 --mode=target --protocol=rdma --mem_backend=cpu --metadata_server='$metadata' \
 --local_server_name='$target_ip:12345' --device_name='$target_device' \
 --buffer_size=67108864 --data_length=4194304 --logtostderr=1" >"$target_log" 2>&1 &
target_pid=$!
trap 'kill "$target_pid" 2>/dev/null || true; wait "$target_pid" 2>/dev/null || true' EXIT
sleep 2
set +e
docker exec "$initiator" sh -lc "timeout 45 /opt/mooncake-rdma/te/rdma_transport_test \
 --mode=initiator --protocol=rdma --mem_backend=cpu --metadata_server='$metadata' \
 --local_server_name='$initiator_ip:12346' --segment_id='$target_ip:12345' \
 --device_name='$initiator_device' --expect_remote_location=cpu:0 \
 --buffer_size=67108864 --data_length=4194304 --logtostderr=1" >"$initiator_log" 2>&1
TE_INITIATOR_RC=$?
set -e
export TE_INITIATOR_RC
classify
