#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=lib/common.sh
source "$suite_dir/lib/common.sh"
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
te_result=${TE_RESULT:-$artifact_root/te.result}
device=${STORE_DEVICE:-mc-rdma-rxe}
metadata=${STORE_METADATA_SERVER:-127.0.0.1:2379}
master_addr=${STORE_MASTER_ADDR:-127.0.0.1:50051}
master=${STORE_MASTER_CONTAINER:-mc-rdma-rust-master}
node_a=${STORE_NODE_A_CONTAINER:-mc-rdma-store-node-a}
node_b=${STORE_NODE_B_CONTAINER:-mc-rdma-store-node-b}
client=${STORE_CLIENT_CONTAINER:-mc-rdma-store-test-client}

grep -q '^status=PASS$' "$te_result" || {
    printf '{"status":"BLOCKED","reason":"te-gate"}\n' >"$artifact_root/store.result"
    exit 1
}
rm -f "$artifact_root/store-a.ready" "$artifact_root/store-b.ready" "$artifact_root/store.result"
wait_for_ready_file() {
    local file=$1 pattern=$2 deadline=$((SECONDS + 30))
    while ((SECONDS <= deadline)); do
        [[ -f $file ]] && grep -q "$pattern" "$file" && return 0
        sleep 0.1
    done
    printf 'timed out waiting for readiness file: %s\n' "$file" >&2
    return 1
}
docker exec "$master" sh -lc 'timeout 120 /opt/mooncake-rdma/store/mooncake-master --rpc-address 0.0.0.0 --rpc-port 50051 --http-metadata-server-host 0.0.0.0 --http-metadata-server-port 8080 --metrics-port 9003' >"$artifact_root/master.log" 2>&1 &
master_pid=$!
trap 'kill "$master_pid" 2>/dev/null || true; wait "$master_pid" 2>/dev/null || true' EXIT
sleep 2
docker cp "$suite_dir/store-node.py" "$node_a:/tmp/store-node.py"
docker cp "$suite_dir/store-node.py" "$node_b:/tmp/store-node.py"
docker cp "$suite_dir/store-e2e.py" "$client:/tmp/store-e2e.py"
docker exec "$node_a" python3 /tmp/store-node.py --node 127.0.0.1:12401 --device "$device" --metadata "$metadata" --master "$master_addr" --ready /artifacts/store-a.ready >"$artifact_root/store-a.log" 2>&1 &
node_a_pid=$!
docker exec "$node_b" python3 /tmp/store-node.py --node 127.0.0.1:12402 --device "$device" --metadata "$metadata" --master "$master_addr" --ready /artifacts/store-b.ready >"$artifact_root/store-b.log" 2>&1 &
node_b_pid=$!
trap 'kill "$node_a_pid" "$node_b_pid" "$master_pid" 2>/dev/null || true; wait "$node_a_pid" "$node_b_pid" "$master_pid" 2>/dev/null || true' EXIT
wait_for_ready_file "$artifact_root/store-a.ready" '"protocol": "rdma"'
wait_for_ready_file "$artifact_root/store-b.ready" '"protocol": "rdma"'
docker exec "$client" python3 /tmp/store-e2e.py --node 127.0.0.1:12403 --device "$device" --metadata "$metadata" --master "$master_addr" --result /artifacts/store.result >"$artifact_root/store-client.log" 2>&1
grep -q '"status": "PASS"' "$artifact_root/store.result"
