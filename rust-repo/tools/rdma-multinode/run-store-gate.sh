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
etcd=${STORE_ETCD_CONTAINER:-mc-rdma-etcd}
mode=${STORE_GATE_MODE:-standard}
scenario_timeout=${STORE_SCENARIO_TIMEOUT:-120}

case $mode in
    standard) result=$artifact_root/store.result ;;
    resilience) result=$artifact_root/store-resilience.result ;;
    *) printf 'unknown Store gate mode: %s\n' "$mode" >&2; exit 2 ;;
esac

mkdir -p -- "$artifact_root"
grep -q '^status=PASS$' "$te_result" || {
    printf '{"status":"BLOCKED","reason":"te-gate"}\n' >"$result"
    exit 1
}

run_id=$(date +%s)-$$-$mode
cluster_id=mc-rdma-$run_id
master_command_file=$artifact_root/store-master-command.json
node_command_file=$artifact_root/store-node-command.json
host_manifest=$artifact_root/host-rdma.json
ready_a=$artifact_root/store-a.ready
ready_b=$artifact_root/store-b.ready
stats_a=$artifact_root/store-a.storage.json
stats_b=$artifact_root/store-b.storage.json
command_a=$artifact_root/store-a.command.json
command_b=$artifact_root/store-b.command.json
rm -f -- "$ready_a" "$ready_b" "$stats_a" "$stats_b" "$command_a" "$command_b" "$result"

master_command=(
    /opt/mooncake-rdma/store/mooncake-master
    --rpc-address 0.0.0.0
    --rpc-port 50051
    --http-metadata-server-host 0.0.0.0
    --http-metadata-server-port 8080
    --metrics-port 9003
    --enable-offload
    --offload-on-evict
    --offload-force-evict
    --eviction-high-watermark-ratio 0.55
    --eviction-ratio 0.20
    --client-ttl-secs 2
    --default-kv-lease-ttl-ms 1
)
if [[ $mode == resilience ]]; then
    master_command+=(
        --enable-ha
        --etcd-endpoints http://127.0.0.1:2379
        --cluster-id "$cluster_id"
        --ha-lease-ttl-secs 3
    )
fi

node_a_command=(
    python3 /tmp/store-node.py
    --node 127.0.0.1:12401
    --device "$device"
    --metadata "$metadata"
    --master "$master_addr"
    --ready /artifacts/store-a.ready
    --stats /artifacts/store-a.storage.json
    --command /artifacts/store-a.command.json
    --storage-root "/tmp/mc-rdma-store-a-$run_id"
    --storage-quota 268435456
    --storage-interval 0.1
    --disk-high-watermark 0.99
    --disk-low-watermark 0.95
)
node_b_command=(
    python3 /tmp/store-node.py
    --node 127.0.0.1:12402
    --device "$device"
    --metadata "$metadata"
    --master "$master_addr"
    --ready /artifacts/store-b.ready
    --stats /artifacts/store-b.storage.json
    --command /artifacts/store-b.command.json
    --storage-root "/tmp/mc-rdma-store-b-$run_id"
    --storage-quota 268435456
    --storage-interval 0.1
    --disk-high-watermark 0.99
    --disk-low-watermark 0.95
)

python3 - "$master_command_file" "${master_command[@]}" <<'PY'
import json
import sys
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump(sys.argv[2:], stream)
    stream.write("\n")
PY
python3 - "$node_command_file" "$node_a" "$node_b" "${node_a_command[*]}" "${node_b_command[*]}" <<'PY'
import json
import shlex
import sys
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump({sys.argv[2]: shlex.split(sys.argv[4]), sys.argv[3]: shlex.split(sys.argv[5])}, stream)
    stream.write("\n")
PY

master_pid=
node_a_pid=
node_b_pid=
cleanup_store_processes() {
    local status=${1:-$?}
    trap - EXIT INT TERM
    stop_pattern='import os,signal,sys
needle=sys.argv[1]
for entry in os.listdir("/proc"):
    if entry.isdigit() and int(entry) != os.getpid():
        try:
            command=open(f"/proc/{entry}/cmdline", "rb").read().replace(b"\\0", b" ").decode()
            if needle in command:
                os.kill(int(entry), signal.SIGTERM)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass'
    docker exec "$node_a" python3 -c "$stop_pattern" /tmp/store-node.py >/dev/null 2>&1 || true
    docker exec "$node_b" python3 -c "$stop_pattern" /tmp/store-node.py >/dev/null 2>&1 || true
    docker exec "$master" python3 -c "$stop_pattern" mooncake-master >/dev/null 2>&1 || true
    for process_id in "$node_a_pid" "$node_b_pid" "$master_pid"; do
        [[ -z $process_id ]] || kill "$process_id" 2>/dev/null || true
        [[ -z $process_id ]] || wait "$process_id" 2>/dev/null || true
    done
    if [[ $status -ne 0 && ! -f $result ]]; then
        printf '{"status":"FAIL","reason":"runner-exit","rc":%s}\n' "$status" >"$result"
    fi
    exit "$status"
}
trap cleanup_store_processes EXIT
trap 'cleanup_store_processes 130' INT
trap 'cleanup_store_processes 143' TERM

wait_for_ready_file() {
    local file=$1 pattern=$2 deadline=$((SECONDS + 45))
    while ((SECONDS <= deadline)); do
        [[ -f $file ]] && grep -q "$pattern" "$file" && return 0
        sleep 0.1
    done
    printf 'timed out waiting for readiness file: %s\n' "$file" >&2
    return 1
}

wait_for_master() {
    local deadline=$((SECONDS + 45))
    while ((SECONDS <= deadline)); do
        if docker exec "$client" python3 -c \
            "import socket;s=socket.create_connection(('127.0.0.1',50051),1);s.close()" \
            >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    printf 'timed out waiting for Rust Store Master\n' >&2
    return 1
}

docker cp "$suite_dir/store-node.py" "$node_a:/tmp/store-node.py"
docker cp "$suite_dir/store-node.py" "$node_b:/tmp/store-node.py"
docker cp "$suite_dir/store-e2e.py" "$client:/tmp/store-e2e.py"

docker exec "$master" "${master_command[@]}" >"$artifact_root/master-$mode.log" 2>&1 &
master_pid=$!
wait_for_master
docker exec "$node_a" "${node_a_command[@]}" >"$artifact_root/store-a-$mode.log" 2>&1 &
node_a_pid=$!
docker exec "$node_b" "${node_b_command[@]}" >"$artifact_root/store-b-$mode.log" 2>&1 &
node_b_pid=$!
wait_for_ready_file "$ready_a" '"storage_backend": "RustFilePerKey"'
wait_for_ready_file "$ready_b" '"storage_backend": "RustFilePerKey"'
grep -q '"cpp_store_loaded": false' "$ready_a"
grep -q '"cpp_store_loaded": false' "$ready_b"
docker exec "$master" sh -c '! grep -h libmooncake_store.so /proc/[0-9]*/maps 2>/dev/null'
docker exec "$node_a" sh -c '! grep -h libmooncake_store.so /proc/[0-9]*/maps 2>/dev/null'
docker exec "$node_b" sh -c '! grep -h libmooncake_store.so /proc/[0-9]*/maps 2>/dev/null'

if [[ $mode == standard ]]; then
    docker exec "$client" timeout "$scenario_timeout" python3 /tmp/store-e2e.py \
        --device "$device" --metadata "$metadata" --master "$master_addr" \
        --mode standard --prefix first-standard --tier-timeout 45 \
        --result /artifacts/store.result >"$artifact_root/store-client.log" 2>&1
    grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"' "$result"
    cp -- "$result" "$artifact_root/store-standard.result"
else
    python3 "$suite_dir/store-resilience-e2e.py" \
        --artifact-root "$artifact_root" \
        --result "$result" \
        --master-command-file "$master_command_file" \
        --node-command-file "$node_command_file" \
        --host-rdma-manifest "$host_manifest" \
        --master-container "$master" \
        --node-a "$node_a" --node-b "$node_b" \
        --client "$client" --etcd "$etcd" \
        --device "$device" --metadata "$metadata" --master "$master_addr" \
        --scenario-timeout "$scenario_timeout"
    grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"' "$result"
fi
