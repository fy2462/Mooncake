#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
verbs_gate="$suite_dir/run-verbs-gate.sh"
te_gate="$suite_dir/run-te-gate.sh"
store_gate="$suite_dir/run-store-gate.sh"
gates=("$verbs_gate" "$te_gate" "$store_gate")

grep -Fq 'server=${VERBS_SERVER_CONTAINER:-mc-rdma-te-node-a}' "$verbs_gate"
grep -Fq 'client=${VERBS_CLIENT_CONTAINER:-mc-rdma-te-node-b}' "$verbs_gate"
grep -Fq 'server_ip=${VERBS_SERVER_IP:-127.0.0.1}' "$verbs_gate"
grep -Fq 'server_dev=${VERBS_SERVER_DEVICE:-mc-rdma-rxe-a}' "$verbs_gate"
grep -Fq 'client_dev=${VERBS_CLIENT_DEVICE:-mc-rdma-rxe-b}' "$verbs_gate"

grep -Fq 'directions=(a:b b:a b:c c:b c:a a:c)' "$te_gate"
grep -Fq '[c]=mc-rdma-te-node-c' "$te_gate"
grep -Fq '[c]=mc-rdma-rxe-c' "$te_gate"
grep -Fq 'direction_%s_%s=%s' "$te_gate"

grep -Fq 'device_a=${STORE_DEVICE_A:-mc-rdma-rxe-a}' "$store_gate"
grep -Fq 'device_b=${STORE_DEVICE_B:-mc-rdma-rxe-b}' "$store_gate"
grep -Fq 'device_c=${STORE_DEVICE_C:-mc-rdma-rxe-c}' "$store_gate"
grep -Fq 'metadata=${STORE_METADATA_SERVER:-127.0.0.1:2379}' "$store_gate"
grep -Fq 'master_addr=${STORE_MASTER_ADDR:-127.0.0.1:50051}' "$store_gate"
grep -Fq 'master=${STORE_MASTER_CONTAINER:-mc-rdma-rust-master}' "$store_gate"
grep -Fq 'node_a=${STORE_NODE_A_CONTAINER:-mc-rdma-store-node-a}' "$store_gate"
grep -Fq 'node_b=${STORE_NODE_B_CONTAINER:-mc-rdma-store-node-b}' "$store_gate"
grep -Fq 'node_c=${STORE_NODE_C_CONTAINER:-mc-rdma-store-node-c}' "$store_gate"
grep -Fq 'client=${STORE_CLIENT_CONTAINER:-mc-rdma-store-test-client}' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-node.py" "$node_a:/tmp/store-node.py"' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-node.py" "$node_b:/tmp/store-node.py"' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-node.py" "$node_c:/tmp/store-node.py"' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-e2e.py" "$client:/tmp/store-e2e.py"' "$store_gate"
grep -Fq 'docker exec "$master" sh -c '\''! grep -h libmooncake_store.so /proc/[0-9]*/maps 2>/dev/null'\''' "$store_gate"

for gate in "${gates[@]}"; do
    ! grep -Eq 'docker[[:space:]]+(run|create)' "$gate"
    ! grep -Eq 'fallback' "$gate"
done
