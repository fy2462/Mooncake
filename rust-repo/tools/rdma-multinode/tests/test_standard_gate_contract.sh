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
grep -Fq 'server_dev=${VERBS_SERVER_DEVICE:-mc-rdma-rxe}' "$verbs_gate"
grep -Fq 'client_dev=${VERBS_CLIENT_DEVICE:-mc-rdma-rxe}' "$verbs_gate"

grep -Fq 'target=${TE_TARGET_CONTAINER:-mc-rdma-te-node-a}' "$te_gate"
grep -Fq 'initiator=${TE_INITIATOR_CONTAINER:-mc-rdma-te-node-b}' "$te_gate"
grep -Fq 'target_ip=${TE_TARGET_IP:-127.0.0.1}' "$te_gate"
grep -Fq 'initiator_ip=${TE_INITIATOR_IP:-127.0.0.1}' "$te_gate"
grep -Fq 'target_device=${TE_TARGET_DEVICE:-mc-rdma-rxe}' "$te_gate"
grep -Fq 'initiator_device=${TE_INITIATOR_DEVICE:-mc-rdma-rxe}' "$te_gate"
grep -Fq -- "--local_server_name='\$target_ip:12345'" "$te_gate"
grep -Fq -- "--local_server_name='\$initiator_ip:12346'" "$te_gate"

grep -Fq 'device=${STORE_DEVICE:-mc-rdma-rxe}' "$store_gate"
grep -Fq 'metadata=${STORE_METADATA_SERVER:-127.0.0.1:2379}' "$store_gate"
grep -Fq 'master_addr=${STORE_MASTER_ADDR:-127.0.0.1:50051}' "$store_gate"
grep -Fq 'master=${STORE_MASTER_CONTAINER:-mc-rdma-rust-master}' "$store_gate"
grep -Fq 'node_a=${STORE_NODE_A_CONTAINER:-mc-rdma-store-node-a}' "$store_gate"
grep -Fq 'node_b=${STORE_NODE_B_CONTAINER:-mc-rdma-store-node-b}' "$store_gate"
grep -Fq 'client=${STORE_CLIENT_CONTAINER:-mc-rdma-store-test-client}' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-node.py" "$node_a:/tmp/store-node.py"' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-node.py" "$node_b:/tmp/store-node.py"' "$store_gate"
grep -Fq 'docker cp "$suite_dir/store-e2e.py" "$client:/tmp/store-e2e.py"' "$store_gate"

for gate in "${gates[@]}"; do
    ! grep -Eq 'docker[[:space:]]+(run|create)' "$gate"
    ! grep -Eq 'fallback|rxe-a|rxe-b' "$gate"
done
