#!/usr/bin/env bash
set -Eeuo pipefail
suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
printf 'PASS\ntool=ib_write_bw\n' >"$tmp_dir/verbs.result"
printf 'status=FAIL\nprotocol=rdma\n' >"$tmp_dir/te.result"
printf '{"status": "PASS"}\n' >"$tmp_dir/store.result"
cat >"$tmp_dir/store-resilience.result" <<'EOF'
{"status":"PASS","first_failure":null,"scenarios":{"store_node_restart":{"status":"PASS"},"master_etcd_restart":{"status":"PASS"},"rdma_reconnect":{"status":"PASS"},"degraded_read":{"status":"PASS","evidence":{"initial_owners":2,"remaining_owners":1}},"watermark_eviction":{"status":"PASS"},"mixed_stress":{"status":"PASS"},"second_standard":{"status":"PASS"}}}
EOF
printf 'status=PASS\n' >"$tmp_dir/open-rdma.result"
printf 'deadbeef\n' >"$tmp_dir/open-rdma-revision.txt"
if RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/render-report.sh"; then
    printf 'report accepted Store PASS with TE FAIL\n' >&2
    exit 1
fi
cat >"$tmp_dir/te.result" <<'EOF'
status=PASS
protocol=rdma
directions=a-b,b-a,b-c,c-b,c-a,a-c
direction_a_b=PASS,target_device=a,initiator_device=b,compare=OK
direction_b_a=PASS,target_device=b,initiator_device=a,compare=OK
direction_b_c=PASS,target_device=b,initiator_device=c,compare=OK
direction_c_b=PASS,target_device=c,initiator_device=b,compare=OK
direction_c_a=PASS,target_device=c,initiator_device=a,compare=OK
direction_a_c=PASS,target_device=a,initiator_device=c,compare=OK
EOF
cat >"$tmp_dir/store.result" <<'EOF'
{"status":"PASS","evidence":{"cpp_store_loaded":false,"objects":{"small":{"remote_segments":["a","b","c"]}},"multilevel":{"fallback":{"replica_type":"LocalDisk","replicas":1},"promotion":{"memory_replicas":1}}}}
EOF
RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/render-report.sh"
grep -q 'mock-only API/driver evidence' "$tmp_dir/report.md"
grep -q 'three RXE devices' "$tmp_dir/report.md"
grep -q 'a-b, b-a, b-c, c-b, c-a, a-c' "$tmp_dir/report.md"
grep -q '3/2/1 replica profiles: PASS' "$tmp_dir/report.md"
grep -q 'Rust Store resilience: PASS' "$tmp_dir/report.md"

printf '{"status":"FAIL","nested":{"status":"PASS"}}\n' >"$tmp_dir/store.result"
if RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/render-report.sh"; then
    printf 'report accepted nested Store PASS with top-level FAIL\n' >&2
    exit 1
fi
cat >"$tmp_dir/store.result" <<'EOF'
{"status":"PASS","evidence":{"cpp_store_loaded":false,"objects":{"small":{"remote_segments":["a","b","c"]}},"multilevel":{"fallback":{"replica_type":"LocalDisk","replicas":1},"promotion":{"memory_replicas":1}}}}
EOF

python3 - "$tmp_dir/store-resilience.result" <<'PY'
import json
import sys
path = sys.argv[1]
value = json.load(open(path, encoding="utf-8"))
value["scenarios"]["rdma_reconnect"]["status"] = "FAIL"
json.dump(value, open(path, "w", encoding="utf-8"))
PY
if RDMA_ARTIFACT_ROOT="$tmp_dir" bash "$suite_dir/render-report.sh"; then
    printf 'report accepted a failed resilience scenario\n' >&2
    exit 1
fi
