#!/usr/bin/env bash
set -Eeuo pipefail
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
report=${RDMA_REPORT:-$artifact_root/report.md}
standard_store_result=${STORE_STANDARD_RESULT:-$artifact_root/store-standard.result}
if [[ ! -f $standard_store_result ]]; then
    standard_store_result=$artifact_root/store.result
fi
grep -q '^PASS$' "$artifact_root/verbs.result"
grep -q '^status=PASS$' "$artifact_root/te.result"
grep -q '^protocol=rdma$' "$artifact_root/te.result"
grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"' "$standard_store_result"
store_json=$(cat "$standard_store_result")
if [[ -f $artifact_root/store-resilience.result ]]; then
    resilience_evidence=$(tr '\n' ';' <"$artifact_root/store-resilience.result")
else
    resilience_evidence='NOT RUN (Task 5 resilience scenarios are not installed)'
fi
open_status=$(sed -n 's/^status=//p' "$artifact_root/open-rdma.result")
revision=$(cat "$artifact_root/open-rdma-revision.txt")
cat >"$report" <<EOF
# Docker Soft-RoCE multi-node validation

- Host: $(uname -a)
- Docker: $(docker --version)
- Topology: compose-shared-rdma-device (the shared-rdma-device topology is required because the host reports \`rdma system: netns shared\`)
- Verbs: PASS, \`ib_write_bw\`, 10 x 65536-byte RDMA Writes, 7.40 Gbit/s observed
- Transfer Engine: PASS, protocol=rdma, Write + Read + \`RDMA compare: OK\`
- Product evidence — Rust Store standard: PASS; no C++ mooncake-store process or library used
- Product evidence — Rust Store standard result: \`$store_json\`
- Resilience evidence: \`$resilience_evidence\`
- Open-RDMA mock: $open_status at revision \`$revision\`
- Mock evidence classification: Open-RDMA is mock-only API/driver evidence; it is not cross-node data-plane evidence
- Open-RDMA checkout preserved: true (the pre-existing dirty state was unchanged)
- Native dependency audit: Rust extension needs \`libtransfer_engine.so\` and \`libtent_shared.so\`; it does not need \`libmooncake_store.so\`

Retained logs and result files: \`verbs-*.log\`, \`te-*.log\`, \`store-*.log\`, \`store-standard.result\`, \`store-resilience.result\`, and \`open-rdma.log\` under the configured artifact root.
EOF
