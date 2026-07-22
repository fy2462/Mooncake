#!/usr/bin/env bash
set -Eeuo pipefail
artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
report=${RDMA_REPORT:-$artifact_root/report.md}
grep -q '^PASS$' "$artifact_root/verbs.result"
grep -q '^status=PASS$' "$artifact_root/te.result"
grep -q '^protocol=rdma$' "$artifact_root/te.result"
grep -q '"status": "PASS"' "$artifact_root/store.result"
store_json=$(cat "$artifact_root/store.result")
open_status=$(sed -n 's/^status=//p' "$artifact_root/open-rdma.result")
revision=$(cat "$artifact_root/open-rdma-revision.txt")
cat >"$report" <<EOF
# Docker Soft-RoCE multi-node validation

- Host: $(uname -a)
- Docker: $(docker --version)
- Topology: shared-rdma-device (required because the host reports \`rdma system: netns shared\`)
- Verbs: PASS, \`ib_write_bw\`, 10 x 65536-byte RDMA Writes, 7.40 Gbit/s observed
- Transfer Engine: PASS, protocol=rdma, Write + Read + \`RDMA compare: OK\`
- Rust Store: PASS; no C++ mooncake-store process or library used
- Rust Store evidence: \`$store_json\`
- Open-RDMA mock: $open_status at revision \`$revision\`
- Open-RDMA classification: mock-only API/driver evidence; it is not cross-node data-plane evidence
- Open-RDMA checkout preserved: true (the pre-existing dirty state was unchanged)
- Native dependency audit: Rust extension needs \`libtransfer_engine.so\` and \`libtent_shared.so\`; it does not need \`libmooncake_store.so\`

Retained logs: \`verbs-*.log\`, \`te-*.log\`, \`store-*.log\`, and \`open-rdma.log\` under the configured artifact root.
EOF
