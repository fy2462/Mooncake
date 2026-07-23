#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=lib/common.sh
source "$suite_dir/lib/common.sh"

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
server=${VERBS_SERVER_CONTAINER:-mc-rdma-te-node-a}
client=${VERBS_CLIENT_CONTAINER:-mc-rdma-te-node-b}
server_ip=${VERBS_SERVER_IP:-127.0.0.1}
server_dev=${VERBS_SERVER_DEVICE:-mc-rdma-rxe}
client_dev=${VERBS_CLIENT_DEVICE:-mc-rdma-rxe}
server_log="$artifact_root/verbs-server.log"
client_log="$artifact_root/verbs-client.log"
result="$artifact_root/verbs.result"

: >"$server_log"
: >"$client_log"
printf 'FAIL\ntool=ib_write_bw\n' >"$result"

docker exec "$server" sh -lc \
    "timeout 45 ib_write_bw -d '$server_dev' --report_gbits" \
    >"$server_log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true' EXIT
sleep 2
if ! docker exec "$client" sh -lc \
    "timeout 30 ib_write_bw -d '$client_dev' '$server_ip' --report_gbits -n 10" \
    >"$client_log" 2>&1; then
    printf 'verbs client failed; see %s\n' "$client_log" >&2
    exit 1
fi

if grep -Eqi 'error|failed|unable|could not|zero byte' "$client_log"; then
    printf 'verbs client log contains an error marker\n' >&2
    exit 1
fi
awk '/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+/ {seen=1} END {exit !seen}' "$client_log" || {
    printf 'verbs client did not report transferred bytes\n' >&2
    exit 1
}
printf 'PASS\ntool=ib_write_bw\nserver_device=%s\nclient_device=%s\n' \
    "$server_dev" "$client_dev" >"$result"
