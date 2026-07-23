#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
store_gate=$suite_dir/run-store-gate.sh
grep -Fq "trap 'cleanup_store_processes 130' INT" "$store_gate"
grep -Fq "trap 'cleanup_store_processes 143' TERM" "$store_gate"
tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT
fake_bin=$tmp_dir/bin
mkdir -p "$fake_bin"

cat >"$fake_bin/docker" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

operation=${1:-}
shift || true
case $operation in
    cp) exit 0 ;;
    exec)
        container=$1
        shift
        if [[ ${1:-} == /opt/mooncake-rdma/store/mooncake-master ]]; then
            while :; do sleep 1; done
        elif [[ ${1:-} == python3 && ${2:-} == /tmp/store-node.py ]]; then
            arguments=("$@")
            ready=
            stats=
            for ((index = 0; index < ${#arguments[@]}; index++)); do
                case ${arguments[index]} in
                    --ready) ready=${arguments[index + 1]} ;;
                    --stats) stats=${arguments[index + 1]} ;;
                esac
            done
            ready=$RDMA_ARTIFACT_ROOT/${ready#/artifacts/}
            stats=$RDMA_ARTIFACT_ROOT/${stats#/artifacts/}
            printf '{"storage_backend": "RustFilePerKey", "cpp_store_loaded": false, "pid": %s}\n' \
                "$$" >"$ready"
            printf '{"cycles":1,"last_error":""}\n' >"$stats"
            while :; do sleep 1; done
        elif [[ $container == "$STORE_CLIENT_CONTAINER" && "$*" == *socket.create_connection* ]]; then
            exit 0
        elif [[ $container == "$STORE_CLIENT_CONTAINER" && "$*" == *'/tmp/store-e2e.py'* ]]; then
            : >"$STORE_SIGNAL_MARKER"
            while :; do sleep 1; done
        else
            exit 0
        fi
        ;;
    *) exit 0 ;;
esac
EOF
chmod +x "$fake_bin/docker"

for signal_name in INT TERM; do
    artifact_root=$tmp_dir/artifacts-$signal_name
    mkdir -p "$artifact_root"
    printf 'status=PASS\nprotocol=rdma\n' >"$artifact_root/te.result"
    marker=$artifact_root/client-started
    export PATH="$fake_bin:$PATH"
    export RDMA_ARTIFACT_ROOT="$artifact_root"
    export STORE_CLIENT_CONTAINER=client
    export STORE_MASTER_CONTAINER=master
    export STORE_NODE_A_CONTAINER=node-a
    export STORE_NODE_B_CONTAINER=node-b
    export STORE_SIGNAL_MARKER="$marker"

    setsid python3 -c '
import os
import signal
import sys

signal.signal(signal.SIGINT, signal.SIG_DFL)
os.execv(sys.argv[1], sys.argv[1:])
' "$store_gate" &
    run_pid=$!
    deadline=$((SECONDS + 10))
    while [[ ! -f $marker && $SECONDS -le $deadline ]]; do sleep 0.05; done
    if [[ ! -f $marker ]]; then
        printf 'Store gate did not reach the client scenario\n' >&2
        kill -s TERM -- "-$run_pid" 2>/dev/null || true
        wait "$run_pid" 2>/dev/null || true
        exit 1
    fi

    kill -s "$signal_name" -- "-$run_pid"
    if wait "$run_pid"; then
        printf 'Store gate converted %s to success\n' "$signal_name" >&2
        exit 1
    else
        status=$?
    fi
    case $signal_name in
        INT) test "$status" -eq 130 ;;
        TERM) test "$status" -eq 143 ;;
    esac
    grep -Fq '"status":"FAIL"' "$artifact_root/store.result"
done
