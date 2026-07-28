#!/usr/bin/env bash
set -u
set -o pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
rust_root=$(cd "$script_dir/../.." && pwd)

docker_cmd=${MOONCAKE_HA_DOCKER:-docker}
cargo_cmd=${MOONCAKE_HA_CARGO:-cargo}
etcd_image=${MOONCAKE_HA_ETCD_IMAGE:-quay.io/coreos/etcd:v3.5.0}
seed=${MOONCAKE_HA_SEED:-0x4d4f4f4e48414348}
run_id=${MOONCAKE_HA_RUN_ID:-"$(date -u +%Y%m%dT%H%M%SZ)-$$"}
artifact_root=${MOONCAKE_HA_ARTIFACT_ROOT:-}

require_command() {
  local configured_command=$1
  if ! command -v "$configured_command" >/dev/null 2>&1; then
    echo "required command is unavailable: $configured_command" >&2
    return 1
  fi
}

validate_run_id() {
  [[ $run_id =~ ^[A-Za-z0-9_.-]+$ ]]
}

if [[ -z "$artifact_root" ]]; then
  echo 'MOONCAKE_HA_ARTIFACT_ROOT is required' >&2
  exit 2
fi
if [[ -z ${LD_LIBRARY_PATH:-} ]]; then
  echo 'LD_LIBRARY_PATH is required for the native Transfer Engine libraries' >&2
  exit 2
fi
if ! validate_run_id; then
  echo "invalid MOONCAKE_HA_RUN_ID: $run_id" >&2
  exit 2
fi
if ! require_command "$docker_cmd" || ! require_command "$cargo_cmd" || ! require_command python3; then
  exit 2
fi
if ! mkdir -p "$artifact_root"; then
  exit 2
fi
artifact_root=$(cd "$artifact_root" && pwd)
runner_log="$artifact_root/runner.log"
result_path="$artifact_root/ha-chaos-result.json"
etcd_container="mc-store-ha-chaos-$run_id"
master_bin="${CARGO_TARGET_DIR:-$rust_root/target}/debug/mooncake-master"
first_status=0
etcd_owned=0
active_pid=""

exec > >(tee -a "$runner_log") 2>&1

cleanup() {
  local cleanup_status=0
  if ((etcd_owned)); then
    "$docker_cmd" rm -f "$etcd_container"
    cleanup_status=$?
    etcd_owned=0
  fi
  return "$cleanup_status"
}

on_exit() {
  local exit_status=$?
  local cleanup_status
  if ((first_status != 0)); then
    exit_status=$first_status
  fi
  cleanup
  cleanup_status=$?
  if ((exit_status == 0 && cleanup_status != 0)); then
    exit_status=$cleanup_status
  fi
  trap - EXIT
  exit "$exit_status"
}

on_signal() {
  local signal_status=$1
  if [[ -n "$active_pid" ]]; then
    kill -TERM "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
    active_pid=""
  fi
  if ((first_status == 0)); then
    first_status=$signal_status
  fi
  exit "$signal_status"
}

wait_for_etcd() {
  local attempt status
  for attempt in $(seq 1 30); do
    "$docker_cmd" exec "$etcd_container" etcdctl endpoint health
    status=$?
    if ((status == 0)); then
      return 0
    fi
    sleep 1
  done
  return 1
}

validate_result() {
  python3 - "$result_path" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
try:
    result = json.loads(path.read_text(encoding="utf-8"))
except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
    raise SystemExit(f"invalid HA chaos result JSON: {error}")

if not isinstance(result, dict):
    raise SystemExit("HA chaos result must be a JSON object")
if result.get("schema_version") != 1:
    raise SystemExit("HA chaos result must have schema_version 1")
if result.get("status") != "PASS":
    raise SystemExit("HA chaos result status must be PASS")
if not isinstance(result.get("seed"), str):
    raise SystemExit("HA chaos result must include a string seed")
masters = result.get("masters")
if not isinstance(masters, list) or len(masters) != 3 or not all(isinstance(master, str) for master in masters):
    raise SystemExit("HA chaos result must include three master addresses")
scenarios = result.get("scenarios")
if not isinstance(scenarios, dict):
    raise SystemExit("HA chaos result must include scenarios")
for name in ("small", "large"):
    scenario = scenarios.get(name)
    if not isinstance(scenario, dict) or scenario.get("status") != "PASS":
        raise SystemExit(f"HA chaos result scenario {name} must PASS")
PY
}

trap on_exit EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

cd "$rust_root"
"$docker_cmd" run -d --name "$etcd_container" \
  -p 127.0.0.1::2379 "$etcd_image" \
  etcd --data-dir=/tmp/etcd-data \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://127.0.0.1:2379
status=$?
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi
etcd_owned=1

if ! wait_for_etcd; then
  first_status=1
  exit 1
fi

published_port=$("$docker_cmd" port "$etcd_container" 2379/tcp)
status=$?
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi
if [[ -z "$published_port" ]]; then
  first_status=1
  exit 1
fi
etcd_endpoint="http://$published_port"

"$cargo_cmd" build -p mooncake-store-master --bin mooncake-master
status=$?
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi

MOONCAKE_RUN_HA_CHAOS=1 \
MOONCAKE_HA_ETCD_ENDPOINT="$etcd_endpoint" \
MOONCAKE_HA_MASTER_BIN="$master_bin" \
MOONCAKE_HA_RESULT="$result_path" \
MOONCAKE_HA_ARTIFACT_ROOT="$artifact_root" \
MOONCAKE_HA_SEED="$seed" \
"$cargo_cmd" test -p mooncake-store-client --features link-native --test test_ha_chaos_live &
active_pid=$!
wait "$active_pid"
status=$?
active_pid=""
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi

if ! validate_result; then
  first_status=1
  exit 1
fi
