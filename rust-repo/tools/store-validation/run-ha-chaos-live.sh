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
test_timeout_seconds=${MOONCAKE_HA_TEST_TIMEOUT_SECONDS:-900}
termination_grace_seconds=${MOONCAKE_HA_TERMINATION_GRACE_SECONDS:-5}
max_test_timeout_seconds=86400
max_termination_grace_seconds=300

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

validate_bounded_seconds() {
  local name=$1
  local value=$2
  local maximum=$3
  if ! [[ $value =~ ^[1-9][0-9]*$ ]] ||
    ((${#value} > ${#maximum})) ||
    ((${#value} == ${#maximum} && 10#$value > 10#$maximum)); then
    echo "$name must be an integer from 1 through $maximum: $value" >&2
    return 1
  fi
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
if ! validate_bounded_seconds \
  MOONCAKE_HA_TEST_TIMEOUT_SECONDS "$test_timeout_seconds" "$max_test_timeout_seconds"; then
  exit 2
fi
if ! validate_bounded_seconds \
  MOONCAKE_HA_TERMINATION_GRACE_SECONDS "$termination_grace_seconds" \
  "$max_termination_grace_seconds"; then
  exit 2
fi
if ! require_command "$docker_cmd" || ! require_command "$cargo_cmd" || \
  ! require_command python3 || ! require_command setsid; then
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
active_pgid=""
timeout_marker="$artifact_root/.cargo-test-timeout-$run_id"
watchdog_error="$artifact_root/.cargo-test-watchdog-error-$run_id"
cargo_status_path="$artifact_root/.cargo-test-status-$run_id"
wrapper_ready_path="$artifact_root/.cargo-test-wrapper-ready-$run_id"

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

terminate_active_group() {
  if [[ -n "$active_pgid" ]]; then
    if ! kill -TERM -- "-$active_pgid" 2>/dev/null; then
      kill -TERM "$active_pid" 2>/dev/null || true
    fi
    sleep "$termination_grace_seconds"
    kill -KILL -- "-$active_pgid" 2>/dev/null || true
    kill -KILL "$active_pid" 2>/dev/null || true
  fi
  if [[ -n "$active_pid" ]]; then
    wait "$active_pid" 2>/dev/null || true
  fi
  active_pid=""
  active_pgid=""
}

publish_failure_result() {
  local stage=$1
  local reason=$2
  python3 - "$result_path" "$seed" "$stage" "$reason" <<'PY'
import json
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
temporary = path.with_name(f".{path.name}.timeout-{os.getpid()}")
result = {
    "schema_version": 1,
    "status": "FAIL",
    "seed": sys.argv[2],
    "masters": [],
    "scenarios": {
        "small": {"status": "FAIL", "evidence": {}},
        "large": {"status": "FAIL", "evidence": {}},
    },
    "first_failure": {"stage": sys.argv[3], "message": sys.argv[4]},
}
temporary.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
os.replace(temporary, path)
PY
}

publish_timeout_result() {
  local reason="HA chaos Cargo/test process group exceeded hard timeout of $test_timeout_seconds seconds"
  echo "$reason" >&2
  publish_failure_result runner_timeout "$reason"
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
  terminate_active_group
  if ((first_status == 0)); then
    first_status=$signal_status
  fi
  exit "$signal_status"
}

wait_for_etcd() {
  local attempt status first_failure=0
  for attempt in $(seq 1 30); do
    "$docker_cmd" exec "$etcd_container" etcdctl endpoint health
    status=$?
    if ((status == 0)); then
      return 0
    fi
    if ((first_failure == 0)); then
      first_failure=$status
    fi
    sleep 1
  done
  return "$first_failure"
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

wait_for_etcd
status=$?
if ((status != 0)); then
  first_status=$status
  exit "$status"
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

rm -f -- "$result_path"
status=$?
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi
rm -f -- "$timeout_marker" "$watchdog_error" "$cargo_status_path" \
  "$wrapper_ready_path"
status=$?
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi

setsid env \
  -u MOONCAKE_RUN_HA_LIVENESS_PREFLIGHT \
  MOONCAKE_RUN_HA_CHAOS=1 \
  MOONCAKE_HA_ETCD_ENDPOINT="$etcd_endpoint" \
  MOONCAKE_HA_MASTER_BIN="$master_bin" \
  MOONCAKE_HA_RESULT="$result_path" \
  MOONCAKE_HA_ARTIFACT_ROOT="$artifact_root" \
  MOONCAKE_HA_SEED="$seed" \
  python3 - "$test_timeout_seconds" "$termination_grace_seconds" \
  "$timeout_marker" "$watchdog_error" "$cargo_status_path" \
  "$wrapper_ready_path" "$cargo_cmd" test -p mooncake-store-client \
  --features link-native --test test_ha_chaos_live <<'PY' &
import os
import pathlib
import signal
import subprocess
import sys
import time

timeout_seconds = int(sys.argv[1])
grace_seconds = int(sys.argv[2])
marker = pathlib.Path(sys.argv[3])
error_path = pathlib.Path(sys.argv[4])
status_path = pathlib.Path(sys.argv[5])
wrapper_ready_path = pathlib.Path(sys.argv[6])
command = sys.argv[7:]
process = None
signal.signal(signal.SIGTERM, lambda _signum, _frame: None)


def shell_status(returncode):
    return returncode if returncode >= 0 else 128 - returncode


def record_normal_completion(returncode):
    status = shell_status(returncode)
    status_path.write_text(f"{status}\n", encoding="utf-8")
    raise SystemExit(status)


def terminate_process_group():
    process_group = os.getpgrp()
    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        return
    if process is not None:
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            pass
    else:
        time.sleep(grace_seconds)
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        pass

try:
    if os.environ.get("MOONCAKE_HA_TEST_PAUSE_BEFORE_CARGO") == "1":
        wrapper_ready_path.touch()
        signal.pause()
        raise SystemExit(143)
    process = subprocess.Popen(command)
    try:
        record_normal_completion(process.wait(timeout=timeout_seconds))
    except subprocess.TimeoutExpired:
        pass

    marker.touch()
    terminate_process_group()
    raise SystemExit(124)
except SystemExit:
    raise
except BaseException as error:
    try:
        error_path.write_text(f"{type(error).__name__}: {error}\n", encoding="utf-8")
    finally:
        terminate_process_group()
        if process is not None:
            process.wait()
    raise SystemExit(125)
PY
active_pid=$! active_pgid=$!
wait "$active_pid" 2>/dev/null
status=$?
if [[ -e "$timeout_marker" ]]; then
  active_pid=""
  active_pgid=""
  publish_timeout_result
  first_status=124
  exit 124
fi
if [[ -s "$watchdog_error" ]] || [[ ! -s "$cargo_status_path" ]]; then
  terminate_active_group
  if [[ -s "$watchdog_error" ]]; then
    watchdog_reason="HA chaos watchdog failed: $(<"$watchdog_error")"
  else
    watchdog_reason="HA chaos watchdog exited unexpectedly with status $status"
  fi
  echo "$watchdog_reason" >&2
  publish_failure_result runner_watchdog "$watchdog_reason"
  first_status=125
  exit 125
fi
active_pid=""
active_pgid=""
if ((status != 0)); then
  first_status=$status
  exit "$status"
fi

if ! validate_result; then
  first_status=1
  exit 1
fi
