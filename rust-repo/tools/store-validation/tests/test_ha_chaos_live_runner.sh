#!/usr/bin/env bash
set -euo pipefail

# This contract catches a runner that leaks an owned etcd container, invokes
# Docker/Cargo with the wrong boundary arguments, or accepts an incomplete
# live-test result.
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tool_dir=$(cd "$script_dir/.." && pwd)
rust_root=$(cd "$tool_dir/../.." && pwd)
runner="$tool_dir/run-ha-chaos-live.sh"
temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT

fake_bin="$temp_dir/bin"
calls="$temp_dir/calls"
mkdir -p "$fake_bin" "$temp_dir/native"
touch "$temp_dir/native/libtransfer_engine.so"

cat >"$fake_bin/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOONCAKE_HA_CALLS"
case "$1" in
  run)
    if [[ ${MOONCAKE_HA_DOCKER_MODE:-success} == conflict ]]; then
      echo 'Conflict. The container name is already in use.' >&2
      exit 125
    fi
    echo fake-etcd
    ;;
  port) echo '127.0.0.1:42379' ;;
  exec)
    if [[ ${MOONCAKE_HA_DOCKER_MODE:-success} == health-fail ]]; then
      health_attempt=0
      if [[ -f "$MOONCAKE_HA_HEALTH_CALLS" ]]; then
        health_attempt=$(<"$MOONCAKE_HA_HEALTH_CALLS")
      fi
      printf '%s\n' "$((health_attempt + 1))" >"$MOONCAKE_HA_HEALTH_CALLS"
      if ((health_attempt == 0)); then exit 37; fi
      exit 41
    fi
    exit 0
    ;;
  rm)
    if [[ -f ${MOONCAKE_HA_DESCENDANT_PID:-} ]]; then
      descendant_pid=$(<"$MOONCAKE_HA_DESCENDANT_PID")
      if kill -0 "$descendant_pid" 2>/dev/null; then
        echo "owned etcd cleanup ran before Cargo descendant $descendant_pid exited" >&2
        exit 70
      fi
    fi
    exit 0
    ;;
  *) echo "unexpected docker command: $*" >&2; exit 64 ;;
esac
SH

cat >"$fake_bin/cargo" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOONCAKE_HA_CALLS"
case "$1" in
  build) exit 0 ;;
  test)
    printf 'test-env run=%s endpoint=%s master=%s result=%s root=%s seed=%s\n' \
      "${MOONCAKE_RUN_HA_CHAOS:-}" "${MOONCAKE_HA_ETCD_ENDPOINT:-}" \
      "${MOONCAKE_HA_MASTER_BIN:-}" "${MOONCAKE_HA_RESULT:-}" \
      "${MOONCAKE_HA_ARTIFACT_ROOT:-}" "${MOONCAKE_HA_SEED:-}" >>"$MOONCAKE_HA_CALLS"
    case ${MOONCAKE_HA_CARGO_MODE:-pass} in
      pass)
        printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"PASS"}}}' >"$MOONCAKE_HA_RESULT"
        ;;
      malformed) printf '%s\n' '{not json' >"$MOONCAKE_HA_RESULT" ;;
      incomplete)
        printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"FAIL"}}}' >"$MOONCAKE_HA_RESULT"
        ;;
      no-output) : ;;
      fail) exit 9 ;;
      term) while :; do sleep 1; done ;;
      hung-descendant)
        printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"stale","masters":["stale","stale","stale"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"PASS"}}}' >"$MOONCAKE_HA_RESULT"
        (
          trap '' TERM
          printf '%s\n' "$BASHPID" >"$MOONCAKE_HA_DESCENDANT_PID"
          while :; do sleep 1; done
        ) &
        wait "$!"
        ;;
      *) echo "unexpected cargo mode: $MOONCAKE_HA_CARGO_MODE" >&2; exit 64 ;;
    esac
    ;;
  *) echo "unexpected cargo command: $*" >&2; exit 64 ;;
esac
SH
chmod +x "$fake_bin/docker" "$fake_bin/cargo"

cat >"$fake_bin/sleep" <<'SH'
#!/usr/bin/env bash
if [[ ${MOONCAKE_HA_FAST_SLEEP:-0} == 1 ]]; then
  exit 0
fi
exec /bin/sleep "$@"
SH
chmod +x "$fake_bin/sleep"

run_runner() {
  local artifact_root=$1
  local docker_mode=$2
  local cargo_mode=$3
  local fast_sleep=${4:-0}
  local test_timeout_seconds=${5:-30}
  mkdir -p "$artifact_root"
  MOONCAKE_HA_CALLS="$calls" \
  MOONCAKE_HA_DOCKER="$fake_bin/docker" \
  MOONCAKE_HA_CARGO="$fake_bin/cargo" \
  MOONCAKE_HA_DOCKER_MODE="$docker_mode" \
  MOONCAKE_HA_CARGO_MODE="$cargo_mode" \
  MOONCAKE_HA_FAST_SLEEP="$fast_sleep" \
  MOONCAKE_HA_HEALTH_CALLS="$artifact_root/health-calls" \
  MOONCAKE_HA_DESCENDANT_PID="$artifact_root/descendant.pid" \
  MOONCAKE_HA_TEST_TIMEOUT_SECONDS="$test_timeout_seconds" \
  MOONCAKE_HA_TERMINATION_GRACE_SECONDS=1 \
  MOONCAKE_HA_RUN_ID=contract \
  MOONCAKE_HA_ARTIFACT_ROOT="$artifact_root" \
  MOONCAKE_HA_ETCD_IMAGE=quay.io/coreos/etcd:v3.5.0 \
  MOONCAKE_HA_SEED=0x4d4f4f4e48414348 \
  LD_LIBRARY_PATH="$temp_dir/native" \
  RUSTFLAGS="-L native=$temp_dir/native" \
  PATH="$fake_bin:$PATH" \
  bash "$runner"
}

expect_failure() {
  set +e
  "$@"
  local status=$?
  set -e
  [[ $status -ne 0 ]]
}

expect_owned_cleanup() {
  grep -F -- 'rm -f mc-store-ha-chaos-contract' "$calls"
}

expect_status() {
  local expected_status=$1
  shift
  set +e
  "$@"
  local actual_status=$?
  set -e
  [[ $actual_status -eq $expected_status ]]
}

: >"$calls"
artifact_root="$temp_dir/artifacts"
run_runner "$artifact_root" success pass
grep -F -- 'run -d --name mc-store-ha-chaos-contract -p 127.0.0.1::2379' "$calls"
grep -F -- 'port mc-store-ha-chaos-contract 2379/tcp' "$calls"
grep -F -- 'exec mc-store-ha-chaos-contract etcdctl endpoint health' "$calls"
grep -F -- 'build -p mooncake-store-master --bin mooncake-master' "$calls"
grep -F -- 'test -p mooncake-store-client --features link-native --test test_ha_chaos_live' "$calls"
grep -F -- "test-env run=1 endpoint=http://127.0.0.1:42379 master=$rust_root/target/debug/mooncake-master result=$artifact_root/ha-chaos-result.json root=$artifact_root seed=0x4d4f4f4e48414348" "$calls"
expect_owned_cleanup

: >"$calls"
timeout_root="$temp_dir/hung-descendant"
expect_status 124 run_runner "$timeout_root" success hung-descendant 0 1
expect_owned_cleanup
test "$(jq -r .status "$timeout_root/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$timeout_root/ha-chaos-result.json")" = runner_timeout
test "$(jq -r .first_failure.message "$timeout_root/ha-chaos-result.json")" = \
  'HA chaos Cargo/test process group exceeded hard timeout of 1 seconds'
descendant_pid=$(<"$timeout_root/descendant.pid")
for _ in $(seq 1 50); do
  if ! kill -0 "$descendant_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$descendant_pid" 2>/dev/null; then
  echo "runner left timed-out Cargo descendant $descendant_pid alive" >&2
  kill -KILL "$descendant_pid" 2>/dev/null || true
  exit 1
fi
grep -F -- 'HA chaos Cargo/test process group exceeded hard timeout of 1 seconds' \
  "$timeout_root/runner.log"
test "$(jq -r .status "$artifact_root/ha-chaos-result.json")" = PASS
test -s "$artifact_root/runner.log"

: >"$calls"
stale_result_root="$temp_dir/stale-result"
mkdir -p "$stale_result_root"
printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"PASS"}}}' >"$stale_result_root/ha-chaos-result.json"
expect_failure run_runner "$stale_result_root" success no-output
expect_owned_cleanup

: >"$calls"
expect_status 37 run_runner "$temp_dir/health-failure" health-fail pass 1
expect_owned_cleanup

: >"$calls"
expect_failure env -u LD_LIBRARY_PATH \
  MOONCAKE_HA_CALLS="$calls" \
  MOONCAKE_HA_DOCKER="$fake_bin/docker" \
  MOONCAKE_HA_CARGO="$fake_bin/cargo" \
  MOONCAKE_HA_RUN_ID=contract \
  MOONCAKE_HA_ARTIFACT_ROOT="$temp_dir/missing-library-path" \
  bash "$runner"
[[ ! -s "$calls" ]]

for cargo_mode in malformed incomplete fail; do
  : >"$calls"
  expect_failure run_runner "$temp_dir/$cargo_mode" success "$cargo_mode"
  expect_owned_cleanup
done

: >"$calls"
expect_failure run_runner "$temp_dir/reused-name" conflict pass
grep -F -- 'run -d --name mc-store-ha-chaos-contract -p 127.0.0.1::2379' "$calls"
if grep -F -- 'rm -f mc-store-ha-chaos-contract' "$calls"; then
  echo 'runner removed a pre-existing container after a name conflict' >&2
  exit 1
fi

: >"$calls"
run_runner "$temp_dir/term" success term &
runner_pid=$!
for _ in $(seq 1 50); do
  if grep -Fq -- 'test -p mooncake-store-client --features link-native --test test_ha_chaos_live' "$calls"; then
    break
  fi
  sleep 0.1
done
grep -Fq -- 'test -p mooncake-store-client --features link-native --test test_ha_chaos_live' "$calls"
kill -TERM "$runner_pid"
for _ in $(seq 1 50); do
  if ! kill -0 "$runner_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$runner_pid" 2>/dev/null; then
  echo 'runner did not handle TERM while Cargo was running' >&2
  kill -TERM "$(pgrep -P "$runner_pid")" 2>/dev/null || true
  wait "$runner_pid" 2>/dev/null || true
  exit 1
fi
set +e
wait "$runner_pid"
status=$?
set -e
[[ $status -ne 0 ]]
expect_owned_cleanup
