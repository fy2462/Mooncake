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

write_complete_pass() {
  cat >"$MOONCAKE_HA_RESULT" <<'JSON'
{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS","evidence":{"eviction_requests":0,"capacity_rejections":0,"successful_unstable_operations":1,"stable_exact_reads":400,"byte_comparisons":400,"crashes":4,"restarts":4,"leader_view_versions":[1,2,3,4,5,6,7,8],"stopped_indices":[0,1,2],"restarted_indices":[0,1,2]}},"large":{"status":"PASS","evidence":{"eviction_requests":1,"capacity_rejections":0,"successful_unstable_operations":1,"stable_exact_reads":168,"byte_comparisons":528482304,"crashes":4,"restarts":4,"leader_view_versions":[1,2,3,4,5,6,7,8],"stopped_indices":[0,1,2],"restarted_indices":[0,1,2]}}},"first_failure":null}
JSON
}

mutate_complete_pass() {
  local filter=$1
  write_complete_pass
  local temporary="$MOONCAKE_HA_RESULT.mutated"
  jq "$filter" "$MOONCAKE_HA_RESULT" >"$temporary"
  mv "$temporary" "$MOONCAKE_HA_RESULT"
}

case "$1" in
  build)
    if [[ ${MOONCAKE_HA_CARGO_MODE:-pass} == build-fail ]]; then
      exit 8
    fi
    exit 0
    ;;
  test)
    printf 'test-env run=%s liveness=%s endpoint=%s master=%s result=%s root=%s seed=%s\n' \
      "${MOONCAKE_RUN_HA_CHAOS:-}" "${MOONCAKE_RUN_HA_LIVENESS_PREFLIGHT:-}" \
      "${MOONCAKE_HA_ETCD_ENDPOINT:-}" \
      "${MOONCAKE_HA_MASTER_BIN:-}" "${MOONCAKE_HA_RESULT:-}" \
      "${MOONCAKE_HA_ARTIFACT_ROOT:-}" "${MOONCAKE_HA_SEED:-}" >>"$MOONCAKE_HA_CALLS"
    case ${MOONCAKE_HA_CARGO_MODE:-pass} in
      pass) write_complete_pass ;;
      late-pass)
        sleep 2
        write_complete_pass
        ;;
      malformed) printf '%s\n' '{not json' >"$MOONCAKE_HA_RESULT" ;;
      incomplete)
        printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"FAIL"}}}' >"$MOONCAKE_HA_RESULT"
        ;;
      no-output) : ;;
      fail) exit 9 ;;
      rust-fail)
        printf '%s\n' '{"schema_version":1,"status":"FAIL","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"FAIL","evidence":{}},"large":{"status":"FAIL","evidence":{}}},"first_failure":{"stage":"rust-gate","message":"injected Rust gate failure"}}' >"$MOONCAKE_HA_RESULT"
        exit 9
        ;;
      evidence-first-failure) mutate_complete_pass '.first_failure = {"stage":"stale","message":"must be null"}' ;;
      evidence-seed) mutate_complete_pass '.seed = "0x1"' ;;
      evidence-masters) mutate_complete_pass '.masters[2] = .masters[1]' ;;
      evidence-unstable) mutate_complete_pass '.scenarios.small.evidence.successful_unstable_operations = 0' ;;
      evidence-stable-reads) mutate_complete_pass '.scenarios.large.evidence.stable_exact_reads = 167' ;;
      evidence-bytes) mutate_complete_pass '.scenarios.large.evidence.byte_comparisons = 1' ;;
      evidence-crash-restart) mutate_complete_pass '.scenarios.small.evidence.restarts = 3' ;;
      evidence-crash-coverage) mutate_complete_pass '.scenarios.small.evidence.crashes = 0 | .scenarios.small.evidence.restarts = 0' ;;
      evidence-victims) mutate_complete_pass '.scenarios.large.evidence.stopped_indices = [0,1]' ;;
      evidence-leader-views) mutate_complete_pass '.scenarios.large.evidence.leader_view_versions = [1,2,3]' ;;
      evidence-pressure) mutate_complete_pass '.scenarios.large.evidence.eviction_requests = 0 | .scenarios.large.evidence.capacity_rejections = 0' ;;
      evidence-shape) mutate_complete_pass '.scenarios.small.evidence.crashes = "4"' ;;
      term) while :; do sleep 1; done ;;
      hung-descendant)
        write_complete_pass
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
  local termination_grace_seconds=${6:-1}
  local pause_before_cargo=${7:-0}
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
  MOONCAKE_HA_TERMINATION_GRACE_SECONDS="$termination_grace_seconds" \
  MOONCAKE_HA_TEST_PAUSE_BEFORE_CARGO="$pause_before_cargo" \
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
MOONCAKE_RUN_HA_LIVENESS_PREFLIGHT=1 run_runner "$artifact_root" success pass
grep -F -- 'run -d --name mc-store-ha-chaos-contract -p 127.0.0.1::2379' "$calls"
grep -F -- 'port mc-store-ha-chaos-contract 2379/tcp' "$calls"
grep -F -- 'exec mc-store-ha-chaos-contract etcdctl endpoint health' "$calls"
grep -F -- 'build -p mooncake-store-master --bin mooncake-master' "$calls"
grep -F -- 'test -p mooncake-store-client --features link-native --test test_ha_chaos_live' "$calls"
grep -F -- "test-env run=1 liveness= endpoint=http://127.0.0.1:42379 master=$rust_root/target/debug/mooncake-master result=$artifact_root/ha-chaos-result.json root=$artifact_root seed=0x4d4f4f4e48414348" "$calls"
expect_owned_cleanup

for cargo_mode in evidence-first-failure evidence-seed evidence-masters evidence-unstable \
  evidence-stable-reads evidence-bytes evidence-crash-restart evidence-crash-coverage \
  evidence-victims evidence-leader-views evidence-pressure evidence-shape; do
  : >"$calls"
  evidence_root="$temp_dir/$cargo_mode"
  expect_failure run_runner "$evidence_root" success "$cargo_mode"
  expect_owned_cleanup
  test "$(jq -r .status "$evidence_root/ha-chaos-result.json")" = FAIL
  test "$(jq -r .first_failure.stage "$evidence_root/ha-chaos-result.json")" = runner_result_validation
done

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

: >"$calls"
late_pass_root="$temp_dir/late-pass"
expect_status 124 run_runner "$late_pass_root" success late-pass 0 1 1
expect_owned_cleanup
test "$(jq -r .status "$late_pass_root/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$late_pass_root/ha-chaos-result.json")" = runner_timeout
test "$(jq -r .status "$artifact_root/ha-chaos-result.json")" = PASS
test -s "$artifact_root/runner.log"

: >"$calls"
stale_result_root="$temp_dir/stale-result"
mkdir -p "$stale_result_root"
printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"PASS"}}}' >"$stale_result_root/ha-chaos-result.json"
expect_failure run_runner "$stale_result_root" success no-output
expect_owned_cleanup
test "$(jq -r .status "$stale_result_root/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$stale_result_root/ha-chaos-result.json")" = runner_result_validation

: >"$calls"
health_failure_root="$temp_dir/health-failure"
mkdir -p "$health_failure_root"
printf '%s\n' '{"schema_version":1,"status":"PASS","seed":"stale"}' >"$health_failure_root/ha-chaos-result.json"
expect_status 37 run_runner "$health_failure_root" health-fail pass 1
expect_owned_cleanup
test "$(jq -r .status "$health_failure_root/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$health_failure_root/ha-chaos-result.json")" = runner_etcd_health

: >"$calls"
expect_failure env -u LD_LIBRARY_PATH \
  MOONCAKE_HA_CALLS="$calls" \
  MOONCAKE_HA_DOCKER="$fake_bin/docker" \
  MOONCAKE_HA_CARGO="$fake_bin/cargo" \
  MOONCAKE_HA_RUN_ID=contract \
  MOONCAKE_HA_ARTIFACT_ROOT="$temp_dir/missing-library-path" \
  bash "$runner"
[[ ! -s "$calls" ]]

for timing_case in '86401 1' '30 301' '999999999999999999999999999999 1'; do
  : >"$calls"
  read -r invalid_timeout invalid_grace <<<"$timing_case"
  expect_status 2 run_runner "$temp_dir/timing-$invalid_timeout-$invalid_grace" \
    success pass 0 "$invalid_timeout" "$invalid_grace"
  if [[ -s "$calls" ]]; then
    echo "invalid timing values caused Docker/Cargo side effects: $timing_case" >&2
    exit 1
  fi
done

for cargo_mode in malformed incomplete fail; do
  : >"$calls"
  failure_root="$temp_dir/$cargo_mode"
  expect_failure run_runner "$failure_root" success "$cargo_mode"
  expect_owned_cleanup
  test "$(jq -r .status "$failure_root/ha-chaos-result.json")" = FAIL
  if [[ $cargo_mode == fail ]]; then
    test "$(jq -r .first_failure.stage "$failure_root/ha-chaos-result.json")" = runner_cargo_test
  else
    test "$(jq -r .first_failure.stage "$failure_root/ha-chaos-result.json")" = runner_result_validation
  fi
done

: >"$calls"
build_failure_root="$temp_dir/stale-build-failure"
mkdir -p "$build_failure_root"
write_stale_result='{"schema_version":1,"status":"PASS","seed":"stale"}'
printf '%s\n' "$write_stale_result" >"$build_failure_root/ha-chaos-result.json"
expect_status 8 run_runner "$build_failure_root" success build-fail
expect_owned_cleanup
test "$(jq -r .status "$build_failure_root/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$build_failure_root/ha-chaos-result.json")" = runner_cargo_build

: >"$calls"
rust_failure_root="$temp_dir/rust-fail"
expect_status 9 run_runner "$rust_failure_root" success rust-fail
expect_owned_cleanup
test "$(jq -r .first_failure.stage "$rust_failure_root/ha-chaos-result.json")" = rust-gate
test "$(jq -r .first_failure.message "$rust_failure_root/ha-chaos-result.json")" = 'injected Rust gate failure'

: >"$calls"
expect_failure run_runner "$temp_dir/reused-name" conflict pass
grep -F -- 'run -d --name mc-store-ha-chaos-contract -p 127.0.0.1::2379' "$calls"
if grep -F -- 'rm -f mc-store-ha-chaos-contract' "$calls"; then
  echo 'runner removed a pre-existing container after a name conflict' >&2
  exit 1
fi

: >"$calls"
early_term_root="$temp_dir/early-term"
run_runner "$early_term_root" success term 0 30 1 1 &
runner_pid=$!
wrapper_ready="$early_term_root/.cargo-test-wrapper-ready-contract"
for _ in $(seq 1 50); do
  if [[ -e "$wrapper_ready" ]]; then
    break
  fi
  sleep 0.1
done
if [[ ! -e "$wrapper_ready" ]]; then
  kill -TERM "$runner_pid" 2>/dev/null || true
  wait "$runner_pid" 2>/dev/null || true
  echo 'runner did not reach the wrapper-controlled pre-Cargo pause' >&2
  exit 1
fi
if grep -Fq -- 'test -p mooncake-store-client --features link-native --test test_ha_chaos_live' "$calls"; then
  echo 'Cargo started before the wrapper-controlled pre-Cargo pause' >&2
  exit 1
fi
kill -TERM "$runner_pid"
for _ in $(seq 1 50); do
  if ! kill -0 "$runner_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$runner_pid" 2>/dev/null; then
  wrapper_pid=$(pgrep -P "$runner_pid" | head -1 || true)
  if [[ -n "$wrapper_pid" ]]; then
    kill -KILL -- "-$wrapper_pid" 2>/dev/null || true
    kill -KILL "$wrapper_pid" 2>/dev/null || true
  fi
  kill -KILL "$runner_pid" 2>/dev/null || true
  wait "$runner_pid" 2>/dev/null || true
  echo 'runner did not handle TERM before Cargo launch' >&2
  exit 1
fi
set +e
wait "$runner_pid"
status=$?
set -e
[[ $status -eq 143 ]]
expect_owned_cleanup
test "$(jq -r .status "$early_term_root/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$early_term_root/ha-chaos-result.json")" = runner_signal

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
test "$(jq -r .status "$temp_dir/term/ha-chaos-result.json")" = FAIL
test "$(jq -r .first_failure.stage "$temp_dir/term/ha-chaos-result.json")" = runner_signal
