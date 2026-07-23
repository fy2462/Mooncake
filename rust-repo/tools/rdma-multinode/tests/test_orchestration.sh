#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT

fake_stage="$tmp_dir/fake-stage"
cat >"$fake_stage" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

stage=$1
printf '%s\n' "$stage" >>"$ORCHESTRATION_LOG"

case "$stage" in
    store-standard)
        if [[ ${STANDARD_DETAIL_FAIL:-0} == 1 ]]; then
            printf '{"status":"FAIL","error":"fallback hash mismatch"}\n' \
                >"$RDMA_ARTIFACT_ROOT/store.result"
        else
            printf '{"status":"PASS"}\n' >"$RDMA_ARTIFACT_ROOT/store.result"
        fi
        ;;
    store-resilience)
        if [[ ${RESILIENCE_TOP_LEVEL_ONLY:-0} == 1 ]]; then
            printf '{"status":"PASS"}\n' >"$RDMA_ARTIFACT_ROOT/store-resilience.result"
        elif [[ ${RESILIENCE_DETAIL_FAIL:-0} == 1 ]]; then
            printf '{"status":"FAIL","first_failure":"rdma_reconnect","scenarios":{"rdma_reconnect":{"status":"FAIL"}}}\n' \
                >"$RDMA_ARTIFACT_ROOT/store-resilience.result"
        else
            printf '%s\n' '{"status":"PASS","first_failure":null,"scenarios":{"store_node_restart":{"status":"PASS","scenario":"store_node_restart","evidence":{"duration_seconds":0.1,"restart":true,"post_restart_read":true}},"master_etcd_restart":{"status":"PASS","scenario":"master_etcd_restart","evidence":{"duration_seconds":0.1,"master_recovered":true,"etcd_recovered":true,"post_restart_read":true}},"rdma_reconnect":{"status":"PASS","scenario":"rdma_reconnect","evidence":{"duration_seconds":0.1,"link_down_observed":true,"interruption_failed_read":true,"link_restored":true,"post_reconnect_read":true}},"degraded_read":{"status":"PASS","scenario":"degraded_read","evidence":{"duration_seconds":0.1,"initial_owners":2,"unavailable_owner":"node-a","remaining_owners":1,"byte_valid":true}},"watermark_eviction":{"status":"PASS","scenario":"watermark_eviction","evidence":{"duration_seconds":0.1,"memory_offloaded":1,"ssd_evicted":1,"high_ratio":0.6,"low_ratio":0.3}},"mixed_stress":{"status":"PASS","scenario":"mixed_stress","evidence":{"duration_seconds":0.1,"operations":12,"sizes":[4096,1048576,8388608],"byte_identical":true}},"second_standard":{"status":"PASS","scenario":"second_standard","evidence":{"duration_seconds":0.1,"standard_status":"PASS","complete":true}}}}' \
                >"$RDMA_ARTIFACT_ROOT/store-resilience.result"
        fi
        ;;
esac

if [[ $stage == store-resilience && ${RESILIENCE_INVALID_EVIDENCE:-0} != 0 ]]; then
    python3 - "$RDMA_ARTIFACT_ROOT/store-resilience.result" <<'PY'
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    result = json.load(stream)
if os.environ["RESILIENCE_INVALID_EVIDENCE"] == "interruption":
    result["scenarios"]["rdma_reconnect"]["evidence"]["interruption_failed_read"] = False
else:
    result["scenarios"]["degraded_read"]["evidence"]["remaining_owners"] = 2
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump(result, stream)
PY
fi

if [[ ${PAUSE_STAGE:-} == "$stage" ]]; then
    while :; do sleep 1; done
fi

if [[ ${FAIL_STAGE:-} == "$stage" ]]; then
    exit "${FAIL_CODE:-1}"
fi
EOF
chmod +x "$fake_stage"

stage_environment() {
    local artifact_root=$1
    export RDMA_ARTIFACT_ROOT="$artifact_root"
    export ORCHESTRATION_LOG="$artifact_root/order.log"
    export RDMA_PREFLIGHT_COMMAND="$fake_stage preflight"
    export RDMA_HOST_RDMA_SETUP_COMMAND="$fake_stage host-rdma-setup"
    export RDMA_BUILD_COMMAND="$fake_stage build"
    export RDMA_COMPOSE_UP_COMMAND="$fake_stage compose-up"
    export RDMA_VERBS_COMMAND="$fake_stage verbs"
    export RDMA_TE_COMMAND="$fake_stage te"
    export RDMA_STORE_STANDARD_COMMAND="$fake_stage store-standard"
    export RDMA_OPEN_RDMA_COMMAND="$fake_stage open-rdma"
    export RDMA_REPORT_COMMAND="$fake_stage report"
    export RDMA_COMPOSE_DOWN_COMMAND="$fake_stage compose-down"
    export RDMA_HOST_RDMA_CLEANUP_COMMAND="$fake_stage host-rdma-cleanup"
}

assert_order() {
    local actual=$1
    shift
    local expected
    expected=$(printf '%s\n' "$@")
    diff -u <(printf '%s\n' "$expected") "$actual"
}

direct_contract_root="$tmp_dir/direct-contract"
mkdir -p "$direct_contract_root"
stage_environment "$direct_contract_root"
unset RDMA_STORE_RESILIENCE_COMMAND
if bash "$suite_dir/run.sh" store-resilience; then
    printf 'accepted a resilience run without its standard prerequisite\n' >&2
    exit 1
fi
grep -Fx 'status=BLOCKED' "$direct_contract_root/store-resilience.result"
grep -Fx 'reason=store-standard-gate' "$direct_contract_root/store-resilience.result"

success_root="$tmp_dir/success"
mkdir -p "$success_root"
stage_environment "$success_root"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
bash "$suite_dir/run.sh" all
assert_order "$success_root/order.log" \
    preflight host-rdma-setup build compose-up verbs te store-standard \
    store-resilience open-rdma report compose-down host-rdma-cleanup
grep -Fx '{"status":"PASS"}' "$success_root/store-standard.result"
grep -Eq '"status"[[:space:]]*:[[:space:]]*"PASS"' \
    "$success_root/store-resilience.result"

mock_failure_root="$tmp_dir/mock-failure"
mkdir -p "$mock_failure_root"
stage_environment "$mock_failure_root"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
export FAIL_STAGE=open-rdma FAIL_CODE=101
bash "$suite_dir/run.sh" all
unset FAIL_STAGE FAIL_CODE
grep -Fx open-rdma "$mock_failure_root/order.log"
grep -Fx report "$mock_failure_root/order.log"

top_level_root="$tmp_dir/top-level-only"
mkdir -p "$top_level_root"
stage_environment "$top_level_root"
printf '{"status":"PASS"}\n' >"$top_level_root/store-standard.result"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
export RESILIENCE_TOP_LEVEL_ONLY=1
if bash "$suite_dir/run.sh" store-resilience; then
    printf 'accepted top-level-only resilience PASS\n' >&2
    exit 1
fi
unset RESILIENCE_TOP_LEVEL_ONLY

for invalid_evidence in interruption degraded-owner-count; do
    invalid_evidence_root="$tmp_dir/invalid-evidence-$invalid_evidence"
    mkdir -p "$invalid_evidence_root"
    stage_environment "$invalid_evidence_root"
    printf '{"status":"PASS"}\n' >"$invalid_evidence_root/store-standard.result"
    export RESILIENCE_INVALID_EVIDENCE=$invalid_evidence
    if bash "$suite_dir/run.sh" store-resilience; then
        printf 'accepted resilience PASS with invalid %s evidence\n' \
            "$invalid_evidence" >&2
        exit 1
    fi
done
unset RESILIENCE_INVALID_EVIDENCE

nested_pass_root="$tmp_dir/nested-pass-prerequisite"
mkdir -p "$nested_pass_root"
stage_environment "$nested_pass_root"
printf '{"status":"FAIL","nested":{"status":"PASS"}}\n' \
    >"$nested_pass_root/store-standard.result"
if bash "$suite_dir/run.sh" store-resilience; then
    printf 'accepted nested PASS as the standard prerequisite\n' >&2
    exit 1
fi
grep -Fx 'status=BLOCKED' "$nested_pass_root/store-resilience.result"
grep -Fx 'reason=store-standard-gate' "$nested_pass_root/store-resilience.result"

detailed_failure_root="$tmp_dir/detailed-failure"
mkdir -p "$detailed_failure_root"
stage_environment "$detailed_failure_root"
printf '{"status":"PASS"}\n' >"$detailed_failure_root/store-standard.result"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
export RESILIENCE_DETAIL_FAIL=1 FAIL_STAGE=store-resilience FAIL_CODE=33
if bash "$suite_dir/run.sh" store-resilience; then
    printf 'accepted a detailed resilience failure\n' >&2
    exit 1
else
    test $? -eq 33
fi
unset RESILIENCE_DETAIL_FAIL FAIL_STAGE FAIL_CODE
grep -Fq '"first_failure":"rdma_reconnect"' "$detailed_failure_root/store-resilience.result"
! grep -Fq 'runner-exit' "$detailed_failure_root/store-resilience.result"
unset RDMA_STORE_RESILIENCE_COMMAND

standard_detail_root="$tmp_dir/standard-detail"
mkdir -p "$standard_detail_root"
stage_environment "$standard_detail_root"
export STANDARD_DETAIL_FAIL=1 FAIL_STAGE=store-standard FAIL_CODE=37
if bash "$suite_dir/run.sh" store-standard; then
    printf 'accepted a detailed standard Store failure\n' >&2
    exit 1
else
    test $? -eq 37
fi
unset STANDARD_DETAIL_FAIL FAIL_STAGE FAIL_CODE
grep -Fq 'fallback hash mismatch' "$standard_detail_root/store-standard.result"
! grep -Fq 'runner-exit' "$standard_detail_root/store-standard.result"

not_implemented_root="$tmp_dir/not-implemented"
mkdir -p "$not_implemented_root"
stage_environment "$not_implemented_root"
unset RDMA_STORE_RESILIENCE_COMMAND
if bash "$suite_dir/run.sh" all; then
    printf 'accepted full validation after the default resilience runner failed\n' >&2
    exit 1
fi
grep -Fx '{"status":"BLOCKED","reason":"te-gate"}' \
    "$not_implemented_root/store-resilience.result"
grep -Fx 'open-rdma' "$not_implemented_root/order.log"
grep -Fx 'report' "$not_implemented_root/order.log"
grep -Fx 'compose-down' "$not_implemented_root/order.log"
grep -Fx 'host-rdma-cleanup' "$not_implemented_root/order.log"

standard_failure_root="$tmp_dir/standard-failure"
mkdir -p "$standard_failure_root"
stage_environment "$standard_failure_root"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
export FAIL_STAGE=store-standard FAIL_CODE=41
if bash "$suite_dir/run.sh" all; then
    printf 'accepted a failed standard Store gate\n' >&2
    exit 1
else
    test $? -eq 41
fi
unset FAIL_STAGE FAIL_CODE
! grep -Fxq store-resilience "$standard_failure_root/order.log"
grep -Fx 'status=BLOCKED' "$standard_failure_root/store-resilience.result"
grep -Fx 'reason=store-standard-gate' "$standard_failure_root/store-resilience.result"
grep -Fx 'open-rdma' "$standard_failure_root/order.log"
grep -Fx 'report' "$standard_failure_root/order.log"

verbs_failure_root="$tmp_dir/verbs-failure"
mkdir -p "$verbs_failure_root"
stage_environment "$verbs_failure_root"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
export FAIL_STAGE=verbs FAIL_CODE=27
if bash "$suite_dir/run.sh" all; then
    printf 'accepted a failed verbs gate\n' >&2
    exit 1
else
    test $? -eq 27
fi
unset FAIL_STAGE FAIL_CODE
assert_order "$verbs_failure_root/order.log" \
    preflight host-rdma-setup build compose-up verbs open-rdma report \
    compose-down host-rdma-cleanup

te_failure_root="$tmp_dir/te-failure"
mkdir -p "$te_failure_root"
stage_environment "$te_failure_root"
export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
export FAIL_STAGE=te FAIL_CODE=29
if bash "$suite_dir/run.sh" all; then
    printf 'accepted a failed Transfer Engine gate\n' >&2
    exit 1
else
    test $? -eq 29
fi
unset FAIL_STAGE FAIL_CODE
assert_order "$te_failure_root/order.log" \
    preflight host-rdma-setup build compose-up verbs te open-rdma report \
    compose-down host-rdma-cleanup

for signal_name in INT TERM; do
    signal_root="$tmp_dir/signal-$signal_name"
    mkdir -p "$signal_root"
    stage_environment "$signal_root"
    export RDMA_STORE_RESILIENCE_COMMAND="$fake_stage store-resilience"
    export PAUSE_STAGE=verbs
    setsid python3 -c '
import os
import signal
import sys

signal.signal(signal.SIGINT, signal.SIG_DFL)
os.execv(sys.argv[1], sys.argv[1:])
' "$suite_dir/run.sh" all &
    run_pid=$!
    while ! grep -Fxq verbs "$signal_root/order.log" 2>/dev/null; do sleep 0.05; done
    signal_ignored=$(awk '/^SigIgn:/ {print $2}' "/proc/$run_pid/status")
    if (( 16#$signal_ignored & 2 )); then
        printf 'runner inherited SIGINT as ignored\n' >&2
        kill -s TERM -- "-$run_pid"
        wait "$run_pid" 2>/dev/null || true
        exit 1
    fi
    kill -s "$signal_name" -- "-$run_pid"
    if wait "$run_pid"; then
        printf 'accepted %s without a signal status\n' "$signal_name" >&2
        exit 1
    else
        signal_status=$?
    fi
    case "$signal_name" in
        INT) test "$signal_status" -eq 130 ;;
        TERM) test "$signal_status" -eq 143 ;;
    esac
    unset PAUSE_STAGE
    test "$(grep -Fxc compose-down "$signal_root/order.log")" -eq 1
    test "$(grep -Fxc host-rdma-cleanup "$signal_root/order.log")" -eq 1
done
