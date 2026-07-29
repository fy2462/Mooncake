#!/usr/bin/env bash
set -u

usage() {
  echo "usage: $0 --artifact-root PATH" >&2
  exit 2
}

artifact_root=""
while (($#)); do
  case "$1" in
    --artifact-root)
      (($# >= 2)) || usage
      artifact_root=$2
      shift 2
      ;;
    *) usage ;;
  esac
done
[[ -n "$artifact_root" ]] || usage

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
rust_root=$(cd "$script_dir/../.." && pwd)
repo_root=$(cd "$rust_root/.." && pwd)
git_common_dir=$(git -C "$repo_root" rev-parse --path-format=absolute --git-common-dir)
repo_root=$(cd "$(dirname "$git_common_dir")" && pwd)
artifact_root=$(mkdir -p "$artifact_root" && cd "$artifact_root" && pwd)
logs_dir="$artifact_root/logs"
records_dir="$artifact_root/records"
mkdir -p "$logs_dir" "$records_dir"
rm -f "$records_dir"/*.json

started_epoch=$(date +%s.%N)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
command_index=0
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-5}
native_cargo_target=${MOONCAKE_VALIDATION_CARGO_TARGET_DIR:-$artifact_root/cargo-target/native}
python_cargo_target=${MOONCAKE_VALIDATION_PYTHON_CARGO_TARGET_DIR:-$native_cargo_target}

record_command() {
  local name=$1
  shift
  local log="$logs_dir/$name.log"
  local command_started command_finished exit_code status
  command_started=$(date +%s.%N)
  (
    cd "$rust_root"
    "$@"
  ) >"$log" 2>&1
  exit_code=$?
  command_finished=$(date +%s.%N)
  if ((exit_code == 0)); then status=PASS; else status=FAIL; fi
  python3 - "$records_dir/$(printf '%03d' "$command_index")-$name.json" \
    "$name" "$status" "$exit_code" "$log" "$command_started" "$command_finished" "$@" <<'PY'
import json, pathlib, sys
path, name, status, exit_code, log, started, finished, *argv = sys.argv[1:]
duration = max(0.0, float(finished) - float(started))
pathlib.Path(path).write_text(json.dumps({
    "name": name, "argv": argv, "status": status,
    "exit_code": int(exit_code), "duration_seconds": round(duration, 6), "log": log,
}, sort_keys=True) + "\n", encoding="utf-8")
PY
  command_index=$((command_index + 1))
}

record_blocked() {
  local name=$1 prerequisite=$2
  python3 - "$records_dir/$(printf '%03d' "$command_index")-$name.json" "$name" "$prerequisite" <<'PY'
import json, pathlib, sys
path, name, prerequisite = sys.argv[1:]
pathlib.Path(path).write_text(json.dumps({
    "name": name, "argv": [], "status": "BLOCKED", "exit_code": None,
    "duration_seconds": 0.0, "log": "", "prerequisite": prerequisite,
}, sort_keys=True) + "\n", encoding="utf-8")
PY
  command_index=$((command_index + 1))
}

native_dir=${MOONCAKE_TE_LIB_DIR:-}
if [[ -z "$native_dir" ]]; then
  IFS=: read -r -a library_dirs <<<"${LD_LIBRARY_PATH:-}"
  for candidate in "${library_dirs[@]}"; do
    if [[ -f "$candidate/libtransfer_engine.so" ]]; then native_dir=$candidate; break; fi
  done
fi

record_command store-core cargo test -p mooncake-store-core
record_command transfer-engine-ffi cargo test -p transfer-engine-ffi --no-default-features --features mock

if [[ -n "$native_dir" && -f "$native_dir/libtransfer_engine.so" ]]; then
  record_command store-client env \
    "CARGO_TARGET_DIR=$native_cargo_target" \
    "RUSTFLAGS=-L native=$native_dir" \
    "LD_LIBRARY_PATH=$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test -p mooncake-store-client --features link-native
else
  record_blocked store-client libtransfer_engine.so
fi
record_command store-master cargo test -p mooncake-store-master
record_command conductor cargo test -p mooncake-conductor
if [[ -n "$native_dir" && -f "$native_dir/libtransfer_engine.so" ]]; then
  record_command workspace env \
    "CARGO_TARGET_DIR=$native_cargo_target" \
    "RUSTFLAGS=-L native=$native_dir" \
    "LD_LIBRARY_PATH=$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test --workspace --exclude mooncake-store-py --exclude mooncake-p2p-store \
      --features mooncake-store-client/link-native
else
  record_blocked workspace libtransfer_engine.so
fi

validation_python=${MOONCAKE_VALIDATION_PYTHON:-$repo_root/.venv/bin/python}
validation_maturin=${MOONCAKE_VALIDATION_MATURIN:-$repo_root/.venv/bin/maturin}
if [[ -x "$validation_python" && -x "$validation_maturin" && -n "$native_dir" && -f "$native_dir/libtransfer_engine.so" ]]; then
  record_command python-binding-build env \
    "CARGO_TARGET_DIR=$python_cargo_target" \
    "RUSTFLAGS=-L native=$native_dir" \
    "LD_LIBRARY_PATH=$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    "$validation_maturin" develop --manifest-path python/Cargo.toml \
      --no-default-features --features link-native
  record_command python-client env \
    "LD_LIBRARY_PATH=$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    "$validation_python" -m pytest python/tests -q
elif [[ ! -x "$validation_python" || ! -x "$validation_maturin" ]]; then
  record_blocked python-binding-build repo-.venv-python+maturin
  record_blocked python-client repo-.venv-python+maturin
else
  record_blocked python-binding-build libtransfer_engine.so
  record_blocked python-client libtransfer_engine.so
fi

finished_epoch=$(date +%s.%N)
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 - "$script_dir" "$records_dir" "$artifact_root/module.result.json" \
  "$started_at" "$finished_at" "$started_epoch" "$finished_epoch" "$repo_root" <<'PY'
import json, pathlib, platform, subprocess, sys
script_dir, records_dir, output, started_at, finished_at, start, finish, repo_root = sys.argv[1:]
sys.path.insert(0, script_dir)
from result import build_gate_result, write_json_atomic

def version(argv):
    try:
        return subprocess.run(argv, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.splitlines()[0]
    except (OSError, IndexError):
        return "unavailable"

commands = [json.loads(path.read_text()) for path in sorted(pathlib.Path(records_dir).glob("*.json"))]
environment = {
    "uname": platform.platform(), "rustc": version(["rustc", "--version"]),
    "cargo": version(["cargo", "--version"]), "python": version([sys.executable, "--version"]),
    "git_commit": version(["git", "-C", repo_root, "rev-parse", "HEAD"]),
}
result = build_gate_result("module", commands, started_at=started_at, finished_at=finished_at,
                           duration_seconds=max(0.0, float(finish) - float(start)), environment=environment)
write_json_atomic(pathlib.Path(output), result)
print(f"module gate: {result['status']} ({output})")
raise SystemExit(0 if result["status"] == "PASS" else 1)
PY
