#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tool_dir=$(cd "$script_dir/.." && pwd)
temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT
fake_bin="$temp_dir/bin"
mkdir -p "$fake_bin" "$temp_dir/native"
touch "$temp_dir/native/libtransfer_engine.so"

cat >"$fake_bin/cargo" <<'SH'
#!/usr/bin/env bash
echo "cargo CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-} $*" >>"$MODULE_GATE_TRACE"
if [[ "$*" == *"mooncake-store-master"* ]]; then exit 7; fi
exit 0
SH
cat >"$fake_bin/python3" <<SH
#!/usr/bin/env bash
exec "$(command -v python3)" "\$@"
SH
cat >"$fake_bin/validation-python" <<'SH'
#!/usr/bin/env bash
echo "python $*" >>"$MODULE_GATE_TRACE"
exit 0
SH
cat >"$fake_bin/validation-maturin" <<'SH'
#!/usr/bin/env bash
echo "maturin $*" >>"$MODULE_GATE_TRACE"
exit 0
SH
chmod +x "$fake_bin/cargo" "$fake_bin/python3" "$fake_bin/validation-python" "$fake_bin/validation-maturin"

export MODULE_GATE_TRACE="$temp_dir/trace"
export MOONCAKE_TE_LIB_DIR="$temp_dir/native"
export MOONCAKE_VALIDATION_PYTHON="$fake_bin/validation-python"
export MOONCAKE_VALIDATION_MATURIN="$fake_bin/validation-maturin"
export MOONCAKE_VALIDATION_CARGO_TARGET_DIR="$temp_dir/shared-cargo-target"
export PATH="$fake_bin:$PATH"
set +e
bash "$tool_dir/run-module-gate.sh" --artifact-root "$temp_dir/artifacts"
status=$?
set -e
[[ $status -eq 1 ]]
python3 - "$temp_dir/artifacts/module.result.json" "$MODULE_GATE_TRACE" <<'PY'
import json, pathlib, sys
result = json.loads(pathlib.Path(sys.argv[1]).read_text())
trace = pathlib.Path(sys.argv[2]).read_text()
assert result["status"] == "FAIL", result
assert result["first_failure"]["name"] == "store-master", result
names = [item["name"] for item in result["commands"]]
assert "native-tent" not in names, result
assert "p2p-store" not in names, result
commands = {item["name"]: item for item in result["commands"]}
assert commands["workspace"]["status"] == "PASS", result
assert commands["python-binding-build"]["status"] == "PASS", result
assert commands["python-client"]["status"] == "PASS", result
assert "mooncake-store-master" in trace and "--workspace" in trace, trace
assert "transfer-engine-ffi --lib -- --skip tent::" in trace, trace
assert "--exclude mooncake-p2p-store" in trace, trace
assert "link-tent-native" not in trace and "mooncake-p2p-store" not in trace.replace("--exclude mooncake-p2p-store", ""), trace
assert "CARGO_TARGET_DIR=" in trace and "shared-cargo-target" in trace, trace
assert "cmake" not in trace.lower(), trace
PY

: >"$MODULE_GATE_TRACE"
export MOONCAKE_TE_LIB_DIR="$temp_dir/missing-native"
set +e
bash "$tool_dir/run-module-gate.sh" --artifact-root "$temp_dir/non-native-artifacts"
status=$?
set -e
[[ $status -eq 1 ]]
python3 - "$temp_dir/non-native-artifacts/module.result.json" "$MODULE_GATE_TRACE" <<'PY'
import json, pathlib, sys
result = json.loads(pathlib.Path(sys.argv[1]).read_text())
trace = pathlib.Path(sys.argv[2]).read_text()
commands = {item["name"]: item for item in result["commands"]}
assert commands["store-client"]["status"] == "BLOCKED", result
assert commands["workspace"]["status"] == "BLOCKED", result
assert commands["python-client"]["status"] == "BLOCKED", result
assert "mooncake-store-client" not in trace and "--workspace" not in trace, trace
assert "pytest" not in trace, trace
assert "native-tent" not in commands, result
assert "p2p-store" not in commands, result
PY

shared_root="$temp_dir/shared-repository"
mkdir -p "$shared_root/.git" "$shared_root/.venv/bin"
ln -s "$fake_bin/validation-python" "$shared_root/.venv/bin/python"
ln -s "$fake_bin/validation-maturin" "$shared_root/.venv/bin/maturin"
cat >"$fake_bin/git" <<'SH'
#!/usr/bin/env bash
if [[ "$*" == *"--git-common-dir"* ]]; then
  printf '%s\n' "$MODULE_GATE_SHARED_GIT_DIR"
else
  printf 'test-commit\n'
fi
SH
chmod +x "$fake_bin/git"

: >"$MODULE_GATE_TRACE"
unset MOONCAKE_VALIDATION_PYTHON MOONCAKE_VALIDATION_MATURIN
export MOONCAKE_TE_LIB_DIR="$temp_dir/native"
export MODULE_GATE_SHARED_GIT_DIR="$shared_root/.git"
set +e
bash "$tool_dir/run-module-gate.sh" --artifact-root "$temp_dir/shared-venv-artifacts"
status=$?
set -e
[[ $status -eq 1 ]]
python3 - "$temp_dir/shared-venv-artifacts/module.result.json" "$MODULE_GATE_TRACE" <<'PY'
import json, pathlib, sys
result = json.loads(pathlib.Path(sys.argv[1]).read_text())
trace = pathlib.Path(sys.argv[2]).read_text()
commands = {item["name"]: item for item in result["commands"]}
assert commands["python-binding-build"]["status"] == "PASS", result
assert commands["python-client"]["status"] == "PASS", result
assert all(item.get("prerequisite") != "repo-.venv-python+maturin" for item in result["commands"]), result
assert "maturin" in trace and "python -m pytest python/tests -q" in trace, trace
PY

python3 - "$tool_dir/../../python/Cargo.toml" "$tool_dir/../../python/src/lib.rs" <<'PY'
import pathlib
import sys
import tomllib

cargo_toml = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
features = cargo_toml["features"]
assert "mooncake-p2p-store/link-native" not in features["link-native"], features
assert "transfer-engine-ffi/link-tent-native" not in features["link-native"], features
assert features["link-tent-native"] == [
    "link-native",
    "transfer-engine-ffi/link-tent-native",
], features

lib_rs = pathlib.Path(sys.argv[2]).read_text()
assert '#[cfg(feature = "link-tent-native")]\nmod transfer_engine;' in lib_rs
assert '#[cfg(feature = "link-tent-native")]\n    m.add_class::<transfer_engine::PyTransferEngine>()?;' in lib_rs
PY
