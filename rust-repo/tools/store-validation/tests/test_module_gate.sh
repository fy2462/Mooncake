#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tool_dir=$(cd "$script_dir/.." && pwd)
temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT
fake_bin="$temp_dir/bin"
mkdir -p "$fake_bin" "$temp_dir/native"
touch "$temp_dir/native/libtransfer_engine.so" "$temp_dir/native/libtent_shared.so"

cat >"$fake_bin/cargo" <<'SH'
#!/usr/bin/env bash
echo "cargo $*" >>"$MODULE_GATE_TRACE"
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
assert any(item["name"] == "native-tent" for item in result["commands"]), result
assert "mooncake-store-master" in trace and "--workspace" in trace, trace
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
assert commands["native-tent"]["status"] == "BLOCKED", result
PY
