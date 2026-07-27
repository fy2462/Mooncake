# Rust Store Full Correctness Validation Implementation Plan

<!-- codespell:ignore crate -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run an auditable correctness gate that translates every in-scope C++ Store test behavior into a reviewed Rust disposition, executes only Transfer Engine plus Rust Store Client/Master tests, and emits reproducible module, parity, and three-node resilience results.

**Architecture:** Add a Rust-owned validation tool under `rust-repo/tools/store-validation` with a JSON coverage manifest, a Python standard-library verifier, shell gate entrypoints, and contract tests. The C++ Store source is parsed only to inventory behavioral reference tests; no C++ Store target is built or executed. The gate composes existing Cargo/Python tests and `rdma-multinode`, preserving each stage result and refusing to report PASS for missing, skipped, blocked, or undiscoverable parity rows.

**Tech Stack:** Rust/Cargo, Python 3 standard library and `unittest`, POSIX shell, Docker Compose, Soft-RoCE, existing `rust-repo/tools/rdma-multinode` scripts.

## Global Constraints

- The tested product is Transfer Engine plus Rust Store Client and Master only.
- C++ Store tests and implementation are a read-only behavioral oracle; never compile, link, load, or execute the C++ Store.
- Rust Store may depend on native code only through `transfer-engine-ffi`; TENT belongs to that Transfer Engine boundary when enabled.
- A skipped or blocked required capability never rolls up to PASS.
- Every semantic repair starts with a focused failing Rust test and follows C++ behavior.
- Performance implementation is gated on a complete correctness PASS and is split into four later plans: TENT, accelerator DLPack, shared-memory hot cache, and `io_uring`.
- Preserve unrelated worktree changes and stage only files named by each task.
- Use `.venv/bin/pre-commit` on touched files; if the project-wide formatter rewrites unrelated files, restore only those hook-created edits and do not stage them.

---

## Planned File Structure

- `rust-repo/tools/store-validation/parity-map.json`: reviewed C++-behavior-to-Rust-test dispositions and the authoritative parity coverage data.
- `rust-repo/tools/store-validation/inventory.py`: discovers C++ GoogleTest cases and Rust test functions without importing or building either implementation.
- `rust-repo/tools/store-validation/validate_parity.py`: validates schema, complete C++ inventory coverage, Rust test discoverability, and allowed dispositions.
- `rust-repo/tools/store-validation/result.py`: creates and aggregates stable `PASS`/`FAIL`/`SKIP`/`BLOCKED` JSON results.
- `rust-repo/tools/store-validation/run-module-gate.sh`: runs TE/FFI, Rust workspace, feature, and Python Client tests and records per-command evidence.
- `rust-repo/tools/store-validation/run-parity-gate.py`: selects and runs mapped Rust tests, refusing incomplete manifest rows.
- `rust-repo/tools/store-validation/run-full-gate.sh`: composes module, parity, and existing three-node gates without masking the first failure.
- `rust-repo/tools/store-validation/render-report.py`: renders the machine-readable gate results and manifest coverage into Markdown.
- `rust-repo/tools/store-validation/tests/`: standard-library unit and shell contract tests for inventory, validation, aggregation, and orchestration.
- `rust-repo/tools/store-validation/README.md`: exact local, privileged, and hardware-gated commands plus artifact layout.
- `rust-repo/docs/source/testing/full-store-validation.md`: maintained testing guide linked from the Rust documentation testing index.

## Task 1: Add deterministic C++ and Rust test inventory

**Files:**
- Create: `rust-repo/tools/store-validation/inventory.py`
- Create: `rust-repo/tools/store-validation/tests/fixtures/cpp_tests.cpp`
- Create: `rust-repo/tools/store-validation/tests/fixtures/rust_tests.rs`
- Create: `rust-repo/tools/store-validation/tests/test_inventory.py`

**Interfaces:**
- Produces: `discover_cpp_tests(root: Path) -> list[TestRef]`, `discover_rust_tests(root: Path) -> list[TestRef]`, and CLI JSON records with `framework`, `file`, and `name` strings.
- Consumes: repository paths only; it must not invoke CMake, Cargo, or test binaries.

- [ ] **Step 1: Write fixture cases and failing inventory tests**

```python
from pathlib import Path
import unittest

from inventory import discover_cpp_tests, discover_rust_tests


class InventoryTest(unittest.TestCase):
    def test_discovers_google_test_macros_and_ignores_comments(self):
        refs = discover_cpp_tests(Path(__file__).parent / "fixtures")
        self.assertEqual(
            [(ref.file, ref.name) for ref in refs],
            [
                ("cpp_tests.cpp", "AllocatorTest.ReusesFreedRange"),
                ("cpp_tests.cpp", "ReplicaParamTest/0.DegradedRead"),
            ],
        )

    def test_discovers_rust_test_functions_and_ignores_helpers(self):
        refs = discover_rust_tests(Path(__file__).parent / "fixtures")
        self.assertEqual(
            [(ref.file, ref.name) for ref in refs],
            [
                ("rust_tests.rs", "degraded_read_uses_remaining_replica"),
                ("rust_tests.rs", "tokio_restart_recovers_catalog"),
            ],
        )
```

The C++ fixture contains `TEST`, `TEST_F`, and one explicitly named synthetic parameterized case; comments contain fake macros that must be ignored. The Rust fixture contains `#[test]`, `#[tokio::test]`, and an unannotated helper.

- [ ] **Step 2: Run the inventory tests and verify they fail**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_inventory -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'inventory'`.

- [ ] **Step 3: Implement inventory parsing and the CLI**

Use a frozen dataclass and deterministic path/name ordering:

```python
@dataclass(frozen=True, order=True)
class TestRef:
    framework: str
    file: str
    name: str


def discover_cpp_tests(root: Path) -> list[TestRef]:
    """Return uncommented TEST/TEST_F/TEST_P declarations below root."""


def discover_rust_tests(root: Path) -> list[TestRef]:
    """Return functions immediately annotated by #[test] or #[tokio::test]."""
```

The CLI accepts `--cpp-root`, `--rust-root`, and `--output`, emits UTF-8 JSON with sorted records, and exits 2 for an unreadable root. Strip line and block comments before scanning. Record parameterized `TEST_P(Suite, Name)` as `Suite.Name`; explicit instantiation names are coverage metadata, not separately executable tests.

- [ ] **Step 4: Run focused tests and a repository inventory smoke check**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_inventory -v
python3 inventory.py \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates \
  --output /tmp/mooncake-store-test-inventory.json
python3 -m json.tool /tmp/mooncake-store-test-inventory.json >/dev/null
```

Expected: unit tests PASS; inventory exits 0 and JSON parsing succeeds.

- [ ] **Step 5: Commit the inventory unit**

```bash
git add rust-repo/tools/store-validation/inventory.py \
  rust-repo/tools/store-validation/tests/fixtures/cpp_tests.cpp \
  rust-repo/tools/store-validation/tests/fixtures/rust_tests.rs \
  rust-repo/tools/store-validation/tests/test_inventory.py
git commit -m "[Store] inventory C++ and Rust parity tests"
```

## Task 2: Add the reviewed parity manifest and completeness verifier

**Files:**
- Create: `rust-repo/tools/store-validation/parity-map.json`
- Create: `rust-repo/tools/store-validation/validate_parity.py`
- Create: `rust-repo/tools/store-validation/tests/test_validate_parity.py`

**Interfaces:**
- Consumes: Task 1 `TestRef` inventory and `parity-map.json` schema version 1.
- Produces: `validate_manifest(manifest: dict, cpp_refs: set[TestRef], rust_refs: set[TestRef]) -> list[Finding]` and CLI exit 0 only when there are no error findings.

- [ ] **Step 1: Write failing schema and completeness tests**

```python
class ValidateParityTest(unittest.TestCase):
    def test_rejects_unmapped_cpp_test(self):
        findings = validate_manifest(
            {"schema_version": 1, "entries": []},
            {TestRef("gtest", "allocator_test.cpp", "AllocatorTest.Reuse")},
            set(),
        )
        self.assertIn("unmapped-cpp-test", {item.code for item in findings})

    def test_covered_entry_requires_discoverable_rust_test(self):
        findings = validate_manifest(self.covered_entry(), self.cpp_refs, set())
        self.assertIn("missing-rust-test", {item.code for item in findings})

    def test_not_applicable_requires_nonempty_reviewed_reason(self):
        manifest = self.not_applicable_entry(reason="")
        findings = validate_manifest(manifest, self.cpp_refs, set())
        self.assertIn("missing-disposition-reason", {item.code for item in findings})
```

Also test duplicate C++ references, unknown status values, missing behavioral assertions, stale C++ references, and a fully valid mixed manifest.

- [ ] **Step 2: Run the verifier tests and verify they fail**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_validate_parity -v
```

Expected: FAIL because `validate_parity` does not exist.

- [ ] **Step 3: Implement schema version 1 and validation**

Each manifest entry has this exact shape:

```json
{
  "cpp": {"file": "allocation_strategy_test.cpp", "test": "AllocationStrategyTest.Allocate"},
  "behavior": "A successful allocation consumes capacity and returns one visible replica.",
  "boundary": ["master", "allocator"],
  "status": "covered",
  "rust": [
    {"file": "mooncake-store-master/tests/test_allocator.rs", "test": "allocate_consumes_capacity"}
  ],
  "reason": "",
  "review": {"oracle": "C++ test and AllocationStrategy implementation", "reviewed": true}
}
```

Allowed statuses are `covered`, `missing`, `not-applicable`, and `blocked`.
`covered` requires one or more discoverable Rust tests. `missing` requires an
empty Rust list and a reason describing the uncovered behavior. `blocked`
requires the exact absent device, kernel feature, permission, or service.
`not-applicable` requires a reviewed reason identifying the C++-only detail.
Every discovered in-scope C++ test appears exactly once.

- [ ] **Step 4: Generate the initial manifest and review every entry**

Run the inventory CLI, then create one manifest entry for every discovered test under `mooncake-store/tests`. Determine the semantic assertion by reading the C++ test and its directly exercised implementation. Map it to existing Rust Client/Master/FFI tests when those tests assert the same observable behavior; otherwise mark it `missing`. Mark only runtime-independent C++ internals outside the Rust boundary `not-applicable`, with `review.reviewed` set to true after inspection. Do not mark hardware absence `not-applicable`.

Verify continuously with:

```bash
cd rust-repo/tools/store-validation
python3 validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
```

Expected: exit 0 when every C++ test has exactly one valid disposition; `missing` and `blocked` rows are permitted at this inventory stage and are summarized on stdout.

- [ ] **Step 5: Run all validation unit tests**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

Expected: PASS.

- [ ] **Step 6: Commit the manifest and verifier**

```bash
git add rust-repo/tools/store-validation/parity-map.json \
  rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py
git commit -m "[Store] map C++ behaviors to Rust parity tests"
```

## Task 3: Add stable result records and the Rust module gate

**Files:**
- Create: `rust-repo/tools/store-validation/result.py`
- Create: `rust-repo/tools/store-validation/run-module-gate.sh`
- Create: `rust-repo/tools/store-validation/tests/test_result.py`
- Create: `rust-repo/tools/store-validation/tests/test_module_gate.sh`

**Interfaces:**
- Produces: result JSON containing `schema_version`, `gate`, `status`, `started_at`, `finished_at`, `duration_seconds`, `environment`, and ordered `commands`.
- Produces: `aggregate_status(statuses: Sequence[str]) -> str`, where required `FAIL` wins, then `BLOCKED`, then `SKIP`, and only all-PASS yields PASS.

- [ ] **Step 1: Write failing aggregation and shell contract tests**

```python
class ResultTest(unittest.TestCase):
    def test_required_skip_cannot_pass(self):
        self.assertEqual(aggregate_status(["PASS", "SKIP"]), "SKIP")

    def test_first_failure_is_retained(self):
        result = build_gate_result("module", [failed("cargo test"), passed("pytest")])
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["first_failure"]["command"], "cargo test")
```

The shell contract test injects fake `cargo`, `python3`, and `env` executables through `PATH`, asserts commands run in declared order, verifies a failure is retained while independent reporting runs, and verifies the runner never contains `mooncake-store` CMake/build invocations.

- [ ] **Step 2: Run the result and module contract tests and verify failure**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_result -v
bash tests/test_module_gate.sh
```

Expected: both fail because the result module and runner do not exist.

- [ ] **Step 3: Implement atomic result writing**

`result.py` writes to `<path>.tmp`, calls `flush()` and `os.fsync()`, and replaces the destination with `os.replace()`. Record:

```json
{
  "schema_version": 1,
  "gate": "module",
  "status": "PASS",
  "environment": {
    "uname": "...",
    "rustc": "...",
    "cargo": "...",
    "python": "...",
    "git_commit": "..."
  },
  "commands": [
    {"name": "store-client", "argv": ["cargo", "test", "-p", "mooncake-store-client"], "status": "PASS", "exit_code": 0, "log": "..."}
  ]
}
```

- [ ] **Step 4: Implement the module gate command matrix**

`run-module-gate.sh --artifact-root PATH` runs from `rust-repo` with `set -u` but captures every command status explicitly. Use `CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-5}`. The required commands are:

```bash
cargo test -p mooncake-store-core
cargo test -p transfer-engine-ffi --no-default-features --features mock
cargo test -p mooncake-store-client
cargo test -p mooncake-store-master
cargo test -p mooncake-p2p-store
cargo test -p mooncake-conductor
cargo test --workspace
.venv/bin/python -m pytest rust-repo/python/tests -q
```

Resolve the repository virtual environment as `<repo-root>/.venv/bin/python`; if it is absent, record `BLOCKED` with prerequisite `repo-.venv`, not PASS. Native TE/TENT feature commands are recorded as separate required capability entries when their shared libraries are present and as `BLOCKED` with the missing library names otherwise. Never invoke a C++ Store target.

- [ ] **Step 5: Run contract tests and a non-native module gate**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_result -v
bash tests/test_module_gate.sh
./run-module-gate.sh --artifact-root /tmp/mooncake-store-validation/module
python3 -m json.tool /tmp/mooncake-store-validation/module/module.result.json >/dev/null
```

Expected: contract tests PASS; the real gate produces a valid result. Any product test failure is preserved for Task 5 remediation and is not relabeled as infrastructure failure.

- [ ] **Step 6: Commit the module gate**

```bash
git add rust-repo/tools/store-validation/result.py \
  rust-repo/tools/store-validation/run-module-gate.sh \
  rust-repo/tools/store-validation/tests/test_result.py \
  rust-repo/tools/store-validation/tests/test_module_gate.sh
git commit -m "[Store] add Rust module validation gate"
```

## Task 4: Add the executable Rust parity gate

**Files:**
- Create: `rust-repo/tools/store-validation/run-parity-gate.py`
- Create: `rust-repo/tools/store-validation/tests/test_parity_gate.py`

**Interfaces:**
- Consumes: validated `parity-map.json`, Rust inventory, and Task 3 result JSON format.
- Produces: `parity.result.json`; PASS requires zero `missing`, `blocked`, stale, failed, or undiscoverable in-scope rows.

- [ ] **Step 1: Write failing selection and roll-up tests**

```python
class ParityGateTest(unittest.TestCase):
    def test_missing_row_blocks_execution_pass(self):
        result = plan_parity_run(self.manifest(status="missing"), self.inventory)
        self.assertEqual(result.status, "BLOCKED")

    def test_groups_discoverable_tests_by_cargo_target(self):
        result = plan_parity_run(self.covered_manifest(), self.inventory)
        self.assertEqual(
            result.commands,
            [["cargo", "test", "-p", "mooncake-store-master", "--test", "test_allocator", "allocate_consumes_capacity", "--", "--exact"]],
        )
```

Also assert that `not-applicable` rows do not execute, required `blocked` rows prevent PASS, duplicate Rust execution is deduplicated, and a failing test records the owning C++ behavior.

- [ ] **Step 2: Run the test and verify failure**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_parity_gate -v
```

Expected: FAIL because `run-parity-gate.py` does not exist.

- [ ] **Step 3: Implement target resolution and execution**

Map paths of the form `mooncake-store-master/tests/test_allocator.rs` to package `mooncake-store-master` and integration target `test_allocator`; map unit tests under `src/` to `cargo test -p <package> <name> -- --exact`. Reject paths outside `rust-repo/crates` and Rust Python tests until an explicit Python target mapper is added. Validate the entire manifest before launching a test. Write one log per unique Rust test and include its source C++ references in the result.

- [ ] **Step 4: Run unit tests and the parity gate**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest tests.test_parity_gate -v
python3 run-parity-gate.py \
  --manifest parity-map.json \
  --repo-root ../../.. \
  --artifact-root /tmp/mooncake-store-validation/parity
```

Expected: unit tests PASS. The real gate is PASS only if the manifest has no in-scope `missing` or `blocked` row and every mapped Rust test passes; otherwise it emits BLOCKED/FAIL with exact rows.

- [ ] **Step 5: Commit the parity gate**

```bash
git add rust-repo/tools/store-validation/run-parity-gate.py \
  rust-repo/tools/store-validation/tests/test_parity_gate.py
git commit -m "[Store] execute mapped Rust parity tests"
```

## Task 5: Close every missing parity row with Rust tests and semantic repairs

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Test: exact Rust Client/Master/FFI test file named by each `missing` manifest row
- Modify: only the Rust Client/Master/FFI source file reached by the focused failing test
- Create: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: Task 4 BLOCKED/FAIL rows and their C++ behavior/oracle metadata.
- Produces: zero in-scope `missing` rows, zero unexplained failures, a regression test for every semantic repair, and a remediation record linking failure, oracle, test, code change, and verification command.

- [ ] **Step 1: Select the first missing row in stable manifest order**

Run:

```bash
cd rust-repo/tools/store-validation
python3 validate_parity.py --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests --rust-root ../../crates \
  --list-status missing
```

Expected: prints the first source file/test, behavioral assertion, Rust boundary, and reason. Process one row or one inseparable behavior family per commit.

- [ ] **Step 2: Add the focused Rust test using the C++ assertion as oracle**

Name the Rust test after the behavior, not the C++ implementation. Use the existing test harness for the owning crate. The test must assert externally meaningful state: returned error/result, Master placement/catalog state, persisted recovery state, Client-visible bytes, or resource cleanup. Do not call or link C++ Store code.

- [ ] **Step 3: Run the focused test and confirm the expected failure**

Run the exact crate/target/test command that Task 4 will generate, for example:

```bash
cd rust-repo
cargo test -p mooncake-store-master --test test_allocator \
  allocate_consumes_capacity -- --exact
```

Expected: FAIL for the specific semantic difference or missing behavior. If it passes, update the manifest to point at this existing coverage without changing production code.

- [ ] **Step 4: Apply systematic debugging before changing production code**

Invoke `superpowers:systematic-debugging`. Trace the C++ oracle path and Rust path from the same input, record the first divergent state in `remediation-log.json`, and identify the smallest Rust-owned correction. If the divergence belongs to data transfer rather than Store semantics, constrain the change to TE C ABI/`transfer-engine-ffi` and preserve its ownership contract.

- [ ] **Step 5: Implement the minimal Rust correction and rerun focused tests**

Run the focused test, then the owning crate:

```bash
cd rust-repo
cargo test -p <owning-package> --test <integration-target> <test-name> -- --exact
cargo test -p <owning-package>
```

Expected: both PASS.

- [ ] **Step 6: Update the manifest and remediation record**

Change the row from `missing` to `covered`, add the exact Rust path/test, retain the C++ oracle description, and append:

```json
{
  "cpp": "file.cpp:Suite.Test",
  "rust": "package/tests/file.rs:test_name",
  "first_divergence": "Master allocator retained capacity after rejected replica placement",
  "repair_commit": "pending",
  "focused_command": "cargo test ...",
  "crate_command": "cargo test -p ..."
}
```

- [ ] **Step 7: Commit one reviewed remediation unit**

```bash
git add rust-repo/tools/store-validation/parity-map.json \
  rust-repo/tools/store-validation/remediation-log.json \
  <focused-rust-test-file> <minimal-rust-source-files>
git commit -m "[Store] align Rust <behavior-name> semantics"
```

After the commit, replace `repair_commit: "pending"` with the commit hash in the next manifest-only bookkeeping commit or include the preceding commit hash in the next remediation record.

- [ ] **Step 8: Repeat Steps 1–7 until no missing row remains**

Completion command:

```bash
cd rust-repo/tools/store-validation
python3 validate_parity.py --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests --rust-root ../../crates \
  --require-complete
```

Expected: exit 0 with `missing=0`; hardware-required `blocked` rows remain explicit for Task 7 and cannot produce an overall PASS.

## Task 6: Compose module, parity, and three-node gates

**Files:**
- Create: `rust-repo/tools/store-validation/run-full-gate.sh`
- Create: `rust-repo/tools/store-validation/render-report.py`
- Create: `rust-repo/tools/store-validation/tests/test_full_gate.sh`
- Create: `rust-repo/tools/store-validation/tests/test_render_report.py`

**Interfaces:**
- Consumes: Task 3 `module.result.json`, Task 4 `parity.result.json`, and existing `rdma-multinode` result files.
- Produces: `full.result.json` plus `full-report.md`; stage order is module, parity, verbs, TE, Store standard, Store resilience.

- [ ] **Step 1: Write failing orchestration and reporting tests**

The shell test injects fake stage runners and asserts:

```text
module FAIL -> parity still reported as BLOCKED, independent report runs,
               privileged multinode stage does not start
module PASS + parity PASS -> rdma-multinode/run.sh all runs exactly once
resilience BLOCKED -> full result is BLOCKED, never PASS
```

The Python test supplies fixture result JSON and asserts the Markdown contains environment, coverage counts, command durations, first failure, standard/resilience scenario statuses, and artifact paths.

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd rust-repo/tools/store-validation
bash tests/test_full_gate.sh
python3 -m unittest tests.test_render_report -v
```

Expected: FAIL because the runner and renderer do not exist.

- [ ] **Step 3: Implement fail-safe orchestration**

`run-full-gate.sh --artifact-root PATH [--skip-multinode]` creates a unique run directory, traps `INT`/`TERM`, preserves the first failing stage, and always invokes the renderer. `--skip-multinode` records required multinode stages as SKIP and therefore cannot yield overall PASS. Without that flag, invoke:

```bash
rust-repo/tools/rdma-multinode/run.sh all
```

Copy or reference `verbs.result`, `te.result`, `store-standard.result`, and `store-resilience.result` without rewriting their product status.

- [ ] **Step 4: Implement deterministic Markdown reporting**

Render sections for scope boundary, environment, parity coverage, module commands, standard scenarios, resilience scenarios, first failure, capability gaps, and artifact paths. Include the sentence: “C++ Store was used only as a source-level behavioral oracle and was not built or executed by this gate.”

- [ ] **Step 5: Run contract tests**

Run:

```bash
cd rust-repo/tools/store-validation
bash tests/test_full_gate.sh
python3 -m unittest tests.test_render_report -v
```

Expected: PASS.

- [ ] **Step 6: Commit the composed gate**

```bash
git add rust-repo/tools/store-validation/run-full-gate.sh \
  rust-repo/tools/store-validation/render-report.py \
  rust-repo/tools/store-validation/tests/test_full_gate.sh \
  rust-repo/tools/store-validation/tests/test_render_report.py
git commit -m "[Store] compose full Rust correctness gate"
```

## Task 7: Document and execute the complete correctness acceptance

**Files:**
- Create: `rust-repo/tools/store-validation/README.md`
- Create: `rust-repo/docs/source/testing/full-store-validation.md`
- Modify: `rust-repo/docs/source/testing/index.md`
- Modify: `rust-repo/tools/store-validation/parity-map.json` only for evidence paths or reviewed hardware dispositions

**Interfaces:**
- Consumes: all prior tasks and the existing Soft-RoCE privileged environment.
- Produces: one retained acceptance artifact root whose `full.result.json` is PASS, or exact unresolved product/hardware evidence that prevents completion.

- [ ] **Step 1: Write the operator documentation**

Document:

```bash
cd /home/fy2462/Mooncake
rust-repo/tools/store-validation/run-full-gate.sh \
  --artifact-root /home/fy2462/workspace/tmp/mooncake/store-validation
```

Explain nonprivileged module/parity diagnostics, `.venv` usage, native TE/TENT library prerequisites, Soft-RoCE host mutations owned by `rdma-multinode`, cleanup, result statuses, artifact retention, and the prohibition on treating SKIP/BLOCKED as PASS. Link the new page from the Rust testing index.

- [ ] **Step 2: Run all validation-tool contract tests**

Run:

```bash
cd rust-repo/tools/store-validation
python3 -m unittest discover -s tests -p 'test_*.py' -v
for test_script in tests/test_*.sh; do bash "$test_script"; done
```

Expected: PASS.

- [ ] **Step 3: Run pre-commit only on touched files**

Run from repository root:

```bash
.venv/bin/pre-commit run --files \
  rust-repo/tools/store-validation/parity-map.json \
  rust-repo/tools/store-validation/inventory.py \
  rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/result.py \
  rust-repo/tools/store-validation/run-module-gate.sh \
  rust-repo/tools/store-validation/run-parity-gate.py \
  rust-repo/tools/store-validation/run-full-gate.sh \
  rust-repo/tools/store-validation/render-report.py \
  rust-repo/tools/store-validation/README.md \
  rust-repo/docs/source/testing/full-store-validation.md \
  rust-repo/docs/source/testing/index.md
```

Expected: relevant hooks PASS. If the repository-wide formatter rewrites unrelated files, restore only those hook-created edits and rerun the file-specific lint/format tools for the touched Python/shell/JSON/Markdown files.

- [ ] **Step 4: Run the complete gate**

Run:

```bash
rust-repo/tools/store-validation/run-full-gate.sh \
  --artifact-root /home/fy2462/workspace/tmp/mooncake/store-validation
```

Expected: `full.result.json` status PASS; module and parity are PASS; verbs and TE report RDMA PASS; Store standard and resilience are PASS; no required row is missing, blocked, or skipped.

- [ ] **Step 5: Diagnose and repair any product failure before acceptance**

For each failure, invoke `superpowers:systematic-debugging`, add a focused regression test, make the minimal Rust/TE-boundary correction, rerun the focused and owning suites, then rerun the complete gate. Do not edit result artifacts to change status. A missing external hardware prerequisite remains BLOCKED and keeps the overall goal incomplete.

- [ ] **Step 6: Build the Rust documentation**

Run:

```bash
cd rust-repo/docs
make html
```

Expected: PASS with no broken link to `testing/full-store-validation`.

- [ ] **Step 7: Review final diff and commit documentation/evidence references**

Run:

```bash
git diff --check
git status --short
git diff -- rust-repo/tools/store-validation rust-repo/docs/source/testing
git add rust-repo/tools/store-validation/README.md \
  rust-repo/docs/source/testing/full-store-validation.md \
  rust-repo/docs/source/testing/index.md \
  rust-repo/tools/store-validation/parity-map.json
git commit -m "[Store] document full Rust correctness validation"
```

Expected: the commit contains only requested validation files and reviewed evidence references.

## Task 8: Gate and plan the four performance subprojects

**Files:**
- Create after correctness PASS: `docs/superpowers/plans/2026-07-27-tent-performance.md`
- Create after correctness PASS: `docs/superpowers/plans/2026-07-27-accelerator-dlpack-performance.md`
- Create after correctness PASS: `docs/superpowers/plans/2026-07-27-shm-hot-cache-performance.md`
- Create after correctness PASS: `docs/superpowers/plans/2026-07-27-io-uring-performance.md`

**Interfaces:**
- Consumes: Task 7 PASS artifacts and capability-specific baseline profiles.
- Produces: four independent implementation plans, each with an unchanged-workload baseline, bottleneck evidence, TDD correctness work, optimization steps, post-change statistics, and a complete correctness rerun.

- [ ] **Step 1: Verify the correctness prerequisite**

Run:

```bash
python3 -c 'import json; p="/home/fy2462/workspace/tmp/mooncake/store-validation/full.result.json"; assert json.load(open(p, encoding="utf-8"))["status"] == "PASS"'
```

Expected: exit 0. Do not begin performance implementation otherwise.

- [ ] **Step 2: Capture one unchanged-workload baseline per capability**

Record environment, warmup, workload distribution, repetitions, throughput, P50/P95/P99, CPU, RSS, copy count, and relevant syscall/device counters. On this machine, DLPack accelerator and physical-RDMA/NUMA/NVMe claims must remain BLOCKED when hardware is absent; functional/mocked measurements cannot be relabeled as hardware performance.

- [ ] **Step 3: Write the four evidence-driven plans**

Invoke `superpowers:writing-plans` separately for TENT, accelerator DLPack, SHM hot cache, and `io_uring`. Each plan names the measured bottleneck and exact files/tests; do not preselect an optimization unsupported by its baseline profile.

- [ ] **Step 4: Commit the performance plans**

```bash
git add docs/superpowers/plans/2026-07-27-tent-performance.md \
  docs/superpowers/plans/2026-07-27-accelerator-dlpack-performance.md \
  docs/superpowers/plans/2026-07-27-shm-hot-cache-performance.md \
  docs/superpowers/plans/2026-07-27-io-uring-performance.md
git commit -m "[Store] plan Rust Store performance optimization"
```
