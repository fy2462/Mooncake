# Rust Flexible Parity Infrastructure and Store Restoration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the one-to-one parity gate with reusable aggregate Rust evidence, migrate the Store manifest to the generic suite schema, restore the 128 structurally downgraded Store rows, and establish source-only inventory gates for the later Transfer Engine, TENT, and wheel audits.

**Architecture:** `suites.py` owns the four trusted reference-suite definitions and routes source-only discovery to GoogleTest or Python declaration parsers. Schema-version-2 manifests use a generic `reference` identity; validation and execution aggregate any number of Rust evidence tests and deduplicate commands without requiring unique ownership. This phase migrates and restores the existing Store manifest; separate follow-up plans will source-audit and create the three new manifests before remediation begins.

**Tech Stack:** Python 3 standard library (`ast`, `dataclasses`, `json`, `pathlib`, `unittest`), Rust/Cargo command planning, JSON manifests, pre-commit from `/home/fy2462/Mooncake/.venv`.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- The approved design is `docs/superpowers/specs/2026-07-30-rust-store-flexible-test-parity-design.md`.
- C/C++ sources and headers and all files beneath `mooncake-wheel/tests` are immutable read-only oracle inputs.
- Never edit, format, build, link, load, stage, or commit C/C++ files. Do not execute C/C++ or wheel reference binaries.
- Use source-only discovery. Python wheel discovery must use `ast` and must not import wheel modules.
- Every repository edit uses `apply_patch`.
- Every pre-commit command sets `SKIP=mooncake-code-format`.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling.
- Approved Rust evidence packages are `mooncake-store-client`, `mooncake-store-core`, `mooncake-store-master`, and `transfer-engine-ffi`.
- A covered/blocked row may cite multiple Rust tests, and a Rust test may serve multiple reference rows. Aggregate assertions must still prove the complete reference oracle.
- This phase does not claim that the new Transfer Engine, TENT, or wheel inventories are semantically audited. It only establishes exact source inventory counts and the infrastructure their later manifests require.
- Store LocalDisk `io_uring` remains gated until all revised correctness gates pass.

---

### Task 1: Add trusted suite definitions and source-only Python discovery

**Files:**
- Create: `rust-repo/tools/store-validation/suites.py`
- Modify: `rust-repo/tools/store-validation/inventory.py`
- Create: `rust-repo/tools/store-validation/tests/fixtures/python_tests.py`
- Create: `rust-repo/tools/store-validation/tests/fixtures/test_release_wheel_tags.py`
- Modify: `rust-repo/tools/store-validation/tests/test_inventory.py`
- Create: `rust-repo/tools/store-validation/tests/test_suites.py`

**Interfaces:**
- Produces: `SuiteDefinition(id: str, framework: str, reference_root: str, excluded_files: frozenset[str])`.
- Produces: `SUITES: dict[str, SuiteDefinition]` with `store-cpp`, `transfer-engine-cpp`, `tent-cpp`, and `wheel-store-python`.
- Produces: `discover_python_tests(root: Path, excluded_files: Collection[str] = ()) -> list[TestRef]`.
- Produces: `discover_suite_tests(repo_root: Path, suite: SuiteDefinition) -> list[TestRef]`.

- [ ] **Step 1: Add failing Python-discovery tests**

Add an AST fixture containing a module-level `test_function`, a `TestClient.test_method`, a non-test helper, a nested `test_nested` helper, and text/comments containing fake declarations. Add this focused test:

```python
def test_discovers_python_test_declarations_without_importing_modules(self):
    refs = discover_python_tests(FIXTURES)
    self.assertIn(
        TestRef("python", "python_tests.py", "test_function"), refs
    )
    self.assertIn(
        TestRef("python", "python_tests.py", "TestClient.test_method"), refs
    )
    self.assertNotIn("test_nested", {ref.name for ref in refs})
```

Add a second test passing `{"test_release_wheel_tags.py"}` and assert no reference from that file is returned.

- [ ] **Step 2: Run the focused inventory tests and verify RED**

Run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m unittest \
  tests.test_inventory.InventoryTest.test_discovers_python_test_declarations_without_importing_modules \
  tests.test_inventory.InventoryTest.test_python_discovery_honors_explicit_file_exclusions -v
```

Expected: both tests fail because `discover_python_tests` does not exist.

- [ ] **Step 3: Implement the minimal AST discovery**

In `inventory.py`, parse `.py` files without importing them. Record only top-level functions named `test_*` and methods named `test_*` on top-level classes named `Test*` or inheriting a syntactic `TestCase` base. Use `ClassName.method_name` for methods. Match exclusions against each root-relative POSIX path.

- [ ] **Step 4: Add failing suite-routing tests**

In `test_suites.py`, assert the four literal definitions and replace their roots with fixture directories to prove `gtest` routes to `discover_cpp_tests` and `python` routes to `discover_python_tests`. Also assert the wheel definition excludes only `test_release_wheel_tags.py`.

- [ ] **Step 5: Run suite tests and verify RED**

Run:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m unittest tests.test_suites -v
```

Expected: FAIL because `suites.py` does not exist.

- [ ] **Step 6: Implement `suites.py`**

Define the exact trusted suite table:

```python
SUITES = {
    "store-cpp": SuiteDefinition(
        "store-cpp", "gtest", "mooncake-store/tests", frozenset()
    ),
    "transfer-engine-cpp": SuiteDefinition(
        "transfer-engine-cpp", "gtest", "mooncake-transfer-engine/tests", frozenset()
    ),
    "tent-cpp": SuiteDefinition(
        "tent-cpp", "gtest", "mooncake-transfer-engine/tent/tests", frozenset()
    ),
    "wheel-store-python": SuiteDefinition(
        "wheel-store-python",
        "python",
        "mooncake-wheel/tests",
        frozenset({"test_release_wheel_tags.py"}),
    ),
}
```

Reject unknown framework values rather than falling back to a parser.

- [ ] **Step 7: Verify discovery counts from source**

Run a read-only Python query through `discover_suite_tests` and assert exact declaration counts:

```text
store-cpp=1399
transfer-engine-cpp=348
tent-cpp=319
wheel-store-python=352
aggregate=2418
```

If a count differs, stop and inspect the discovered identities; do not change the expected count merely to make the test pass.

- [ ] **Step 8: Run the full inventory/suite tests and commit**

Run both test modules, `git diff --check`, scoped pre-commit, and the C/C++ immutability guard. Commit:

```bash
git add rust-repo/tools/store-validation/inventory.py \
  rust-repo/tools/store-validation/suites.py \
  rust-repo/tools/store-validation/tests/fixtures/python_tests.py \
  rust-repo/tools/store-validation/tests/fixtures/test_release_wheel_tags.py \
  rust-repo/tools/store-validation/tests/test_inventory.py \
  rust-repo/tools/store-validation/tests/test_suites.py
git commit -m '[Store] discover expanded parity suites'
```

---

### Task 2: Replace primary ownership with flexible aggregate evidence

**Files:**
- Modify: `rust-repo/tools/store-validation/validate_parity.py`
- Modify: `rust-repo/tools/store-validation/tests/test_validate_parity.py`

**Interfaces:**
- Consumes: schema-version-2 manifest suite descriptors matching `SUITES`.
- Produces: `validate_manifest(manifest, reference_refs, rust_refs) -> list[Finding]` using `reference` identities.
- Produces: `summarize_manifest(manifest) -> dict[str, int]` with `reference_total`, `applicable`, `rust_references`, `unique_rust_tests`, `shared_rust_tests`, `multi_test_rows`, `covered`, `missing`, `blocked`, and `not_applicable`.

- [ ] **Step 1: Convert the test fixture helper to schema version 2**

Change test entries from `cpp` to:

```python
"reference": {"file": REFERENCE_REF.file, "test": REFERENCE_REF.name}
```

Add a valid `suite` descriptor to every manifest fixture. Remove `primary_reviewed` from the helper review object.

- [ ] **Step 2: Write the two required flexible-mapping tests**

Replace the rejection tests with:

```python
def test_covered_entry_accepts_multiple_rust_evidence_tests(self):
    covered = entry(rust=[rust_value(RUST_REF), rust_value(RUST_REF_2)])
    self.assertEqual(
        validate_manifest(valid_manifest([covered]), {REFERENCE_REF}, {RUST_REF, RUST_REF_2}),
        [],
    )

def test_rust_evidence_test_can_serve_multiple_reference_rows(self):
    second = entry(reference=SECOND_REFERENCE_REF)
    self.assertEqual(
        validate_manifest(
            valid_manifest([entry(), second]),
            {REFERENCE_REF, SECOND_REFERENCE_REF},
            {RUST_REF},
        ),
        [],
    )
```

- [ ] **Step 3: Write failing schema, package-scope, and summary tests**

Add tests that reject schema version 1, an unknown suite descriptor, and a Rust reference under `mooncake-conductor`. Add a passing `transfer-engine-ffi` reference. Assert a manifest with two covered rows sharing one Rust test and one row citing two Rust tests reports:

```python
{
    "reference_total": 3,
    "applicable": 3,
    "rust_references": 4,
    "unique_rust_tests": 2,
    "shared_rust_tests": 1,
    "multi_test_rows": 1,
    "covered": 3,
    "missing": 0,
    "blocked": 0,
    "not_applicable": 0,
}
```

- [ ] **Step 4: Run the focused tests and verify RED**

Expected failures must be the old `multiple-primary-rust-tests`, `duplicate-primary-rust-test`, schema-version, primary-marker, or missing new summary-field behavior.

- [ ] **Step 5: Implement the minimum validator change**

Rename `_cpp_key` to `_reference_key`, use generic reference terminology in findings, require one-or-more Rust references for covered/blocked, and remove global ownership errors. Count all evidence references, their unique set, references with more than one owning row, and covered rows with more than one Rust test. Validate the suite descriptor against `SUITES` and the first path component of every Rust reference against:

```python
ALLOWED_RUST_PACKAGES = frozenset({
    "mooncake-store-client",
    "mooncake-store-core",
    "mooncake-store-master",
    "transfer-engine-ffi",
})
```

Remove `missing-primary-review` and `unexpected-primary-review` enforcement.

- [ ] **Step 6: Run the focused and full validation tests**

Run `tests.test_validate_parity` first, then the entire `tests/test_*.py` suite. Expected: all pass with no warnings or unexpected output.

- [ ] **Step 7: Run static/pre-commit guards and commit**

Compile the changed Python files, run `git diff --check`, scoped pre-commit, and the C/C++ immutability guard. Commit:

```bash
git add rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py
git commit -m '[Store] allow aggregate Rust parity evidence'
```

---

### Task 3: Deduplicate shared evidence during parity execution

**Files:**
- Modify: `rust-repo/tools/store-validation/run_parity_gate.py`
- Modify: `rust-repo/tools/store-validation/tests/test_parity_gate.py`

**Interfaces:**
- Consumes: schema-version-2 `reference` rows and flexible Rust evidence lists.
- Produces: one `PlannedCommand` per unique `(rust.file, rust.test)` with `references: list[dict[str, str]]` containing every owning reference row.

- [ ] **Step 1: Convert gate fixtures to schema version 2**

Rename `cpp_references` to `references`, `cpp_reference` to `reference`, and update the fixture manifest suite descriptor.

- [ ] **Step 2: Write failing shared and aggregate planning tests**

Replace `test_duplicate_primary_is_rejected_even_when_planner_is_called_directly` with a test asserting two rows sharing the same Rust test produce one command with two references. Add a test asserting one row with two Rust tests produces two commands, each pointing back to the same reference.

- [ ] **Step 3: Write a failing Transfer Engine FFI command test**

For `transfer-engine-ffi/tests/test_engine.rs:test_submit`, require:

```text
cargo test -p transfer-engine-ffi --test test_engine test_submit -- --exact
```

and no Store Client `link-native` feature flag.

- [ ] **Step 4: Run focused gate tests and verify RED**

Expected: the current planner raises the `reused by` error and still reads `cpp`/`cpp_references`.

- [ ] **Step 5: Implement command/reference aggregation**

When a command already exists, append a new reference if absent instead of raising. Preserve command deduplication so shared evidence executes once. Keep a missing or blocked row as a gate blocker even when other rows share its supporting code.

- [ ] **Step 6: Run gate tests and commit**

Run `tests.test_parity_gate`, the full validation-tool suite, Python compilation, `git diff --check`, scoped pre-commit, and the immutability guard. Commit:

```bash
git add rust-repo/tools/store-validation/run_parity_gate.py \
  rust-repo/tools/store-validation/tests/test_parity_gate.py
git commit -m '[Store] execute shared parity evidence once'
```

---

### Task 4: Migrate and restore the Store manifest

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: the current manifest, historical pre-one-to-one manifest at `daa8b64c`, and structural baseline at `cb5d4a0d`.
- Produces: schema-version-2 `store-cpp` manifest with exactly 180 covered, 1,121 missing, 0 blocked, and 98 N/A rows.

- [ ] **Step 1: Capture read-only identity and transition evidence**

Run a query loading the three revisions and assert:

```text
current entries=1399
daa8b64c covered=227
cb5d4a0d covered=99
structural-only covered->missing=128
later semantic covered->missing=47
```

The restoration set is exactly rows covered at `daa8b64c` and missing at `cb5d4a0d`. Do not infer the set from reason prose.

- [ ] **Step 2: Apply the schema migration and exact restoration**

Using `apply_patch`, add the trusted `store-cpp` suite descriptor, change each row identity from `cpp` to `reference`, remove `review.primary_reviewed`, and restore the 128 exact rows' status, Rust evidence list, reason, and reviewed oracle metadata from `daa8b64c`. Preserve the current version of all other row data, especially the 47 later semantic downgrades.

- [ ] **Step 3: Verify the migration invariants**

Run a read-only comparison proving:

```text
reference identities=1399, unique=1399
covered=180, missing=1121, blocked=0, not-applicable=98
rust references=189, unique Rust tests=108
shared Rust tests=47, multi-test rows=9
restored rows=128
later semantic downgrades still missing=47
```

Also prove every Rust reference begins with an approved package and no row retains `cpp`, `primary_reviewed`, or a one-to-one-only reason.

- [ ] **Step 4: Run the real Store validator and full tool tests**

Run JSON parsing, schema-version-2 validation against source discovery, and all validation-tool tests. Expected: validator exits zero with no findings; strict completeness remains nonzero solely because 1,121 rows are missing.

- [ ] **Step 5: Run scoped guards and commit**

Run `git diff --check`, scoped pre-commit on the manifest, and the C/C++ immutability guard. Commit:

```bash
git add rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] restore aggregate parity coverage'
```

---

### Task 5: Add multi-suite validation entrypoints and establish expansion inventory

**Files:**
- Modify: `rust-repo/tools/store-validation/validate_parity.py`
- Modify: `rust-repo/tools/store-validation/run_parity_gate.py`
- Modify: `rust-repo/tools/store-validation/tests/test_validate_parity.py`
- Modify: `rust-repo/tools/store-validation/tests/test_parity_gate.py`
- Modify locally only: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Produces: repeatable `--manifest PATH` CLI inputs plus `--repo-root PATH`; each manifest is routed by its trusted suite ID.
- Produces: per-suite summaries and one aggregate summary without requiring the three not-yet-audited manifests to exist.

- [ ] **Step 1: Write failing multi-manifest CLI tests**

Use temporary Store and Transfer Engine fixture manifests. Assert the validator reports both suite summaries plus an aggregate total, rejects duplicate suite IDs, and reports a missing requested manifest as an input error. Add the equivalent planner test proving a Rust command shared across manifests executes once and records both suite-qualified references.

- [ ] **Step 2: Run the focused CLI tests and verify RED**

Expected: current parsers accept only one manifest and cannot aggregate suite results.

- [ ] **Step 3: Implement repeated manifests and aggregate output**

Change `--manifest` to `action="append"`, require `--repo-root`, derive each reference inventory through `SUITES`, and keep `--require-complete` semantics across all supplied manifests. Do not silently synthesize or skip an absent suite.

- [ ] **Step 4: Verify all four source inventories without creating unaudited manifests**

Run `discover_suite_tests` for all four trusted definitions and record exactly 1,399/348/319/352 and aggregate 2,418. Validate the migrated Store manifest independently. The expansion inventory is not a correctness PASS until the three later semantic-audit plans create complete reviewed manifests.

- [ ] **Step 5: Record the honest phase status locally**

Append to the ignored progress file:

```text
Flexible mapping infrastructure complete; Store restored to 180 covered / 1121 missing / 98 N/A. TE 348, TENT 319, and wheel 352 identities are source-discovered but not yet semantically dispositioned, so aggregate correctness and io_uring remain gated.
```

- [ ] **Step 6: Run final phase verification**

Run all validation-tool tests, Python compilation, the real Store validator, exact four-suite discovery counts, `git diff --check`, scoped pre-commit for every tracked changed file, and the C/C++ plus wheel-test immutability guards. Confirm `git status --short --branch` is clean except the ignored local progress record.

- [ ] **Step 7: Commit the multi-suite entrypoints**

```bash
git add rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/run_parity_gate.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py \
  rust-repo/tools/store-validation/tests/test_parity_gate.py
git commit -m '[Store] validate scoped parity suites'
```

---

## Follow-up Plans Required After This Phase

1. C++ Transfer Engine 348-row semantic inventory and disposition plan.
2. C++ TENT 319-row semantic inventory and disposition plan.
3. wheel Store/TE 352-row semantic inventory and disposition plan.
4. Cross-suite Store-missing re-evaluation using newly admitted `transfer-engine-ffi` evidence.
5. Stable-order Rust TDD remediation plans for genuine missing rows, grouped by shared production prerequisite rather than by raw row count.

No missing remediation or LocalDisk `io_uring` optimization starts until the four audit inventories distinguish complete aggregate evidence, partial evidence, genuine absence, blocked execution, and reviewed N/A.
