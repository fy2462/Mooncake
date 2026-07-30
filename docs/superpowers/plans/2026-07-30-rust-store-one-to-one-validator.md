# Rust Store One-to-One Validator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce the approved one-C++-test-to-one-unique-Rust-primary-test contract and convert every structurally invalid covered row back to an explicit missing backlog item.

**Architecture:** Extend the existing Python manifest validator with status-specific primary-test cardinality, blocked-prerequisite, global uniqueness, and count-summary rules. Keep the C++ inventory immutable, normalize only the 128 covered rows that structurally violate the new contract, and make the parity runner reject shared primaries defensively even when called without the validator CLI.

**Tech Stack:** Python 3 standard library, JSON, `unittest`, Rust-test discovery from `inventory.py`, pre-commit from `/home/fy2462/Mooncake/.venv`.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- The design authority is `docs/superpowers/specs/2026-07-30-rust-store-one-to-one-test-parity-design.md`.
- C/C++ files matching `.c`, `.cc`, `.cpp`, `.cxx`, `.cu`, `.cuh`, `.h`, `.hh`, or `.hpp` are read-only and must never be formatted, built, linked, loaded, staged, or committed.
- Every pre-commit command sets `SKIP=mooncake-code-format`.
- Use `/home/fy2462/Mooncake/.venv/bin/python` and use `apply_patch` for every repository edit.
- A `covered` or `blocked` row has exactly one discoverable Rust primary test; a primary is globally unique across those applicable rows.
- A `blocked` row additionally has a nonempty string `prerequisite`; a `missing` or `not-applicable` row has no primary Rust reference.
- The current 98 reviewed N/A rows remain unchanged unless validation exposes a metadata error.
- Do not invent coverage: a structurally invalid covered row becomes `missing` until a complete unique Rust test exists.

---

### Task 1: Enforce primary-test cardinality and blocked prerequisites

**Files:**
- Modify: `rust-repo/tools/store-validation/tests/test_validate_parity.py`
- Modify: `rust-repo/tools/store-validation/validate_parity.py`

**Interfaces:**
- Consumes: `validate_manifest(manifest, cpp_refs, rust_refs) -> list[Finding]`.
- Produces: findings `multiple-primary-rust-tests`, `blocked-without-rust-test`, `missing-blocked-prerequisite`, and `unexpected-blocked-prerequisite`; preserves `covered-without-rust-test`, `noncovered-with-rust-test`, and `missing-rust-test` where applicable.

- [ ] **Step 1: Write failing cardinality and prerequisite tests**

Add these literal fixtures and four focused tests:

```python
RUST_REF_2 = TestRef(
    "rust", "mooncake-store-master/tests/test_allocator.rs", "reuse_adjacent_range"
)


def rust_value(reference: TestRef) -> dict[str, str]:
    return {"file": reference.file, "test": reference.name}


def finding_codes(candidate: dict, rust_refs: set[TestRef]) -> set[str]:
    findings = validate_manifest(
        {"schema_version": 1, "entries": [candidate]}, {CPP_REF}, rust_refs
    )
    return {item.code for item in findings}


def test_covered_entry_rejects_multiple_primary_tests(self):
    covered = entry(rust=[rust_value(RUST_REF), rust_value(RUST_REF_2)])
    codes = finding_codes(covered, {RUST_REF, RUST_REF_2})
    self.assertIn("multiple-primary-rust-tests", codes)

def test_blocked_entry_requires_one_discoverable_primary_and_prerequisite(self):
    blocked = entry(status="blocked", rust=[], reason="GPU execution is blocked.")
    codes = finding_codes(blocked, set())
    self.assertIn("blocked-without-rust-test", codes)
    self.assertIn("missing-blocked-prerequisite", codes)

def test_blocked_entry_accepts_one_primary_and_explicit_prerequisite(self):
    blocked = entry(
        status="blocked",
        reason="The parity test exists but cannot execute on this host.",
    )
    blocked["prerequisite"] = "CUDA-capable GPU"
    findings = validate_manifest(
        {"schema_version": 1, "entries": [blocked]}, {CPP_REF}, {RUST_REF}
    )
    self.assertEqual(findings, [])

def test_missing_and_not_applicable_entries_reject_primary_tests(self):
    for status in ("missing", "not-applicable"):
        candidate = entry(
            status=status,
            reason="This disposition must not claim a primary Rust test.",
        )
        if status == "not-applicable":
            candidate["review"]["na_category"] = "absent-rust-product-boundary"
        with self.subTest(status=status):
            self.assertIn(
                "noncovered-with-rust-test",
                finding_codes(candidate, {RUST_REF}),
            )
```

The production mutation caught by these tests is accepting an applicable row with zero/multiple primaries, accepting a blocked row without its named external requirement, or allowing missing/N/A to inflate the primary count.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m unittest \
  tests.test_validate_parity.ValidateParityTest.test_covered_entry_rejects_multiple_primary_tests \
  tests.test_validate_parity.ValidateParityTest.test_blocked_entry_requires_one_discoverable_primary_and_prerequisite \
  tests.test_validate_parity.ValidateParityTest.test_blocked_entry_accepts_one_primary_and_explicit_prerequisite \
  tests.test_validate_parity.ValidateParityTest.test_missing_and_not_applicable_entries_reject_primary_tests -v
```

Expected: failures are caused only by the absent cardinality/prerequisite rules.

- [ ] **Step 3: Implement minimal status-specific validation**

Replace the broad `status != "covered"` rule with literal status branches:

```python
if status == "covered":
    if not rust_keys:
        findings.append(
            _finding(
                "covered-without-rust-test",
                reference,
                "covered entries require exactly one Rust primary test",
            )
        )
    elif len(rust_keys) > 1:
        findings.append(
            _finding(
                "multiple-primary-rust-tests",
                reference,
                "covered entries require exactly one Rust primary test",
            )
        )
elif status == "blocked":
    if not rust_keys:
        findings.append(
            _finding(
                "blocked-without-rust-test",
                reference,
                "blocked entries require exactly one existing Rust primary test",
            )
        )
    elif len(rust_keys) > 1:
        findings.append(
            _finding(
                "multiple-primary-rust-tests",
                reference,
                "blocked entries require exactly one Rust primary test",
            )
        )
    prerequisite = raw_entry.get("prerequisite")
    if not isinstance(prerequisite, str) or not prerequisite.strip():
        findings.append(
            _finding(
                "missing-blocked-prerequisite",
                reference,
                "blocked entries require a named external prerequisite",
            )
        )
elif status in {"missing", "not-applicable"} and rust_keys:
    findings.append(
        _finding(
            "noncovered-with-rust-test",
            reference,
            f"{status} entries must not claim a Rust primary test",
        )
    )

if status != "blocked" and "prerequisite" in raw_entry:
    findings.append(
        _finding(
            "unexpected-blocked-prerequisite",
            reference,
            "only blocked entries may set prerequisite",
        )
    )
```

- [ ] **Step 4: Verify GREEN and record the expected transitional failure**

Run the focused tests and then:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
git diff --check
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
```

Expected: unit tests pass, C/C++ diff guard exits 0, and the real manifest exits 1 with exactly 9 `multiple-primary-rust-tests` findings. Keep the validator and tests uncommitted until Task 3 restores a valid tracked manifest.

---

### Task 2: Enforce global primary uniqueness and expose exact counts

**Files:**
- Modify: `rust-repo/tools/store-validation/tests/test_validate_parity.py`
- Modify: `rust-repo/tools/store-validation/validate_parity.py`

**Interfaces:**
- Produces: `duplicate-primary-rust-test` findings for a primary reused by covered/blocked rows.
- Produces: `summarize_manifest(manifest) -> dict[str, int]` with keys `cpp_total`, `applicable`, `unique_primary`, `duplicate_primary`, `covered`, `missing`, `blocked`, and `not_applicable`.

- [ ] **Step 1: Write failing uniqueness and summary tests**

Add `test_rejects_primary_reused_by_multiple_cpp_rows`: clone `entry()`, replace the clone's C++ reference with `client_test.cpp:ClientTest.Timeout`, validate both rows against both C++ references and `{RUST_REF}`, and assert `duplicate-primary-rust-test`.

Add `test_summary_reports_one_to_one_counts`. Construct four literal rows: two covered rows sharing `RUST_REF`, one missing row with no Rust reference, and one allowed N/A row with no Rust reference. Assert this exact result from `summarize_manifest`:

```python
{
    "cpp_total": 4,
    "applicable": 3,
    "unique_primary": 1,
    "duplicate_primary": 1,
    "covered": 2,
    "missing": 1,
    "blocked": 0,
    "not_applicable": 1,
}
```

The production mutation caught is counting repeated references as independent parity tests or omitting applicable/missing counts from the acceptance summary.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m unittest \
  tests.test_validate_parity.ValidateParityTest.test_rejects_primary_reused_by_multiple_cpp_rows \
  tests.test_validate_parity.ValidateParityTest.test_summary_reports_one_to_one_counts -v
```

Expected: `duplicate-primary-rust-test` is absent and `summarize_manifest` is not yet importable/implemented.

- [ ] **Step 3: Implement global uniqueness and summary**

Collect valid primary keys only from `covered` and `blocked` rows. After entry validation, emit one finding per key whose owner count exceeds one, with the Rust `file:test` as reference and every owning C++ reference in the message. Implement `summarize_manifest` from literal status and primary counters; `duplicate_primary` counts distinct Rust primary keys with more than one owner.

Change the CLI summary to render all eight named counts in this order:

```python
summary = summarize_manifest(manifest)
print(
    "summary "
    + " ".join(
        f"{label}={summary[key]}"
        for label, key in (
            ("cpp-total", "cpp_total"),
            ("applicable", "applicable"),
            ("unique-primary", "unique_primary"),
            ("duplicate-primary", "duplicate_primary"),
            ("covered", "covered"),
            ("missing", "missing"),
            ("blocked", "blocked"),
            ("not-applicable", "not_applicable"),
        )
    )
)
```

- [ ] **Step 4: Verify GREEN and record the expected real-manifest failure**

Run the full Python suite, then run the real validator. Expected: unit tests pass; the real manifest exits 1 only because the known 47 shared primaries and 9 multiple-primary rows violate the newly active contract. Save exact finding counts for Task 3.

Do not commit a validator that leaves the tracked manifest invalid; continue directly to Task 3.

---

### Task 3: Normalize structurally invalid covered rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Commit together with the uncommitted Task 1–2 validator/test changes.

**Interfaces:**
- Consumes: 227 covered rows, 236 Rust references, 155 unique references, 9 multi-reference rows, 47 shared reference keys, and 119 single-reference rows using shared keys.
- Produces: exactly 99 structurally valid covered rows and 1,202 missing rows before any later semantic completeness corrections.

- [ ] **Step 1: Generate and inspect the exact 128-row conversion set**

Run this read-only report and inspect all 128 printed rows:

```bash
/home/fy2462/Mooncake/.venv/bin/python - <<'PY'
import json
from collections import Counter

path = "rust-repo/tools/store-validation/parity-map.json"
manifest = json.load(open(path, encoding="utf-8"))
covered = [entry for entry in manifest["entries"] if entry["status"] == "covered"]
owners = Counter(
    (rust["file"], rust["test"])
    for entry in covered
    for rust in entry["rust"]
)
selected = [
    entry
    for entry in covered
    if len(entry["rust"]) != 1
    or owners[(entry["rust"][0]["file"], entry["rust"][0]["test"])] > 1
]
multi = sum(len(entry["rust"]) > 1 for entry in covered)
shared_single = sum(
    len(entry["rust"]) == 1
    and owners[(entry["rust"][0]["file"], entry["rust"][0]["test"])] > 1
    for entry in covered
)
assert (multi, shared_single, len(selected), len(covered) - len(selected)) == (
    9,
    119,
    128,
    99,
)
for entry in selected:
    print(entry["cpp"], entry["rust"])
PY
```

- [ ] **Step 2: Convert each selected row using one deterministic unique test name**

For each selected row:

- set `status` to `missing`;
- set `rust` to `[]`;
- preserve its C++ behavior, boundary, review oracle, and reviewed flag;
- replace `reason` with a sentence naming the existing partial/shared Rust evidence, the exact structural delta, the intended Rust file, and a deterministic unique primary test name. Build the name as `cpp_parity_` plus the lowercase non-alphanumeric-to-underscore slug of `cpp.file + "_" + cpp.test`, followed by `_` and the first eight hexadecimal digits of SHA-256 over `cpp.file + ":" + cpp.test`. Assert every generated name is a valid Rust identifier and globally unique before patching;
- do not add `review.na_category` or `prerequisite`.

Apply the generated JSON diff only through `apply_patch`. Reject the patch if the proposed names are not globally unique or if any row outside the exact 128-row set changes.

- [ ] **Step 3: Verify the normalized invariant**

Run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m json.tool parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

Expected: validator exits 0; summary is `cpp-total=1399 applicable=1301 unique-primary=99 duplicate-primary=0 covered=99 missing=1202 blocked=0 not-applicable=98`.

- [ ] **Step 4: Run scoped hygiene and commit Tasks 1–3**

Run C/C++ diff guard, `git diff --check`, and scoped pre-commit with the formatter skipped. Stage only the validator, validator tests, and parity manifest, inspect `git diff --cached --name-only`, then commit:

```bash
git commit -m '[Store] enforce unique Rust parity primaries'
```

---

### Task 4: Make the parity runner reject shared primary tests defensively

**Files:**
- Modify: `rust-repo/tools/store-validation/tests/test_parity_gate.py`
- Modify: `rust-repo/tools/store-validation/run_parity_gate.py`

**Interfaces:**
- Consumes: normalized manifests that already pass `validate_manifest`.
- Produces: `plan_parity_run` raises `ValueError` if a caller bypasses validation and reuses a Rust primary; every planned command owns exactly one C++ reference.

- [ ] **Step 1: Replace the obsolete deduplication test and verify RED**

Replace `test_duplicate_rust_test_executes_once_and_keeps_both_oracles` with:

```python
def test_duplicate_primary_is_rejected_even_when_planner_is_called_directly(self):
    manifest = {
        "schema_version": 1,
        "entries": [entry(), entry(cpp_test="AllocatorTest.Reallocate")],
    }
    with self.assertRaisesRegex(ValueError, "reused by"):
        plan_parity_run(manifest, self.inventory)
```

Run that exact test. Expected: FAIL because the current planner deduplicates and retains two C++ owners.

- [ ] **Step 2: Implement minimal defensive rejection and verify GREEN**

When `by_test` already contains the key, raise `ValueError` naming both C++ references instead of appending a second owner. Keep `cpp_references` for result-schema compatibility, but every valid command contains one item.

Run the focused test, full Python suite, and real manifest validator.

- [ ] **Step 3: Verify immutability, run pre-commit, and commit**

Run C/C++ diff guard, `git diff --check`, and scoped pre-commit for the two Python files. Commit:

```bash
git commit -m '[Store] reject shared parity commands'
```

---

### Task 5: Publish the strict-validator handoff

**Files:**
- No production changes unless verification exposes a defect.
- Update locally: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`.

**Interfaces:**
- Produces: a clean strict structural baseline for semantic covered-row review and stable-order missing implementation.

- [ ] **Step 1: Run final verification**

Run JSON parsing, the full Python suite, real manifest validation, `py_compile`, `git diff --check`, C/C++ diff guard, and `git status --short --branch`. The manifest must have zero validator findings and exactly the Task 3 counts.

- [ ] **Step 2: Record limitations without overstating completion**

Record that the remaining 99 covered rows are structurally one-to-one but still require source-level semantic completeness review. Record all 1,202 missing rows as correctness backlog; do not begin Store LocalDisk `io_uring` measurement until missing/blocked are zero and module, multi-node, and fault gates pass.

- [ ] **Step 3: Obtain the pending independent cross-review**

An independent reviewer checks the N/A audit and strict validator/migration. Any finding is corrected and re-reviewed. This checkpoint may be deferred when inline-only execution is required, but the baseline cannot be called final or independently approved until it occurs.
