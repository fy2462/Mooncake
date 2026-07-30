# Rust Store N/A Re-audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-review all 107 current C++-only `not-applicable` Store tests, convert every Rust-observable case to an explicit parity backlog or exact coverage row, and make retained N/A categories machine-validated.

**Architecture:** Audit the immutable C++ oracle in three bounded source families and update only the parity manifest. Retained N/A rows receive one explicit `review.na_category`; applicable rows become `missing` unless one unique Rust test already proves the complete oracle. After all rows are classified, extend the Python validator test-first so future N/A entries cannot bypass the approved exclusion policy.

**Tech Stack:** Python 3 standard library, JSON, `jq`, Rust/C++ source inspection with `rg` and `sed`, unittest, pre-commit from `/home/fy2462/Mooncake/.venv`.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- The design authority is `docs/superpowers/specs/2026-07-30-rust-store-one-to-one-test-parity-design.md`.
- Every `.c`, `.cc`, `.cpp`, `.cxx`, `.cu`, `.cuh`, `.h`, `.hh`, and `.hpp` file is read-only: never edit, format, generate, stage, or commit one.
- Never run the repository-wide C++ formatter; every pre-commit command in this plan sets `SKIP=mooncake-code-format`.
- Never build, link, load, or execute C++ Store code. C++ source is a read-only behavioral oracle.
- Production HA serving remains etcd-only. Redis/K8s internal leadership behavior may be N/A only when the Rust fail-fast product boundary is separately tested or backlogged.
- Difficulty, duration, unavailable hardware, missing libraries, missing implementation, and architectural difference with a Rust-visible result are not N/A reasons.
- Retained N/A categories are exactly `language-unrepresentable`, `absent-rust-product-boundary`, `cpp-build-or-abi`, `noncanonical-duplicate-source`, `excluded-component-internal`, `no-executable-cpp-oracle`, and `excluded-performance-scope`.
- `no-executable-cpp-oracle` is limited to a disabled or assertion-free C++ body with no returned status, state transition, or other pass/fail result. `excluded-performance-scope` is limited to performance-only cases for TENT, accelerator DLPack, or SHM hot cache; it never excludes correctness behavior or Store LocalDisk `io_uring` performance.
- A row changed to `covered` must cite exactly one complete, independently discoverable, globally unique Rust primary test. Otherwise mark it `missing` with a concrete unique test name.
- Use `/home/fy2462/Mooncake/.venv/bin/python`; `.venv` is the repository Python environment.
- Use `apply_patch` for every file edit. Preserve unrelated user changes.

---

### Task 1: Re-audit ownership, runtime, and codec N/A rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only: `mooncake-store/tests/mutex_test.cpp`
- Read only: `mooncake-store/tests/client_buffer_test.cpp`
- Read only: `mooncake-store/tests/uds_transport_test.cpp`
- Read only: `mooncake-store/tests/thread_pool_test.cpp`
- Read only: `mooncake-store/tests/zstd_util_test.cpp`
- Read only: `mooncake-store/tests/serializer_test.cpp`
- Read only: `mooncake-store/tests/transfer_task_test.cpp`
- Read only: `mooncake-store/tests/dummy_client_get_buffer_test.cpp`
- Read only: implementation search roots `mooncake-store/include/`, `mooncake-store/src/`, and `rust-repo/crates/`

**Interfaces:**
- Consumes: the 48 N/A rows from the eight exact C++ files above.
- Produces: reviewed dispositions whose retained N/A entries contain `review.na_category` and whose applicable entries are actionable `missing` or uniquely `covered` rows.

- [ ] **Step 1: Prove the C/C++ worktree is clean and capture the exact batch**

Run:

```bash
cd /home/fy2462/Mooncake/.worktrees/ha-chaos-live
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
jq -r '
  .entries[]
  | select(.status == "not-applicable")
  | select(.cpp.file == "mutex_test.cpp"
      or .cpp.file == "client_buffer_test.cpp"
      or .cpp.file == "uds_transport_test.cpp"
      or .cpp.file == "thread_pool_test.cpp"
      or .cpp.file == "zstd_util_test.cpp"
      or .cpp.file == "serializer_test.cpp"
      or .cpp.file == "transfer_task_test.cpp"
      or .cpp.file == "dummy_client_get_buffer_test.cpp")
  | [.cpp.file, .cpp.test] | @tsv
' rust-repo/tools/store-validation/parity-map.json | tee /tmp/mooncake-na-batch-1.tsv | wc -l
```

Expected: C/C++ diff check exits 0 and the row count is exactly `48`.

- [ ] **Step 2: Review every C++ assertion and Rust boundary**

For each line in `/tmp/mooncake-na-batch-1.tsv`, locate the test with the following command, read through its closing brace with bounded `sed -n`, identify every directly called Store symbol, and search the two exact C++ implementation roots plus all Rust crates. Search for the tested result, not merely a similarly named type.

```bash
while IFS=$'\t' read -r cpp_file cpp_test; do
  test_case=${cpp_test#*.}
  rg -n "${test_case}" "mooncake-store/tests/${cpp_file}"
done </tmp/mooncake-na-batch-1.tsv
printf 'Directly called C++ symbol: ' >&2
IFS= read -r symbol
rg -n -- "$symbol" mooncake-store/include mooncake-store/src
printf 'Observable result or Rust symbol: ' >&2
IFS= read -r rust_query
rg -n -- "$rust_query" rust-repo/crates -g '*.rs'
```

Run the two prompted searches separately for every directly called symbol and observable result extracted from each row.

Apply this decision table literally:

```text
Rust-visible result and exact unique Rust test exists -> covered, one Rust ref
Rust-visible result but no exact unique Rust test     -> missing, empty Rust list
No Rust-visible result and allowed category proven   -> not-applicable + category
External prerequisite prevents required execution    -> missing with the exact
                                                        prerequisite; never N/A
```

For a retained row, add an allowed category inside `review`:

```json
"review": {
  "na_category": "language-unrepresentable",
  "oracle": "mooncake-store/tests/client_buffer_test.cpp BufferHandleMoveConstructor and the move constructor implementation",
  "reviewed": true
}
```

For an applicable row without a complete test, set `rust` to an empty array,
set `status` to `missing`, remove `review.na_category`, and write a concrete
reason containing one unique `cpp_parity_...` name, one exact Rust file path,
the literal C++ result to assert, and the exact delta from any partial evidence.

- [ ] **Step 3: Validate the focused batch and full manifest**

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

Expected: JSON parse and ordinary validator exit 0 with no `ERROR`; all 19 pre-existing validation tests pass. Record the new global disposition counts without claiming N/A audit completion.

- [ ] **Step 4: Obtain independent scoped review and correct findings**

The reviewer reads all 48 C++ bodies, directly exercised implementations, every changed reason/category, and any claimed Rust test. Approval requires no N/A based on cost, difficulty, environment, or implementation absence, and no false `covered` row. Apply only reviewed corrections and repeat Step 3.

- [ ] **Step 5: Verify immutability, run scoped hygiene, and commit**

Run:

```bash
cd /home/fy2462/Mooncake/.worktrees/ha-chaos-live
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
git diff --check
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
  --files rust-repo/tools/store-validation/parity-map.json
git add rust-repo/tools/store-validation/parity-map.json
git commit -m "[Store] re-audit ownership and runtime N/A parity"
```

Expected: no C/C++ diff, all scoped hooks pass, and the commit contains only `parity-map.json`.

---

### Task 2: Re-audit LocalFS, optional HA, and duplicate-source N/A rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only: `mooncake-store/tests/ha/oplog/localfs_oplog_store_test.cpp`
- Read only: `mooncake-store/tests/localfs_hot_standby_integration_test.cpp`
- Read only: `mooncake-store/tests/ha/leadership/ha_backend_availability_test.cpp`
- Read only: `mooncake-store/tests/ha/leadership/backends/redis/high_availability_redis_test.cpp`
- Read only: `mooncake-store/tests/ha/leadership/backends/k8s/high_availability_k8s_test.cpp`
- Read only: `mooncake-store/tests/ha/snapshot/master_snapshot_codec_test.cpp`
- Read only: `mooncake-store/tests/ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store_test.cpp`
- Read only: `mooncake-store/tests/ha/oplog/oplog_serializer_test.cpp`
- Read only: `mooncake-store/tests/ha/oplog/oplog_manager_test.cpp`
- Read only: implementation search roots `mooncake-store/include/ha/`, `mooncake-store/src/ha/`, and `rust-repo/crates/mooncake-store-master/src/ha/`

**Interfaces:**
- Consumes: the 29 N/A rows from the nine exact C++ files above and the approved etcd-only serving boundary.
- Produces: independently reviewed HA/component dispositions that distinguish excluded backend internals from required Rust fail-fast behavior.

- [ ] **Step 1: Capture and count the immutable source batch**

Run the C/C++ clean check, then extract N/A rows whose `.cpp.file` equals one of the nine listed paths:

```bash
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
jq -r '
  .entries[]
  | select(.status == "not-applicable")
  | select(.cpp.file == "ha/oplog/localfs_oplog_store_test.cpp"
      or .cpp.file == "localfs_hot_standby_integration_test.cpp"
      or .cpp.file == "ha/leadership/ha_backend_availability_test.cpp"
      or .cpp.file == "ha/leadership/backends/redis/high_availability_redis_test.cpp"
      or .cpp.file == "ha/leadership/backends/k8s/high_availability_k8s_test.cpp"
      or .cpp.file == "ha/snapshot/master_snapshot_codec_test.cpp"
      or .cpp.file == "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store_test.cpp"
      or .cpp.file == "ha/oplog/oplog_serializer_test.cpp"
      or .cpp.file == "ha/oplog/oplog_manager_test.cpp")
  | [.cpp.file, .cpp.test] | @tsv
' rust-repo/tools/store-validation/parity-map.json | tee /tmp/mooncake-na-batch-2.tsv | wc -l
```

Expected: exactly `29` rows.

- [ ] **Step 2: Reconcile component-internal and product-visible behavior**

Read every test body and direct implementation. Apply these non-negotiable distinctions:

```text
C++ Redis/K8s monitor callback only + Rust production excludes backend
    -> excluded-component-internal may be N/A
Rust must reject Redis/K8s serving at its public configuration/start boundary
    -> fail-fast result is applicable and needs a unique Rust test
C++ CMake build-flag equality only
    -> cpp-build-or-abi may be N/A
LocalFS writer/reader/factory distinction absent in Rust
    -> N/A only if no persistence/error result remains observable
LocalFS durability, ordering, corruption, recovery, or cleanup result
    -> applicable even when Rust API shape differs
Byte-identical source not built by CMake
    -> noncanonical-duplicate-source only after `cmp` and CMake target proof
```

Patch each retained N/A with this exact metadata shape:

```json
"review": {
  "na_category": "excluded-component-internal",
  "oracle": "mooncake-store/tests/ha/leadership/backends/redis/high_availability_redis_test.cpp HighAvailabilityTest.RedisLeadershipMonitorReportsDeletedView and the Redis leader-coordinator monitor implementation",
  "reviewed": true
}
```

Patch each applicable uncovered row with `"status": "missing"`, `"rust": []`, no `na_category`, and a complete reason that names one unique `cpp_parity_...` test, its exact Rust file, the exact C++ result to assert, and any specific delta from existing partial evidence.

- [ ] **Step 3: Prove duplicate-source exclusions**

For each `noncanonical-duplicate-source` row, run read-only comparison and inspect CMake registration:

```bash
cmp mooncake-store/tests/localfs_hot_standby_integration_test.cpp \
  mooncake-store/tests/ha/oplog/localfs_hot_standby_integration_test.cpp
rg -n 'localfs_hot_standby_integration_test' mooncake-store/tests/CMakeLists.txt \
  mooncake-store/tests/ha -g 'CMakeLists.txt'
```

Expected: `cmp` exits 0 and only the canonical built source is identified in the recorded oracle. If either proof fails, do not retain the duplicate-source N/A category.

- [ ] **Step 4: Validate, independently review, and commit**

Obtain independent review of all 29 rows and correct every finding. Then run the complete validation and hygiene matrix:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m json.tool parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
cd ../../..
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
git diff --check
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
  --files rust-repo/tools/store-validation/parity-map.json
git add rust-repo/tools/store-validation/parity-map.json
git commit -m "[Store] re-audit LocalFS and HA N/A parity"
```

---

### Task 3: Re-audit storage, memory, cache, and miscellaneous N/A rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only: `mooncake-store/tests/mmap_arena_test.cpp`
- Read only: `mooncake-store/tests/mmap_arena_fallback_test.cpp`
- Read only: `mooncake-store/tests/posix_file_test.cpp`
- Read only: `mooncake-store/tests/file_storage_test.cpp`
- Read only: `mooncake-store/tests/eviction_strategy_test.cpp`
- Read only: `mooncake-store/tests/client_local_hot_cache_test.cpp`
- Read only: `mooncake-store/tests/client_metrics_test.cpp`
- Read only: `mooncake-store/tests/client_integration_test.cpp`
- Read only: `mooncake-store/tests/master_service_ssd_test.cpp`
- Read only: `mooncake-store/tests/tenant_quota_test.cpp`
- Read only: `mooncake-store/tests/replica_selection_test.cpp`
- Read only: `mooncake-store/tests/utils_test.cpp`
- Read only: implementation search roots `mooncake-store/include/`, `mooncake-store/src/`, `rust-repo/crates/mooncake-store-client/`, `rust-repo/crates/mooncake-store-core/`, and `rust-repo/crates/mooncake-store-master/`

**Interfaces:**
- Consumes: the remaining 30 N/A rows from the twelve exact C++ files above.
- Produces: complete second-review dispositions for the original 107-row N/A baseline.

- [ ] **Step 1: Capture and count the final immutable source batch**

Run:

```bash
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
jq -r '
  .entries[]
  | select(.status == "not-applicable")
  | select(.cpp.file == "mmap_arena_test.cpp"
      or .cpp.file == "mmap_arena_fallback_test.cpp"
      or .cpp.file == "posix_file_test.cpp"
      or .cpp.file == "file_storage_test.cpp"
      or .cpp.file == "eviction_strategy_test.cpp"
      or .cpp.file == "client_local_hot_cache_test.cpp"
      or .cpp.file == "client_metrics_test.cpp"
      or .cpp.file == "client_integration_test.cpp"
      or .cpp.file == "master_service_ssd_test.cpp"
      or .cpp.file == "tenant_quota_test.cpp"
      or .cpp.file == "replica_selection_test.cpp"
      or .cpp.file == "utils_test.cpp")
  | [.cpp.file, .cpp.test] | @tsv
' rust-repo/tools/store-validation/parity-map.json | tee /tmp/mooncake-na-batch-3.tsv | wc -l
```

Expected: C/C++ diff check exits 0 and `/tmp/mooncake-na-batch-3.tsv` contains exactly `30` rows.

- [ ] **Step 2: Review replacement-boundary observability**

For every row, read the test and direct implementation, then apply these boundaries:

```text
Raw address ownership / singleton initialization with no Rust call surface
    -> language-unrepresentable or absent-rust-product-boundary may be N/A
Capacity, bounds, alignment, persistence, partial I/O, or cleanup result
    -> applicable regardless of RAII or API-shape differences
C++ hot-cache pointer/refcount mechanism only
    -> N/A only when owned-byte Rust reads expose no equivalent lifecycle result
Admission, hit/miss, invalidation, coherence, or eviction result
    -> applicable
Standalone C++ selection helper with unreachable Rust intermediate state
    -> absent-rust-product-boundary only after tracing the complete Rust selector
Metric/config parser text that typed Rust construction cannot represent
    -> language-unrepresentable or cpp-build-or-abi only when no public text config path exists
```

Patch each retained N/A with one allowed `review.na_category`; convert every observable gap to an actionable `missing` row. Claim `covered` only with one complete unique Rust test.

- [ ] **Step 3: Validate, independently review, and commit**

The reviewer checks all 30 C++ bodies and every identified Rust boundary, with special attention to POSIX/LocalDisk results that are prerequisites for later Rust `io_uring` work. Correct all findings, then run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m json.tool parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
cd ../../..
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
git diff --check
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
  --files rust-repo/tools/store-validation/parity-map.json
git add rust-repo/tools/store-validation/parity-map.json
git commit -m "[Store] re-audit storage and cache N/A parity"
```

---

### Task 4: Enforce approved N/A categories in the validator

**Files:**
- Modify: `rust-repo/tools/store-validation/validate_parity.py`
- Modify: `rust-repo/tools/store-validation/tests/test_validate_parity.py`
- Modify: `rust-repo/tools/store-validation/parity-map.json` only if the new tests reveal an audit metadata error

**Interfaces:**
- Consumes: retained N/A rows with `review.na_category` from Tasks 1–3.
- Produces: `ALLOWED_NA_CATEGORIES: frozenset[str]` and validator findings `missing-na-category`, `invalid-na-category`, and `unexpected-na-category`.

- [ ] **Step 1: Write failing category-contract tests**

Add these cases to `test_validate_parity.py`, adapting the existing `entry()` helper so valid N/A fixtures carry a category:

```python
def test_not_applicable_requires_allowed_na_category(self):
    missing = entry(status="not-applicable", rust=[], reason="No Rust boundary.")
    self.assertIn(
        "missing-na-category",
        {item.code for item in validate_manifest(
            {"schema_version": 1, "entries": [missing]}, {CPP_REF}, set()
        )},
    )

    invalid = entry(status="not-applicable", rust=[], reason="Too expensive.")
    invalid["review"]["na_category"] = "too-expensive"
    self.assertIn(
        "invalid-na-category",
        {item.code for item in validate_manifest(
            {"schema_version": 1, "entries": [invalid]}, {CPP_REF}, set()
        )},
    )

def test_non_na_row_rejects_stale_na_category(self):
    covered = entry()
    covered["review"]["na_category"] = "absent-rust-product-boundary"
    self.assertIn(
        "unexpected-na-category",
        {item.code for item in validate_manifest(
            {"schema_version": 1, "entries": [covered]}, {CPP_REF}, {RUST_REF}
        )},
    )
```

Update `test_not_applicable_requires_nonempty_reviewed_reason` to supply an allowed category so it still isolates the empty-reason finding.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m unittest \
  tests.test_validate_parity.ValidateParityTest.test_not_applicable_requires_allowed_na_category \
  tests.test_validate_parity.ValidateParityTest.test_non_na_row_rejects_stale_na_category -v
```

Expected: FAIL because the validator does not yet emit the three category findings.

- [ ] **Step 3: Implement the minimal category validation**

Add this constant near `ALLOWED_STATUSES`:

```python
ALLOWED_NA_CATEGORIES = frozenset(
    {
        "language-unrepresentable",
        "absent-rust-product-boundary",
        "cpp-build-or-abi",
        "noncanonical-duplicate-source",
        "excluded-component-internal",
        "no-executable-cpp-oracle",
        "excluded-performance-scope",
    }
)
```

Inside the existing validated `review` object branch, add:

```python
na_category = review.get("na_category")
if status == "not-applicable":
    if not isinstance(na_category, str) or not na_category.strip():
        findings.append(
            _finding(
                "missing-na-category",
                reference,
                "not-applicable entries require review.na_category",
            )
        )
    elif na_category not in ALLOWED_NA_CATEGORIES:
        findings.append(
            _finding(
                "invalid-na-category",
                reference,
                f"review.na_category must be one of {sorted(ALLOWED_NA_CATEGORIES)}",
            )
        )
elif "na_category" in review:
    findings.append(
        _finding(
            "unexpected-na-category",
            reference,
            "only not-applicable entries may set review.na_category",
        )
    )
```

- [ ] **Step 4: Run focused and full tests and verify GREEN**

Run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m unittest tests.test_validate_parity -v
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
```

Expected: focused suite and the now-21-test full suite pass; real manifest exits 0 without findings.

- [ ] **Step 5: Review, prove C/C++ immutability, and commit**

Obtain an independent code review of the tests, allowed set, and exact manifest results. Then run:

```bash
cd /home/fy2462/Mooncake/.worktrees/ha-chaos-live
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
git diff --check
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run --files \
  rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py \
  rust-repo/tools/store-validation/parity-map.json
git add rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py \
  rust-repo/tools/store-validation/parity-map.json
git commit -m "[Store] enforce reviewed N/A parity categories"
```

Expected: commit contains no C/C++ path and all hooks pass without invoking the C++ formatter.

---

### Task 5: Run the final N/A cross-review and publish the audited baseline

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json` only for reviewer-approved corrections
- Update locally: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Consumes: the three audited batches and enforced category validator.
- Produces: final retained-N/A count, converted-to-missing count, converted-to-covered count, category totals, and an independently approved base for the next one-to-one migration plan.

- [ ] **Step 1: Generate the authoritative audit summary**

Run:

```bash
cd /home/fy2462/Mooncake/.worktrees/ha-chaos-live
jq -r '
  [.entries[] | select(.status == "not-applicable") | .review.na_category]
  | group_by(.) | map({category: .[0], count: length})
' rust-repo/tools/store-validation/parity-map.json
jq -r '
  [(.entries | length),
   ([.entries[] | select(.status == "covered")] | length),
   ([.entries[] | select(.status == "missing")] | length),
   ([.entries[] | select(.status == "not-applicable")] | length),
   ([.entries[] | select(.status == "blocked")] | length)] | @tsv
' rust-repo/tools/store-validation/parity-map.json
```

Expected: category counts sum to the final N/A count; total remains exactly 1,399. Record exact disposition deltas relative to `dc7da5d3` rather than assuming all 107 remain N/A.

- [ ] **Step 2: Perform independent full cross-review**

The reviewer receives the immutable baseline commit `dc7da5d3`, all audit commits, the design, and this plan. They verify:

```text
all original 107 rows received a second source review
every retained N/A has one allowed category and a source-backed reason
no retained N/A has a Rust-visible Store result
all converted missing rows have executable unique test names and exact deltas
all converted covered rows have one complete unique discoverable Rust test
etcd-only policy excludes only backend internals, not fail-fast product behavior
no C/C++ file changed in any audit commit
```

Any Critical, Important, or Minor finding is corrected in a manifest-only commit and re-reviewed by the same independent reviewer.

- [ ] **Step 3: Run final verification**

Run:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m json.tool parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --cpp-root ../../../mooncake-store/tests \
  --rust-root ../../crates
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
cd ../../..
git diff --quiet -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.cu' '*.cuh' '*.h' '*.hh' '*.hpp'
git diff --check
git status --short --branch
```

Expected: JSON and ordinary validator pass without `ERROR`, all validation tests pass, C/C++ paths have no diff, and the tracked worktree is clean.

- [ ] **Step 4: Record handoff and start the next focused plan**

Append the approved disposition/category totals and verification commands to the local progress ledger. Do not mark full correctness complete. The next plan must implement the one-to-one validator and normalize the 227 current covered rows before beginning the 1,065-plus stable-order missing backlog.
