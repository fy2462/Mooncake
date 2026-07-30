# Rust Store Covered Primary Semantic Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-read every remaining structurally one-to-one covered row and retain coverage only when its unique Rust primary test independently asserts the complete observable C++ oracle.

**Architecture:** Audit the immutable C++ test body, its directly exercised implementation, and the exact Rust primary body plus assertion-bearing helpers in five bounded source families. Retained rows receive `review.primary_reviewed: true`; partial primaries are removed and the row becomes an actionable `missing` entry. After all 99 rows are reviewed, extend the validator test-first so covered and blocked rows cannot omit this semantic-review marker.

**Tech Stack:** Python 3 standard library, JSON, `unittest`, `rg`, `sed`, Cargo source inspection without C/C++ execution, pre-commit from `/home/fy2462/Mooncake/.venv`.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- The design authority is `docs/superpowers/specs/2026-07-30-rust-store-one-to-one-test-parity-design.md`.
- C/C++ files matching `.c`, `.cc`, `.cpp`, `.cxx`, `.cu`, `.cuh`, `.h`, `.hh`, or `.hpp` are immutable read-only oracle inputs. Never edit, format, build, link, load, stage, or commit them.
- Every pre-commit command sets `SKIP=mooncake-code-format`.
- Use `/home/fy2462/Mooncake/.venv/bin/python` and apply every repository edit through `apply_patch`.
- Retain `covered` only when the one Rust primary test drives and asserts every executed C++ result relevant at the Rust Store boundary. Similar names, source logic, supporting tests, comments, cleanup-only behavior, or partial assertions are insufficient.
- A downgraded row becomes `missing`, has `rust: []`, has no N/A category or prerequisite, and names the removed partial evidence, exact uncovered C++ result, exact Rust target file, and one globally unique `cpp_parity_*` test.
- A retained row adds `review.primary_reviewed: true` without changing its C++ behavior, boundary, primary reference, or oracle provenance.
- Production HA remains etcd-only. Optional Redis/K8s internals cannot be introduced as required serving behavior.
- No correctness or performance implementation occurs in this review plan. Store LocalDisk `io_uring` remains gated on the later zero-missing correctness result.

## Shared Audit Procedure

For each exact batch below:

1. Run the C/C++ diff guard and extract covered rows whose `cpp.file` is in the task's literal file set. Assert the task's exact row count.
2. Locate each C++ `TEST`, `TEST_F`, or `TEST_P` with `rg -n`, read its complete body, and read every directly called Store implementation needed to interpret its assertions.
3. Locate the exact Rust function using its manifest `rust.file` and `rust.test`, read its complete body and every helper containing assertions or hiding observable operations.
4. Write a literal checklist of the C++ executed assertions/results and mark which exact Rust assertion proves each item. Ignore C++ comments and unexecuted cleanup.
5. Retain only complete primaries and add `review.primary_reviewed: true`. Downgrade every partial row using the Global Constraints.
6. Validate JSON, run `validate_parity.py`, run the full Python tests, prove the C/C++ diff guard, run scoped pre-commit, and commit only the manifest.

The validation command for every batch is:

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
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run --files \
  rust-repo/tools/store-validation/parity-map.json
```

---

### Task 1: Review portable and client-helper primaries

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only C++ tests: `kv_event_publisher_test.cpp`, `registered_pinned_memory_test.cpp`, `rpc_timeout_test.cpp`, `http_metadata_server_test.cpp`, `tenant_id_test.cpp`, `uds_transport_test.cpp`, `zstd_util_test.cpp`, `utils_test.cpp`, `object_data_type_test.cpp`, `transfer_task_test.cpp`, `file_storage_test.cpp`
- Read only Rust roots: `rust-repo/crates/mooncake-store-client/src/`, `rust-repo/crates/mooncake-store-core/`, `rust-repo/crates/mooncake-store-master/src/`, `rust-repo/crates/mooncake-store-master/tests/`

**Interfaces:**
- Consumes: exactly 25 covered rows across the eleven C++ files above.
- Produces: 25 explicit retained-or-downgraded semantic dispositions.

- [ ] **Step 1: Extract and assert the 25-row batch**

Use a read-only Python query over `parity-map.json` with the literal file set above and assert `len(rows) == 25`; print C++ and Rust references.

- [ ] **Step 2: Execute the Shared Audit Procedure for all 25 rows**

Pay particular attention to parsing rejection matrices, exact error class/context, timeout bounds, payload/FD partial-transfer handling, quota refund/retention, environment-variable names, and legacy/default serialization values.

- [ ] **Step 3: Validate and commit**

Run the shared validation matrix and commit only the manifest:

```bash
git add rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] review helper parity primaries'
```

---

### Task 2: Review allocator, memory, and storage primaries

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only C++ tests: `allocation_strategy_test.cpp`, `buffer_allocator_test.cpp`, `client_buffer_test.cpp`, `mmap_arena_fallback_test.cpp`, `eviction_strategy_test.cpp`, `storage_backend_test.cpp`
- Read only Rust roots: allocator, memory FFI, buffer allocator, eviction, and storage backend modules/tests under `rust-repo/crates/`

**Interfaces:**
- Consumes: exactly 24 covered rows across the six C++ files above.
- Produces: 24 explicit retained-or-downgraded semantic dispositions.

- [ ] **Step 1: Extract and assert the 24-row batch**

Print every C++/Rust pair and assert `len(rows) == 24` before inspecting source.

- [ ] **Step 2: Execute the Shared Audit Procedure for all 24 rows**

Check exact capacity/offset/alignment results, allocator-family parameterization, replica uniqueness and preferred-segment behavior, large/full error behavior, concurrency postconditions, hugepage fallback strictness, eviction order, and mixed-bucket batch results.

- [ ] **Step 3: Validate and commit**

Run the shared validation matrix and commit only the manifest:

```bash
git add rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] review allocator parity primaries'
```

---

### Task 3: Review client integration, cache, metrics, and selection primaries

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only C++ tests: `e2e/e2e_rand_test.cpp`, `batch_remove_test.cpp`, `client_integration_test.cpp`, `client_local_hot_cache_test.cpp`, `client_metrics_test.cpp`, `non_ha_reconnect_test.cpp`, `replica_selection_test.cpp`
- Read only Rust roots: `rust-repo/crates/mooncake-store-client/src/` and `rust-repo/crates/mooncake-store-client/tests/`

**Interfaces:**
- Consumes: exactly 25 covered rows across the seven C++ files above.
- Produces: 25 explicit retained-or-downgraded semantic dispositions.

- [ ] **Step 1: Extract and assert the 25-row batch**

Print every C++/Rust pair and assert `len(rows) == 25` before inspecting source.

- [ ] **Step 2: Execute the Shared Audit Procedure for all 25 rows**

Check exact bytes and cross-client identity, batch cardinality and per-item status, cache hit/miss/admission behavior, every metric family/label/HTTP status assertion, heartbeat activation timing, remount identity, and replica-order tie/fallback semantics.

- [ ] **Step 3: Validate and commit**

Run the shared validation matrix and commit only the manifest:

```bash
git add rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] review client parity primaries'
```

---

### Task 4: Review master scheduler, heartbeat, quota, promotion, and task primaries

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only C++ tests: `deadline_scheduler_test.cpp`, `nof_heartbeat_test.cpp`, `master_service_tenant_quota_test.cpp`, `promotion_on_hit_test.cpp`, `tenant_quota_test.cpp`, `task_executor_test.cpp`
- Read only Rust roots: `rust-repo/crates/mooncake-store-master/src/` and `rust-repo/crates/mooncake-store-master/tests/`

**Interfaces:**
- Consumes: exactly 11 covered rows across the six C++ files above.
- Produces: 11 explicit retained-or-downgraded semantic dispositions.

- [ ] **Step 1: Extract and assert the 11-row batch**

Print every C++/Rust pair and assert `len(rows) == 11` before inspecting source.

- [ ] **Step 2: Execute the Shared Audit Procedure for all 11 rows**

Check cancellation/idempotency, exact NoF failure thresholds and ownership, tenant object-count invariants, promotion deadline and staged-buffer lifecycle, connector failure atomicity, and source-segment extraction rather than adjacent task-selection behavior.

- [ ] **Step 3: Validate and commit**

Run the shared validation matrix and commit only the manifest:

```bash
git add rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] review master parity primaries'
```

---

### Task 5: Review HA, oplog, snapshot, and serializer primaries

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Read only C++ tests: `serializer_test.cpp`, `ha/oplog/localfs_oplog_store_test.cpp`, `ha/oplog/oplog_applier_test.cpp`, `ha/oplog/oplog_manager_test.cpp`, `ha/oplog/oplog_replicator_test.cpp`, `ha/leadership/ha_backend_availability_test.cpp`, `ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store_test.cpp`, `ha/snapshot/snapshot_child_process_test.cpp`, `ha/standby/standby_state_machine_test.cpp`
- Read only Rust roots: HA, oplog, snapshot, catalog, configuration, and state-machine modules/tests under `rust-repo/crates/mooncake-store-master/`

**Interfaces:**
- Consumes: exactly 14 covered rows across the nine C++ files above.
- Produces: 14 explicit retained-or-downgraded semantic dispositions while preserving etcd-only production serving.

- [ ] **Step 1: Extract and assert the 14-row batch**

Print every C++/Rust pair and assert `len(rows) == 14` before inspecting source.

- [ ] **Step 2: Execute the Shared Audit Procedure for all 14 rows**

Check exact wire/default compatibility, synchronous durability and latest-sequence state, read limits, replay allocator/state effects, size-boundary errors, initial sequence behavior, bootstrap baseline meaning, backend rejection, snapshot latest-marker deletion, legacy metadata/config fallback, and every promotion-flow transition.

- [ ] **Step 3: Validate and commit**

Run the shared validation matrix and commit only the manifest:

```bash
git add rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] review HA parity primaries'
```

---

### Task 6: Enforce the primary semantic-review marker

**Files:**
- Modify: `rust-repo/tools/store-validation/tests/test_validate_parity.py`
- Modify: `rust-repo/tools/store-validation/validate_parity.py`

**Interfaces:**
- Produces: `missing-primary-review` for a covered or blocked row whose `review.primary_reviewed` is not exactly `true`.
- Produces: `unexpected-primary-review` when missing or N/A rows carry the marker.

- [ ] **Step 1: Write focused failing tests**

Change the `entry()` helper to add `primary_reviewed: True` only when `status` is covered or blocked. Add these focused tests:

```python
def test_covered_and_blocked_require_primary_review(self):
    for status in ("covered", "blocked"):
        candidate = entry(
            status=status,
            reason=(
                "The existing primary is externally blocked."
                if status == "blocked"
                else ""
            ),
        )
        if status == "blocked":
            candidate["prerequisite"] = "CUDA-capable GPU"
        candidate["review"].pop("primary_reviewed")
        with self.subTest(status=status):
            self.assertIn(
                "missing-primary-review",
                finding_codes(candidate, {RUST_REF}),
            )

def test_missing_row_rejects_stale_primary_review(self):
    missing = entry(
        status="missing",
        rust=[],
        reason="No unique primary exists yet.",
    )
    missing["review"]["primary_reviewed"] = True
    self.assertIn(
        "unexpected-primary-review",
        finding_codes(missing, set()),
    )
```

- [ ] **Step 2: Run the two exact tests and verify RED**

Expected: both fail only because the validator lacks the marker rules.

- [ ] **Step 3: Implement the minimal review-marker branches**

Inside the valid review-object branch, add exactly:

```python
if status in {"covered", "blocked"}:
    if review.get("primary_reviewed") is not True:
        findings.append(
            _finding(
                "missing-primary-review",
                reference,
                f"{status} entries require review.primary_reviewed=true",
            )
        )
elif "primary_reviewed" in review:
    findings.append(
        _finding(
            "unexpected-primary-review",
            reference,
            "only covered or blocked entries may set review.primary_reviewed",
        )
    )
```

Do not infer the marker from `review.reviewed`.

- [ ] **Step 4: Verify GREEN and the real manifest**

Run the full Python suite and real validator. Expected: both pass; every retained covered row is marked, every downgraded row is unmarked, and primary uniqueness remains zero-duplicate.

- [ ] **Step 5: Run scoped hygiene and commit**

```bash
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run --files \
  rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py
git add rust-repo/tools/store-validation/validate_parity.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py
git commit -m '[Store] require reviewed parity primaries'
```

---

### Task 7: Publish the semantically reviewed covered baseline

**Files:**
- Modify only reviewer-approved manifest corrections.
- Update locally: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`.

**Interfaces:**
- Produces: exact retained-covered and downgraded-to-missing totals, with every retained primary structurally unique and semantically reviewed.

- [ ] **Step 1: Run final verification**

Run JSON parsing, the complete Python suite, real validator, Python compilation, scoped pre-commit, C/C++ diff guard, `git diff --check`, and `git status --short --branch`.

- [ ] **Step 2: Obtain the pending independent cross-review**

An independent reviewer reads all 99 C++ bodies and claimed Rust primaries, plus the final 98 N/A aggregate. Correct and re-review every finding. Inline self-review is not labeled independent.

- [ ] **Step 3: Record the next gate honestly**

Record exact counts and begin stable-order missing remediation only after validator success. Do not claim correctness parity or begin LocalDisk `io_uring` while any missing or required blocked row remains.
