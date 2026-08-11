# Promotion Failure Lifecycle Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three one-to-one Rust witnesses proving promotion per-key liveness and exact failure-notification attempts across allocation, LocalDisk, transfer, and commit failures.

**Architecture:** Extend only the existing `cfg(test)` promotion observer with keyed failure attempts. Drive a shared five-task matrix through real Master and FilePerKey paths plus the existing isolated allocation/transfer outcome seams, while keeping three distinct test entry points.

**Tech Stack:** Rust 2024, Tokio, tonic, Mooncake Store client/Master crates, native Transfer Engine link feature, JSON parity validation.

## Global Constraints

- Do not alter production promotion semantics.
- Keyed observations are isolated by client UUID and record immediately before the real failure-notification RPC.
- Each C++ row receives a distinct discoverable Rust test name.
- Preserve unrelated working-tree edits and stage only intended files or exact JSON hunks.

---

### Task 1: Add Compile-Red Lifecycle Witnesses

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: `PromotionTestCallCounts`, allocation/transfer test seams, real Master fixture, and persistent FilePerKey backend.
- Produces: three test names plus a compile-time requirement for `PromotionTestCallCounts::notify_failure_keys` and `run_promotion_failure_matrix()`.

- [ ] **Step 1: Add the shared-result assertions to three tests**

Add:

```rust
cpp_parity_file_storage_promotion_per_key_failures_are_independent
cpp_parity_file_storage_promotion_alloc_failure_notifies_master
cpp_parity_file_storage_promotion_post_alloc_failures_all_notify_master
```

The first and third call `run_promotion_failure_matrix().await`. The first asserts `promoted=0` and aggregate counts `alloc=5`, `disk=3`, `transfer=2`, `success=1`, `failure=5`. The third asserts the keyed vector equals, in order:

```rust
vec![
    ("tenant-a", "alloc-fails"),
    ("tenant-a", "load-fails"),
    ("tenant-a", "transfer-fails"),
    ("tenant-a", "notify-fails"),
    ("tenant-a", "after-failures"),
]
```

The allocation witness runs one real-Master missing-object task and asserts `alloc=1`, `failure=1`, keyed entry exactly `tenant-a/alloc-fails`, and every later stage zero.

- [ ] **Step 2: Run one exact test and capture compile red**

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib \
client::storage_local::tests::cpp_parity_file_storage_promotion_per_key_failures_are_independent \
-- --exact --nocapture
```

Expected: compile failure because the shared matrix helper or keyed observer field is absent.

### Task 2: Add Keyed Failure Observation

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: UUID-indexed `PROMOTION_TEST_CALL_COUNTS` registry.
- Produces: `PromotionTestCallCounts::notify_failure_keys: Mutex<Vec<(String, String)>>` and `record_promotion_test_failure(Uuid, &str, &str)`.

- [ ] **Step 1: Extend the observer**

Add the keyed vector to the defaultable counts struct. Implement:

```rust
#[cfg(test)]
fn record_promotion_test_failure(client_id: Uuid, tenant_id: &str, key: &str) {
    if let Some(counts) = PROMOTION_TEST_CALL_COUNTS.lock().unwrap().get(&client_id) {
        counts.notify_failure.fetch_add(1, Ordering::Relaxed);
        counts
            .notify_failure_keys
            .lock()
            .unwrap()
            .push((tenant_id.to_string(), key.to_string()));
    }
}
```

- [ ] **Step 2: Replace all four failure-counter calls**

In allocation, disk-read, transfer-write, and success-notify failure branches, replace the atomic-field observer call with:

```rust
record_promotion_test_failure(self.client_id, tenant_id, key);
```

Leave the real `notify_promotion_failure_for_tenant` calls immediately after each observer call.

### Task 3: Implement Shared Matrix and Allocation Witness

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: keyed observer, `inject_promotion_test_alloc_success`, transfer success/failure seams, and `process_promotion_tasks`.
- Produces: `async fn run_promotion_failure_matrix() -> (usize, Arc<PromotionTestCallCounts>)` under the tests module.

- [ ] **Step 1: Build the real fixture**

Start an offload-enabled Master and persistent FilePerKey backend. Write distinct exact 1 KiB payloads for `transfer-fails` and `notify-fails`. Attach the backend to a real client and install allocation success for `load-fails`, `transfer-fails`, and `notify-fails`; install transfer failure/success for the latter two.

- [ ] **Step 2: Process the ordered five-task matrix**

Pass `alloc-fails`, `load-fails`, `transfer-fails`, `notify-fails`, and `after-failures` to `process_promotion_tasks`. Drop the client, abort the server, and return promoted count plus the observer `Arc`.

- [ ] **Step 3: Implement the single allocation-failure witness**

Use a separate real Master/client/backend fixture with one `alloc-fails` task and no allocation seam. Assert the exact aggregate tuple and keyed vector.

- [ ] **Step 4: Run formatting and all three exact tests**

Run `rustfmt --edition 2024 --check` on `storage_local.rs`, then run each new test with `--exact --nocapture` and the existing native library path.

Expected: each reports `1 passed`.

- [ ] **Step 5: Run all promotion witnesses and full client lib**

Run the `cpp_parity_file_storage_promotion_` filter, then:

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib
```

Expected: full client library `387 passed, 0 failed`.

- [ ] **Step 6: Commit the Rust tests**

Stage only `storage_local.rs`, inspect the cached diff, run `git diff --cached --check`, and commit:

```bash
git commit -m "test(store): cover promotion failure lifecycle"
```

### Task 4: Publish Three Parity Rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: three test names, immutable Task 3 repair SHA, compile-red and green evidence.
- Produces: three missing-to-covered transitions and three remediation records.

- [ ] **Step 1: Update the three exact rows**

Map each C++ row to its corresponding distinct Rust test. Reasons must identify the real Master/FilePerKey portions, exact stage counts, keyed release attempts, and the difference between aggregate liveness and per-key release assertions.

- [ ] **Step 2: Append three remediation entries**

Each entry records the same repair SHA but its own C++/Rust references and row-specific evidence. Include exact commands, promotion group result, full client count, formatting result, and compile-red cause.

- [ ] **Step 3: Validate manifests and validator tests**

```bash
python3 rust-repo/tools/store-validation/validate_parity.py --repo-root . \
  --manifest rust-repo/tools/store-validation/parity-map.json \
  --manifest rust-repo/tools/store-validation/tent-parity-map.json \
  --manifest rust-repo/tools/store-validation/transfer-engine-parity-map.json \
  --manifest rust-repo/tools/store-validation/wheel-store-parity-map.json
PYTHONPATH=rust-repo/tools/store-validation \
python3 -m unittest discover -s rust-repo/tools/store-validation/tests -p 'test_*.py'
```

Expected active store count: `1118 covered / 165 missing / 116 not-applicable`; validator unit tests: 44 pass.

- [ ] **Step 4: Commit exact JSON hunks**

Parse both staged blobs as JSON, run `git diff --cached --check`, and commit:

```bash
git commit -m "docs(store): cover promotion failure lifecycle"
```

### Task 5: Independent Review

**Files:**
- Review only: Task 3 and Task 4 commits.

**Interfaces:**
- Consumes: C++ oracles, Rust observer/helper/tests, manifest rows, ledger entries, and verification outputs.
- Produces: Ready with no Critical/Important findings, or a concrete repair loop.

- [ ] **Step 1: Request focused review**

Ask the existing reviewer to verify each row independently, observer isolation, exact branch reachability, keyed failure attribution, SHA/references, and counts.

- [ ] **Step 2: Resolve findings**

Reproduce and repair every Critical/Important issue, rerun proportional tests/validators, update repair SHAs when code changes, and repeat review until Ready.
