# Promotion Success-Notify Failure Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair promotion success-notification failure handling and add a deterministic Rust parity witness for `FileStoragePromotionTest.NotifyFailureDoesNotAbortBatch`.

**Architecture:** Add a one-shot test-only transfer-success outcome keyed by client UUID, tenant, and key, while leaving the real FilePerKey read and real Master notification RPCs intact. Replace the production success-notify `?` with the C++ release-and-continue branch.

**Tech Stack:** Rust 2024, Tokio, tonic, Mooncake Store client/Master crates, native Transfer Engine link feature, JSON parity validation.

## Global Constraints

- Test outcomes are `cfg(test)`, one-shot, RAII-cleaned, and keyed by `(client UUID, tenant, key)`.
- Opposing injected transfer outcomes for one tuple are rejected immediately.
- The success-notification and failure-notification RPC calls remain real production calls.
- Preserve all unrelated working-tree edits and stage only intended files or exact JSON hunks.

---

### Task 1: Add the Compile-Red Witness

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: existing promotion allocation seam, observer, real Master fixture, and `LocalStorageBackend::write_object`.
- Produces: `cpp_parity_file_storage_promotion_notify_failure_releases_and_continues` and a compile-time requirement for `inject_promotion_test_transfer_success(Uuid, &str, &str)`.

- [ ] **Step 1: Add the witness fixture and assertions**

Start an offload-enabled real Master. Initialize persistent FilePerKey storage and write `vec![0x6b; 1024]` under `local_storage_key("tenant-a", "notify-fails")`. Attach it to a real client, inject allocation success and the not-yet-defined transfer success for the first task, and process a later positive task.

Assert the repaired final contract:

```rust
assert_eq!(promoted, 0);
assert_eq!(counts.alloc.load(Ordering::Relaxed), 2);
assert_eq!(counts.disk_read.load(Ordering::Relaxed), 1);
assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 1);
assert_eq!(counts.notify_success.load(Ordering::Relaxed), 1);
assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 2);
```

- [ ] **Step 2: Run exact test and capture compile red**

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib \
client::storage_local::tests::cpp_parity_file_storage_promotion_notify_failure_releases_and_continues \
-- --exact --nocapture
```

Expected: compile failure because `inject_promotion_test_transfer_success` is absent.

### Task 2: Add the Transfer-Success Seam and Capture Behavior Red

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: existing `(Uuid, String, String)` transfer-failure registry and transfer result selection.
- Produces: `inject_promotion_test_transfer_success(Uuid, &str, &str) -> PromotionTestTransferSuccessGuard` and `take_promotion_test_transfer_success(Uuid, &str, &str) -> bool` under `cfg(test)`.

- [ ] **Step 1: Add the success registry and RAII guard**

Use a separate `LazyLock<Mutex<HashSet<(Uuid, String, String)>>>`. On failure or success injection, assert the opposing registry does not contain the tuple. Consume success once at the production transfer boundary.

- [ ] **Step 2: Select the test transfer outcome without changing production**

In test builds, take both outcomes and assert they are not simultaneously true. Use:

```rust
let write_result = if injected_transfer_failure {
    Err(StoreError::OperationFailed(-1))
} else if injected_transfer_success {
    Ok(())
} else {
    self.write_to_replica(&replica, &data).await
};
```

In non-test builds, both flags are `false`.

- [ ] **Step 3: Run exact test and capture behavioral red**

Run the Task 1 command.

Expected: the test compiles but fails at `.unwrap()` because the real Master success-notification RPC returns `StoreError::KeyNotFound`; later-task counts are not reached. This proves the production `?` divergence.

### Task 3: Repair Success-Notify Failure Handling

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: `notify_promotion_success_for_tenant` and `notify_promotion_failure_for_tenant`.
- Produces: per-key release-and-continue semantics with `StoreResult<usize>` returning normally.

- [ ] **Step 1: Replace `?` with explicit result handling**

In the transfer-success branch, keep the success-attempt observer before the RPC, then use:

```rust
match self.notify_promotion_success_for_tenant(key, tenant_id).await {
    Ok(()) => promoted += 1,
    Err(error) => {
        tracing::warn!(target: "storage_debug", %tenant_id, %key, %error,
            "promotion: success notification failed");
        #[cfg(test)]
        record_promotion_test_call(self.client_id, |counts| &counts.notify_failure);
        let _ = self
            .notify_promotion_failure_for_tenant(key, tenant_id)
            .await;
        continue;
    }
}
```

- [ ] **Step 2: Run formatting and exact green test**

Run `rustfmt --edition 2024 --check` on `storage_local.rs`, then the Task 1 exact command.

Expected: `1 passed`, with the exact 2/1/1/1/2 observer tuple and `promoted = 0`.

- [ ] **Step 3: Run adjacent promotion regressions and full client lib**

Run exact TransferWriteFailure, BatchLoadFailure, AllocStartFailure, and NonPositiveSize witnesses, followed by:

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib
```

Expected: all focused tests pass; full client library increases from 383 to 384 tests with zero failures.

- [ ] **Step 4: Commit the Rust repair**

Stage only `storage_local.rs`, inspect the cached diff, run `git diff --cached --check`, and commit:

```bash
git commit -m "fix(store): continue after promotion notify failure"
```

### Task 4: Publish and Validate Parity Evidence

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: witness name, code repair SHA, red/green outputs, observer tuple, and full-suite count.
- Produces: one exact missing-to-covered row and one remediation record.

- [ ] **Step 1: Mark only the target row covered**

Reference `mooncake-store-client/src/client/storage_local.rs:cpp_parity_file_storage_promotion_notify_failure_releases_and_continues`. Explain the real FilePerKey read, real Master success-notify failure, failure notify, and second-task continuation.

- [ ] **Step 2: Append the remediation record**

Record both red phases, immutable repair SHA, exact/full commands, observer counts, and verification results.

- [ ] **Step 3: Run validators**

```bash
python3 rust-repo/tools/store-validation/validate_parity.py --repo-root . \
  --manifest rust-repo/tools/store-validation/parity-map.json \
  --manifest rust-repo/tools/store-validation/tent-parity-map.json \
  --manifest rust-repo/tools/store-validation/transfer-engine-parity-map.json \
  --manifest rust-repo/tools/store-validation/wheel-store-parity-map.json
PYTHONPATH=rust-repo/tools/store-validation \
python3 -m unittest discover -s rust-repo/tools/store-validation/tests -p 'test_*.py'
```

Expected active store count: `1115 covered / 168 missing / 116 not-applicable`; validator unit tests: 44 pass.

- [ ] **Step 4: Commit exact staged JSON changes**

Parse both staged blobs as JSON, run `git diff --cached --check`, and commit:

```bash
git commit -m "docs(store): cover promotion notify failure"
```

### Task 5: Independent Review

**Files:**
- Review only: Task 3 and Task 4 commits.

**Interfaces:**
- Consumes: C++ oracle, Rust production change, seams, witness, manifests, and verification evidence.
- Produces: Ready with no Critical/Important findings, or a focused repair loop.

- [ ] **Step 1: Request review from the existing parity reviewer**

Ask the reviewer to verify that success-notify itself is a real failing Master RPC, failure notify is attempted once, later allocation proves continuation, seams are isolated, and refs/SHA/counts are correct.

- [ ] **Step 2: Resolve findings**

Reproduce and repair each Critical/Important finding, rerun proportional verification, update the ledger repair SHA when code changes, and repeat review until Ready.
