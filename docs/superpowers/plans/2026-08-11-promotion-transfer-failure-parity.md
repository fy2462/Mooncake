# Promotion Transfer-Failure Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic one-to-one Rust witness for `FileStoragePromotionTest.TransferWriteFailureLeavesNoNotify` while preserving the production promotion data flow.

**Architecture:** Extend the existing `cfg(test)` promotion seam registry with a one-shot transfer failure keyed by client UUID, tenant, and key. The witness uses a real Master and FilePerKey backend, forces only the transfer result, and observes the existing production failure-notification and continuation branch.

**Tech Stack:** Rust 2024, Tokio, tonic, Mooncake Store client/Master crates, native Transfer Engine link feature, JSON parity validation.

## Global Constraints

- The seam is compiled only under `cfg(test)` and is isolated by client UUID, tenant, and key.
- Allocation orchestration, FilePerKey I/O, notification RPCs, and loop control remain production code.
- The witness must prove the transfer branch was reached after a successful LocalDisk read.
- Preserve all unrelated working-tree changes and stage only the intended files or exact JSON hunks.

---

### Task 1: Add the Red Transfer-Failure Witness

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: `inject_promotion_test_alloc_success(Uuid, &str, u64)`, `observe_promotion_test_calls(Uuid)`, `LocalStorageBackend::write_object(&str, &[u8])`.
- Produces: test `cpp_parity_file_storage_promotion_transfer_failure_releases_and_continues` and a compile-time requirement for `inject_promotion_test_transfer_failure(Uuid, &str, &str)`.

- [ ] **Step 1: Write the failing test**

Add a Tokio test beside the BatchLoad failure witness. Start a real offload-enabled Master, initialize a persistent FilePerKey backend, and store exact bytes under `local_storage_key("tenant-a", "transfer-fails")`:

```rust
let payload = vec![0x5a; 1024];
disk.write_object(
    &local_storage_key("tenant-a", "transfer-fails"),
    &payload,
)
.unwrap();

let _alloc_success =
    inject_promotion_test_alloc_success(client.client_id, "transfer-fails", 1024);
let _transfer_failure = inject_promotion_test_transfer_failure(
    client.client_id,
    "tenant-a",
    "transfer-fails",
);
```

Process `transfer-fails` followed by `later-task`, then assert:

```rust
assert_eq!(promoted, 0);
assert_eq!(counts.alloc.load(Ordering::Relaxed), 2);
assert_eq!(counts.disk_read.load(Ordering::Relaxed), 1);
assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 1);
assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 2);
```

- [ ] **Step 2: Run the exact test and capture the red result**

Run:

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib \
client::storage_local::tests::cpp_parity_file_storage_promotion_transfer_failure_releases_and_continues \
-- --exact --nocapture
```

Expected: compilation fails because `inject_promotion_test_transfer_failure` does not exist. This demonstrates the deterministic transfer boundary is absent.

### Task 2: Implement the One-Shot Transfer Seam

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`
- Test: `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

**Interfaces:**
- Consumes: `MooncakeClient::write_to_replica(&ReplicaDescriptor, &[u8]) -> StoreResult<()>` and existing promotion result handling.
- Produces: `inject_promotion_test_transfer_failure(Uuid, &str, &str) -> PromotionTestTransferFailureGuard` and `take_promotion_test_transfer_failure(Uuid, &str, &str) -> bool`, both under `cfg(test)`.

- [ ] **Step 1: Add the isolated registry and RAII guard**

Use a `LazyLock<Mutex<HashSet<(Uuid, String, String)>>>`. The injection function inserts `(client_id, tenant_id, key)` and returns a guard. `Drop` removes an unused entry, and `take` removes and returns whether the entry existed.

```rust
#[cfg(test)]
static PROMOTION_TEST_TRANSFER_FAILURES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<(Uuid, String, String)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));
```

- [ ] **Step 2: Route the transfer result through the test-only seam**

Immediately after recording `counts.transfer_write`, compute the existing result:

```rust
#[cfg(test)]
let injected_transfer_failure =
    take_promotion_test_transfer_failure(self.client_id, tenant_id, key);
#[cfg(not(test))]
let injected_transfer_failure = false;

let write_result = if injected_transfer_failure {
    Err(StoreError::OperationFailed(-1))
} else {
    self.write_to_replica(&replica, &data).await
};
```

Feed `write_result` into the unchanged success/failure `match`, so the failure branch records `notify_failure`, calls `notify_promotion_failure_for_tenant`, and continues.

- [ ] **Step 3: Run formatting and the exact green test**

Run:

```bash
rustfmt --edition 2024 --check rust-repo/crates/mooncake-store-client/src/client/storage_local.rs
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib \
client::storage_local::tests::cpp_parity_file_storage_promotion_transfer_failure_releases_and_continues \
-- --exact --nocapture
```

Expected: formatting passes and exact test reports `1 passed`.

- [ ] **Step 4: Run adjacent and full regression coverage**

Run the exact BatchLoadFailure, AllocStartFailure, and NonPositiveSize tests, then:

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib
```

Expected: every focused witness passes and the full library count increases from 382 to 383 with zero failures.

- [ ] **Step 5: Commit the Rust repair**

Stage only `storage_local.rs`, inspect `git diff --cached`, then commit:

```bash
git commit -m "test(store): cover promotion transfer failure"
```

### Task 3: Publish Parity Evidence

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: committed Rust witness name and repair SHA from Task 2.
- Produces: one `missing` to `covered` transition and one remediation record.

- [ ] **Step 1: Update the exact manifest row**

For `FileStoragePromotionTest.TransferWriteFailureLeavesNoNotify`, set:

```json
"rust": [{
  "file": "mooncake-store-client/src/client/storage_local.rs",
  "test": "cpp_parity_file_storage_promotion_transfer_failure_releases_and_continues"
}],
"status": "covered"
```

Explain that the first task completes a real FilePerKey read, reaches the injected transfer error, sends failure rather than success notification, and the second task proves continuation.

- [ ] **Step 2: Append the remediation record**

Record the C++ and Rust test references, the Task 2 repair SHA, exact/full commands, compile-red evidence, exact observer counts, and full-suite count.

- [ ] **Step 3: Validate all parity manifests and validator tests**

Run:

```bash
python3 rust-repo/tools/store-validation/validate_parity.py --repo-root . \
  --manifest rust-repo/tools/store-validation/parity-map.json \
  --manifest rust-repo/tools/store-validation/tent-parity-map.json \
  --manifest rust-repo/tools/store-validation/transfer-engine-parity-map.json \
  --manifest rust-repo/tools/store-validation/wheel-store-parity-map.json
PYTHONPATH=rust-repo/tools/store-validation \
python3 -m unittest discover -s rust-repo/tools/store-validation/tests -p 'test_*.py'
```

Expected store active count: `1114 covered / 169 missing / 116 not-applicable`; validator unit tests: `44 passed`.

- [ ] **Step 4: Commit only the exact JSON changes**

Inspect staged blobs as valid JSON, run `git diff --cached --check`, and commit:

```bash
git commit -m "docs(store): cover promotion transfer failure"
```

### Task 4: Independent Review

**Files:**
- Review only: the Task 2 and Task 3 commits.

**Interfaces:**
- Consumes: code repair, witness, manifest row, ledger record, and verification evidence.
- Produces: no unresolved Critical/Important findings, or a concrete repair loop.

- [ ] **Step 1: Request focused review**

Ask the existing parity reviewer to compare the C++ oracle, Rust reached branch, test seam isolation, real disk-read evidence, continuation proof, SHA, references, and counts.

- [ ] **Step 2: Resolve every Critical/Important finding**

For each finding, reproduce the issue, repair it with a focused regression, rerun proportional verification, update the ledger repair SHA if needed, and request re-review. Completion requires the reviewer to report Ready with no Critical/Important finding.
