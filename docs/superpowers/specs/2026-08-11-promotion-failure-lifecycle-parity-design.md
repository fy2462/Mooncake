# Promotion Failure Lifecycle Parity Design

## Scope

Cover three adjacent C++ promotion lifecycle rows with three distinct, discoverable Rust tests:

- `FileStoragePromotionTest.PerKeyFailuresAreIndependent`
- `FileStoragePromotionTest.AllocStartFailureNotifiesMaster`
- `FileStoragePromotionTest.PostAllocFailuresAllNotifyMaster`

The production allocation, LocalDisk read, transfer, success notification, failure notification, and loop-continuation behavior was aligned by the preceding promotion repairs. This batch adds stronger one-to-one evidence and test-only observation; it does not alter production semantics.

## Considered Approaches

1. Extend the existing test observer with tenant/key failure-notification attempts and drive shared deterministic failure scenarios. This is selected because it matches the C++ FakeClient test boundary while proving both aggregate control flow and per-key release attempts.
2. Assert aggregate call counts only. This is smaller but cannot establish that every failed key received exactly one release attempt.
3. Build complete real staged Master replicas for every failure type. This would exercise more infrastructure but requires native segment/transfer setup and is substantially heavier than the C++ unit-test oracle.

## Observer Extension

Add `notify_failure_keys: Mutex<Vec<(String, String)>>` to `PromotionTestCallCounts`. Add a test-only helper that, for the active client UUID, increments the existing failure counter and records `(tenant_id, key)` atomically under the observer registry lookup.

Replace the four promotion-loop test-observer failure increments with this keyed helper:

- allocation-start rejection;
- LocalDisk read failure;
- transfer-write failure;
- success-notification failure;

The actual `NotifyPromotionFailure` RPC remains unchanged and real. The observer records an attempt immediately before that RPC, just as the existing atomic count does. Client UUID isolation and the existing observer RAII guard preserve parallel-test safety.

## Shared Failure Matrix Fixture

Create a test helper that starts a real offload-enabled Master and client with persistent FilePerKey storage. It prepares the following ordered tasks under one tenant:

1. `alloc-fails`: no injected allocation; the real Master returns `KeyNotFound`.
2. `load-fails`: injected allocation success, but no LocalDisk record.
3. `transfer-fails`: injected allocation success, real FilePerKey bytes, injected transfer failure.
4. `notify-fails`: injected allocation success, real FilePerKey bytes, injected transfer success, then real Master success notification returns `KeyNotFound`.
5. `after-failures`: no injected allocation; its real allocation attempt proves every prior failure continued.

Expected aggregate result:

- `promoted = 0`;
- allocation attempts: 5;
- LocalDisk reads: 3;
- transfer attempts: 2;
- success-notification attempts: 1;
- failure-notification attempts: 5;
- keyed failure attempts contain each task exactly once in processing order.

The helper returns an owned snapshot of counts and keyed attempts rather than exposing guards, clients, or server tasks to individual tests.

## One-to-One Witnesses

### Per-Key Independence

`cpp_parity_file_storage_promotion_per_key_failures_are_independent` runs the full failure matrix. It asserts the aggregate stage tuple and that `after-failures` reached allocation and failure notification. This demonstrates that allocation, load, transfer, and commit failures do not abort later work.

### Allocation Failure Release

`cpp_parity_file_storage_promotion_alloc_failure_notifies_master` uses a single positive task against the real Master without an object. It asserts allocation=1, failure notification=1 for the exact tenant/key, and disk/transfer/success=0. This is intentionally distinct from the existing two-task `AllocStartFailureSkipsKey` witness.

### All Failure Paths Release

`cpp_parity_file_storage_promotion_post_alloc_failures_all_notify_master` runs the full failure matrix and asserts that `alloc-fails`, `load-fails`, `transfer-fails`, and `notify-fails` each appear exactly once in the keyed failure attempts. It also asserts `after-failures` once so continuation is not inferred only from aggregate totals.

Although the C++ fixture's missing backing file can prevent its configured notify failure from being reached, the current C++ production source is the oracle for all four release branches. The Rust witness deliberately supplies backing bytes and deterministic transfer outcomes so every claimed branch is actually reached.

## Verification and Accounting

Use TDD by first writing tests against the absent keyed observer snapshot/helper and capturing compile failures. Implement only test-support changes, then run each exact witness, all existing promotion witnesses, and the complete `mooncake-store-client` library suite. The full count should rise from 384 to 387.

Run `rustfmt --check`, `git diff --check`, all four parity manifest validators, and the store-validation Python unit suite. Commit Rust tests before documentation. Update exactly three manifest rows from missing to covered with distinct Rust test names, append three remediation records using the immutable code SHA, and obtain independent review with no Critical/Important findings.
