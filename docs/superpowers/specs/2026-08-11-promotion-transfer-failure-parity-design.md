# Promotion Transfer-Failure Parity Design

## Scope

Cover the manifest row `FileStoragePromotionTest.TransferWriteFailureLeavesNoNotify` with a one-to-one, discoverable Rust witness. The witness must reach the production promotion transfer branch after a successful LocalDisk read, prove that a transfer error triggers best-effort failure notification rather than success notification, and prove that the batch continues to a later task.

This change is limited to deterministic test control and parity evidence. It does not change production promotion behavior or refactor the transfer engine.

## Considered Approaches

1. Add a `cfg(test)` one-shot transfer-failure seam keyed by `(client UUID, tenant, key)`. This is the selected approach because it reaches the exact production orchestration branch while remaining parallel-safe and deterministic.
2. Return an invalid replica descriptor and depend on the native Transfer Engine to reject it. This would exercise more native code, but the exact failure and stability would depend on local transport behavior and could fail before the intended write boundary.
3. Introduce a promotion transport trait and a full fake client. This would provide broad injection control but is disproportionate for one missing row and would increase production abstraction surface.

## Design

Add a test-only registry beside the existing promotion observer and allocation-success seam. Entries are indexed by client UUID plus tenant and key, consumed at most once, and removed by an RAII guard. Production builds contain no registry lookup or altered write behavior.

Immediately before `write_to_replica`, the production processor continues recording the existing transfer observer event. In test builds only, it consumes a matching injected transfer error; otherwise it calls the real `write_to_replica`. Both outcomes then use the existing production result branch:

- success sends `NotifyPromotionSuccess` and increments the promoted count;
- failure records and attempts `NotifyPromotionFailure`, does not send success notification, and continues the loop.

The seam controls only the return value at the transfer boundary. Allocation orchestration, LocalDisk lookup/read, failure handling, notification RPC calls, and loop control remain the real implementation.

## Witness

Create `cpp_parity_file_storage_promotion_transfer_failure_releases_and_continues` in `storage_local.rs`.

The fixture starts a real offload-enabled Master and a real client with a persistent FilePerKey backend. It stores exact bytes under the tenant-scoped LocalDisk key for the first task, injects allocation success for that task, and injects its transfer failure. A second positive task has no Master object and therefore reaches the real allocation-failure branch after the first task.

Expected result and observer counts:

- processor returns `Ok(0)`;
- allocation attempts: 2;
- LocalDisk reads: 1;
- transfer attempts: 1;
- success notifications: 0;
- failure notifications: 2;
- the stored bytes are read through the real backend before the injected transfer error.

The second allocation attempt is the batch-liveness proof. The first failure notification belongs to the reached transfer failure, while the second belongs to the later real allocation failure.

## Verification and Accounting

Run the new exact witness, the adjacent BatchLoad/AllocStart/NonPositive focused regressions, and the complete `mooncake-store-client` library suite with the existing native library path. Run `rustfmt --check`, `git diff --check`, all four parity-manifest validations, and the store-validation Python unit suite.

After the code repair commit exists, mark only `TransferWriteFailureLeavesNoNotify` covered and add a remediation-log entry containing the exact witness reference, repair SHA, commands, and observed counts. Preserve unrelated working-tree changes and stage only the intended files/hunks.
