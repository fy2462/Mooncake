# Promotion Success-Notify Failure Parity Design

## Scope

Cover `FileStoragePromotionTest.NotifyFailureDoesNotAbortBatch` with a discoverable Rust witness and repair the production divergence where `NotifyPromotionSuccess` failure currently aborts the entire promotion batch through `?`.

The required behavior is the current C++ contract: after a completed promotion write, a failed success notification triggers a best-effort failure notification, does not increment the promoted count, and leaves later tasks eligible for processing.

## Considered Approaches

1. Add a one-shot `cfg(test)` transfer-success seam keyed by client UUID, tenant, and key. This is selected because it lets the first task pass a real FilePerKey read and reach the real Master success-notification RPC deterministically, without requiring a native Transfer Engine destination.
2. Inject the success-notification failure itself. This is smaller but weaker because it would replace the Master commit RPC whose failure boundary the witness must prove.
3. Create a complete real Master object, registered memory segment, and native transfer, then race or invalidate the commit state. This exercises more infrastructure but introduces unnecessary native setup and timing dependence.

## Production Behavior

Replace the `NotifyPromotionSuccess(...).await?` propagation in the promotion loop with an explicit match:

- On success, increment `promoted` exactly once.
- On error, log the error, record the existing test observer's failure-notification attempt, call `NotifyPromotionFailure` best-effort for the same tenant/key, and continue to the next task.

The failure notification remains best-effort. Its own error must not replace the original per-key failure or abort the batch, matching C++.

## Test Seam

Add a test-only transfer-success registry beside the existing promotion allocation and transfer-failure seams. Entries are indexed by `(client UUID, tenant, key)`, consumed once, and removed by an RAII guard if unused. Production builds contain no registry or alternate transfer result.

After the existing transfer-attempt observer increment, test builds select the injected outcome in this order:

1. matching transfer failure;
2. matching transfer success;
3. real `write_to_replica`.

The two outcome registries must not both be configured for the same tuple. Injection helpers assert that the opposing outcome is absent so ambiguous tests fail immediately.

## Witness

Add `cpp_parity_file_storage_promotion_notify_failure_releases_and_continues` to `storage_local.rs`.

The fixture starts a real offload-enabled Master and client with persistent FilePerKey storage. It writes exact 1 KiB bytes under the first task's tenant-scoped key, injects allocation success and transfer success only for that task, and then processes a second positive task.

Because the first allocation exists only in the C++-FakeClient-equivalent test seam, the real Master `NotifyPromotionSuccess` RPC returns `KeyNotFound`. The repaired processor then attempts a real failure notification and continues. The second task reaches a real Master allocation failure, proving batch liveness.

Expected result and observer counts:

- `promoted = 0`;
- allocation attempts: 2;
- LocalDisk reads: 1;
- transfer attempts: 1;
- success-notification attempts: 1;
- failure-notification attempts: 2.

The transfer attempt occurs only after the real disk read returns successfully. The success-notification count proves the commit RPC was attempted, and the second allocation plus second failure notification proves continuation.

## Verification and Accounting

Use TDD: first add the witness and capture compilation failure for the missing transfer-success seam. After adding the seam but before repairing production behavior, the exact witness must fail because `process_promotion_tasks` returns `KeyNotFound`; this is the behavioral red proof.

After repair, run the exact witness, adjacent promotion failure witnesses, and the complete `mooncake-store-client` library suite. Run `rustfmt --check`, `git diff --check`, all four parity manifest validators, and the store-validation Python unit suite.

Commit code before documentation so the remediation log can record an immutable repair SHA. Update only the target manifest row from missing to covered, append one remediation record, preserve unrelated working-tree edits, and obtain independent review with no Critical/Important findings.
