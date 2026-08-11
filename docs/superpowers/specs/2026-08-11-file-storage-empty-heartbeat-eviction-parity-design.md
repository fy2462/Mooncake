# FileStorage Empty-Work Heartbeat Eviction Parity Design

## Goal

Close `FileStorageTest.HeartbeatRunsDiskWatermarkEvictionWithoutOffloadWork`
with one production-path Rust integration witness.

## C++ Oracle

The C++ fixture publishes three FilePerKey objects as LocalDisk replicas, then
runs one `FileStorage::Heartbeat`. The offload heartbeat has no work, but disk
watermark eviction must still run. All three backend records disappear and all
three Master queries return `OBJECT_NOT_FOUND`.

## Rust Boundary

Rust's storage background iteration already sequences offload, promotion, and
watermark eviction independently. In particular, `offload_objects` returning
zero does not return from `storage_worker_loop`; the same iteration continues
to `run_disk_watermark_eviction`.

Add one `link-native` in-process integration witness in
`test_client_inproc_e2e.rs`:

1. Create a zero-memory-segment client and mount an empty persistent FilePerKey
   backend as an offloading LocalDisk segment.
2. Write three canonical default-tenant records, then use the public classic
   completion API to publish generation-bearing LocalDisk metadata. Each object
   therefore has no masking Memory replica. Writing after mount is deliberate:
   fresh-Master recovery correctly discards records the Master does not know.
3. Call the real offload heartbeat once and assert it returns no tasks.
4. Start the real background workers with offloading and disk watermark
   eviction enabled, promotion/task polling/capacity reporting disabled, a
   short storage interval, and tiny ordered watermark ratios.
5. Wait until all three backend records are absent and all three Master queries
   return `KeyNotFound`.

This setup tests scheduling rather than merely calling the eviction helper.

## Scope

No production change is expected. If the witness fails, repair only the first
observed scheduling, notification, or metadata divergence. Do not add sleeps as
the success condition; use a bounded polling deadline.

## Counts

The committed Store manifest moves from `753/531/115` to `754/530/115`.
The accumulated checkout moves from `1095/189/115` to `1096/188/115`.

## Verification

Run the exact integration witness, the complete `test_client_inproc_e2e`
binary when practical, the complete client lib, all four parity validators,
validator pytest, both shell contracts, scoped formatting, JSON parsing, and
`git diff --check`. Stage only the new test hunk because the integration file
already contains unrelated accumulated changes.
