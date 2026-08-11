# FileStorage Batch-Load SSD Metrics Parity Design

## Context

The Store parity manifest has two applicable missing rows from
`file_storage_test.cpp`: `FileStorageTest.BatchLoadRecordsSsdMetrics` and
`FileStorageTest.BatchLoadFailureDoesNotRecordSsdMetrics`.

C++ times one `FileStorage::BatchLoad` call and publishes its SSD metrics only
after the whole backend batch succeeds. A successful batch increments read
operations once per slice, adds the sum of all slice sizes, and records exactly
one read-latency observation while leaving write metrics unchanged. If any
backend read fails, it publishes none of those read metrics.

Rust already owns the corresponding FilePerKey backend and `ClientMetrics`
families. Its production P2P offload handler already accepts a batch of
LocalDisk keys and reads them through `AttachedLocalStorage`, but it does not
publish SSD read metrics. That leaves the all-or-nothing,
one-observation-per-batch contract uncovered.

## Chosen Design

Add one client-layer LocalDisk batch-read helper in `offload/server.rs`. It
accepts the production `AttachedLocalStorage`, an ordered collection of
tenant-scoped storage keys and expected sizes, and optional `ClientMetrics`.
It reads every value through the real backend, validates each exact size, and
returns the values in input order. Only after all reads succeed does it call
`observe_ssd_read` once with the total byte count, key count, and elapsed
duration.

The production `batch_get_offload_object` handler calls this helper before it
registers the returned values with Transfer Engine. `OffloadReadHandler`
receives the same optional `Arc<ClientMetrics>` owned by `MooncakeClient`, so
metrics configuration and lifecycle remain shared with the client HTTP and
summary surfaces. The handler then performs its existing registration and
buffer-pool commit steps without changing their error semantics.

Alternatives rejected:

- Injecting `ClientMetrics` into `LocalStorageBackend` would make the reusable
  persistence layer depend on client observability.
- Testing `observe_ssd_read` directly would prove only the metric primitive,
  not that a production batch-load result controls publication.
- Rebatching `promote_objects` would alter the ordering between read failures,
  invalid tasks, and promotion notifications even though a direct production
  batch-read boundary already exists.

## Data and Error Semantics

The successful fixture uses multiple differently sized FilePerKey values. The
returned bytes must match every value exactly. Read operations equal the
number of requested keys, read bytes equal the checked sum of returned value
lengths, read and total latency counts each increase by exactly one, and all
write counters and write latency counts remain zero.

The failure fixture requests one existing key followed by one absent key from
an initialized FilePerKey backend. The helper returns the backend error and
leaves read operations, bytes, read latency, total operations, total bytes,
and total latency at zero. This strengthens the two-missing-key C++ fixture by
proving that an earlier successful read cannot cause partial publication.

Byte and key totals use checked conversions and addition. Overflow returns a
Store error before metric publication. Metrics remain optional: the batch read
must behave identically when metrics are disabled.

## TDD and Witnesses

Add two independently discoverable tests in the offload server test module:

- `cpp_parity_file_storage_batch_load_records_ssd_metrics` writes several
  real FilePerKey values, invokes the production batch helper, verifies exact
  returned bytes, and parses `render_prometheus` to assert the complete success
  metric tuple.
- `cpp_parity_file_storage_batch_load_failure_does_not_record_ssd_metrics`
  invokes the same helper with an existing key followed by a missing key,
  verifies the error, and asserts the complete read/total metric tuple remains
  zero.

Write both tests before the helper exists and retain their missing-symbol RED.
After the minimal implementation passes, run both exact filters, the complete
offload server tests, and the full client library suite with the native
Transfer Engine link feature.

## Manifest and Ledger

Change exactly the two selected rows in
`rust-repo/tools/store-validation/parity-map.json` from `missing` to `covered`.
Each row names its exact Rust witness and describes the production batch helper
boundary. Append exactly two entries to
`rust-repo/tools/store-validation/remediation-log.json` after the tests pass.
Against the committed parent before this wave, the Store manifest moves from
`covered=745, missing=539, not-applicable=115` to
`covered=747, missing=537, not-applicable=115`; the committed wheel manifest is
unchanged at `covered=5, missing=95, not-applicable=252`. In the active
accumulated checkout, which also contains earlier uncommitted parity work, the
same two rows move Store missing from 197 to 195 and leave wheel missing at 52.

## Verification

Run both exact tests, the complete client library suite, and an offload RPC
integration target covering successful batch reads. Then run all
four parity validators, validator self-tests, Rust 2024 formatting checks,
pre-commit on touched files, JSON parsing, and `git diff --check`. Confirm the
two manifest entries are covered and no unrelated manifest row changes status.

## Non-Goals

- No storage-backend metrics dependency.
- No change to offload write metrics or remote LocalDisk RPC metrics.
- No change to promotion, allocation, notification, or failure-retry policy.
- No C/C++ source changes.
- No claim that these two witnesses cover the seven other missing
  `FileStorageTest` rows.
