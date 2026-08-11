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
families. Its promotion path currently reads LocalDisk objects one at a time
and records one SSD latency observation after each successful object. That
does not provide the all-or-nothing, one-observation-per-batch contract.

## Chosen Design

Add one client-layer LocalDisk batch-read helper beside `promote_objects`. It
accepts the production `LocalStorageBackend`, an ordered collection of storage
keys, and optional `ClientMetrics`. It reads every value through the real
backend, returning the exact values in input order. Only after all reads
succeed does it call `observe_ssd_read` once with the total byte count, key
count, and elapsed duration.

Change `promote_objects` to load the complete heartbeat task batch through
this helper before processing allocations and notifications. Pair each task
with its already-loaded value in the existing order. This preserves the
current failure boundary: a LocalDisk read error aborts promotion before any
allocation or success notification. It improves metric fidelity without
moving client concerns into the storage backend.

Alternatives rejected:

- Injecting `ClientMetrics` into `LocalStorageBackend` would make the reusable
  persistence layer depend on client observability.
- Testing `observe_ssd_read` directly would prove only the metric primitive,
  not that a production batch-load result controls publication.
- Retaining per-object metric calls and summing them in tests would contradict
  the C++ requirement of exactly one latency observation per batch.

## Data and Error Semantics

The successful fixture uses multiple differently sized FilePerKey values. The
returned bytes must match every value exactly. Read operations equal the
number of requested keys, read bytes equal the checked sum of returned value
lengths, read and total latency counts each increase by exactly one, and all
write counters and write latency counts remain zero.

The failure fixture requests two absent keys from an initialized FilePerKey
backend. The helper returns the backend error and leaves read operations,
bytes, read latency, total operations, total bytes, and total latency at zero.
No partial metric publication is allowed if an earlier key was readable but a
later key failed.

Byte and key totals use checked conversions and addition. Overflow returns a
Store error before metric publication. Metrics remain optional: the batch read
must behave identically when metrics are disabled.

## TDD and Witnesses

Add two independently discoverable tests in the client storage-local test
module:

- `cpp_parity_file_storage_batch_load_records_ssd_metrics` writes several
  real FilePerKey values, invokes the production batch helper, verifies exact
  returned bytes, and parses `render_prometheus` to assert the complete success
  metric tuple.
- `cpp_parity_file_storage_batch_load_failure_does_not_record_ssd_metrics`
  invokes the same helper with an existing key followed by a missing key,
  verifies the error, and asserts the complete read/total metric tuple remains
  zero.

Write both tests before the helper exists and retain their missing-symbol RED.
After the minimal implementation passes, run both exact filters and the full
client library suite with the native Transfer Engine link feature.

## Manifest and Ledger

Change exactly the two selected rows in
`rust-repo/tools/store-validation/parity-map.json` from `missing` to `covered`.
Each row names its exact Rust witness and describes the production batch helper
boundary. Append exactly two entries to
`rust-repo/tools/store-validation/remediation-log.json` after the tests pass.
The Store manifest missing count moves from 197 to 195; the wheel manifest is
unchanged at 52 missing rows.

## Verification

Run both exact tests, the complete client library suite, and the relevant
client integration target if the helper changes its behavior. Then run all
four parity validators, validator self-tests, Rust 2024 formatting checks,
pre-commit on touched files, JSON parsing, and `git diff --check`. Confirm the
two manifest entries are covered and no unrelated manifest row changes status.

## Non-Goals

- No storage-backend metrics dependency.
- No change to offload write metrics or remote LocalDisk RPC metrics.
- No change to allocation, promotion notification, or failure-retry policy.
- No C/C++ source changes.
- No claim that these two witnesses cover the seven other missing
  `FileStorageTest` rows.
