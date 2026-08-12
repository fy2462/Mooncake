# Master Metrics Initial Contract Parity Design

## Scope

Cover `MasterMetricsTest.InitialStatusTest` with an exact Rust witness. The
batch adds every metric family observed by that C++ test, exposes one typed
read-only snapshot of their current values, and proves that a fresh process
reports zero for the complete contract.

This batch defines and registers missing metrics but does not yet increment
them in request handlers. Operation-path instrumentation belongs to the later
`BasicRequestTest`, `BatchRequestTest`, and eviction/SSD metric batches, where
non-zero transitions can be proved independently.

## Considered Approaches

1. Add the missing metric families and a typed compatibility snapshot. This is
   selected because it makes absent metrics distinguishable from zero-valued
   metrics and gives later behavior tests one stable inspection boundary.
2. Parse the global Prometheus text output in the test. This would prove
   registration, but a missing family could be accidentally treated as zero
   unless every expected name were separately checked, and later tests would
   duplicate text parsing.
3. Assert only the Rust metrics that already exist. This would leave the C++
   copy/move, eviction, batch, and PutStart lifecycle observations uncovered
   while incorrectly marking the row complete.

## Metric Families

Keep the existing module ownership:

- `metrics/operations.rs`: request and failure counters for CopyStart,
  CopyEnd, CopyRevoke, MoveStart, MoveEnd, and MoveRevoke.
- `metrics/batch.rs`: request, failure, partial-success, item, and failed-item
  counters for BatchGetReplicaList, BatchPutStart, BatchPutEnd, and
  BatchPutRevoke. Existing BatchPutEnd and BatchPutRevoke request/failure
  counters remain the authoritative instances; only their missing matrix
  members are added.
- `metrics.rs`: cumulative eviction attempts, successes, evicted keys, and
  evicted bytes; cumulative PutStart discard and release counts; and the
  current discarded-staging-bytes gauge.

Every new family uses the existing `mooncake_store_` naming convention and is
registered by `register_metrics`. Counters use `IntCounter`; the staging value
uses `IntGauge` because it represents current retained bytes.

## Typed Initial Snapshot

Add `MasterMetricSnapshot` and `master_metric_snapshot()` in `metrics.rs`.
The struct contains a named field for every value asserted by the C++ oracle:

- allocated/total Memory and file bytes plus their zero-safe used ratios;
- key count;
- all scalar Put/Get/Exist/Remove/Mount/Unmount request and failure counters;
- all Copy/Move request and failure counters;
- the four eviction counters;
- the complete five-value matrix for each of the four C++ batch operations;
- PutStart discard count, release count, and discarded staging bytes.

Ratios are derived at read time as `allocated / total`, returning `0.0` when
capacity is zero. The snapshot is read-only and does not reset process-global
metrics. Field names are explicit so later non-zero tests cannot silently omit
one part of a matrix.

## Exact Witness and Isolation

Add a unit test in `metrics.rs` using the repository's existing child-process
pattern. The parent invokes the current test binary with the exact test name
and a private environment marker. The child reads `master_metric_snapshot()`
before constructing a service or touching any operation and asserts every
integer field equals zero and both ratios equal `0.0`.

Process isolation is required because Prometheus counters are global lazy
singletons and Rust's test order is not a correctness boundary. The child
marker prevents recursion; child assertion failure becomes a non-zero process
status reported by the parent.

The test also calls `register_metrics()` and gathers Prometheus families,
asserting that each newly introduced family name is present. This separately
proves registration rather than merely proving the backing singleton starts at
zero.

## Parity Accounting and Verification

Update only `MasterMetricsTest.InitialStatusTest` from `missing` to `covered`.
The manifest reason must state that this batch proves initial existence and
zero values, not request-path increments.

Use TDD: first add the test against the desired snapshot API and observe a
compile failure. Then add the minimum definitions, registration, ratios, and
snapshot implementation. Run the exact test with one test thread, the adjacent
metrics unit tests in small filters, formatting, JSON validation, the parity
validator, and cached-diff checks. Obtain independent Critical/Important review
before committing the implementation batch.
