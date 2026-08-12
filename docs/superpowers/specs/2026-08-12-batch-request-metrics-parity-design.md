# Batch Request Metrics Parity Design

## Scope

Cover `MasterMetricsTest.BatchRequestTest` by wiring the five C++ batch metric
matrices to the real Rust batch RPC paths and executing the same two-stage
three-key/four-key fixture. This batch changes observation only; existing RPC
responses, status codes, allocation, and object lifecycle semantics remain
unchanged.

## Outcome Semantics

Each batch operation maintains five cumulative values:

- requests: increment once per accepted RPC;
- failures: increment once only when every item fails;
- partial successes: increment once only when at least one item succeeds and
  at least one item fails;
- items: increment by input item count;
- failed items: increment by the number of unsuccessful item results.

An empty input has zero successes and zero failures and therefore increments
neither failure nor partial-success. RPC-level validation/infrastructure errors
that return before per-item results remain outside the accepted-batch matrix.

`BatchExistKey` is special: `false` is a successful existence query, not an
item error. Its accepted calls therefore increment requests and items, while
failure/partial/failed-item remain unchanged for the C++ fixture.

## Implementation Boundary

Add a small internal helper in `metrics/batch.rs` that records a `(total,
failed)` outcome against references to one operation's five counters. Use it at
the end of BatchGetReplicaList, BatchPutStart, BatchPutEnd, and BatchPutRevoke,
after their result vectors have been finalized. Keep BatchExistKey's existing
request/item increments because its boolean results contain no failure status.

For BatchPutStart, success is `BatchStatus::Success` in each result. For the
other three status-bearing RPCs, status `0` is success and every nonzero status
is a failed item. The helper does not inspect or alter business status codes.

## Exact Witness and Isolation

Add a child-process integration test in `tests/test_master_metrics.rs`. The
parent invokes the exact test with a private marker; the child starts with fresh
global counters, mounts one 64-MiB Memory segment, and performs the C++ sequence:

1. BatchExistKey on three absent keys; all three boolean results are false.
2. BatchPutStart for lengths 1,024, 2,048, and 512; all succeed.
3. BatchGetReplicaList before completion; all three fail.
4. BatchPutEnd for all three; all succeed.
5. BatchExistKey and BatchGetReplicaList again; all three succeed.
6. BatchPutRevoke on the completed keys; all three fail.
7. Append one absent 512-byte key; BatchGetReplicaList is a 3/1 partial result.
8. BatchPutStart on all four keys; the three existing keys fail and the new key
   succeeds.

After every call, compare the relevant five fields in
`master_metric_snapshot()` to the exact cumulative C++ matrix. Also assert
response/result vector lengths and per-item success/failure shape, so direct
counter manipulation cannot satisfy the witness.

## Considered Alternatives

1. The shared outcome helper is selected because it centralizes the C++
   all-failed versus partial-success definition.
2. Duplicating counter arithmetic in each RPC would invite semantic drift.
3. Mutating counters directly in the test would not prove production RPC
   instrumentation and is rejected.

## Verification and Parity Accounting

Use TDD: add the exact child test first and observe failure because only
BatchExistKey is currently instrumented. Then implement the helper and four
call sites. Run the exact integration test with one test thread, the existing
batch unit/integration tests in small filters, formatting, JSON validation,
parity validation, and cached-diff checks.

Update only `MasterMetricsTest.BatchRequestTest` to covered, explicitly naming
the exact witness. Obtain independent Critical/Important review before
committing.
