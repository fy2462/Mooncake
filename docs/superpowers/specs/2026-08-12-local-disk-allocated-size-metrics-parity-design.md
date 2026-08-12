# Local Disk Allocated Size Metrics Parity Design

## Scope

Cover C++ `MasterMetricsTest.LocalDiskReplicaAllocatedSize` with an exact Rust
integration witness and make Rust's `master_allocated_file_size_bytes` gauge
follow the lifetime of `Disk` and `LocalDisk` replica descriptors. The change
does not alter allocation, routing, persistence, or removal behavior.

## Required Semantics

C++ increments the gauge in both disk-replica constructors and decrements it
when a live replica is destroyed or overwritten. Rust must therefore count the
sum of `size` for every `Disk` and `LocalDisk` descriptor attached to a live
object, regardless of replica status. Memory and NoF SSD descriptors do not
contribute.

Each `ObjectEntry` records the number of disk bytes it has already contributed
to the process-wide gauge. This field is runtime-only (`serde(skip)`) because a
process-local Prometheus gauge is rebuilt from restored object state. The
existing cache-accounting synchronization helper computes the current disk
byte sum, applies the signed delta, and updates the recorded amount. The common
object-removal helper subtracts the recorded amount exactly once.

All production paths that add, replace, retain, remove, or clear disk replica
descriptors must call the synchronization helper while holding their existing
object mutation guard. This includes offload completion, upsert, disk eviction,
recovery cleanup, replication/copy/move cleanup, and background reaping.

## Exact C++ Witness

Add a fresh-child integration test to `test_master_metrics.rs` so global metric
state cannot leak from adjacent tests. The child mounts a 64-MiB Memory segment,
creates a 4,096-byte object with real PutStart/PutEnd RPCs, reports a classic
unsolicited LocalDisk completion through the production NotifyOffloadSuccess
compatibility path, and asserts the gauge rises from zero to exactly 4,096.
It then removes the object through the production Remove RPC and asserts the
gauge returns exactly to zero. Response status and object existence are also
checked so direct metric manipulation cannot satisfy the witness.

## Safety and Failure Handling

Disk byte summation uses checked conversion to the signed Prometheus gauge
domain and saturates only for observation if corrupt/untrusted aggregate input
exceeds `i64::MAX`; normal mutation validation already constrains practical
sizes. Delta application distinguishes add and subtract, avoiding unsigned
underflow. A removed or replaced object is accounted from its recorded runtime
amount, not by rescanning after its descriptors have been moved away.

## Considered Alternatives

1. The selected per-object runtime ledger centralizes idempotence and survives
   cloning/projection without coupling the core descriptor type to metrics.
2. Adding counters only in NotifyOffloadSuccess and Remove would pass one test
   but drift on upsert, eviction, recovery, and background cleanup.
3. Giving `ReplicaDescriptor` a metric-aware `Drop` implementation would make
   ordinary clones and snapshot DTO copies mutate global state, so it is unsafe.
4. Recomputing the gauge only when sampled would require global MasterState
   access in the metrics module and leave the exported Prometheus gauge stale.

## Verification and Parity Accounting

Use TDD: observe the exact integration test fail at the post-offload gauge
assertion before production changes. Then run the exact test, focused accounting
unit tests (including two disk replicas, replacement, and idempotent removal),
nearby offload/removal tests, formatting, JSON validation, and parity validation
in low-memory single-threaded phases. Update only
`MasterMetricsTest.LocalDiskReplicaAllocatedSize` to covered and require an
independent Critical/Important review before committing.
