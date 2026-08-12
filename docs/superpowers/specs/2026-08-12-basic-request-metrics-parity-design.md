# Basic Request Metrics Parity Design

## Scope

Cover C++ `MasterMetricsTest.BasicRequestTest` by executing its exact Memory
segment and object lifecycle through Rust production RPCs. Add the missing
global/per-segment Memory gauges and normalize request/failure accounting for
the eight scalar operations observed by this oracle.

## Request Counter Semantics

At the Tonic trait boundary, MountSegment, UnmountSegment, PutStart, PutEnd,
PutRevoke, ExistKey, GetReplicaList, Remove, and RemoveAll increment their
request counter once for every invocation and increment the matching failure
counter exactly when the RPC returns `Err`. Existing success-only increments in
implementation bodies move to that boundary so a successful call remains one,
not two. This also counts service-gate rejection as a failed request, matching
the already-established PutStart pattern.

## Authoritative Gauge Semantics

Expose two labeled Memory gauges keyed by segment name: allocated bytes and
capacity bytes. A single synchronization helper derives:

- global allocated/capacity from `SegmentAllocator::usage_totals()`;
- per-segment allocated bytes from allocator `used_bytes(segment_id)`;
- per-segment capacity from the mounted segment descriptor;
- key count from the authoritative object table.

The helper updates all currently mounted labels and removes labels for segments
that disappeared, so missing/empty segment ratios are exactly zero. It runs
after successful authoritative topology/object mutations and after restore or
background cleanup paths that change those sources. Observation helpers read
the same gauges and calculate zero-safe ratios.

## Exact Witness

Use a fresh child process to isolate process-global Prometheus state. Mount one
16-MiB `test_segment`, then execute the C++ sequence with one 1,024-byte
`test_key`: PutStart/Revoke, PutStart/End, ExistKey, GetReplicaList, non-forced
Remove, completed put followed by RemoveAll, and completed put followed by
UnmountSegment. After every checkpoint assert the exact counters, key count,
global allocation/capacity/ratio, and segment allocation/capacity/ratio from the
C++ fixture. Set the object's runtime lease deadline to the Unix epoch under a
test-only accessor before Remove instead of sleeping 100 ms; the public Remove
remains non-forced and exercises its production lease check. Assert empty and
unknown segment names return zero ratios after unmount.

## Considered Alternatives

1. Selected: synchronize exported gauges from authoritative allocator/object
   state and count RPC outcomes at one boundary. This prevents incremental
   drift and makes failures observable consistently.
2. Computing values only in test accessors would leave Prometheus metrics stale
   and would not satisfy the production oracle.
3. Hand-incrementing gauges in every handler is rejected because rollback,
   restore, background eviction, and unmount paths can easily diverge.

## Verification and Parity Accounting

Use TDD: first observe the fresh-child matrix fail on the first missing mount
counter/capacity gauge. Then implement counters and state synchronization, run
the exact test, the complete low-memory metrics test binary, focused object and
segment tests, formatting, JSON/parity validation, and independent review.
Update only `MasterMetricsTest.BasicRequestTest` to covered.
