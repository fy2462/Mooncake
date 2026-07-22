# `up_main` Rust Store Parity Design

## Goal

Bring the Store and Transfer Engine behavior introduced by the `up_main`
merge into `rust-repo` without compiling, linking, or calling the C++ Store.
The work covers promotion retry, bounded CUDA host pinning, the Python
structured-object API, promotion observability, and RDMA NIC load statistics.

## Compatibility boundary

The C++ Store is a behavior oracle only. Store-owned state machines,
lifecycle, metrics, and Python APIs are implemented in `rust-repo`. Native
data-plane behavior remains in `mooncake-transfer-engine` and is consumed only
through `transfer-engine-ffi`.

The following are explicitly outside this effort:

- linking `libmooncake_store.so` into a Rust crate or Python extension;
- translating C++ classes or locking structures one for one;
- porting C++ CMake, release-wheel, or GitHub workflow implementation details;
- persisting transient promotion retry candidates in snapshots or oplogs.

## Delivery strategy

Deliver four independently testable behavior slices in this order:

1. promotion retry and metrics;
2. host CUDA pinned-memory lifecycle;
3. Python structured-object API;
4. Transfer Engine NIC load statistics FFI.

Each slice receives focused tests and a separate implementation commit. A
final integration pass runs the workspace, Python, native FFI, documentation,
and dependency-boundary checks.

## Promotion retry and metrics

### State model

Add a `PromotionQueueResult` enum that distinguishes queued, permanent
rejection, and transient rejection outcomes. A transient watermark rejection,
queue-cap rejection, or queue-push failure records a `PromotionCandidate` in a
concurrent map keyed by the existing tenant-scoped object key.

Each candidate records:

- highest observed Count-Min Sketch score;
- first and most recent observation times;
- next retry time;
- last rejection reason and Store error;
- retry count.

Candidate state is bounded to 50,000 entries, expires 60 seconds after the last
observation, and stops after 8 evaluated retries. Backoff starts at 10 ms and
doubles to a maximum of 1 second. Re-observing an existing candidate refreshes
its last-seen time, preserves the maximum score, and makes it immediately
eligible again.

### Scheduler

The existing eviction worker drives retry evaluation. Each tick scans at most
64 logical shards and evaluates at most 128 due candidates. Rust currently
uses concurrent maps rather than the C++ metadata shard table, so the logical
shard cursor is derived from a stable hash partition of the tenant-scoped key.
This preserves bounded work and scan fairness without reproducing C++ locks.

Before retrying, the scheduler removes candidates whose object disappeared,
already has a complete memory replica, lacks a complete local-disk source, or
already has an in-flight promotion. Successful admission removes the
candidate. Transient rejection advances backoff. Permanent rejection removes
the candidate.

Snapshot restore and standby bootstrap start with an empty candidate table,
zero candidate count, and a reset scan cursor. Promotion tasks continue to use
the existing snapshot/oplog behavior.

### Metrics

Expose counters with the C++ metric names:

- `master_promotion_candidate_recorded_total`;
- `master_promotion_candidate_admitted_total`;
- `master_promotion_candidate_admission_rejected_total`;
- `master_promotion_candidate_expired_evaluated_total`;
- `master_promotion_candidate_expired_unevaluated_total`;
- `master_promotion_candidate_dropped_limit_total`.

Tests cover watermark recovery, queue-cap recovery, deduplication, permanent
ineligibility, retry backoff, TTL, retry limit, global cap, state reset, and
metric increments.

## Host CUDA pinned-memory lifecycle

### Ownership

Implement an RAII `PinnedMemoryManager` in the Rust client. It is separate from
Transfer Engine memory registration: Transfer Engine registration makes a
range available to transports, while CUDA host registration makes pageable
host memory directly accessible to CUDA.

The manager reads `MC_STORE_PIN_MEMORY_MAX_BYTES` once. Missing, invalid, or
zero values disable pinning. Pinning is attempted only for non-empty host
segments using an empty, TCP, RDMA, EFA, CXI, or `rpc_only` protocol. It is
best-effort on registration failure.

### Backend and safety

A small backend trait provides `register(addr, len)` and `unregister(addr)`.
Tests use a mock backend. The production CUDA backend is feature-gated and
loads the CUDA runtime symbols dynamically, so CPU-only Rust Store builds do
not gain a CUDA link dependency.

The manager rejects overlapping active regions and reservations that exceed
the configured byte cap. A region reservation is established before calling
the backend and rolled back if registration fails.

Dropping a successfully pinned region unregisters it before releasing its
backing allocation. If unregistration fails, the backing allocation is
intentionally retained for process lifetime; freeing a range still registered
with CUDA is forbidden. CUDA-runtime-unloading is treated as a successful
terminal release because the runtime no longer owns the mapping.

The client applies this lifecycle to setup segments and dynamically allocated
Store segments. Transfer Engine unregister and segment close/unmount occur
before CUDA unregistration.

## Python structured-object API

### Placement and dependency model

Add a pure-Python module under `rust-repo/python/mooncake_store/`. It imports
the Rust `MooncakeClient`, `BufferPool`, and `RegisteredBufferPool` APIs and
does not import the legacy `mooncake.store` extension.

The public surface matches the merged Python behavior:

- unified structured-object `put` and `get`;
- `put_dataproto` and DataProto reference import/export;
- schema-guided section and codec selection;
- dense ndarray, bytes, JSON, msgpack, and ragged tensor-dictionary fields;
- nullable-field validation and row-count validation;
- pool-backed zero-copy reads with explicit lease lifetime;
- multi-buffer and buffer-group puts;
- automatic, batch, and parallel put policies;
- cleanup of partially written objects after a failed bundle commit.

The module adapts to the Rust binding's existing async Python methods through a
narrow Store protocol adapter. Serialization and manifest formats remain byte
compatible with the merged module so references can move between C++-wheel and
Rust-wheel clients.

Tests reuse behavior vectors from the merged Python tests while replacing the
legacy extension with the Rust binding or an in-memory protocol double. They
cover byte compatibility, schema errors, ragged values, parallel limits,
buffer-pool lease ownership, partial failure cleanup, and cross-adapter reads.

## Transfer Engine NIC load statistics

The native Transfer Engine already owns `getNicLoadStats` and
`tent_get_nic_load_stats`. Extend bindgen allowlists and add safe wrappers in
`transfer-engine-ffi` for both classic and TENT engines.

The safe API returns `Vec<NicLoadStats>` where each item contains the device
name, in-flight bytes, and EWMA bandwidth in bytes per second. It first queries
the required count, allocates capacity, retries if the count grows, validates
UTF-8/device-name termination, and preserves non-zero native status codes as
`TransferEngineError`.

The FFI tests use injectable raw-call helpers to cover empty results, multiple
devices, capacity growth, invalid names, and native errors. Native C++ tests
remain the authority for live RDMA selector statistics.

## Error handling

- Promotion distinguishes transient and permanent outcomes internally while
  preserving current public Store error responses.
- CUDA registration failure logs and continues with pageable memory; CUDA
  unregistration failure retains the backing allocation and reports an error.
- Structured-object writes remove already-created chunks when a later write or
  manifest commit fails.
- NIC-stat native failures retain their status code and include operation
  context in the Rust error.

## Verification

Every slice follows red-green TDD. Final acceptance requires:

1. `cargo test --workspace` with `CARGO_BUILD_JOBS=5`;
2. focused promotion, pinned-memory, Python structured-object, and FFI tests;
3. compilation of the CUDA pin feature without requiring CUDA in the default
   feature set;
4. native Transfer Engine shared libraries and FFI tests for enabled engines;
5. Python tests from the project `.venv`, with caches and build products under
   `/home/fy2462/workspace/tmp/mooncake`;
6. Sphinx HTML construction using `/home/fy2462/Mooncake/.venv` and a shared
   output directory;
7. a dependency audit proving Rust Store artifacts do not link
   `libmooncake_store.so` or compile C++ Store sources.

## Implementation status

As of 2026-07-23, the promotion retry and metrics slice is implemented on
`rust_repo_main` by commits `520ebab0`, `881d057e`, `5d511ef1`, and
`b5137921`. The implementation includes bounded candidate recording,
partitioned retry with backoff and expiry, eviction-worker integration, the
six compatible Prometheus counters, and transient-state reset during snapshot
or oplog recovery. The remaining CUDA pinning, structured-object, and NIC load
statistics slices are unchanged and still required.

Focused verification used:

```text
cargo test -p mooncake-store-master --test test_promotion_retry
cargo test -p mooncake-store-master --test test_master_metrics
cargo test -p mooncake-store-master --test test_master_eviction_offload --test test_master_promotion --test test_service_cluster_parity_p1_p2
cargo test -p mooncake-store-master --lib test_recover_clears_transient_promotion_candidates_only
```

All Cargo commands used `CARGO_BUILD_JOBS=5` and the shared target directory
`/home/fy2462/workspace/tmp/mooncake/cargo-target`.
