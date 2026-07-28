# Rust Store Sequenced OpLog Group Commit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Rust Store Master's globally locked synchronous oplog writer with a dedicated, bounded, sequenced group-commit worker that preserves every current durability guarantee and makes the canonical three-Master large-object chaos gate pass.

**Architecture:** `OpLogManager` becomes a thread-safe facade over a replaceable `SequencedOpLogWorker`; callers validate and enqueue encoded records, then wait for durable completion without holding an oplog-global mutex. One dedicated OS thread owns the mutable `OpLogStore`, assigns sequence IDs in dequeue order, batches up to 10 ms/100 records/1,000,000 encoded payload bytes, calls one `flush_durable`, publishes the committed boundary, and permanently poisons itself after any ambiguous failure. Etcd keeps its existing fenced atomic transaction containing all buffered entries and `/latest`, while service-level same-key guards and durability-failure fencing remain unchanged.

**Tech Stack:** Rust 2024 workspace, `std::sync::mpsc::sync_channel`, dedicated OS threads, Tokio runtime detection and `block_in_place`, `parking_lot`, `etcd-client`, Prometheus metrics, Cargo tests, Docker etcd HA gate, `.venv` pre-commit.

## Global Constraints

- Preserve the uncommitted Task 5 baseline exactly: `Cargo.lock`, `test_ha_chaos_live.rs`, `Cargo.toml`, `main.rs`, `main_args.rs`, `main_config.rs`, and `service/background_ops.rs` already contain evidence-backed work and must never be reset or overwritten wholesale.
- Every mutation that is durable before this change must still return success only after its oplog batch commits; do not make `PutEnd` or any other mutation asynchronous.
- Keep one strict, contiguous total sequence order and commit `/latest = N` in the same fenced transaction as all new entries through `N`.
- Keep same-key mutation guards held until durable acknowledgement; remove only the oplog-global mutex so unrelated keys can progress.
- After a timeout, transport error, fence mismatch, malformed response, sequence inconsistency, queue rejection following in-memory mutation, or ambiguous completion, fail closed and use the existing service-fencing path.
- A poisoned writer must reject all later commands without another mutating backend call; no retry is allowed after an ambiguous transaction result.
- Production defaults are batch window 10 ms, maximum batch records 100, maximum encoded payload bytes 1,000,000, queue capacity 1,024, etcd request deadline 10 seconds, and caller completion deadline 20 seconds.
- The byte limit ends a batch before adding a later record; the first individually valid record is always admitted so the existing 10 MiB per-record validation contract is not silently narrowed.
- Do not add production CLI tuning flags in this change; test constructors may inject shorter windows, capacities, and deadlines.
- Non-HA construction with no store remains a successful no-op returning sequence 0.
- Standby readers and change notifiers remain separate read-only store clients and never use the writer queue.
- Use etcd as the election and oplog persistence backend for the live gate.
- Use `/home/fy2462/Mooncake/.venv` only as the Python virtual environment; do not treat `.venv` as project source.
- Use `apply_patch` for source edits, run scoped pre-commit on touched files, preserve unrelated worktree changes, and review the exact staged diff before each commit.

## File Map

- Create `rust-repo/crates/mooncake-store-master/src/oplog/oplog_worker.rs`: bounded command protocol, batching loop, blocking-aware durable wait, poison state, query commands, replacement-safe shutdown, and deterministic worker tests.
- Modify `rust-repo/crates/mooncake-store-master/src/oplog.rs`: register the worker module and expose only the facade/test configuration needed by callers and benchmarks.
- Modify `rust-repo/crates/mooncake-store-master/src/oplog/oplog_manager.rs`: turn the manager into an interior-thread-safe facade, route mutations and queries through the worker, expose committed sequence atomically, and support worker replacement on promotion.
- Modify `rust-repo/crates/mooncake-store-master/src/oplog/oplog_etcd.rs`: make `append` buffer-only, retain one fenced transaction in `flush_durable`, and reuse a worker-local current-thread Tokio runtime outside an existing runtime.
- Modify `rust-repo/crates/mooncake-store-master/src/metrics.rs`: register writer submissions, queue state/rejection, batch records, queue/durable latency, poison, and post-poison failure metrics.
- Modify `rust-repo/crates/mooncake-store-master/src/service/mod.rs` and `src/service/state.rs`: store `Arc<OpLogManager>` without a surrounding mutex, use committed snapshot boundaries, and provide bounded manager replacement.
- Modify the exact service mutation files listed in Task 4: remove `.lock()` calls while preserving per-key guards and existing durability-failure fencing.
- Modify `rust-repo/crates/mooncake-store-master/src/main_ha.rs`: replace the leader writer through the manager facade and update the election view without a global mutex.
- Modify affected tests in `src/ha/oplog_applier.rs`, `src/service/background_ops.rs`, `tests/test_oplog.rs`, `tests/test_master_object.rs`, `tests/test_master_mount.rs`, and `tests/test_service_cluster_parity_p1_p2.rs`: query through the facade and add concurrency/fencing regressions.
- Preserve and validate `rust-repo/crates/mooncake-store-master/src/main.rs`, `src/main_args.rs`, `src/main_config.rs`, `Cargo.toml`, and `Cargo.lock`: retain the bounded etcd transport defense and 5 ms eviction configuration already proven necessary but insufficient alone.
- Preserve and validate `rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs`: canonical 42 x 3 MiB large-object, three-client, four-round exact-byte gate.
- Create `rust-repo/crates/mooncake-store-master/examples/oplog_group_commit_bench.rs`: reproducible synthetic synchronous-baseline versus group-commit throughput/latency/flush-count benchmark, executed only after correctness is green.

---

### Task 1: Dedicated sequenced worker and failure semantics

**Files:**
- Create: `rust-repo/crates/mooncake-store-master/src/oplog/oplog_worker.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics.rs`

**Interfaces:**
- Consumes: `OpLogStore`, `OpLogRecord`, `HaError`, and `validate_record_size` from the parent oplog module.
- Produces: `pub(crate) struct SequencedOpLogWorker`, `pub struct OpLogWorkerConfig`, `SequencedOpLogWorker::start(store: Box<dyn OpLogStore + Send>, config: OpLogWorkerConfig) -> Self`, `submit_durable(&self, payload: String, producer_view_version: u64, operation: &'static str) -> Result<u64, HaError>`, `submit_buffered(&self, payload: String, producer_view_version: u64, operation: &'static str) -> Result<(), HaError>`, `latest_assigned(&self) -> u64`, `latest_committed(&self) -> u64`, query/admin methods named in Task 3, and `shutdown(&self) -> Result<(), HaError>`. If `std::thread::Builder::spawn` fails, `start` returns an already poisoned facade carrying the spawn error so the unchanged `OpLogManager::new(...) -> Self` API remains fail-closed.
- `OpLogWorkerConfig` fields are `batch_window: Duration`, `max_batch_records: usize`, `max_batch_payload_bytes: usize`, `queue_capacity: usize`, and `completion_timeout: Duration`; `Default` uses the exact Global Constraints values.

- [ ] **Step 1: Add deterministic RED tests for batching and acknowledgement order**

  Add `CountingGateStore` inside `oplog_worker.rs::tests`. It wraps `InMemoryOpLog`, increments shared `append_calls` and `flush_calls`, optionally blocks `flush_durable` on a `Condvar`, and records the committed records. Add `concurrent_durable_commands_share_one_flush_and_get_consecutive_sequences`: start eight threads behind a `Barrier`, use a 50 ms test batch window, submit one distinct payload per thread, join them, and assert returned sequence IDs sort to `1..=8`, `flush_calls == 1`, `latest_assigned() == 8`, and `latest_committed() == 8`. While the flush gate is closed, assert no submitter has returned; after opening it, assert all eight return successfully.

- [ ] **Step 2: Run the worker test to prove RED**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --lib \
    oplog::oplog_worker::tests::concurrent_durable_commands_share_one_flush_and_get_consecutive_sequences \
    -- --exact --nocapture
  ```

  Expected: FAIL to compile because `oplog_worker` and `SequencedOpLogWorker` do not exist.

- [ ] **Step 3: Implement the bounded batching loop**

  Implement a bounded `sync_channel`. A `Persist` command carries payload, producer view, operation label, submission `Instant`, and optional one-slot completion sender. The worker thread owns the only `Box<dyn OpLogStore + Send>`. After receiving the first `Persist`, drain until 10 ms/test window, 100/test record limit, or 1,000,000/test payload-byte limit; preserve a non-`Persist` command in a one-element deferred slot. Call `store.append` for every accepted record in queue order, update `latest_assigned` after each append, call `flush_durable` exactly once, update `latest_committed` only after success, then deliver each durable result. Increment/decrement a writer-specific queue-depth gauge around enqueue/dequeue rather than reusing standby `OPLOG_PENDING_ENTRIES`.

- [ ] **Step 4: Add RED tests for poison fan-out, post-poison rejection, and queue full**

  Add three tests:

  - `failed_batch_poisons_all_waiters_and_prevents_later_backend_calls`: make the first flush return `HaError::InvalidBackend("injected ambiguous flush")`, assert every member sees the same cloned error, capture append/flush counts, submit again, and assert counts do not change.
  - `full_queue_rejects_immediately_and_never_blocks_sender`: configure capacity 1, hold the first flush, queue a second command, and assert a third call returns an error containing `oplog writer queue is full` in under 100 ms.
  - `shutdown_is_bounded_and_rejects_new_submissions`: shut down an idle worker, assert completion inside the injected 200 ms deadline, then assert a new call returns `oplog writer is shut down` without backend activity.

- [ ] **Step 5: Run poison/overload tests to prove RED**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --lib oplog::oplog_worker::tests -- --nocapture
  ```

  Expected: the new failure tests FAIL until terminal poison, `try_send` queue rejection, and bounded shutdown are implemented.

- [ ] **Step 6: Implement fail-closed terminal behavior and blocking-aware waits**

  Store `Option<HaError>` in shared poison state, clone the first terminal error to current waiters, drain already queued commands as failures, and reject later submissions before touching the channel. `submit_durable` validates the record before enqueue, uses `try_send`, then waits with `recv_timeout(completion_timeout)`. On a Tokio multi-thread runtime, execute that wait inside `tokio::task::block_in_place`; outside Tokio and on a current-thread runtime, wait directly because the backend runs on its own OS thread. A caller completion timeout installs a terminal poison before returning `HaError::InvalidBackend("oplog durable completion timed out after ...")`.

- [ ] **Step 7: Register exact writer metrics**

  Add and register:

  - `OPLOG_WRITER_SUBMITTED` counter: `mooncake_store_oplog_writer_submitted_total`.
  - `OPLOG_WRITER_QUEUE_REJECTIONS` counter: `mooncake_store_oplog_writer_queue_rejections_total`.
  - `OPLOG_WRITER_QUEUE_DEPTH` gauge: `mooncake_store_oplog_writer_queue_depth`.
  - `OPLOG_WRITER_BATCH_RECORDS` histogram with buckets `1, 2, 4, 8, 16, 32, 64, 100`.
  - `OPLOG_WRITER_QUEUE_WAIT_US` and `OPLOG_WRITER_DURABLE_WAIT_US` histograms with buckets `100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 20_000_000`.
  - `OPLOG_WRITER_POISON_EVENTS` and `OPLOG_WRITER_POST_POISON_FAILURES` counters.

- [ ] **Step 8: Verify Task 1 GREEN and commit only its files**

  Run:

  ```bash
  cd rust-repo
  rustfmt --edition 2024 --check crates/mooncake-store-master/src/oplog.rs \
    crates/mooncake-store-master/src/oplog/oplog_worker.rs \
    crates/mooncake-store-master/src/metrics.rs
  cargo test -p mooncake-store-master --lib oplog::oplog_worker::tests -- --nocapture
  cd ..
  git diff --check
  git add rust-repo/crates/mooncake-store-master/src/oplog.rs \
    rust-repo/crates/mooncake-store-master/src/oplog/oplog_worker.rs \
    rust-repo/crates/mooncake-store-master/src/metrics.rs
  git diff --cached --check
  git commit -m '[Store] add sequenced oplog commit worker'
  ```

  Expected: all worker tests PASS; the staged diff contains no pre-existing Task 5 file.

### Task 2: Etcd buffer-only append and one fenced transaction per batch

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog/oplog_etcd.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog/test_support.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_oplog.rs`

**Interfaces:**
- Consumes: worker contract `append` N times followed by exactly one `flush_durable`.
- Produces: `EtcdOpLogStore::append` that validates/fences/assigns/buffers without network I/O; `flush_durable` remains the only commit boundary and writes buffered entries plus `/latest` with the election compare.
- Produces for black-box regression: `buffered_etcd_records_for_test(last_seq: u64, entries: &[OpLogRecord]) -> Result<Vec<OpLogRecord>, HaError>`, a pure test helper using the same sequence assignment routine as `append`.

- [ ] **Step 1: Write RED regressions for multi-record buffering and sequence validation**

  Add `test_etcd_append_path_builds_contiguous_buffer_without_flushing` in `tests/test_oplog.rs`. Feed three zero-sequence records with producer view 7 to `buffered_etcd_records_for_test(40, ...)`; assert sequences `41, 42, 43`, unchanged payload/view, and no committed boundary is returned by the helper. Extend the internal transaction validation test to assert a three-record contiguous buffer accepts `expected_previous_seq == 40` and `max_seq == 43`, while a gap fails.

- [ ] **Step 2: Run focused tests to prove RED**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --test test_oplog \
    test_etcd_append_path_builds_contiguous_buffer_without_flushing -- --exact
  ```

  Expected: FAIL because the shared buffer-assignment helper does not exist.

- [ ] **Step 3: Extract buffer assignment and remove eager etcd flush**

  Extract one pure function that checks `u64` overflow, increments from `last_seq`, copies each record with its assigned sequence, and returns the new buffer. Use it in `EtcdOpLogStore::append`; retain `ensure_not_poisoned`, `writer_fence`, `last_seq`, and `buffer.push`, but remove `block_on_runtime(self.flush())`. Do not change `flush_unpoisoned`: it must still compare previous `/latest`, require absent entry keys, compare the election revision, put every buffered entry plus `/latest`, poison on any failure, and clear the buffer only on success.

- [ ] **Step 4: Reuse a persistent current-thread runtime on the worker OS thread**

  Change the no-current-runtime branch of `block_on_runtime` to use a thread-local `RefCell<tokio::runtime::Runtime>` built with `Builder::new_current_thread().enable_all()`. Keep the existing Tokio multi-thread branch using `block_in_place(|| handle.block_on(future))`. Add an internal test that invokes `block_on_runtime` twice from the same plain OS thread and verifies a thread-local marker is retained, proving runtime construction is not repeated per flush.

- [ ] **Step 5: Verify backend GREEN and commit**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --lib oplog::oplog_etcd::tests -- --nocapture
  cargo test -p mooncake-store-master --test test_oplog -- --nocapture
  rustfmt --edition 2024 --check crates/mooncake-store-master/src/oplog/oplog_etcd.rs \
    crates/mooncake-store-master/src/oplog/test_support.rs \
    crates/mooncake-store-master/tests/test_oplog.rs
  cd ..
  git diff --check
  git add rust-repo/crates/mooncake-store-master/src/oplog/oplog_etcd.rs \
    rust-repo/crates/mooncake-store-master/src/oplog/test_support.rs \
    rust-repo/crates/mooncake-store-master/tests/test_oplog.rs
  git diff --cached --check
  git commit -m '[Store] batch fenced etcd oplog commits'
  ```

  Expected: all oplog backend/wire tests PASS and no unrelated dirty files are staged.

### Task 3: Thread-safe `OpLogManager` facade and query protocol

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog/oplog_worker.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog/oplog_manager.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_oplog.rs`

**Interfaces:**
- Consumes: `SequencedOpLogWorker` from Task 1.
- Produces: all existing record methods on `OpLogManager` with `&self`; `read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError>`; `max_sequence_id(&self) -> Result<u64, HaError>`; `set_initial_sequence_id(&self, sequence_id: u64) -> Result<(), HaError>`; `cleanup_before(&self, before_sequence_id: u64) -> Result<(), HaError>`; `record_snapshot_sequence_id(&self, snapshot_id: &str, sequence_id: u64) -> Result<(), HaError>`; `get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError>`; `replace_with(&self, replacement: OpLogManager) -> Result<(), HaError>`; `set_view_version(&self, version: u64)`; `latest_sequence(&self) -> u64` returning committed, never merely assigned, sequence.
- The manager contains `parking_lot::RwLock<Option<Arc<SequencedOpLogWorker>>>` and `AtomicU64` producer view. The read lock is held only long enough to clone the worker `Arc`, never while waiting for persistence or a query.

- [ ] **Step 1: Convert manager tests to the new read interface and prove RED**

  In `tests/test_oplog.rs`, replace both `into_store()` assertions with `manager.read_since(1, 10)`. Add `manager_latest_sequence_moves_only_after_durable_flush`: gate the backend flush, submit from a thread, assert `latest_sequence() == 0` while gated, release, then assert the call returns sequence 1 and `latest_sequence() == 1`. In `oplog_applier.rs` tests, replace every `manager.store().unwrap().read_since(...)` with `manager.read_since(...)` and keep all exact payload assertions.

- [ ] **Step 2: Run focused tests to prove RED**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --test test_oplog \
    manager_latest_sequence_moves_only_after_durable_flush -- --exact --nocapture
  ```

  Expected: FAIL because the manager still owns a mutable store directly and exposes no worker-backed query API.

- [ ] **Step 3: Add query/admin commands to the worker**

  Add command variants and bounded reply waits for `ReadSince`, `MaxSequenceId`, `UpdateLatestSequenceId`, `RecordSnapshotSequenceId`, `GetSnapshotSequenceId`, and `CleanupBefore`. Before executing an admin mutation or read that requires committed state, flush an already formed persist batch; never let an admin command overtake an earlier persist. Any backend error, including an admin read timeout or malformed read response, installs the same terminal poison before it is returned; this keeps the facade fail-closed and prevents a process with uncertain durable state from continuing to mutate.

- [ ] **Step 4: Replace direct manager ownership with the facade**

  In `OpLogManager::new`, start a worker when `store` is `Some` and retain `None` for non-HA. Change `view_version` to `AtomicU64`; load it once while building each command. Route every existing durable method through `submit_durable`, every legacy best-effort record method through `submit_buffered`, and every store query through the matching command. Delete `store()` and `into_store()`. Implement `replace_with` under the documented precondition that the service gate is closed: clone and bounded-shut down the old worker without holding the manager lock, then take the replacement worker, swap it under the short write lock, and publish the replacement view. The stopped old worker remains visible during shutdown but rejects new commands, so no old/new writer overlap or successful sequence-0 no-op window exists. Implement `Drop` so the last manager owner requests bounded shutdown without an unbounded join.

- [ ] **Step 5: Add replacement, ordering, and runtime regressions**

  Add:

  - `replacement_stops_old_worker_before_new_view_accepts_records`: record view 7, replace with view 8, record again, and assert shared old-backend counters stop changing before the new store receives its first view-8 record.
  - `current_thread_runtime_can_wait_for_dedicated_worker`: a `#[tokio::test(flavor = "current_thread")]` calls `record_remove_durable` and completes within the injected deadline.
  - `queries_cannot_overtake_an_earlier_durable_record`: enqueue a record, immediately query from another thread, and assert the query includes the committed record.

- [ ] **Step 6: Verify facade GREEN and commit**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --lib oplog::oplog_manager::tests -- --nocapture
  cargo test -p mooncake-store-master --lib ha::oplog_applier::tests -- --nocapture
  cargo test -p mooncake-store-master --test test_oplog -- --nocapture
  rustfmt --edition 2024 --check crates/mooncake-store-master/src/oplog/oplog_worker.rs \
    crates/mooncake-store-master/src/oplog/oplog_manager.rs \
    crates/mooncake-store-master/src/ha/oplog_applier.rs \
    crates/mooncake-store-master/tests/test_oplog.rs
  cd ..
  git diff --check
  git add rust-repo/crates/mooncake-store-master/src/oplog/oplog_worker.rs \
    rust-repo/crates/mooncake-store-master/src/oplog/oplog_manager.rs \
    rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs \
    rust-repo/crates/mooncake-store-master/tests/test_oplog.rs
  git diff --cached --check
  git commit -m '[Store] expose lock-free oplog manager facade'
  ```

  Expected: manager, replay, and wire tests PASS with no public access to the mutable writer store.

### Task 4: Remove the service-wide oplog mutex without weakening key ordering

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/state.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/helpers.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/drain.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/segment.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops/background_drain.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/main_ha.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_object.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_service_cluster_parity_p1_p2.rs`
- Preserve and extend: `rust-repo/crates/mooncake-store-master/src/service/background_ops.rs`

**Interfaces:**
- Consumes: thread-safe `OpLogManager` from Task 3.
- Produces: `MasterState.oplog_manager: Arc<OpLogManager>`, `MasterServiceImpl.oplog_manager: Arc<OpLogManager>`, `MasterServiceImpl::oplog_manager(&self) -> &OpLogManager`, and `MasterServiceImpl::replace_oplog_manager(&self, replacement: OpLogManager) -> Result<(), HaError>`.
- Snapshot capture reads `latest_sequence()` twice from the committed atomic boundary; promotion uses `replace_oplog_manager` and `set_view_version` without a surrounding mutex.

- [ ] **Step 1: Strengthen the existing eviction regression to prove unrelated-key progress**

  In the already modified `service/background_ops.rs`, retain the ungrouped per-key victim guard and grouped snapshot barrier behavior. Extend `ungrouped_eviction_durable_flush_does_not_hold_global_snapshot_barrier` so a gateable first durable flush starts on one key, an unrelated `PutStart` is launched on a second key under a Tokio multi-thread runtime with two workers, and a heartbeat future completes within 200 ms while both durable callers are waiting. Release the backend, then assert both mutations complete, sequences are consecutive, and the counting backend reports one or two batches but no globally serialized manager lock wait.

- [ ] **Step 2: Run the service concurrency test to prove RED**

  Run with the native environment:

  ```bash
  cd rust-repo
  RUSTFLAGS='-L native=/tmp/mooncake-native-libs' \
  LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src' \
  cargo test -p mooncake-store-master --features link-native --lib \
    service::background_ops::tests::ungrouped_eviction_durable_flush_does_not_hold_global_snapshot_barrier \
    -- --exact --nocapture
  ```

  Expected: FAIL or time out under the existing `Arc<parking_lot::Mutex<OpLogManager>>` because the unrelated request blocks a runtime worker on the global mutex.

- [ ] **Step 3: Change service ownership and every production call site**

  Construct one `Arc<OpLogManager>` in `try_new_with_runtime_config_and_oplog_with_availability`, clone it into service/state, and remove only the surrounding `parking_lot::Mutex`. Replace all production `oplog_manager.lock().method(...)` and multiline equivalents in the files above with direct facade calls. Do not shorten the lifetime of object/key guards, allocator guards, background mutation gates, or foreground request gates. Keep every existing `fence_after_durability_failure`/error return exactly on the same error path.

- [ ] **Step 4: Migrate snapshots, promotion, and tests**

  Read committed snapshot boundaries directly at the three current snapshot sites in `service/mod.rs`. In `main_ha.rs`, replace `*service_arc.oplog_manager().lock() = manager` with `service_arc.replace_oplog_manager(manager)` and fail leadership acquisition if replacement fails; set the acquired producer view through the facade. Convert test queries from `.oplog_manager().lock().latest_sequence()` to `.oplog_manager().latest_sequence()` and from locked `.store().read_since` to `.oplog_manager().read_since`.

- [ ] **Step 5: Prove no global oplog lock remains**

  Run:

  ```bash
  rg -n -U 'oplog_manager\s*\.lock\(\)' \
    rust-repo/crates/mooncake-store-master/src \
    rust-repo/crates/mooncake-store-master/tests
  rg -n 'Mutex<crate::oplog::OpLogManager>|Mutex<OpLogManager>' \
    rust-repo/crates/mooncake-store-master/src \
    rust-repo/crates/mooncake-store-master/tests
  ```

  Expected: both commands return no matches. Matches inside comments or obsolete test fixtures must be removed rather than waived.

- [ ] **Step 6: Run focused service, snapshot, and promotion suites**

  Run:

  ```bash
  cd rust-repo
  export RUSTFLAGS='-L native=/tmp/mooncake-native-libs'
  export LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src'
  cargo test -p mooncake-store-master --features link-native --lib \
    service::background_ops::tests::ungrouped_eviction_durable_flush_does_not_hold_global_snapshot_barrier \
    -- --exact --nocapture
  cargo test -p mooncake-store-master --features link-native --test test_master_object -- --nocapture
  cargo test -p mooncake-store-master --features link-native --test test_master_mount -- --nocapture
  cargo test -p mooncake-store-master --features link-native \
    --test test_service_cluster_parity_p1_p2 -- --nocapture
  cargo test -p mooncake-store-master --features link-native --lib hot_standby::tests -- --nocapture
  ```

  Expected: all suites PASS; the concurrency test heartbeat completes before the backend gate opens.

- [ ] **Step 7: Commit the global-lock removal while preserving other Task 5 edits**

  Use `git diff -- <file>` on every file before staging. Stage all Task 4 files except unrelated hunks in `service/background_ops.rs`; stage only the eviction locking regression/fix plus worker migration hunks there with `git add -p`. Then run `git diff --cached --check` and commit:

  ```bash
  git commit -m '[Store] remove global oplog persistence lock'
  ```

  Expected: the commit contains the worker facade migration and evidence-backed eviction guard fix, but not the live chaos scenario, CLI configuration, Cargo dependency, or raw tonic timeout fixture.

### Task 5: Preserve bounded etcd defense and verify service fencing

**Files:**
- Preserve and modify as needed: `rust-repo/crates/mooncake-store-master/src/main.rs`
- Preserve: `rust-repo/crates/mooncake-store-master/Cargo.toml`
- Preserve: `rust-repo/Cargo.lock`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs`

**Interfaces:**
- Consumes: existing 10-second `ConnectOptions::with_timeout` and bounded connect timeout in `build_leader_oplog_manager`; the raw tonic fixture and `tokio-stream` dev dependency already present in the dirty baseline.
- Produces: proof that a backend request timeout occurs independently of Master request-worker availability and that any durable ambiguity fences the service before another mutation.

- [ ] **Step 1: Retain and run the raw tonic timeout regression**

  Identify the exact test name with `rg -n 'timeout|tonic' rust-repo/crates/mooncake-store-master/src/main.rs`, then run that one test with `cargo test -p mooncake-store-master --bin mooncake-master <exact-test-name> -- --exact --nocapture`. Expected: PASS in well below the configured request deadline using the test's shorter injected timeout.

- [ ] **Step 2: Add a fail-closed service regression**

  In `test_master_mount.rs`, use `FailingFlushOpLog` with the worker facade. Perform a mount whose in-memory mutation reaches durable persistence, inject an ambiguous flush failure, assert the RPC fails and `service_available()` is false/service is fenced, then attempt an unrelated mount and assert it is rejected before the backend append/flush counters change.

- [ ] **Step 3: Run the fencing test to prove RED then GREEN**

  First run the exact new test before adapting the fixture and expect a compile/assertion failure. Make only the minimal fixture/facade changes, then rerun with the native environment from Task 4. Expected GREEN: the second mutation performs no backend call.

- [ ] **Step 4: Verify and commit the defensive timeout separately**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --bin mooncake-master -- --nocapture
  RUSTFLAGS='-L native=/tmp/mooncake-native-libs' \
  LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src' \
  cargo test -p mooncake-store-master --features link-native --test test_master_mount -- --nocapture
  cd ..
  git diff --check
  git add rust-repo/crates/mooncake-store-master/src/main.rs \
    rust-repo/crates/mooncake-store-master/Cargo.toml rust-repo/Cargo.lock \
    rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs
  git diff --cached --check
  git commit -m '[Store] bound leader oplog transport stalls'
  ```

  Expected: raw timeout and service-fencing regressions PASS; the staged dependency change is only the test fixture's `tokio-stream` net feature/dependency resolution.

### Task 6: Canonical correctness gate and retained large-object scenario

**Files:**
- Preserve: `rust-repo/crates/mooncake-store-master/src/main_args.rs`
- Preserve: `rust-repo/crates/mooncake-store-master/src/main_config.rs`
- Preserve: `rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs`
- Read/execute: `rust-repo/tools/store-validation/run-ha-chaos-live.sh`
- Update: `.superpowers/sdd/2026-07-28-rust-store-three-master-ha-chaos/progress.md`

**Interfaces:**
- Consumes: 5 ms eviction interval configuration and the canonical live scenario already present in the dirty baseline.
- Produces: one retained result JSON and logs proving three Masters, four large-object crash/restart rounds, all victims, pressure evidence, at least 168 stable exact reads, and no stale Master/etcd process.

- [ ] **Step 1: Run all focused correctness gates before live execution**

  Run:

  ```bash
  cd rust-repo
  export RUSTFLAGS='-L native=/tmp/mooncake-native-libs'
  export LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src'
  cargo test -p mooncake-store-master --features link-native --lib -- --nocapture
  cargo test -p mooncake-store-master --features link-native --test test_oplog -- --nocapture
  cargo test -p mooncake-store-master --features link-native --test test_master_object -- --nocapture
  cargo test -p mooncake-store-master --features link-native --test test_master_mount -- --nocapture
  cargo test -p mooncake-store-master --features link-native \
    --test test_service_cluster_parity_p1_p2 -- --nocapture
  cargo test -p mooncake-store-client --features link-native --test test_ha_chaos_live \
    large_profile_exceeds_capacity_and_values_are_key_distinct -- --exact --nocapture
  ```

  Expected: every command PASS. Do not launch the expensive live gate if any focused test fails.

- [ ] **Step 2: Execute the canonical large-object gate exactly once**

  Run from the worktree root:

  ```bash
  export RUSTFLAGS='-L native=/tmp/mooncake-native-libs'
  export LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src'
  export MOONCAKE_HA_SCENARIOS=large
  export MOONCAKE_HA_SEED=0x4d4f4f4e48414348
  export MOONCAKE_HA_RUN_ID=oplog-group-commit-green
  export MOONCAKE_HA_ARTIFACT_ROOT=/tmp/mooncake-ha-chaos-large-oplog-group-commit-green
  export MOONCAKE_HA_TEST_TIMEOUT_SECONDS=900
  bash rust-repo/tools/store-validation/run-ha-chaos-live.sh
  ```

  Expected: exit 0 and result status PASS. If it fails, retain the artifact directory, record the first failing stage/key/client/round in the progress ledger, add one deterministic focused RED regression, and do not stack a second architectural hypothesis before that regression is green.

- [ ] **Step 3: Validate evidence and cleanup explicitly**

  Run:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python - <<'PY'
  import json
  from pathlib import Path
  path = Path('/tmp/mooncake-ha-chaos-large-oplog-group-commit-green/ha-chaos-result.json')
  data = json.loads(path.read_text())
  large = data['scenarios']['large']
  evidence = large['evidence']
  assert data['status'] == 'PASS'
  assert len(data['masters']) == 3
  assert large['status'] == 'PASS'
  assert evidence['stable_exact_reads'] >= 168
  assert evidence['crashes'] >= 4
  assert evidence['restarts'] >= 4
  assert evidence['eviction_requests'] + evidence['capacity_rejections'] > 0
  assert evidence['successful_unstable_operations'] > 0
  assert evidence['byte_comparisons'] >= evidence['stable_exact_reads']
  assert set(evidence['stopped_indices']) == {0, 1, 2}
  assert set(evidence['restarted_indices']) == {0, 1, 2}
  print(json.dumps(evidence, sort_keys=True, indent=2))
  PY
  pgrep -af 'mooncake-master|mc-store-ha-chaos-oplog-group-commit-green' && exit 1 || true
  ```

- [ ] **Step 4: Commit retained CLI and live-scenario changes as lean commits**

  First stage only `main_args.rs` and `main_config.rs`, review, and commit:

  ```bash
  git add rust-repo/crates/mooncake-store-master/src/main_args.rs \
    rust-repo/crates/mooncake-store-master/src/main_config.rs
  git diff --cached --check
  git commit -m '[Store] configure fast HA eviction cadence'
  ```

  Then stage only the live scenario, review, and commit:

  ```bash
  git add rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs
  git diff --cached --check
  git commit -m '[Store] validate eviction-prone HA crash recovery'
  ```

  Expected: the worktree contains no remaining Task 5 source changes; artifacts stay under `/tmp` and are never committed.

### Task 7: Reproducible group-commit performance measurement after correctness

**Files:**
- Create: `rust-repo/crates/mooncake-store-master/examples/oplog_group_commit_bench.rs`
- Modify only if required to expose the documented constructor: `rust-repo/crates/mooncake-store-master/src/oplog.rs`

**Interfaces:**
- Consumes: public `OpLogManager::new`, `OpLogStore`, `OpLogRecord`, and the production 10 ms/100-record defaults.
- Produces: a CLI example accepting `--threads`, `--ops-per-thread`, and `--flush-latency-ms`; it prints one JSON object per mode with `mode`, `threads`, `operations`, `elapsed_ms`, `throughput_ops_s`, `p50_us`, `p99_us`, `flushes`, and `mean_records_per_flush`.

- [ ] **Step 1: Write the benchmark with a correctness oracle**

  Implement a `DelayCountingStore` backed by `InMemoryOpLog`; `flush_durable` sleeps the requested duration and increments a shared counter. Mode `sync_baseline` uses `Arc<Mutex<DelayCountingStore>>` and performs append+flush per operation. Mode `group_commit` uses `Arc<OpLogManager>` and `record_remove_durable` from all threads. Before printing results, assert each mode completed exactly `threads * ops_per_thread` successful operations, sequence IDs are contiguous, and the manager committed boundary equals the operation count.

- [ ] **Step 2: Run a smoke benchmark only after Task 6 is GREEN**

  Run:

  ```bash
  cd rust-repo
  cargo run -p mooncake-store-master --example oplog_group_commit_bench --release -- \
    --threads 8 --ops-per-thread 20 --flush-latency-ms 2
  ```

  Expected: two valid JSON lines; `sync_baseline.flushes == 160`, `group_commit.flushes < 160`, and both modes report 160 successful operations.

- [ ] **Step 3: Record the canonical measurement**

  Run three times and retain stdout under `/tmp/mooncake-oplog-group-commit-bench`:

  ```bash
  mkdir -p /tmp/mooncake-oplog-group-commit-bench
  cd rust-repo
  for run in 1 2 3; do
    cargo run -p mooncake-store-master --example oplog_group_commit_bench --release -- \
      --threads 32 --ops-per-thread 100 --flush-latency-ms 5 \
      > "/tmp/mooncake-oplog-group-commit-bench/run-${run}.jsonl"
  done
  ```

  Expected: every group-commit run has fewer flushes and higher mean records per flush than the synchronous baseline. Treat throughput/latency as measured evidence, not a hard CI threshold.

- [ ] **Step 4: Verify, pre-commit, and commit benchmark**

  Run:

  ```bash
  cd rust-repo
  rustfmt --edition 2024 --check crates/mooncake-store-master/examples/oplog_group_commit_bench.rs
  cargo check -p mooncake-store-master --example oplog_group_commit_bench
  cd ..
  SKIP=mooncake-code-format,codespell /home/fy2462/Mooncake/.venv/bin/pre-commit run --files \
    rust-repo/crates/mooncake-store-master/examples/oplog_group_commit_bench.rs
  git diff --check
  git add rust-repo/crates/mooncake-store-master/examples/oplog_group_commit_bench.rs
  git diff --cached --check
  git commit -m '[Store] benchmark oplog group commit'
  ```

  Expected: format/check/pre-commit PASS and only the benchmark is staged.

### Task 8: Full regression, diff audit, and handoff to broader performance work

**Files:**
- Review: every file changed since `a1b4be43`
- Update: `.superpowers/sdd/2026-07-28-rust-store-three-master-ha-chaos/progress.md`

**Interfaces:**
- Consumes: all Task 1-7 commits and retained artifact evidence.
- Produces: a clean correctness handoff with exact test commands/results, live artifact path, benchmark path, remaining risks, and the next gate for TENT, accelerator DLPack, SHM hot cache, and io_uring benchmarks.

- [ ] **Step 1: Run the full scoped verification**

  Run:

  ```bash
  cd rust-repo
  export RUSTFLAGS='-L native=/tmp/mooncake-native-libs'
  export LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src'
  cargo test -p mooncake-store-master --features link-native --lib
  cargo test -p mooncake-store-master --features link-native --tests
  cargo test -p mooncake-store-client --features link-native --test test_ha_chaos_live \
    large_profile_exceeds_capacity_and_values_are_key_distinct -- --exact
  cargo check -p mooncake-store-master --example oplog_group_commit_bench
  ```

  Expected: all commands PASS.

- [ ] **Step 2: Run scoped pre-commit and static lock checks**

  Run from the worktree root:

  ```bash
  git diff --diff-filter=ACMR --name-only -z a1b4be43..HEAD | \
    xargs -0 env SKIP=mooncake-code-format,codespell \
    /home/fy2462/Mooncake/.venv/bin/pre-commit run --files
  git diff --check
  rg -n -U 'oplog_manager\s*\.lock\(\)' \
    rust-repo/crates/mooncake-store-master/src \
    rust-repo/crates/mooncake-store-master/tests
  rg -n 'Mutex<crate::oplog::OpLogManager>|Mutex<OpLogManager>' \
    rust-repo/crates/mooncake-store-master/src \
    rust-repo/crates/mooncake-store-master/tests
  ```

  Expected: pre-commit and diff checks PASS; both `rg` commands return no matches.

- [ ] **Step 3: Audit history and worktree scope**

  Run:

  ```bash
  git log --oneline --decorate a1b4be43..HEAD
  git diff --stat a1b4be43..HEAD
  git diff --name-status a1b4be43..HEAD
  git status --short
  ```

  Expected: lean commits in task order, no artifacts or `.venv` files, and no unexplained dirty source changes.

- [ ] **Step 4: Update the progress ledger with evidence**

  Record the focused/full test results, `/tmp/mooncake-ha-chaos-large-oplog-group-commit-green`, `/tmp/mooncake-oplog-group-commit-bench`, exact commit IDs, and whether any benchmark tuning was intentionally deferred. Mark original HA Task 5 complete only if the canonical result is PASS and cleanup checks are clean.

- [ ] **Step 5: Request two-stage review before claiming completion**

  Use the subagent-driven workflow's specification-compliance reviewer first and code-quality reviewer second. Resolve every blocking finding with a focused regression and rerun the affected verification. Do not claim the broader Mooncake audit/performance goal complete: hand off explicitly to the already planned TENT, accelerator DLPack, SHM hot cache, and io_uring reproducible benchmark phases after this correctness gate.
