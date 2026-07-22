# Promotion Retry Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded, observable background retry for Rust Store promotion-on-hit requests rejected by memory watermark, queue capacity, or transient queue insertion failure.

**Architecture:** Keep transient candidate state in `MasterState` as a non-persistent `DashMap`, refactor promotion admission to return a typed outcome, and let the existing eviction worker evaluate a bounded fair slice every tick. Preserve public RPC behavior while matching the merged C++ retry limits, backoff, expiry rules, and metric names.

**Tech Stack:** Rust 2024, DashMap, parking_lot, Prometheus, Tokio integration tests, Cargo.

## Global Constraints

- Do not compile, link, or call `libmooncake_store.so` or any C++ Store source.
- Use the C++ Store only as a behavioral compatibility oracle.
- Candidate state is transient and must not be serialized into snapshots or oplogs.
- Use a 50,000 candidate cap, 60-second TTL, 8 evaluated retries, 10-ms initial backoff, 1-second maximum backoff, 64 scan partitions per tick, and 128 evaluations per tick.
- Preserve tenant-scoped keys already produced by `make_tenant_scoped_key`.
- Run Cargo commands with `CARGO_BUILD_JOBS=5` and put `CARGO_TARGET_DIR` under `/home/fy2462/workspace/tmp/mooncake`.
- Run every Cargo and formatting command from `/home/fy2462/Mooncake/rust-repo`.

---

### Task 1: Model transient promotion candidate state

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/state.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs`

**Interfaces:**
- Produces: `PromotionCandidateReason`, `PromotionCandidate`, and the `MasterState` fields `promotion_candidates`, `promotion_candidate_count`, and `promotion_retry_cursor`.
- Consumes: existing tenant-scoped object keys and `StoreError` values.

- [ ] **Step 1: Add the failing state-construction test**

Create `tests/test_promotion_retry.rs` with a service configured for promotion and assert that the new hidden helper starts at zero:

```rust
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};

#[test]
fn promotion_candidate_state_starts_empty() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        ..Default::default()
    });
    assert_eq!(service.promotion_candidate_count_for_test(), 0);
}
```

- [ ] **Step 2: Run the focused test and verify the API is absent**

Run:

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_promotion_retry \
  promotion_candidate_state_starts_empty --no-run
```

Expected: compilation fails because `promotion_candidate_count_for_test` does not exist.

- [ ] **Step 3: Add the candidate model and initialize every `MasterState` constructor**

Add to `service/state.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromotionCandidateReason {
    Watermark,
    QueueCap,
    PushFailed,
}

#[derive(Debug, Clone)]
pub(crate) struct PromotionCandidate {
    pub(crate) sketch_score: u8,
    pub(crate) first_seen: Instant,
    pub(crate) last_seen: Instant,
    pub(crate) retry_after: Instant,
    pub(crate) last_reason: PromotionCandidateReason,
    pub(crate) last_error_code: Option<i32>,
    pub(crate) retry_count: u32,
}
```

Add these fields to `MasterState` and initialize them in `MasterState::empty`,
`MasterServiceImpl::new_with_runtime_config...`, and the HA test constructor:

```rust
pub(crate) promotion_candidates: DashMap<String, PromotionCandidate>,
pub(crate) promotion_candidate_count: AtomicUsize,
pub(crate) promotion_retry_cursor: AtomicUsize,
```

Expose a hidden test helper in `service/mod.rs`:

```rust
#[doc(hidden)]
pub fn promotion_candidate_count_for_test(&self) -> usize {
    self.state
        .promotion_candidate_count
        .load(std::sync::atomic::Ordering::Relaxed)
}
```

- [ ] **Step 4: Run the focused test and format check**

Run the command from Step 2 without `--no-run`, then:

```bash
cargo fmt --all -- --check
```

Expected: one passing test and clean formatting.

- [ ] **Step 5: Commit the state model**

```bash
git add rust-repo/crates/mooncake-store-master/src/service/state.rs \
  rust-repo/crates/mooncake-store-master/src/service/mod.rs \
  rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs \
  rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs
git commit -m "[Store] model Rust promotion retry candidates"
```

### Task 2: Return typed promotion admission outcomes and record candidates

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs`

**Interfaces:**
- Consumes: `PromotionCandidate` and `PromotionCandidateReason` from Task 1.
- Produces: `PromotionQueueResult`, `try_push_promotion_queue(state, key, record_candidate)`, candidate insertion, refresh, removal, and exact global-cap accounting.

- [ ] **Step 1: Add failing watermark and queue-cap candidate tests**

Build a complete local-disk replica with the existing mount/notify RPC helpers.
For watermark rejection, configure `eviction_high_watermark_ratio: 0.0`; for
queue-cap rejection, configure `promotion_queue_limit: 0`. Trigger
`get_replica_list` and assert:

```rust
assert_eq!(service.promotion_candidate_count_for_test(), 1);
```

Trigger the same lookup again and assert the count remains one, proving keyed
deduplication.

- [ ] **Step 2: Run both tests and verify they fail with zero candidates**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_promotion_retry \
  promotion_rejection_records_candidate -- --nocapture
```

Expected: assertions report `left: 0, right: 1`.

- [ ] **Step 3: Refactor admission into typed outcomes**

Add to `background_ops.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromotionQueueResult {
    Queued,
    Disabled,
    FrequencyRejected,
    WatermarkRejected,
    QueueCapRejected,
    AlreadyInFlight,
    MemoryReplicaPresent,
    NoLocalDiskSource,
    NotFound,
    PushFailed,
}

impl PromotionQueueResult {
    fn is_transient(self) -> bool {
        matches!(
            self,
            Self::WatermarkRejected | Self::QueueCapRejected | Self::PushFailed
        )
    }
}
```

Change the signature to:

```rust
pub(crate) fn try_push_promotion_queue(
    state: &MasterState,
    key: &str,
    record_candidate: bool,
) -> PromotionQueueResult
```

Map every current early return to the matching enum value. Callers in
`grpc_objects_query.rs` and `grpc_batches.rs` pass `true` and ignore the
internal result.

- [ ] **Step 4: Implement cap-safe candidate bookkeeping**

Add `record_or_refresh_candidate`, `erase_promotion_candidate`, and
`decrement_candidate_count`. Reserve the global slot with an atomic
compare-exchange before insertion; if another thread inserted the same key,
undo the reservation. Existing entries update in place without incrementing
the count. Watermark, queue-cap, and push failures record only when
`record_candidate` is true. Queued and permanently ineligible outcomes erase
an existing candidate. Define `const PROMOTION_CANDIDATE_LIMIT: usize = 50_000`
beside these bookkeeping functions and use it for the reservation loop.

Expose this helper for cap tests:

```rust
#[doc(hidden)]
pub fn has_promotion_candidate_for_test(&self, key: &str, tenant_id: &str) -> bool {
    self.state
        .promotion_candidates
        .contains_key(&make_tenant_scoped_key(tenant_id, key))
}
```

- [ ] **Step 5: Run focused and existing promotion parity tests**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_promotion_retry
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_service_cluster_parity_p1_p2 promotion
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit typed admission and recording**

```bash
git add rust-repo/crates/mooncake-store-master/src/service/background_ops.rs \
  rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs \
  rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs \
  rust-repo/crates/mooncake-store-master/src/service/mod.rs \
  rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs
git commit -m "[Store] record transient Rust promotion rejections"
```

### Task 3: Add bounded retry scheduling and worker integration

**Files:**
- Create: `rust-repo/crates/mooncake-store-master/src/service/background_ops/promotion_retry.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/workers.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_master_eviction_offload.rs`

**Interfaces:**
- Consumes: typed admission and candidate bookkeeping from Task 2.
- Produces: `run_promotion_candidate_retry`, backoff/expiry processing, fair partition cursor, and deterministic test helpers.

- [ ] **Step 1: Add failing recovery, expiry, and retry-limit tests**

Add tests that:

- fill a mounted memory segment above a 50% watermark, record a rejected
  local-disk candidate, remove the temporary memory object to lower usage,
  reset `retry_after`, run one retry pass, and observe one heartbeat task;
- set a candidate's last-seen time beyond 60 seconds and assert retry removes it;
- force 8 transient evaluations and assert the candidate is removed;
- record two candidates for different tenants with the same user key and assert
  they remain independent.

Use hidden deterministic helpers rather than sleeping:

```rust
service.make_promotion_candidates_due_for_test();
service.age_promotion_candidates_for_test(Duration::from_secs(61));
service.set_promotion_candidate_retry_count_for_test(key, tenant_id, 7);
let queued = service.run_promotion_candidate_retry_for_test();
```

- [ ] **Step 2: Run the new tests and verify missing helpers fail compilation**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_promotion_retry --no-run
```

Expected: the deterministic retry helper methods are absent.

- [ ] **Step 3: Implement the retry module**

Define the exact limits in `promotion_retry.rs`:

```rust
const MAX_RETRIES: u32 = 8;
const RETRY_BATCH_SIZE: usize = 128;
const PARTITION_COUNT: usize = 256;
const PARTITIONS_PER_TICK: usize = 64;
const CANDIDATE_TTL: Duration = Duration::from_secs(60);
const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const MAX_BACKOFF: Duration = Duration::from_secs(1);
```

Implement a deterministic `DefaultHasher::new()` partition function. Each pass
advances `promotion_retry_cursor` by 64 partitions, collects at most 128 due
keys without holding a map guard across admission, then evaluates each key.
Queued candidates increment the return count and are erased. Transient results
advance retry count and backoff. Permanent results erase the candidate.

Export from `background_ops.rs`:

```rust
pub(crate) use promotion_retry::{
    make_promotion_candidates_due_for_test, run_promotion_candidate_retry,
};
```

- [ ] **Step 4: Run retry from the existing eviction worker**

Change the timeout branch in `EvictionWorker` to:

```rust
let _ = run_automatic_eviction_once(&state);
let _ = run_promotion_candidate_retry(&state);
```

Expose service test helpers that call the module with all 256 partitions for a
deterministic full pass. Add the three mutation helpers shown in Step 1; each
must only alter candidate timestamps/retry counts and must not change object or
promotion-task state. Keep the production pass bounded to 64 partitions.

- [ ] **Step 5: Run focused tests and the eviction worker regression**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_promotion_retry
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_master_eviction_offload
```

Expected: all tests pass without wall-clock sleeps in promotion retry tests.

- [ ] **Step 6: Commit scheduler integration**

```bash
git add rust-repo/crates/mooncake-store-master/src/service/background_ops.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/promotion_retry.rs \
  rust-repo/crates/mooncake-store-master/src/service/workers.rs \
  rust-repo/crates/mooncake-store-master/src/service/mod.rs \
  rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs \
  rust-repo/crates/mooncake-store-master/tests/test_master_eviction_offload.rs
git commit -m "[Store] retry rejected Rust promotions in background"
```

### Task 4: Add compatible Prometheus metrics

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops/promotion_retry.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_master_metrics.rs`

**Interfaces:**
- Consumes: candidate lifecycle events from Tasks 2 and 3.
- Produces: six C++-compatible Prometheus counters registered by `register_metrics`.

- [ ] **Step 1: Add a failing metric-name and increment test**

Under `METRICS_TEST_LOCK`, capture all six counter baselines, trigger candidate
recording and successful retry, and assert the recorded/admitted counters each
increase by one. Gather Prometheus text and assert it contains:

```text
master_promotion_candidate_recorded_total
master_promotion_candidate_admitted_total
master_promotion_candidate_admission_rejected_total
master_promotion_candidate_expired_evaluated_total
master_promotion_candidate_expired_unevaluated_total
master_promotion_candidate_dropped_limit_total
```

- [ ] **Step 2: Run the focused metric test and verify missing symbols fail**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_master_metrics promotion_candidate --no-run
```

Expected: compilation fails on absent metric statics.

- [ ] **Step 3: Define and register all six counters**

Use `IntCounter::new` with the exact metric names from the design. Register all
six in `register_metrics()`. Increment counters only at these transitions:

- new map entry: recorded;
- successful retry admission: admitted;
- transient retry result: admission rejected;
- evaluated candidate removed by retry count or TTL: expired evaluated;
- never-evaluated candidate removed by TTL: expired unevaluated;
- new candidate refused by the 50,000 cap: dropped limit.

- [ ] **Step 4: Run metric and promotion suites**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_master_metrics
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_promotion_retry
```

Expected: both suites pass.

- [ ] **Step 5: Commit metrics**

```bash
git add rust-repo/crates/mooncake-store-master/src/metrics.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/promotion_retry.rs \
  rust-repo/crates/mooncake-store-master/tests/test_master_metrics.rs
git commit -m "[Store] expose Rust promotion retry metrics"
```

### Task 5: Verify transient-state reset and promotion parity

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs`
- Modify: `rust-repo/docs/superpowers/specs/2026-07-23-up-main-parity-design.md`

**Interfaces:**
- Consumes: completed promotion retry implementation.
- Produces: restore/reset evidence and documented completion of the first parity slice.

- [ ] **Step 1: Add a failing HA reset test**

Construct standby state through the existing `OpLogApplier` test helper, insert
a candidate, apply a snapshot reset/bootstrap path, and assert candidates,
count, and cursor are zero. The test must also assert promotion tasks retain
their existing documented restore behavior.

- [ ] **Step 2: Implement a single transient reset helper**

Add to `MasterState`:

```rust
pub(crate) fn clear_transient_promotion_candidates(&self) {
    self.promotion_candidates.clear();
    self.promotion_candidate_count.store(0, Ordering::Relaxed);
    self.promotion_retry_cursor.store(0, Ordering::Relaxed);
}
```

Call it from standby bootstrap/snapshot replacement paths rather than adding
candidate data to snapshot or oplog schemas.

- [ ] **Step 3: Run all master tests and static checks**

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master
cargo fmt --all -- --check
cargo clippy -p mooncake-store-master --all-targets -- -D warnings
```

Expected: zero test failures, no formatting diff, and no Clippy warnings.

- [ ] **Step 4: Verify the C++ Store dependency boundary**

```bash
rg -n 'mooncake_store|libmooncake_store|mooncake-store/src' \
  rust-repo/crates/mooncake-store-master/Cargo.toml \
  rust-repo/crates/mooncake-store-master/build.rs \
  rust-repo/Cargo.toml
```

Expected: no C++ Store source or library dependency. Rust crate/package names
containing `mooncake-store-master` are allowed.

- [ ] **Step 5: Mark the promotion slice complete in the design document**

Add a dated implementation-status section recording the commit IDs and exact
test commands, without changing the remaining three slice requirements.

- [ ] **Step 6: Commit verification and documentation**

```bash
git add rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs \
  rust-repo/crates/mooncake-store-master/tests/test_promotion_retry.rs \
  rust-repo/docs/superpowers/specs/2026-07-23-up-main-parity-design.md
git commit -m "[Store] verify Rust promotion retry parity"
```
