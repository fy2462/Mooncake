# Task Manager Clock Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cover the three remaining `task_manager_test.cpp` pruning and timeout rows with exact production Rust witnesses, reducing authoritative missing from 862 to 859.

**Architecture:** `MasterState` owns one crate-private `TaskLifecycleClock`; normal builds return real UTC while crate tests may set and advance a fixed instant. Every production `TaskInfo` creation and lifecycle transition reads this shared clock, and three unit tests drive real Master RPCs plus the production reaper seam without sleeps or private-map assertions.

**Tech Stack:** Rust 2024, Tokio, tonic Master RPCs, chrono, parking_lot, Store parity JSON validators, Python from `/home/fy2462/Mooncake/.venv`, git, pre-commit.

## Global Constraints

- Treat all four manifests in `rust-repo/tools/store-validation` as authoritative.
- Change exactly the three named `ClientTaskManagerTest` rows from missing to covered.
- Production uses `Utc::now()` and keeps the exact strict `elapsed > timeout` rule.
- Manual clock control exists only under `cfg(test)` and is crate-private.
- Tests exercise public RPC observations and the production reaper seam; no sleeps or private task-map assertions.
- Finished pruning must remove the first two of seven and retain the last five in strict completion order.
- Timeout tests must preserve the failed task record and prove admission-slot handoff.
- Do not modify or format C/C++ files; preserve zero C/C++ changes across baseline `186bd256..HEAD`.

---

### Task 1: Shared production task-lifecycle clock

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/state.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Test: `rust-repo/crates/mooncake-store-master/src/service/state.rs`

**Interfaces:**
- Produces: `TaskLifecycleClock::default()` and `TaskLifecycleClock::now(&self) -> DateTime<Utc>`.
- Produces under `cfg(test)`: `set_for_test(&self, DateTime<Utc>)` and `advance_for_test(&self, chrono::Duration)`.
- Produces: `MasterState::task_clock: TaskLifecycleClock` initialized in every struct literal.

- [ ] **Step 1: Add a compile-failing clock contract (RED)**

Add a small `#[cfg(test)]` unit test in `state.rs` that constructs the clock,
sets `2026-08-03T00:00:00Z`, advances two seconds, and expects exactly
`2026-08-03T00:00:02Z`:

```rust
#[test]
fn task_lifecycle_clock_can_advance_deterministically() {
    let clock = TaskLifecycleClock::default();
    let start = DateTime::parse_from_rfc3339("2026-08-03T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    clock.set_for_test(start);
    clock.advance_for_test(chrono::Duration::seconds(2));
    assert_eq!(clock.now(), start + chrono::Duration::seconds(2));
}
```

Run:

```bash
cargo test -p mooncake-store-master --lib task_lifecycle_clock_can_advance_deterministically -- --exact --nocapture
```

Expected: compilation fails because `TaskLifecycleClock` is not defined. Retain
the transcript as the implementation RED.

- [ ] **Step 2: Implement the minimal clock**

In `state.rs`, import `chrono::{DateTime, Utc}` and add:

```rust
#[derive(Debug, Default)]
pub(crate) struct TaskLifecycleClock {
    #[cfg(test)]
    fixed_now: RwLock<Option<DateTime<Utc>>>,
}

impl TaskLifecycleClock {
    pub(crate) fn now(&self) -> DateTime<Utc> {
        #[cfg(test)]
        if let Some(now) = *self.fixed_now.read() {
            return now;
        }
        Utc::now()
    }

    #[cfg(test)]
    pub(crate) fn set_for_test(&self, now: DateTime<Utc>) {
        *self.fixed_now.write() = Some(now);
    }

    #[cfg(test)]
    pub(crate) fn advance_for_test(&self, delta: chrono::Duration) {
        let mut fixed_now = self.fixed_now.write();
        let current = fixed_now
            .as_ref()
            .cloned()
            .expect("test clock must be fixed before advance");
        *fixed_now = Some(current + delta);
    }
}
```

Add `pub(crate) task_clock: TaskLifecycleClock` adjacent to `tasks` in
`MasterState`. Initialize it with `TaskLifecycleClock::default()` in the
production constructor in `service/mod.rs` and the test constructor in
`ha/oplog_applier.rs`.

- [ ] **Step 3: Prove the clock GREEN and commit**

Run the exact clock test, `cargo check -p mooncake-store-master --all-targets`,
Rust 2024 formatting check on the three files, and `git diff --check`. Expected:
all exit zero. Commit:

```bash
git add rust-repo/crates/mooncake-store-master/src/service/state.rs \
  rust-repo/crates/mooncake-store-master/src/service/mod.rs \
  rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs
git commit -m '[Store] add shared task lifecycle clock'
```

---

### Task 2: Route every task timestamp through the shared clock

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops/background_reaper.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops/background_drain.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/drain.rs`

**Interfaces:**
- Consumes: `MasterState::task_clock.now()` from Task 1.
- Produces: one time domain for Copy/Move creation, fetch, completion, timeout/pruning, and synchronous/background drain task creation.

- [ ] **Step 1: Route foreground task RPC timestamps**

Replace all four lifecycle reads in `grpc_tasks.rs`:

```rust
let now = self.state.task_clock.now();
task.info.last_updated_at = self.state.task_clock.now();
```

The two `let now` sites are Copy and Move creation; the two update sites are
Fetch and MarkTaskToComplete. Remove the now-unused direct `Utc` import if
rustfmt/clippy reports it.

- [ ] **Step 2: Route reaper and drain timestamps**

Use `state.task_clock.now()` in `reap_client_tasks` and background drain task
creation. Use `self.state.task_clock.now()` in synchronous drain task creation.
After editing, this audit must show no production client-task timestamp creation
using direct UTC:

```bash
rg -n 'Utc::now\(\)' \
  rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/background_reaper.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/background_drain.rs \
  rust-repo/crates/mooncake-store-master/src/service/cluster/drain.rs
```

Expected: no matches.

- [ ] **Step 3: Verify unchanged production behavior and commit**

Run:

```bash
cargo test -p mooncake-store-master --test test_master_tasks
cargo test -p mooncake-store-master --lib service::snapshot_restore_tests
cargo check -p mooncake-store-master --all-targets
rustfmt --edition 2024 --check \
  rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/background_reaper.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/background_drain.rs \
  rust-repo/crates/mooncake-store-master/src/service/cluster/drain.rs
git diff --check
```

Expected: all exit zero. Commit:

```bash
git add rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/background_reaper.rs \
  rust-repo/crates/mooncake-store-master/src/service/background_ops/background_drain.rs \
  rust-repo/crates/mooncake-store-master/src/service/cluster/drain.rs
git commit -m '[Store] unify task lifecycle timestamps'
```

---

### Task 3: Exact pruning and timeout parity witnesses

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs`
- Test: `rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs`

**Interfaces:**
- Consumes: `MasterServiceImpl::with_runtime_config`, public `MasterService` RPCs, `service.state.task_clock` test controls, and `reap_expired_background_tasks_for_test()`.
- Produces: `cpp_parity_pruning_retains_latest_five_finished_tasks`, `cpp_parity_pending_timeout_frees_pending_slot`, and `cpp_parity_processing_timeout_frees_processing_slot`.

- [ ] **Step 1: Add public-boundary unit-test fixtures**

Inside a `#[cfg(test)] mod task_lifecycle_parity_tests`, add helpers equivalent
to the existing integration fixtures: `mount_task_segment`,
`commit_task_source_object`, `create_task_copy`, `query_task`, `fetch_tasks`,
and `complete_task`. Each helper must call `MasterService` trait RPC methods;
only clock setup may access `service.state`.

Use a fixed base time:

```rust
fn fixed_task_time() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-08-03T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}
```

- [ ] **Step 2: Add pruning witness and prove a mutation RED**

Configure `max_total_finished_tasks: 5`. Mount one source and one target. For
keys `prune-key-0` through `prune-key-6`, commit the object, create the Copy
task, fetch exactly its ID, complete it SUCCESS, and advance the test clock one
second after each completion. Run the production reaper once. Query IDs 0 and
1 and assert tonic `Code::NotFound`; query IDs 2 through 6 and assert exact
`TaskSuccess`.

Temporarily expect ID 1 to remain queryable and run:

```bash
cargo test -p mooncake-store-master --lib service::grpc_tasks::task_lifecycle_parity_tests::cpp_parity_pruning_retains_latest_five_finished_tasks -- --exact --nocapture
```

Expected: FAIL because ID 1 is pruned. Retain the failure, restore the exact
oracle, rerun, and expect one pass.

- [ ] **Step 3: Add pending-timeout witness and prove a mutation RED**

Configure `max_total_pending_tasks: 1`, `pending_task_timeout: 1s`, and enough
finished retention to keep the failed record. Create `pending-key-1`, advance
two seconds, and invoke the reaper. Query it and assert exact `TaskFailed` plus
message `pending timeout`; fetch for the source client and assert empty; create
`pending-key-2` and assert success.

Temporarily assert message `processing timeout` and run the exact test. Expected:
FAIL with actual `pending timeout`. Retain, restore, rerun, and expect one pass.

- [ ] **Step 4: Add processing-timeout witness and prove a mutation RED**

Configure `max_total_processing_tasks: 1`, `processing_task_timeout: 1s`, and
enough pending/finished capacity. Create `processing-key-1`, advance one second,
then create `processing-key-2`. Fetch batch ten and assert exactly task 1 in the
assignment. Advance two seconds and invoke the reaper. Query task 1 and assert
exact `TaskFailed` plus `processing timeout`. Fetch again and assert exactly
task 2, then query task 2 and assert `TaskProcessing`.

Temporarily compare the second fetch ID with task 1 and run the exact test.
Expected: FAIL on exact ID. Retain, restore, rerun, and expect one pass.

- [ ] **Step 5: Verify all witnesses and commit**

Run all three exact tests, the complete library, Rust 2024 formatting for
`grpc_tasks.rs`, and `git diff --check`. Expected: all exit zero. Commit:

```bash
git add rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs
git commit -m '[Store] cover task pruning and timeout parity'
```

---

### Task 4: Exact evidence, manifest, and remediation ledger

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: the three exact Rust test names and Task 3 implementation SHA.
- Produces: three covered rows, three remediation records, `/tmp/mooncake-task-manager-clock-parity.typescript`, and `/tmp/mooncake-task-manager-clock-mutation-red.typescript`.

- [ ] **Step 1: Retain ten-round exact GREEN evidence**

Use `script -q -e` to run each exact test independently for ten rounds. Audit
anchored round/test markers, exactly 30 summaries with one passed test, zero
failures, zero zero-test selections, and command exit zero. Record SHA256.

- [ ] **Step 2: Update exactly three manifest rows**

Change only `PruningLogic`, `PruneExpiredTasksPendingTimeout`, and
`PruneExpiredTasksProcessingTimeoutFreesSlot` from `missing` to `covered`.
Map each to its exact Rust witness and describe the public RPC observations and
production reaper boundary. Do not alter any other status.

- [ ] **Step 3: Append exactly three remediation records**

Each record must cite the exact C++ reference, exact Rust test, first
divergence, Task 3 repair SHA, exact focused command, complete Master package
gate, ten-round GREEN transcript, and mutation RED transcript. Do not use a
prefix filter that selects more than one test.

- [ ] **Step 4: Validate counts and commit**

Parse both JSON files with the workspace `.venv`, run all four manifest
validators, and confirm Store missing decreases from 564 to 561, aggregate
missing from 862 to 859, remediation records increase from 454 to 457, and
`repair_commit == "PENDING"` remains zero. Commit:

```bash
git add rust-repo/tools/store-validation/parity-map.json \
  rust-repo/tools/store-validation/remediation-log.json
git commit -m '[Store] record task pruning parity batch'
```

---

### Task 5: Broad gates, review, integration, and handoff

**Files:**
- Verify: every file changed by this batch
- Modify outside worktree: `.git/codex-parity-handoff.md`

**Interfaces:**
- Consumes: committed implementation, exact evidence, manifest, and ledger.
- Produces: clean `rust_repo_main` at aggregate missing 859 and an updated stable handoff while the full goal stays active.

- [ ] **Step 1: Run broad Rust gates**

Run the three exact tests, complete `cargo test -p mooncake-store-master`, and
`cargo check -p mooncake-store-master --all-targets`. Capture logs and exact
pass/fail summaries; all commands must exit zero.

- [ ] **Step 2: Run repository gates**

Run four validators, 44 validator self-tests, both shell contract suites, JSON
parsing, exact-file Rust 2024 formatting, `git diff --check`, and pre-commit on
only touched files with `SKIP=mooncake-code-format,codespell`. Confirm:

```bash
git diff --name-only 186bd256..HEAD -- \
  '*.c' '*.cc' '*.cpp' '*.cxx' '*.h' '*.hh' '*.hpp' '*.hxx'
```

Expected: no output.

- [ ] **Step 3: Obtain independent review and resolve findings**

Review C++ oracles against exact assertions, clock scope, every TaskInfo
timestamp site, mutation REDs, manifest mapping, repair SHA, evidence counts,
and C/C++ guard. Resolve every Critical or Important finding and rerun affected
gates.

- [ ] **Step 4: Fast-forward integrate and clean up**

Verify both worktrees are clean, fast-forward `rust_repo_main` to the feature
branch, rerun the complete Master package and validators on merged main, remove
the feature worktree, and delete the feature branch.

- [ ] **Step 5: Update handoff and select the next batch**

Update `.git/codex-parity-handoff.md` with exact HEAD, count 859, commits,
evidence hashes, full gates, review, and the next coherent applicable missing
batch. Keep the persistent 869-row goal active because 859 rows remain.
