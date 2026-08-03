# Task Manager Seven-Row Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cover seven portable `task_manager_test.cpp` parity rows with exact production Rust RPC and snapshot witnesses, reducing the authoritative missing count from 869 to 862.

**Architecture:** Four integration tests drive real Master task RPC lifecycles over mounted source/target segments and committed objects. Two catalog tests round-trip exact `TaskEntry` vectors through `CatalogBackedSnapshotProvider`, while one service unit test exercises the atomic production snapshot restore boundary with an empty task vector. Existing production behavior is unchanged unless a RED test proves a real gap.

**Tech Stack:** Rust 2024, Tokio, tonic Master RPCs, `CatalogBackedSnapshotProvider`, embedded/local snapshot stores, Store parity JSON validators, Python from `/home/fy2462/Mooncake/.venv`, git, pre-commit.

## Global Constraints

- Treat the four manifests in `rust-repo/tools/store-validation` as authoritative.
- Change exactly seven selected `ClientTaskManagerTest` rows from missing to covered.
- Keep `PruningLogic`, `PruneExpiredTasksPendingTimeout`, and `PruneExpiredTasksProcessingTimeoutFreesSlot` missing in this batch.
- Use production RPC, catalog, and atomic restore boundaries; do not substitute source inspection, private-map-only assertions, mocks, or sleeps.
- Do not claim payload byte-format, transfer execution, retry-count, cleanup, or persisted-message parity.
- Do not modify or format C/C++ files.
- Maintain zero C/C++ changes across baseline `186bd256..HEAD`.

---

### Task 1: Public Master task lifecycle witnesses

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_tasks.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_master_tasks.rs`

**Interfaces:**
- Consumes: `MasterServiceImpl::with_runtime_config`, `MasterService::{mount_segment, put_start, put_end, create_copy_task, query_task, fetch_tasks, mark_task_to_complete}`, `MasterRuntimeConfig`, and the generated proto request/response types.
- Produces: `cpp_parity_submit_and_pop_task`, `cpp_parity_mark_task_complete_lifecycle`, `cpp_parity_multiple_clients_fetch_only_own_tasks`, and `cpp_parity_pending_limit_exceeded`.

- [ ] **Step 1: Add focused fixture helpers**

Add helpers that use only public RPCs:

```rust
fn proto_uuid(id: Uuid) -> proto::Uuid {
    let (high, low) = id.as_u64_pair();
    proto::Uuid { high, low }
}

async fn mount_task_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}
```

Add `commit_task_source_object` that calls `PutStart` with `replica_num=1`, the
literal preferred source segment, size 128, then `PutEnd` with Memory replica
type. Add `create_task_copy` returning the public proto UUID from a one-target
`CreateCopyTask` call. Do not add production code or test-only methods to
`MasterServiceImpl`.

- [ ] **Step 2: Write and mutation-check submit/pop (RED)**

Add `cpp_parity_submit_and_pop_task`: mount `seg1` for the source client and
`seg2` for a distinct target client, commit `test_key` on `seg1`, create exactly
one copy to `seg2`, and fetch with batch size ten. Assert one assignment and its
exact task ID, then query that ID and assert `TaskProcessing`.

Temporarily assert `TaskPending` after fetch and run:

```bash
cargo test -p mooncake-store-master --test test_master_tasks cpp_parity_submit_and_pop_task -- --exact --nocapture
```

Expected: FAIL because the queried production state is PROCESSING. Restore the
PROCESSING assertion and rerun; expected: one test passes.

- [ ] **Step 3: Write and mutation-check complete lifecycle (RED)**

Add `cpp_parity_mark_task_complete_lifecycle` using source `seg1`, target
`seg2`, and key `key1`. Query the returned task before fetch and assert PENDING;
fetch exactly one with batch size one; query and assert PROCESSING; call
`MarkTaskToComplete` as the assigned source client with SUCCESS and literal
message `Completed successfully`; query and assert SUCCESS.

Temporarily assert FAILED for the final state and run the exact test command.
Expected: FAIL with actual SUCCESS. Restore SUCCESS and rerun; expected: one
test passes.

- [ ] **Step 4: Write and mutation-check two-client isolation (RED)**

Add `cpp_parity_multiple_clients_fetch_only_own_tasks`: mount source `seg1`
and target `seg2` for client one, source `seg3` and target `seg4` for client two;
commit exact keys `key1` and `key2`; create one copy for each matching target.
Fetch `(client1, 10)` and assert exactly `id1`, fetch `(client2, 10)` and assert
exactly `id2`, then fetch client one again and assert empty.

Temporarily compare client one's first result with `id2` and run the exact
test. Expected: FAIL on task ID equality. Restore `id1` and rerun; expected:
one test passes.

- [ ] **Step 5: Write and mutation-check pending admission limit (RED)**

Add `cpp_parity_pending_limit_exceeded` with
`max_total_pending_tasks: 1`. Mount one source and one target, commit distinct
keys `k1` and `k2`, submit the first task without fetching it, then submit the
second and assert tonic `Code::ResourceExhausted`.

Temporarily configure `max_total_pending_tasks: 2` while keeping the error
assertion and run the exact test. Expected: FAIL because the second submission
succeeds. Restore limit one and rerun; expected: one test passes.

- [ ] **Step 6: Verify and commit the RPC witnesses**

Run all four exact tests, then:

```bash
cargo test -p mooncake-store-master --test test_master_tasks
rustfmt --edition 2024 --check rust-repo/crates/mooncake-store-master/tests/test_master_tasks.rs
git diff --check
```

Expected: all commands exit zero. Commit only the Rust integration test:

```bash
git add rust-repo/crates/mooncake-store-master/tests/test_master_tasks.rs
git commit -m '[Store] cover task manager RPC lifecycles'
```

---

### Task 2: Catalog round-trip and atomic reset witnesses

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Test: both files above

**Interfaces:**
- Consumes: `LoadedSnapshot`, `TaskEntry`, `TaskInfo`, `TaskType`, `TaskStatus`, `CatalogBackedSnapshotProvider::publish_loaded_snapshot`, `SnapshotProvider::load_latest_snapshot`, and crate-private `restore_loaded_snapshot_state` inside its existing unit module.
- Produces: `cpp_parity_task_snapshot_round_trip_preserves_four_states`, `cpp_parity_empty_task_catalog_round_trip`, and `cpp_parity_empty_snapshot_replaces_live_task_state`.

- [ ] **Step 1: Write and mutation-check the four-state catalog round trip (RED)**

Construct exactly four `TaskEntry` values with fixed `Utc` timestamps, unique
IDs, two exact assigned clients, Copy/Copy/Move/Move task types, and
SUCCESS/FAILED/PENDING/PROCESSING states. Use valid nonempty JSON payloads:

```rust
r#"{"tenant_id":"default","key":"copy-success","source":"seg1","targets":["seg2"]}"#
r#"{"tenant_id":"default","key":"copy-failed","source":"seg1","targets":["seg2"]}"#
r#"{"tenant_id":"default","key":"move-pending","source":"seg1","target":"seg2"}"#
r#"{"tenant_id":"default","key":"move-processing","source":"seg1","target":"seg2"}"#
```

Publish a `LoadedSnapshot` through an embedded catalog plus local object store,
load it through the production provider, assert exactly four tasks, and zip
original/restored order to assert ID, type, status, assigned client, nonempty
payload, and absolute created/update timestamp differences no greater than one
second.

Temporarily assert three restored tasks and run:

```bash
cargo test -p mooncake-store-master --test test_catalog_snapshot cpp_parity_task_snapshot_round_trip_preserves_four_states -- --exact --nocapture
```

Expected: FAIL with actual cardinality four. Restore four and rerun; expected:
one test passes.

- [ ] **Step 2: Write and mutation-check the empty catalog round trip (RED)**

Add `cpp_parity_empty_task_catalog_round_trip`: publish an
`empty_loaded_snapshot` whose `tasks` is explicitly empty, load it, and assert
`loaded.tasks.len() == 0`.

Temporarily assert length one and run the exact test. Expected: FAIL with actual
zero. Restore zero and rerun; expected: one test passes.

- [ ] **Step 3: Write and mutation-check atomic empty-task restore (RED)**

Inside `snapshot_restore_tests`, add the asynchronous test
`cpp_parity_empty_snapshot_replaces_live_task_state`. Create a real
`MasterServiceImpl::default()`, seed its state with one valid
`pending_move_task`, capture its ID and assigned client, then call
`restore_loaded_snapshot_state` on the service state with empty segment,
object, task, replication, graceful-unmount, delayed-release, and local-disk
vectors plus no allocator configuration. Assert the call succeeds. Call public
`MasterService::query_task` with the old ID and assert tonic `Code::NotFound`,
then call public `MasterService::fetch_tasks` for the assigned client with batch
size ten and assert the response is empty.

Temporarily assert that the old ID remains present and run:

```bash
cargo test -p mooncake-store-master --lib service::snapshot_restore_tests::cpp_parity_empty_snapshot_replaces_live_task_state -- --exact --nocapture
```

Expected: FAIL because the production atomic restore makes QueryTask return
NotFound. Restore the NotFound assertion and rerun; expected: one test passes.

- [ ] **Step 4: Verify and commit snapshot witnesses**

Run all three exact tests, then:

```bash
cargo test -p mooncake-store-master --test test_catalog_snapshot
cargo test -p mooncake-store-master --lib service::snapshot_restore_tests
rustfmt --edition 2024 --check \
  rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs \
  rust-repo/crates/mooncake-store-master/src/service/mod.rs
git diff --check
```

Expected: all commands exit zero. Commit the two Rust files:

```bash
git add rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs \
  rust-repo/crates/mooncake-store-master/src/service/mod.rs
git commit -m '[Store] cover task snapshot lifecycle parity'
```

---

### Task 3: Exact evidence, manifest mapping, and remediation ledger

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: the seven exact Rust test names and implementation SHAs from Tasks 1 and 2.
- Produces: seven covered rows, seven remediation records, and `/tmp/mooncake-task-manager-seven-row-parity.typescript`.

- [ ] **Step 1: Generate ten-round exact evidence**

Use `script -q -e` to run each exact test independently for ten rounds and
retain one transcript. Audit anchored round/test markers, 70 summaries with
exactly one passed test, zero failures, zero zero-test runs, and command exit
zero.

- [ ] **Step 2: Update exactly seven manifest rows**

Change only the seven reference tests named in the design from `missing` to
`covered`, map each to its exact Rust witness, and describe the direct
production boundary and asserted semantics. Preserve the three clock-dependent
Task Manager rows as missing.

- [ ] **Step 3: Append exactly seven remediation records**

For each row record the exact C++ reference, exact Rust test, first divergence,
the matching Task 1 or Task 2 implementation SHA, an exact focused command, the
relevant full target, and the retained transcript. Do not use prefix filters
that can select more than one test.

- [ ] **Step 4: Validate and commit the ledger**

Run JSON parsing and all four validators. Confirm the Store missing count drops
by exactly seven and aggregate missing becomes exactly 862, with `PENDING=0`.
Commit only the two JSON files:

```bash
git add rust-repo/tools/store-validation/parity-map.json \
  rust-repo/tools/store-validation/remediation-log.json
git commit -m '[Store] record task manager parity batch'
```

---

### Task 4: Broad gates, review, and stable handoff

**Files:**
- Verify: all files changed by this batch
- Modify outside the worktree: `.git/codex-parity-handoff.md`

**Interfaces:**
- Consumes: committed Rust witnesses, manifest, ledger, and retained evidence.
- Produces: fully gated clean `rust_repo_main` HEAD and an updated stable handoff while the 869-row goal remains active at 862.

- [ ] **Step 1: Run broad Rust gates**

Run:

```bash
cargo test -p mooncake-store-master --test test_master_tasks
cargo test -p mooncake-store-master --test test_catalog_snapshot
cargo test -p mooncake-store-master --lib
cargo check -p mooncake-store-master --all-targets
```

Record exact fresh pass counts; every command must exit zero.

- [ ] **Step 2: Run validation and repository gates**

Run all four parity validators, all validator self-tests, both shell contract
suites, exact-file Rust 2024 formatting, JSON parsing, `git diff --check`, and
pre-commit on only the batch files with C/C++ formatting and codespell skipped.

- [ ] **Step 3: Obtain independent review**

Review the seven C++ oracles against exact Rust assertions, production-boundary
honesty, mutation RED evidence, manifest mapping, remediation SHAs, ten-round
evidence, and C/C++ zero-change audit. Resolve every Critical or Important
finding and rerun affected gates.

- [ ] **Step 4: Update handoff and audit current state**

Update `.git/codex-parity-handoff.md` with current HEAD, exact counts, this
batch, fresh gate counts, review result, and transcript. Verify:

```bash
git status --porcelain=v1
git diff --name-only 186bd256..HEAD -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.h' '*.hh' '*.hpp' '*.hxx'
git diff --check
```

Expected: clean worktree, zero C/C++ paths, no whitespace errors, exactly seven
new covered rows and remediation records, aggregate missing 862, and handoff
HEAD equal to repository HEAD. Keep the persistent 869-row goal active and
select the next coherent missing batch.
