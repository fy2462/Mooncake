# OpLog Notifier Single-Error Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one Rust oplog notifier error immediately drive HotStandby from `Watching` to `Reconnecting`, with exact executable evidence for the corresponding C++ oracle.

**Architecture:** Extend the existing in-file notifier fixture with a deterministic one-error mode, then exercise the real `HotStandbyService::start` callback wiring. Change only that callback's state event mapping; keep backend notifier retry policy intact.

**Tech Stack:** Rust, Tokio tests, `OpLogStore`/`OpLogChangeNotifier`, Store parity JSON validator.

## Global Constraints

- Use TDD and observe the new test fail before editing production behavior.
- Run tests with `--test-threads=1`; do not run the full workspace suite.
- Do not change etcd notifier retry thresholds or polling behavior.
- Update only `OpLogReplicatorTest.InjectError_NotifiesCallback` in the parity manifest.

---

### Task 1: Exact Single-Error Witness

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/hot_standby.rs`

**Interfaces:**
- Consumes: `OpLogChangeNotifier::start`, `HotStandbyService::start`, `StandbyStateMachine` transition history.
- Produces: `cpp_parity_ha_oplog_oplog_replicator_test_cpp_oplogreplicatortest_injecterror_notifiescallback`.

- [ ] **Step 1: Add a deterministic one-error notifier fixture**

Add a test-only notifier/store that becomes healthy in `start`, invokes its supplied `OpLogErrorCallback` exactly once on a worker thread, and remains healthy until `stop`. This isolates the callback from the separate unhealthy-notifier polling path.

- [ ] **Step 2: Write the failing service test**

Start HotStandby with oplog following, wait for the transition history to contain `Watching -> Reconnecting` through `WatchBroken`, then assert disconnected/watch-unhealthy/not-ready-for-promotion and clean stop.

- [ ] **Step 3: Run the exact test and verify RED**

Run:

```bash
timeout 180 cargo test -p mooncake-store-master cpp_parity_ha_oplog_oplog_replicator_test_cpp_oplogreplicatortest_injecterror_notifiescallback -- --exact --test-threads=1
```

Expected: one test runs and times out/fails because the current callback increments the error counter and leaves the state in `Watching`.

### Task 2: Minimal Callback Repair

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/hot_standby.rs`

**Interfaces:**
- Consumes: existing `StandbyEvent::WatchBroken` transition table.
- Produces: notifier errors immediately reflected in state-machine and sync-status state.

- [ ] **Step 1: Replace callback-side error accumulation**

In the closure supplied to `notifier.start`, process `StandbyEvent::WatchBroken` whenever the state is neither `Recovering` nor `Failed`; retain the existing sync-status refresh.

- [ ] **Step 2: Run the exact test and verify GREEN**

Repeat Task 1's command and require `running 1 test` plus PASS.

- [ ] **Step 3: Run adjacent low-memory tests**

List matching tests first, then run the new exact witness, notifier sequence tracking, and the state-machine watch-broken transition individually with one test thread.

### Task 3: Parity Evidence and Delivery

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: the exact passing Rust witness.
- Produces: one additional covered applicable C++ Store row.

- [ ] **Step 1: Update the target manifest row**

Set status to `covered`, add the exact Rust file/test evidence, and state narrowly that HotStandby forwards one notifier callback as `WatchBroken`; retain the distinction from notifier-internal retry policy.

- [ ] **Step 2: Run static and manifest gates**

```bash
cargo fmt --all -- --check
python3 -m json.tool tools/store-validation/parity-map.json >/dev/null
python3 tools/store-validation/validate_parity.py --repo-root .. --manifest tools/store-validation/parity-map.json
git diff --cached --check
```

- [ ] **Step 3: Obtain independent review**

Stage only the implementation and manifest files. Require no Critical or Important findings; fix and retest any finding before proceeding.

- [ ] **Step 4: Commit the batch**

Commit with `test(store): cover notifier error callback`, confirm a clean worktree, recount missing applicable parity rows, and select the next exact oracle.
