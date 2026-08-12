# Master Metrics Initial Contract Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide and verify every zero-valued metric observed by C++ `MasterMetricsTest.InitialStatusTest` in a fresh Rust process.

**Architecture:** Extend the existing operations and batch metric modules, define lifecycle/eviction families centrally, and expose a typed aggregate snapshot. A child-process unit test verifies both zero values and Prometheus registration without contamination from other tests.

**Tech Stack:** Rust, `prometheus` counters/gauges, standard child-process test harness, Store parity JSON.

## Global Constraints

- Observe a compile-time RED before adding production metric definitions.
- Do not instrument request handlers in this batch.
- Do not reset global Prometheus metrics in production or tests.
- Run targeted tests with `--test-threads=1`; never run the full workspace suite.
- Update only `MasterMetricsTest.InitialStatusTest` in the parity manifest.

---

### Task 1: Exact Child-Process Contract Test

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics.rs`

**Interfaces:**
- Consumes: proposed `master_metric_snapshot() -> MasterMetricSnapshot`, `register_metrics()`.
- Produces: `cpp_parity_master_metrics_test_cpp_mastermetricstest_initialstatustest`.

- [ ] **Step 1: Add the child-process test against the desired API**

The child branch reads the typed snapshot and asserts zero for Memory/file allocation and capacity, ratios, key count, scalar request/failure fields, all Copy/Move fields, eviction fields, four complete batch matrices, and PutStart lifecycle fields. It registers and gathers metrics and asserts every new Prometheus family name exists. The parent launches the current executable with `--exact` and `MOONCAKE_INITIAL_METRICS_CHILD=1` and requires success.

- [ ] **Step 2: Verify RED**

```bash
timeout 180 cargo test -p mooncake-store-master --lib metrics::tests::cpp_parity_master_metrics_test_cpp_mastermetricstest_initialstatustest -- --exact --test-threads=1
```

Expected: compilation fails because `master_metric_snapshot` and its fields do not exist.

### Task 2: Metric Families and Typed Snapshot

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics/operations.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics/batch.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics.rs`

**Interfaces:**
- Produces: Copy/Move request/failure `IntCounter`s; four complete batch metric matrices; eviction and PutStart lifecycle metrics; public `MasterMetricSnapshot`; `master_metric_snapshot()`.

- [ ] **Step 1: Define missing scalar and batch families**

Add CopyStart/End/Revoke and MoveStart/End/Revoke request/failure counters in `operations.rs`. In `batch.rs`, add BatchGetReplicaList and BatchPutStart five-field matrices and add partial/items/failed-items to existing BatchPutEnd and BatchPutRevoke families.

- [ ] **Step 2: Define lifecycle/eviction families and registration**

Add four eviction counters, discard/release counters, and discarded-staging gauge in `metrics.rs`. Re-export and register all newly defined operation and batch metrics in `register_metrics()`.

- [ ] **Step 3: Implement the typed snapshot**

Define explicit integer fields corresponding one-for-one with the C++ getters and two `f64` ratio fields. Derive ratios with a zero-capacity guard and return current singleton values without mutation.

- [ ] **Step 4: Verify GREEN**

Repeat Task 1's exact command and require `running 1 test` and PASS.

- [ ] **Step 5: Run adjacent metrics tests**

List tests under `metrics::tests`, then run that filter with one test thread and a bounded timeout.

### Task 3: Manifest and Delivery

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: exact passing initial-contract witness.
- Produces: one newly covered applicable C++ Store row.

- [ ] **Step 1: Map exact evidence**

Change only `MasterMetricsTest.InitialStatusTest` to covered, naming the exact Rust test and stating that metric existence and initial zero values are covered while non-zero request paths remain separate missing rows.

- [ ] **Step 2: Run gates**

```bash
cargo fmt --all -- --check
python3 -m json.tool tools/store-validation/parity-map.json >/dev/null
python3 tools/store-validation/validate_parity.py --repo-root .. --manifest tools/store-validation/parity-map.json
git diff --cached --check
```

- [ ] **Step 3: Independent review and commit**

Stage only the three metric files and manifest. Require no Critical/Important findings, fix and retest any finding, then commit as `test(store): cover initial metrics contract`. Confirm a clean worktree and recount remaining parity rows.
