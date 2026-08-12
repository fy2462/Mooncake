# Basic Request Metrics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproduce the exact C++ BasicRequest metrics lifecycle through real Rust scalar RPCs.

**Architecture:** Count scalar RPC requests/failures at the Tonic boundary and synchronize global/labeled Memory gauges from authoritative allocator, topology, and object state. Verify all checkpoints in an isolated child process.

**Tech Stack:** Rust, Tonic, Prometheus GaugeVec/IntGauge, Tokio integration tests, Store parity JSON.

## Global Constraints

- Observe behavioral RED before production changes.
- Preserve RPC business responses and mutation ordering.
- Never count one successful request twice.
- Derive capacity/allocation/key gauges from authoritative state, not deltas.
- Use deterministic lease expiry and a non-forced Remove.
- Run targeted tests with `--test-threads=1`; never run the full workspace suite.
- Update only `MasterMetricsTest.BasicRequestTest`.

---

### Task 1: Exact Fresh-Child Witness

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_metrics.rs`

**Interfaces:**
- Produces: `cpp_parity_master_metrics_test_cpp_mastermetricstest_basicrequesttest`.
- Consumes: real scalar MasterService RPCs and production metric observation helpers.

- [ ] **Step 1: Add the full C++ lifecycle**

Mount 16 MiB, run the four put generations and all intermediate Revoke, End,
Exist, Get, Remove, RemoveAll, and Unmount calls, asserting exact response shape
and every C++ metric checkpoint. Use a child marker for fresh state.

- [ ] **Step 2: Verify RED**

```bash
timeout 240 cargo test -p mooncake-store-master --test test_master_metrics cpp_parity_master_metrics_test_cpp_mastermetricstest_basicrequesttest -- --exact --test-threads=1
```

Expected: one test runs and fails immediately after Mount because capacity or
the Mount request count remains zero.

### Task 2: Production Counter and Gauge Wiring

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_trait.rs`
- Modify: affected authoritative service/helper/restore modules.

**Interfaces:**
- Produces: labeled segment allocated/capacity gauges and zero-safe observation helpers.
- Produces: one authoritative `sync_memory_metrics(&MasterState)` helper.

- [ ] **Step 1: Add labeled gauges and observation API**

Register `IntGaugeVec` families keyed by `segment`, expose allocated/capacity and
ratio getters that return zero for missing labels, and ensure removed labels no
longer retain stale values.

- [ ] **Step 2: Normalize scalar request accounting**

Wrap the nine BasicRequest scalar RPCs at the Tonic boundary with request once
and failure-on-Err. Delete any corresponding impl success increments.

- [ ] **Step 3: Synchronize authoritative state**

After allocator/topology/object mutations and restore/background cleanup,
derive global and per-segment Memory values plus key count. Audit all paths used
by Put/Revoke/Remove/RemoveAll/Unmount and adjacent restore/eviction paths.

- [ ] **Step 4: Verify GREEN in phases**

Run the exact witness, metrics binary, then focused object/segment test filters
with one test thread.

### Task 3: Manifest, Review, Commit

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Produces: one newly covered C++ row.

- [ ] **Step 1: Map exact evidence only**

Name the exact child test and its complete checkpoint matrix without claiming
other metrics rows.

- [ ] **Step 2: Run gates**

```bash
cargo fmt --all -- --check
python3 -m json.tool tools/store-validation/parity-map.json >/dev/null
python3 tools/store-validation/validate_parity.py --repo-root .. --manifest tools/store-validation/parity-map.json
git diff --cached --check
```

- [ ] **Step 3: Independent review and commit**

Require no Critical/Important findings, fix and retest every finding, then
commit `test(store): cover basic request metrics`. Confirm clean state and
recount missing rows.
