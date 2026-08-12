# Batch Request Metrics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the real Rust batch RPCs produce the exact cumulative metric matrices asserted by C++ `MasterMetricsTest.BatchRequestTest`.

**Architecture:** Add one shared batch outcome recorder and call it after four status-bearing RPCs finalize their result vectors. Exercise the original C++ three-key then four-key partial sequence in a fresh child process.

**Tech Stack:** Rust, Tonic service trait calls, Prometheus counters, Tokio integration tests, Store parity JSON.

## Global Constraints

- Observe a behavioral RED before changing RPC instrumentation.
- Preserve all existing batch response and mutation semantics.
- Treat BatchExistKey false results as successful queries.
- Run targeted tests with `--test-threads=1`; do not run the full workspace suite.
- Update only `MasterMetricsTest.BatchRequestTest` in the manifest.

---

### Task 1: Exact Real-RPC Witness

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_metrics.rs`

**Interfaces:**
- Consumes: `MasterService` batch RPCs and `metrics::master_metric_snapshot()`.
- Produces: `cpp_parity_master_metrics_test_cpp_mastermetricstest_batchrequesttest`.

- [ ] **Step 1: Add child-process harness and C++ fixture**

Use an exact-test child marker. In the child, mount a 64-MiB Memory segment and run BatchExistKey, BatchPutStart, BatchGetReplicaList, BatchPutEnd, second Exist/Get, BatchPutRevoke, four-key partial Get, and four-key partial PutStart in order. Assert every response shape and the exact cumulative five-field matrix after each call.

- [ ] **Step 2: Verify behavioral RED**

```bash
timeout 240 cargo test -p mooncake-store-master --test test_master_metrics cpp_parity_master_metrics_test_cpp_mastermetricstest_batchrequesttest -- --exact --test-threads=1
```

Expected: one test runs and the child fails at the first uninstrumented BatchPutStart or BatchGetReplicaList matrix assertion.

### Task 2: Production Metric Wiring

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics/batch.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches/batch_put_start.rs`

**Interfaces:**
- Produces: `record_batch_outcome(total, failed, requests, failures, partial, items, failed_items)` internal helper.

- [ ] **Step 1: Implement shared all-failed/partial arithmetic**

Increment request once and items by total. Increment failed-items by failed. Increment failures iff `total > 0 && failed == total`; increment partial iff `failed > 0 && failed < total`.

- [ ] **Step 2: Wire status-bearing batch RPCs**

After final results/statuses exist, count non-success items and call the helper for BatchGetReplicaList, BatchPutStart, BatchPutEnd, and BatchPutRevoke. Do not record when the RPC returns early with validation/infrastructure error.

- [ ] **Step 3: Verify GREEN and adjacent suites**

Repeat the exact test, require `running 1 test` and PASS, then run `metrics::tests::` and relevant `test_batch` metric/status filters separately with one test thread.

### Task 3: Manifest, Review, Commit

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: exact passing child witness.
- Produces: one newly covered C++ row.

- [ ] **Step 1: Map only BatchRequestTest**

Name the exact Rust test and describe all five matrices and both fixture stages without claiming other metrics cases.

- [ ] **Step 2: Run gates**

```bash
cargo fmt --all -- --check
python3 -m json.tool tools/store-validation/parity-map.json >/dev/null
python3 tools/store-validation/validate_parity.py --repo-root .. --manifest tools/store-validation/parity-map.json
git diff --cached --check
```

- [ ] **Step 3: Independent review and commit**

Stage only the test, three production files, and manifest. Require no Critical/Important findings, fix and retest any finding, then commit `test(store): cover batch request metrics`. Confirm clean state and recount missing rows.
