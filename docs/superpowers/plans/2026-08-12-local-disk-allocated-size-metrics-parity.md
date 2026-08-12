# Local Disk Allocated Size Metrics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Rust track `Disk` and `LocalDisk` replica bytes exactly and cover C++ `MasterMetricsTest.LocalDiskReplicaAllocatedSize` through real service RPCs.

**Architecture:** Store a runtime-only accounted-byte ledger on each object and extend the common cache-accounting synchronization/removal helpers to maintain the global gauge. Audit every production replica-vector mutation and synchronize after changes.

**Tech Stack:** Rust, Tonic service calls, Prometheus gauges, Tokio integration tests, Store parity JSON.

## Global Constraints

- Observe a behavioral RED before changing production accounting.
- Count every live `Disk` and `LocalDisk` descriptor by descriptor size, independent of status.
- Never persist the process-local accounted-byte ledger.
- Preserve existing object, replica, allocator, quota, and durability semantics.
- Run targeted tests with `--test-threads=1`; do not run the full workspace suite.
- Update only `MasterMetricsTest.LocalDiskReplicaAllocatedSize` in the manifest.

---

### Task 1: Exact Real-RPC Witness

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_metrics.rs`

**Interfaces:**
- Consumes: production PutStart, PutEnd, NotifyOffloadSuccess, Remove, ExistKey, and `master_metric_snapshot()`.
- Produces: `cpp_parity_master_metrics_test_cpp_mastermetricstest_localdiskreplicaallocatedsize`.

- [ ] **Step 1: Add the isolated C++ fixture**

Use an exact-test child marker. In the child, mount a 64-MiB Memory segment,
put one 4,096-byte object, report its classic LocalDisk completion, assert the
allocated-file gauge is 4,096, remove the key, verify it is absent, and assert
the gauge is zero.

- [ ] **Step 2: Verify behavioral RED**

```bash
timeout 240 cargo test -p mooncake-store-master --test test_master_metrics cpp_parity_master_metrics_test_cpp_mastermetricstest_localdiskreplicaallocatedsize -- --exact --test-threads=1
```

Expected: one test runs and fails at the post-offload `allocated_file_size == 4096` assertion because the gauge remains zero.

### Task 2: Central Disk-Byte Accounting

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/state.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/helpers.rs`
- Modify: production service modules identified by the replica-mutation audit.

**Interfaces:**
- Produces: runtime-only `ObjectEntry::disk_allocated_bytes_accounted: u64`.
- Extends: `sync_cache_total_accounting(&mut ObjectEntry)` and `account_cache_total_removal(&mut ObjectEntry)`.

- [ ] **Step 1: Add focused accounting tests**

Exercise a fresh object, multiple Disk/LocalDisk descriptors, a size-changing
replacement, repeated synchronization, and repeated removal. Assert exact
signed gauge deltas and no contribution from Memory/NoF SSD descriptors.

- [ ] **Step 2: Implement the runtime ledger**

Sum disk descriptor sizes, apply only the difference from the recorded amount,
and set the ledger. Removal subtracts the ledger and resets it to zero.

- [ ] **Step 3: Audit and wire all mutation sites**

For each production `replicas.push`, replacement, `retain`, `remove`, or
`clear` that can affect Disk/LocalDisk descriptors, call the common sync helper
after mutation and before dropping the object guard. Ensure projections/clones
do not double-count; only authoritative objects may carry a nonzero ledger.

- [ ] **Step 4: Verify GREEN in phases**

Repeat the exact integration test, then run the focused helper tests and nearby
offload, remove, upsert, eviction, replication, and reaper filters separately
with one test thread.

### Task 3: Manifest, Review, and Commit

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: exact passing real-RPC witness.
- Produces: one newly covered C++ row.

- [ ] **Step 1: Map only LocalDiskReplicaAllocatedSize**

Name the exact Rust test and state that real offload creation increments by
4,096 and real removal restores zero. Do not claim unrelated SSD metrics rows.

- [ ] **Step 2: Run gates**

```bash
cargo fmt --all -- --check
python3 -m json.tool tools/store-validation/parity-map.json >/dev/null
python3 tools/store-validation/validate_parity.py --repo-root .. --manifest tools/store-validation/parity-map.json
git diff --cached --check
```

- [ ] **Step 3: Independent review and commit**

Stage only the design, plan, test, accounting implementation, audited mutation
sites, and manifest. Require no Critical/Important findings, fix and retest any
finding, then commit `test(store): cover allocated file size metrics`. Confirm a
clean worktree and recount remaining rows.
