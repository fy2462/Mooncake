# FileStorage Batch-Load SSD Metrics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cover `FileStorageTest.BatchLoadRecordsSsdMetrics` and `FileStorageTest.BatchLoadFailureDoesNotRecordSsdMetrics` through the production Rust LocalDisk offload batch-read path.

**Architecture:** Give `OffloadReadHandler` the client-owned optional `ClientMetrics`, extract its real FilePerKey batch reads into one synchronous helper inside the existing blocking worker, and publish one SSD read observation only after every read and exact-size check succeeds. Keep storage backends metrics-free and leave promotion semantics unchanged.

**Tech Stack:** Rust 2024, Tokio blocking workers, tonic, Prometheus `ClientMetrics`, FilePerKey `AttachedLocalStorage`, Cargo, Store parity JSON validators, pre-commit.

## Global Constraints

- Run every Cargo command from `/home/fy2462/Mooncake/rust-repo`.
- Treat the four manifests in `rust-repo/tools/store-validation` as authoritative.
- A successful batch publishes exact key count, summed bytes, and one latency observation; a failed batch publishes nothing.
- Use the real FilePerKey backend in both witnesses; do not substitute a mock metric sink or mock backend.
- Keep `ClientMetrics` out of `LocalStorageBackend` and every concrete backend.
- Do not change promotion, allocation, notification, retry, C/C++, or wheel-manifest behavior.
- Change exactly the two selected Store manifest rows from `missing` to `covered`.

---

### Task 1: RED witnesses for success and failure metric atomicity

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/offload/server.rs`
- Test: `rust-repo/crates/mooncake-store-client/src/offload/server.rs`

**Interfaces:**
- Consumes: `ClientMetrics::new`, `ClientMetrics::render_prometheus`, `AttachedLocalStorage::FilePerKey`, `LocalStorageBackend::new_ephemeral`, `local_storage_key`, and `read_object`.
- Produces: `cpp_parity_file_storage_batch_load_records_ssd_metrics` and `cpp_parity_file_storage_batch_load_failure_does_not_record_ssd_metrics`.

- [ ] **Step 1: Write the successful batch witness before the helper exists**

In `offload/server.rs`, add a `#[cfg(test)] mod tests` with a FilePerKey fixture and an exact Prometheus line helper. Add this test using the not-yet-defined `batch_load_from_local_storage`:

```rust
#[test]
fn cpp_parity_file_storage_batch_load_records_ssd_metrics() {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalStorageBackend::new_ephemeral(LocalStorageConfig {
        root_dir: temp.path().to_path_buf(),
        fsdir: "batch-metrics-success".to_string(),
        enable_eviction: false,
        quota_bytes: 1024 * 1024,
    }));
    backend.init().unwrap();
    let keys = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    let tenant_ids = vec!["tenant-a".to_string(); keys.len()];
    let values = vec![b"abc".to_vec(), b"12345".to_vec(), vec![0x5a; 9]];
    for (key, value) in keys.iter().zip(&values) {
        backend.write_object(&local_storage_key("tenant-a", key), value).unwrap();
    }
    let storage = AttachedLocalStorage::FilePerKey(backend);
    let metrics = ClientMetrics::new(HashMap::new(), true, true).unwrap();
    let loaded = batch_load_from_local_storage(
        &storage,
        &keys,
        &tenant_ids,
        &[3, 5, 9],
        Some(&metrics),
    ).unwrap();
    assert_eq!(loaded, values);
    let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
    assert_metric_sample(&text, "mooncake_ssd_read_ops_total", 3);
    assert_metric_sample(&text, "mooncake_ssd_read_bytes_total", 17);
    assert_metric_sample(&text, "mooncake_ssd_read_latency_us_count", 1);
    assert_metric_sample(&text, "mooncake_ssd_total_ops_total", 3);
    assert_metric_sample(&text, "mooncake_ssd_total_bytes_total", 17);
    assert_metric_sample(&text, "mooncake_ssd_total_latency_us_count", 1);
    assert_metric_sample(&text, "mooncake_ssd_write_ops_total", 0);
    assert_metric_sample(&text, "mooncake_ssd_write_bytes_total", 0);
    assert_metric_sample(&text, "mooncake_ssd_write_latency_us_count", 0);
}
```

- [ ] **Step 2: Write the failed partial-read witness**

Add `cpp_parity_file_storage_batch_load_failure_does_not_record_ssd_metrics`. Its real FilePerKey backend contains only `tenant-a/existing`; the requested key list is `existing, missing` with exact sizes `4, 7`. Assert `batch_load_from_local_storage(...).unwrap_err()` is `tonic::Code::Internal`, then assert all six read/total ops, bytes, and latency-count samples are zero. Also assert all write samples remain zero.

- [ ] **Step 3: Verify the missing production boundary RED**

Run:

```bash
cargo test -p mooncake-store-client --features link-native --lib cpp_parity_file_storage_batch_load_ --no-run
```

Expected: compilation fails because `ClientMetrics` is not yet exported to the sibling offload module and `batch_load_from_local_storage` is not defined. Retain the compiler diagnostics as the TDD RED evidence.

---

### Task 2: Minimal production batch-load metrics implementation

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/client/metrics.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/offload/server.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/ha.rs`
- Test: `rust-repo/crates/mooncake-store-client/src/offload/server.rs`

**Interfaces:**
- Consumes: the two RED witnesses, `ClientMetrics::observe_ssd_read`, `OffloadReadHandler`, and the existing blocking read/registration worker.
- Produces: `batch_load_from_local_storage(storage, keys, tenant_ids, expected_sizes, metrics) -> Result<Vec<Vec<u8>>, tonic::Status>` and a metrics-aware production handler.

- [ ] **Step 1: Expose the existing crate-internal metrics type**

Change `ClientMetrics::new` from `pub(super)` to `pub(crate)` and re-export the type from `client/mod.rs`:

```rust
pub(crate) use metrics::ClientMetrics;
```

This changes no public API; it lets the sibling production offload module share the same internal registry and lets its unit tests build isolated registries.

- [ ] **Step 2: Implement the all-or-nothing helper**

Add this helper above the tonic trait implementation:

```rust
fn batch_load_from_local_storage(
    storage: &AttachedLocalStorage,
    keys: &[String],
    tenant_ids: &[String],
    expected_sizes: &[usize],
    metrics: Option<&ClientMetrics>,
) -> Result<Vec<Vec<u8>>, Status> {
    let started_at = Instant::now();
    let mut values = Vec::with_capacity(keys.len());
    let mut total_bytes = 0_u64;
    for (index, ((key, expected_size), tenant_id)) in keys
        .iter()
        .zip(expected_sizes)
        .zip(tenant_ids.iter().map(String::as_str).chain(std::iter::repeat("")))
        .take(keys.len())
        .enumerate()
    {
        let storage_key = local_storage_key(tenant_id, key);
        let data = storage
            .read_object(&storage_key)
            .map_err(|error| Status::internal(format!("read {key} failed: {error}")))?;
        if data.len() != *expected_size {
            return Err(Status::failed_precondition(format!(
                "offload object {key} at index {index} has {} bytes, expected {expected_size}",
                data.len()
            )));
        }
        total_bytes = total_bytes
            .checked_add(u64::try_from(data.len()).map_err(|_| {
                Status::invalid_argument("offload batch byte count exceeds u64")
            })?)
            .ok_or_else(|| Status::invalid_argument("offload batch byte count overflows u64"))?;
        values.push(data);
    }
    let key_count = u64::try_from(values.len())
        .map_err(|_| Status::invalid_argument("offload batch key count exceeds u64"))?;
    if let Some(metrics) = metrics {
        metrics.observe_ssd_read(total_bytes, key_count, started_at.elapsed());
    }
    Ok(values)
}
```

The helper returns before the observer call for every read, size, or overflow error.

- [ ] **Step 3: Wire the helper into the existing production handler**

Add `pub metrics: Option<Arc<ClientMetrics>>` to `OffloadReadHandler`. In `MooncakeClient::start_offload_server`, initialize it with `metrics: self.metrics.clone()`.

Inside the existing `spawn_blocking` closure, call the helper once, then consume its returned values to create `RemoteReadableRegistration`s. Preserve the existing key-specific registration error text and `reservation.commit(registrations)` call. Remove only the duplicated per-key backend read and exact-size check.

- [ ] **Step 4: Verify both exact tests GREEN**

Run each exact filter independently:

```bash
cargo test -p mooncake-store-client --features link-native --lib offload::server::tests::cpp_parity_file_storage_batch_load_records_ssd_metrics -- --exact --nocapture
cargo test -p mooncake-store-client --features link-native --lib offload::server::tests::cpp_parity_file_storage_batch_load_failure_does_not_record_ssd_metrics -- --exact --nocapture
```

Expected: one selected test passes for each command, with zero failures and no
zero-test selection.

- [ ] **Step 5: Run local regression and formatting gates**

Run:

```bash
cargo test -p mooncake-store-client --features link-native --lib offload::server::tests
cargo test -p mooncake-store-client --features link-native --lib
cargo fmt --check -p mooncake-store-client
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 6: Commit only the implementation and witnesses**

```bash
git add rust-repo/crates/mooncake-store-client/src/client/metrics.rs \
  rust-repo/crates/mooncake-store-client/src/client/mod.rs \
  rust-repo/crates/mooncake-store-client/src/client/ha.rs \
  rust-repo/crates/mooncake-store-client/src/offload/server.rs
git commit -m '[Store] record LocalDisk batch read metrics'
```

Record the resulting SHA for both remediation entries.

---

### Task 3: Manifest, remediation ledger, and broad verification

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Verify: `rust-repo/tools/store-validation/wheel-store-parity-map.json`

**Interfaces:**
- Consumes: both exact test names, fresh passing output, and the Task 2 implementation SHA.
- Produces: exactly two newly covered Store rows and exactly two remediation entries. In the independently reviewable committed wave, Store becomes `covered=747, missing=537, not-applicable=115` and wheel remains `covered=5, missing=95, not-applicable=252`. In the active accumulated checkout, Store missing becomes 195 and wheel missing remains 52.

- [ ] **Step 1: Update exactly two Store manifest rows**

For the two named C++ tests, replace the missing reason with the direct production offload-server batch boundary, set `rust` to `mooncake-store-client/src/offload/server.rs` plus the exact matching test, and change `status` to `covered`. Do not change any other row status.

- [ ] **Step 2: Append exactly two remediation records**

Each record includes the exact C++ and Rust names, the original divergence, the Task 2 SHA, its exact focused command, `cargo test -p mooncake-store-client --features link-native --lib`, and fresh pass evidence. Use one record per C++ row.

- [ ] **Step 3: Parse JSON and run all manifest validators**

Run:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m json.tool rust-repo/tools/store-validation/parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python -m json.tool rust-repo/tools/store-validation/remediation-log.json >/dev/null
for manifest in parity-map.json transfer-engine-parity-map.json tent-parity-map.json wheel-store-parity-map.json; do
  /home/fy2462/Mooncake/.venv/bin/python rust-repo/tools/store-validation/validate_parity.py \
    --manifest "rust-repo/tools/store-validation/$manifest" \
    --repo-root .
done
```

Expected: JSON parsing and all four validators exit zero.

- [ ] **Step 4: Run validator contracts and scoped pre-commit**

Run:

```bash
/home/fy2462/Mooncake/.venv/bin/pytest -q \
  rust-repo/tools/store-validation/tests/test_inventory.py \
  rust-repo/tools/store-validation/tests/test_parity_gate.py \
  rust-repo/tools/store-validation/tests/test_result.py \
  rust-repo/tools/store-validation/tests/test_suites.py \
  rust-repo/tools/store-validation/tests/test_validate_parity.py
bash rust-repo/tools/store-validation/tests/test_module_gate.sh
bash rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
SKIP=mooncake-code-format,codespell \
  VIRTUAL_ENV=/home/fy2462/Mooncake/.venv \
  PRE_COMMIT_HOME=/tmp/mooncake-pre-commit \
  /home/fy2462/Mooncake/.venv/bin/pre-commit run --files \
    docs/superpowers/specs/2026-08-11-file-storage-batch-load-ssd-metrics-parity-design.md \
    docs/superpowers/plans/2026-08-11-file-storage-batch-load-ssd-metrics-parity.md \
    rust-repo/crates/mooncake-store-client/src/client/metrics.rs \
    rust-repo/crates/mooncake-store-client/src/client/mod.rs \
    rust-repo/crates/mooncake-store-client/src/client/ha.rs \
    rust-repo/crates/mooncake-store-client/src/offload/server.rs \
    rust-repo/tools/store-validation/parity-map.json \
    rust-repo/tools/store-validation/remediation-log.json
```

Expected: every command exits zero. If a broader hook rewrites unrelated files, exclude those unrelated edits.

- [ ] **Step 5: Audit counts, diff scope, and commit ledger**

Use `jq` to assert both reproducible views. The independently reviewable committed wave has Store `covered=747`, `missing=537`, `not-applicable=115`, while wheel remains `covered=5`, `missing=95`, `not-applicable=252`. The active accumulated checkout, including earlier uncommitted parity work, has Store `covered=1089`, `missing=195`, `not-applicable=115`, while wheel has `covered=48`, `missing=52`, `not-applicable=252`. Confirm exactly the two selected references changed status in this wave and no C/C++ path changed. Run `git diff --check`, then stage only the two JSON files and commit:

```bash
git add rust-repo/tools/store-validation/parity-map.json \
  rust-repo/tools/store-validation/remediation-log.json
git commit -m '[Store] record FileStorage batch metrics parity'
```

- [ ] **Step 6: Continue the active global parity goal**

Do not mark the global goal complete. The independently reviewable committed wave still has 537 Store and 95 wheel missing rows; the active accumulated checkout still has 195 Store and 52 wheel missing rows. Select the next bounded behavior cluster from the authoritative manifests and repeat design, TDD, direct evidence, validator, and ledger steps until every applicable `missing` row is resolved.
