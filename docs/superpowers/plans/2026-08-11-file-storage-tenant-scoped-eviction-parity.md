# FileStorage Tenant-Scoped Eviction Parity Implementation Plan

**Goal:** Prove the Rust scoped-key helper and Master metadata path jointly
match `FileStorageTest.NotifyEvictedDiskReplicasUsesTenantScopedKeys`.

---

## Task 1: Add the client routing witness

**File:** `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`

- [ ] Add
  `cpp_parity_notify_evicted_disk_replicas_routes_same_key_by_tenant`.
- [ ] Use the real `notify_evicted_disk_replicas_with` helper with two scoped
  storage keys sharing one user key.
- [ ] Assert both scoped keys are accepted and notifier calls are exactly
  `tenant-a/shared-key`, then `tenant-b/shared-key`.
- [ ] Run the exact lib test.

## Task 2: Add the real Master metadata witness

**File:** `rust-repo/crates/mooncake-store-master/tests/test_tenant_id_parity.rs`

- [ ] Add
  `cpp_parity_notify_evicted_disk_replicas_removes_same_key_for_both_tenants`.
- [ ] Register both tenants, mount memory and LocalDisk, complete same-named
  memory objects, obtain both generation-bearing offload tasks, and publish
  LocalDisk metadata.
- [ ] Assert both tenant replica queries contain LocalDisk before eviction.
- [ ] Call `batch_evict_disk_replica` once per tenant, assert success status,
  then assert both replica queries return `NotFound`.
- [ ] Run the exact integration test and the complete `test_tenant_id_parity`
  binary.

## Task 3: Verify and commit witnesses

- [ ] Run complete `mooncake-store-client --lib` and scoped rustfmt.
- [ ] Stage only the two new witness hunks and commit:

```bash
git commit -m '[Store] test tenant-scoped LocalDisk eviction'
```

## Task 4: Record parity and run gates

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move exactly the selected row to covered with both witnesses and append
  one ledger record using the Task 3 SHA.
- [ ] Run JSON parsing, all four validators, validator pytest, both shell
  contracts, scoped pre-commit, and `git diff --check`.
- [ ] Assert committed Store counts `753/531/115` and accumulated counts
  `1095/189/115`; wheel remains unchanged.
- [ ] Commit the exact JSON changes, request read-only review, address all
  Critical/Important findings, then continue to the final FileStorage heartbeat
  row without marking the global goal complete.
