# Tenant Quota etcd Persistence Parity Implementation Plan

**Goal:** Add discoverable Rust witnesses for missing-key load and exact
tenant-quota snapshot round-trip through live etcd.

---

## Task 1: Add the isolated live-etcd fixture

**File:** `rust-repo/crates/mooncake-store-master/tests/test_tenant_quota.rs`

- [ ] Read `MOONCAKE_TENANT_QUOTA_ETCD_ENDPOINTS` without silently probing a
  developer's local service.
- [ ] Connect an etcd client only when configured, generate a unique valid
  cluster ID, and delete the exact tenant-quota key before the witness.
- [ ] Make cleanup run on fixture drop and constrain it to the exact key.

## Task 2: Add both parity witnesses

- [ ] Add `cpp_parity_etcd_missing_key_loads_empty_snapshot` and assert the
  complete default snapshot from the public load API.
- [ ] Add `cpp_parity_etcd_round_trips_snapshot` and assert an exact two-tenant
  snapshot after public save/load.
- [ ] Run both exact tests in the default no-etcd environment and, when the
  service is available, with the live endpoint configured.
- [ ] Run the complete `test_tenant_quota` binary, scoped rustfmt, and
  `git diff --check`.

## Task 3: Record parity and run gates

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move exactly the two tenant-quota etcd rows to covered and reference the
  exact Rust witnesses.
- [ ] Append one remediation record with the witness commit SHA and truthful
  live-service verification result.
- [ ] Run JSON parsing, all four validators, validator pytest, both shell
  contracts, and `git diff --check`.
- [ ] Commit only exact scoped hunks, request read-only review, address all
  Critical/Important findings, then continue with the remaining Store missing
  rows.
