# K8s HA Client Spec Availability Parity Implementation Plan

**Goal:** Prove the exact C++ K8s client-spec input is preserved by Rust config
parsing and rejected at the production serving capability boundary.

---

## Task 1: Add the combined witness

**File:** `rust-repo/crates/mooncake-store-master/tests/test_main_config.rs`

- [ ] Build a K8s HA spec with exact connstring `default/master`.
- [ ] Assert backend type and connstring.
- [ ] Validate the same spec for serving and assert
  `UnavailableInCurrentMode` plus the shared ordered oplog explanation.
- [ ] Run the exact and complete test binary and scoped formatting.

## Task 2: Record and verify parity

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move only the exact K8s client-spec row to covered.
- [ ] Record the witness commit and verification evidence.
- [ ] Run all manifest validators, validator pytest, shell contracts, JSON
  parsing, and `git diff --check`.
- [ ] Commit exact hunks and request read-only review before moving to the next
  missing Store cluster.
