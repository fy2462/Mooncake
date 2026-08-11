# K8s HA Client Spec Availability Parity Implementation Plan

**Goal:** Record the C++ build-gated client parser as not applicable to Rust
and separately prove Rust preserves the connstring payload at its own Master
configuration boundary before serving rejection.

---

## Task 1: Add the combined witness

**File:** `rust-repo/crates/mooncake-store-master/tests/test_main_config.rs`

- [ ] Build a K8s HA spec with exact connstring `default/master`.
- [ ] Assert backend type and connstring.
- [ ] Validate the same spec for serving and assert
  `UnavailableInCurrentMode` plus the shared ordered oplog explanation.
- [ ] Run the exact and complete test binary and scoped formatting.

## Task 2: Record and verify classification

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move only the exact K8s client-spec row to `not-applicable` under
  `cpp-build-or-abi`; do not claim the Master CLI test parses `k8s://`.
- [ ] Record the witness as evidence of the distinct Rust serving policy.
- [ ] Run all manifest validators, validator pytest, shell contracts, JSON
  parsing, and `git diff --check`.
- [ ] Commit exact hunks and request read-only review before moving to the next
  missing Store cluster.
