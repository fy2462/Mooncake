# Storage Backend Cross-Client Readback Parity Implementation Plan

**Goal:** Prove a Rust reader client can see and read exact bytes from shared
Disk replicas published by a distinct writer.

---

## Task 1: Add the integration witness

**File:** `rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs`

- [ ] Start an isolated global-Disk master and two distinct TCP clients.
- [ ] Put five indexed 4-KiB values from the writer.
- [ ] Assert Disk visibility from both writer and reader query boundaries.
- [ ] Read every key through the reader and assert exact indexed bytes.
- [ ] Run the exact test, full practical integration binary, client lib, scoped
  formatting, and `git diff --check`.

## Task 2: Record and verify parity

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move only `CrossClientReadback` to covered and reference the exact test.
- [ ] Record the witness commit and verification evidence.
- [ ] Run all manifest validators, validator pytest, both shell contracts,
  JSON parsing, and `git diff --check`.
- [ ] Commit exact hunks, request read-only review, and continue with the two
  remaining storage-backend lifecycle rows.
