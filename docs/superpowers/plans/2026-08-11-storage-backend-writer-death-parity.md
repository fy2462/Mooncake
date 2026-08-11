# Storage Backend Writer-Death Parity Implementation Plan

**Goal:** Prove shared Disk metadata survives stale-writer cleanup while the
writer-owned Memory replica is removed.

---

## Task 1: Add the liveness witness

**File:** `rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs`

- [ ] Start an isolated global Disk Master with bounded client TTL and monitor
  intervals.
- [ ] Write four indexed 2-KiB objects, assert Disk for each, then drop the
  writer without explicit teardown.
- [ ] Wait past TTL without metadata queries, create a fresh reader, and assert
  every object is Disk-only.
- [ ] Attempt all public reads and compare exact size/bytes on success.
- [ ] Run repeated exact, full non-CXL integration, client lib, format, and
  diff checks.

## Task 2: Record parity

- [ ] Move only `DiskReplicaSurvivesWriterDeath` to covered.
- [ ] Append truthful remediation evidence with the final witness SHA.
- [ ] Run all manifest validators, validator pytest, both shell contracts, JSON
  parsing, and exact staged-diff checks.
- [ ] Commit exact hunks and obtain read-only review.
