# Storage Backend Disk-Only Read Parity Implementation Plan

**Goal:** Prove expired Memory eviction leaves a readable exact-size global
Disk replica under real Master pressure.

---

## Task 1: Add the pressure witness

**File:** `rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs`

- [ ] Start isolated global Disk storage with a 16-MiB Memory segment and
  short production eviction/lease intervals.
- [ ] Write four 256-KiB seeds, assert Disk metadata, expire leases, and write
  twelve pressure values.
- [ ] Bounded-poll until a seed is Disk-only.
- [ ] Read it through `get_buffer` and assert exact size and indexed bytes.
- [ ] Run exact/repeated, full non-CXL integration, client lib, rustfmt check,
  and `git diff --check`.

## Task 2: Record parity

- [ ] Move only `DiskOnlyReadAfterEviction` to covered.
- [ ] Append truthful remediation evidence with the witness SHA.
- [ ] Run JSON parsing, four validators, validator pytest, both shell
  contracts, and exact staged-diff checks.
- [ ] Commit exact hunks, obtain read-only review, then proceed to
  `DiskReplicaSurvivesWriterDeath`.
