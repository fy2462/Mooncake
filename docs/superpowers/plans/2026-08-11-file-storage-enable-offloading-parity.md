# FileStorage IsEnableOffloading Parity Implementation Plan

**Goal:** Implement the C++ whole-bucket global admission preflight in Rust and
cover `FileStorageTest.IsEnableOffloading` directly.

**Architecture:** A crate-private query on `BucketStorageBackend` consumes the
FileStorage-owned global limits as arguments and combines them with current
backend metadata and backend-owned bucket/eviction limits.

---

## Task 1: Establish the failing witness

**File:** `rust-repo/crates/mooncake-store-client/src/local_storage_backend/bucket.rs`

- [ ] Add
  `cpp_parity_file_storage_is_enable_offloading_preflights_full_bucket` with
  the empty/default, 9-vs-10 key, and 100-vs-969 byte cases.
- [ ] Call the planned production method directly.
- [ ] Run the exact test from `/home/fy2462/Mooncake/rust-repo`; expected red is
  a missing `is_enable_offloading` method.

## Task 2: Implement and verify the admission query

**File:** `rust-repo/crates/mooncake-store-client/src/local_storage_backend/bucket.rs`

- [ ] Add the eviction-capacity fast path.
- [ ] Add checked whole-bucket key and byte projections against explicit global
  limits.
- [ ] Run the exact witness, all `local_storage_backend::bucket::tests`, and:

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-transfer-engine/mooncake-common \
  cargo test -p mooncake-store-client --features link-native --lib
```

Expected complete result: 373 passing tests, unless additional pre-existing
working-tree tests are discovered.

- [ ] Run scoped rustfmt, stage only this wave's hunk, and commit:

```bash
git commit -m '[Store] add whole-bucket offload admission check'
```

## Task 3: Record parity and validate

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move exactly `FileStorageTest.IsEnableOffloading` to covered with the
  exact witness and append one ledger record using the Task 2 SHA.
- [ ] Parse JSON, run all four validators, validator pytest, both shell
  contracts, scoped pre-commit, and `git diff --check`.
- [ ] Assert committed Store counts `752/532/115` and accumulated Store counts
  `1094/190/115`; wheel remains unchanged.
- [ ] Commit only the exact JSON changes:

```bash
git commit -m '[Store] record whole-bucket offload admission parity'
```

## Task 4: Review and continue

- [ ] Request read-only review and address all Critical/Important findings.
- [ ] Continue with the two remaining FileStorage integration rows; do not mark
  the global parity goal complete.
