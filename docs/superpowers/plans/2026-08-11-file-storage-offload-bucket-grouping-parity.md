# FileStorage Offload Bucket Grouping Parity Implementation Plan

> Execute with test-driven development and verify every claim from fresh command output.

**Goal:** Add direct Rust parity for the four C++
`FileStorageTest.GroupOffloadingKeysByBucket*` contracts and move exactly those
four manifest rows from `missing` to `covered`.

**Architecture:** `BucketStorageBackend` owns a mutex-protected residue map and
exposes a crate-private `allocate_offloading_buckets` compatibility boundary.
It applies the existing configured task key/data-size limits and consults the
durable record index, without changing the transactional per-object offload
loop.

**Tech stack:** Rust 2024, `parking_lot::Mutex`, Cargo unit tests, JSON parity
manifests, Python validators, pre-commit.

---

## Task 1: Add four failing parity witnesses

**Files:**

- Modify: `rust-repo/crates/mooncake-store-client/src/local_storage_backend/bucket.rs`

- [ ] Add exact tests named:
  - `cpp_parity_file_storage_group_offloading_keys_by_bucket_key_limit`
  - `cpp_parity_file_storage_group_offloading_keys_by_bucket_size_limit`
  - `cpp_parity_file_storage_group_offloading_keys_by_bucket_combined_limits`
  - `cpp_parity_file_storage_group_offloading_keys_by_bucket_retains_residue`
- [ ] Drive the planned `allocate_offloading_buckets` and residue-count
  boundary; do not emulate grouping in test-only code.
- [ ] Run each exact test from `/home/fy2462/Mooncake/rust-repo` and record the
  expected compile failure because the production boundary does not exist:

```bash
cargo test -p mooncake-store-client --features link-native --lib \
  local_storage_backend::bucket::tests::cpp_parity_file_storage_group_offloading_keys_by_bucket_key_limit -- --exact
```

Repeat for the other three exact names.

## Task 2: Implement the minimum production planner

**Files:**

- Modify: `rust-repo/crates/mooncake-store-client/src/local_storage_backend/bucket.rs`

- [ ] Add the mutex-protected ungrouped map and initialize it in `new`.
- [ ] Implement `allocate_offloading_buckets` with empty-input preservation,
  residue preload, key/data-byte limits, oversized/existing-key skipping,
  checked accumulation, incomplete-tail retention, and C++ repeated-task
  behavior.
- [ ] Add a test-only residue count accessor.
- [ ] Rerun all four exact tests; expected: each passes.
- [ ] Run the complete client library suite:

```bash
cargo test -p mooncake-store-client --features link-native --lib
```

Expected baseline after this wave: 370 passing library tests, subject only to
additional pre-existing working-tree tests discovered by Cargo.

- [ ] Run scoped formatting without rewriting unrelated dirty files:

```bash
cargo fmt --check -p mooncake-store-client
```

If the repository-wide package check reports pre-existing dirty-file drift,
format only this wave's changed `bucket.rs` hunks and verify the scoped diff.

- [ ] Stage only this wave's production/test hunks and commit:

```bash
git commit -m '[Store] add offload bucket grouping planner'
```

## Task 3: Update exactly four parity rows and ledger records

**Files:**

- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Change only the four named C++ rows to `covered`, referencing
  `local_storage_backend/bucket.rs` and the exact witness for each.
- [ ] Append one remediation record per row with the Task 2 repair SHA, exact
  focused command, full-library command, and fresh evidence.
- [ ] Parse both JSON files and run all four validators from repository root:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m json.tool rust-repo/tools/store-validation/parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python -m json.tool rust-repo/tools/store-validation/remediation-log.json >/dev/null
for manifest in parity-map.json transfer-engine-parity-map.json tent-parity-map.json wheel-store-parity-map.json; do
  /home/fy2462/Mooncake/.venv/bin/python rust-repo/tools/store-validation/validate_parity.py \
    --manifest "rust-repo/tools/store-validation/$manifest" --repo-root .
done
```

- [ ] Run validator pytest and both shell contract suites, then scoped
  pre-commit on this wave's docs, Rust file, and JSON files.
- [ ] Assert the independently reviewable committed view is Store
  `covered=751, missing=533, not-applicable=115`; assert the accumulated
  checkout is Store `covered=1093, missing=191, not-applicable=115`. Wheel is
  unchanged in both views.
- [ ] Run `git diff --check`, confirm no C/C++ path changed, stage only the four
  JSON records, and commit:

```bash
git commit -m '[Store] record offload bucket grouping parity'
```

## Task 4: Review and continue

- [ ] Request a read-only code review of the independently reviewable wave.
- [ ] Address all critical and important findings, then rerun affected checks.
- [ ] Do not mark the global parity goal complete. Select the next bounded
  missing cluster from the authoritative manifest; after this wave the active
  checkout still has 191 Store and 52 wheel missing rows.
