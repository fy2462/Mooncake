# Local File Snapshot Object Store Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Cover ten `LocalFileSnapshotObjectStoreTest` rows with direct production Rust witnesses, reducing authoritative missing from 855 to 845.

**Architecture:** Ten unit tests directly exercise `LocalFileSnapshotObjectStore` through `SnapshotObjectStore`. One private atomic writer serves buffer and string upload; buffer rejects empty input while string retains its independent C++ behavior. The existing constructor shape remains and rejects an empty OS path immediately.

**Tech Stack:** Rust 2024, tempfile, local filesystem, Store parity JSON validators, `/home/fy2462/Mooncake/.venv`, git, pre-commit.

## Constraints

- One stable direct Rust test per selected C++ row.
- Exact byte vectors, strings, keys, prefix cardinalities, and error variants.
- Do not use catalog indirection as evidence.
- Preserve atomic write/fsync and empty-string upload semantics.
- Do not modify or format C/C++ files.

---

### Task 1: Exact tests and initial REDs

**File:** `rust-repo/crates/mooncake-store-master/src/ha/snapshot.rs`

- [ ] Add the ten stable `cpp_parity_local_file_snapshot_object_store_*` tests.
- [ ] Use one fresh `tempdir` and direct store per test.
- [ ] For listing, assert exact sorted vectors for narrow and broad prefixes.
- [ ] For missing/deleted downloads, assert `HaError::Snapshot` and `is_not_found_error`.
- [ ] For empty root, use `catch_unwind`; for empty buffer, assert exact `InvalidParams` and absent key.
- [ ] Run the ten exact tests against current production code and retain the two genuine REDs.

---

### Task 2: Minimal production validation

**File:** `rust-repo/crates/mooncake-store-master/src/ha/snapshot.rs`

- [ ] Reject `base_path.as_os_str().is_empty()` in `new` with a precise message.
- [ ] Extract existing atomic write logic into a private `upload_bytes` helper.
- [ ] Reject empty slices at the start of trait `upload_buffer` with `HaError::InvalidParams`.
- [ ] Override trait `upload_string` to call `upload_bytes` directly, preserving empty strings.
- [ ] Prove the two genuine REDs become GREEN and existing catalog tests remain GREEN.

---

### Task 3: Mutation and stability evidence

- [ ] Produce eight independent assertion mutation REDs for the already-present behaviors.
- [ ] Include the two genuine behavior REDs in the retained RED transcript.
- [ ] Restore exact source and run all ten stable tests for ten rounds.
- [ ] Require 100 markers, 100 one-test passes, zero failures, and zero zero-test selections.
- [ ] Run the complete `ha::snapshot::tests` module, `test_ha`, `test_catalog_snapshot`, Master lib, all-targets check, Rust 2024 rustfmt, and diff checks.
- [ ] Commit production and tests.

---

### Task 4: Manifest, ledger, review, and merge

**Files:**
- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`
- `.git/codex-parity-handoff.md`

- [ ] Change exactly the ten selected rows to covered and append ten remediation records.
- [ ] Run the complete Master package on feature and merged main.
- [ ] Run all four validators, 44 validator self-tests, two shell contracts, and touched-file pre-commit.
- [ ] Verify JSON, exact Rust formatting, lean diff scope, and C/C++ zero changes.
- [ ] Obtain independent review and resolve every Critical/Important/Minor finding.
- [ ] Fast-forward `rust_repo_main`, update handoff, remove the feature worktree/branch, and confirm a clean main tree at aggregate missing 845.
