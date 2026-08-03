# Snapshot Child Restore Semantics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close four portable SnapshotChild restore rows with production-boundary Rust tests and the minimum required non-complete restore cleanup.

**Architecture:** Public Master RPCs build source state, `CatalogBackedSnapshotProvider` persists and reloads the real catalog payload set, and `restore_loaded_snapshot_state` atomically installs it into a fresh service. Public RPCs provide final evidence; one production-loaded snapshot assertion proves exact group-ID serialization.

**Tech Stack:** Rust 2024, Tokio tests, tonic Master RPC trait, local snapshot object store, embedded snapshot catalog, Cargo, JSON parity validators, pre-commit.

## Global Constraints

- Do not modify or format C or C++ files.
- Do not claim C++ fork, signal, child-process, destructor, or fixed metadata-shard mechanics.
- Do not use private Master maps as final parity evidence.
- Preserve durable replication-task recovery and committed in-place-upsert restore behavior.
- Update exactly four missing rows and add exactly four remediation records only after GREEN evidence.

---

### Task 1: Add shared public snapshot restore fixture helpers

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

**Interfaces:**
- Consumes: existing `snapshot_codec_provider`, `publish_service_snapshot`, `restore_loaded_snapshot`, `MasterService` RPC methods.
- Produces: test-only helpers for mounting/remounting one exact Memory segment, starting/completing an object with optional group ID, reading existence/all keys, and loading/restoring the latest production candidate.

- [ ] **Step 1: Add only test-module helpers using existing public RPC request types**

Keep constants for the 16-MiB segment and base address in the test module. The object helper must accept `complete: bool` and `group_id: Option<&str>` so PutStart-only state is created by omitting `PutEnd`, not by mutating private state.

- [ ] **Step 2: Compile the focused module**

Run: `cargo test -p mooncake-store-master service::snapshot_restore_tests --no-run`

Expected: compilation succeeds without production behavior changes.

### Task 2: Prove grouped routing, corrupt fallback, and expired cleanup

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

**Interfaces:**
- Consumes: Task 1 helpers and production provider/object-store APIs.
- Produces tests `cpp_parity_snapshot_child_restore_rebuilds_grouped_object_routing`, `cpp_parity_snapshot_child_restore_falls_back_when_latest_metadata_is_corrupt`, and `cpp_parity_snapshot_child_restore_cleans_expired_lease`.

- [ ] **Step 1: Write all three exact tests**

The grouped test asserts the loaded `ObjectEntry.group_id`, then public restored query/remove/existence. The fallback test corrupts `format!("{}metadata", newest.object_prefix)`, asserts the selected older ID, and proves old-present/new-absent publicly. The expiry test uses PutEnd's zero lease versus a lease refreshed by `GetReplicaList`, then proves retention/removal publicly.

- [ ] **Step 2: Run independent mutation REDs**

Temporarily invert one primary assertion in one test at a time, run only that exact test, capture each expected failure, and immediately restore the source before the next mutation.

- [ ] **Step 3: Run the restored tests GREEN**

Run each exact test separately with `cargo test -p mooncake-store-master <exact-name> -- --exact --nocapture`.

Expected: one selected test and one pass for every command.

- [ ] **Step 4: Commit the three behavior witnesses**

Commit only the test-module changes with a `[Store]`-prefixed message.

### Task 3: Reproduce and fix initial PutStart restore cleanup

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

**Interfaces:**
- Consumes: Task 1 helpers and `restore_loaded_snapshot_state`.
- Produces: `cpp_parity_snapshot_child_restore_cleans_non_complete_replica` plus the minimal retention predicate correction.

- [ ] **Step 1: Write the failing end-to-end test**

Create a complete leased key and an uncommitted PutStart-only key, publish/load/restore, remount, assert the complete key is readable, and assert `GetAllKeys` omits the incomplete key.

- [ ] **Step 2: Run RED against current production behavior**

Run: `cargo test -p mooncake-store-master cpp_parity_snapshot_child_restore_cleans_non_complete_replica -- --exact --nocapture`

Expected: FAIL because the incomplete key remains in the public all-keys result.

- [ ] **Step 3: Correct the restore retention predicate**

In `restore_loaded_snapshot_state`, remove unconditional retention based only on `put_start_time`. Retain Complete replicas, durable replication-task targets, and the existing committed in-place-upsert generation; let the existing orphan quarantine handle the uncommitted initial PutStart allocation.

- [ ] **Step 4: Run the exact test GREEN and preserve adjacent contracts**

Run the new exact test, `snapshot_restore_preserves_in_place_upsert_charge_and_processing_generation`, and the durable Copy/Move snapshot restore tests selected by the focused module.

Expected: all selected tests pass.

- [ ] **Step 5: Commit the production fix and regression**

Commit only the service production/test changes with a `[Store]`-prefixed message.

### Task 4: Stress and full Rust verification

**Files:**
- No source changes expected.

**Interfaces:**
- Consumes: all four exact tests.
- Produces: reproducible GREEN transcripts and hashes.

- [ ] **Step 1: Run ten exact rounds**

Run all four exact test names for ten consecutive rounds and require exactly 40 passes, zero failures, and no zero-test selection.

- [ ] **Step 2: Run focused and package gates**

Run the snapshot restore module, `test_catalog_snapshot`, Master library tests, Master complete package tests, and `cargo check -p mooncake-store-master --all-targets`.

- [ ] **Step 3: Record evidence hashes**

Hash the semantic RED, mutation RED, ten-round GREEN, and full-package logs with `sha256sum`.

### Task 5: Record parity and validate repository hygiene

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: the authoritative remediation ledger used by adjacent completed rows.
- Modify: `.git/codex-parity-handoff.md`

**Interfaces:**
- Consumes: committed behavior/fix SHA and Task 4 evidence.
- Produces: four covered rows, four non-pending remediation records, refreshed exact counts, and a restart-safe handoff.

- [ ] **Step 1: Update exactly four rows and four ledger entries**

Use the exact stable Rust test names and the behavior/fix commit SHA. Do not broaden any neighboring row.

- [ ] **Step 2: Run all parity validators and self-tests**

Run all four manifest validators, the validator test suite, and both shell contract tests.

- [ ] **Step 3: Run hygiene gates**

Run touched-file pre-commit with C++ formatting/codespell skipped, Rust 2024 formatting checks, JSON parsing, `git diff --check`, and the zero C/C++ change audit.

- [ ] **Step 4: Commit parity records**

Commit the manifests, ledger, and tracked documentation with a `[Store]`-prefixed message. Keep `.git/codex-parity-handoff.md` as local handoff state.

### Task 6: Review, integrate, and verify merged main

**Files:**
- Review all files changed since the design commit.

**Interfaces:**
- Consumes: the completed isolated feature branch.
- Produces: reviewer-ready branch, fast-forwarded `rust_repo_main`, merged-main verification, and cleaned worktree.

- [ ] **Step 1: Request independent correctness review**

Require explicit Critical/Important/Minor findings and READY only when none remain. Fix findings through RED/GREEN where behavior changes.

- [ ] **Step 2: Fast-forward merge into `rust_repo_main`**

Require a clean main worktree and use a non-interactive fast-forward merge.

- [ ] **Step 3: Rerun the complete package and validators on merged main**

Expected: all result summaries pass, all validators pass, remediation PENDING count is zero, and aggregate missing decreases by exactly four.

- [ ] **Step 4: Refresh handoff and remove the isolated worktree**

Record final HEAD, exact counts, evidence paths/hashes, review outcome, and the next honest missing batch before deleting the feature worktree and branch.
