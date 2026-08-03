# Master Snapshot Codec Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Cover four portable `MasterSnapshotCodecTest` rows with exact production Rust witnesses, reducing authoritative missing from 859 to 855.

**Architecture:** Crate unit tests drive a real `MasterServiceImpl` through public RPCs, capture a `LoadedSnapshot`, publish/load the real C++ catalog object layout, atomically restore a fresh service, and observe it through public RPCs. Negative tests overwrite the actual published object keys and use the production candidate loader.

**Tech Stack:** Rust 2024, Tokio, tonic Master RPCs, `CatalogBackedSnapshotProvider`, embedded catalog, local object store, rmpv, zstd, Store parity JSON validators, Python from `/home/fy2462/Mooncake/.venv`, git, pre-commit.

## Global Constraints

- Change exactly the four selected `MasterSnapshotCodecTest` rows.
- Positive witnesses must restore a fresh service and use public RPC observations.
- The corrupt witness must replace all three payload objects in one snapshot.
- The invalid task must contain exactly eight fields and an integer id, must not unwind, must return `HaError::Snapshot` without a healthy candidate, and must preserve older-candidate fallback.
- Add no production API or test-only production behavior unless a RED proves it necessary.
- Do not modify or format C/C++ files; preserve zero C/C++ changes across baseline `186bd256..HEAD`.

---

### Task 1: Shared production snapshot test fixture

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

- [ ] Add imports for catalog/object-store traits, `Arc`, `AssertUnwindSafe`, and the required proto helpers inside `snapshot_restore_tests`.
- [ ] Add a fixture that owns a `TempDir`, `Arc<LocalFileSnapshotObjectStore>`, and `CatalogBackedSnapshotProvider` built with `EmbeddedSnapshotCatalogStore` over the same object store.
- [ ] Add helpers to publish a captured service snapshot, download/overwrite exact descriptor object keys, and atomically install a loaded snapshot into a fresh service.
- [ ] Compile the snapshot-restore module and keep helper scope local to tests.

---

### Task 2: Empty and Memory replica round trips

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

- [ ] Add `cpp_parity_master_snapshot_codec_empty_round_trip`.
- [ ] Assert exact raw `segments`, `metadata`, and `task_manager` objects are nonempty.
- [ ] Load and restore a fresh service; prove empty objects/segments/tasks and empty public FetchTasks.
- [ ] Add `cpp_parity_master_snapshot_codec_memory_replica_round_trip`.
- [ ] Mount the exact 16-MiB Memory segment at `0x300000000` and commit the exact 1024-byte default-tenant key through public RPCs.
- [ ] Publish/load/restore and assert public GetReplicaList returns exactly one Memory replica with exact name, endpoint, and size.
- [ ] Retain one primary assertion mutation RED for each test, then restore and prove both GREEN.

---

### Task 3: Corrupt and invalid-task payloads

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

- [ ] Add `cpp_parity_master_snapshot_codec_corrupt_payloads_fail`.
- [ ] Publish a valid empty snapshot, overwrite all three exact objects with the C++ byte triples, and assert the single-candidate load returns `HaError::Snapshot`.
- [ ] Add a helper that encodes the exact eight-field integer-id MessagePack task payload and zstd-compresses at level 3.
- [ ] Add `cpp_parity_master_snapshot_codec_invalid_task_field_type_returns_error_without_unwind`.
- [ ] Prove a newer malformed candidate does not unwind and falls back to the exact older healthy snapshot id.
- [ ] In an isolated single-candidate fixture, prove the same malformed payload does not unwind and returns `HaError::Snapshot`.
- [ ] Retain one primary assertion mutation RED for each test, then restore and prove both GREEN.

---

### Task 4: Focused and broad verification

**Files:**
- Verify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

- [ ] Run each exact stable test ten times into one anchored transcript; require 40 one-test passes and zero failures/zero-test selections.
- [ ] Run `cargo test -p mooncake-store-master --lib service::snapshot_restore_tests`.
- [ ] Run `cargo test -p mooncake-store-master --test test_catalog_snapshot`.
- [ ] Run `cargo test -p mooncake-store-master --lib`.
- [ ] Run `cargo test -p mooncake-store-master` and retain the complete package log.
- [ ] Run `cargo check -p mooncake-store-master --all-targets`.
- [ ] Run Rust 2024 rustfmt check and `git diff --check`.
- [ ] Commit the exact test implementation.

---

### Task 5: Manifest, remediation ledger, review, and merge

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Modify: `.git/codex-parity-handoff.md`

- [ ] Change exactly the four selected rows to `covered` and cite their stable Rust tests.
- [ ] Append exactly four remediation records with implementation SHA, exact focused command, complete package gate, and retained evidence paths.
- [ ] Run all four parity validators, 44 validator self-tests, and both shell contracts.
- [ ] Run pre-commit on touched files with `SKIP=mooncake-code-format,codespell`.
- [ ] Check JSON, exact Rust formatting, `git diff --check`, lean diff scope, and C/C++ zero-change guard.
- [ ] Obtain independent review and resolve all Critical/Important/Minor findings.
- [ ] Merge into `rust_repo_main`, rerun the complete Master package, update stable handoff to exact HEAD/counts/evidence, remove the feature worktree/branch, and confirm the main worktree is clean.
