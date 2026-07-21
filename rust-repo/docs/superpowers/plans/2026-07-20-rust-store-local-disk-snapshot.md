# Rust Store Local-Disk Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve Rust master local-disk client state, including `ssd_total_capacity_bytes`, across standalone and HA snapshot restore while remaining backward-compatible with older snapshots.

**Architecture:** Add a serialization-safe local-disk snapshot record shared by the standalone backend result and `LoadedSnapshot`. Persist `client_id`, `enable_offloading`, `offloading_objects`, and `ssd_total_capacity_bytes`; intentionally reset runtime-only `promotion_objects`. Extend the C++-compatible HA `ld` map using the upstream trailing-capacity representation.

**Tech Stack:** Rust, serde/rmp-serde, rmpv, zstd, DashMap, Cargo tests.

## Global Constraints

- Baseline is `110bfa47aabc713ef1cdf`; this batch migrates upstream `525c7305`.
- Old msgpack/JSON snapshots without local-disk state must still load with an empty map.
- Old HA local-disk entries without a trailing capacity must restore capacity as `0`.
- `promotion_objects` is runtime-only and must restore empty.
- Run unit/integration tests plus a local RDMA smoke test when the batch is complete.

---

### Task 1: Standalone snapshot round trip

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/state.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/storage_backend/storage_backend_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/snapshot.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_storage.rs`

**Interfaces:**
- Produces: `LocalDiskSnapshotEntry { client_id, enable_offloading, offloading_objects, ssd_total_capacity_bytes }`.
- Extends `StorageBackend::save` with `&DashMap<Uuid, LocalDiskSegmentEntry>` and `load` with `Vec<LocalDiskSnapshotEntry>`.

- [ ] Add a test that saves one local-disk client and asserts all persisted fields round-trip while `promotion_objects` is absent.
- [ ] Run `cargo test -p mooncake-store-master --test test_storage local_disk -- --nocapture`; expect failure because `save`/`load` do not carry local-disk state.
- [ ] Add serde-defaulted local-disk records, thread them through save/load and `LoadedSnapshot`, and restore them into `MasterState.local_disk_segments`.
- [ ] Add a legacy snapshot test whose top-level payload omits the new field and assert it loads an empty local-disk collection.
- [ ] Re-run the focused storage and HA tests; expect pass.

### Task 2: Catalog/HA snapshot compatibility

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/catalog_snapshot.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs`

**Interfaces:**
- Consumes: `LoadedSnapshot.local_disk_segments: Vec<LocalDiskSnapshotEntry>`.
- Produces: C++-compatible `ld` entries encoded as `[enable_offloading, count, key, size, ..., ssd_total_capacity_bytes]`.

- [ ] Extend the catalog snapshot test with local-disk state and assert publish/load preserves capacity and offload queue.
- [ ] Run `cargo test -p mooncake-store-master --test test_catalog_snapshot local_disk -- --nocapture`; expect failure because `ld` is currently empty and ignored on decode.
- [ ] Encode deterministic client/key ordering and decode both new trailing-capacity and old no-capacity entries.
- [ ] Reject malformed UUIDs, negative sizes/capacity, and truncated key/size pairs with `HaError::Snapshot`.
- [ ] Re-run the catalog snapshot tests; expect pass.

### Task 3: Migration record and verification

**Files:**
- Create: `rust-repo/change_logs/2026-07-20-002.md`

**Interfaces:**
- Records upstream commit, compatibility contract, exact tests, RDMA smoke result, and resulting Rust commit subject.

- [ ] Run `cargo fmt --all -- --check` from `rust-repo`; expect pass.
- [ ] Run `cargo test -p mooncake-store-master --no-fail-fast`; expect all master tests pass.
- [ ] Run the repository's existing Rust/RDMA smoke path against the local simulated RDMA devices; record the exact command and result.
- [ ] Run pre-commit on touched files when available and review `git diff --check` plus `git diff`.
- [ ] Write `2026-07-20-002.md` with observed results only, then commit with `fix(store-rust): persist local disk snapshot state`.
