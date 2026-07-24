# OffsetAllocator Crash-Consistency Implementation Plan

Design: `docs/superpowers/specs/2026-07-25-offset-allocator-crash-consistency-design.md`

## Task 1: Configuration and record primitives

- Add persistence mode, interval, and CRC settings with C++ environment names.
- Add CRC-32C and v3 record header encode/decode/alignment helpers.
- Write failing config/header/CRC tests first, then implement.
- Commit: `[Store] add OffsetAllocator persistence primitives`.

## Task 2: Versioned checkpoint and legacy loading

- Introduce a checksummed, versioned checkpoint envelope and atomic
  tmp-write/sync/rename/directory-sync helper.
- Preserve loading of the legacy JSON/raw-value index.
- Add deterministic failure injection at write, sync, rename, and directory
  sync boundaries.
- Test stale tmp cleanup, corrupt/versioned checkpoints, and previous-checkpoint
  survival.
- Commit: `[Store] add crash-safe OffsetAllocator checkpoints`.

## Task 3: Durable write and recovery protocol

- Write aligned v3 records with sequence and optional CRC.
- Enforce data sync before checkpoint publication.
- Recover transactionally, validate each record, drop isolated corruption, and
  rebuild free/FIFO state.
- Add strict, relaxed, abrupt-restart, torn-record, CRC-disabled sequence-guard,
  and mixed legacy/v3 tests.
- Commit: `[Store] recover Rust OffsetAllocator after crashes`.

## Task 4: Eviction tombstones and mode semantics

- Persist finalized eviction tombstones, exclude rollback, and clear stale
  tombstones on rewrite.
- Implement strict per-mutation checkpoints, relaxed interval/final checkpoint,
  and disabled fresh-start behavior.
- Test notification rollback, finalized eviction restart, rewrite, checkpoint
  failure behavior, and public API compatibility.
- Commit: `[Store] make OffsetAllocator eviction restart-safe`.

## Task 5: Verification, disposition, and review

- Run client OffsetAllocator tests, client crate tests, formatting, Clippy
  disposition, and touched-file pre-commit.
- Audit the diff for protobuf/C++ dependencies and unrelated master backend
  changes.
- Record `30eff961` parity and exact verification in a migration log and update
  the design implementation status.
- Request independent whole-slice review and fix every Critical/Important
  finding with RED/GREEN evidence.
- Commit: `[Store] record OffsetAllocator persistence parity`.

