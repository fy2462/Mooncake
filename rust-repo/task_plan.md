# Rust Rewrite Gap Fix Plan

## Goal
Fix the highest-priority functional gaps between rust-repo and the C++ codebase.

## Phases

### Phase 1: HA System Wiring (P1A → P0B)
**Status:** ✅ DONE

- [x] 1a: Wire OpLog pipeline — OpLogManager created, wired into MasterServiceImpl at all mutation points
- [x] 1b: HotStandbyService created with snapshot bootstrap + oplog following + promote
- [x] 1c: MasterServiceSupervisor wired into main.rs with perpetual HA retry loop
- [x] 1d: Warmup phase + leadership keepalive

### Phase 2: Storage Backend (P0A)
**Status:** ✅ DONE

- [x] 2a: FilePerKey backend
- [x] 2b: batch_offload / 2c: batch_load / remove_keys / is_exist / remove_by_regex / remove_all / scan_meta

### Phase 3: Segment Struct Alignment (P1C)
**Status:** ✅ DONE

- [x] 3a: Segment aligned with C++ (base, te_endpoint, protocol)
- [x] 3b: All downstream code fixed (50+ call sites)
- [x] 3c: ReplicaType::All added

### Phase 4: Client Buffer Management (P1B)
**Status:** ✅ DONE

- [x] 4a: ClientBufferAllocator with offset-based first-fit
- [x] 4b: 4K-aligned allocation
- [x] 4c: BufferHandle with RAII deallocation

## Test Results
- **274 tests, 274 passed, 0 failed**
- cargo check: 0 warnings
- cargo test: 0 failures

## Files Changed
- New: hot_standby.rs, buffer_allocator.rs
- Rewritten: main.rs, test_segment.rs, test_allocator.rs, test_storage.rs, test_ha.rs
- Modified: oplog.rs, ha.rs, types.rs, state.rs, mod.rs, allocator/mod.rs, storage_backend.rs,
  grpc_objects.rs, grpc_cluster.rs, grpc_batches.rs, helpers.rs, workers.rs, lib.rs, Cargo.toml,
  test_types.rs, test_master_allocator_eviction.rs
