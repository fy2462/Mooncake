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

---

# Async Prefetch Feature Plan

## Architecture Overview

```
MooncakeClient::get(key)
  1. fetch_replicas → select_best_replica → hit → RDMA read
  2. miss → MissHandler.handle_miss(key)
     ├─ Dedup: coalesce concurrent misses on same key (tokio::sync::Mutex + oneshot)
     ├─ Admission: Semaphore-bounded concurrency
     └─ RemoteSource::get(key)
        ├─ LocalFsSource (test/dev)
        └─ S3RemoteSource (prod, Phase 2)
  3. On miss: optionally prefetch neighbor keys in background (tokio::spawn)
```

## Phase 1: Core Framework ✅ DONE

- [x] 1a: RemoteSource trait + RemoteSourceError — `remote_source/mod.rs`, `remote_source/error.rs`
- [x] 1b: MissHandler with dedup + admission + background prefetch — `remote_source/miss_handler.rs`
- [x] 1c: LocalFsSource with key→file mapping + neighbor discovery — `remote_source/local_fs.rs`
- [x] 1d: Integration into MooncakeClient::get() — fallback on KeyNotFound
- [x] 1e: Unit tests for LocalFsSource (get, miss, neighbor_keys)

### New Files
- `crates/mooncake-store-client/src/remote_source/mod.rs` — RemoteSource trait + Arc<T> blanket impl
- `crates/mooncake-store-client/src/remote_source/error.rs` — RemoteSourceError enum
- `crates/mooncake-store-client/src/remote_source/miss_handler.rs` — MissHandler with dedup/admission/prefetch
- `crates/mooncake-store-client/src/remote_source/local_fs.rs` — LocalFsSource + tests

### Modified Files
- `crates/mooncake-store-client/src/lib.rs` — added remote_source module + re-exports
- `crates/mooncake-store-client/src/client/mod.rs` — MooncakeClient gets miss_handler field + with_remote_source()
- `crates/mooncake-store-client/src/client/read.rs` — get() falls back to MissHandler on KeyNotFound
- `crates/mooncake-store-client/Cargo.toml` — added async-trait, toml dependencies

## Phase 1b: Distributed Coordination ✅ DONE

- [x] Proto RPCs: AcquireRemotePull / CompleteRemotePull / ReleaseRemotePull
- [x] Master state: `pending_remote_pulls: DashMap<String, RemotePullEntry>` + stale TTL cleanup
- [x] Master config: `remote_source_enabled`, `remote_pull_ttl`
- [x] Master handlers in grpc_cluster.rs + grpc_trait.rs
- [x] Client: DistributedMissHandler wraps MissHandler with master coordination

```
Node A: miss k1 → AcquireRemotePull → PULL → S3.fetch → PutEnd → CompleteRemotePull
Node B: miss k1 → AcquireRemotePull → WAIT → retry store get → RDMA read (data from A)
```

### New Files
- `crates/mooncake-store-client/src/remote_source/config.rs` — RemoteSourceConfig + S3Config
- `crates/mooncake-store-client/src/remote_source/distributed.rs` — DistributedMissHandler

### Modified Files
- `proto/mooncake_store_grpc.proto` — 3 new RPCs
- `proto/mooncake_store_types.proto` — 5 new messages + RemotePullAction enum
- `crates/mooncake-store-master/src/service/state.rs` — RemotePullEntry + config fields
- `crates/mooncake-store-master/src/service/mod.rs` — pending_remote_pulls init
- `crates/mooncake-store-master/src/service/grpc_trait.rs` — 3 RPC trait methods
- `crates/mooncake-store-master/src/service/grpc_cluster.rs` — 3 RPC handler impls
- `crates/mooncake-store-master/src/service/background_ops.rs` — stale pull reaping
- `crates/mooncake-store-client/src/remote_source/mod.rs` — added config, distributed modules
- `crates/mooncake-store-client/src/remote_source/miss_handler.rs` — RemoteSourceConfig, is_enabled()
- `crates/mooncake-store-client/src/client/read.rs` — get() checks is_enabled()

## Phase 2: S3 Integration ✅ DONE

- [x] 2a: S3RemoteSource based on aws-sdk-s3 — `remote_source/s3_source.rs`
- [x] 2b: Config from S3Config (region, bucket, endpoint, prefix, credentials)
- [x] 2c: Parallel prefetch with bounded concurrency (JoinSet, max 8 concurrent)
- [x] 2d: Integration tests gated behind `MOONCAKE_S3_TEST_BUCKET` env var

### New Files
- `crates/mooncake-store-client/src/remote_source/s3_source.rs` — S3RemoteSource + tests
- `crates/mooncake-store-client/tests/test_s3_source.rs` — S3 integration tests (ignored by default)

### Key design
- Feature-gated behind `s3` feature (`aws-sdk-s3`, `aws-config`, `bytes`)
- `S3RemoteSource::new(config: &S3Config)` — Async constructor loading AWS SDK config
- Credential chain: explicit keys → env vars → IAM/IMDS/`~/.aws/`
- `prefetch_keys()`: parallel GetObject via JoinSet, max 8 concurrent
- `list_neighbor_keys()`: ListObjectsV2 with prefix + client-side proximity sort
- Timeout: per-request tokio::time::timeout(default 30s)

## Phase 3: Advanced Features ✅ DONE

- [x] 3a: Hot cache integration — `get()` 三层查找: hot_cache → RDMA → RemoteSource
- [x] 3b: Metrics — `MissHandlerSnapshot`: miss_rate, avg_fetch_latency_us, cache_hits, bytes_transferred
- [x] 3c: Mock S3 tests — in-process mock S3 server (axum) — 6 end-to-end tests, zero external deps

### get() flow (final)
```
Level 0: LocalHotCache::get(key) → hit → return (fastest, no network)
Level 1: fetch_replicas → select_best_replica → RDMA read → store in hot_cache
Level 2: MissHandler::handle_miss(key)
  ├─ hot_cache check (dedup)
  ├─ Semaphore admission
  ├─ RemoteSource::get(key)  [S3 / LocalFS]
  ├─ store result + prefetched neighbors in hot_cache
  └─ MissHandlerSnapshot updated atomically
```

### New / Modified Files
- `crates/mooncake-store-client/src/remote_source/miss_handler.rs` — hot cache + atomic metrics
- `crates/mooncake-store-client/src/remote_source/s3_source.rs` — force_path_style fix
- `crates/mooncake-store-client/src/client/mod.rs` — hot_cache field + with_hot_cache()
- `crates/mooncake-store-client/src/client/read.rs` — 3-level get() path
- `crates/mooncake-store-client/src/lib.rs` — MissHandlerSnapshot export
- `crates/mooncake-store-client/tests/test_s3_source.rs` — 6 mock S3 e2e tests
- `crates/mooncake-store-client/Cargo.toml` — axum dev-dep, force_path_style S3 config

## Test Results
- All tests pass (0 failures)
- cargo check: 0 errors, 0 warnings (default + `--features s3`)
- cargo test: all pass including 6 mock S3 tests
