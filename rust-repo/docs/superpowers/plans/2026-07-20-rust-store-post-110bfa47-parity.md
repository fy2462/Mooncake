# Rust Store Post-110bfa47 Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Audit every upstream commit after `110bfa47aabc713ef1cdf` and give `rust-repo` the same applicable Store capabilities across master, client, Python, and Transfer Engine integration.

**Architecture:** Preserve the existing Rust master/client boundaries and port behavior rather than C++ class structure. Land independent compatibility batches in dependency order, with an upstream-commit matrix and a dated migration log for every batch.

**Tech Stack:** Rust, Tokio, Tonic, Axum, serde/rmp-serde, libc, transfer-engine-ffi, PyO3, Cargo tests.

## Global Constraints

- Audit range is `110bfa47aabc713ef1cdf..38c5d726` (82 upstream commits). The later `7e39ebf1` is a Rust migration result, not an upstream input.
- Every upstream commit must have exactly one recorded disposition, including Store, Transfer Engine/TENT, wheel/Python, build, CI, documentation, benchmark, EP, and p2p changes.
- Runtime behavior and public configuration are parity requirements; C++-only refactors, build-warning cleanup, and benchmarks are recorded but do not require a Rust port.
- Port observable behavior, state transitions, error semantics, and compatibility—not C++ API shapes. New code must use idiomatic Rust ownership, value returns, enums and pattern matching, `Result`, iterators, and RAII; avoid output parameters, sentinel states, manual resource management, and pointer-shaped interfaces unless required at an FFI boundary.
- Preserve backward-compatible defaults and serialized formats unless the audited upstream commit intentionally changes them.
- Use red/green TDD for every behavior change and run the affected crate's complete test suite before recording a batch as complete.
- Keep implementation commits scoped; plan documents live under
  `rust-repo/docs/superpowers/` and belong only in documentation checkpoints.

---

## Audit Matrix

### Direct `mooncake-store` changes

| Upstream | Change | Rust status | Action |
|---|---|---|---|
| `b590fa29` | S3 list pagination | Equivalent | Rust loops over `next_continuation_token` in `ha/snapshot.rs`; retain catalog coverage. |
| `6dbb5e39` | RFC #1527 KV event publisher | Equivalent | Rust has configuration, ZMQ publisher, lifecycle publishing, status endpoint, and tests. |
| `d224021c` | Accept `host:port` for client data-plane host | Equivalent | Rust client parses the endpoint and separates host/port during lifecycle setup. |
| `2f2f7abb` | C++ warning cleanup | Not applicable | No runtime capability to port. |
| `74af2a5b` | Snapshot manager extraction | Equivalent architecture | Rust already separates service, snapshot provider/catalog, codec, and restore orchestration. |
| `49c9081a` | Snapshot codec extraction | Equivalent architecture | No additional wire behavior beyond the Rust snapshot modules. |
| `711fac99` | Layered snapshot restore | Equivalent behavior | Rust catalog restore supports candidates/fallback and state application. |
| `cc745659` | Snapshot cleanup/documentation | Not a new capability | Keep the current Rust layering; no class-for-class rewrite. |
| `70ba3158` | Parallel HugeTLB population | Migrated | Deferred bounded parallel page touching before TE registration landed in `91a90098`; real HugeTLB + RDMA registration remains environment-gated. |
| `12304b92` | Client metrics HTTP config exposed to Python | Migrated | Rust client owns optional health/Prometheus endpoints; PyO3 exposes appended configuration arguments. |
| `b996ac4b` | Opt-in topology-aware remote replica scoring | Migrated | Rust has opt-in scoring with protocol propagation and verification logs `2026-07-21-009..011`. |
| `a5b938cd` | SSD publish-before-commit race | Migrated | Covered by migration log `2026-07-20-001`. |
| `63bcc646` | FIFO eviction for OffsetAllocator backend | Migrated | Client SSD OffsetAllocator has quota/key watermarks, FIFO eviction, persistence, and extent reuse; commit `eb4eb1eb`. |
| `f6cc5625` | Master default tuning | Migrated | Rust defaults now match upstream; commit `c7c52fd9`. |
| `f7299ff3` | Batch-evict benchmark knobs | Benchmark-only | No runtime port; add a Rust benchmark only if benchmark parity becomes a separate goal. |
| `ea5fd923` | GPU-address-aware local copies | Migrated | Pointer classification and registered-host staging landed in `eff1b3ee`; a real CUDA smoke test remains environment-gated. |
| `f5bc1c15` | Proactive disk watermark eviction | Migrated | Covered by migration log `2026-07-20-001`. |
| `52dfb1c6` | SPDK DMA buffer teardown | Not applicable today | Rust client does not allocate its client buffer with `spdk_zmalloc`; reassess if that allocation path is added. |
| `7e39f640` | Surface metadata HTTP bind failure | Migrated | Rust binds before spawning and propagates startup errors; commit `c7c52fd9`. |
| `3fa5ccd4` | Roll back failed OffsetAllocator eviction | Migrated | Prepare leaves victims readable and live; notification failure drops the selection, while post-notification failures keep victims committed; commit `eb4eb1eb`. |
| `525c7305` | Persist SSD capacity/local-disk snapshot state | Migrated | Snapshot state and compatibility coverage landed in `007c5b8f`. |

### Transfer Engine and TENT changes

Rust links the repository's native `libtransfer_engine` through `transfer-engine-ffi`. Internal transport correctness and reliability fixes are inherited when that native library is rebuilt; new opt-in request fields or public tools are not automatically exposed through the older C ABI.

| Upstream | Change | Rust status | Action |
|---|---|---|---|
| `4d1116ad` | Deadline-infeasible drop/degradation | Migrated | Versioned TENT request/configuration bindings landed in `75d311b7`. |
| `74a52a1f` | Cross-NUMA same-device rail mapping | Inherited native behavior | Rebuild/link current native TE; add no duplicate Rust algorithm. |
| `ab89448f` | Per-entry priority promotion | Migrated | Rust and Python priority controls landed in `75d311b7`. |
| `88be9e92` | Expose deadline and policy to Python | Migrated | Python request deadline and policy controls landed in `75d311b7`. |
| `469a85ce` | TPU staging corruption fix | Inherited native behavior | Native TE regression suite is the acceptance gate. |
| `9cd1277a` | Transfer intent enum | Migrated | The versioned C ABI plus Rust/Python intent enums landed in `75d311b7`. |
| `cc9e5250` | Deadline-proximity admission promotion | Migrated | Absolute deadline and policy controls landed in `75d311b7`. |
| `f1cf2e06` | `show-link` diagnostic tool | Tooling gap, not Store runtime | Expose/port only as the diagnostic subtask. |
| `7423c7c7` | TE signal shutdown | Inherited by native executable, library teardown already explicit | Verify Rust owns and destroys the engine cleanly. |
| `5e955b95` | Batch memory-registration validation | Inherited through called C APIs | Add Rust negative-input wrapper tests. |
| `0e118c5f` | Per-instance thread-local storage reclamation | Inherited native behavior | Native TE lifetime stress test. |
| `178d01e0` | IBGDA host-control fallback | Inherited native behavior | Hardware-gated native verification. |
| `b99167a3` | Acknowledged TCP completion framing | Inherited native behavior | Rust TCP integration test must observe completion semantics. |
| `cd60091f` | Reserve in-flight registrations | Inherited native behavior | Native concurrency regression test. |
| `3649d368` | MACA P2P optimization | Inherited native behavior | Hardware-gated verification only. |
| `725e9c54` | RDMA rail failure handling/diagnostics | Inherited native behavior | Hardware-gated failover verification. |
| `2b5a1f92` | Best-effort RDMA cancellation | Migrated | Rust/Python cancellation through the existing native API landed in `75d311b7`. |
| `5fccdc9e` | Gate sends until QP confirmation | Inherited native behavior | RDMA regression smoke. |
| `6aa0ae65` | Causal latency stage metrics | Migrated | TENT configuration and metrics visibility landed in `75d311b7`. |
| `3a962002` | Segment-cache metadata refresh polling | Inherited native behavior | Rust uses the same native segment cache. |
| `6a4627f5` | Bind policies to intent type | Migrated | Intent and generic TENT configuration controls landed in `75d311b7`. |
| `e68b30d0` | Deadline-aware NIC bandwidth arbitration | Migrated | Transport, deadline, and generic TENT configuration controls landed in `75d311b7`. |
| `d687d670` | Refresh RDMA metadata on HCA/GID changes | Inherited native behavior | Hardware-gated refresh test. |
| `1fadb9ca` | Reuse/release SHM relocation mappings | Inherited native behavior | Native lifetime tests. |
| `407ccc47` | Reject empty RDMA completion resources | Inherited through install/setup | Rust must propagate the native setup error. |
| `6d92ac1c` | Feed live bandwidth into admission degradation | Migrated | Generic TENT configuration overrides landed in `75d311b7`. |
| `b8e63a63` | ARM64 atomic portability | Inherited native compile fix | Validate in aarch64 CI, no Rust port. |
| `5295215f` | Reject unsupported NVMe-oF batches/correlate completions | Inherited native behavior | NVMe-oF native/Rust integration test. |
| `85e724f8` | Receiver-credit ledger/invariants | Inherited native behavior | Native protocol tests. |
| `cdba6f8a` | TENT QoS metrics baseline | Migrated | Rust/Python metrics status and TENT configuration bindings landed in `75d311b7`. |
| `74f7177c` | Correct GDS batch-status semantics | Inherited native behavior | Hardware-gated GDS verification. |
| `f8d363ff` | Share dma-buf fd across NICs | Inherited native behavior | Hardware-gated registration test. |
| `6223076c` | QoS contract schema resolver | Migrated | Native-owned QoS contract selection is reachable through configuration overrides from `75d311b7`. |
| `87716175` | Surface terminal NVMe-oF slice failures | Inherited status through C API | Verify Rust maps failed terminal status. |
| `49fc6f50` | QP teardown before MR deregistration | Inherited native behavior | RDMA shutdown smoke. |
| `4915a2c5` | Propagate batch memory-operation errors | Inherited through batch C APIs | Add wrapper error-propagation tests. |
| `dd2f1d33` | Pause reconnects to failed peers | Inherited native behavior | RDMA failure smoke. |
| `e18f70ef` | Roll back partial local registration | Inherited through C APIs | Add Rust partial-registration failure test. |
| `4fe38289` | MNNVL Device API support | Native implementation present, Rust exposure incomplete | Confirm the C install path can select MNNVL; extend FFI only if required. |
| `c812aa8c` | RDMA NIC failover recovery | Inherited native behavior | Hardware-gated failover smoke. |
| `8009138b` | TENT metrics bind failure reporting | Migrated | Bound-port versus unavailable/log-only status is exposed by `75d311b7`. |

### Wheel, Python service, p2p, EP, build, CI, docs, and benchmarks

| Upstream | Change | Rust status | Action |
|---|---|---|---|
| `db5a86f8` | SSD benchmark replay/multithreading | Benchmark-only | Record as non-runtime; optional Rust benchmark project. |
| `b2cc26f5` | Go p2p `x/net` bump | Different implementation/dependency graph | No Rust port; audit Rust dependency advisories separately. |
| `33016145` | Reject blank keys in Python HTTP metadata server | Migrated | Rust-wheel-owned service validation landed in `402f66ee`. |
| `fab88a1e` | vLLM benchmark documentation | Documentation-only | No runtime port. |
| `4c41eace` | Go `x/crypto` bump | No Rust dependency equivalent | No port. |
| `fbebc515` | Performance docs reorganization | Documentation-only | No runtime port. |
| `87142bf4` | Avoid redundant `chmod` in wheel loader | Rust extension packaging gap only if loader is retained | Verify Rust wheel loader behavior; do not duplicate obsolete loader code. |
| `5bb22277` | CUDA 13 tone-test CI | CI-only | Add Rust CUDA 13 coverage when Rust wheel CI is in scope. |
| `1c6acb09` | EP active-RoCE-QP cap | EP subsystem, not Store | No Store port. |
| `d265dcc2` | DeepEP V2 elastic buffer | EP subsystem, not Store | No Store port. |
| `093149cc` | Kubernetes deployment guide | Documentation-only | Check Rust binary flags against examples during release docs work. |
| `82d16086` | README news | Documentation-only | No runtime port. |
| `7da9a730` | MUSA EP/PG image | EP/build-only | No Store port. |
| `6aa80e25` | Graceful Store REST SIGTERM | Migrated | Signal-driven shutdown and exactly-once close landed in `402f66ee`. |
| `6b9a221a` | SGLang PD benchmark docs | Documentation-only | No runtime port. |
| `53bd12de` | aarch64 wheel build | CI/package coverage | Add Rust wheel aarch64 job when publishing Rust wheels. |
| `7889a143` | Pre-release artifact naming | CI-only | Apply only to Rust release workflow artifacts. |
| `10422eb2` | Pre-release wheel build fix | CI/package-only | Audit Rust wheel workflow before release. |
| `30167b10` | Ubuntu wheel-test flake fix | CI-only | No runtime port; reuse test fix if Rust wheel workflow has same failure. |
| `38c5d726` | llm-d Kubernetes integration docs | Documentation-only | No runtime port. |

The audit intentionally excludes `7e39ebf1` from upstream input: it is the first Rust migration output in this branch.

## File Map

- `rust-repo/crates/mooncake-store-master/src/main_args.rs`: public master defaults.
- `rust-repo/crates/mooncake-store-master/src/http_metadata.rs`: fallible HTTP listener creation and serving.
- `rust-repo/crates/mooncake-store-master/src/main_server.rs`: startup ordering and error propagation.
- `rust-repo/crates/mooncake-store-client/src/local_storage_backend/config.rs`: client SSD OffsetAllocator quota and FIFO configuration.
- `rust-repo/crates/mooncake-store-client/src/local_storage_backend/offset.rs`: persisted index, reusable extents, and transactional FIFO eviction.
- `rust-repo/crates/mooncake-store-client/src/local_storage_backend/mod.rs`: idiomatic enum dispatch across file-per-key and OffsetAllocator backends.
- `rust-repo/crates/mooncake-store-client/src/client/transfer_meta.rs`: topology-aware replica scoring.
- `rust-repo/crates/mooncake-store-client/src/client/mod.rs`: client configuration and HTTP task ownership.
- `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs`: client HTTP startup and HugeTLB population before registration.
- `rust-repo/crates/mooncake-store-client/src/client/buffer.rs`: deferred/parallel HugeTLB page population.
- `rust-repo/crates/mooncake-store-client/src/client/transfer.rs`: device-aware staging for pointer-based transfers.
- `rust-repo/python/src/client.rs`: Python-visible client HTTP configuration.

### Task 1: Checkpoint local-disk snapshot persistence

**Files:**
- Modify: the eight currently changed `mooncake-store-master` source/test files
- Create: `rust-repo/change_logs/2026-07-20-002.md`

**Interfaces:**
- Consumes: current worktree implementation for `525c7305`.
- Produces: a clean, reviewable baseline for the remaining parity batches.

- [x] **Step 1: Review only the intended snapshot diff**

Run: `git diff -- rust-repo/crates/mooncake-store-master rust-repo/change_logs/2026-07-20-002.md`

Expected: local-disk snapshot fields, standalone/HA restore, compatibility tests, and the migration log only.

- [x] **Step 2: Re-run the final package verification**

Run: `cd rust-repo && CARGO_BUILD_JOBS=1 cargo test -p mooncake-store-master --no-fail-fast && cargo fmt --all -- --check`

Expected: all master tests pass and rustfmt reports no diff.

- [x] **Step 3: Commit the checkpoint**

```bash
git add rust-repo/crates/mooncake-store-master rust-repo/change_logs/2026-07-20-002.md
git commit -m "fix(store-rust): persist local disk state in snapshots"
```

### Task 2: Align master defaults and surface metadata bind failures

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/main_args.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/http_metadata.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/main_server.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_main_config.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_http_metadata.rs`
- Create: next dated file under `rust-repo/change_logs/`

**Interfaces:**
- Produces: `bind_metadata_listener(addr) -> std::io::Result<tokio::net::TcpListener>` and `serve_metadata_listener(listener, state) -> std::io::Result<()>`.

- [x] **Step 1: Add failing default-value assertions**

Assert that `Args::parse_from(["mooncake-master"])` yields `rpc_thread_num == 16`, `default_kv_lease_ttl_ms == 10_000`, and `eviction_high_watermark_ratio == 0.90`.

- [x] **Step 2: Add a failing occupied-port startup test**

Bind a `TcpListener` on `127.0.0.1:0`, then call `bind_metadata_listener` with its address and assert `ErrorKind::AddrInUse`.

- [x] **Step 3: Verify both tests fail for the audited reasons**

Run: `cargo test -p mooncake-store-master --test test_main_config --test test_http_metadata -- --nocapture`

Expected: old defaults and detached bind behavior fail the new assertions.

- [x] **Step 4: Apply the upstream defaults and bind before spawning**

Use these exact clap defaults:

```rust
#[arg(long, default_value_t = 16)]
pub rpc_thread_num: usize,
#[arg(long, default_value_t = 10_000)]
pub default_kv_lease_ttl_ms: u64,
#[arg(long, default_value_t = 0.90)]
pub eviction_high_watermark_ratio: f64,
```

Construct the metadata listener synchronously in both standalone and HA startup paths; only spawn Axum serving after binding succeeds.

- [x] **Step 5: Run focused and complete master tests, write the migration log, and commit**

Run: `CARGO_BUILD_JOBS=1 cargo test -p mooncake-store-master --no-fail-fast`

Commit: `fix(store-rust): align master defaults and report metadata bind errors`

### Task 3: Add transactional FIFO eviction to OffsetAllocator

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/local_storage_backend/config.rs`
- Create: `rust-repo/crates/mooncake-store-client/src/local_storage_backend/offset.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/local_storage_backend/mod.rs`
- Modify: client offload/promotion/remove integration under `rust-repo/crates/mooncake-store-client/src/client/`
- Test: `rust-repo/crates/mooncake-store-client/tests/test_local_storage_offset.rs`
- Create: next dated migration log.

**Interfaces:**
- Produces: persisted `fifo_seq` per offset entry, `PendingOffsetEviction`, prepare/commit/rollback operations, and an eviction callback that returns `Result`.

- [x] **Step 1: Add failing FIFO capacity tests**

Create a small-quota OffsetAllocator, write A/B, then write C and assert the oldest eligible object is selected while batch keys are excluded.

- [x] **Step 2: Add the failed-notification rollback regression**

Make the eviction callback return an error and assert the victim remains readable, its FIFO sequence is unchanged, and its extent is not reused.

- [x] **Step 3: Verify failures against the append-only implementation**

Run: `cargo test -p mooncake-store-client --test test_local_storage_offset -- --nocapture`

Expected: tests fail because the client has no quota-aware OffsetAllocator backend or eviction transaction.

- [x] **Step 4: Implement persisted FIFO metadata compatibly**

Add `#[serde(default)] fifo_seq: u64` to `OffsetIndexEntry` and `#[serde(default)] next_fifo_seq: u64` to `OffsetAllocatorIndex`. When loading an old index, deterministically assign missing sequences by offset order before the next write.

- [x] **Step 5: Implement prepare, notify, commit, and rollback**

`PendingOffsetEviction` must retain the removed key/index entries until notification succeeds. On error, restore entries and FIFO ordering; only successful notification makes extents reusable.

- [x] **Step 6: Run the full client suite, log both upstream commits, and commit**

Run: `cargo test -p mooncake-store-client --test test_local_storage_offset -- --nocapture && CARGO_BUILD_JOBS=1 cargo test -p mooncake-store-client --no-fail-fast`

Commit: `fix(store-rust): make offset allocator eviction transactional`

### Task 4: Add opt-in topology-aware remote replica scoring

**Prerequisite:** Complete the behavior-preserving Rust-idiomatic refactor in
`2026-07-21-rust-idiomatic-refactor-before-replica-scoring.md`. Do not mix the
refactor and `b996ac4b` behavior change in one commit.

**Files:**
- Modify: `rust-repo/proto/mooncake_store_types.proto`
- Modify: `rust-repo/crates/mooncake-store-core/src/types.rs`
- Modify: master allocation/proto-conversion paths that propagate segment protocol
- Modify: `rust-repo/crates/mooncake-store-client/src/client/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/transfer_meta.rs`
- Test: `rust-repo/crates/mooncake-store-client/src/client/tests.rs`
- Create: next dated migration log.

**Interfaces:**
- Produces: an opt-in selection policy that scores complete remote memory replicas and falls back to current deterministic ordering when topology data is absent or disabled.

- [x] **Step 1: Port the upstream scoring cases as table-driven failing tests**

Cover disabled policy, local replica dominance, injected lower score, built-in `rdma < tcp < unknown` ordering, unavailable protocol fallback, incomplete replica exclusion, and stable tie-breaking.

- [x] **Step 2: Run the focused tests and confirm locality-only selection fails cost ordering**

Run: `cargo test -p mooncake-store-client client::tests::topology -- --nocapture`

- [x] **Step 3: Add the minimal policy/configuration and scoring adapter**

Carry segment protocol additively in the proto/domain model with backward-compatible defaults. Keep local Memory and local NoF short-circuits. When enabled, rank remote complete Memory candidates with a client-owned scorer or the built-in protocol priority; preserve first-seen order for equal or unavailable scores. Do not copy the C++ process-global mutable scorer into Rust.

- [x] **Step 4: Run the full client suite, log, and commit**

Run: `cargo test -p mooncake-store-client --no-fail-fast`

Commit: `feat(store-rust): score remote replicas by topology`

### Task 5: Expose optional client health/metrics HTTP service to Python

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/Cargo.toml`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs`
- Create: `rust-repo/crates/mooncake-store-client/src/client/http.rs`
- Modify: `rust-repo/python/src/client.rs`
- Test: `rust-repo/crates/mooncake-store-client/tests/test_client_http.rs`
- Create: next dated migration log.

**Interfaces:**
- Produces: `enable_client_http_server: bool` and `client_http_port: u16` with default port `9300`, plus `/health` and `/metrics` endpoints.

- [x] **Step 1: Add failing configuration and endpoint tests**

Verify disabled-by-default behavior, explicit port selection, invalid/occupied port behavior matching upstream (client setup continues without endpoints on bind failure), health response, and Prometheus metrics response.

- [x] **Step 2: Add an owned HTTP task to the client lifecycle**

Start it only when enabled, retain its shutdown sender/task handle, and stop it during client teardown. A duplicate start must not create a second listener.

- [x] **Step 3: Extend PyO3 setup without breaking positional callers**

Append keyword arguments `enable_client_http_server=false` and `client_http_port=9300`; do not reorder existing parameters.

- [x] **Step 4: Run Rust and Python binding tests, log, and commit**

Run: `cargo test -p mooncake-store-client --no-fail-fast && cargo test -p mooncake-store-python --no-fail-fast`

Commit: `feat(store-rust): expose client metrics HTTP configuration`

### Task 6: Make pointer-based local copies accelerator-aware

**Files:**
- Modify: `rust-repo/crates/transfer-engine-ffi/src/lib.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/transfer.rs`
- Modify: `rust-repo/python/src/client.rs`
- Test: `rust-repo/crates/mooncake-store-client/tests/test_gpu_copy.rs`
- Create: next dated migration log.

**Interfaces:**
- Produces: pointer classification plus device-to-host and host-to-device copy wrappers; Rust slice APIs remain host-only and safe.

- [ ] **Step 1: Add mock-accelerator failing tests**

Cover GPU-source put/upsert, GPU-destination full/ranged get, host-pointer fast paths, copy failure propagation, and temporary-buffer allocation failure.

- [ ] **Step 2: Expose only the native accelerator primitives required by Store**

Wrap pointer lookup and directional copies in `transfer-engine-ffi`; return typed errors and never construct a Rust slice directly over a device pointer.

- [ ] **Step 3: Stage device-addressed operations through registered host buffers**

For writes, copy device-to-host before transfer. For reads, transfer into a host buffer and copy host-to-device afterward. Preserve zero-copy behavior for host memory.

- [ ] **Step 4: Run CPU tests and a CUDA-gated smoke test, log, and commit**

Run: `cargo test -p mooncake-store-client --no-fail-fast`; on a CUDA host also run the ignored GPU pointer test explicitly.

Commit: `fix(store-rust): handle accelerator pointers in local copies`

### Task 7: Parallelize HugeTLB population before registration

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/client/buffer.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs`
- Test: `rust-repo/crates/mooncake-store-client/tests/test_hugepage_buffer.rs`
- Create: next dated migration log.

**Interfaces:**
- Produces: deferred HugeTLB allocation and `populate_hugetlb_pages(ptr, len, page_size, workers)` invoked immediately before TE registration.

- [ ] **Step 1: Add page-range and fallback unit tests**

Verify each huge page is touched exactly once, worker ranges cover the mapping without overlap, zero-sized inputs are rejected, and failed HugeTLB allocation still falls back to the existing buffer path.

- [ ] **Step 2: Implement bounded parallel page touching**

Use `std::thread::available_parallelism()` capped by page count. Each worker writes the first byte of its assigned pages; join all workers before memory registration.

- [ ] **Step 3: Preserve NUMA semantics**

Do not move page touching ahead of NUMA binding. If the Rust allocation path lacks per-region NUMA metadata, keep the optimization disabled for that path rather than populating pages on the wrong node.

- [ ] **Step 4: Run unit tests plus a HugeTLB-enabled smoke test, log, and commit**

Run: `cargo test -p mooncake-store-client --no-fail-fast`; where huge pages are configured, run the ignored allocation/registration smoke test explicitly.

Commit: `perf(store-rust): populate huge pages before registration`

### Task 8: Expose applicable TENT intent, QoS, and status controls

**Files:**
- Modify: `mooncake-transfer-engine/include/transfer_engine_c.h`
- Modify: the C adapter implementing `submitTransfer` beside the existing C API
- Modify: `rust-repo/crates/transfer-engine-ffi/build.rs`
- Modify: `rust-repo/crates/transfer-engine-ffi/src/lib.rs`
- Modify: `rust-repo/python/src/transfer_engine.rs`
- Test: `rust-repo/crates/transfer-engine-ffi/tests/`
- Create: next dated migration log.

**Interfaces:**
- Produces: a versioned C request extension carrying intent type, deadline, policy name, and priority without changing the layout of legacy `transfer_request_t`.
- Produces: Rust enums/options for intent submission, best-effort cancellation where supported, and TENT metrics bound-port/log-only status.

- [ ] **Step 1: Add ABI-layout and legacy-submission tests**

Assert the old `transfer_request_t` size/layout and `submitTransfer` behavior remain unchanged. Add a new `transfer_request_v2_t` or options entry point rather than appending fields to the legacy struct.

- [ ] **Step 2: Add failing Rust request-option tests**

Cover intent enum round trips, absolute `deadline_ns`, policy-name lifetime, per-entry priority, disabled-TENT fallback, cancellation result mapping, and metrics HTTP unavailable status.

- [ ] **Step 3: Implement the versioned C adapter and bind it explicitly**

Allowlist only the new stable C symbols in `build.rs`. Convert Rust-owned strings to `CString` for the duration of submission and reject interior NULs before entering native code.

- [ ] **Step 4: Add backward-compatible Rust and Python APIs**

Keep existing submit methods unchanged. Add option-bearing variants whose defaults produce the legacy request path, and expose the same keyword-only fields in PyO3.

- [ ] **Step 5: Verify native and Rust suites, log, and commit**

Run the relevant native TENT tests, then `cargo test -p transfer-engine-ffi --no-fail-fast` and the Python binding tests.

Commit: `feat(store-rust): expose transfer intent and qos controls`

### Task 9: Align Rust-backed Python service lifecycle behavior

**Files:**
- Inspect/modify: Rust wheel/package service entry point selected for distribution
- Modify: `rust-repo/python/` service wrapper modules and packaging metadata
- Test: Rust Python service tests under `rust-repo/python/tests/`
- Create: next dated migration log.

**Interfaces:**
- Produces: REST service shutdown on SIGINT/SIGTERM and blank metadata-key rejection where the Rust distribution exposes the bootstrap metadata service.

- [ ] **Step 1: Prove which service surface the Rust wheel ships**

Build the Rust wheel and inspect its installed console scripts/modules. If it intentionally reuses `mooncake-wheel/mooncake/mooncake_store_service.py`, record that the two upstream fixes are inherited and test that exact installed artifact; otherwise continue with the Rust-owned wrapper below.

- [ ] **Step 2: Add failing lifecycle and validation tests**

Assert SIGTERM interrupts startup retry, stops an initialized store exactly once, exits the HTTP loop, and rejects missing/empty/whitespace-only metadata keys with HTTP 400 while trimming valid keys.

- [ ] **Step 3: Implement signal-driven shutdown and key validation**

Use one asyncio shutdown event, install SIGINT/SIGTERM handlers, check it between synchronous setup retries, and close the store in `finally`. Normalize the metadata key with `strip()` before dispatch.

- [ ] **Step 4: Run installed-wheel tests, log, and commit**

Run the Rust wheel build and its service tests in a clean virtual environment.

Commit: `fix(store-rust): align python service shutdown and metadata validation`

### Task 10: Final parity audit and verification

**Files:**
- Modify: this plan's audit matrix statuses.
- Create: final dated migration summary under `rust-repo/change_logs/`.

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: evidence that every audited commit has a final disposition.

- [x] **Step 1: Re-enumerate the source range**

Run: `git log --format='%H %s' 110bfa47aabc713ef1cdf..38c5d726`

Expected: every resulting commit appears exactly once in the audit matrix.

- [x] **Step 2: Run repository formatting and package suites**

Run: `cd rust-repo && cargo fmt --all -- --check && cargo test -p mooncake-store-master --no-fail-fast && cargo test -p mooncake-store-client --no-fail-fast`

- [x] **Step 3: Run lint and available pre-commit checks**

Run: `cd rust-repo && cargo clippy -p mooncake-store-master -p mooncake-store-client --all-targets`; from repository root run pre-commit on touched files if installed.

- [x] **Step 4: Review scope and record environment-gated checks**

Run: `git diff --check && git status --short`. Record CUDA/HugeTLB tests as passed, failed, or unavailable; never silently treat unavailable hardware as passing.

- [x] **Step 5: Commit the audit summary**

Commit: `docs(store-rust): record post-baseline parity audit`
