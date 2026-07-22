# Rust Store CUDA Host-Pin Parity Implementation Plan

> Execute task by task with red-green tests. Build/cache data belongs under
> `/home/fy2462/workspace/tmp/mooncake`, and Cargo uses five jobs.

## Goal

Give Rust-owned host Store segments the same optional, process-wide,
quota-bounded CUDA host-registration lifecycle as merged C++ commit
`12df58c7`, without linking the Rust Store to the C++ Store and without making
CUDA a default runtime requirement.

## Compatibility decisions

- Parse `MC_STORE_PIN_MEMORY_MAX_BYTES` exactly as the C++ path: unset, empty,
  zero, whitespace-only, or invalid values disable pinning.
- Pin only Store-owned host segment buffers. The scratch local buffer,
  externally supplied mappings, user buffers, file/device segments, and
  staging buffers remain outside this feature.
- Registration failure, overlap, or quota exhaustion falls back to pageable
  memory. Unregistration failure retains both the quota reservation and the
  backing allocation so CUDA cannot retain a dangling mapping.
- CUDA runtime unloading is a successful terminal release.
- The optional `cuda-host-pin` feature resolves CUDA runtime symbols at run
  time; default builds and feature compilation do not require CUDA headers or
  a link-time CUDA installation.
- Rust currently has no Store-owned `allocateAndMountSegment` equivalent.
  The ownership wrapper must be reusable by that API when it is added;
  externally mapped `mount_segment` remains intentionally excluded.

## Task 1: Implement and test quota bookkeeping

**Files:**

- Create `crates/mooncake-store-client/src/pinned_memory.rs`
- Modify `crates/mooncake-store-client/src/lib.rs`

Add an injectable `PinOps` boundary and tests for quota refund, overlap and
duplicate rejection, adjacency, registration failure rollback, normal release,
runtime-unloading release, and unregister-error reservation retention. Use
checked address arithmetic and a mutex-protected process-wide active-region
table.

Commit: `[Store] manage Rust CUDA host pin quota`

## Task 2: Add optional CUDA runtime loading

**Files:**

- Modify `crates/mooncake-store-client/Cargo.toml`
- Modify `crates/mooncake-store-client/src/pinned_memory.rs`
- Test `crates/mooncake-store-client/tests/test_pinned_memory_config.rs`

Add the `cuda-host-pin` feature. Resolve `cudaHostRegister`,
`cudaHostUnregister`, `cudaGetErrorString`, and `cudaGetLastError` from common
`libcudart.so` sonames with `dlopen`/`dlsym` on Linux. Missing runtime or
symbols yields unavailable pin operations and pageable fallback. Unit-test env
parsing without mutating the process environment concurrently; compile both
default and feature configurations.

Commit: `[Store] load optional CUDA host pin runtime`

## Task 3: Couple pins to Store-owned segment memory

**Files:**

- Modify `crates/mooncake-store-client/src/memory_ffi.rs`
- Modify `crates/mooncake-store-client/src/client/mod.rs`
- Modify `crates/mooncake-store-client/src/client/lifecycle.rs`
- Modify `crates/mooncake-store-client/src/client/accessors.rs`
- Test `crates/mooncake-store-client/tests/test_pinned_segment_lifecycle.rs`

Introduce an owned segment wrapper that holds `OwnedBuffer` plus an optional
pin. Its explicit release first completes master/Transfer Engine segment
teardown, then unregisters CUDA, and finally frees memory. If CUDA unregister
fails, move the buffer into `ManuallyDrop` and report the leak instead of
freeing it. Ensure partial create failures obey the same ordering and the
scratch buffer is never pinned.

Commit: `[Store] pin Rust-owned Store segments`

## Task 4: Verify and document the slice

Run:

```text
cargo test -p mooncake-store-client
cargo test -p mooncake-store-client --features cuda-host-pin
cargo check -p mooncake-store-client
cargo check -p mooncake-store-client --features cuda-host-pin
cargo fmt --all -- --check
```

Use `CARGO_BUILD_JOBS=5`, the shared Cargo target, and the shared `TMPDIR` for
all commands. Audit the client crate for C++ Store linkage. Record the results
in the parity design without marking the structured-object or NIC-stat slices
complete.

Commit: `[Store] verify Rust CUDA host pin parity`
