# Rust Store Migration Scope and Dependency Boundary

## Objective

`rust-repo` is a functional replacement for the C++ `mooncake-store`. The end
state is a Rust master, client, service, persistence, HA, and Python-facing
Store implementation that can be built and operated without compiling or
linking the C++ Store library.

This is a behavior and compatibility migration, not a class-for-class source
translation. Preserve observable behavior, protocols, serialized formats,
configuration, state transitions, and error semantics while preferring
idiomatic Rust architecture.

## Native dependency boundary

The expected native dependency is the repository's Transfer Engine:

```text
Rust Store -> transfer-engine-ffi -> libtransfer_engine.so
                                  -> libtent_shared.so (only when enabled)
```

The C++ `mooncake-store` is outside this dependency graph. In particular,
`libmooncake_store.so` and Store implementation files such as
`mooncake-store/src/spdk/spdk_wrapper.cpp` must not become prerequisites for
building or testing the Rust Store.

The boundary is based on ownership:

- Store control plane, metadata, allocation, persistence, HA, lifecycle,
  configuration, and user-facing APIs belong in `rust-repo`.
- Data movement and transport internals belong in `mooncake-transfer-engine`.
- Rust calls native Transfer Engine behavior through a narrow, explicit C
  ABI in `transfer-engine-ffi`.
- C++ Store code is a reference for parity, not a reusable implementation
  dependency.

## How to handle a missing capability

When an audit or test finds behavior present in the C++ Store but absent from
the Rust Store:

1. Identify the observable contract rather than copying the C++ class shape.
2. If it is Store behavior, implement it in `rust-repo` and add Rust tests.
3. If it is a transport/data-plane primitive already implemented by Transfer
   Engine, expose the smallest required C ABI and safe Rust FFI wrapper.
4. If it is missing from Transfer Engine itself and genuinely belongs there,
   implement it in Transfer Engine, expose it through the C ABI, and test both
   the native and Rust boundaries.
5. Do not solve the gap by linking the Rust Store to `libmooncake_store.so` or
   calling into C++ Store implementation files.

“Inherited from Transfer Engine” is valid only when the Rust execution path
actually reaches the current native implementation through the C ABI. New
native options, request fields, status values, or tools require explicit FFI
exposure before they count as Rust parity.

## NoF and SPDK ownership

The old C++ Store implements Store-side NoF support with
`spdk_wrapper.cpp` and the top-level CMake `USE_NOF` option. That build path
does not validate the Rust replacement.

The Rust implementation has its own NoF control-plane model, registration,
allocation, replica selection, status handling, and master probing. The Rust
master's optional direct SPDK probe is selected with the `spdk-nof-probe`
Cargo feature and uses OpenEBS `spdk-rs` v2.11.0 with its pinned OpenEBS SPDK
25.05 and DPDK 25.03.0 SDK. NoF data transfers exposed by
the native Transfer Engine are consumed through `transfer-engine-ffi`.

Therefore:

- Compile-check Rust SPDK support with the Rust master
  `spdk-nof-probe` feature.
- Test NoF registration, allocation, selection, and error/status behavior in
  Rust tests.
- Build the required Transfer Engine shared library for native NoF data-plane
  integration.
- Do not require the C++ Store `USE_NOF=ON` build or
  `spdk_wrapper.cpp` for Rust acceptance.
- Skip a real NVMe-oF end-to-end test only when its actual target/device or
  system prerequisite is unavailable; record the precise reason.

## Default verification matrix

For Rust Store parity work, the normal acceptance matrix is:

1. Build `libtransfer_engine.so` with the required CPU, TCP, HTTP metadata,
   RDMA, or other transport options.
2. Build `libtent_shared.so` only when the selected Rust features use TENT.
3. Run the full Rust workspace tests, plus focused feature tests.
4. Run Open RDMA mock tests when RDMA behavior is in scope; run hardware tests
   only where the device and privileges exist.
5. Compile and test Rust optional integrations such as
   `mooncake-store-master --features spdk-nof-probe`.
6. Run real hardware/service end-to-end tests when their prerequisites exist,
   otherwise report them as environment-gated with exact evidence.

Building the C++ `mooncake-store` is not part of this matrix unless a task
explicitly asks to test the legacy C++ implementation itself.

## Session checklist

At the start of future migration sessions:

- Confirm that proposed work advances the Rust replacement rather than
  repairing or depending on the legacy C++ Store.
- Inspect Rust feature flags and FFI link features before choosing native build
  targets.
- Treat a C++ Store build failure as unrelated unless the task explicitly
  includes legacy C++ validation.
- Put newly discovered Store gaps and their tests in `rust-repo`.
- Keep changes and validation reports explicit about what is Rust-owned,
  Transfer-Engine-inherited, hardware-gated, or still missing.
