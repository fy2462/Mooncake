# Transfer Engine Native Parity Execution Design

## Objective

Complete the semantic review of every in-scope Mooncake Store, Transfer Engine,
TENT, and selected wheel Store/TE reference test, then execute every Rust or
FFI evidence test that this aarch64 host can support. Build local native shared
libraries so `mooncake-store-client` and `transfer-engine-ffi` evidence is no
longer blocked merely because `libtransfer_engine.so` is absent.

The work does not require an overall PASS on hardware-specific behavior that
this host cannot represent. It does require every such limitation to remain an
explicit, evidence-backed `blocked` disposition rather than being skipped or
reported as passing.

## Scope and Boundaries

The authoritative review suites are:

- 1,399 C++ Store test identities;
- 348 C++ classic Transfer Engine test identities;
- 352 C++ TENT test identities; and
- 352 selected `mooncake-wheel/tests` Store/TE test identities.

C/C++ and wheel sources remain immutable behavioral references. The build may
compile the Transfer Engine implementation into shared libraries, but it must
not modify reference sources or build, link, load, or execute the C++ Store,
classic Transfer Engine, or TENT GoogleTest binaries. Rust Store Client/Core/
Master and `transfer-engine-ffi` remain the executable evidence boundary.

The existing flexible evidence policy remains in force: several Rust tests may
jointly prove one reference behavior, and one Rust test may support several
reference rows. A row is complete only when the combined evidence proves every
observable result asserted by its reference body.

Store LocalDisk `io_uring` performance work remains outside this phase. It may
start only after correctness review and runnable-test execution are complete.

## Build Architecture

Use the current worktree's ignored `build/` directory because
`transfer-engine-ffi/build.rs` already publishes linker search paths for:

- `build/mooncake-transfer-engine/src`;
- `build/mooncake-common/src`; and
- `build/mooncake-transfer-engine/tent/src` when TENT native linking is enabled.

No linker-script or Cargo-source change is needed merely to discover the
libraries. Runtime commands will use a scoped `LD_LIBRARY_PATH` containing the
same directories rather than installing artifacts into `/usr/local`.

The build is deliberately staged.

### Stage 1: Classic Host Transfer Engine

Configure the top-level project with only the components needed to produce a
host shared library:

```text
WITH_TE=ON
WITH_STORE=OFF
WITH_STORE_RUST=OFF
WITH_P2P_STORE=OFF
WITH_EP=OFF
BUILD_SHARED_LIBS=ON
BUILD_UNIT_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARK=OFF
USE_TCP=ON
USE_HTTP=ON
USE_TENT=OFF
```

All vendor accelerator, EFA, CXI, UB, CXL, NVMe-oF, Redis, and etcd options stay
off for this first build. Classic RDMA support remains linked through the
installed ibverbs development package as required by the existing target.

Build only the `transfer_engine` target. Before using it as evidence, verify
that `libtransfer_engine.so` exists, has the host `AArch64` ELF architecture,
exports the C ABI symbols consumed by `transfer-engine-ffi`, and has no missing
dynamic dependencies under the scoped runtime library path.

### Stage 2: Host-Only TENT

After classic native tests have a recorded baseline, reconfigure the same
canonical build directory with `USE_TENT=ON` and build `transfer_engine` plus
`tent_shared`. Keep all unavailable vendor SDK options disabled. TENT may
automatically compile its host transports when their installed dependencies
are found:

- TCP and shared memory;
- ibverbs-backed RDMA; and
- `io_uring` transport support from `liburing`.

This stage builds correctness dependencies only. It does not authorize the
Store LocalDisk `io_uring` optimization.

Validate both shared libraries with the same ELF, symbol, and dependency
checks before enabling Rust's `link-tent-native` feature.

## Dependency and Credential Handling

Inspect installed headers, libraries, CMake packages, and tool versions before
installing anything. Use system package installation only for a concrete
missing build dependency and install the smallest corresponding development
package. Do not add vendor GPU/NPU SDKs or fabricate unavailable hardware.

Administrative credentials must be supplied only to an interactive `sudo`
prompt. They must never be stored in repository files, build scripts, shell
history fragments, environment variables, test artifacts, or reports, and
must never be echoed in command output.

## Test Execution Flow

After Stage 1, execute in increasing scope:

1. exact `transfer-engine-ffi` tests that use classic native linking;
2. exact `mooncake-store-client --features link-native` tests referenced by
   Store and wheel manifests;
3. the owning FFI and Store Client test suites under native linking;
4. all four ordinary manifest validators and validation-tool contract tests;
5. strict validators, whose nonzero result is acceptable only when caused by
   reviewed incomplete rows; and
6. the four-manifest formal parity gate.

After Stage 2, repeat the applicable sequence with `link-tent-native`, including
the TENT FFI evidence commands. Deduplicate shared Rust evidence exactly as the
formal runner does.

Each command must record its exact argv, exit code, executed-test count, and
log. A zero-test Cargo invocation is a failure even when Cargo exits zero.

## Disposition Re-evaluation

Native-library availability invalidates the generic prerequisite currently
attached to blocked `mooncake-store-client` rows. Re-evaluate those rows in
stable manifest order:

- change to `covered` only after all attached tests execute and pass;
- retain `blocked` only for a newly demonstrated external prerequisite such as
  absent device hardware, kernel capability, privilege, or required service;
- change to `missing` when the native test runs but proves that required Rust
  evidence does not exist or is semantically incomplete; and
- preserve `not-applicable` only for reviewed language, build/ABI, excluded
  component, or absent-product-boundary categories allowed by the manifest
  schema.

Any actual Rust/C ABI semantic failure enters the existing Rust TDD remediation
workflow: reproduce with a focused failing test, trace the first C++/Rust
divergence, make the smallest Rust-owned or TE-C-ABI correction, and rerun the
focused, owning, aggregate, and formal gates. C++ reference behavior is never
changed to accommodate Rust.

## Failure Handling

Build configuration, compilation, dynamic loading, and test execution are
separate checkpoints. Preserve the first failure log and diagnose that layer
before changing flags or installing dependencies. Do not hide a missing shared
dependency with broad global linker configuration.

If Stage 2 fails while Stage 1 is valid, keep the classic build and its executed
results. Treat TENT as a separate checkpoint rather than invalidating classic
Store coverage.

Hardware probes are read-only unless an existing approved test explicitly
requires a scoped runtime setup. Lack of a device cannot convert an absent Rust
test into `blocked`; the test must first exist and prove the complete semantic
oracle.

## Verification and Completion Criteria

This review-and-execution phase is complete when:

- all 2,451 manifest identities retain an explicit reviewed disposition;
- every `missing`, `blocked`, and N/A row has a source-verified, non-generic
  reason;
- `libtransfer_engine.so` is reproducibly built and consumed from the local
  canonical build directory;
- `libtent_shared.so` is built and consumed when the host-only TENT build is
  supported by installed dependencies;
- every Rust/FFI evidence command runnable on this host executes at least one
  test and passes;
- no formal gate command reports FAIL or a zero-test false pass;
- remaining blocked rows name only real external prerequisites;
- validation-tool tests, scoped pre-commit, JSON validation, diff checks, and
  staged/unstaged immutable-reference guards pass; and
- the tracked worktree contains only reviewed changes required by parity
  evidence or an explicitly authorized Rust remediation.

The final report must distinguish semantic completion from environmental
coverage. It must list built library paths, build flags, executed PASS counts,
remaining missing/blocked/N/A counts, formal artifact paths, and every external
capability that still prevents a complete correctness PASS.
