# Task 10 Validation Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every software-only Task 10 validation gate pass from Rust Store through Linux TE/TENT, while reporting real hardware prerequisites as skips.

**Architecture:** Keep Store behavior in Rust and native transfer behavior in TE/TENT. Fix each failure at its owning layer with a focused red-green cycle; external Open-RDMA changes remain isolated in its own repository.

**Tech Stack:** Rust 2024, Cargo, CMake/Ninja/CTest, C++20/GTest, spdk-rs v2.11.0, OpenEBS SPDK 25.05, DPDK 25.03.0, clang-format 20, Open-RDMA Rust driver.

## Global Constraints

- Do not compile, link, or call the C++ Store.
- Preserve unrelated staged, modified, and untracked files.
- Do not count unavailable hardware or services as passing tests.
- Use a clean out-of-tree Linux TE/TENT build with shared libraries.
- Make one root-cause fix at a time and verify its original failure before continuing.

---

### Task 1: Rust formatting and edition-2024 S3 environment test

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_engram.rs`
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_s3_source.rs`

**Interfaces:**
- Consumes: Rust 1.96 rustfmt and edition-2024 `std::env` contract.
- Produces: formatting-clean tests and serialized, restored process-environment mutation.

- [x] **Step 1: Reproduce both failures**

Run from `rust-repo`:

```bash
cargo fmt --all -- --check
cargo test -p mooncake-store-client --features s3 --test test_s3_source --no-fail-fast
```

Expected: rustfmt diff in `test_engram.rs`; E0133 on four environment calls.

- [x] **Step 2: Apply rustfmt and add a scoped environment guard**

Keep `ENV_LOCK` held for the guard lifetime. Use a test-only guard that saves
the prior value, performs edition-2024 unsafe mutation with a safety comment,
and restores it in `Drop`:

```rust
struct ScopedEnvVar {
    name: &'static str,
    old_value: Option<std::ffi::OsString>,
}

impl ScopedEnvVar {
    fn set(name: &'static str, value: &str) -> Self {
        let old_value = std::env::var_os(name);
        // SAFETY: ENV_LOCK serializes every mutation performed by this test module.
        unsafe { std::env::set_var(name, value) };
        Self { name, old_value }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        // SAFETY: the caller still holds ENV_LOCK while guards are dropped.
        unsafe {
            match &self.old_value {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }
}
```

- [x] **Step 3: Verify focused and package tests**

```bash
cargo fmt --all -- --check
cargo test -p mooncake-store-client --features s3 --test test_s3_source --no-fail-fast
```

Expected: both exit 0.

### Task 2: spdk-rs v2.11.0 probe and static link closure

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/build.rs`
- Test: `rust-repo/crates/mooncake-store-master/src/service/spdk_rs_probe.rs`

**Interfaces:**
- Consumes: the manifest-controlled SPDK 25.05 SDK under `/usr/local` and Ubuntu static RDMA dependencies.
- Produces: a tested raw NVMe-oF probe adapter and a complete static link.

- [x] **Step 1: Capture the failing link output**

```bash
SPDK_ROOT_DIR=/usr/local \
PKG_CONFIG_PATH=$SPDK_ROOT_DIR/lib/pkgconfig \
  cargo test -p mooncake-store-master --features spdk-nof-probe --no-fail-fast -vv
```

Expected before repair: missing jitterentropy, duplicate zstd, or unresolved
private libibverbs symbols on current Ubuntu.

- [x] **Step 2: Fix the Ubuntu static dependency closure**

Install `libjitterentropy3-dev`, make `zstd-sys` reuse pkg-config zstd, and
append the pkg-config-discovered `libibverbs.a` archive after the RDMA provider
archives. This keeps the upstream `spdk-rs` revision unchanged.

- [x] **Step 3: Cover the raw adapter without hardware**

Test TCP/RDMA and IPv4/IPv6 transport conversion, namespace propagation,
initialization/attach/allocation/submission/completion errors, timeout, and
cleanup through the injectable operation seam.

- [x] **Step 4: Verify the feature build and tests**

Repeat the Task 2 Step 1 command. Expected: final link succeeds and all
executed master tests pass.

### Task 3: Out-of-tree common configuration fixtures

**Files:**
- Modify: `mooncake-common/tests/CMakeLists.txt`
- Modify: `mooncake-common/tests/default_config_test.cpp`

**Interfaces:**
- Consumes: CMake source directory for `mooncake-common/tests`.
- Produces: an absolute, build-layout-independent test fixture directory.

- [x] **Step 1: Reproduce the focused failure**

```bash
ctest --test-dir /home/fy2462/workspace/tmp/mooncake/mooncake-te-validation -R '^default_config_test$' --output-on-failure
```

Expected: four fixture-backed cases fail to load `test.json` or `test.yaml`.

- [x] **Step 2: Inject and consume the fixture directory**

Add:

```cmake
target_compile_definitions(default_config_test PRIVATE
    MOONCAKE_COMMON_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
```

Set `path_ = MOONCAKE_COMMON_TEST_DATA_DIR` and replace
`path_ + "/../../mooncake-common/tests/test.json"` with
`path_ + "/test.json"` (and likewise for YAML).

- [x] **Step 3: Rebuild and verify**

```bash
cmake --build /home/fy2462/workspace/tmp/mooncake/mooncake-te-validation -j5 --target default_config_test
ctest --test-dir /home/fy2462/workspace/tmp/mooncake/mooncake-te-validation -R '^default_config_test$' --output-on-failure
```

Expected: 1/1 target passes, including all seven cases.

### Task 4: TE metadata test isolation

**Files:**
- Modify: `mooncake-transfer-engine/tests/tcp_transport_test.cpp`
- Modify: `mooncake-transfer-engine/tests/transfer_metadata_test.cpp`

**Interfaces:**
- Consumes: P2P handshake metadata backend and loopback TCP endpoints.
- Produces: deterministic default configuration and collision-free local endpoints.

- [x] **Step 1: Reproduce both failures without environment overrides**

```bash
ctest --test-dir /home/fy2462/workspace/tmp/mooncake/mooncake-te-validation \
  -R '^(tcp_transport_test|transfer_metadata_test)$' --output-on-failure
```

Expected: empty metadata plugin initialization fails.

- [x] **Step 2: Correct the fixture default and reserve local ports**

Replace both self-assignments with:

```cpp
metadata_server = "P2PHANDSHAKE";
```

Add a loopback port reservation helper using a socket bound to port zero, and
construct `local_server_name` by joining `127.0.0.1:` with the decimal port
returned by `getsockname` when `MC_LOCAL_SERVER_NAME` is absent. Keep the
reservation alive until the test component is ready, then close it explicitly.

- [x] **Step 3: Verify tests individually and together**

Rebuild both targets, run each with `-R`, then run the combined regex from
Step 1. Expected: both pass without external environment variables.

### Task 5: Deterministic TENT invalid configuration test

**Files:**
- Modify: `mooncake-transfer-engine/tent/tests/transfer_engine_config_override_test.cpp`

**Interfaces:**
- Consumes: TENT RPC bind validation.
- Produces: a failure case independent of DNS wildcard interception.

- [x] **Step 1: Reproduce the isolated failure**

```bash
ctest --test-dir /home/fy2462/workspace/tmp/mooncake/mooncake-te-validation \
  -R '^transfer_engine_config_override_test$' --output-on-failure
```

Expected: invalid hostname resolves synthetically and the engine reports available.

- [x] **Step 2: Replace DNS-dependent invalid input**

Use a syntactically invalid bind literal that the resolver cannot reinterpret,
such as `"[invalid"`, and retain assertions that availability is false, the
configured value is preserved, and the bound RPC port is zero.

- [x] **Step 3: Rebuild and verify all ten cases**

Expected: focused CTest passes 10/10 cases.

### Task 6: Open-RDMA mock termination and hardware-aware tests

**Files:**
- Modify: `/home/fy2462/workspace/PFS/open-rdma-driver/rust-driver/src/rdma_utils/pagemaps.rs`
- Modify: `/home/fy2462/workspace/PFS/open-rdma-driver/rust-driver/src/ring/spec.rs`
- Modify: `/home/fy2462/workspace/PFS/open-rdma-driver/rust-driver/src/verbs/mock.rs`
- Modify: the focused meta-report worker test module identified by the hanging test names.

**Interfaces:**
- Consumes: emulated device, optional HugeTLB mappings, and Linux pagemap visibility.
- Produces: terminating pure mock tests and explicit ignored hardware tests.

- [x] **Step 1: Reproduce each failing or hanging test by exact name with a 60-second process timeout**

Run the following exact tests separately with a 60-second process timeout:

```bash
for test_name in \
  test_translate_va_to_pa test_check_addr_is_anon_hugepage \
  test_build_rings_100g test_build_rings_400g \
  test_handle_ack_local_hw test_handle_ack_remote_driver \
  test_handle_nak_local_hw test_handle_nak_remote_hw \
  test_handle_nak_remote_driver; do
  timeout 60 cargo test --lib --no-default-features \
    --features 'mock page_size_2m' "$test_name" -- --nocapture
done
```

Record whether each depends on HugeTLB, privileged PFNs, or a missing
producer/consumer.

- [x] **Step 2: Make hardware prerequisites explicit**

Mark HugeTLB/PFN tests ignored with the exact prerequisite so unavailable
hardware is not counted as a pass. Run them explicitly with `--ignored` on a
capable host, and always `munmap` successful mappings through a scoped guard.

- [x] **Step 3: Remove mock deadlocks at their source**

For ring/meta-report tests, construct finite emulated producer/consumer state
or use the existing shutdown channel before joining workers. Add bounded
`recv_timeout`/completion assertions so a regression fails rather than hangs.

- [x] **Step 4: Verify the complete Open-RDMA library suite**

```bash
timeout 300 cargo test --lib --no-default-features --features 'mock page_size_2m'
```

Expected: all pure software tests pass, hardware cases are reported as ignored,
and the process exits normally before the timeout.

### Task 7: Install and exercise clang-format 20

**Files:**
- Modify only if needed: `scripts/code_format.sh`

**Interfaces:**
- Consumes: official clang-format major version 20.
- Produces: a discoverable `clang-format-20` or version-20 `clang-format` binary.

- [x] **Step 1: Confirm the existing negative gate**

```bash
./scripts/code_format.sh --check
```

Expected before installation: `clang-format version 20 not found`.

- [x] **Step 2: Install official LLVM 20 tooling**

Use the Ubuntu LLVM repository/package route documented by the script. Do not
replace the repository version check with an older formatter.

- [x] **Step 3: Verify discovery and format touched C/C++ files**

```bash
clang-format-20 --version
./scripts/code_format.sh --check
```

Expected: version reports 20.x and the script reaches formatting checks. Apply
clang-format 20 only to touched C/C++ files, then rerun check mode.

### Task 8: Full Linux validation and final record

**Files:**
- Update: `rust-repo/change_logs/2026-07-22-002.md`

**Interfaces:**
- Consumes: all repaired software gates and exact hardware inventory.
- Produces: final Task 10 evidence with no stale failure descriptions.

- [x] **Step 1: Run the complete Rust and optional-feature matrix**

Run formatting, workspace tests, clippy, S3, SPDK, native FFI/client, and
Python tests using the clean Linux native libraries.

- [x] **Step 2: Reconfigure and rebuild TE/TENT from a clean temporary directory**

Use the approved Linux flags from the design and run all 54 CTest targets with
`--output-on-failure`.

- [x] **Step 3: Run Open-RDMA and inventory hardware gates**

Run the complete software suite and record exact device, HugeTLB, service, or
permission prerequisites for tests that cannot execute.

- [x] **Step 4: Run repository hygiene gates**

```bash
git diff --check
VIRTUAL_ENV=.venv \
  UV_CACHE_DIR=/home/fy2462/workspace/tmp/mooncake/uv-cache \
  PRE_COMMIT_HOME=/home/fy2462/workspace/tmp/mooncake/pre-commit \
  uv run --active pre-commit run --files \
  rust-repo/crates/mooncake-store-client/tests/test_engram.rs \
  rust-repo/crates/mooncake-store-client/tests/test_s3_source.rs \
  rust-repo/crates/mooncake-store-client/build.rs \
  rust-repo/crates/transfer-engine-ffi/src/lib.rs \
  mooncake-common/tests/CMakeLists.txt \
  mooncake-common/tests/default_config_test.cpp \
  mooncake-transfer-engine/tests/tcp_transport_test.cpp \
  mooncake-transfer-engine/tests/transfer_metadata_test.cpp \
  mooncake-transfer-engine/tent/tests/transfer_engine_config_override_test.cpp \
  rust-repo/change_logs/2026-07-22-002.md
```

Expected: all applicable hooks pass using clang-format 20.

- [x] **Step 5: Update the migration log and review both repositories**

Record commands, counts, and remaining hardware gates. Review `git diff` and
`git status` separately in Mooncake and Open-RDMA; do not stage unrelated
changes.
