# Rust Store Continuous Parity Validation Implementation Plan

> Superseded for SPDK work by `2026-07-22-spdk-rs-nof-probe-migration.md`.
> The v26.01 and local `spdk-io` instructions below are retained only as
> historical context and must not be executed.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the repository SPDK default to v26.01, restore all Rust Store quality gates, and produce a complete Task 10 validation record covering Rust and the native Transfer Engine/TENT boundary.

**Architecture:** The Rust Store remains independent from the C++ Store and links only Transfer Engine/TENT through `transfer-engine-ffi`. Rust-owned SPDK probing uses `spdk-io` against the repository-installed SPDK v26.01. Each gate is independently attributable so unavailable hardware never hides software failures.

**Tech Stack:** Bash, SPDK v26.01, DPDK, RDMA, CMake/Ninja/CTest, Rust 2024, Cargo, PyO3/Python 3.14, Open RDMA mock.

## Global Constraints

- Work on branch `rust_repo_main`.
- Preserve unrelated staged, unstaged, and untracked user changes.
- Do not build or link the C++ `mooncake-store` as a Rust Store prerequisite.
- Store gaps are fixed in `rust-repo`; Transfer Engine changes are limited to data-plane ownership.
- SPDK v26.01 must be the repository installer default and must expose pkg-config metadata usable by `spdk-io-sys`.
- Use `PATH=/usr/local/go/bin:$PATH` for commands requiring Go.
- Record exact reasons for hardware/service-gated skips.

---

### Task 1: Make SPDK v26.01 the tested installer default

**Files:**
- Modify: `dependencies.sh`
- Create: `scripts/test_dependencies_installer.py`

**Interfaces:**
- Consumes: `dependencies.sh --with-spdk`.
- Produces: one `SPDK_VERSION=v26.01` source of truth used by checkout and completion output.

- [ ] **Step 1: Add a failing installer regression test**

Add a test that reads `dependencies.sh`, requires a single `SPDK_VERSION=v26.01` assignment, requires checkout and status output to reference `$SPDK_VERSION`, and rejects literal `v23.01.1`.

```python
from pathlib import Path
import unittest


class DependenciesInstallerTest(unittest.TestCase):
    def test_spdk_version_has_one_v26_source_of_truth(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertEqual(script.count("SPDK_VERSION=v26.01"), 1)
        self.assertIn('git checkout "$SPDK_VERSION"', script)
        self.assertIn('SPDK ($SPDK_VERSION)', script)
        self.assertNotIn("v23.01.1", script)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and observe the version failure**

Run: `python3 scripts/test_dependencies_installer.py`

Expected: FAIL because the installer still contains literal `v23.01.1` references.

- [ ] **Step 3: Centralize and upgrade the installer version**

Add `SPDK_VERSION=v26.01` beside `GOVER`, replace the checkout/status literals with the variable, and add `libmsgpack-cxx-dev` to Ubuntu/Debian packages because `libmsgpack-dev` is transitional on Ubuntu 26.04.

- [ ] **Step 4: Verify installer tests and shell syntax**

Run: `python3 scripts/test_dependencies_installer.py && bash -n dependencies.sh`

Expected: PASS and no shell syntax output.

- [ ] **Step 5: Commit the scoped installer change**

```bash
git add dependencies.sh scripts/test_dependencies_installer.py
git commit -m "build: upgrade default SPDK to v26.01"
```

### Task 2: Install and validate SPDK v26.01 with RDMA

**Files:**
- Modify only external/system installation state under `extern/spdk` and `/usr/local`.

**Interfaces:**
- Consumes: SPDK v26.01 source and system dependency installer.
- Produces: headers, static libraries, and `.pc` files discoverable through `PKG_CONFIG_PATH`.

- [ ] **Step 1: Fetch and checkout v26.01 with submodules**

Run: `git -C extern/spdk fetch --tags origin && git -C extern/spdk checkout v26.01 && git -C extern/spdk submodule update --init --recursive`

Expected: `git -C extern/spdk describe --tags --exact-match` prints `v26.01`.

- [ ] **Step 2: Install SPDK build dependencies**

Run: `sudo env PIP_BREAK_SYSTEM_PACKAGES=1 extern/spdk/scripts/pkgdep.sh --rdma`

Expected: exit 0.

- [ ] **Step 3: Configure and build with RDMA**

Run from `extern/spdk`: `./configure --with-rdma --prefix=/usr/local && make -j5 DPDKBUILD_FLAGS=-Dplatform=generic`

Expected: exit 0 and configuration reports the verbs RDMA provider.

- [ ] **Step 4: Install complete metadata**

Run: `sudo make install`

Expected: `/usr/local/lib/pkgconfig/spdk_nvme.pc` exists; locate `libdpdk.pc` and retain its directory for Rust feature commands.

- [ ] **Step 5: Verify pkg-config closure**

Run: `PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:/home/fy2462/Mooncake/extern/spdk/dpdk/build/lib/pkgconfig pkg-config --modversion spdk_nvme libdpdk`

Expected: both packages resolve without warnings.

### Task 3: Restore Rust SPDK and Clippy gates

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_engram.rs`
- Modify only if v26.01 still requires it: Rust-owned Cargo/build configuration under `rust-repo/`.

**Interfaces:**
- Consumes: SPDK pkg-config closure and current Rust tests.
- Produces: passing `spdk-nof-probe` compilation/tests and non-error Clippy execution.

- [ ] **Step 1: Re-run the SPDK feature gate unchanged**

Run: `PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:/home/fy2462/Mooncake/extern/spdk/dpdk/build/lib/pkgconfig cargo test -p mooncake-store-master --features spdk-nof-probe --no-fail-fast`

Expected: v23 packed/aligned binding errors are absent; all device-independent tests pass.

- [ ] **Step 2: Preserve the observed Clippy red test**

Run: `cargo clippy -p mooncake-store-master -p mooncake-store-client --all-targets`

Expected before the fix: FAIL on `clippy::erasing_op` in `test_engram.rs`.

- [ ] **Step 3: Replace constant arithmetic with explicit expected offsets**

Use `0`, `64`, and the existing nonzero multiples directly in expected vectors. Do not change test meaning or production behavior.

- [ ] **Step 4: Verify focused tests and Clippy**

Run: `cargo test -p mooncake-store-client --test test_engram && cargo clippy -p mooncake-store-master -p mooncake-store-client --all-targets`

Expected: test PASS; Clippy exits 0. Existing non-deny warnings may be recorded for a later cleanup batch.

- [ ] **Step 5: Commit the scoped gate fix**

```bash
git add rust-repo/crates/mooncake-store-client/tests/test_engram.rs
git commit -m "test(store-rust): keep engram expectations clippy-clean"
```

### Task 4: Run the complete Rust and Python matrix

**Files:**
- No source changes unless a reproducible regression is found through a failing test.

**Interfaces:**
- Consumes: `libtransfer_engine.so`, optional `libtent_shared.so`, and Python 3.14.
- Produces: complete Rust workspace and feature evidence.

- [ ] **Step 1: Verify formatting and all non-Python workspace tests**

Run: `cargo fmt --all -- --check && cargo test --workspace --exclude mooncake-store-py --no-fail-fast` with TE/TENT library paths exported.

Expected: exit 0.

- [ ] **Step 2: Verify the Python extension separately**

Run with `RUSTFLAGS='-C link-arg=/usr/lib/aarch64-linux-gnu/libpython3.14.so.1.0'`: `cargo test -p mooncake-store-py --no-fail-fast`.

Expected: 12 tests pass.

- [ ] **Step 3: Run optional S3 coverage**

Run: `cargo test -p mooncake-store-client --features s3 --test test_s3_source --no-fail-fast`

Expected: in-process mock S3 tests pass; external S3 tests remain environment gated.

- [ ] **Step 4: Run native-link feature checks**

Run with TE/TENT library paths: `cargo test -p transfer-engine-ffi --features link-tent-native --no-fail-fast` and `cargo test -p mooncake-store-client --features link-native --no-fail-fast`.

Expected: exit 0.

### Task 5: Run Transfer Engine and TENT C++ tests

**Files:**
- No source changes unless a native regression is reproduced.

**Interfaces:**
- Consumes: CPU/TCP/HTTP/RDMA CMake configuration with `USE_TENT=ON` and `WITH_STORE=OFF`.
- Produces: native shared libraries and CTest results used by Rust.

- [ ] **Step 1: Configure a clean native build**

Run: `cmake -S . -B /tmp/mooncake-te-validation -G Ninja -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF -DWITH_TE=ON -DUSE_TCP=ON -DUSE_HTTP=ON -DUSE_TENT=ON -DBUILD_SHARED_LIBS=ON -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=OFF -DUSE_CUDA=OFF -DUSE_CXL=OFF -DCMAKE_BUILD_TYPE=Release`

Expected: configure succeeds.

- [ ] **Step 2: Build native libraries and tests**

Run: `cmake --build /tmp/mooncake-te-validation -j5`

Expected: `libtransfer_engine.so`, `libtent_shared.so`, and test binaries exist.

- [ ] **Step 3: Run software-only CTest suite**

Run: `ctest --test-dir /tmp/mooncake-te-validation --output-on-failure -j2`.

Expected: software tests pass; any device-gated failures are rerun individually and classified with evidence.

- [ ] **Step 4: Run TCP-focused tests explicitly**

Run the CTest regex covering `tcp_transport_test`, `tcp_write_visibility_test`, `tcp_address_validation_test`, and `tent_tcp_transport_test` with verbose failure output.

Expected: all selected tests pass.

### Task 6: Run Open RDMA mock and environment-gated checks

**Files:**
- No repository changes unless a reproducible integration defect is found.

**Interfaces:**
- Consumes: `/home/fy2462/workspace/PFS/open-rdma-driver` and repository smoke entrypoint.
- Produces: mock-driver evidence plus explicit hardware/service availability report.

- [ ] **Step 1: Verify Open RDMA driver identity and prerequisites**

Run: `git -C /home/fy2462/workspace/PFS/open-rdma-driver status --short` and inspect driver/test entrypoints.

- [ ] **Step 2: Run Rust mock tests**

Run in `dtld-ibverbs`: `cargo test --no-default-features --features mock tap`.

Expected: exit 0.

- [ ] **Step 3: Run the repository Open RDMA smoke test**

Run the existing strict smoke entrypoint with the driver directory and required sudo privileges.

Expected: pass, or record the exact absent kernel/device prerequisite.

- [ ] **Step 4: Inventory hardware/service gates**

Record hugepage totals, `/dev` RDMA/SPDK devices, `ibv_devices`, `ib_write_bw`, accelerator devices, Kubernetes config, and external etcd/Redis/S3/NVMe-oF endpoints without exposing credentials.

- [ ] **Step 5: Execute every available real integration test**

Run applicable HugeTLB, RDMA loopback, and configured local service tests. Skip NVMe-oF device E2E only if no target/namespace exists.

### Task 7: Final Task 10 evidence and next incremental audit

**Files:**
- Create: `rust-repo/change_logs/2026-07-22-002.md`
- Modify: audit plan/matrix only when new commits require disposition updates.

**Interfaces:**
- Consumes: all command outputs from Tasks 1–6 and the previous audit boundary `38c5d726`.
- Produces: an evidence-backed completion record and the next Rust migration queue.

- [ ] **Step 1: Audit commits after the prior boundary**

Enumerate C++ Store and Transfer Engine changes after `38c5d726`, excluding already recorded Rust migration outputs, and assign each applicable commit exactly one disposition.

- [ ] **Step 2: Write the validation summary**

Record environment versions, exact commands, pass/fail counts, Clippy warnings, feature results, and evidence-backed skips in `rust-repo/change_logs/2026-07-22-002.md`.

- [ ] **Step 3: Run repository hygiene checks**

Run: `git diff --check`, `git status --short`, and pre-commit on only the files touched by this plan when available.

Expected: no whitespace errors and no unrelated files included.

- [ ] **Step 4: Commit only the scoped summary and audit updates**

```bash
git add rust-repo/change_logs/2026-07-22-002.md rust-repo/docs/superpowers/plans/2026-07-20-rust-store-post-110bfa47-parity.md
git commit -m "docs(store-rust): record SPDK 26 parity validation"
```
