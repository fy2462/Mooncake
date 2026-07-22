# SPDK-RS NoF Probe Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace local `spdk-io` patches with spdk-rs v2.11.0 and its officially matched OpenEBS SPDK 25.05/DPDK 25.03.0 installation.

**Architecture:** Cargo fetches the `spdk-rs` v2.11.0 commit directly from GitHub. A shared-filesystem checkout holds its exact OpenEBS SPDK 25.05 source/build tree, DPDK 25.03.0 submodule, and staging SDK; a manifest-controlled deployment installs the selected SDK under `/usr/local`. A narrow Mooncake adapter owns raw NVMe probe operations while the existing safe parser and policy remain unchanged.

**Tech Stack:** Rust 2024, Cargo Git dependencies, OpenEBS `spdk-rs` v2.11.0, OpenEBS SPDK 25.05, DPDK 25.03.0, C FFI, pkg-config, Ubuntu AArch64.

## Global Constraints

- Do not vendor `spdk-rs`, `spdk-io`, or SPDK source in Mooncake.
- Do not compile, link, or call the C++ Store.
- Preserve `spdk-nof-probe` as the public feature name and preserve existing error categories.
- Pin all three remote revisions exactly; do not depend on a moving branch.
- Keep build output and temporary data under `$HOME/workspace/tmp`.
- Use `CARGO_BUILD_JOBS=5` for Mooncake compilation and validation.
- Keep one selected SPDK/DPDK installation; never mix pkg-config files,
  headers, or archives from different revisions.
- Establish a compiling replacement before removing the currently working backend.
- Never silently delete a modified `SPDK_SOURCE_DIR` checkout.
- Delete only paths recorded in `/usr/local/share/mooncake/spdk-25.05.manifest`;
  never recursively delete `/usr/local`.

---

### Task 1: Compatible system SPDK installer

**Files:**
- Modify: `dependencies.sh`
- Modify: `scripts/test_dependencies_installer.py`

**Interfaces:**
- Consumes: `--with-spdk`, caller-selected `SPDK_SOURCE_DIR`, OpenEBS SPDK commit `cc090cd2b64775545eb38022bb0ec8f37f4741a6`, and DPDK commit `cf36799c473a686fa14fde9af97f917a2125d3d5`.
- Produces: compatible headers, archives, pkg-config files under `/usr/local`, and `SPDK_ROOT_DIR=/usr/local` guidance.

- [x] **Step 1: Add failing installer assertions**

Assert that the script contains the exact SPDK and DPDK revisions, honors an
existing `SPDK_SOURCE_DIR`, defaults its checkout beneath `$HOME/workspace/tmp`,
and no longer clones into `${REPO_ROOT}/extern` or runs `rm -rf spdk`.

- [x] **Step 2: Run the installer unit test and verify RED**

```bash
python3 scripts/test_dependencies_installer.py
```

Expected: the new SPDK location/revision assertions fail.

- [x] **Step 3: Implement idempotent installation**

Set:

```bash
SPDK_REPOSITORY=openebs/spdk
SPDK_COMMIT=cc090cd2b64775545eb38022bb0ec8f37f4741a6
DPDK_COMMIT=cf36799c473a686fa14fde9af97f917a2125d3d5
SPDK_SOURCE_DIR=${SPDK_SOURCE_DIR:-$HOME/workspace/tmp/mooncake/spdk-25.05}
```

Clone only when absent. For an existing checkout, reject local modifications,
fetch the exact commit, checkout detached, update nested submodules, assert the
DPDK gitlink, configure with RDMA and io_uring support, build, and install.
Use `spdk-rs/build_scripts/build_spdk.sh` to stage the SDK under
`$HOME/workspace/tmp/mooncake/spdk-sdk-25.05`, then deploy only those files to
`/usr/local` using an owned-file manifest. Print `SPDK_ROOT_DIR=/usr/local`
and the matching `PKG_CONFIG_PATH` exports.

- [x] **Step 4: Verify unit test and perform system install**

Run the installer test, then execute the SPDK install path. Verify:

```bash
git -C "$SPDK_SOURCE_DIR" rev-parse HEAD
git -C "$SPDK_SOURCE_DIR/dpdk" rev-parse HEAD
PKG_CONFIG_PATH=/usr/local/lib/pkgconfig pkg-config --modversion libdpdk
rg 'SPDK_VERSION_(MAJOR|MINOR)' /usr/local/include/spdk/version.h
```

Expected: exact commits, SPDK 25.05, and DPDK 25.03.0.

### Task 2: Remote `spdk-rs` compile spike

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/Cargo.toml`
- Modify: `rust-repo/Cargo.lock`
- Create: `rust-repo/crates/mooncake-store-master/src/service/spdk_rs_probe.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

**Interfaces:**
- Consumes: spdk-rs v2.11.0 revision `78d6018af041e80a42e222165b86070bae631821` and `SPDK_ROOT_DIR`.
- Produces: a feature-gated module that compiles representative `spdk-rs` types and raw bindings.

- [x] **Step 1: Point the existing feature at the pinned Git dependency**

Replace `dep:spdk-io` with `dep:spdk-rs`, using package name `spdk-rs` and the
exact Git revision. Add a compile-only test importing `DmaBuf`, `Thread`, and
`libspdk::spdk_nvme_transport_id`.

- [x] **Step 2: Verify RED before replacing the old backend**

```bash
SPDK_ROOT_DIR=/usr/local cargo check -p mooncake-store-master --features spdk-nof-probe
```

Expected: `nof_probe.rs` still references missing `spdk_io` APIs.

- [x] **Step 3: Add a temporary backend seam**

Define `probe_nof_endpoint_with_spdk_rs(endpoint, timeout)` in
`spdk_rs_probe.rs`. A compile-only test must instantiate the public `spdk-rs`
types and raw transport identifier while the existing production probe remains
unchanged until the adapter tests in Task 3 are in place.

- [x] **Step 4: Verify the remote package and native link compile**

Expected: Cargo downloads the Git revision into its global cache and
`cargo check` succeeds without any local `spdk-rs` source tree.

### Task 3: Test-driven NVMe-oF probe adapter

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/nof_probe.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/spdk_rs_probe.rs`

**Interfaces:**
- Consumes: `NoFTransportSpec`, `NoFTransportKind`, timeout, and injectable raw operations.
- Produces: `probe_nof_endpoint_with_spdk_rs(&str, Duration) -> Result<(), String>`.

- [x] **Step 1: Add failing pure adapter tests**

Cover TCP/RDMA transport conversion, namespace id propagation, initialization
failure, attach failure, zero block size, allocation failure, submission
failure, completion error, timeout, and cleanup. Use a trait-backed fake raw
operation table so these tests require no SPDK hardware.

- [x] **Step 2: Verify RED**

Run the focused service tests with `spdk-nof-probe`; expect missing adapter
behavior assertions to fail.

- [x] **Step 3: Implement the minimal raw adapter**

Keep C strings and native structs alive across calls. Initialize SPDK global
state once, create a probe thread, create/attach the NVMe bdev/controller,
find the namespace, allocate one aligned DMA block, submit read LBA zero,
poll with the existing deadline, and clean up controller/bdev resources.

- [x] **Step 4: Verify focused tests and feature suite**

```bash
SPDK_ROOT_DIR=/usr/local \
  CARGO_BUILD_JOBS=5 \
  cargo test -p mooncake-store-master --features spdk-nof-probe --no-fail-fast
```

Expected: all software tests pass; real target access remains environment-gated.

### Task 4: Remove local SPDK wrappers and repository SPDK submodule

**Files:**
- Modify: `rust-repo/Cargo.toml`
- Modify: `rust-repo/Cargo.lock`
- Delete: `rust-repo/third-party/spdk-io/`
- Delete: `rust-repo/third-party/spdk-io-sys/`
- Modify: `.gitmodules`
- Delete gitlink: `extern/spdk`
- Update: Rust migration scope/spec/plan references that prescribe `spdk-io` or repository SPDK source.

**Interfaces:**
- Consumes: passing Task 3 replacement.
- Produces: Cargo metadata containing `spdk-rs` and no `spdk-io` packages.

- [x] **Step 1: Remove patches, directories, and gitlink**

Use Git-aware removal only after Task 3 is green. Keep the shared source
checkout for reproducible rebuilds; Cargo consumes the installed SDK through
`SPDK_ROOT_DIR=/usr/local`.

- [x] **Step 2: Regenerate and assert dependency metadata**

```bash
cargo metadata --format-version 1 --no-deps
rg -n 'spdk-io|spdk-io-sys' Cargo.toml Cargo.lock crates
```

Expected: metadata includes the pinned `spdk-rs` Git source and ripgrep finds
no live code/manifest dependency.

- [x] **Step 3: Verify normal and feature builds**

Run workspace tests without SPDK, then the complete master feature suite with
`SPDK_ROOT_DIR` set. Both must pass.

### Task 5: Rebase Task 10 validation on the new backend

**Files:**
- Update: `rust-repo/change_logs/2026-07-22-002.md`
- Update: `rust-repo/docs/superpowers/plans/2026-07-22-task-10-validation-repair.md`

**Interfaces:**
- Consumes: completed migration and remaining TE/Open-RDMA/clang-format tasks.
- Produces: accurate validation commands and status.

- [x] **Step 1: Replace stale spdk-io and SPDK 26.01 instructions**

Record all three remote revisions, the shared source checkout, the single
installed SDK, software test result, and exact real-NVMe environment gate.

- [x] **Step 2: Run hygiene checks on migration files**

Run rustfmt, `git diff --check`, Cargo tests, installer tests, and pre-commit on
all touched Mooncake files.

- [x] **Step 3: Resume Task 10 repairs**

Continue TE metadata/TCP, TENT, Open-RDMA, clang-format 20, and the final clean
Linux validation from the revised plan.

### Task 6: Deploy the selected SDK under `/usr/local`

**Files:**
- Modify: `dependencies.sh`
- Modify: `scripts/test_dependencies_installer.py`
- Update: `rust-repo/change_logs/2026-07-22-002.md`

**Interfaces:**
- Consumes: shared `SPDK_SOURCE_DIR`, `SPDK_RS_SOURCE_DIR`, and staging SDK.
- Produces: `/usr/local/include/spdk`, `/usr/local/lib/libspdk*.a`, matching
  DPDK archives/pkg-config metadata, and an owned-file manifest.

- [x] **Step 1: Add failing system-prefix assertions**

Assert that the installer defaults `SPDK_INSTALL_PREFIX` to `/usr/local`,
stages under `$HOME/workspace/tmp/mooncake`, records an install manifest, and
prints `SPDK_ROOT_DIR=/usr/local`. Assert that broad deletion such as
`rm -rf /usr/local` is absent.

- [x] **Step 2: Verify RED**

```bash
python3 -m unittest scripts/test_dependencies_installer.py
```

Expected: system-prefix and manifest assertions fail.

- [x] **Step 3: Implement manifest-controlled deployment**

Build and stage with the pinned `build_spdk.sh`; remove only files listed by a
validated prior manifest, copy staging contents into `/usr/local`, rewrite
staging prefixes in installed `.pc` files to `/usr/local`, and atomically
install the new manifest under `/usr/local/share/mooncake`.

- [x] **Step 4: Install and verify the exact system SDK**

```bash
sudo ./dependencies.sh -y --with-spdk
SPDK_ROOT_DIR=/usr/local PKG_CONFIG_PATH=/usr/local/lib/pkgconfig \
  pkg-config --modversion libdpdk
rg 'SPDK_VERSION_(MAJOR|MINOR|PATCH)' /usr/local/include/spdk/version.h
```

Expected: DPDK `25.03.0`, SPDK `25.05.1`, and no pkg-config path into the
shared staging directory.

### Task 7: Correct completion and teardown semantics

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/spdk_rs_probe.rs`

**Interfaces:**
- Consumes: raw NVMe completion status and outstanding qpair state.
- Produces: phase-independent completion classification, stable transport
  error categories, and quiesced timeout cleanup.

- [x] **Step 1: Add failing regression tests**

Cover raw status `1` as success, preserve `transport_id_parse_fail` without an
`open_fail` wrapper, and assert timeout cleanup quiesces outstanding I/O before
DMA buffer release.

- [x] **Step 2: Verify RED**

```bash
SPDK_ROOT_DIR=/usr/local CARGO_BUILD_JOBS=5 \
  cargo test -p mooncake-store-master --features spdk-nof-probe \
  service::spdk_rs_probe::tests --lib
```

Expected: the three new assertions fail.

- [x] **Step 3: Implement the minimal fixes**

Mask the phase bit before classifying a completion, preserve transport parsing
errors as their own category, and free/disconnect the qpair (which synchronously
aborts queued requests) before dropping the DMA buffer.

- [x] **Step 4: Verify system-only linkage and full suites**

Unset shared SDK/library overrides, clean the master package, and run both the
focused tests and complete `spdk-nof-probe` suite with
`SPDK_ROOT_DIR=/usr/local`, followed by the feature-off workspace suite.
