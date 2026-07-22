# SPDK-RS NoF Probe Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace local `spdk-io` patches with a remote pinned `spdk-rs` dependency and a compatible system-managed OpenEBS SPDK 25.05 installation.

**Architecture:** Cargo fetches `spdk-rs` directly from GitHub at a fixed revision. `/opt/mooncake/spdk` holds the exact compatible native source/build tree; a narrow Mooncake adapter owns raw NVMe probe operations while the existing safe parser and policy remain unchanged.

**Tech Stack:** Rust 2024, Cargo Git dependencies, OpenEBS `spdk-rs` 0.2.0, OpenEBS SPDK 25.05, C FFI, pkg-config, Ubuntu AArch64.

## Global Constraints

- Do not vendor `spdk-rs`, `spdk-io`, or SPDK source in Mooncake.
- Do not compile, link, or call the C++ Store.
- Preserve `spdk-nof-probe` as the public feature name and preserve existing error categories.
- Pin both remote revisions exactly; do not depend on a moving branch.
- Establish a compiling replacement before removing the currently working backend.
- Never silently delete a modified `/opt/mooncake/spdk` checkout.

---

### Task 1: Compatible system SPDK installer

**Files:**
- Modify: `dependencies.sh`
- Modify: `scripts/test_dependencies_installer.py`

**Interfaces:**
- Consumes: `--with-spdk`, `/opt/mooncake/spdk`, OpenEBS SPDK commit `bc57f3ea7933b0965c09e9d751c21a3968c6cc11`.
- Produces: compatible headers, archives, pkg-config files, and `SPDK_ROOT_DIR` guidance.

- [ ] **Step 1: Add failing installer assertions**

Assert that the script contains the OpenEBS repository, exact revision, and
`/opt/mooncake/spdk`, and no longer clones into `${REPO_ROOT}/extern` or runs
`rm -rf spdk`.

- [ ] **Step 2: Run the installer unit test and verify RED**

```bash
python3 scripts/test_dependencies_installer.py
```

Expected: the new SPDK location/revision assertions fail.

- [ ] **Step 3: Implement idempotent installation**

Set:

```bash
SPDK_REPOSITORY=openebs/spdk
SPDK_COMMIT=bc57f3ea7933b0965c09e9d751c21a3968c6cc11
SPDK_ROOT_DIR=/opt/mooncake/spdk
```

Clone only when absent. For an existing checkout, reject local modifications,
fetch the exact commit, checkout detached, update nested submodules, configure
with RDMA and the OpenEBS-supported options, build, and install. Print the
required `SPDK_ROOT_DIR` export.

- [ ] **Step 4: Verify unit test and perform system install**

Run the installer test, then execute the SPDK install path. Verify:

```bash
git -C /opt/mooncake/spdk rev-parse HEAD
SPDK_ROOT_DIR=/opt/mooncake/spdk pkg-config --modversion spdk_nvme libdpdk
```

Expected: exact commit and compatible package versions.

### Task 2: Remote `spdk-rs` compile spike

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/Cargo.toml`
- Modify: `rust-repo/Cargo.lock`
- Create: `rust-repo/crates/mooncake-store-master/src/service/spdk_rs_probe.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`

**Interfaces:**
- Consumes: remote `spdk-rs` revision `77ef361d236ccac00ba0dd11a37c0b14b85ba730` and `SPDK_ROOT_DIR`.
- Produces: a feature-gated module that compiles representative `spdk-rs` types and raw bindings.

- [ ] **Step 1: Point the existing feature at the pinned Git dependency**

Replace `dep:spdk-io` with `dep:spdk-rs`, using package name `spdk-rs` and the
exact Git revision. Add a compile-only test importing `DmaBuf`, `Thread`, and
`libspdk::spdk_nvme_transport_id`.

- [ ] **Step 2: Verify RED before replacing the old backend**

```bash
SPDK_ROOT_DIR=/opt/mooncake/spdk cargo check -p mooncake-store-master --features spdk-nof-probe
```

Expected: `nof_probe.rs` still references missing `spdk_io` APIs.

- [ ] **Step 3: Add a temporary backend seam**

Define `probe_nof_endpoint_with_spdk_rs(endpoint, timeout)` in
`spdk_rs_probe.rs`. A compile-only test must instantiate the public `spdk-rs`
types and raw transport identifier while the existing production probe remains
unchanged until the adapter tests in Task 3 are in place.

- [ ] **Step 4: Verify the remote crate and native link compile**

Expected: Cargo downloads the Git revision into its global cache and
`cargo check` succeeds without any local `spdk-rs` source tree.

### Task 3: Test-driven NVMe-oF probe adapter

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/nof_probe.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/spdk_rs_probe.rs`

**Interfaces:**
- Consumes: `NoFTransportSpec`, `NoFTransportKind`, timeout, and injectable raw operations.
- Produces: `probe_nof_endpoint_with_spdk_rs(&str, Duration) -> Result<(), String>`.

- [ ] **Step 1: Add failing pure adapter tests**

Cover TCP/RDMA transport conversion, namespace id propagation, initialization
failure, attach failure, zero block size, allocation failure, submission
failure, completion error, timeout, and cleanup. Use a trait-backed fake raw
operation table so these tests require no SPDK hardware.

- [ ] **Step 2: Verify RED**

Run the focused service tests with `spdk-nof-probe`; expect missing adapter
behavior assertions to fail.

- [ ] **Step 3: Implement the minimal raw adapter**

Keep C strings and native structs alive across calls. Initialize SPDK global
state once, create a probe thread, create/attach the NVMe bdev/controller,
find the namespace, allocate one aligned DMA block, submit read LBA zero,
poll with the existing deadline, and clean up controller/bdev resources.

- [ ] **Step 4: Verify focused tests and feature suite**

```bash
SPDK_ROOT_DIR=/opt/mooncake/spdk \
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

- [ ] **Step 1: Remove patches, directories, and gitlink**

Use Git-aware removal only after Task 3 is green. Do not delete the system
checkout under `/opt/mooncake/spdk`.

- [ ] **Step 2: Regenerate and assert dependency metadata**

```bash
cargo metadata --format-version 1 --no-deps
rg -n 'spdk-io|spdk-io-sys' Cargo.toml Cargo.lock crates
```

Expected: metadata includes the pinned `spdk-rs` Git source and ripgrep finds
no live code/manifest dependency.

- [ ] **Step 3: Verify normal and feature builds**

Run workspace tests without SPDK, then the complete master feature suite with
`SPDK_ROOT_DIR` set. Both must pass.

### Task 5: Rebase Task 10 validation on the new backend

**Files:**
- Update: `rust-repo/change_logs/2026-07-22-002.md`
- Update: `rust-repo/docs/superpowers/plans/2026-07-22-task-10-validation-repair.md`

**Interfaces:**
- Consumes: completed migration and remaining TE/Open-RDMA/clang-format tasks.
- Produces: accurate validation commands and status.

- [ ] **Step 1: Replace stale SPDK 26.01/spdk-io instructions**

Record the remote revisions, `/opt` system dependency, software test result,
and exact real-NVMe environment gate.

- [ ] **Step 2: Run hygiene checks on migration files**

Run rustfmt, `git diff --check`, Cargo tests, installer tests, and pre-commit on
all touched Mooncake files.

- [ ] **Step 3: Resume Task 10 repairs**

Continue TE metadata/TCP, TENT, Open-RDMA, clang-format 20, and the final clean
Linux validation from the revised plan.
