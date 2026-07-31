# Transfer Engine Native Parity Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build local classic Transfer Engine and host-only TENT shared
libraries, execute every Rust/FFI parity test supported by this aarch64 host,
and finish the semantic review of all 2,451 in-scope reference-test rows.

**Architecture:** Build into the current worktree's canonical ignored `build/`
directory so existing Rust build scripts discover the libraries without source
changes. Execute currently blocked native evidence through validated temporary
manifests under `/tmp`, promote source-manifest rows only after exact tests pass,
then re-audit every remaining missing, blocked, and N/A row in stable suite/file
order.

**Tech Stack:** CMake, Ninja, ELF/binutils, classic Mooncake Transfer Engine,
TENT, Rust/Cargo, Python 3, JSON parity manifests, existing validation runners,
Git, and repository pre-commit hooks.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- C/C++ and `mooncake-wheel/tests` sources are immutable references. Compiling
  Transfer Engine implementation sources into shared libraries is authorized;
  editing those sources or building/running any C++ GoogleTest target is not.
- Do not build Mooncake Store C++, examples, benchmarks, or C++ unit tests.
- Keep all native artifacts in the ignored worktree `build/` directory. Do not
  install Mooncake libraries into `/usr/local`.
- Use scoped `LD_LIBRARY_PATH`, `RUSTFLAGS`, and Cargo target directories. Do
  not add global linker configuration.
- Inspect dependencies before package installation. Use `sudo` only for a
  demonstrated missing development package, through an interactive prompt;
  never persist or echo credentials.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling.
- Use `apply_patch` for tracked semantic edits.
- Set `SKIP=mooncake-code-format` on every pre-commit invocation.
- Preserve etcd-only Store HA policy. Classic TE metadata remains HTTP-only in
  the host build; Store leader election configuration is unchanged.
- Do not start Store LocalDisk `io_uring` optimization in this plan.
- Existing Rust production or test code remains unchanged unless a failing
  native test is diagnosed and the user separately authorizes remediation.
- A Cargo command that reports `running 0 tests` is a failure regardless of its
  process exit code.
- Before every commit, both staged and unstaged diffs for `mooncake-store`,
  `mooncake-transfer-engine`, and `mooncake-wheel/tests` must be empty.

---

### Task 1: Capture the native-build preflight and configure classic TE

**Files:**
- Read: `CMakeLists.txt`
- Read: `mooncake-common/common.cmake`
- Read: `mooncake-transfer-engine/CMakeLists.txt`
- Read: `mooncake-transfer-engine/src/CMakeLists.txt`
- Generate, ignored: `build/`
- Append, ignored: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Produces a CMake cache in `build/` with shared classic TE enabled and all
  unrelated components disabled.
- Preserves a preflight record containing architecture, compiler, CMake, Ninja,
  free memory/disk, dependency versions, and the exact configure argv.

- [ ] **Step 1: Verify the tracked baseline and immutable-source guards**

  Run from the worktree root:

  ```bash
  git status --short
  git branch --show-current
  git diff --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  git diff --cached --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  ```

  Expected: clean tracked worktree, branch `codex/store-cpp-parity-only`, and
  both immutable guards exit zero.

- [ ] **Step 2: Record toolchain, resource, and dependency evidence**

  Run:

  ```bash
  uname -a
  cmake --version
  ninja --version
  c++ --version
  free -h
  df -h . /tmp
  dpkg-query -W libibverbs-dev libnuma-dev libgoogle-glog-dev \
    libgflags-dev libjsoncpp-dev libyaml-cpp-dev libcurl4-openssl-dev \
    liburing-dev
  test -f /usr/local/lib/cmake/yalantinglibs/yalantinglibsConfig.cmake
  ```

  Expected: aarch64 host, required development packages installed, at least 8
  GiB available memory including swap, at least 8 GiB worktree filesystem
  space, and installed yalantinglibs CMake configuration.

- [ ] **Step 3: Install only a demonstrated missing host dependency**

  Skip this step when Step 2 passes. If one named package is absent, install
  only that package through an interactive sudo session, then rerun Step 2.
  Do not install accelerator or vendor-fabric SDKs.

- [ ] **Step 4: Configure the canonical classic shared-library build**

  Run:

  ```bash
  cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DENABLE_DEBUG_SYMBOLS=ON \
    -DWITH_TE=ON \
    -DWITH_STORE=OFF \
    -DWITH_STORE_RUST=OFF \
    -DWITH_STORE_GO=OFF \
    -DWITH_P2P_STORE=OFF \
    -DWITH_EP=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_UNIT_TESTS=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_BENCHMARK=OFF \
    -DUSE_TCP=ON \
    -DUSE_HTTP=ON \
    -DUSE_ETCD=OFF \
    -DUSE_REDIS=OFF \
    -DUSE_TENT=OFF \
    -DUSE_CUDA=OFF \
    -DUSE_HIP=OFF \
    -DUSE_MUSA=OFF \
    -DUSE_MACA=OFF \
    -DUSE_MLU=OFF \
    -DUSE_ASCEND=OFF \
    -DUSE_ASCEND_DIRECT=OFF \
    -DUSE_UBSHMEM=OFF \
    -DUSE_ASCEND_HETEROGENEOUS=OFF \
    -DUSE_NVMEOF=OFF \
    -DUSE_EFA=OFF \
    -DUSE_CXI=OFF \
    -DUSE_UB=OFF \
    -DUSE_CXL=OFF \
    -DUSE_SUNRISE=OFF \
    -DUSE_TPU=OFF
  ```

  Expected: configuration exits zero, reports TCP and HTTP enabled, reports
  TENT and vendor transports disabled, and writes `build/build.ninja`.

- [ ] **Step 5: Verify the cache rather than trusting configure output**

  Run:

  ```bash
  cmake -LA -N build | rg \
    '^(BUILD_SHARED_LIBS|BUILD_UNIT_TESTS|BUILD_EXAMPLES|BUILD_BENCHMARK|WITH_TE|WITH_STORE|WITH_STORE_RUST|USE_TCP|USE_HTTP|USE_TENT):'
  ```

  Expected: values exactly match Step 4.

### Task 2: Build and validate `libtransfer_engine.so`

**Files:**
- Generate, ignored: `build/mooncake-transfer-engine/src/libtransfer_engine.so`
- Generate, ignored: shared dependencies below `build/mooncake-common/` and
  `build/mooncake-transfer-engine/`
- Append, ignored: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Produces an AArch64 shared library with the C ABI required by
  `transfer-engine-ffi`.
- Produces a scoped runtime-library directory list for subsequent Cargo tests.

- [ ] **Step 1: Build only the classic Transfer Engine target**

  Run:

  ```bash
  cmake --build build --target transfer_engine --parallel 3
  ```

  Expected: exit zero and no C++ test, example, benchmark, Store, or wheel
  target is compiled.

- [ ] **Step 2: Verify artifact identity and architecture**

  Run:

  ```bash
  test -f build/mooncake-transfer-engine/src/libtransfer_engine.so
  file build/mooncake-transfer-engine/src/libtransfer_engine.so
  readelf -h build/mooncake-transfer-engine/src/libtransfer_engine.so | rg \
    'Class:.*ELF64|Machine:.*AArch64|Type:.*DYN'
  ```

  Expected: one ELF64 AArch64 shared object.

- [ ] **Step 3: Verify the Rust-consumed C ABI symbols**

  Run:

  ```bash
  nm -D --defined-only build/mooncake-transfer-engine/src/libtransfer_engine.so | rg \
    ' (createTransferEngine|destroyTransferEngine|installTransport|registerLocalMemory|unregisterLocalMemory|allocateBatchID|submitTransfer|getTransferStatus|freeBatchID)$'
  ```

  Expected: every named C ABI symbol is exported.

- [ ] **Step 4: Build the scoped runtime-library path and verify dependencies**

  Compute the value for this shell only:

  ```bash
  native_library_path=$(find build -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  test -n "$native_library_path"
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    ldd -r build/mooncake-transfer-engine/src/libtransfer_engine.so
  ```

  Expected: no `not found` and no `undefined symbol` line.

### Task 3: Execute classic native FFI, Store Client, and module baselines

**Files:**
- Generate, ignored: `/tmp/mooncake-native-classic-*/`
- Append, ignored: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Consumes the Stage 1 runtime-library path.
- Produces full-suite logs and an existing module-gate result before any
  manifest status changes.

- [ ] **Step 1: Execute all 67 FFI library tests with classic native linking**

  Run from `rust-repo`:

  ```bash
  native_library_path=$(find "$PWD/../build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  test -n "$native_library_path"
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR=target/native-parity-classic \
  RUSTFLAGS="-L native=../build/mooncake-transfer-engine/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test -p transfer-engine-ffi --no-default-features \
      --features link-native --lib
  ```

  Expected: exit zero and 67 tests executed; `running 0 tests` is forbidden.

- [ ] **Step 2: Execute the complete native Store Client suite**

  Run:

  ```bash
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR=target/native-parity-classic \
  RUSTFLAGS="-L native=../build/mooncake-transfer-engine/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test -p mooncake-store-client --features link-native
  ```

  Expected: every selected lib/integration target executes at least one test
  or is explicitly reported by Cargo as filtered/ignored for a documented
  reason; no executed test fails.

- [ ] **Step 3: Run the existing module gate with the local library**

  Run from the worktree root:

  ```bash
  native_library_path=$(find "$PWD/build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  test -n "$native_library_path"
  classic_artifact_root=$(mktemp -d /tmp/mooncake-native-classic-module.XXXXXX)
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  MOONCAKE_TE_LIB_DIR="$PWD/build/mooncake-transfer-engine/src" \
  MOONCAKE_VALIDATION_CARGO_TARGET_DIR="$PWD/rust-repo/target/native-parity-classic" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    rust-repo/tools/store-validation/run-module-gate.sh \
      --artifact-root "$classic_artifact_root"
  ```

  Expected: Store Client and workspace commands are executed rather than
  recorded as `libtransfer_engine.so` blocked. Preserve any actual failure.

### Task 4: Execute every row blocked only by the missing classic library

**Files:**
- Read: all four parity manifests under `rust-repo/tools/store-validation/`
- Generate, ignored: `/tmp/mooncake-native-candidate-manifests-*/`
- Generate, ignored: `/tmp/mooncake-native-candidate-gate-*/`

**Interfaces:**
- Produces validated temporary manifests that differ only by promoting the
  exact prebuilt-library prerequisite to candidate `covered`.
- Uses the existing parity runner for command derivation, evidence de-duplication,
  log capture, reference ownership, and zero-test rejection.

- [ ] **Step 1: Generate complete temporary candidate manifests**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  candidate_manifest_root=$(mktemp -d /tmp/mooncake-native-candidate-manifests.XXXXXX)
  /home/fy2462/Mooncake/.venv/bin/python - \
    "$candidate_manifest_root" \
    parity-map.json transfer-engine-parity-map.json tent-parity-map.json \
    wheel-store-parity-map.json <<'PY'
  import json
  from pathlib import Path
  import sys

  prerequisite = (
      "Prebuilt native Transfer Engine library discoverable as "
      "-ltransfer_engine through the existing aarch64 linker configuration."
  )
  output = Path(sys.argv[1])
  output.mkdir(parents=True, exist_ok=True)
  promoted = 0
  for raw in sys.argv[2:]:
      source = Path(raw)
      manifest = json.loads(source.read_text(encoding="utf-8"))
      for entry in manifest["entries"]:
          if entry.get("status") == "blocked" and entry.get("prerequisite") == prerequisite:
              entry["status"] = "covered"
              entry.pop("prerequisite")
              promoted += 1
      (output / source.name).write_text(
          json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
          encoding="utf-8",
      )
  print(f"candidate_promoted={promoted}")
  if promoted == 0:
      raise SystemExit("no native-library blocked rows were selected")
  PY
  ```

  Expected: a positive deterministic promoted count and four complete JSON
  manifests. These files are execution probes, not tracked evidence.

- [ ] **Step 2: Validate all four temporary manifests against source inventory**

  Run:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --repo-root ../../.. \
    --manifest "$candidate_manifest_root/parity-map.json" \
    --manifest "$candidate_manifest_root/transfer-engine-parity-map.json" \
    --manifest "$candidate_manifest_root/tent-parity-map.json" \
    --manifest "$candidate_manifest_root/wheel-store-parity-map.json"
  ```

  Expected: exit zero with no structural or discoverability findings.

- [ ] **Step 3: Execute the candidate manifests through the formal runner**

  Run:

  ```bash
  candidate_gate_root=$(mktemp -d /tmp/mooncake-native-candidate-gate.XXXXXX)
  native_library_path=$(find "$PWD/../../../build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  test -n "$native_library_path"
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR="$PWD/../../target/native-parity-classic" \
  RUSTFLAGS="-L native=$PWD/../../../build/mooncake-transfer-engine/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    /home/fy2462/Mooncake/.venv/bin/python run-parity-gate.py \
      --repo-root ../../.. \
      --artifact-root "$candidate_gate_root" \
      --manifest "$candidate_manifest_root/parity-map.json" \
      --manifest "$candidate_manifest_root/transfer-engine-parity-map.json" \
      --manifest "$candidate_manifest_root/tent-parity-map.json" \
      --manifest "$candidate_manifest_root/wheel-store-parity-map.json"
  ```

  Expected: overall `BLOCKED` while genuine missing/blocked rows remain, zero
  FAIL commands, and zero zero-test executions. If any command fails, retain its
  log and diagnose before changing the tracked manifest.

- [ ] **Step 4: Map every candidate result back to all owning reference rows**

  Inspect `candidate_gate_root/parity.result.json`. For every promoted Rust
  reference, require a PASS record with the same `rust.file` and `rust.test`.
  Shared evidence must update every owning reference row consistently.

### Task 5: Reclassify classic native rows in stable manifest batches

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify when selected: `rust-repo/tools/store-validation/wheel-store-parity-map.json`
- Append, ignored: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Converts only freshly passing candidate rows from `blocked` to `covered`.
- Removes the obsolete native-library prerequisite and replaces blocker-only
  prose with the exact execution artifact and command evidence.

- [ ] **Step 1: Process one reference file per review batch**

  In stable manifest order, use `apply_patch` to update all passing rows from
  one reference file. Preserve the semantic explanation and Rust evidence,
  remove `prerequisite`, set `status` to `covered`, and record the exact native
  gate artifact in the reason.

- [ ] **Step 2: Validate each batch before commit**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --repo-root ../../.. \
    --manifest parity-map.json \
    --manifest transfer-engine-parity-map.json \
    --manifest tent-parity-map.json \
    --manifest wheel-store-parity-map.json
  /home/fy2462/Mooncake/.venv/bin/python -m unittest discover \
    -s tests -p 'test_*.py' -v
  ```

  Expected: validators exit zero and all 41 validation-tool tests pass.

- [ ] **Step 3: Run scoped formatting and immutable guards**

  Run from the worktree root:

  ```bash
  git diff --check
  SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
    --files rust-repo/tools/store-validation/parity-map.json \
    rust-repo/tools/store-validation/wheel-store-parity-map.json
  git diff --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  git diff --cached --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  ```

  Expected: all checks pass and only selected manifests are changed.

- [ ] **Step 4: Commit the reviewed batch**

  Stage only changed manifests and commit with:

  ```bash
  git commit -m "[Store] execute native parity evidence"
  ```

  Repeat Steps 1-4 until every classic-library prerequisite row has a final
  result-backed disposition. Keep each commit limited to one reference file so
  the generic message still names one auditable evidence unit.

### Task 6: Re-run classic validators and the formal source manifest gate

**Files:**
- Generate, ignored: `/tmp/mooncake-native-classic-final-*/`

**Interfaces:**
- Proves the tracked source manifests execute all newly covered native evidence.

- [ ] **Step 1: Run ordinary and strict validators**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --repo-root ../../.. \
    --manifest parity-map.json \
    --manifest transfer-engine-parity-map.json \
    --manifest tent-parity-map.json \
    --manifest wheel-store-parity-map.json
  set +e
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --repo-root ../../.. --manifest parity-map.json --require-complete
  strict_rc=$?
  set -e
  test "$strict_rc" -eq 1
  ```

  Expected: strict exits one only because reviewed missing/blocked rows remain;
  no `ERROR` finding is allowed.

- [ ] **Step 2: Execute the tracked four-manifest parity gate**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  classic_final_root=$(mktemp -d /tmp/mooncake-native-classic-final.XXXXXX)
  native_library_path=$(find "$PWD/../../../build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  test -n "$native_library_path"
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR="$PWD/../../target/native-parity-classic" \
  RUSTFLAGS="-L native=$PWD/../../../build/mooncake-transfer-engine/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    /home/fy2462/Mooncake/.venv/bin/python run-parity-gate.py \
      --repo-root ../../.. \
      --artifact-root "$classic_final_root" \
      --manifest parity-map.json \
      --manifest transfer-engine-parity-map.json \
      --manifest tent-parity-map.json \
      --manifest wheel-store-parity-map.json
  ```

  Expected: zero FAIL, zero zero-test records, and every tracked covered command
  PASS. Remaining incomplete rows keep the overall result BLOCKED.

### Task 7: Reconfigure and build host-only TENT

**Files:**
- Update, ignored: `build/CMakeCache.txt`
- Generate, ignored: `build/mooncake-transfer-engine/tent/src/libtent_shared.so`
- Rebuild, ignored: `build/mooncake-transfer-engine/src/libtransfer_engine.so`

**Interfaces:**
- Produces classic TE linked with host-only TENT and a separate TENT C ABI
  shared library in paths already recognized by `transfer-engine-ffi/build.rs`.

- [ ] **Step 1: Reconfigure the canonical build with TENT enabled**

  Run from the worktree root:

  ```bash
  cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DENABLE_DEBUG_SYMBOLS=ON \
    -DWITH_TE=ON \
    -DWITH_STORE=OFF \
    -DWITH_STORE_RUST=OFF \
    -DWITH_STORE_GO=OFF \
    -DWITH_P2P_STORE=OFF \
    -DWITH_EP=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_UNIT_TESTS=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_BENCHMARK=OFF \
    -DUSE_TCP=ON \
    -DUSE_HTTP=ON \
    -DUSE_ETCD=OFF \
    -DUSE_REDIS=OFF \
    -DUSE_TENT=ON \
    -DTENT_METRICS_ENABLED=ON \
    -DUSE_CUDA=OFF \
    -DUSE_HIP=OFF \
    -DUSE_MUSA=OFF \
    -DUSE_MACA=OFF \
    -DUSE_MLU=OFF \
    -DUSE_ASCEND=OFF \
    -DUSE_ASCEND_DIRECT=OFF \
    -DUSE_UBSHMEM=OFF \
    -DUSE_ASCEND_HETEROGENEOUS=OFF \
    -DUSE_NVMEOF=OFF \
    -DUSE_EFA=OFF \
    -DUSE_CXI=OFF \
    -DUSE_UB=OFF \
    -DUSE_CXL=OFF \
    -DUSE_SUNRISE=OFF \
    -DUSE_TPU=OFF
  ```

  Expected: configuration reports TENT enabled and detects host TCP, shared
  memory, ibverbs RDMA, and liburing where supported; vendor SDK transports
  remain disabled.

- [ ] **Step 2: Build both required targets**

  Run:

  ```bash
  cmake --build build --target transfer_engine tent_shared --parallel 3
  ```

  Expected: both targets build without compiling C++ test binaries.

- [ ] **Step 3: Verify TENT ELF, C ABI, and dynamic dependencies**

  Run:

  ```bash
  test -f build/mooncake-transfer-engine/tent/src/libtent_shared.so
  file build/mooncake-transfer-engine/tent/src/libtent_shared.so
  readelf -h build/mooncake-transfer-engine/tent/src/libtent_shared.so | rg \
    'Class:.*ELF64|Machine:.*AArch64|Type:.*DYN'
  nm -D --defined-only build/mooncake-transfer-engine/tent/src/libtent_shared.so | rg \
    ' (tent_create_engine|tent_destroy_engine|tent_submit|tent_task_status)$'
  native_library_path=$(find "$PWD/build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    ldd -r build/mooncake-transfer-engine/tent/src/libtent_shared.so
  ```

  Expected: AArch64 shared object, required `tent_*` exports, no missing library,
  and no undefined symbol.

### Task 8: Execute TENT-native FFI and Python bindings

**Files:**
- Generate, ignored: `rust-repo/target/native-parity-tent/`
- Generate, ignored: `/tmp/mooncake-native-tent-*/`

**Interfaces:**
- Executes every discoverable FFI unit test against both native libraries.
- Exercises the Python binding with TENT native feature when the repository
  virtual environment contains maturin.

- [ ] **Step 1: Run the FFI library suite with `link-tent-native`**

  Run from `rust-repo`:

  ```bash
  native_library_path=$(find "$PWD/../build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  test -n "$native_library_path"
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR=target/native-parity-tent \
  RUSTFLAGS="-L native=../build/mooncake-transfer-engine/src -L native=../build/mooncake-transfer-engine/tent/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test -p transfer-engine-ffi --no-default-features \
      --features link-tent-native --lib
  ```

  Expected: exit zero, at least 67 tests executed, and no zero-test target.

- [ ] **Step 2: Build the Python binding with TENT native support**

  Run only when `/home/fy2462/Mooncake/.venv/bin/maturin` is executable:

  ```bash
  CARGO_INCREMENTAL=0 \
  CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR=target/native-parity-tent \
  RUSTFLAGS="-L native=../build/mooncake-transfer-engine/src -L native=../build/mooncake-transfer-engine/tent/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    /home/fy2462/Mooncake/.venv/bin/maturin develop --manifest-path python/Cargo.toml \
      --no-default-features --features link-tent-native
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    /home/fy2462/Mooncake/.venv/bin/python -m pytest python/tests -q
  ```

  Expected: binding build and selected Python tests pass. If maturin is absent,
  record the exact `repo-.venv-maturin` prerequisite rather than installing an
  unrelated Python tool globally.

### Task 9: Finish semantic review of every incomplete or N/A row

**Files:**
- Modify as evidence requires: `rust-repo/tools/store-validation/parity-map.json`
- Modify as evidence requires: `rust-repo/tools/store-validation/transfer-engine-parity-map.json`
- Modify as evidence requires: `rust-repo/tools/store-validation/tent-parity-map.json`
- Modify as evidence requires: `rust-repo/tools/store-validation/wheel-store-parity-map.json`
- Append, ignored: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Produces a final reviewed disposition for all 2,451 identities.
- Eliminates generic or stale reasons while preserving genuine semantic gaps
  and external prerequisites.

- [ ] **Step 1: Generate stable review queues for each suite and status**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  review_root=../../../.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/review-queues
  mkdir -p "$review_root"
  for manifest in parity-map.json transfer-engine-parity-map.json \
    tent-parity-map.json wheel-store-parity-map.json; do
    for row_status in missing blocked not-applicable; do
      /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
        --repo-root ../../.. --manifest "$manifest" --list-status "$row_status" \
        >"$review_root/${manifest%.json}-$row_status.txt"
    done
  done
  ```

  Expected: twelve stable queue files under the ignored progress directory.
  Process them by reference file, then reference test name; never stage the
  queue files.

- [ ] **Step 2: Review Store rows one reference file at a time**

  For each queued Store row, read the complete C++ test body and directly
  exercised implementation, inspect all discoverable approved Rust evidence,
  and decide covered, missing, blocked, or N/A from the complete result oracle.
  Do not use numeric-shape differences as gaps when source tracing proves one
  shared production branch, and do not erase a distinct error/result branch as
  mere scale.

- [ ] **Step 3: Review classic Transfer Engine rows one reference file at a time**

  Apply the same method to all 348 classic identities. Use freshly executed
  `transfer-engine-ffi` native evidence where complete. Keep direct private C++
  state N/A only when no Store or C ABI returns that value. Hardware absence
  blocks an existing complete Rust test; it does not excuse an absent test.

- [ ] **Step 4: Review TENT rows one reference file at a time**

  Apply the same method to all 352 TENT identities using `transfer-engine-ffi`
  and the Stage 2 execution results. Preserve private-runtime N/A boundaries,
  typed-language exclusions, build-conditioned rows, and genuine missing native
  lifecycle/status/data-integrity tests as distinct categories.

- [ ] **Step 5: Review selected wheel rows one Python test at a time**

  Read but never import or execute wheel reference modules. Use Rust Store and
  FFI results as evidence. Keep Python-only import/alias/configuration behavior
  N/A only where no Rust product boundary exists.

- [ ] **Step 6: Validate and commit every reference-file review batch**

  For every batch, run from `rust-repo/tools/store-validation`:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --repo-root ../../.. \
    --manifest parity-map.json \
    --manifest transfer-engine-parity-map.json \
    --manifest tent-parity-map.json \
    --manifest wheel-store-parity-map.json
  /home/fy2462/Mooncake/.venv/bin/python -m unittest discover \
    -s tests -p 'test_*.py' -v
  cd ../../..
  git diff --check
  SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
    --files rust-repo/tools/store-validation/parity-map.json \
    rust-repo/tools/store-validation/transfer-engine-parity-map.json \
    rust-repo/tools/store-validation/tent-parity-map.json \
    rust-repo/tools/store-validation/wheel-store-parity-map.json
  git diff --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  git diff --cached --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  ```

  Expected: all validators and tests pass; only the selected manifest rows
  change. Commit only the selected manifest files with:

  ```bash
  git commit -m "[Store] review parity disposition batch"
  ```

  Continue until every row has a specific source-verified reason and every
  claimed executable test has fresh result evidence.

- [ ] **Step 7: Stop on a genuine Rust semantic failure**

  Preserve the failing command and first-divergence diagnosis. Mark the row
  missing with that evidence and request explicit Rust remediation authority;
  do not edit production/test code under this review-only plan.

### Task 10: Run final acceptance and report remaining capabilities

**Files:**
- Generate, ignored: `/tmp/mooncake-native-final-*/`
- Append, ignored: `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Produces the final module, parity, validation, and immutable-source evidence.
- Distinguishes completed review from unavailable hardware coverage.

- [ ] **Step 1: Run all validation-tool tests and shell contracts**

  Run:

  ```bash
  cd rust-repo/tools/store-validation
  /home/fy2462/Mooncake/.venv/bin/python -m unittest discover \
    -s tests -p 'test_*.py' -v
  for test_script in tests/test_*.sh; do bash "$test_script"; done
  ```

  Expected: all Python tests and shell contracts pass.

- [ ] **Step 2: Run all four ordinary and strict validators**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --repo-root ../../.. \
    --manifest parity-map.json \
    --manifest transfer-engine-parity-map.json \
    --manifest tent-parity-map.json \
    --manifest wheel-store-parity-map.json
  for manifest in parity-map.json transfer-engine-parity-map.json \
    tent-parity-map.json wheel-store-parity-map.json; do
    set +e
    /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
      --repo-root ../../.. --manifest "$manifest" --require-complete
    strict_rc=$?
    set -e
    if [[ "$strict_rc" -ne 0 && "$strict_rc" -ne 1 ]]; then
      exit "$strict_rc"
    fi
  done
  ```

  Expected: no structural ERROR. A strict nonzero result is allowed only for
  explicitly reviewed missing/blocked rows.

- [ ] **Step 3: Run final classic module and four-manifest parity gates**

  Run from the worktree root:

  ```bash
  native_library_path=$(find "$PWD/build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  final_module_root=$(mktemp -d /tmp/mooncake-native-final-module.XXXXXX)
  MOONCAKE_TE_LIB_DIR="$PWD/build/mooncake-transfer-engine/src" \
  MOONCAKE_VALIDATION_CARGO_TARGET_DIR="$PWD/rust-repo/target/native-parity-final" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    rust-repo/tools/store-validation/run-module-gate.sh \
      --artifact-root "$final_module_root"

  cd rust-repo/tools/store-validation
  final_parity_root=$(mktemp -d /tmp/mooncake-native-final-parity.XXXXXX)
  RUSTFLAGS="-L native=$PWD/../../../build/mooncake-transfer-engine/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    /home/fy2462/Mooncake/.venv/bin/python run-parity-gate.py \
      --repo-root ../../.. \
      --artifact-root "$final_parity_root" \
      --manifest parity-map.json \
      --manifest transfer-engine-parity-map.json \
      --manifest tent-parity-map.json \
      --manifest wheel-store-parity-map.json
  ```

  Expected: every runnable command PASS, zero FAIL, zero zero-test record, and
  no row blocked merely because `-ltransfer_engine` is absent.

- [ ] **Step 4: Re-run TENT-native FFI and Python evidence**

  Run from `rust-repo` after recomputing the scoped path:

  ```bash
  native_library_path=$(find "$PWD/../build" -type f -name '*.so*' -printf '%h\n' | sort -u | paste -sd:)
  CARGO_TARGET_DIR=target/native-parity-final-tent \
  RUSTFLAGS="-L native=../build/mooncake-transfer-engine/src -L native=../build/mooncake-transfer-engine/tent/src" \
  LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test -p transfer-engine-ffi --no-default-features \
      --features link-tent-native --lib
  if [[ -x ../.venv/bin/maturin ]]; then
    CARGO_TARGET_DIR=target/native-parity-final-tent \
    RUSTFLAGS="-L native=../build/mooncake-transfer-engine/src -L native=../build/mooncake-transfer-engine/tent/src" \
    LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
      ../.venv/bin/maturin develop --manifest-path python/Cargo.toml \
        --no-default-features --features link-tent-native
    LD_LIBRARY_PATH="$native_library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
      ../.venv/bin/python -m pytest python/tests -q
  fi
  ```

  Expected: native FFI tests pass with a nonzero executed count; Python binding
  and tests pass when maturin is available. Retain exact logs and artifact paths.

- [ ] **Step 5: Run final formatting, diff, and immutable guards**

  Run from the worktree root:

  ```bash
  git diff --check
  SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
    --files docs/superpowers/specs/2026-07-31-transfer-engine-native-parity-execution-design.md \
    docs/superpowers/plans/2026-07-31-transfer-engine-native-parity-execution.md \
    rust-repo/tools/store-validation/parity-map.json \
    rust-repo/tools/store-validation/transfer-engine-parity-map.json \
    rust-repo/tools/store-validation/tent-parity-map.json \
    rust-repo/tools/store-validation/wheel-store-parity-map.json
  git diff --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  git diff --cached --exit-code -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
  git status --short
  ```

  Expected: clean tracked worktree after commits and no C++/wheel reference
  source change.

- [ ] **Step 6: Produce the final review report**

  Report:

  - classic and TENT library paths plus exact CMake flags;
  - ELF architecture, required-symbol, and `ldd -r` results;
  - executed PASS/FAIL/zero-test counts for FFI, Store Client, module, Python,
    and formal parity gates;
  - final covered/missing/blocked/N/A counts per suite and aggregate;
  - artifact paths for every retained gate;
  - every remaining external device, kernel, permission, or service
    prerequisite; and
  - every genuine missing Rust behavior awaiting separately authorized TDD.
