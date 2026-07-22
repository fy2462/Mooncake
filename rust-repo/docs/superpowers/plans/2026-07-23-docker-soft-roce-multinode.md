# Docker Soft-RoCE Multi-Node Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a reproducible Docker Soft-RoCE environment where a passing two-node Transfer Engine RDMA Read/Write test is a hard prerequisite for Rust Master/Store cross-node tests, with Open-RDMA mock results reported separately.

**Architecture:** A host orchestrator creates a named Docker bridge and two privileged data-plane containers. Each container creates an RXE device on its own `eth0`; a verbs smoke gate proves the kernel path, the existing `rdma_transport_test` proves TE Read/Write, and only its machine-readable PASS marker unlocks a Rust Store scenario. If per-container RXE is rejected by the host's shared RDMA namespace, the orchestrator uses a clearly labeled host-network/shared-device fallback without permitting TCP fallback.

**Tech Stack:** Bash, Docker Compose v2, Ubuntu 24.04 ARM64, Linux `rdma_rxe`, rdma-core, perftest/rping, etcd 3.5, Mooncake Transfer Engine C++, Rust Store/PyO3, pytest, Open-RDMA Rust mock.

## Global Constraints

- Use `/home/fy2462/workspace/tmp/mooncake/rdma-multinode` for build trees, Docker build cache, logs, reports, pytest/cache data, and Open-RDMA Cargo target data.
- Use `CARGO_BUILD_JOBS=5` for every Cargo command.
- Gate order is strict: verbs PASS → TE PASS → Rust Store test. Store tests must never run after TE failure.
- Force `protocol=rdma`; a TCP fallback is a failed test even if data arrives.
- Rust Store may link only Transfer Engine/TENT through `transfer-engine-ffi`; it must not compile, link, or load `libmooncake_store.so`.
- Preserve the dirty checkout at `/home/fy2462/workspace/PFS/open-rdma-driver`; do not edit, stage, clean, reset, or commit it.
- Do not persist kernel-module, sysctl, firewall, Docker-daemon, or network configuration.
- Cleanup may remove only suite-owned names prefixed `mc-rdma-` and explicitly resolved RXE links.
- The primary topology requires separate container IPs, RXE device names, and GIDs. The fallback must be labeled `shared-rdma-device` in the report.

---

### Task 1: Define the orchestration contract and safe shell primitives

**Files:**
- Create: `rust-repo/tools/rdma-multinode/lib/common.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_common.sh`

**Interfaces:**
- Consumes: `MOONCAKE_ROOT`, optional `RDMA_ARTIFACT_ROOT`, Docker CLI.
- Produces: `require_command NAME`, `require_path PATH`, `record KEY VALUE`, `wait_for_log CONTAINER REGEX TIMEOUT`, `owned_name NAME`, and `run_if_te_passed RESULT_FILE COMMAND...`.

- [ ] **Step 1: Write failing shell contract tests**

Create a temporary fake `docker` executable and assertions that:

```bash
owned_name mc-rdma-te-a
! owned_name unrelated-container
printf 'status=FAIL\n' >"$tmp/te.result"
! run_if_te_passed "$tmp/te.result" touch "$tmp/store-ran"
test ! -e "$tmp/store-ran"
printf 'status=PASS\nprotocol=rdma\n' >"$tmp/te.result"
run_if_te_passed "$tmp/te.result" touch "$tmp/store-ran"
test -e "$tmp/store-ran"
```

Also assert `RDMA_ARTIFACT_ROOT` defaults to
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode` and that cleanup rejects
empty names, `/`, `~`, and names without `mc-rdma-`.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```text
bash rust-repo/tools/rdma-multinode/tests/test_common.sh
```

Expected: FAIL because `lib/common.sh` does not exist.

- [ ] **Step 3: Implement the minimal common library**

Use `set -Eeuo pipefail`, resolve `MOONCAKE_ROOT` from the script location,
validate every destructive target through `owned_name`, write evidence as
tab-separated `key<TAB>value`, and make `run_if_te_passed` require both
`status=PASS` and `protocol=rdma` before executing its command.

- [ ] **Step 4: Verify shell behavior and syntax**

Run:

```text
bash rust-repo/tools/rdma-multinode/tests/test_common.sh
bash -n rust-repo/tools/rdma-multinode/lib/common.sh
```

Expected: both exit zero.

- [ ] **Step 5: Commit**

```text
git commit -m "[Test] define RDMA multi-node gate contract"
```

### Task 2: Build the CPU/RDMA runtime and container image

**Files:**
- Create: `rust-repo/tools/rdma-multinode/Containerfile`
- Create: `rust-repo/tools/rdma-multinode/compose.yaml`
- Create: `rust-repo/tools/rdma-multinode/build-runtime.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_runtime_manifest.sh`

**Interfaces:**
- Consumes: Mooncake source, `.venv`, local `/usr/local` dependencies, Docker BuildKit.
- Produces: `${RDMA_ARTIFACT_ROOT}/runtime/{te,store}/`, image `mc-rdma-runtime:<git-sha>`, and `runtime-manifest.tsv`.

- [ ] **Step 1: Write a failing runtime-manifest test**

The test must reject a runtime missing any of:

```text
te/rdma_transport_test
te/libtransfer_engine.so
te/libtent_shared.so
store/mooncake-master
store/_mooncake_store.abi3.so
runtime-manifest.tsv entries for git_sha, architecture, cmake_flags, cargo_features
```

It must also run `readelf -d` on the Rust extension and fail if
`libmooncake_store.so` appears.

- [ ] **Step 2: Verify RED**

Run:

```text
RDMA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/rdma-multinode \
  bash rust-repo/tools/rdma-multinode/tests/test_runtime_manifest.sh
```

Expected: FAIL because `build-runtime.sh` and the manifest are absent.

- [ ] **Step 3: Implement the runtime build**

Configure the top-level CMake build at
`${RDMA_ARTIFACT_ROOT}/build/te` with:

```text
-DWITH_TE=ON
-DWITH_STORE=OFF
-DWITH_STORE_RUST=OFF
-DBUILD_UNIT_TESTS=ON
-DUSE_ETCD=ON
-DUSE_CUDA=OFF
-DUSE_TENT=ON
```

Build `rdma_transport_test`, `transfer_engine`, and `tent_shared` with `-j5`.
Build `mooncake-master` and the Python extension with
`CARGO_BUILD_JOBS=5` and `CARGO_TARGET_DIR=${RDMA_ARTIFACT_ROOT}/cargo-target`.
Copy only runtime files into `${RDMA_ARTIFACT_ROOT}/runtime`; do not install
Mooncake libraries under `/usr/local`.

- [ ] **Step 4: Define the ARM64-compatible image and services**

Base `Containerfile` on `ubuntu:24.04`. Install `rdma-core`,
`ibverbs-providers`, `librdmacm1`, `perftest`, `iproute2`, `kmod`, `numactl`,
`libgoogle-glog0v6`, `libgflags2.2`, `libjsoncpp25`, Python 3, and CA
certificates. Copy the staged runtime to `/opt/mooncake-rdma`.

Define fixed bridge addresses in `compose.yaml`:

```text
mc-rdma-etcd  10.89.10.10
mc-rdma-te-a  10.89.10.21
mc-rdma-te-b  10.89.10.22
mc-rdma-master 10.89.10.30
mc-rdma-store-a 10.89.10.31
mc-rdma-store-b 10.89.10.32
mc-rdma-client 10.89.10.40
```

Data-plane services receive `NET_ADMIN`, `SYS_ADMIN`, `IPC_LOCK`, unlimited
memlock, `/dev/infiniband`, and the suite artifact directory as a bind mount.

- [ ] **Step 5: Build and inspect the image**

Run:

```text
DOCKER_BUILDKIT=1 docker build \
  -t mc-rdma-runtime:$(git rev-parse --short HEAD) \
  -f rust-repo/tools/rdma-multinode/Containerfile \
  /home/fy2462/workspace/tmp/mooncake/rdma-multinode/runtime
docker run --rm mc-rdma-runtime:$(git rev-parse --short HEAD) \
  sh -lc 'uname -m; rdma --version; ibv_devices; test -x /opt/mooncake-rdma/te/rdma_transport_test'
```

Expected: ARM64, tools present, runtime executable present. `ibv_devices` may
be empty before Gate 1.

- [ ] **Step 6: Run the manifest test and commit**

Expected: manifest test passes and dependency audit has no C++ Store library.

```text
git commit -m "[Build] stage Docker RDMA validation runtime"
```

### Task 3: Create independent RXE devices and prove verbs connectivity

**Files:**
- Create: `rust-repo/tools/rdma-multinode/setup-rxe.sh`
- Create: `rust-repo/tools/rdma-multinode/run-verbs-gate.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_rxe_contract.sh`

**Interfaces:**
- Consumes: `RXE_DEVICE`, `RXE_NETDEV`, container bridge IP, `rdma` CLI.
- Produces: `rxe-a.json`, `rxe-b.json`, `verbs.result`, `verbs-server.log`, and `verbs-client.log`.

- [ ] **Step 1: Write failing RXE contract tests with fake commands**

Assert `setup-rxe.sh` issues exactly:

```text
rdma link add rxe-a type rxe netdev eth0
rdma link show rxe-a
ibv_devinfo -d rxe-a
```

for node A, refuses device names outside `rxe-[ab]`, and treats an inactive
port or missing GID as failure. Assert repeated setup first recognizes an
already-valid owned link instead of creating a duplicate.

- [ ] **Step 2: Verify RED, implement, and rerun**

Run the test before and after implementation. The implementation must capture
device, netdev, container IP, link state, GID, and MTU as JSON without using
`eval` or unresolved globs.

- [ ] **Step 3: Implement the verbs gate**

Start `rping -s -a 10.89.10.21 -v` on A and run
`rping -c -a 10.89.10.21 -I 10.89.10.22 -C 10 -v` on B. If Ubuntu's rping
package is unavailable, use `ib_write_bw -d rxe-a --report_gbits` on A and
`ib_write_bw -d rxe-b 10.89.10.21 --report_gbits` on B. Record the selected
tool. Require exit zero, ten completed exchanges or non-zero transferred
bytes, and no connection/error markers.

- [ ] **Step 4: Run the primary topology**

Load only the explicit module:

```text
sudo modprobe rdma_rxe
```

Start `mc-rdma-te-a` and `mc-rdma-te-b`, execute `setup-rxe.sh` inside each,
then execute the verbs gate. Record `rdma system show`, `rdma link`,
`ibv_devices`, `ibv_devinfo`, and container IPs before deciding PASS.

- [ ] **Step 5: Implement and test the documented fallback**

If and only if primary setup fails due to network-namespace/device visibility,
create explicitly named host RXE links `mc-rdma-rxe-a` and
`mc-rdma-rxe-b`, use host-network containers with distinct RPC ports, and set
`topology=shared-rdma-device`. Do not use fallback for a verbs data-transfer
failure after devices are active.

- [ ] **Step 6: Verify cleanup and commit**

After a forced test failure, require no `rxe-a`, `rxe-b`,
`mc-rdma-rxe-a`, or `mc-rdma-rxe-b` link remains unless it pre-existed and was
recorded as not owned.

```text
git commit -m "[Test] validate Docker Soft-RoCE verbs"
```

### Task 4: Make Transfer Engine RDMA the hard gate

**Files:**
- Create: `rust-repo/tools/rdma-multinode/run-te-gate.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_te_gate.sh`

**Interfaces:**
- Consumes: passing `verbs.result`, etcd endpoint, RXE device/IP pairs, staged `rdma_transport_test`.
- Produces: `te-target.log`, `te-initiator.log`, and `te.result` containing `status`, `protocol`, `target_device`, `initiator_device`, and `compare`.

- [ ] **Step 1: Write failing log-classification tests**

Fixtures must cover:

- PASS: target ready, initiator exit zero, `Remote segment protocol: rdma`,
  `Stage 1: Write Data`, `Stage 2: Read Data`, and `RDMA compare: OK`;
- FAIL: `protocol: tcp`, missing compare marker, timeout, transfer failure, or
  non-zero initiator exit;
- BLOCKED: verbs gate is not PASS.

Assert a failed/blocked fixture never invokes the Store command through
`run_if_te_passed`.

- [ ] **Step 2: Verify RED and implement the classifier**

The classifier writes `status=PASS` only when every required marker and exit
status is present. It must not infer RDMA merely from the binary name.

- [ ] **Step 3: Run target and initiator with explicit flags**

Target A:

```text
/opt/mooncake-rdma/te/rdma_transport_test \
  --mode=target --protocol=rdma --mem_backend=cpu \
  --metadata_server=10.89.10.10:2379 \
  --local_server_name=10.89.10.21:12345 \
  --device_name=rxe-a --buffer_size=67108864 --data_length=4194304 \
  --logtostderr=1
```

Initiator B:

```text
/opt/mooncake-rdma/te/rdma_transport_test \
  --mode=initiator --protocol=rdma --mem_backend=cpu \
  --metadata_server=10.89.10.10:2379 \
  --local_server_name=10.89.10.22:12346 \
  --segment_id=10.89.10.21:12345 --device_name=rxe-b \
  --expect_remote_location=cpu:0 \
  --buffer_size=67108864 --data_length=4194304 --logtostderr=1
```

Bound both processes with timeouts. Stop the target only after the initiator
finishes and logs are flushed.

- [ ] **Step 4: Verify a real PASS and negative gate**

Run the real gate once, then feed a copy of the log with the compare marker
removed into the classifier and require FAIL. Confirm the Store scenario is
not started for the negative result.

- [ ] **Step 5: Commit**

```text
git commit -m "[TransferEngine] gate Store E2E on two-node RDMA"
```

### Task 5: Run Rust Master and two Store nodes over RDMA

**Files:**
- Create: `rust-repo/tools/rdma-multinode/store-node.py`
- Create: `rust-repo/tools/rdma-multinode/store-e2e.py`
- Create: `rust-repo/tools/rdma-multinode/run-store-gate.sh`
- Create: `rust-repo/python/tests/test_rdma_multinode_scenario.py`

**Interfaces:**
- Consumes: `te.result=PASS`, Rust `MooncakeClient`, Master address, etcd endpoint, node IP/device.
- Produces: two live registered Store clients, `store.result`, node/client logs, and byte-for-byte assertions.

- [ ] **Step 1: Write failing pure-Python scenario tests**

Use an async fake client to assert the scenario executes, in order:

```text
put/get 4 KiB deterministic payload
put/get 12 MiB deterministic multi-slice payload
three repeated reads
exists/remove(force=True)/not-exists
close
```

Assert any mismatched byte, missing remote RDMA replica evidence, or failed
remove returns a non-zero scenario result.

- [ ] **Step 2: Verify RED and implement scenario logic**

Keep payload generation deterministic without storing another 12 MiB copy:
generate each byte as `(index * 31 + 7) & 0xff` and validate length plus
SHA-256. Expose `run_scenario(client) -> int` for unit tests and a CLI wrapper
for the container.

- [ ] **Step 3: Implement long-lived Store nodes**

Each node calls:

```python
await MooncakeClient.create(
    local_hostname=node_ip,
    metadata_server="10.89.10.10:2379",
    master_server_addr="10.89.10.30:50051",
    protocol="rdma",
    device=rxe_device,
    global_segment_size=128 * 1024 * 1024,
    local_buffer_size=32 * 1024 * 1024,
)
```

Node readiness requires health/ping success plus registered-segment metadata
showing protocol `rdma`. The two nodes use distinct IP/device values.

- [ ] **Step 4: Start services only through the TE gate**

Use `run_if_te_passed te.result` to start Master, Store A, Store B, and client.
Run Master with explicit RPC/metadata/metrics ports and `--enable-nof=false`.
Wait for Master health and both node-ready markers with bounded timeouts.

- [ ] **Step 5: Prove a remote replica was used**

Before each client read, query replica descriptors through the Rust Python API
and require at least one complete memory replica owned by the other Store node
with `protocol == "rdma"`. Record the remote segment name and node in
`store.result`; local-only success is a failed multi-node test.

- [ ] **Step 6: Run unit and real tests**

Run:

```text
PYTHONPATH=rust-repo/python \
PYTHONPYCACHEPREFIX=/home/fy2462/workspace/tmp/mooncake/rdma-multinode/pycache \
/home/fy2462/Mooncake/.venv/bin/python -m pytest \
  rust-repo/python/tests/test_rdma_multinode_scenario.py -q \
  --basetemp=/home/fy2462/workspace/tmp/mooncake/rdma-multinode/pytest
```

Then run the container gate and require `status=PASS`, both payload hashes,
three repeat-read markers, remote node/protocol evidence, and removal success.

- [ ] **Step 7: Audit dependencies and commit**

Run `ldd` on `_mooncake_store.abi3.so`; require TE/TENT and reject
`libmooncake_store.so`. Search the new scripts for `mooncake.store` imports.

```text
git commit -m "[Store] verify two-node Rust Store over RDMA"
```

### Task 6: Add Open-RDMA mock evidence and final report generation

**Files:**
- Create: `rust-repo/tools/rdma-multinode/run-open-rdma-mock.sh`
- Create: `rust-repo/tools/rdma-multinode/render-report.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_report.sh`
- Create: `rust-repo/change_logs/2026-07-23-001.md`

**Interfaces:**
- Consumes: gate result files/logs and dirty external Open-RDMA checkout.
- Produces: `open-rdma.result`, `report.md`, and a committed change log containing no credentials or absolute temporary session IDs.

- [ ] **Step 1: Write failing report tests**

Fixtures must prove the renderer rejects:

- Store PASS with TE FAIL or missing TE result;
- TE PASS without `protocol=rdma`;
- an unlabeled shared-device fallback;
- a report claiming Open-RDMA mock is cross-node evidence;
- cleanup evidence that still lists suite-owned resources.

- [ ] **Step 2: Verify RED and implement the renderer**

The report contains host/kernel/Docker versions, topology, device/GID table,
exact commands, gate status and durations, TE compare evidence, Store hashes
and remote replica, Open-RDMA revision/dirty state/results, dependency audit,
cleanup inventory, and paths to retained logs.

- [ ] **Step 3: Run Open-RDMA without altering its checkout**

Record `git status --short` and revision before and after. Run:

```text
cd /home/fy2462/workspace/PFS/open-rdma-driver/rust-driver
CARGO_BUILD_JOBS=5 \
CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/rdma-multinode/open-rdma-target \
cargo test --no-default-features --features mock --lib -- --nocapture
```

Require the before/after status text to be identical. Report failures as
Open-RDMA mock failures without changing Soft-RoCE gate results.

- [ ] **Step 4: Generate and review the change log**

Populate `rust-repo/change_logs/2026-07-23-001.md` from the fresh report. Do
not hand-edit pass counts that disagree with machine-readable result files.

- [ ] **Step 5: Commit**

```text
git commit -m "[Test] report RDMA multi-node validation"
```

### Task 7: Provide one idempotent entrypoint and run final verification

**Files:**
- Create: `rust-repo/tools/rdma-multinode/run.sh`
- Create: `rust-repo/tools/rdma-multinode/README.md`
- Modify: `rust-repo/docs/superpowers/specs/2026-07-23-docker-soft-roce-multinode-design.md`

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces: `run.sh [preflight|build|verbs|te|store|open-rdma|all|cleanup]`, documented reproduction commands, and a clean host inventory.

- [ ] **Step 1: Write a failing orchestration-order test**

With fake gate commands, assert `run.sh all` invokes exactly:

```text
preflight build verbs te store open-rdma report cleanup
```

Assert verbs failure skips TE and Store; TE failure skips Store but still runs
Open-RDMA, report, and cleanup; signals and command failures always run cleanup.

- [ ] **Step 2: Verify RED and implement `run.sh`**

Use traps for `EXIT INT TERM`, preserve the original exit status, retain logs,
and make cleanup idempotent. `cleanup` resolves and removes only explicit
Compose project `mc-rdma`, network `mc-rdma-net`, containers beginning
`mc-rdma-`, and owned RXE links recorded in the current run manifest.

- [ ] **Step 3: Document operator workflow**

README must distinguish primary versus fallback topology, state required sudo
actions (`modprobe` and explicit RDMA link lifecycle), show each subcommand,
explain result files, and state that TE PASS is required before Store.

- [ ] **Step 4: Run the complete suite fresh**

Run:

```text
CARGO_BUILD_JOBS=5 \
RDMA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/rdma-multinode \
bash rust-repo/tools/rdma-multinode/run.sh all
```

Expected: verbs PASS, TE PASS with RDMA compare OK, Store PASS with remote
RDMA replica and all content checks, Open-RDMA mock result recorded, report
generated, cleanup PASS.

- [ ] **Step 5: Run repository verification**

Run shell tests, focused Python tests, `cargo test -p transfer-engine-ffi`,
`cargo test -p mooncake-store-client`, formatting, `git diff --check`, and
pre-commit on touched files. If the repository-wide format hook rewrites
unrelated files, reverse only those hook-generated diffs and do not stage them.

- [ ] **Step 6: Verify post-cleanup inventory**

Require all of these to return empty for suite-owned names:

```text
docker ps -a --format '{{.Names}}' | grep '^mc-rdma-'
docker network ls --format '{{.Name}}' | grep '^mc-rdma-'
rdma link show | grep -E 'rxe-[ab]|mc-rdma-rxe-[ab]'
```

Do not unload `rdma_rxe` if it was loaded before the run or if any non-suite
RXE device uses it.

- [ ] **Step 7: Commit**

```text
git commit -m "[Test] automate Docker RDMA multi-node validation"
```
