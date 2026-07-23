# Standard Compose Shared-RXE E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the obsolete independent-container RXE Compose topology with the validated host-network shared-RXE topology and make `run.sh all` the canonical acceptance test.

**Architecture:** Host lifecycle scripts create and remove one suite-owned `mc-rdma-rxe` over an explicitly named veth pair. Compose owns etcd, two TE processes, Rust Master, two Rust Store segment owners, and the Store test client, all on the host network. Machine-readable gates enforce verbs → TE → Store ordering, while cleanup always runs.

**Tech Stack:** Bash, Docker Compose, Linux `ip`/`rdma`, Soft-RoCE (`rdma_rxe`), C++ Transfer Engine test binary, Rust Store Python bindings, pytest.

## Global Constraints

- Put compilation-related dependency caches, build output, Cargo targets, and generated binaries/libraries under `/home/fy2462/workspace/tmp/mooncake/rdma-multinode`. Test-runtime state and working directories may use system/container `/tmp`. Retain acceptance logs and machine-readable results under the artifact root.
- Use `CARGO_BUILD_JOBS=5`.
- Use interactive `sudo`; never store, accept, or log a password.
- Delete only manifest-recorded suite-owned `mc-rdma-*` resources.
- TE must PASS with `protocol=rdma`, Write, Read, and compare evidence before Store starts.
- Rust Store must not load or run C++ `libmooncake_store.so`.
- Both Store result groups, `standard` and `resilience`, are mandatory for full E2E acceptance.
- Multi-level cache evidence must prove DRAM/SSD transition and byte-identical fallback/promotion, not merely that an SSD file exists.
- Open-RDMA mock is independent and never counts as cross-node data-plane evidence.

---

### Task 1: Add safe host shared-RXE lifecycle

**Files:**
- Create: `rust-repo/tools/rdma-multinode/setup-host-rdma.sh`
- Create: `rust-repo/tools/rdma-multinode/cleanup-host-rdma.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_host_rdma_lifecycle.sh`
- Retire: `rust-repo/tools/rdma-multinode/setup-rxe.sh`
- Retire: `rust-repo/tools/rdma-multinode/tests/test_rxe_contract.sh`

**Interfaces:**
- Consumes: `RDMA_ARTIFACT_ROOT`, `sudo`, `modprobe`, `ip`, `rdma`, `ibv_devinfo`.
- Produces: `host-rdma.env`, `host-rdma.json`, device `mc-rdma-rxe`, veth names `mc-rdma-net-a`/`mc-rdma-net-b`, address `10.90.0.1/30`.

- [ ] **Step 1: Write the failing lifecycle contract test**

Use fake commands and assert setup issues module load, veth creation/address/up, RXE creation, active/GID validation, and an atomic ownership manifest. Assert cleanup reads that manifest and deletes only `mc-rdma-rxe` and `mc-rdma-net-a`. Assert repeated setup/cleanup is idempotent and repository scripts contain no password-input option.

- [ ] **Step 2: Run RED**

Run `bash rust-repo/tools/rdma-multinode/tests/test_host_rdma_lifecycle.sh`.
Expected: FAIL because `setup-host-rdma.sh` is absent.

- [ ] **Step 3: Implement minimal lifecycle scripts**

Setup must use separate `sudo` commands so the terminal owns authentication, validate names before mutation, record `owned_rxe` and `owned_veth`, and write the manifest only after the device is ACTIVE with a non-empty GID. Cleanup must tolerate absent resources and remove only entries marked owned.

- [ ] **Step 4: Run GREEN and syntax checks**

Run the lifecycle test plus `bash -n` on both scripts. Expected: PASS.

- [ ] **Step 5: Commit**

Commit as `[Test] manage standard shared RXE lifecycle`.

### Task 2: Make Compose the sole container owner

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/compose.yaml`
- Create: `rust-repo/tools/rdma-multinode/tests/test_compose_topology.sh`

**Interfaces:**
- Consumes: `RDMA_IMAGE`, `RDMA_ARTIFACT_ROOT`, `/dev/infiniband`, host ports.
- Produces services `etcd`, `te-node-a`, `te-node-b`, `rust-master`, `store-node-a`, `store-node-b`, `store-test-client`.

- [ ] **Step 1: Write the failing Compose contract test**

Render `docker compose config --format json` and assert all seven service names exist, every data-plane service uses host networking, no `10.89.10.*` fixed address or `mc-rdma-net` exists in the rendered Compose model, and `/dev/infiniband` plus artifacts are mounted. Gate-script fallback names are asserted and removed in Task 3, whose file scope owns those scripts.

- [ ] **Step 2: Run RED**

Expected: FAIL against the current bridge topology and old service names.

- [ ] **Step 3: Rewrite Compose**

Use host networking, shared runtime anchors, health checks where available, standard container names, and sleep commands so gate scripts own process lifetime. Etcd advertises `127.0.0.1:2379`.

- [ ] **Step 4: Run GREEN**

Run the contract test and `docker compose config`. Expected: PASS.

- [ ] **Step 5: Commit**

Commit as `[Test] standardize Compose shared-RXE topology`.

### Task 3: Migrate verbs, TE, and Store gates to standard services

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/run-verbs-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/run-te-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/run-store-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/tests/test_te_gate.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_standard_gate_contract.sh`

**Interfaces:**
- Consumes: Compose standard service/container names, `host-rdma.env`, `mc-rdma-rxe`, localhost metadata/master endpoints.
- Produces unchanged `verbs.result`, `te.result`, and `store.result` schemas.

- [ ] **Step 1: Write failing gate contract tests**

Assert defaults use Compose standard containers and `mc-rdma-rxe`, both verbs processes share the same device, TE nodes use distinct logical ports, Store launches only Compose-owned services, and no `docker run`, fallback name, `rxe-a`, or `rxe-b` remains.

- [ ] **Step 2: Run RED**

Expected: FAIL on current fallback defaults.

- [ ] **Step 3: Update gate defaults and process handling**

Use localhost/host-veth endpoints and distinct ports, copy only scenario scripts needed by Store, preserve bounded timeouts, and kill only `docker exec` processes created by the current gate.

- [ ] **Step 4: Run GREEN and negative gates**

Run gate contract tests and existing TE/report tests. Remove the compare marker fixture and require TE FAIL; require Store BLOCKED.

- [ ] **Step 5: Commit**

Commit as `[Test] run all RDMA gates through Compose`.

### Task 4: Make `run.sh all` canonical and failure-safe

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/run.sh`
- Create: `rust-repo/tools/rdma-multinode/tests/test_orchestration.sh`
- Modify: `rust-repo/tools/rdma-multinode/README.md`
- Modify: `rust-repo/tools/rdma-multinode/render-report.sh`

**Interfaces:**
- Consumes: Tasks 1-3 scripts and gate result files.
- Produces ordered stages `preflight host-rdma-setup build compose-up verbs te store-standard store-resilience open-rdma report compose-down host-rdma-cleanup`.

- [ ] **Step 1: Write failing orchestration tests**

Inject fake stage commands and assert exact success order. Assert verbs failure skips TE/Store; TE failure skips both Store groups; standard Store failure blocks dependent resilience scenarios; Open-RDMA/report still run when safe; cleanup runs once for success, failure, `INT`, and `TERM`.

- [ ] **Step 2: Run RED**

Expected: FAIL because current `all` neither provisions RDMA nor starts Compose.

- [ ] **Step 3: Implement orchestration**

Add explicit `host-rdma-setup`, `compose-up`, `compose-down`, and `host-rdma-cleanup` commands. Use one trap that preserves the original status. Ensure cleanup is idempotent and does not hide gate failure.

- [ ] **Step 4: Update documentation and report validation**

Document the interactive sudo prompt, standard Compose service roles, physical-host limitation, standard/resilience result files, diagnostic subcommands, and exact acceptance command. Require reports to name `compose-shared-rdma-device` and to distinguish product, resilience, and mock evidence.

- [ ] **Step 5: Run GREEN and commit**

Run orchestration/report tests. Commit as `[Test] automate standard Compose RDMA acceptance`.

### Task 5: Add multi-level cache and resilience scenarios

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/store-e2e.py`
- Create: `rust-repo/tools/rdma-multinode/store-resilience-e2e.py`
- Create: `rust-repo/tools/rdma-multinode/tests/test_store_multilevel_contract.py`
- Create: `rust-repo/tools/rdma-multinode/tests/test_store_resilience_contract.py`
- Modify: `rust-repo/tools/rdma-multinode/run-store-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/render-report.sh`

**Interfaces:**
- Consumes: healthy standard Compose topology, Rust Store SSD backend, two RDMA replicas, bounded scenario timeouts.
- Produces: `store-standard.result`, `store-resilience.result`, per-scenario JSON and logs.

- [ ] **Step 1: Write failing standard multi-level cache contracts**

Require deterministic small, large, and cross-slice objects; concurrent put/get; overwrite/delete; DRAM-to-SSD offload or eviction; byte-identical SSD fallback; memory promotion/restore; and two-replica metadata consistency. Assert the Rust process never loads C++ `libmooncake_store.so`.

- [ ] **Step 2: Write failing resilience contracts**

Require bounded Store-node restart, Master/etcd restart and recovery, RDMA link interruption/reconnection, one-owner degraded read, memory/SSD watermark eviction, concurrent mixed-size stress, and a second complete standard execution. Each scenario must emit machine-readable status and preserve diagnostic logs.

- [ ] **Step 3: Implement the scenario runner and Store gate integration**

Use only Compose-owned services. Restore every intentionally disrupted service/link before the next scenario, preserve the first failure, and make cleanup safe after partial execution. A capability-based SKIP is reported but does not satisfy full acceptance.

- [ ] **Step 4: Run focused contracts and negative cases**

Prove corrupted fallback data, missing promotion evidence, failed reconnection, unavailable replica without valid degraded read, and timeout all fail their scenario and the relevant Store result group.

- [ ] **Step 5: Commit**

Commit as `[Test] cover multi-level and resilient Store E2E`.

### Task 6: Execute clean E2E acceptance and repository verification

**Files:**
- Modify: `rust-repo/change_logs/2026-07-23-001.md`
- Modify: `rust-repo/docs/superpowers/specs/2026-07-23-compose-shared-rxe-e2e-design.md` only if execution reveals a required clarification.

**Interfaces:**
- Consumes: canonical `run.sh all`.
- Produces: fresh gate logs/results/report and an empty suite resource inventory.

- [ ] **Step 1: Verify an empty pre-run inventory**

Require no `mc-rdma-*` container/network/RDMA/veth resource.

- [ ] **Step 2: Run the canonical acceptance command**

Run `CARGO_BUILD_JOBS=5 RDMA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/rdma-multinode bash rust-repo/tools/rdma-multinode/run.sh all` and enter the sudo password only at the terminal prompt.

Expected product gates: verbs PASS, TE RDMA Write/Read/compare PASS, Rust Store standard PASS, and Rust Store resilience PASS. Standard evidence includes dual replicas, concurrent/mixed-size correctness, and a complete DRAM/SSD offload-fallback-promotion loop. Resilience evidence includes restart/recovery, RDMA interruption/reconnection, degraded read, watermark eviction, stress, and repeated execution. Record Open-RDMA independently.

- [ ] **Step 3: Verify cleanup inventory**

Require no `mc-rdma-*` container/network/RDMA/veth resource after `all` exits.

- [ ] **Step 4: Run focused and repository checks**

Run all shell contract tests, the RDMA Python scenario test, `cargo test -p transfer-engine-ffi -p mooncake-store-client`, `cargo fmt --check`, `git diff --check`, dependency audit, and pre-commit on touched files. Revert only unrelated hook rewrites.

- [ ] **Step 5: Update evidence and commit**

Update the change log from machine-readable results and commit as `[Test] certify standard Compose RDMA E2E`.
