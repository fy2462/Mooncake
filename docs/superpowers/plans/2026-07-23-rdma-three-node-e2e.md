# Three-Node RDMA E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the standard privileged RDMA acceptance suite to three RXE,
TE, and Rust Store nodes with six direct TE directions, explicit 3/2/1 Store
replica profiles, multi-level cache proof, and rotating A/B/C resilience.

**Architecture:** Extend the existing manifest-driven host setup and Compose
topology from A/B to A/B/C, then make each gate consume the topology rather
than infer a two-node pair. Keep the standard three-replica correctness gate,
two-replica degraded-read gate, and one-replica tiering gate separate so each
result proves one behavior without masking another.

**Tech Stack:** Bash, Docker Compose, Linux RXE/rdma-core, Python asyncio,
pytest, Rust/Tokio, Cargo.

## Global Constraints

- Use `CARGO_BUILD_JOBS=5` for Cargo builds.
- Put compilation outputs under `/home/fy2462/workspace/tmp/mooncake/rdma-multinode`.
- Test runtime data may use host or container `/tmp`.
- Rust Store must not load or depend on the C++ Store shared library.
- Modify no files in the external Open-RDMA checkout.
- Preserve manifest-owned cleanup and leave no suite Docker, RXE, or veth resources.

---

### Task 1: Stabilize Empty-Cluster HA Promotion

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/hot_standby.rs`
- Test: `rust-repo/crates/mooncake-store-master/src/hot_standby.rs`

**Interfaces:**
- Consumes: `OpLogChangeNotifier::is_healthy() -> bool`.
- Produces: `wait_for_notifier_startup(&mut dyn OpLogChangeNotifier, Duration) -> bool`.

- [ ] **Step 1: Add the delayed-health regression test**

Create a notifier backed by `Arc<AtomicBool>`, set it healthy after 20 ms, and
assert the startup helper returns true within 100 ms.

- [ ] **Step 2: Verify the test fails for the missing helper**

Run:

```bash
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/rdma-multinode/cargo-test-target \
  cargo test -p mooncake-store-master \
  hot_standby::tests::test_notifier_startup_allows_health_initialization_grace_period
```

Expected: compilation fails because `wait_for_notifier_startup` is absent.

- [ ] **Step 3: Implement bounded notifier startup grace**

Add a helper that polls `is_healthy()` until the deadline with a 10 ms sleep.
Call it immediately after successful `notifier.start(...)`; only emit
`WatchBroken` when the grace expires.

- [ ] **Step 4: Verify HA tests**

Run the focused test and `cargo test -p mooncake-store-master test_ha` with the
same target and job settings. Expected: PASS.

- [ ] **Step 5: Commit the HA fix**

```bash
git add rust-repo/crates/mooncake-store-master/src/hot_standby.rs
git commit -m '[Fix] allow HA notifier startup grace'
```

### Task 2: Provision Three Manifest-Owned RXE Nodes

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/setup-host-rdma.sh`
- Modify: `rust-repo/tools/rdma-multinode/cleanup-host-rdma.sh`
- Test: `rust-repo/tools/rdma-multinode/tests/test_host_rdma_lifecycle.sh`

**Interfaces:**
- Consumes: host manifest path from `RDMA_ARTIFACT_ROOT`.
- Produces: manifest entries for A/B/C veth endpoints and RXE device names.

- [ ] **Step 1: Extend the lifecycle fake with C assertions**

Require creation, verbose `ibv_devinfo -v -d`, manifest output, and cleanup for
the C veth/RXE resources. Require `rdma link delete NAME`, never `NAME/1`.

- [ ] **Step 2: Run the lifecycle test and observe failure**

```bash
bash rust-repo/tools/rdma-multinode/tests/test_host_rdma_lifecycle.sh
```

Expected: FAIL because C resources are missing.

- [ ] **Step 3: Add C setup and cleanup using the existing manifest loop**

Use a third deterministic subnet and ensure rollback removes only resources
created by this invocation.

- [ ] **Step 4: Re-run the lifecycle test**

Expected: PASS including failure rollback and idempotent cleanup.

- [ ] **Step 5: Commit host topology changes**

```bash
git add rust-repo/tools/rdma-multinode/{setup-host-rdma.sh,cleanup-host-rdma.sh,tests/test_host_rdma_lifecycle.sh}
git commit -m '[Test] provision three RXE nodes'
```

### Task 3: Expand Compose and Direct TE Gate

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/compose.yaml`
- Modify: `rust-repo/tools/rdma-multinode/run-te-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/run.sh`
- Test: `rust-repo/tools/rdma-multinode/tests/test_compose_topology.sh`
- Test: `rust-repo/tools/rdma-multinode/tests/test_orchestration.sh`

**Interfaces:**
- Consumes: A/B/C host network namespaces and RXE devices.
- Produces: three TE services, three Store services, and six direction records
  in `te.result`.

- [ ] **Step 1: Assert C services and all directed TE paths**

Extend topology and orchestration tests to require `te-node-c`,
`store-node-c`, the shared runtime mount, and directions `a-b`, `b-a`, `b-c`,
`c-b`, `c-a`, `a-c`, each with `protocol=rdma` and `compare=OK`.

- [ ] **Step 2: Run shell contract tests and observe failure**

```bash
bash rust-repo/tools/rdma-multinode/tests/test_compose_topology.sh
bash rust-repo/tools/rdma-multinode/tests/test_orchestration.sh
```

Expected: FAIL on absent C services/directions.

- [ ] **Step 3: Add the C services and table-driven TE directions**

Reuse the existing service anchor and runtime bind. Make the TE gate loop over
the six source/destination pairs and fail if any transfer lacks RDMA or byte
comparison evidence.

- [ ] **Step 4: Re-run topology and orchestration tests**

Expected: PASS.

- [ ] **Step 5: Commit Compose and TE changes**

```bash
git add rust-repo/tools/rdma-multinode/{compose.yaml,run-te-gate.sh,run.sh,tests/test_compose_topology.sh,tests/test_orchestration.sh}
git commit -m '[Test] cover three-node TE topology'
```

### Task 4: Enforce Store 3/2/1 Replica Profiles

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/run-store-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/store-e2e.py`
- Modify: `rust-repo/tools/rdma-multinode/store-node.py`
- Test: `rust-repo/tools/rdma-multinode/tests/test_store_multilevel_contract.py`

**Interfaces:**
- Consumes: three Store node endpoints and `ReplicateConfig(replica_num)`.
- Produces: standard evidence with three endpoints, degraded inputs with two
  endpoints, and tiering evidence with one endpoint.

- [ ] **Step 1: Add failing three-node replica and capacity tests**

Require baseline helpers to observe three distinct complete RDMA Memory
replicas. Require tier pressure bytes to exceed
`3 * 128 MiB * 0.55`. Verify one-replica fallback restores its exact original
endpoint.

- [ ] **Step 2: Run the Python test and observe failure**

```bash
/home/fy2462/Mooncake/.venv/bin/python -m pytest -q \
  rust-repo/tools/rdma-multinode/tests/test_store_multilevel_contract.py
```

Expected: FAIL on two-node constants or replica assertions.

- [ ] **Step 3: Implement explicit standard and tier configurations**

Create standard `ReplicateConfig(3)`, tier `ReplicateConfig(1)`, and derive
pressure count from three-node aggregate capacity plus a safety margin. Start
node C with its own endpoint, ready/stats/command files, and `/tmp` storage.

- [ ] **Step 4: Re-run the full Store Python contract suite**

Expected: PASS with no reentrant use of one Python client.

- [ ] **Step 5: Commit Store profile changes**

```bash
git add rust-repo/tools/rdma-multinode/{run-store-gate.sh,store-e2e.py,store-node.py,tests/test_store_multilevel_contract.py}
git commit -m '[Test] validate Store three two one replica profiles'
```

### Task 5: Rotate Resilience Across A/B/C

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/store-resilience-e2e.py`
- Modify: `rust-repo/tools/rdma-multinode/run-store-gate.sh`
- Modify: `rust-repo/tools/rdma-multinode/run.sh`
- Test: `rust-repo/tools/rdma-multinode/tests/test_store_resilience_contract.py`
- Test: `rust-repo/tools/rdma-multinode/tests/test_store_gate_signal.sh`

**Interfaces:**
- Consumes: node command map and process handles for A/B/C.
- Produces: structured per-scenario evidence naming affected and surviving
  nodes, plus a seven-scenario PASS/FAIL result.

- [ ] **Step 1: Add failing C-node and rotation evidence tests**

Require command maps, health checks, cancellation cleanup, restart selection,
degraded owners, and RDMA reconnect ownership to accept all three node names.

- [ ] **Step 2: Run resilience tests and observe failure**

```bash
/home/fy2462/Mooncake/.venv/bin/python -m pytest -q \
  rust-repo/tools/rdma-multinode/tests/test_store_resilience_contract.py
bash rust-repo/tools/rdma-multinode/tests/test_store_gate_signal.sh
```

Expected: FAIL where A/B are hard-coded.

- [ ] **Step 3: Implement node maps and deterministic A/B/C rotation**

Pass C container and command through the gate, replace pair-specific branches
with node dictionaries, and retain strict evidence checks for every scenario.

- [ ] **Step 4: Re-run resilience and signal tests**

Expected: PASS and no stopper PID reuse.

- [ ] **Step 5: Commit resilience changes**

```bash
git add rust-repo/tools/rdma-multinode/{store-resilience-e2e.py,run-store-gate.sh,run.sh,tests/test_store_resilience_contract.py,tests/test_store_gate_signal.sh}
git commit -m '[Test] rotate Store resilience across three nodes'
```

### Task 6: Run Real Acceptance and Finish Evidence

**Files:**
- Modify: `rust-repo/tools/rdma-multinode/render-report.sh`
- Modify: `rust-repo/tools/rdma-multinode/README.md`
- Modify: `rust-repo/change_logs/2026-07-23-001.md`
- Test: `rust-repo/tools/rdma-multinode/tests/test_report.sh`

**Interfaces:**
- Consumes: verbs, six-direction TE, Store standard, Store resilience, and
  independent Open-RDMA results.
- Produces: retained report and documented three-node acceptance command.

- [ ] **Step 1: Update report contracts before report code**

Require the report to list all six TE directions, 3/2/1 Store evidence, all
resilience scenarios, independent mock classification, and empty final
inventory.

- [ ] **Step 2: Run all local contract and Rust tests**

Run every `tests/test_*.sh`, both Python contract modules, focused Store Master
HA tests, `cargo fmt --check`, and `git diff --check`. Expected: PASS.

- [ ] **Step 3: Run the canonical privileged suite**

```bash
sudo -v
CARGO_BUILD_JOBS=5 \
RDMA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/rdma-multinode \
bash rust-repo/tools/rdma-multinode/run.sh all
```

Expected: product gates PASS; Open-RDMA mock remains separately classified.

- [ ] **Step 4: Verify retained evidence and empty inventory**

Inspect JSON/results and confirm `docker ps -a`, `rdma link show`, and
`ip -brief link` contain no suite-owned resources.

- [ ] **Step 5: Update README/change log and run pre-commit on touched files**

Record the exact command, three-node topology, results, and independent mock
classification. Do not include unrelated hook rewrites.

- [ ] **Step 6: Commit final acceptance evidence**

```bash
git add rust-repo/tools/rdma-multinode rust-repo/change_logs/2026-07-23-001.md
git commit -m '[Test] certify three-node RDMA acceptance'
```
