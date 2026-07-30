# TENT 319-Test Semantic Parity Audit Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Execute
> inline in the existing worktree; do not delegate source review to subagents.

**Goal:** Produce a source-reviewed schema-v2 parity manifest for every one of
the 319 GoogleTest declarations under `mooncake-transfer-engine/tent/tests`,
using complete Rust-observable evidence from `transfer-engine-ffi` and the
approved Store crates, then establish the exact covered/missing/blocked/N/A
baseline before any Rust remediation.

**Architecture:** Freeze the 319-test source inventory, audit nine stable
behavior groups, and record complete executed C++ oracles in ignored local
drafts. A Rust test may support many reference rows and several Rust tests may
jointly support one row. Materialize the tracked manifest only after all 319
identities have been reviewed exactly once.

**Tech Stack:** Read-only C++ source inspection, Rust source/test inspection,
Python 3 source-only discovery and schema validation, JSON schema-v2 parity
manifests, Cargo test planning without execution of the reference suite, Git,
and repository pre-commit hooks.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- Never edit, format, build, link, load, execute, stage, or commit C/C++ files.
- Never edit or execute files beneath `mooncake-wheel/tests` in this phase.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling.
- Use `apply_patch` for semantic repository edits. A deterministic bulk JSON
  render may mechanically order already-reviewed rows, but must not invent
  dispositions, behavior descriptions, reasons, or evidence mappings.
- Every pre-commit invocation sets `SKIP=mooncake-code-format`.
- Approved Rust evidence packages are `mooncake-store-client`,
  `mooncake-store-core`, `mooncake-store-master`, and
  `transfer-engine-ffi`. Rust `transfer-engine-ffi` tests are valid TENT
  evidence when their assertions prove the complete C++ oracle.
- `covered` requires complete assertion equivalence. ABI shape, enum values,
  shared implementation, comments, or partial policy assertions are not
  sufficient.
- `missing` keeps `rust: []` and records the exact unproved assertion or
  scenario plus any partial Rust evidence.
- `blocked` requires complete discoverable Rust evidence and a named external
  prerequisite. Missing hardware cannot turn absent Rust logic into blocked.
- `not-applicable` requires a validator-approved category and source proof that
  no suitable Rust-observable Store/TENT FFI product boundary exists.
- Do not change Rust behavior until the 319-row manifest validates.
- Store LocalDisk `io_uring` remains gated until Store, Transfer Engine, TENT,
  and wheel correctness audits and applicable remediation all pass.

---

### Task 1: Freeze TENT and Rust evidence inventories

**Files:**
- Read: `mooncake-transfer-engine/tent/tests/**/*.cpp`
- Read: `rust-repo/crates/transfer-engine-ffi/**/*.rs`
- Update locally:
  `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Consumes: trusted suite definition `SUITES["tent-cpp"]`.
- Produces: exact 319-reference identity set and approved Rust evidence set.

- [ ] Run `discover_suite_tests` and assert 319 unique GoogleTest identities
      across 35 files.
- [ ] Run `discover_rust_tests` over `rust-repo/crates` and retain only the four
      approved package identities.
- [ ] Record the nine audit groups below and assert their total is 319.
- [ ] Record both staged and unstaged immutable-reference diffs for
      `mooncake-transfer-engine` and `mooncake-wheel/tests`.

| Group | Tests |
|---|---:|
| Admission, bandwidth arbitration, causal chains, and coalescing | 39 |
| Intent, QoS, receiver credit, merge, progress, and queue dispatch | 54 |
| Metrics configuration, HTTP exposure, and recording | 39 |
| Engine configuration overrides, transport hints, and selection | 46 |
| Endpoint lifecycle/store, failover, fault proxy, and rail monitor | 63 |
| QP layout, RDMA cancellation, and RDMA transport | 17 |
| IP/segment/platform and SHM/Sunrise/TCP transports | 39 |
| Thread-local storage and RW spin lock | 10 |
| TPU PJRT shim and transport | 12 |

### Task 2: Audit admission, arbitration, causal, and coalescing behavior

**Files:**
- Read: `admission_queue_test.cpp` (22)
- Read: `bw_arbitration_test.cpp` (3)
- Read: `causal_chain_test.cpp` (4)
- Read: `coalesce_regions_test.cpp` (10)
- Create locally: `tent-admission-arbitration-audit.md`

**Interfaces:**
- Consumes: TENT scheduler/admission C++ test bodies and Rust TENT request
  option/priority evidence.
- Produces: 39 reviewed draft rows.

- [ ] Trace deadlines, promotion, queue capacity, ordering, bandwidth weights,
      causal dependencies, cancellation, and region merge/split assertions.
- [ ] Require exact boundary values and ordering for covered rows; enum or ABI
      layout checks alone remain partial.
- [ ] Verify the draft identity set is exactly the 39 discovered references.

### Task 3: Audit intent, QoS, credit, merge, progress, and dispatch behavior

**Files:**
- Read: `intent_type_test.cpp` (6)
- Read: `qos_contract_test.cpp` (10)
- Read: `receiver_credit_test.cpp` (14)
- Read: `request_merge_test.cpp` (6)
- Read: `progress_worker_test.cpp` (6)
- Read: `runtime_queue_dispatch_test.cpp` (12)
- Create locally: `tent-qos-runtime-audit.md`

**Interfaces:**
- Consumes: TENT intent/priority/request v2 Rust types and native queue tests.
- Produces: 54 reviewed draft rows; cumulative 93.

- [ ] Review intent conversion, QoS contract defaults/overrides, credit
      accounting, merge safety, worker progress, queue dispatch, cancellation,
      terminal status, and concurrency oracles.
- [ ] Separate Rust request-construction evidence from native scheduler
      execution; do not infer behavior from identical enum numbers.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 4: Audit metrics behavior

**Files:**
- Read: `metrics_config_loader_test.cpp` (25)
- Read: `metrics_http_server_test.cpp` (2)
- Read: `metrics_recording_test.cpp` (12)
- Create locally: `tent-metrics-audit.md`

**Interfaces:**
- Consumes: TENT metrics C ABI and Rust `TentMetricsStatus`/NIC-stat tests.
- Produces: 39 reviewed draft rows; cumulative 132.

- [ ] Review configuration precedence and invalid inputs, HTTP lifecycle and
      payloads, metric names/labels/counters, log-only fallback, availability,
      and NIC-stat decoding.
- [ ] Treat Rust status/decoder tests as evidence only for the exact fields and
      errors they assert, not for server or recorder side effects.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 5: Audit configuration, hints, and transport selection

**Files:**
- Read: `transfer_engine_config_override_test.cpp` (10)
- Read: `transport_hint_test.cpp` (5)
- Read: `transport_selector_test.cpp` (31)
- Create locally: `tent-config-selector-audit.md`

**Interfaces:**
- Consumes: TENT configuration C API, Rust `TentTransport`, and Rust transport
  hint conversion tests.
- Produces: 46 reviewed draft rows; cumulative 178.

- [ ] Review precedence, parsing, invalid/empty values, provider choice,
      fallback ordering, locality, capability, and explicit-hint behavior.
- [ ] Distinguish public selection results from C++-private selector scoring or
      candidate lists that the ABI cannot return.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 6: Audit endpoint and failover behavior

**Files:**
- Read: `endpoint_lifecycle_test.cpp` (14)
- Read: `endpoint_store_test.cpp` (4)
- Read: `engine_failover_e2e_test.cpp` (16)
- Read: `failover_test.cpp` (14)
- Read: `fault_proxy_test.cpp` (8)
- Read: `rail_monitor_test.cpp` (7)
- Create locally: `tent-endpoint-failover-audit.md`

**Interfaces:**
- Consumes: TENT open/close/transfer/status/cancel FFI methods and native
  endpoint/fault injection helpers.
- Produces: 63 reviewed draft rows; cumulative 241.

- [ ] Review endpoint construction/destruction, concurrent reuse, stale state,
      failover ordering, retry budgets, proxy faults, rail health transitions,
      data/status continuity, and cleanup.
- [ ] Preserve full end-to-end outcomes as missing when no Rust test exists;
      mark direct fault-injector/private state only N/A when the ABI has no
      observable result.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 7: Audit QP and RDMA behavior

**Files:**
- Read: `qp_pool_layout_test.cpp` (12)
- Read: `rdma_cancel_test.cpp` (2)
- Read: `rdma_transport_test.cpp` (3)
- Create locally: `tent-rdma-audit.md`

**Interfaces:**
- Consumes: TENT batch/cancel/status FFI and private QP layout state.
- Produces: 17 reviewed draft rows; cumulative 258.

- [ ] Review QP pool sizing/indexing/isolation, cancel races and terminal
      status, native RDMA registration/transfer/data integrity, and cleanup.
- [ ] Do not use unavailable RNIC hardware as blocked unless the complete Rust
      test already exists.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 8: Audit utilities, platform, segments, and compact transports

**Files:**
- Read: `ip_utils_test.cpp` (14)
- Read: `segment_manager_test.cpp` (7)
- Read: `rocm_platform_test.cpp` (13)
- Read: `shm_transport_test.cpp` (1)
- Read: `sunrise_link_transport_test.cpp` (3)
- Read: `tcp_transport_test.cpp` (1)
- Create locally: `tent-platform-transports-audit.md`

**Interfaces:**
- Consumes: public TENT segment/register/transfer FFI and platform/private
  utility helpers.
- Produces: 39 reviewed draft rows; cumulative 297.

- [ ] Review IP parsing/normalization, segment ownership/lifetime, ROCm pointer
      and device behavior, SHM/Sunrise/TCP transfer statuses, byte integrity,
      invalid ranges, and cleanup.
- [ ] Classify helper-only values N/A only when no Rust-observable FFI product
      result exists.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 9: Audit TLS and spin-lock behavior

**Files:**
- Read: `thread_local_storage_test.cpp` (8)
- Read: `rw_spinlock_test.cpp` (2)
- Create locally: `tent-concurrency-primitives-audit.md`

**Interfaces:**
- Consumes: C++ helper implementations and any exact Rust wrapper boundary.
- Produces: 10 reviewed draft rows; cumulative 307.

- [ ] Review thread identity/isolation/destruction and reader/writer exclusion
      or progress assertions.
- [ ] Use N/A for pure C++ template/runtime constructs only when no ABI exposes
      the value or effect.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 10: Audit TPU behavior

**Files:**
- Read: `tpu/tpu_pjrt_shim_test.cpp` (6)
- Read: `tpu/tpu_transport_test.cpp` (6)
- Create locally: `tent-tpu-audit.md`

**Interfaces:**
- Consumes: TENT TPU transport selector/transfer FFI and PJRT shim internals.
- Produces: 12 reviewed draft rows and the final 319-row exact set.

- [ ] Review shim initialization/error mapping, device/buffer handling,
      registration, transfer/status/data integrity, and cleanup.
- [ ] Separate private PJRT shim state from TENT FFI-observable transport
      outcomes; do not use missing TPU hardware as false blocked coverage.
- [ ] Assert exactly 319 unique reviewed identities, zero omissions, zero stale
      rows, and zero duplicates.

### Task 11: Materialize and validate the TENT manifest

**Files:**
- Create: `rust-repo/tools/store-validation/tent-parity-map.json`
- Update locally:
  `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Consumes: the nine source-reviewed draft files.
- Produces: one trusted schema-v2 manifest with suite id `tent-cpp`.

- [ ] Render all reviewed rows in stable `(reference.file, reference.test)`
      order using suite `{id: tent-cpp, framework: gtest,
      reference_root: mooncake-transfer-engine/tent/tests}`.
- [ ] Validate TENT alone; require exit 0 and `reference-total=319` without
      structural findings.
- [ ] Validate Store + Transfer Engine + TENT; require distinct suite summaries
      and aggregate `reference-total=2066`.
- [ ] Run `--require-complete`; require exit 1 if and only if missing/blocked
      remains, while structural findings return 2.
- [ ] Call `plan_parity_runs` source-only and prove shared Rust evidence is
      scheduled once with the intended Cargo selector. Do not call
      `execute_plan` during audit materialization.
- [ ] Record exact disposition and evidence counts without normalizing them.

### Task 12: Verify and commit the TENT baseline

**Files:**
- Verify: `rust-repo/tools/store-validation/tent-parity-map.json`
- Verify: `rust-repo/tools/store-validation/parity-map.json`
- Verify: `rust-repo/tools/store-validation/transfer-engine-parity-map.json`

**Interfaces:**
- Consumes: structurally valid 319-row TENT manifest.
- Produces: committed, reproducible TENT audit baseline.

- [ ] Run all validation-tool unit tests and Python compilation.
- [ ] Run `tests/test_module_gate.sh`, `git diff --check`, and semantic
      draft/render equality.
- [ ] Run scoped pre-commit with `SKIP=mooncake-code-format`.
- [ ] Verify staged and unstaged diffs under `mooncake-transfer-engine` and
      `mooncake-wheel/tests` are empty.
- [ ] Review the staged diff and commit only the TENT manifest with
      `[Store] audit TENT parity baseline`.
- [ ] Record real missing rows as later Rust TDD backlog, keep reviewed N/A
      rows unchanged, and continue to the wheel Store/TE 352-row audit.

## Phase Acceptance Criteria

- Exactly 319 discovered TENT tests have exactly one reviewed manifest row.
- Covered rows cite only discoverable approved Rust tests whose aggregate
  assertions prove the complete reference oracle.
- Missing rows name the exact unproved assertion/scenario and claim no Rust
  evidence.
- Blocked rows have complete Rust evidence plus a named prerequisite.
- N/A rows use an allowed category and concrete source-backed rationale.
- Validator exit semantics are 0 for a valid baseline, 1 only for strict
  incompleteness, and 2 for structural findings.
- No C/C++ or wheel file is modified, staged, built, loaded, or executed.
- No Rust remediation or LocalDisk `io_uring` work starts in this phase.
