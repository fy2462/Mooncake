# Transfer Engine 348-Test Semantic Parity Audit Plan

> **Execution mode:** Run inline in the current worktree. Do not delegate this
> plan to subagents. Use source-only inspection of the C++ oracle and Rust FFI
> code; no native reference execution is permitted.

**Goal:** Produce a source-reviewed schema-v2 parity manifest for every one of
the 348 GoogleTest declarations under `mooncake-transfer-engine/tests`, using
only complete Rust-observable evidence from the approved Store crates and
`transfer-engine-ffi`, then establish the exact covered/missing/blocked/N/A
baseline before any Rust remediation.

**Architecture:** Audit in eight stable file groups. For each reference test,
trace the assertions and relevant helper/call path, identify the observable
Store or FFI contract, and compare that complete contract with discoverable
Rust tests. One Rust test may support many reference rows and several Rust
tests may jointly support one row. The final manifest is materialized only
after all groups have been reviewed so an intermediate blanket `missing`
classification cannot be mistaken for an audited baseline.

**Tech stack:** C++ source as a read-only oracle, Rust source and tests,
Python 3 source-only inventory/manifest validation, JSON schema-v2 manifests,
Cargo test selection, Git, and repository pre-commit hooks.

## Global constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- Never edit, format, build, link, load, execute, stage, or commit C/C++ files.
- Never edit or execute files beneath `mooncake-wheel/tests` in this phase.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling.
- Use `apply_patch` for semantic repository edits. A deterministic bulk JSON
  render may mechanically order already-reviewed rows, but it must not invent
  dispositions, reasons, behavior descriptions, or evidence mappings.
- Every pre-commit invocation sets `SKIP=mooncake-code-format`.
- Approved Rust evidence packages are `mooncake-store-client`,
  `mooncake-store-core`, `mooncake-store-master`, and
  `transfer-engine-ffi`.
- `covered` means the cited Rust assertions collectively prove the complete
  executed reference oracle. Similar names, shared implementation, comments,
  or partial assertions are not enough.
- `missing` keeps `rust: []` and names the existing partial evidence, if any,
  plus the exact remaining assertion/scenario gap.
- `blocked` is allowed only when discoverable complete Rust evidence already
  exists and a named external prerequisite prevents execution.
- `not-applicable` requires a permitted category and source proof that the
  reference behavior is language-unrepresentable or has no suitable
  Rust-observable product boundary. C++-internal organization alone is not
  sufficient.
- No Rust correctness fix begins until the 348-row audit baseline validates.
- Store LocalDisk `io_uring` remains gated until all Store, Transfer Engine,
  TENT, and wheel correctness gates pass.

## Required row shape

Every final row in
`rust-repo/tools/store-validation/transfer-engine-parity-map.json` contains:

```json
{
  "reference": {
    "file": "common_test.cpp",
    "test": "GetPortFromString.ReturnsParsedPort"
  },
  "behavior": "The complete externally observable assertion contract.",
  "boundary": ["transfer-engine-ffi", "configuration"],
  "status": "covered",
  "rust": [
    {
      "file": "transfer-engine-ffi/src/example.rs",
      "test": "exact_discoverable_test_name"
    }
  ],
  "reason": "",
  "review": {
    "oracle": "Exact reference file and inspected helper/call path",
    "reviewed": true
  }
}
```

For `not-applicable`, add one of the validator-approved `na_category` values.
For `blocked`, add `prerequisite`. Missing and N/A rows must not cite Rust
tests.

---

### Task 1: Freeze the inventories and create an audit checkpoint

**Read-only inputs:**
- `mooncake-transfer-engine/tests/**/*.cpp`
- `rust-repo/crates/transfer-engine-ffi/**/*.rs`
- `rust-repo/crates/mooncake-store-{client,core,master}/**/*.rs`

**Local checkpoint:**
- Update: `.superpowers/sdd/2026-07-30-rust-store-full-correctness-validation/progress.md`

- [ ] Run `discover_suite_tests` for `transfer-engine-cpp` and assert exactly
      348 unique identities across 42 files.
- [ ] Run `discover_rust_tests` over `rust-repo/crates` and save only the
      approved-package evidence identities in the local checkpoint.
- [ ] Record the eight review groups and their exact counts. Assert their sum
      is 348 before source review begins.
- [ ] Record `git diff --name-only -- mooncake-transfer-engine
      mooncake-wheel/tests` as the immutable-reference baseline.

The required groups are:

| Group | Files | Tests |
|---|---:|---:|
| Common parsing/helpers and configuration | 2 | 69 |
| Portable memory, topology, locality, validation, transport utilities | 5 | 51 |
| Endpoint lifecycle, metadata, discovery, shutdown | 7 | 31 |
| TCP, CXL, MP, and NVMe-oF observable transfer/status behavior | 6 | 23 |
| RDMA context, endpoint, GID, recovery, and loopback | 6 | 43 |
| EFA, CXI, UB, and UB shared-memory transports | 7 | 35 |
| Ascend, DMA-BUF, HIP, and NVLink behavior | 4 | 72 |
| Sunrise allocator, copy, runtime, and transport behavior | 5 | 24 |

---

### Task 2: Audit common helpers and configuration (69 rows)

**Reference files:**
- `mooncake-transfer-engine/tests/common_test.cpp` (38)
- `mooncake-transfer-engine/tests/config_test.cpp` (31)

- [ ] Read every test body and every helper whose result is asserted; do not
      infer behavior from test names.
- [ ] Separate public/FFI-visible parsing and configuration semantics from
      C++-only utility/template behavior.
- [ ] Search all approved Rust test inventories for exact assertions covering
      valid values, invalid values, boundaries, defaults, and environment
      override behavior.
- [ ] For every partial match, record the omitted input class or assertion in
      the missing reason.
- [ ] Add 69 reviewed draft rows to the local checkpoint and verify identity
      uniqueness against source discovery.

### Task 3: Audit portable primitives and validation (51 rows)

**Reference files:**
- `memory_location_test.cpp` (3)
- `multi_transport_locality_test.cpp` (8)
- `tcp_address_validation_test.cpp` (10)
- `topology_test.cpp` (12)
- `transport_uint_test.cpp` (18)

- [ ] Review exact NUMA/memory-location, host locality, IPv4/IPv6, registered
      range, file I/O, topology selection, overlap, rollback, and batch error
      oracles.
- [ ] Follow the tested C++ call paths far enough to distinguish a product
      contract from a C++ helper-only contract.
- [ ] Compare against Rust registered-memory bounds, segment, transfer, Store
      client/core, and FFI error tests; require exact boundary and failure
      assertions for covered rows.
- [ ] Add 51 reviewed draft rows and recheck the cumulative 120 identities.

### Task 4: Audit endpoint lifecycle and metadata (31 rows)

**Reference files:**
- `connect_pause_tracker_test.cpp` (7)
- `context_lookup_test.cpp` (2)
- `endpoint_store_integration_test.cpp` (1)
- `endpoint_store_test.cpp` (5)
- `graceful_shutdown_test.cpp` (6)
- `show_links_test.cpp` (6)
- `transfer_metadata_test.cpp` (4)

- [ ] Review concurrency, expiry, reclamation, shutdown/signal, topology
      display/C API, cached metadata refresh, and segment/memory metadata
      assertions including negative cases.
- [ ] Treat signal/fork/process details as N/A only if no Rust-visible Store or
      FFI outcome exists; otherwise record a concrete missing integration test.
- [ ] Add 31 reviewed draft rows and recheck the cumulative 151 identities.

### Task 5: Audit TCP, CXL, MP, and NVMe-oF (23 rows)

**Reference files:**
- `cxl_transport_test.cpp` (2)
- `mp_transport_test.cpp` (2)
- `nvmeof_status_test.cpp` (6)
- `nvmeof_transport_test.cpp` (2)
- `tcp_transport_test.cpp` (4)
- `tcp_write_visibility_test.cpp` (7)

- [ ] Review read/write byte integrity, batching, status aggregation, partial
      completions, deterministic failure precedence, unsupported combinations,
      visibility, descriptor-version interoperability, rejection, quiescence,
      and deadlines.
- [ ] A generic enum/status unit test is only partial evidence for native
      status aggregation or transport lifecycle behavior.
- [ ] Add 23 reviewed draft rows and recheck the cumulative 174 identities.

### Task 6: Audit RDMA behavior (43 rows)

**Reference files:**
- `rdma_context_reprobe_test.cpp` (4)
- `rdma_endpoint_reestablish_test.cpp` (8)
- `rdma_endpoint_state_test.cpp` (5)
- `rdma_gid_probe_test.cpp` (23)
- `rdma_loopback_test.cpp` (1)
- `rdma_transport_test2.cpp` (2)

- [ ] Review resource preconditions, endpoint state transitions, stale ACKs,
      retry budgets, GID selection/ranking, reprobe decisions, RNIC failure
      fallback, error capture, and loopback integrity.
- [ ] Distinguish hardware-dependent execution from missing Rust test logic:
      hardware absence cannot justify `blocked` unless the complete Rust test
      already exists.
- [ ] Add 43 reviewed draft rows and recheck the cumulative 217 identities.

### Task 7: Audit EFA, CXI, UB, and UB shared-memory (35 rows)

**Reference files:**
- `cxi_transport_test.cpp` (12)
- `cxi_unit_tests.cpp` (2)
- `efa_c_api_test.cpp` (4)
- `efa_gpu_loopback_test.cpp` (3)
- `efa_transport_test.cpp` (10)
- `ub_transport_test.cpp` (2)
- `ubshmem_transport_test.cpp` (2)

- [ ] Review install sequencing, topology discovery, registration/batch
      registration, repeated segment open, warmup, host/device transfer,
      multi-batch stress, and byte-integrity assertions.
- [ ] Record exact provider/GPU/NPU prerequisites only for discoverable Rust
      tests that already implement the full corresponding oracle.
- [ ] Add 35 reviewed draft rows and recheck the cumulative 252 identities.

### Task 8: Audit Ascend, DMA-BUF, HIP, and NVLink (72 rows)

**Reference files:**
- `ascend_direct_transport_test.cpp` (62)
- `dmabuf_export_test.cpp` (8)
- `hip_transport_test.cpp` (1)
- `nvlink_transport_test.cpp` (1)

- [ ] Review initialization/rollback, context preservation, endpoint
      selection, HCCS/RoCE configuration, synchronous/asynchronous transfer,
      status/timeout/retry/disconnect behavior, registration variants,
      DMA-BUF ownership/lifetime, active-device restoration, and byte
      integrity.
- [ ] Compare Rust accelerator pointer classification and FFI transfer tests
      without treating that single classification test as evidence for native
      transport behavior it does not execute.
- [ ] Add 72 reviewed draft rows and recheck the cumulative 324 identities.

### Task 9: Audit Sunrise behavior (24 rows)

**Reference files:**
- `sunrise_allocator_test.cpp` (10)
- `sunrise_link_copy_test.cpp` (6)
- `sunrise_link_transport_runtime_test.cpp` (5)
- `sunrise_link_transport_test.cpp` (1)
- `sunrise_link_transport_unit_test.cpp` (2)

- [ ] Review allocation tracking/range semantics, host/device staging and data
      consistency, transport lifetime isolation, device-state preservation,
      strict device-id parsing, remote-range overflow, and transfer integrity.
- [ ] Add 24 reviewed draft rows and assert exactly 348 unique reviewed
      identities with no stale or unmapped source reference.

### Task 10: Materialize and validate the Transfer Engine manifest

**Files:**
- Create:
  `rust-repo/tools/store-validation/transfer-engine-parity-map.json`
- Update:
  `.superpowers/sdd/2026-07-30-rust-store-full-correctness-validation/progress.md`

- [ ] Render the 348 reviewed rows in stable `(reference.file,
      reference.test)` order with this exact suite descriptor:

```json
{
  "id": "transfer-engine-cpp",
  "framework": "gtest",
  "reference_root": "mooncake-transfer-engine/tests"
}
```

- [ ] Run the validator against the new manifest alone. Expected: exit 0,
      `reference-total=348`, and no structural findings. Missing or blocked
      dispositions are allowed for this non-strict audit command.
- [ ] Run the validator against Store plus Transfer Engine manifests. Expected:
      exit 0, distinct per-suite summaries, aggregate `reference-total=1747`,
      and deduplicated aggregate Rust evidence metrics.
- [ ] Run `--require-complete` and confirm exit 1 if and only if at least one
      reviewed missing/blocked row remains; structural errors must return 2.
- [ ] Invoke `plan_parity_runs` from a source-only Python check without calling
      `execute_plan`. Confirm every covered FFI unit test maps to the intended
      Cargo selector and reused evidence produces one planned command.
- [ ] Record the exact covered/missing/blocked/N/A and Rust evidence counts in
      the progress checkpoint. Do not forecast or normalize them.

### Task 11: Verify audit integrity and commit the baseline

**Files:**
- Verify:
  `rust-repo/tools/store-validation/transfer-engine-parity-map.json`
- Verify:
  `rust-repo/tools/store-validation/parity-map.json`

- [ ] Run all Python validation-tool tests:

```bash
cd rust-repo/tools/store-validation
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

- [ ] Run Python compilation for the validation tools, `git diff --check`, and
      the module-gate shell contract test.
- [ ] Run scoped pre-commit with `SKIP=mooncake-code-format` on the new manifest
      and any touched validation/progress files.
- [ ] Verify both immutable reference trees remain unchanged:

```bash
git diff --exit-code -- mooncake-transfer-engine mooncake-wheel/tests
git diff --cached --exit-code -- mooncake-transfer-engine mooncake-wheel/tests
```

- [ ] Review `git diff` and commit only the manifest and requested audit
      records:

```bash
git add rust-repo/tools/store-validation/transfer-engine-parity-map.json
git commit -m '[Store] audit Transfer Engine parity baseline'
```

### Task 12: Hand the verified baseline to the remaining global audit

- [ ] Sort genuine missing rows by reference file and test name, then group
      only rows sharing one observable Rust boundary.
- [ ] Keep N/A rows as reviewed records; do not implement them merely to reduce
      the count.
- [ ] Record the remediation groups as backlog only. Do not change Rust
      behavior during this audit phase.
- [ ] Continue with the TENT 319-row audit, then the wheel Store/TE 352-row
      audit, then the Store 1,121-row missing re-evaluation. This preserves one
      global reviewed baseline before choosing the stable Rust TDD remediation
      order.
- [ ] Keep Store LocalDisk `io_uring` gated until those correctness audits and
      their applicable remediation gates pass.

## Phase acceptance criteria

- Exactly 348 discovered Transfer Engine tests have exactly one reviewed row.
- Every covered row cites one or more discoverable approved Rust tests whose
  aggregate assertions prove its complete oracle.
- Every missing reason identifies an exact unproved assertion or scenario.
- Every blocked row has complete existing Rust evidence plus a named external
  prerequisite.
- Every N/A row cites an allowed category and concrete source rationale.
- Validator exit semantics are 0 for a structurally valid baseline, 1 for a
  strict incomplete gate, and 2 for structural findings.
- No C/C++ or wheel reference file is modified, staged, built, or executed.
- The exact baseline counts are recorded before any Rust remediation begins.
