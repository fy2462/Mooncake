# Wheel Store/TE 352-Test Semantic Parity Audit Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Execute inline in the existing
> worktree; do not delegate reference-source review to subagents.

**Goal:** Produce a source-reviewed schema-v2 parity manifest for all 352
selected Store/Transfer Engine Python test declarations under
`mooncake-wheel/tests`, using complete Rust-observable evidence from
`transfer-engine-ffi` and the approved Store crates, then establish the exact
covered/missing/blocked/N/A baseline before Rust remediation.

**Architecture:** Freeze the selected source inventory without importing or
executing the Python modules, review seven stable behavior groups, and record
each complete parameter/assertion matrix in ignored local drafts. A single
source test declaration is one manifest identity even when pytest
parameterization expands it at runtime; the row's oracle must include every
declared parameter case. Materialize the tracked manifest only after all 352
identities have been reviewed exactly once.

**Tech Stack:** Read-only Python source inspection, Rust source/test
inspection, Python 3 source-only discovery and schema validation, JSON
schema-v2 parity manifests, Cargo planning without reference execution, Git,
and repository pre-commit hooks.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- Never edit, format, import, execute, stage, or commit files beneath
  `mooncake-wheel/tests`.
- Never edit, format, build, link, load, or execute C/C++ reference inputs.
- Exclude only `test_release_wheel_tags.py` through the trusted
  `wheel-store-python` suite definition; do not add ad hoc inventory filters.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling, but do
  not use it to import or run wheel reference tests.
- Use `apply_patch` for semantic repository edits. A deterministic bulk JSON
  render may mechanically order already-reviewed rows, but must not invent
  dispositions, behaviors, reasons, or evidence mappings.
- Every pre-commit invocation sets `SKIP=mooncake-code-format`.
- Approved Rust evidence packages are `mooncake-store-client`,
  `mooncake-store-core`, `mooncake-store-master`, and
  `transfer-engine-ffi`.
- Rust evidence may be reused across reference rows and several Rust tests may
  jointly prove one row; `covered` still requires the aggregate assertions to
  prove the complete Python oracle, including every parametrized case.
- `missing` keeps `rust: []` and records the exact unproved assertion/scenario
  plus any partial Rust evidence.
- `blocked` requires complete discoverable Rust evidence and a named external
  prerequisite. Missing CUDA/ROCm/CXL hardware or native libraries cannot turn
  absent Rust logic into blocked.
- `not-applicable` requires a validator-approved category and source proof that
  the Python-only wrapper/language behavior has no suitable Rust Store/TE
  product boundary or that the reference contains no executable product
  oracle.
- Do not change Rust behavior until the 352-row wheel manifest validates.
- Store LocalDisk io_uring remains gated until Store, Transfer Engine, TENT,
  wheel audits, and applicable correctness remediation pass.

---

### Task 1: Freeze wheel and Rust evidence inventories

**Files:**
- Read: `mooncake-wheel/tests/**/*.py`
- Read: `rust-repo/crates/{mooncake-store-client,mooncake-store-core,mooncake-store-master,transfer-engine-ffi}/**/*.rs`
- Update locally:
  `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Consumes: trusted suite definition `SUITES["wheel-store-python"]`.
- Produces: exact 352-reference identity set and approved Rust evidence set.

- [ ] Call `discover_suite_tests` and assert 352 unique Python test
      declarations across 24 selected files.
- [ ] Assert `test_release_wheel_tags.py` is the only suite exclusion and no
      selected module is imported or executed.
- [ ] Call `discover_rust_tests` and retain only the four approved package
      identities; assert the established inventory remains 1,252 unless a
      source change is explicitly reviewed.
- [ ] Record the seven audit groups below and assert their total is 352.
- [ ] Record staged and unstaged immutable-reference diffs for
      `mooncake-wheel/tests` and `mooncake-transfer-engine`.

| Group | Tests |
|---|---:|
| CLI, import structure, metadata, configuration, and endpoint helpers | 48 |
| Native buffer pool, dummy client, and multi-client behavior | 21 |
| Distributed, CXL, and replicated object stores | 30 |
| Store service API and batch replica clearing | 91 |
| Tensor, safetensor, registration, CUDA/ROCm, and TE initiator behavior | 56 |
| Structured object store | 99 |
| Eviction offload, promotion-on-hit, and SSD eviction offload | 7 |

### Task 2: Audit CLI, import, metadata, configuration, and endpoint behavior

**Files:**
- Read: `test_cli.py` (2)
- Read: `test_http_metadata_server.py` (5)
- Read: `test_import_structure.py` (3)
- Read: `test_meta_server.py` (1)
- Read: `test_mooncake_config.py` (35)
- Read: `test_mooncake_ep.py` (2)
- Create locally: `wheel-config-metadata-audit.md`

**Interfaces:**
- Consumes: Rust configuration, endpoint parsing/reservation, metadata, and
  public startup/error tests.
- Produces: 48 reviewed draft rows.

- [ ] Trace every CLI exit/output, import/export shape, HTTP metadata
      lifecycle, configuration default/override/validation, and endpoint
      parsing/reservation assertion.
- [ ] Separate Python import/module-surface behavior from Store/TE product
      behavior; use language or absent-boundary N/A only when Rust cannot
      represent the asserted result.
- [ ] Preserve all parametrized inputs and exact expected errors/defaults in
      the single owning source row.
- [ ] Verify the draft identity set equals the 48 discovered references.

### Task 3: Audit native buffer and dummy-client behavior

**Files:**
- Read: `test_buffer_pool_native.py` (14)
- Read: `test_dummy_client.py` (6)
- Read: `test_multi_dummy_clients.py` (1)
- Create locally: `wheel-buffer-dummy-audit.md`

**Interfaces:**
- Consumes: Rust owned/registered memory, staging/buffer-pool, client lifecycle,
  and dummy/shared-memory evidence.
- Produces: 21 reviewed draft rows; cumulative 69.

- [ ] Review allocation, reuse, bounds, registration ownership, shared-memory
      descriptors, dummy-client operations, error propagation, and concurrent
      client isolation.
- [ ] Reject shape-only FFI tests as coverage for native memory lifetime or
      cross-client behavior unless their aggregate assertions prove all side
      effects.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 4: Audit distributed object-store behavior

**Files:**
- Read: `test_distributed_object_store.py` (14)
- Read: `test_distributed_object_store_cxl.py` (11)
- Read: `test_replicated_distributed_object_store.py` (5)
- Create locally: `wheel-distributed-store-audit.md`

**Interfaces:**
- Consumes: Rust Store client/master put/get/remove, replica, topology,
  CXL/transport, and multi-client evidence.
- Produces: 30 reviewed draft rows; cumulative 99.

- [ ] Trace object lifecycle, multi-node visibility, replica placement,
      failure/timeout semantics, CXL selection/fallback, byte equality, and
      cleanup for every fixture and parameter case.
- [ ] Classify hardware/library prerequisites as blocked only when the complete
      Rust test already exists; otherwise retain observable scenarios as
      missing.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 5: Audit Store service API and batch replica clearing

**Files:**
- Read: `test_mooncake_store_service_api.py` (79)
- Read: `test_batch_replica_clear.py` (12)
- Create locally: `wheel-service-batch-audit.md`

**Interfaces:**
- Consumes: Rust Store client/master API, tenant/object/replica state,
  batched operations, and error-code evidence.
- Produces: 91 reviewed draft rows; cumulative 190.

- [ ] Review exact request/response status, tenant and key scope, put/get/remove
      lifecycle, batching, replica clearing, idempotency, partial failure,
      retries, cleanup, and concurrency assertions.
- [ ] Require the Rust evidence aggregate to prove every Python-visible return
      value and state transition, not merely the underlying RPC method name.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 6: Audit tensor, registration, accelerator, and TE initiator behavior

**Files:**
- Read: `test_put_get_tensor.py` (31)
- Read: `test_safetensor_functions.py` (6)
- Read: `test_regmr_overhead.py` (7)
- Read: `test_transfer_on_cuda.py` (4)
- Read: `test_transfer_on_hip.py` (2)
- Read: `transfer_engine_initiator_test.py` (6)
- Create locally: `wheel-tensor-accelerator-audit.md`

**Interfaces:**
- Consumes: Rust registered-memory ownership/bounds, accelerator pointer type,
  batch transfer/status, and Store tensor/object evidence.
- Produces: 56 reviewed draft rows; cumulative 246.

- [ ] Trace tensor dtype/shape/stride/device preservation, safetensor metadata
      and bounds, registration timing assertions, CPU/CUDA/ROCm transfer status
      and byte equality, multi-request batches, and resource cleanup.
- [ ] Use `excluded-performance-scope` only for assertions whose sole oracle is
      timing/throughput and retain any co-located correctness assertions in the
      row's applicable disposition.
- [ ] Do not claim DLPack/accelerator parity from enum or pointer-shape tests
      when the reference asserts native device transfer or tensor contents.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 7: Audit structured object-store behavior

**Files:**
- Read: `test_structured_object_store.py` (99)
- Create locally: `wheel-structured-store-audit.md`

**Interfaces:**
- Consumes: Rust serialization-compatible Store object operations, nested
  metadata, batching, partial reads, lifecycle, and validation evidence.
- Produces: 99 reviewed draft rows; cumulative 345.

- [ ] Review every supported scalar/container/tensor structure, nested and
      empty values, schema/metadata preservation, invalid inputs, overwrite,
      missing keys, partial/batch operations, cleanup, and parameter matrix.
- [ ] Distinguish Python serialization/language semantics from Store-visible
      byte/object lifecycle; use N/A only where the complete oracle is
      intrinsically Python-specific and has no Rust product counterpart.
- [ ] Verify exact identities and cumulative uniqueness.

### Task 8: Audit eviction, promotion, and SSD-offload behavior

**Files:**
- Read: `test_offload_on_eviction.py` (1)
- Read: `test_promotion_on_hit.py` (3)
- Read: `test_ssd_offload_in_evict.py` (3)
- Create locally: `wheel-eviction-promotion-audit.md`

**Interfaces:**
- Consumes: Rust Store LocalDisk/global-disk, eviction, promotion, replica
  state, and restart/lifecycle evidence.
- Produces: 7 reviewed draft rows and the exact final 352-row set.

- [ ] Review memory pressure, eviction-triggered offload, hit-triggered
      promotion, SSD presence/content, replica transitions, return values, and
      cleanup without converting these correctness rows into performance work.
- [ ] Assert exactly 352 unique reviewed identities, zero omissions, zero stale
      rows, and zero duplicates across all seven drafts.

### Task 9: Materialize and validate the wheel manifest

**Files:**
- Create: `rust-repo/tools/store-validation/wheel-store-parity-map.json`
- Update locally:
  `.superpowers/sdd/2026-07-27-rust-store-full-correctness-validation/progress.md`

**Interfaces:**
- Consumes: the seven source-reviewed draft files.
- Produces: one trusted schema-v2 manifest with suite id
  `wheel-store-python`.

- [ ] Render all reviewed rows in stable `(reference.file, reference.test)`
      order using suite `{id: wheel-store-python, framework: python,
      reference_root: mooncake-wheel/tests}`.
- [ ] Validate wheel alone; require exit zero and `reference-total=352`
      without structural findings.
- [ ] Validate Store + Transfer Engine + TENT + wheel; require distinct suite
      summaries and aggregate `reference-total=2451`.
- [ ] Run `--require-complete`; require exit one if and only if
      missing/blocked remains, while structural findings return two.
- [ ] Call `plan_parity_runs` source-only and prove every shared Rust test is
      scheduled once with all owning suite/reference contexts. Do not call
      `execute_plan` during audit materialization.
- [ ] Record exact disposition/evidence/category counts without normalizing
      them.

### Task 10: Verify and commit the wheel baseline

**Files:**
- Verify: `rust-repo/tools/store-validation/wheel-store-parity-map.json`
- Verify: `rust-repo/tools/store-validation/tent-parity-map.json`
- Verify: `rust-repo/tools/store-validation/transfer-engine-parity-map.json`
- Verify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: structurally valid 352-row wheel manifest.
- Produces: committed, reproducible wheel Store/TE audit baseline.

- [ ] Run all validation-tool unit tests and Python compilation.
- [ ] Run `tests/test_module_gate.sh`, `git diff --check`, and semantic
      draft/render equality.
- [ ] Run scoped pre-commit with `SKIP=mooncake-code-format`.
- [ ] Verify staged and unstaged diffs under `mooncake-transfer-engine` and
      `mooncake-wheel/tests` are empty.
- [ ] Review the staged diff and commit only the wheel manifest with
      `[Store] audit wheel Store and TE parity baseline`.
- [ ] Preserve real missing rows as later Rust TDD backlog, keep reviewed N/A
      rows unchanged, and move to the cross-suite remediation plan; do not
      start io_uring until correctness gates pass.

## Phase Acceptance Criteria

- Exactly 352 selected wheel test declarations have exactly one reviewed
  manifest row, including complete parameter matrices.
- Covered rows cite only discoverable approved Rust tests whose aggregate
  assertions prove the complete Python oracle.
- Missing rows name the exact unproved assertion/scenario and claim no Rust
  evidence.
- Blocked rows cite complete existing Rust evidence plus a named external
  prerequisite; absent tests are never blocked.
- N/A rows use only approved categories with source-reviewed reasons.
- `mooncake-wheel/tests` and all C/C++ reference files remain unchanged and
  unexecuted.
- Store LocalDisk io_uring remains gated after this audit until applicable
  correctness remediation and gates pass.
