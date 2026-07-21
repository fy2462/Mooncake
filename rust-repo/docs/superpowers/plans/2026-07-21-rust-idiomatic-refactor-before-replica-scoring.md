# Rust-Idiomatic Store Refactor Before Replica Scoring

**Goal:** Refactor the Rust Store implementation toward idiomatic Rust without
changing observable behavior, then use that foundation to migrate upstream
`b996ac4b` remote MEMORY replica scoring.

**Scope rule:** This phase changes code structure and type boundaries only. It
must preserve C++-compatible selection order, status fallback, error mapping,
RPC fields, snapshot bytes, persistence compatibility, concurrency semantics,
and public API behavior.

## Non-goals

- Do not fix unrelated behavioral bugs discovered during refactoring.
- Do not change protobuf field numbers, serialized enum values, snapshot
  formats, default values, allocation ordering, or retry behavior.
- Do not remove existing public constructors or Python-visible call shapes.
- Do not migrate `b996ac4b` until the refactor verification gate passes.
- Do not replace required FFI pointers or wire integers with abstractions at
  the actual FFI/protobuf boundary.

## Design rules

- Keep raw numeric status and replica-type values at wire boundaries; use
  typed enums in domain and public Rust APIs.
- Centralize wire/domain conversion and preserve the existing unknown-value
  fallback explicitly in tests.
- Prefer owned return values and state objects over output collection
  parameters. Retain `&mut` where an in-place data-structure algorithm is the
  natural Rust interface.
- Represent lifecycle invariants with typed state and RAII guards rather than
  unrelated public booleans.
- Keep compatibility facades: old constructors delegate to typed config
  objects, so callers do not change during this phase.
- Use client-owned state and `Arc` for injected behavior; do not introduce
  process-global mutable callbacks.

## Batch 1: Characterization baseline

**Targets:**

- Replica status/type wire conversion and unknown-value fallback.
- Client constructor defaults, tenant handling, environment overrides, and
  master candidate order.
- Shutdown, remount exclusion, health state, and offload-server lifecycle.
- Allocator strategy ordering, fallback, exclusions, and partial success.
- Local-storage scan ordering and metadata reconstruction.

**Exit criteria:** New tests pass against the current implementation before
any structural refactor begins.

## Batch 2: Typed wire/domain conversion

- Implement shared typed conversions for `ReplicaStatus` and `ReplicaType`.
- Change Rust domain-facing batch types away from public `i32` fields.
- Keep protobuf request/response construction numeric only at the adapter.
- Remove duplicate hand-written matches from master and client adapters.

**Exit criteria:** Golden proto cases and all core/master/client tests pass;
unknown numeric values retain their current fallback behavior.

## Batch 3: Client configuration object

- Introduce `ClientConfig` and a builder with validated typed fields.
- Preserve every existing `MooncakeClient::create*` signature as a facade.
- Move environment resolution into a pure, testable configuration step.
- Pass one configuration value through internal bootstrap functions instead
  of repeated 8–10 argument lists.

**Exit criteria:** Existing callers compile unchanged and characterization
tests observe identical defaults, validation errors, and connection order.

## Batch 4: Typed lifecycle ownership

- Encapsulate shutdown signaling, remount exclusion, health status, and
  offload-server task/port/address invariants.
- Use an RAII remount guard so every return path clears in-progress state.
- Preserve current atomic ordering until a separate concurrency proof permits
  relaxing it.
- Preserve task abort/shutdown timing and public health results.

**Exit criteria:** Lifecycle race and failure-path tests pass under repeated
execution; no externally visible state transition changes.

## Batch 5: Value-oriented allocation and scanning

- Replace allocator `replicas`/`used_segment_names` output parameters with an
  internal `AllocationPlan` or `AllocationOutcome` state value.
- Replace local file scan tuple/output parameters with a named `FileEntry`
  result value.
- Preserve RNG use, candidate order, exclusion checks, fallback order,
  filesystem traversal order, and partial-success semantics exactly.

**Exit criteria:** Seeded/table-driven allocator cases and local-storage
restart cases are identical before and after refactoring.

## Refactor verification gate

- Run `cargo fmt --all -- --check`.
- Run complete tests for `mooncake-store-core`, `mooncake-store-master`, and
  `mooncake-store-client` with one Cargo build job.
- Run proto and snapshot golden/compatibility tests.
- Run clippy and distinguish pre-existing lints from new lints.
- Review `git diff --check` and confirm each commit contains structural changes
  only.
- Commit every batch independently; do not mix `b996ac4b` implementation into
  these commits.

## Phase 2: Migrate `b996ac4b`

Only after the verification gate:

- Add backward-compatible replica protocol propagation through core, proto,
  master allocation/query, and client conversion.
- Add a default-disabled `ReplicaSelectionPolicy` owned by each client.
- Support an injected `Arc` scorer and built-in `rdma < tcp < unknown`
  priority.
- Preserve local MEMORY/NoF precedence, COMPLETE filtering, master-order tie
  breaking, and all disk fallbacks.
- Verify single, batch, and range reads share the same selection policy.

