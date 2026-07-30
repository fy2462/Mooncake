# Batch Replica Clear Parity Remediation Design

## Purpose

This is the first bounded remediation wave after the Store, Transfer Engine,
TENT, and wheel parity baselines were materialized. It closes the ordinary
Store and wheel `batch_replica_clear` evidence gaps without broadening the
public API or weakening a reference oracle.

The implementation remains C++-authoritative: C++ Store tests define the
master-service semantics, and the selected wheel tests define client-visible
byte and multi-client results. C/C++ and `mooncake-wheel/tests` remain immutable
and are never built, imported, loaded, or executed.

## Chosen Approach

Use an evidence-first, two-layer test design:

1. Add direct `mooncake-store-master` service tests for the eight ordinary C++
   `MasterServiceTest.BatchReplicaClear*` rows. These tests isolate ownership,
   lease, segment filtering, ordering, missing/empty inputs, and post-clear
   metadata without requiring the native Transfer Engine.
2. Add `mooncake-store-client` in-process E2E tests for the seven currently
   missing wheel rows. These tests prove the byte-level and multi-client
   assertions that master metadata tests cannot prove.
3. Run every new test before changing production code. A passing test closes an
   evidence-only gap. A failing test becomes the RED proof for the smallest
   production correction in `batch_replica_clear_impl`; no production edit is
   allowed merely because a manifest row is missing.

This approach is preferred to one monolithic client test because master-only
failures stay fast and diagnosable, while native E2E coverage is limited to
assertions that truly require data movement. It is preferred to production-first
changes because the inspected implementation already contains owner, lease,
completion, segment, quota, persistence, task-cancellation, and release logic;
the unresolved question is which source oracles it actually satisfies.

## Scope

### Ordinary C++ master-service rows

Add exact Rust evidence for these eight rows from `master_service_test.cpp`:

| Reference row | Required Rust observation |
|---|---|
| `BatchReplicaClearAllSegments` | Five complete one-replica objects exist; after the 50 ms lease plus 10 ms, one all-segment request returns all five and all five no longer exist. |
| `BatchReplicaClearSpecificSegment` | A one-replica object on the named segment is polled after the initial 10 ms until cleared within five seconds, then is absent. |
| `BatchReplicaClearWithLeaseActive` | A completed/read object with a 2000 ms active lease returns no cleared keys and remains present. |
| `BatchReplicaClearWithDifferentClientId` | After expiry, a different client receives an empty result and the object remains present. |
| `BatchReplicaClearWithNonExistentKeys` | Two exact missing keys return a successful empty result. |
| `BatchReplicaClearWithEmptyKeys` | Empty input returns a successful empty result. |
| `BatchReplicaClearWithEmptyStringKeys` | Ordered empty/valid/empty/missing input returns only the valid key in position zero. |
| `BatchReplicaClearMixedScenario` | Client 1 clears its two expired keys while client 2's key, a missing key, and an empty key are skipped; exact existence state follows. |

The tests live in
`rust-repo/crates/mooncake-store-master/tests/test_master_object.rs` and use the
existing gRPC service boundary, object setup helpers, deterministic lease
durations, and exact response/state assertions.

### Wheel client-visible rows

Add focused in-process E2E evidence for the seven missing rows in
`test_batch_replica_clear.py`:

| Wheel row | Required Rust observation |
|---|---|
| `test_clear_with_active_lease` | Three active keys return an empty clear result and all three retain exact bytes. |
| `test_clear_mixed_expired_and_active` | One request returns exactly two expired keys and both active keys retain exact bytes. |
| `test_clear_with_invalid_segment_name` | A non-hosting segment returns empty and the key retains exact bytes. |
| `test_clear_large_batch` | One request clears and returns all 50 expired keys. |
| `test_clear_replicated_key_all_replicas` | A replica-two value is readable from both clients before all-segment clear returns the key. |
| `test_clear_specific_segment_replica` | Named clear returns the key and the second client still reads exact bytes from the remaining replica. |
| `test_clear_does_not_affect_other_keys` | Selective clear excludes the unrelated key and that key retains exact bytes. |

These tests live in
`rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs` and use
real in-process master/client/transport code. They may share fixtures, but each
test name must expose one reviewable behavior family. The 50-key test must use a
single clear request rather than several smaller requests.

## Production Change Boundary

The only permitted production edit is the minimal change required by a new
failing parity test in
`rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs`, or a
directly invoked Store client method if the failing test proves the client
mapping itself is wrong.

The existing invariants must remain intact:

- tenant normalization happens before object lookup;
- only the owning client can clear a replica;
- an active lease skips the object without failing the whole batch;
- empty, missing, foreign-owner, and nonmatching-segment keys are skipped while
  later keys continue;
- all-segment clear requires every replica to be complete;
- named-segment clear removes only complete matches;
- successful removal persists the authoritative object image/removal marker
  before releasing replicas or returning success;
- quota accounting, offload/promotion task cleanup, and response order remain
  consistent with the removed keys.

No new public API, retry loop, configuration switch, storage backend, or
performance optimization is introduced.

## TDD and Result Classification

Each new test is run immediately after it is written:

- If it passes, the gap was evidence-only. Production code is not changed.
- If it fails with the expected semantic mismatch, that result is the RED
  baseline. Apply the smallest production correction and rerun the focused test
  plus the owning test target.
- If it errors because of test construction, fix the test until it produces a
  meaningful pass or semantic failure; an infrastructure error is not RED.
- If a client E2E cannot link solely because the configured native Transfer
  Engine library is unavailable, record the exact linker prerequisite and keep
  the affected wheel rows non-covered. A written but unexecuted test is not
  coverage.

The master target must remain runnable without building or linking C/C++
reference code. Client E2E uses the repository's existing `link-native` feature
only; it must not build the reference test suites.

## Manifest Updates

After fresh test execution, update only rows whose complete oracle was proved:

- `rust-repo/tools/store-validation/parity-map.json` may move the eight ordinary
  `MasterServiceTest` rows from `missing` to `covered` and cite the exact new
  master tests.
- `rust-repo/tools/store-validation/wheel-store-parity-map.json` may move each
  of the seven wheel rows only after its full client-visible assertions execute
  successfully.
- Rust evidence may be shared across reference rows, and one row may aggregate
  several Rust tests when required.

The following rows remain out of scope and must not be upgraded by this wave:

- the eight `MasterServiceSnapshotTest.BatchReplicaClear*` rows, because they
  additionally require deterministic live-save, fresh-master restore,
  second-save, and full restored-state comparison;
- `MasterServiceSSDTest.BatchReplicaClearAllSegmentsReleasesLocalDiskUsageTracking`,
  because it requires its own LocalDisk usage/ranking and direct offload setup;
- any wheel row whose native byte-path test did not execute successfully.

## Verification and Commits

Use separate reviewable commits:

1. master-service parity tests and any RED-proven master correction;
2. client E2E parity tests and any RED-proven client correction;
3. manifest status/evidence updates after all cited tests pass.

Before each commit, run focused tests and `git diff --check`. Before the manifest
commit, run all Store validation-tool unit tests, Python compilation, ordinary
and strict validators, source-only parity planning, the module-gate contract,
and scoped pre-commit with `SKIP=mooncake-code-format`. Verify staged and
unstaged diffs below `mooncake-transfer-engine` and `mooncake-wheel/tests` are
empty.

The etcd-only HA production policy is unchanged. Store LocalDisk io_uring work
remains gated until all applicable correctness remediation and gates pass.
