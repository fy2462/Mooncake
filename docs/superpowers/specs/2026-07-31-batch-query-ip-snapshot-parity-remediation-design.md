# Batch Query IP Snapshot Parity Remediation Design

## Objective

Prove the eight `MasterServiceSnapshotTest.BatchQueryIp*` reference behaviors
through the Rust master's real native snapshot save and restore boundary. Keep
one Rust test identity per C++ reference while sharing test-only roundtrip
machinery. Change production Rust only if the new tests expose a real snapshot
or restored-query defect.

This wave covers only:

- `MasterServiceSnapshotTest.BatchQueryIpTest`
- `MasterServiceSnapshotTest.BatchQueryIpMultipleSegmentsTest`
- `MasterServiceSnapshotTest.BatchQueryIpEmptyClientIdTest`
- `MasterServiceSnapshotTest.BatchQueryIpMultipleSegmentsEmptyTeEndpointTest`
- `MasterServiceSnapshotTest.BatchQueryIpBracketedIpv6Test`
- `MasterServiceSnapshotTest.BatchQueryIpLinkLocalIpv6WithScopeTest`
- `MasterServiceSnapshotTest.BatchQueryIpIpv6NoPortTest`
- `MasterServiceSnapshotTest.BatchQueryIpMixedIpv4AndIpv6Test`

The ordinary `MasterServiceTest.BatchQueryIp*` rows are already covered and are
not part of this wave.

## Authoritative Reference Behavior

The reference test bodies are in
`mooncake-store/tests/ha/snapshot/master_service_test_for_snapshot.cpp`. Each
body mounts its fixture and checks the same BatchQueryIp result as its ordinary
counterpart. The inherited `MasterServiceSnapshotTestBase::TearDown` in
`master_service_test_for_snapshot_base.h` then performs the snapshot-specific
oracle:

1. Persist the live master.
2. Construct a fresh master in restore mode.
3. Persist the restored master again.
4. Compare the two serialized snapshots.
5. Compare the complete observable service state before and after restore.

The Rust equivalent must therefore execute BatchQueryIp both before and after
a real native LocalDisk snapshot restore and prove that a second saved snapshot
can be restored to the same service state. Ordinary QueryIp tests plus a DTO
serialization unit test are insufficient evidence.

All C/C++ reference files and `mooncake-wheel/tests` remain read-only. They are
never built, linked, loaded, imported, formatted, or executed.

## Considered Approaches

### Recommended: eight tests with one shared double-roundtrip helper

Add eight exact integration test identities to
`mooncake-store-master/tests/test_storage.rs`. Each test constructs only its
own C++-matching fixture and delegates the native save/restore sequence to one
shared test helper.

This preserves the C++ test dimension and quantity, keeps failures attributable
to one behavior, and avoids copying asynchronous snapshot synchronization and
state comparison logic eight times.

### Two aggregate matrix tests

One populated-state test and one empty-state test could prove all eight rows
with fewer snapshot writes. The parity schema permits such evidence reuse, but
failures would be less localized and the Rust test inventory would no longer
mirror the eight C++ reference dimensions. This is not selected.

### Reuse ordinary QueryIp and storage serialization tests

The existing ordinary tests prove QueryIp parsing, and existing storage tests
prove that a segment DTO retains `te_endpoint`. Their combination does not call
BatchQueryIp on a fresh restored `MasterServiceImpl`, does not execute the
second save, and cannot prove restored service-state equivalence. This approach
is rejected as incomplete evidence.

## Test Architecture

### Test-only fixture helpers

Add focused helpers in `test_storage.rs`:

- mount a Memory segment through the tonic `MasterService::mount_segment`
  boundary with an exact client, name, base address, and `te_endpoint`;
- invoke `MasterService::batch_query_ip` and normalize its map by sorting each
  address vector, so assertions preserve C++ set semantics without depending
  on map iteration order;
- capture a canonical Memory-segment signature sorted by segment identity;
- wait with a bounded poll for a nonempty `master_snapshot.msgpack` file;
- execute and assert the double snapshot roundtrip.

The canonical segment signature includes every persisted field relevant to
the complete state in these fixtures:

- segment UUID;
- name;
- base and size;
- transfer endpoint;
- protocol and host ID;
- used bytes;
- owning client UUID;
- segment status.

The helper also asserts that NoF segments, objects, tasks, replication tasks,
graceful-unmount intents, delayed releases, and LocalDisk segment state remain
empty. Thus, for these segment-only fixtures, the comparison covers the full
Rust master snapshot state rather than only QueryIp output.

### Double-roundtrip data flow

For each test:

1. Create an isolated temporary directory and a
   `MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), ...)`.
2. Mount the exact fixture and assert normalized BatchQueryIp output.
3. Capture the canonical live state and call `save_snapshot`.
4. Wait for a nonempty native snapshot, drop the live service, and construct a
   fresh service from the same directory.
5. Assert exact canonical state equality and exact normalized BatchQueryIp
   output on the restored service.
6. Rename the first snapshot inside the test temporary directory, then call
   `save_snapshot` on the restored service. Because the destination is absent,
   bounded existence polling cannot mistake the first snapshot for completion
   of the second asynchronous save.
7. Drop the first restored service, construct a second fresh service from the
   second snapshot, and repeat the state and BatchQueryIp assertions.

Retaining the first file also makes both snapshot artifacts available during a
failure. The test compares decoded, canonical service state rather than raw
bytes: Rust and C++ use different snapshot formats, while the required parity
contract is idempotent state and behavior.

### Exact Rust test identities

- `batch_query_ip_snapshot_single_and_unknown_client_parity`
- `batch_query_ip_snapshot_deduplicates_multiple_segment_addresses_parity`
- `batch_query_ip_snapshot_empty_request_parity`
- `batch_query_ip_snapshot_retains_client_with_only_empty_endpoints_parity`
- `batch_query_ip_snapshot_parses_bracketed_ipv6_parity`
- `batch_query_ip_snapshot_preserves_ipv6_scope_parity`
- `batch_query_ip_snapshot_accepts_ipv6_without_port_parity`
- `batch_query_ip_snapshot_mixed_ipv4_ipv6_parity`

Each identity maps to the corresponding C++ snapshot row. No ordinary,
LocalDisk SSD, TE, TENT, or wheel row is upgraded by these tests.

## Expected Results and Failure Policy

The existing snapshot serializer stores `Segment.te_endpoint`, and the restored
master rebuilds segment entries before serving QueryIp. The expected outcome is
that all eight new tests pass without a production change.

An immediately passing test is valid evidence work, not a manufactured RED.
If any test fails:

- keep all eight manifest rows `missing`;
- verify that the failure is semantic rather than a fixture, timeout, or
  compilation defect;
- use systematic debugging to trace capture, serialization, load, state
  publication, and restored BatchQueryIp;
- make only the smallest production Rust correction justified by the observed
  C++ oracle;
- rerun the focused failing test before the complete eight-test subset.

No public protocol, persisted format version, HA election policy, etcd-only
production policy, or unrelated snapshot behavior changes in this wave unless
a failing test proves that such a change is necessary and a revised design is
approved.

## Parity Evidence and Gates

Only after all eight exact tests execute successfully:

1. Upgrade exactly the eight snapshot rows in
   `rust-repo/tools/store-validation/parity-map.json` from `missing` to
   `covered` with their exact Rust identities.
2. Run the focused eight-test subset and the full `test_storage` target.
3. Run `cargo fmt --check -p mooncake-store-master`.
4. Run all validation-tool unit tests, ordinary Store validation, four-manifest
   combined validation, and strict Store validation. Strict mode must return
   one solely because unrelated rows remain missing and must emit zero
   `ERROR` lines.
5. Source-plan the parity run and prove all eight tests are scheduled without
   calling `execute_plan`.
6. Run scoped pre-commit with `SKIP=mooncake-code-format`.
7. Verify staged and unstaged diffs under `mooncake-store`,
   `mooncake-transfer-engine`, and `mooncake-wheel/tests` are empty.

The Store LocalDisk io_uring optimization remains gated until the full
correctness objective passes.

## Investigation Outcome (Deferred)

The first implementation attempt produced a repeatable semantic RED in seven
of the eight fixtures. The empty request passed, while every fixture containing
a mounted segment lost its saved `base`, `te_endpoint`, and non-CXL `protocol`
after the first restore. The serializer and decoder preserve those fields; the
loss occurs deliberately in `restore_loaded_snapshot_state`, which invalidates
process-local routing coordinates and marks allocator segments runtime-unbound
until the owning client executes `ReMountSegment`. The same safety rule is
encoded in oplog replay and hot-standby tests.

Further reference inspection also narrowed the C++ oracle: BatchQueryIp is
called before restore, while inherited teardown compares the first and second
serialized segment snapshots and broader service state. Requiring restored
BatchQueryIp to expose the saved endpoint was therefore stronger than the C++
test and unsafe for the Rust new-term remount model. However, Rust's second
snapshot currently serializes the scrubbed coordinates, so C++-equivalent
snapshot idempotence still represents a real parity gap.

A safe correction requires separating durable snapshot coordinates from live
routing availability, or otherwise preserving re-save metadata without making
stale endpoints observable or allocatable. That change crosses snapshot
restore, oplog replay, hot standby, remount collision checks, QueryIp, and
segment-detail reporting, so it is not a low-risk correction local to these
eight tests. The experiment was removed, the 28-test storage baseline was
restored, and all eight manifest rows remain `missing`. This family is deferred
for a separately approved HA-state design; it is not N/A and must not be
upgraded using the ordinary BatchQueryIp tests.
