# Master Snapshot Codec Parity Design

## Context

The Store parity manifest has four applicable missing rows from
`ha/snapshot/master_snapshot_codec_test.cpp`:

- `MasterSnapshotCodecTest.EncodeDecodeRoundTrip`
- `MasterSnapshotCodecTest.EncodeDecodeRoundTripWithMemoryReplica`
- `MasterSnapshotCodecTest.DecodeWithCorruptPayloadFails`
- `MasterSnapshotCodecTest.DecodeWithInvalidTaskFieldTypeReturnsError`

Rust already writes the C++ three-object catalog layout (`segments`,
`metadata`, and `task_manager`), loads segments before metadata, maps decoder
failures to `HaError::Snapshot`, and restores a `LoadedSnapshot` atomically into
a service. Existing tests cover parts of those boundaries independently, but
none proves the exact four C++ scenarios through one fresh-service pipeline.

## Chosen Design

Add four crate unit tests to `service::snapshot_restore_tests`. That location
can use the production `CatalogBackedSnapshotProvider`, public Master RPCs,
public `capture_loaded_snapshot`, and the crate-private production atomic
restore function without exposing a new API.

Each positive test follows the same boundary:

1. Create a real source `MasterServiceImpl`.
2. Capture its state with `capture_loaded_snapshot`.
3. Publish it through an embedded catalog and local snapshot object store.
4. Load it through `CatalogBackedSnapshotProvider`.
5. Restore it atomically into a fresh `MasterServiceImpl`.
6. Observe the result through public Master RPCs.

This is preferred over a catalog-only codec test because the C++ oracle
requires a fresh serving Master after decode. A full HotStandby controller
would add leader-state machinery unrelated to these codec rows and would make
the exact raw-payload assertions less direct.

Tests may call crate-private production functions because they live inside the
crate, but they must not assert private maps as their semantic evidence.

## Exact Parity Witnesses

### Empty round trip

Capture a default empty service, publish it, and download the exact
`segments`, `metadata`, and `task_manager` object keys. Assert all three byte
buffers are nonempty. Load and restore into a fresh service without error.
Use public `FetchTasks` to prove the restored task catalog is empty and capture
the fresh service only to prove its public snapshot surface contains no
objects, segments, or tasks.

### Memory replica round trip

Use public RPCs to mount a 16-MiB Memory segment at base `0x300000000` with
name and endpoint `codec_test_segment`. Use public `PutStart`/`PutEnd` for
default-tenant key `memory_replica_key`, size 1024, and one Memory replica.
Capture, publish, load, and restore into a fresh service. Public
`GetReplicaList` must return exactly one Memory replica with the original
segment name, endpoint, and size. This makes the segments-before-metadata
dependency observable: metadata restore cannot bind the replica if the segment
was not restored first.

Rust's production catalog loader intentionally rejects completed objects whose
read lease and soft pin have both expired. Like C++, `PutEnd` grants a
zero-duration initial lease, while the C++ codec fixture does not run the
catalog loader's expiry policy. The Rust witness therefore performs one public
source `GetReplicaList` after `PutEnd` and before capture. This both proves the
source object is readable and establishes the ordinary configured read lease;
it does not mutate private state, add pinning, or bypass the production loader.

Rust also intentionally invalidates process-local Memory addresses and
transport endpoints on fresh-service restore. After atomic restore, the
witness remounts the exact durable segment identity through public
`MountSegment` with the original client id and runtime coordinates before the
final query. This is the safe Rust replacement for C++ reusing snapshot-time
coordinates, and it proves the restored segment/metadata relationship without
flipping private handle flags.

### Corrupt all three payloads

Publish one valid empty snapshot, then overwrite its exact `metadata`,
`segments`, and `task_manager` objects with `{1,2,3}`, `{4,5,6}`, and
`{7,8,9}` respectively. With no healthy candidate, loading must return
`HaError::Snapshot`. All three objects are corrupted in the same snapshot, as
in the C++ oracle; the test does not substitute isolated decoder helper calls.

### Invalid task field type and fallback

Publish an older healthy empty snapshot and then a newer valid snapshot.
Replace the newer `task_manager` object with zstd-compressed MessagePack for
one eight-field task whose id is integer `12345`, followed by integer type and
status, strings `payload`, `message`, and `assigned`, and zero integer
timestamps. Wrap the provider load in `catch_unwind`: it must not unwind.

The production candidate loader must reject the newer snapshot with the
snapshot error category and fall back to the older healthy descriptor. Assert
the returned snapshot id is the older id. A second provider containing only
the malformed candidate must return `HaError::Snapshot`, proving both the
error category and the fallback-preserving behavior described by the C++
regression.

## TDD and Evidence

Add the four tests first with their final stable names and retain the initial
RED result. Because the production behavior is expected to exist, the RED may
be an assertion mutation for each test rather than a production compile gap.
Each mutation must change a primary assertion: raw-payload nonemptiness,
restored replica cardinality/type, corrupt-load success, or malformed-candidate
selection/error category.

After restoring the exact assertions, run every stable test ten times and
retain a transcript containing 40 one-test passes, no failures, no zero-test
selections, and a zero command exit. Run the snapshot-restore module, catalog
snapshot integration target, Master library, complete Master package,
all-targets check, four parity validators, validator self-tests and shell
contracts, Rust 2024 formatting, pre-commit on touched files, JSON/diff checks,
and the C/C++ zero-change guard.

## Manifest and Ledger

Change exactly the four named manifest rows from `missing` to `covered` and
append exactly four remediation records. Cite the implementation commit, exact
Rust test filter, retained RED/GREEN evidence, and broad gates. The
authoritative aggregate missing count moves from 859 to 855; no other row
changes status.

## Non-Goals

- No new production API or test-only production behavior.
- No private-map assertion as parity evidence.
- No claim of byte-for-byte C++ encoding beyond the shared catalog format.
- No weakening of the invalid-field witness to a short or malformed array.
- No changes or formatting to C/C++ files.
- No changes to unrelated missing or not-applicable rows.
