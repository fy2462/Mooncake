# Snapshot child restore semantics parity design

## Scope

Close four portable `SnapshotChildProcessTest` rows through Rust's production
Master, catalog, and restore boundaries:

- `RestoreRebuildsGroupedObjectRouting`
- `RestoreFallsBackToPreviousHealthySnapshotWhenLatestIsCorrupted`
- `RestoreCleansNonCompleteReplica`
- `RestoreCleansExpiredLease`

This batch does not claim C++ fork, signal, child-process, fixed metadata-shard,
or destructor mechanics. Rust has no C++ metadata-shard routing table; the
portable grouped-object contract is that a nonempty group identity survives the
catalog round trip and does not prevent public lookup and removal by key after
restore.

## Production replacement boundary

Each witness starts with public `MasterService` RPCs, captures state with
`MasterServiceImpl::capture_loaded_snapshot`, publishes the production C++
three-object catalog layout through `CatalogBackedSnapshotProvider`, loads a
candidate through the production catalog reader, and atomically installs it
with `restore_loaded_snapshot_state`. Restored Memory replicas are rebound
through the public segment-mount RPC before public observations, because Rust
intentionally invalidates old-process addresses and endpoints during restore.

The final evidence uses public RPCs only: `GetReplicaList`, `ExistKey`,
`GetAllKeys`, and `Remove`. Inspecting the `LoadedSnapshot` returned by the
production provider is permitted only to prove the serialized group identity;
no private Master map is a final oracle.

## Per-row behavior

### Grouped object routing

Create and complete one Memory object with a nonempty, distinct group ID, grant
it a normal read lease, publish and load it, and assert that the production
loaded snapshot retains the exact group ID. Restore into a fresh service,
remount the durable segment identity, fetch the replica by its original key,
force-remove it, and prove `ExistKey` is false.

### Corrupt newest fallback

Create key 1, grant its read lease, and publish snapshot 1. Then create key 2,
grant its read lease, and publish snapshot 2. Overwrite only snapshot 2's
`metadata` object with the C++ corruption payload. The production candidate
loader must select snapshot 1. After atomic restore and public remount, key 1
must be readable and the public all-keys result must omit key 2, proving no
partial state from the corrupt candidate was installed.

### Non-complete cleanup

Create one complete leased object and one PutStart-only object in the same
source service, publish, load, and restore. The complete object must remain
publicly readable, while the public all-keys result must omit the incomplete
object.

Rust currently retains every non-complete replica whenever `put_start_time` is
present. The minimal production correction is to retain a non-complete replica
only when it is owned by a durable replication task, or when it belongs to an
already committed in-place upsert generation. An uncommitted initial PutStart
has neither protection and is quarantined/released by the existing restore
cleanup path. This preserves durable Copy/Move recovery and the existing
committed in-place-upsert contract.

### Expired lease cleanup

Complete two ordinary objects. Leave one with PutEnd's zero-duration lease and
refresh the other's lease through a public read. Publish and restore the same
snapshot. The refreshed object must remain publicly readable and the public
all-keys result must omit the expired object.

## Error handling and atomicity

Catalog corruption is handled by candidate fallback; a malformed newest
candidate must never reach the atomic restore function. Restore validation and
allocator rebuilding remain fail-closed. The non-complete cleanup reuses the
existing delayed-release quarantine so allocator ranges are not made reusable
without the established release delay.

## Evidence and gates

Write each exact test before changing production behavior. Existing behavior
tests must receive an independent primary-assertion mutation RED; the
non-complete row must first fail semantically against current production code.
After GREEN, run every exact test for ten consecutive rounds, the focused
snapshot-restore module, catalog integration target, Master library and complete
package, all-target check, four parity validators, validator self-tests, both
shell contracts, touched-file pre-commit, Rust 2024 formatting, JSON/diff
checks, and a zero C/C++ change audit. Upgrade exactly four manifest rows and
append exactly four remediation records only after the behavior is proven.
