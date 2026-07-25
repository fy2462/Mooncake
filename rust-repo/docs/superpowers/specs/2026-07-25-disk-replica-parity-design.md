# Rust Store Disk Replica Parity Design

Date: 2026-07-25

## Objective

Match the observable C++ Store `Disk` replica lifecycle in the Rust Store:
allocation during put, client-side durable write, type-specific finalize or
revoke, read fallback, eviction notification, removal, administration, and HA
recovery. The implementation remains Rust-owned and does not link or call the
C++ Store.

This work is deliberately limited to the Rust FilePerKey and OffsetAllocator
storage implementations. Bucket, HF3FS/distributed data storage, LocalDisk
offload/promotion changes, and access-heat visualization are outside scope.

## C++ compatibility contract

When the master's configured storage root is non-empty, C++ `PutStart`
allocates one `Disk` replica in addition to the requested Memory and NoF
replicas. Its descriptor contains a deterministic file path and object size.
The client writes that disk replica before transferring Memory or NoF replicas,
then reports `PutEnd(Disk)` or `PutRevoke(Disk)` independently of the other
replica types.

The Rust implementation must preserve these externally visible properties:

- an empty storage root disables `Disk` allocation;
- an enabled storage root produces at most one `Disk` replica per object;
- the path is deterministic, tenant-safe, cluster-scoped, and contains no raw
  path traversal from a user key;
- `Disk` is not owned by a client and is not removed when a client heartbeat
  expires;
- disk completion and failure do not masquerade as Memory or LocalDisk state;
- a completed `Disk` replica is a cold-tier read fallback;
- disk backend eviction removes only `Disk` metadata;
- snapshots and oplog replay preserve enough data to read and manage the
  replica after failover.

## Architecture

### Typed descriptor

`mooncake_store_core::ReplicaDescriptor` gains a `file_path: String` field.
The field is empty for Memory, NoFSsd, and LocalDisk and non-empty for Disk.
The existing protobuf fields `file_path` and `object_size` remain wire-compatible
and become fully connected instead of being discarded by Rust conversion.

The descriptor remains one type rather than introducing a new enum payload in
this parity slice. This matches the existing Rust model and avoids a broad
refactor, while still making the Disk path explicit and strongly typed across
core, RPC, HA, and client code. Constructors and test fixtures must initialize
the field deliberately; serde defaults retain compatibility with older Rust
snapshots that lack it.

### Master allocation

`MasterRuntimeConfig.storage_fs_dir` continues to represent the C++
`root_fs_dir` switch. If it is non-empty, `PutStart` and batch put-start append
one allocating Disk descriptor after successful Memory/NoF allocation.

The path resolver produces:

```text
<storage_fs_dir>/<cluster_id>/<tenant-safe encoded object key>
```

The resolver is shared by scalar and batch operations. It accepts a canonical
`TenantId` and the validated user key, uses the existing scoped-key encoding,
and never joins an untrusted raw key as a path component. Parent directories
are created by the configured client backend, not by the Master.

Disk allocation does not consume Memory/NoF allocator handles. Tenant object
quota remains charged once per logical object, not once per replica. Disk
capacity enforcement stays in the storage backend.

### Client storage role

The client exposes a distinct attached `Disk` storage role backed by the
existing FilePerKey or OffsetAllocator implementation. It may reuse the same
concrete backend types as LocalDisk, but its lifecycle and RPC behavior remain
separate:

- LocalDisk is created by offload and carries `holder_client_id` and an RPC
  endpoint.
- Disk is allocated by `PutStart`, carries `file_path`, and has no holder.

Attaching LocalDisk storage alone must not silently enable C++-style Disk
replicas. Client construction validates that a master-returned Disk descriptor
can be served by the configured Disk role; otherwise the Disk replica is
revoked with a clear Store error.

For FilePerKey, the descriptor path identifies the durable object file. For
OffsetAllocator, `file_path` is the stable logical storage key/path identity
returned by the Master; physical arena offset remains private to the backend.
The client must not expose OffsetAllocator arena offsets in replica metadata.

### Write lifecycle

Scalar put, unsafe put-from/parts, and batch put follow the same ordering:

1. Call put-start and partition returned descriptors by replica type.
2. If a Disk descriptor exists, gather the source slices and write it through
   the attached Disk role first.
3. On disk success, issue `PutEnd(Disk)`; on disk failure, issue
   `PutRevoke(Disk)` and retain the original storage error for finalization.
4. Transfer Memory and NoFSsd replicas through Transfer Engine as today.
5. Apply the existing reliability-mode decision to Memory/NoFSsd results while
   accounting for the independently finalized Disk result exactly as C++ does.

Only one Disk descriptor is accepted. Multiple Disk descriptors or a Disk
descriptor with an empty path are treated as invalid master responses and are
revoked rather than partially written.

Upsert must use the same Disk-first rule and replace the durable object
atomically according to the selected backend. Remove and remove-all delete the
backend object only after the corresponding Master operation succeeds, matching
the existing Rust cleanup boundary.

### Read lifecycle

Replica selection retains the current tier order: Memory and NoFSsd are tried
before cold storage, LocalDisk before Disk. A selected Disk descriptor is read
through the attached Disk role and not through Transfer Engine.

The client validates that returned bytes equal `object_size`/`size`. Missing,
short, or corrupt backend data returns a storage error and does not fabricate a
successful cache hit. Automatic retry against a second descriptor is outside
this slice unless the existing client retry loop already provides it.

Disk reads do not trigger LocalDisk-to-Memory promotion. Promotion remains a
LocalDisk feature.

### Eviction and deletion

FilePerKey and OffsetAllocator eviction callbacks already identify evicted
keys. When the backend is attached in the Disk role, callbacks issue scalar or
batch `EvictDiskReplica` with `ReplicaType::Disk`. A LocalDisk role continues to
use `ReplicaType::LocalDisk` plus holder identity.

Master eviction removes all matching ordinary Disk descriptors for the key,
updates disk accounting once, and deletes the object only when no completed
replica remains. Busy Memory/NoF handle release rules are unaffected.

### HA and administration

Snapshot serialization, snapshot loading, and oplog application preserve
`file_path`, size, status, and `ReplicaType::Disk`. Older snapshots without
`file_path` remain loadable, but an empty-path Disk descriptor is not offered as
a readable replica and should be surfaced as degraded metadata in admin output.

Admin HTTP object detail reports Disk replicas with `file_path` and
`object_size`. Aggregate metrics remain bounded-cardinality: counts and bytes by
replica type are allowed, per-key Prometheus labels are not.

## Error and crash behavior

- Master path construction or configuration failure aborts put-start before
  publishing a Disk descriptor.
- Backend write failure is followed by `PutRevoke(Disk)`; revoke failure is
  reported without hiding the original write failure.
- `PutEnd(Disk)` failure leaves the stored bytes orphaned but not visible as a
  completed replica. Cleanup/reconciliation may remove the orphan; it must not
  be returned by reads.
- Strict OffsetAllocator persistence must complete before `PutEnd(Disk)`.
  Relaxed and Disabled modes retain their documented durability semantics.
- A process crash after durable write but before `PutEnd` yields an orphan file;
  a crash after `PutEnd` yields recoverable metadata plus backend bytes.
- HA replay must not convert Disk into LocalDisk or attach a client owner.
- Disk data I/O failures never require a new Transfer Engine ABI because this
  data path belongs to Store.

## Compatibility and rollout

The protobuf schema already contains the required Disk fields, so no wire field
number changes are needed. Rust-to-C++ and C++-to-Rust conversion tests must
prove the existing values round-trip.

Disk allocation is feature-enabled by non-empty `storage_fs_dir`, preserving
the C++ configuration switch. With an empty path, behavior remains unchanged.
Operators must configure a storage root that is meaningful to every Rust client
expected to service Disk descriptors. Local-only per-node storage should use
LocalDisk rather than Disk.

## Acceptance matrix

### Core and protocol

- Disk descriptor proto round-trip preserves path, size, status, and type.
- Memory, NoFSsd, and LocalDisk conversions keep an empty Disk path.
- Old serde snapshots without `file_path` load with an empty default.

### Master

- Empty storage root allocates no Disk replica.
- Enabled root allocates exactly one tenant-safe, cluster-scoped Disk replica
  for scalar and batch put-start.
- PutEnd/Revoke transition only the requested Disk replicas.
- Disk replicas survive client expiry.
- EvictDiskReplica(Disk) removes Disk but not LocalDisk.
- HA snapshot and oplog recovery retain the complete Disk descriptor.

### Client

- FilePerKey and OffsetAllocator each pass put/read/remove Disk lifecycle tests.
- Disk is written before Memory/NoF finalization.
- Disk success, write failure, end failure, and revoke failure have explicit
  assertions.
- Batch results remain aligned with input keys under mixed disk outcomes.
- Reads select LocalDisk before Disk and bypass Transfer Engine for Disk.
- Disk backend eviction sends the correct replica type.

### Integration

- Rust client + Rust master: put creates Memory plus Disk, data survives client
  restart, Memory removal falls back to Disk, and final deletion removes both
  metadata and bytes.
- C++-shaped protobuf fixtures interoperate with Rust conversion.
- Full `mooncake-store-core`, `mooncake-store-master`, and
  `mooncake-store-client` tests pass.
- `cargo check --workspace --all-targets`, formatting, Clippy disposition,
  pre-commit on touched files, and `git diff --check` are recorded.

## Explicit non-goals

- Bucket and HF3FS/distributed Disk backend completion.
- Changes to Transfer Engine or its ABI.
- LocalDisk offload/promotion redesign.
- Disk-to-Memory promotion.
- Per-key heat-map telemetry or Web UI.
- Migration or reconciliation of arbitrary legacy orphan files beyond the
  C++-compatible lifecycle described above.
