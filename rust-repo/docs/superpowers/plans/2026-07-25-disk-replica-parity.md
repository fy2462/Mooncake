# Rust Store Disk Replica Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the C++-compatible asynchronous `Disk` replica lifecycle in Rust Store with the existing FilePerKey and OffsetAllocator backends.

**Architecture:** Carry `file_path` as typed replica metadata from Master through protobuf and HA. Master conditionally allocates one Disk replica; Client copies caller data and schedules a managed background write that independently ends or revokes Disk. Disk reads and eviction stay in Store and bypass Transfer Engine.

**Tech Stack:** Rust, Tokio, tonic/prost, serde, existing FilePerKey and OffsetAllocator storage, Cargo tests.

## Global Constraints

- C++ Store is a compatibility oracle, never a Rust runtime/build dependency.
- No Transfer Engine ABI or implementation changes for Disk I/O.
- Only FilePerKey and OffsetAllocator; no Bucket or HF3FS completion.
- Do not alter LocalDisk offload or promotion.
- Empty `MasterRuntimeConfig.storage_fs_dir` disables Disk allocation.
- Disk has no client holder and survives client expiry.
- Accepted Disk writes are asynchronous and cannot retroactively change a successful foreground put.
- Every task uses RED → GREEN → focused regression → review → commit.

---

### Task 1: Make Disk replica metadata lossless

**Files:**
- Modify: `rust-repo/crates/mooncake-store-core/src/types.rs`
- Modify: `rust-repo/crates/mooncake-store-core/tests/test_types.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/proto_conv.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/replica_selection.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/transfer_meta.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/admin_http.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/allocator/strategies.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/catalog_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/offload.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_batch.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_ha.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_oplog.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_segment.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_storage.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_tenant_id_parity.rs`

**Interfaces:**
- Produces: `ReplicaDescriptor::file_path: String`
- Produces: `ReplicaDescriptor::disk(file_path: String, size: u64) -> Self`

- [ ] **Step 1: Write failing descriptor and conversion tests**

```rust
let disk = ReplicaDescriptor::disk("/cache/mooncake/key".into(), 4096);
assert_eq!(disk.replica_type, ReplicaType::Disk);
assert_eq!(disk.file_path, "/cache/mooncake/key");
assert_eq!(disk.size, 4096);
assert!(disk.holder_client_id.is_none());
let restored = replica_from_proto(&replica_to_proto(&disk));
assert_eq!(restored.file_path, disk.file_path);
assert_eq!(restored.size, disk.size);
```

Also deserialize an old JSON fixture without `file_path` and assert empty default.

- [ ] **Step 2: Run RED**

```bash
cargo test -p mooncake-store-core disk_replica_descriptor
cargo test -p mooncake-store-master proto_conv::tests::disk_descriptor_round_trip
```

Expected: missing field/constructor or discarded path.

- [ ] **Step 3: Implement the typed field**

Add `#[serde(default)] pub file_path: String`. The `disk` constructor sets nil
segment, empty transport fields, `Allocating`, `Disk`, no holder, and the given
path/size. Proto conversion maps `file_path` both ways and uses `object_size` for
Disk size. All non-Disk constructors set an empty path.

- [ ] **Step 4: Verify and commit**

```bash
cargo test -p mooncake-store-core
cargo test -p mooncake-store-master proto_conv
cargo check -p mooncake-store-master -p mooncake-store-client --all-targets
git add rust-repo/crates/mooncake-store-core rust-repo/crates/mooncake-store-master
git commit -m '[Store] preserve typed Disk replica paths'
```

---

### Task 2: Allocate Disk replicas in Master put/upsert

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/helpers.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_put.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches/batch_put_start.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_upsert.rs`
- Create: `rust-repo/crates/mooncake-store-master/tests/test_disk_replica.rs`

**Interfaces:**
- Produces: `disk_replica_for_object(&MasterRuntimeConfig, &TenantId, &str, u64) -> Result<Option<ReplicaDescriptor>, Status>`

- [ ] **Step 1: Write RED scalar, batch, upsert, and path tests**

Assert empty root yields no Disk. Enabled root yields exactly one Disk with no
holder, correct size, and a path below `<root>/<cluster>`. Use `/`, `..`, Unicode,
and the same key in two tenants; paths must be safe, deterministic, and distinct.

- [ ] **Step 2: Run RED**

```bash
cargo test -p mooncake-store-master --test test_disk_replica
```

- [ ] **Step 3: Implement one shared resolver**

Build a path from the canonical tenant-scoped key using a stable xxh64 hex
component. Reject invalid cluster configuration. Append one descriptor only
after Memory/NoF allocation succeeds and before object metadata publication.
Reuse the helper in scalar put, batch put, scalar upsert, and batch upsert.

- [ ] **Step 4: Test type-specific transitions and quota**

Assert `PutEnd(Disk)` completes only Disk, `PutRevoke(Disk)` removes only Disk,
and tenant quota remains charged once per logical object.

```bash
cargo test -p mooncake-store-master --test test_disk_replica
cargo test -p mooncake-store-master --test test_batch
cargo test -p mooncake-store-master --test test_tenant_id_parity
git add rust-repo/crates/mooncake-store-master
git commit -m '[Store] allocate Disk replicas from configured storage root'
```

---

### Task 3: Add an explicit client Disk role and managed executor

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/local_storage_backend/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/ha.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs`
- Create: `rust-repo/crates/mooncake-store-client/src/client/disk.rs`
- Create: `rust-repo/crates/mooncake-store-client/tests/test_disk_replica.rs`

**Interfaces:**
- Produces: `DiskStorage::{write, read, delete, remove_all}` accepting both
  descriptor path and canonical tenant-scoped storage key
- Produces: `DiskWriteExecutor::try_submit(DiskWriteJob) -> StoreResult<()>`
- Produces explicit FilePerKey and OffsetAllocator Disk attachment methods.

- [ ] **Step 1: Write RED role separation tests**

LocalDisk attachment alone must not serve Disk. Explicit Disk attachment must
round-trip bytes with both backends. Empty descriptor path must fail.

- [ ] **Step 2: Write RED executor lifecycle tests**

With barriers and an injected capacity-one executor, prove accepted jobs run
once, full/stopped queues reject synchronously, shutdown rejects new jobs and
drains accepted jobs, and backend/Master handles remain alive through drain.

- [ ] **Step 3: Implement the role wrapper**

```rust
#[derive(Clone)]
pub(crate) struct DiskStorage { backend: AttachedLocalStorage }
```

For FilePerKey, add path-aware operations on the descriptor's actual path after
proving it is contained by the configured root/cluster directory. Never join or
reinterpret an untrusted path. For OffsetAllocator, use the canonical
tenant-scoped storage key because arena placement is private, while retaining
`file_path` as descriptor identity. Keep this role separate from existing
`local_storage`, even when both wrap the same concrete backend type.

- [ ] **Step 4: Implement the bounded executor**

Use bounded `tokio::sync::mpsc` and one tracked worker. A job owns `Vec<u8>`,
descriptor, canonical tenant-scoped storage key, key, tenant, storage, and a
cloneable Master RPC handle. Shutdown
closes the sender and awaits the worker. Do not detach tasks.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p mooncake-store-client --test test_disk_replica disk_role
cargo test -p mooncake-store-client --test test_disk_replica executor
cargo check -p mooncake-store-client --all-targets
git add rust-repo/crates/mooncake-store-client
git commit -m '[Store] add managed client Disk storage jobs'
```

---

### Task 4: Schedule Disk writes from every write API

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/client/disk.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/write.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/write_parts.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/write_batch.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/upsert.rs`
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_disk_replica.rs`

**Interfaces:**
- Produces: `schedule_disk_write(key, tenant, replicas, payload) -> StoreResult<DiskSchedule>`
- Produces: `DiskSchedule::{NotAllocated, Accepted, Rejected}`

- [ ] **Step 1: Write RED ordering/ownership tests**

For put, put-from, put-parts, batch-put, upsert, and batch-upsert prove payload
copy and enqueue precede the first TE write; foreground completion does not wait
for a blocked accepted job; duplicate/empty-path descriptors revoke; and mixed
batch statuses stay aligned.

- [ ] **Step 2: Implement shared validation and gathering**

Allow zero or one Disk descriptor. Checked-sum slices into one owned `Vec<u8>`.
Raw-pointer APIs copy before yielding or scheduling so background work never
references caller memory.

- [ ] **Step 3: Implement background finalize behavior**

On backend success, notify any evicted Disk keys then call `PutEnd(Disk)`. On
write failure call `PutRevoke(Disk)` and record write plus revoke failures.
Queue rejection revokes synchronously. Foreground Memory/NoF continues through
the existing `determine_finalize_decision`; accepted Disk outcome is independent.

- [ ] **Step 4: Verify and commit**

```bash
cargo test -p mooncake-store-client --test test_disk_replica write_
cargo test -p mooncake-store-client client::write
cargo test -p mooncake-store-client client::upsert
cargo test -p mooncake-store-client --test test_client
git add rust-repo/crates/mooncake-store-client
git commit -m '[Store] schedule asynchronous Disk writes before transfers'
```

---

### Task 5: Complete Disk read, eviction, and deletion

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/client/transfer_read.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/replica_selection.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/remove.rs`
- Modify: `rust-repo/crates/mooncake-store-client/src/client/disk.rs`
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_disk_replica.rs`

- [ ] **Step 1: Write RED read tests**

Disk must bypass mocked TE, return exact bytes from both backends, reject
missing/short data, and lose selection to a completed LocalDisk when both exist.

- [ ] **Step 2: Implement the read branch**

Before the TE branch, require attached Disk storage, read `replica.file_path`,
and verify returned length equals `replica.size`. Do not trigger promotion.

- [ ] **Step 3: Write RED eviction/removal tests**

Force each backend to evict and assert `BatchEvictDiskReplica(..., Disk)`.
The same backend in LocalDisk role must still send LocalDisk. Successful Master
remove/remove-all deletes Disk bytes; failed Master removal preserves them.

- [ ] **Step 4: Implement callbacks, verify, and commit**

```bash
cargo test -p mooncake-store-client --test test_disk_replica read_
cargo test -p mooncake-store-client --test test_disk_replica eviction_
cargo test -p mooncake-store-client client::remove
git add rust-repo/crates/mooncake-store-client
git commit -m '[Store] complete Disk read eviction and removal lifecycle'
```

---

### Task 6: Preserve Disk through HA and administration

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/catalog_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/admin_http.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_disk_replica.rs`

- [ ] **Step 1: Write RED snapshot/oplog/admin tests**

Round-trip a completed Disk and assert path, size, status, type, and empty
holder. Load an old empty-path Disk fixture as degraded metadata and exclude it
from readable query results. Admin JSON must expose `file_path/object_size`.

- [ ] **Step 2: Implement lossless recovery**

Extend the existing C++-compatible Disk snapshot shape, construct via
`ReplicaDescriptor::disk`, then restore status. Master startup does not touch
the file. Stale-client cleanup removes LocalDisk but retains Disk.

- [ ] **Step 3: Verify and commit**

```bash
cargo test -p mooncake-store-master --test test_catalog_snapshot
cargo test -p mooncake-store-master --test test_disk_replica ha_
cargo test -p mooncake-store-master admin_http
git add rust-repo/crates/mooncake-store-master
git commit -m '[Store] retain Disk replicas across HA and administration'
```

---

### Task 7: End-to-end parity and disposition

**Files:**
- Create: `rust-repo/crates/mooncake-store-client/tests/test_disk_live_e2e.rs`
- Create: `rust-repo/change_logs/2026-07-25-003.md`

- [ ] **Step 1: Add Rust Master + Client lifecycle tests**

For FilePerKey and Strict OffsetAllocator: start Master with a temporary root,
attach Disk storage, put and drain, assert completed Memory+Disk, restart the
client/backend, remove Memory, read via Disk fallback, then remove the object
and prove metadata plus bytes disappear.

- [ ] **Step 2: Add C++-shaped wire fixtures**

Decode a protobuf Disk descriptor with path/size and encode the Rust descriptor
back with unchanged field numbers and values. Do not link C++ Store.

- [ ] **Step 3: Run focused and complete verification**

```bash
cargo test -p mooncake-store-core
cargo test -p mooncake-store-master --test test_disk_replica
cargo test -p mooncake-store-client --test test_disk_replica
cargo test -p mooncake-store-client --test test_disk_live_e2e
cargo test -p mooncake-store-master -p mooncake-store-client --no-fail-fast
cargo check --workspace --all-targets
cargo fmt --all -- --check
cargo clippy -p mooncake-store-core -p mooncake-store-master -p mooncake-store-client --all-targets -- -D warnings
git diff --check
```

If strict Clippy is blocked by existing warnings, record the exact baseline and
prove no new warning originates in touched lines. Run pre-commit only on touched
files and do not retain unrelated rewrites.

- [ ] **Step 4: Record disposition and request final review**

Document C++ oracle locations, async semantics, supported backends, TE
non-involvement, test totals, environment gates, and excluded Bucket/HF3FS,
LocalDisk promotion, and heat-map work. Review the full change range for wire
compatibility, async lifetime safety, tenant isolation, and HA durability.

- [ ] **Step 5: Commit**

```bash
git add rust-repo/crates/mooncake-store-client/tests/test_disk_live_e2e.rs rust-repo/change_logs/2026-07-25-003.md
git commit -m '[Store] complete Rust Disk replica parity'
```

---

## Final self-review checklist

- Every approved C++ contract maps to a named task and test.
- `file_path` is never hidden in `segment_name` or lost in conversion.
- Disk I/O never reaches Transfer Engine.
- Foreground calls copy payload before asynchronous scheduling.
- Accepted jobs drain before client resources are released.
- Disk and LocalDisk ownership, cleanup, eviction, and promotion stay distinct.
- Empty storage root preserves current behavior.
- No Bucket, HF3FS, heat-map, or unrelated refactor work enters the change.
