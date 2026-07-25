# Rust OffsetAllocator Crash-Consistent Persistence Design

Date: 2026-07-25

## Implementation status

Completed on 2026-07-25. Commits `18485720` through `0bd6e962`
implement the approved client-side persistence design, including follow-up
review fixes for record bounds, read-extent lifetime, arena generations,
ownership, retry behavior, relaxed-generation rollover, capacity-bound
recovery, and fresh Relaxed scheduling. The final
verification and disposition are recorded in
`change_logs/2026-07-25-001.md`.

The delivered boundary remains Rust-owned: neither protobuf nor C++ Store
code is a runtime dependency, and the master crate's separate append-only
`StorageBackendType::OffsetAllocator` adapter is unchanged. The verified
limitations are the Rust-native checkpoint/allocator representation, legacy
raw-record integrity, unconditional per-record `sync_data` cost, best-effort
`Drop` error visibility, and logical abrupt-exit tests rather than a physical
power-loss harness.

## Goal and compatibility oracle

Port the observable persistence and recovery contract introduced by C++ Store
commit `30eff961` into the Rust client-side
`OffsetAllocatorStorageBackend`. The C++ Store remains a behavioral oracle and
is not linked or called by Rust.

This is the client SSD backend corresponding to the C++
`OffsetAllocatorStorageBackend`. The master crate's append-only
`StorageBackendType::OffsetAllocator` snapshot adapter is a separate surface
and is not the target of this commit.

## Chosen approach

Use the C++ v3 protocol semantics with a Rust-native allocator checkpoint.
Do not serialize the C++ allocator tree or copy its implementation. Rust keeps
its `PersistedIndex` and free-extent reconstruction, but stores enough durable
record information to prove that a checkpoint never exposes a torn or newer
record after a crash.

Alternatives rejected:

- Copying the C++ allocator serialization would couple Rust to an unrelated
  internal data structure and violate the replacement boundary.
- Merely fsyncing the current JSON index would not detect an extent overwritten
  after an older relaxed checkpoint, especially when record CRC is disabled.
- Keeping always-on JSON persistence would miss the C++ disabled/relaxed/strict
  configuration contract.

## Configuration

Add `OffsetPersistMode::{Disabled, Relaxed, Strict}` through the separate
`OffsetPersistenceConfig`, preserving the existing `OffsetAllocatorConfig`
field layout.

- `Disabled` is the C++-compatible default. No checkpoint is recovered or
  written.
- `Relaxed` starts its interval clock when a fresh or recovered backend is
  initialized, checkpoints dirty state once that interval elapses, and makes a
  best-effort final checkpoint from its graceful destructor.
- `Strict` performs a data durability barrier and checkpoint for every
  successful write/eviction mutation. A barrier or checkpoint failure is
  returned to the caller.

Environment compatibility:

- `MOONCAKE_OFFSET_PERSIST_MODE=disabled|relaxed|strict`;
- `MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS`, with relaxed mode requiring at
  least five seconds;
- `MOONCAKE_OFFSET_RECORD_CRC`, where `0`, `false`, or `off` disables CRC.

Unknown or malformed environment values retain defaults, matching existing
configuration parsing style. Programmatic validation rejects an invalid
interval.

## Durable record format

New writes use a Rust implementation of the C++ v3 logical record:

```text
[u32 key_len][u32 value_len][u64 write_seq][u32 flags][u32 crc32]
[key bytes][zero padding][value bytes]
```

The value begins at a 4 KiB-aligned offset relative to the record start. Header
integers use little-endian encoding, which is explicitly encoded/decoded rather
than relying on Rust struct layout. The known flag is `HAS_CRC`.

CRC-32C covers the header prefix before `crc32`, the key bytes, and the value
bytes. Recovery rejects unknown flags, impossible lengths, key/header/index
mismatches, sequence mismatches, out-of-range extents, truncated records, and
CRC mismatches. Runtime reads use the already validated checkpoint metadata and
read only the value region.

Every successful write receives a monotonic sequence. A checkpoint stores the
next sequence. If an extent referenced by an older checkpoint was overwritten
after that checkpoint, the on-disk header sequence no longer matches the entry
and the entry is dropped. This is the integrity guard when CRC is disabled.

## Checkpoint format and commit ordering

The checkpoint is a versioned Rust index containing:

- checkpoint format version;
- next write and FIFO sequences;
- current key-to-record entries;
- allocator high-water offset needed to rebuild free extents;
- finalized eviction tombstones accumulated since the last checkpoint.
- configured allocator capacity (`quota_bytes`), where zero preserves the
  automatic-capacity configuration rather than a transient resolved value.

The checkpoint file is written as a version-3 envelope with a CRC-32C over its
serialized payload, including capacity. Each entry records the expected record
flags so recovery cannot downgrade a checksummed record by trusting a corrupted
arena flag. Version-1 and version-2 envelopes remain deliberate legacy inputs:
they are accepted only when their arena and every recovered extent fit the
current quota, then upgraded to version 3 at the next required checkpoint.
Version-1 flags are recovered from the validated arena header because the older
envelope did not carry an independently authenticated copy. The exact file is
Rust-owned; protobuf and C++ formats do not change.

Checkpoint ordering is:

1. finish every record write;
2. `sync_data` the arena file;
3. serialize the in-memory checkpoint;
4. write a temporary checkpoint file and `sync_all` it;
5. atomically rename it over the checkpoint;
6. fsync the parent directory;
7. only then clear dirty/tombstone state and advance the checkpoint timestamp.

A failure before the rename in step 5 leaves the previous checkpoint
authoritative. After rename but before the parent-directory sync succeeds, the
new checkpoint is visible to the running process, while crash recovery may see
the old or new name depending on filesystem durability. In either case, dirty
state remains set so a later operation can retry. Stale temporary files are
removed during recovery.

## Recovery

Recovery is transactional: build a candidate state, validate all metadata and
records, then publish it. It never partially installs a recovered index.

- No checkpoint in an enabled mode means a genuine fresh start.
- A version-3 checkpoint is recovered only when its authenticated capacity
  exactly equals the current configured capacity (including zero for automatic
  capacity) and its arena is no larger than the currently resolved capacity. A
  mismatch starts fresh and removes the incompatible
  checkpoint/arena generation while the backend holds directory ownership.
- Legacy unversioned, version-1, and version-2 state is recovered only when its
  arena and all surviving extents are within the current quota, and is marked
  for a capacity-bearing upgrade.
- An unsupported version, invalid envelope checksum, malformed checkpoint,
  missing/truncated arena, or corrupt records causes a safe fresh index while
  preserving valid records from the same checkpoint when record-level recovery
  can isolate them.
- Resource/open/permission failures are returned and do not truncate potentially
  recoverable data.
- Fresh-start orphan cleanup occurs only after checkpoint and arena inspection
  succeeds; cleanup I/O failures are returned. Reusable and appended allocations
  also independently reject any requested end beyond the configured quota.
- Record-level corruption drops only the affected key and frees its extent.
- Tombstoned keys are removed after record scanning.
- FIFO state is repaired deterministically from surviving sequence numbers.

The current legacy JSON index/raw-value layout remains readable. Legacy entries
are marked as headerless compatibility records and retain their existing byte
offsets. New writes use v3 records. A later checkpoint may contain both kinds;
this avoids destructive in-place rewriting. Integrity guarantees for legacy
records remain limited to their pre-feature format.

## Mutation and eviction rules

Data is durable before a checkpoint may reference it. Finalized evictions are
recorded only after the caller commits eviction; rolled-back victims never
become tombstones. Rewriting a tombstoned key removes its stale tombstone before
the next checkpoint.

Strict mode returns persistence failures. Relaxed mode retains dirty state and
continues with the in-memory cache after a failed periodic checkpoint, matching
the weaker availability contract. Disabled mode does not promise restart
recovery.

## Tests and failure injection

TDD coverage must include:

- config parsing/defaults/validation;
- strict recovery round trip and graceful relaxed checkpoint;
- abrupt relaxed restart recovering only the last checkpoint;
- data-before-metadata ordering and strict failure propagation;
- failure before/after temporary-file sync and rename, proving the prior
  checkpoint remains usable;
- stale temporary checkpoint cleanup;
- corrupt checkpoint checksum/version fallback;
- truncated header/value, unknown flags, bad sequence/key/length, and CRC
  corruption dropping only affected records;
- CRC-disabled round trip and overwrite detection through the sequence guard;
- finalized eviction versus rollback and tombstone/rewrite behavior;
- legacy raw-index compatibility;
- capacity-authenticated checkpoints, v1/v2 upgrade, quota increase/decrease,
  oversized legacy/orphan cleanup, and defensive free-extent bounds;
- injected-clock fresh Relaxed behavior before and at the configured interval;
- no schema, C++ dependency, or unrelated master-backend change.

## Delivery boundary

Keep public read/write/eviction APIs source compatible. Changing the default
persistence mode to `Disabled` is intentional C++ parity; restart tests that
require recovery must opt into `Strict` or `Relaxed` explicitly. Preserve the
existing exhaustive-literal shape of `OffsetAllocatorConfig`; persistence
controls live in the separate `OffsetPersistenceConfig`, with `new(config)`
reading the C++ environment controls and `new_with_persistence` providing an
explicit, deterministic path.
