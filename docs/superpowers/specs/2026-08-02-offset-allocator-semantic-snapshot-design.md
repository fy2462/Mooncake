# Offset Allocator Semantic Snapshot Design

## Goal

Cover the five applicable C++ OffsetAllocator serialization rows with a stable
Rust production boundary that serializes one real Offset-backed segment,
restores the same semantic allocator state atomically, and returns validated
descriptors for every restored live allocation. The implementation must not
modify or format C/C++ sources and must not claim compatibility with the C++
private bin or node representation.

## Scope

The covered C++ rows are:

- `OffsetAllocatorTest.SerializationEmptyAllocator`
- `OffsetAllocatorTest.SerializationOneElementAllocator`
- `OffsetAllocatorTest.SerializationRandomAllocatedAllocator`
- `OffsetAllocatorTest.AllocateAfterDeserialization`
- `OffsetAllocatorTest.ChainedAllocationAndDeserialization`

The feature applies only to ordinary, runtime-bound, Offset-backed Memory or
NoF segments managed by `SegmentAllocator`. Cachelib layouts, CXL routing
aliases, the CXL global layout, whole-facade persistence, and C++ wire-format
compatibility are explicitly outside this batch.

## Alternatives

### Recommended: canonical per-segment semantic snapshot

Serialize one segment's public identity, allocator configuration, accounting,
live ranges, and canonical free ranges. Restore through the existing validated
descriptor-driven `restore_segment` path. This keeps the new boundary small,
reuses production overlap and capacity checks, and avoids coupling persistence
to the Offset implementation's container layout.

### Rejected: serialize the entire `SegmentAllocator`

This would mix unrelated placement strategy, multiple segments, Cachelib, and
CXL state into tests whose C++ oracle is one Offset allocator. It increases the
migration and validation surface without improving parity evidence.

### Rejected: derive serde directly on private allocator structs

Raw serde would freeze `HashMap` ordering and private representation, make
snapshots nondeterministic, and make future implementation changes into format
breaks. It would also encourage equality claims about internals that the Rust
replacement boundary does not share with C++.

## Architecture

Add `allocator/offset_snapshot.rs` as the sole owner of the wire envelope,
payload types, validation, and `SegmentAllocator` snapshot methods. The
existing `allocator/mod.rs` registers the module but retains allocation,
release, and descriptor-driven restore behavior. The five parity tests remain
in `allocator/offset_layout.rs`, matching the manifest evidence location and
driving the public facade rather than private snapshot helpers.

The public methods are:

```rust
pub fn serialize_offset_segment(
    &self,
    segment_id: &Uuid,
) -> Result<Vec<u8>, String>;

pub fn restore_offset_segment_snapshot(
    &mut self,
    encoded: &[u8],
) -> Result<Vec<ReplicaDescriptor>, String>;
```

The restore return value is the Rust replacement for rebinding C++ allocation
handles. Each descriptor is constructed only after the snapshot is validated
and the live range is installed. Existing `SegmentAllocator::release` then
revalidates segment UUID, segment name, offset, and exact size before freeing
the range.

## Wire Format

The envelope contains:

1. an eight-byte Rust-specific magic;
2. a little-endian envelope version;
3. the exact MessagePack payload length;
4. an xxHash64 checksum of the payload;
5. one named-field MessagePack payload.

The decoder requires exact total input consumption. Removing or appending one
byte therefore fails before any allocator mutation. Unknown envelope or
payload schema versions and checksum mismatches fail closed.

The version-one payload contains:

- `schema_version`;
- `AllocatorSnapshotConfig`;
- the complete `Segment` identity, including base and capacity;
- the owning client UUID;
- exact accounted used bytes;
- live `(offset, size)` ranges sorted by offset;
- free `(offset, size)` ranges sorted by offset.

Sorted vectors make serialization deterministic. Free ranges are included so
the payload explicitly preserves the C++ tests' semantic free-space state,
but restore does not trust the redundant data. It derives the exact complement
of the validated live ranges and requires it to equal the encoded free ranges.

## Validation and Atomicity

Serialization rejects absent segments, Cachelib layouts, CXL aliases, unbound
runtime state, and inconsistent internal accounting. It checks that live and
free ranges are nonzero, sorted, disjoint, in bounds, cover the complete
capacity exactly once, and that live-byte sum equals `used`.

Restore performs all envelope, schema, allocator-configuration, identity,
range, accounting, and node-budget validation before making state visible. It
requires the receiving `SegmentAllocator` snapshot configuration to equal the
encoded configuration and rejects duplicate segment UUIDs. Installation uses
`restore_segment`, whose failure path removes the provisional segment. After
installation, the generated report and deterministic reserialization must
match the encoded semantic state; any failure removes the restored segment.

The returned descriptors use the restored segment's UUID, name, base,
protocol, client owner, exact offset, and exact requested size. No allocation
is created, released, or moved merely by decoding corrupt input.

## Test Design

All expectations are derived from literal capacities, request sizes, and
seeded operation sequences rather than snapshot internals.

- Empty: base 16 KiB and capacity 1 MiB round-trip deterministically; one-byte
  truncation and extension fail atomically; a second clean restore succeeds.
- One live allocation: retain an exact 1,024-byte descriptor, round-trip,
  reject short and long images, restore again, release the rebound descriptor,
  and prove the segment is completely free.
- Random large matrix: for 100 seeded capacities in `[2 GiB + 1, 1 TiB]`, run
  200 optional-add/random-remove/mandatory-same-size replacement steps,
  round-trip, reject short and long images, restore again, release every
  rebound descriptor, and prove full free space.
- Continue after restore: run 100 seeded 1-to-1,024-byte allocation steps with
  50-percent random frees in a 1-MiB segment, round-trip and rebind survivors,
  then run a second 100-step phase while checking exact sizes, bounds, and
  complete live-set non-overlap after every success.
- Chained lifecycle: for ten seeded capacities in `[2 GiB + 1, 1 TiB]`, run all
  ten groups of 100 replacement iterations, round-trip once, rebind every
  survivor, release them, and prove complete free-space restoration.

Focused tests run ten consecutive rounds. Broader gates include the Master
library, allocator integration target, oplog integration target, catalog
snapshot target, all-target cargo check, rustfmt on touched Rust files, all
four manifest validators, validator unit and shell tests, touched-file
pre-commit, `git diff --check`, JSON validation, and a zero C/C++ diff audit.

## Manifest and Audit Updates

Only the five named rows change from `missing` to `covered`. Each row names its
discoverable Rust test and describes semantic state preservation without
claiming C++ bin/node byte compatibility or C++ RAII. Five remediation records
capture the first divergence, focused/full commands, and the final ten-round
evidence path. The implementation is committed first; the ledger is committed
after replacing every new `PENDING` value with the implementation SHA.
