# Offset Allocator Semantic Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a strict, versioned per-segment Offset allocator snapshot and use it to cover the five remaining C++ OffsetAllocator serialization semantics.

**Architecture:** A new `allocator/offset_snapshot.rs` module owns a deterministic semantic payload and strict length/checksum envelope. `SegmentAllocator` serializes one ordinary Offset segment and restores it through the existing descriptor-driven atomic restore path, returning validated descriptors as rebound live handles.

**Tech Stack:** Rust 2024, serde/rmp-serde, xxhash-rust xxh64, existing `SegmentAllocator` and `ReplicaDescriptor` APIs, Python parity validator.

## Global Constraints

- Work only in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- Treat all four JSON manifests under `rust-repo/tools/store-validation/` as authoritative.
- Do not modify or format C/C++ files.
- Cover exactly the five named serialization rows without claiming C++ private bin/node wire compatibility or C++ RAII.
- Keep Cachelib, CXL, and whole-facade snapshots outside this batch.
- Write every production behavior through a witnessed RED-to-GREEN test cycle.
- Commit implementation before replacing new remediation `PENDING` values; commit the ledger separately.

---

### Task 1: Strict empty and one-allocation semantic snapshots

**Files:**
- Create: `rust-repo/crates/mooncake-store-master/src/allocator/offset_snapshot.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/allocator/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/allocator/offset_layout.rs`

**Interfaces:**
- Consumes: existing private `SegmentAllocator.segments`, `SegmentState`, `SegmentLayout::Offset`, `AllocatorSnapshotConfig`, and `restore_segment`.
- Produces: `SegmentAllocator::serialize_offset_segment(&Uuid) -> Result<Vec<u8>, String>` and `SegmentAllocator::restore_offset_segment_snapshot(&[u8]) -> Result<Vec<ReplicaDescriptor>, String>`.

- [ ] **Step 1: Add the empty-snapshot RED test**

Add `cpp_parity_empty_allocator_snapshot_short_long_not_equal` to the existing
`allocator::offset_layout::tests` module. Construct a valid Offset segment with
base `16 * 1024`, capacity `MIB`, and maximum nodes `Some(10_000)`. Call the new
serialization method, restore into a fresh identically configured allocator,
and require deterministic reserialization and identical reports. Remove the
last byte and append one zero byte in separate fresh allocators; both restores
must return `Err` and leave the segment absent. A second clean restore must
succeed.

- [ ] **Step 2: Run the empty test and verify RED**

Run:

```bash
cd rust-repo
cargo test -p mooncake-store-master --lib cpp_parity_empty_allocator_snapshot_short_long_not_equal -- --nocapture
```

Expected: compile failure because `serialize_offset_segment` and
`restore_offset_segment_snapshot` do not exist.

- [ ] **Step 3: Add the strict snapshot module and minimal empty-state implementation**

Register `mod offset_snapshot;` in `allocator/mod.rs`. In the new module define
private serde payloads with these exact fields:

```rust
#[derive(Serialize, Deserialize)]
struct OffsetSegmentSnapshotV1 {
    schema_version: u32,
    allocator_config: AllocatorSnapshotConfig,
    segment: Segment,
    client_id: Uuid,
    used: u64,
    allocations: Vec<OffsetSnapshotRange>,
    free_ranges: Vec<OffsetSnapshotRange>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct OffsetSnapshotRange {
    offset: u64,
    size: u64,
}
```

Use an eight-byte magic, envelope version `1_u32`, little-endian `u64` payload
length, little-endian xxHash64 checksum, and named-field MessagePack. Decode
with checked header arithmetic and require `encoded.len() == header_len +
payload_len`. Reject unknown versions, invalid checksum, unknown payload schema,
missing/Cachelib/CXL/unbound segments, mismatched receiving configuration,
duplicate UUIDs, zero/overlapping/out-of-bounds ranges, noncanonical ordering,
non-complementary free ranges, incorrect `used`, and a partition count above
the configured node maximum before calling `restore_segment`.

Build live descriptors with exact segment UUID/name/base/protocol/client,
offset, and size. Use `ReplicaStatus::Complete`, `ReplicaType::Memory`,
`handle_valid = true`, and absent local-disk identities. If a post-install
report or deterministic-reserialization check fails, remove the segment before
returning `Err`.

- [ ] **Step 4: Run the empty test and verify GREEN**

Run the Step 2 command. Expected: one test passes and both malformed inputs
leave the destination allocator unchanged.

- [ ] **Step 5: Add the one-live-allocation RED test**

Add `cpp_parity_one_live_allocation_snapshot_rebinds_and_frees`. Allocate one
exact 1,024-byte descriptor, snapshot, clean-restore and compare bytes/reports,
reject short and long inputs atomically, restore clean bytes a second time,
require exactly one rebound descriptor with the literal offset and size, release
it through `SegmentAllocator::release`, and assert zero allocated bytes/count and
exact `MIB` total/largest free space.

- [ ] **Step 6: Run the one-allocation test and verify RED**

Temporarily make `restore_offset_segment_snapshot` return an empty descriptor
vector after successful installation, run:

```bash
cargo test -p mooncake-store-master --lib cpp_parity_one_live_allocation_snapshot_rebinds_and_frees -- --nocapture
```

Expected: assertion failure because the restored live descriptor is absent.

- [ ] **Step 7: Return validated rebound descriptors and verify GREEN**

Return the sorted descriptors used by `restore_segment`. Restore the real
implementation removed for the RED probe, run both new test filters, then run:

```bash
cargo test -p mooncake-store-master --lib allocator::offset_layout::tests -- --nocapture
```

Expected: all Offset tests pass.

---

### Task 2: Random large snapshot matrix

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/allocator/offset_layout.rs`

**Interfaces:**
- Consumes: Task 1 snapshot methods and the existing seeded random lifecycle helpers.
- Produces: `cpp_parity_100_random_huge_snapshots_preserve_live_state`.

- [ ] **Step 1: Add the 100-capacity RED test**

Use a fixed `StdRng` seed and literal inclusive capacity range
`(1_u64 << 31) + 1..=1_u64 << 40`. For each of exactly 100 capacities, execute
exactly 200 optional-add/random-remove/mandatory-same-size replacement steps
with request range `1..=capacity / 100`. Validate every successful request's
exact size and the complete live set's bounds/non-overlap. Snapshot and clean
restore, require deterministic bytes and equal report, reject one-byte-short and
one-byte-long buffers in fresh allocators, restore clean bytes again, release
every rebound descriptor, and require full free capacity.

- [ ] **Step 2: Run the test and verify RED**

Add a temporary test-only mutation that swaps the first two encoded live ranges
before encoding one nonempty case. Run:

```bash
cargo test -p mooncake-store-master --lib cpp_parity_100_random_huge_snapshots_preserve_live_state -- --nocapture
```

Expected: restore fails on noncanonical range ordering.

- [ ] **Step 3: Remove the mutation and verify GREEN**

Restore sorted canonical encoding, rerun the focused test, and require exactly
100 complete round trips with no ignored or conditional cases.

---

### Task 3: Continue allocation after restore and chained lifecycle

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/allocator/offset_layout.rs`

**Interfaces:**
- Consumes: Task 1 rebound descriptors and Task 2 live-set helpers.
- Produces: `cpp_parity_random_allocation_continues_after_snapshot_restore` and `cpp_parity_ten_huge_allocators_survive_1000_replacements_and_rebind`.

- [ ] **Step 1: Add the continue-after-restore RED test**

Use base `16 * 1024`, capacity `MIB`, maximum nodes `Some(10_000)`, and one fixed
seed. Execute 100 iterations of an exact `1..=1_024` allocation followed by a
50-percent random free; every allocation must succeed with exact size and the
complete live set must remain bounded and non-overlapping. Snapshot, restore,
replace the old descriptor vector with the returned rebound vector, and execute
a second distinct seeded 100-iteration phase with the same assertions.

- [ ] **Step 2: Run the test and verify RED**

Temporarily mark restored segments unbound immediately after successful
installation. Run:

```bash
cargo test -p mooncake-store-master --lib cpp_parity_random_allocation_continues_after_snapshot_restore -- --nocapture
```

Expected: the first post-restore allocation returns `NoAvailableHandle`.

- [ ] **Step 3: Preserve the serialized bound state and verify GREEN**

Remove the mutation and keep ordinary snapshot restores bound to the serialized
runtime identity. Rerun the focused test; both 100-step phases must pass.

- [ ] **Step 4: Add the chained-lifecycle RED test**

Use a second fixed seed. For each of exactly ten capacities in
`(1_u64 << 31) + 1..=1_u64 << 40`, run ten groups of 100 optional-add,
random-remove, and mandatory-same-size replacement iterations. Check exact
sizes and the complete live set after every success. Snapshot once after all
1,000 iterations, restore, require deterministic bytes/equal report, rebind all
survivors, release them, and require full free capacity.

- [ ] **Step 5: Run the chained test and verify RED**

Temporarily omit the last live range from the returned descriptor vector and
run:

```bash
cargo test -p mooncake-store-master --lib cpp_parity_ten_huge_allocators_survive_1000_replacements_and_rebind -- --nocapture
```

Expected: the final report retains one live allocation and is not fully free.

- [ ] **Step 6: Restore the complete vector and verify GREEN**

Remove the mutation and run all five new `cpp_parity_*snapshot*` or exact-name
filters plus the complete Offset test module.

---

### Task 4: Manifest, evidence, review, and commits

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Update after commits: `/home/fy2462/Mooncake/.git/worktrees/ha-chaos-live/codex-parity-handoff.md`

**Interfaces:**
- Consumes: all five discoverable tests and final verification output.
- Produces: five covered rows, five auditable remediation records, clean implementation and ledger commits, updated handoff totals.

- [ ] **Step 1: Update only the five serialization rows**

Set each row to `covered`, point it at its exact Rust test, and describe the
semantic snapshot/rebinding boundary. Explicitly disclaim C++ bin/node wire
compatibility and C++ RAII. Append five remediation rows with
`repair_commit: "PENDING"` and one final ten-round evidence path.

- [ ] **Step 2: Run focused tests ten consecutive rounds**

Capture a `script` transcript that runs the complete Offset test module plus
the exact snapshot tests ten times. Audit the transcript with anchored regular
expressions for exactly ten run markers and zero failed tests.

- [ ] **Step 3: Run all relevant gates**

Run:

```bash
cargo test -p mooncake-store-master --lib
cargo test -p mooncake-store-master --test test_allocator
cargo test -p mooncake-store-master --test test_oplog
cargo test -p mooncake-store-master --test test_catalog_snapshot
cargo check -p mooncake-store-master --all-targets
```

Then run standalone rustfmt check on touched Rust files, all four parity
validators, the 44 Python validator unit tests, both shell selftests, touched
pre-commit with `SKIP=mooncake-code-format,codespell`, JSON validation,
`git diff --check`, and an empty C/C++ diff audit.

- [ ] **Step 4: Request independent review and repair findings**

Ask a read-only reviewer to inspect the dirty diff against the five manifest
requirements, envelope validation, atomicity, deterministic encoding, safe
descriptor rebinding, and claim accuracy. Resolve every Critical or Important
finding through a fresh RED-to-GREEN cycle and rerun affected gates.

- [ ] **Step 5: Commit implementation and ledger**

Stage all production/tests/parity-map changes except remediation-log and commit
with `[Store] snapshot offset allocator segments`. Replace all five new
`PENDING` values with that implementation SHA, validate there are zero pending
records, and commit the ledger with `[Store] record offset allocator snapshots`.

- [ ] **Step 6: Update handoff and continue the full goal**

Record the two SHAs, exact final gate counts, evidence path, Store covered/missing
totals, aggregate remaining total, and next coherent missing batch. Verify a
clean worktree and zero C/C++ diff before selecting the next batch.
