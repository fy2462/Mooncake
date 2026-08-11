# FileStorage Offload Bucket Grouping Parity Design

## Goal

Close the four remaining C++ `FileStorageTest.GroupOffloadingKeysByBucket*`
rows with direct Rust witnesses against a production grouping boundary:

- `bucket_keys_limit`
- `bucket_size_limit`
- `bucket_size_limit_and_bucket_keys_limit`
- `ungrouped_offloading_objects`

## C++ Oracle

`BucketStorageBackend::AllocateOffloadingBuckets` delegates to the stateful
`GroupOffloadingKeysByBucket` implementation. The implementation serializes
grouping, prepends residue retained by an earlier call, skips objects that are
too large or already stored, and emits buckets bounded by both configured key
count and data-byte limits. A final incomplete group is retained rather than
emitted. An empty input leaves that residue intact.

The two 35-object tests establish the observable two-call contract: the first
call emits three ten-object buckets and retains five objects; the second call
with the same input combines the retained entries with the new task stream,
emits four ten-object buckets, and clears residue. The mixed-limit test checks
every emitted group independently. The final test establishes that small and
empty calls preserve a valid residue state.

## Rust Boundary

Add a stateful, thread-safe offload bucket planner to
`BucketStorageBackend`. The public-to-crate method mirrors the C++ allocation
boundary and returns grouped storage keys while retaining an internal map of
ungrouped `(key, data_size)` entries. A test-only residue count accessor makes
the state transition observable without exposing mutable state.

The planner uses the backend's existing `BucketStorageConfig` limits and its
record index for the already-stored check. It counts task data bytes, matching
the C++ grouping oracle; the durable Rust bucket writer continues to account
for key bytes as part of physical logical size.

The existing Rust offload loop writes objects transactionally one at a time and
lets the durable backend pack those writes. This wave does not defer or reorder
that live path. Wiring a residue planner into it would change heartbeat and
Master-notification timing beyond the four requested grouping contracts. The
new method is the direct compatibility boundary for callers that need C++
batch planning semantics.

## Algorithm and Invariants

The planner holds its residue lock for one complete grouping call.

1. If the new input is empty, return no groups and preserve residue.
2. Start each candidate group with all retained residue, then clear the
   retained map.
3. Consume new entries until the key limit, exact byte limit, or the next entry
   would exceed the byte limit.
4. Skip an entry whose size exceeds the byte limit or whose key is already in
   durable storage.
5. If input ends before a candidate is complete, restore its unique key/size
   pairs to residue and do not emit it.
6. Otherwise emit the candidate, including repeated key occurrences when a
   repeated task also appeared in the retained residue, matching the C++
   two-call behavior.

All additions use checked size accumulation. Invalid zero limits remain
rejected by the existing configuration validation.

## Tests

Add four exact unit witnesses beside the production planner:

- 35 one-byte tasks under a ten-key limit: `3 × 10 + residue 5`, followed by
  `4 × 10 + residue 0`.
- 35 one-byte tasks under a ten-byte limit with the same two-call assertions.
- 500 varied sizes under nine-key and 496-byte limits: every emitted group
  satisfies both limits.
- one task, then empty input, then seven tasks: no panic, no premature group,
  and residue remains coherent across all calls.

Tests use stable generated keys and assert counts and constraints rather than
depending on `HashMap` iteration order.

## Manifest and Ledger

After fresh focused and full-library passes, change exactly the four selected
Store rows from `missing` to `covered` and append four remediation records.
Against the independently reviewable committed parent, Store moves from
`covered=747, missing=537, not-applicable=115` to
`covered=751, missing=533, not-applicable=115`. In the accumulated checkout,
including earlier uncommitted parity work, Store moves from
`covered=1089, missing=195, not-applicable=115` to
`covered=1093, missing=191, not-applicable=115`. Wheel counts do not change.

## Verification

Run the four exact witnesses, the complete `mooncake-store-client` library
suite, scoped formatting, all four parity validators, validator contract tests,
JSON parsing, scoped pre-commit, and `git diff --check`. Commit only this wave's
production/test hunks and exact manifest/ledger records; preserve all earlier
working-tree changes.
