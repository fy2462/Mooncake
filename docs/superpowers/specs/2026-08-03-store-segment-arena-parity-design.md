# Store Segment Arena Parity Design

## Goal

Cover the 24 applicable C++ `MmapArena` rows with a safe Rust production
boundary that owns one aligned backing allocation, reserves non-overlapping
slices concurrently, exposes monotonic allocation statistics, and proves the
backing is immediately usable. The implementation must not recreate the C++
process-global singleton or modify or format C/C++ sources.

## Scope

The batch covers initialization/capacity planning, basic and mixed-size
allocation, zero and invalid alignment rejection, checked overflow and OOM,
concurrent allocation and observation, reservation/peak telemetry, mixed
alignment ordering, physical backing, large-buffer writability, and injected
legacy hugepage fallback. The seven manifest rows already classified
`not-applicable` remain unchanged because Rust has no separately initialized
global arena, arbitrary-pointer ownership query, or public fork-advice
contract.

## Alternatives

### Recommended: one owned arena with monotonic slice capabilities

Add a focused `StoreSegmentArena` to the existing client memory boundary. It
owns a single aligned, prefaulted buffer behind `Arc`, immutable capacity and
default alignment metadata, and atomic cursor, peak, success, and failure
counters. Successful reservations return non-cloneable range owners that keep
the backing alive and provide only their disjoint mutable slice. This matches
the observable C++ allocation semantics without exposing raw free routing.

### Rejected: logical budget with independent buffers

A counter-only budget is easy to test but cannot establish address ordering,
non-overlap within one backing, or arena-wide residency. Independent allocator
addresses also make the mixed-alignment row nondeterministic.

### Rejected: process-global Rust arena

Installing a singleton into the client lifecycle would restore initialization
races and shared mutable state that Rust deliberately removed. It would expand
production behavior merely to imitate non-applicable C++ lifecycle details.

## Architecture

Keep the implementation in `memory_ffi.rs`, the existing centralized unsafe
boundary. `StoreSegmentArena::new(requested_capacity, default_alignment)`
validates a positive power-of-two alignment, raises it to at least 64 bytes,
and checked-rounds capacity to a 2-MiB unit. The backing allocation itself is
2-MiB aligned so every in-arena power-of-two alignment through 2 MiB can be
obtained by aligning offsets.

The arena stores:

- a private `Arc` backing with no whole-buffer mutable accessor after setup;
- effective capacity and default alignment;
- atomic reserved cursor, peak reservation, successful count, and failed
  count.

`allocate(size, requested_alignment)` rejects zero and non-power-of-two input,
computes the effective alignment and aligned size with checked arithmetic, and
uses a compare-exchange loop to reserve one disjoint range. Overflow and OOM
increment failure exactly once and never mutate the cursor. Zero-size and
invalid-alignment errors do not count as successful reservations. A successful
reservation increments success and monotonically advances peak.

`StoreArenaAllocation` owns `Arc<StoreArenaBacking>` plus offset and requested
length. Its safe slice access is backed by one narrowly documented unsafe
conversion: compare-exchange grants every returned owner a unique in-bounds
range, no API exposes mutable access to the complete backing, and the `Arc`
prevents deallocation while a range exists. Dropping a range does not reclaim
space, preserving arena reservation semantics.

`StoreArenaStats` is a copyable snapshot containing capacity, default
alignment, reserved, peak, successful, and failed values. Atomic loads may
observe concurrent progress at different instants, but every field maintains
its invariant: reserved never exceeds capacity, peak never decreases and is
at least each completed reservation, and success/failure account completed
allocation outcomes.

## Backing and Residency

Construction allocates the effective capacity with the existing aligned owner
and explicitly touches one byte in every system page before publishing the
arena. Linux tests query `mincore`; if the platform refuses that query, they
read one byte per page as the C++ fallback oracle does. Large writes operate
through range owners and therefore prove that published backing is immediately
readable and writable.

The existing `OwnedBuffer::allocate_for_registration` hugepage path remains
the Store segment path. Its injected non-strict HugeTLB failure test is
strengthened to retain and fully write the real fallback owner. Checked
capacity/alignment helpers are shared where useful, but this batch does not
replace normal one-shot client segment allocation with a global arena.

## Test Design

Add 24 discoverable `cpp_parity_*` unit tests named exactly by the parity
manifest. They use literal C++ matrices and concurrency scales while keeping
memory bounded: 64-KiB OOM arenas, one shared backing for concurrent requests,
1/4/8/16-MiB backing cases, and non-materializing checked rejection for
`usize::MAX` inputs. Tests retain range owners whenever address uniqueness,
ordering, independence, or writability is part of the oracle.

The TDD sequence first establishes checked planning and telemetry, then safe
range ownership, then concurrency/OOM/statistics, and finally residency and
fallback. Mutation probes must demonstrate that cursor corruption, missing
failure accounting, overlapping ranges, or skipped backing population make a
focused test fail before the implementation is accepted.

## Manifest and Verification

Only the 24 named `mmap_arena_test.cpp` rows move from `missing` to `covered`.
Each points to one exact Rust test and describes the Rust owner-capability
replacement without claiming C++ singleton lifecycle or raw-pointer
ownership. Add one remediation record per row, commit production/tests first,
replace every `PENDING` repair SHA in a separate ledger commit, and update the
external handoff totals.

Final evidence includes ten consecutive focused rounds, the complete
`mooncake-store-client` library suite, all-target cargo check, all four manifest
validators, validator unit and shell tests, rustfmt, touched-file pre-commit,
JSON/diff checks, and a zero C/C++ diff audit.
