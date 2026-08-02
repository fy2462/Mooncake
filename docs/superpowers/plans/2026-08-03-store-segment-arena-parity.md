# Store Segment Arena Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a safe concurrent Rust Store segment arena and close the 24 applicable C++ `MmapArena` parity rows.

**Architecture:** `memory_ffi.rs` gains one private 2-MiB-aligned prefaulted backing, an `Arc`-shared arena with checked atomic monotonic reservations and statistics, and non-cloneable disjoint range owners. Existing direct registration and hugepage fallback paths remain production surfaces and receive exact boundary tests where the manifest calls for them.

**Tech Stack:** Rust 2024, std atomics/Arc/Layout, Linux `mincore`, existing `OwnedBuffer` hugepage injection hooks, Python parity validators.

## Global Constraints

- Work only in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- Treat all four manifests under `rust-repo/tools/store-validation/` as authoritative.
- Do not modify or format C/C++ files.
- Keep the seven current `MmapArena` `not-applicable` rows unchanged.
- Every production behavior requires a witnessed RED-to-GREEN cycle and at least one mutation probe for its invariant group.
- Commit implementation before replacing remediation `PENDING` values; commit the ledger separately.

---

### Task 1: Checked policy, snapshot, and basic allocation

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

- [ ] Add RED tests `cpp_parity_one_mib_segment_budget_starts_as_two_mib_with_zero_counters`, `cpp_parity_one_kib_allocation_updates_count_and_reservation`, `cpp_parity_registered_zero_size_is_rejected_without_success_count`, `cpp_parity_usize_max_rejects_once_without_reservation`, `cpp_parity_usize_max_minus_ten_alignment_overflow_is_nonmutating`, and `cpp_parity_half_usize_max_exceeds_budget_and_counts_once`.
- [ ] Run each exact filter and record compile or assertion failure before implementation.
- [ ] Add `StoreSegmentArena`, `StoreArenaStats`, `StoreArenaBacking`, `StoreArenaAllocation`, checked 2-MiB capacity rounding, alignment validation, and checked reservation planning.
- [ ] Implement compare-exchange reservation with non-mutating failure and monotonic peak/success/failure accounting.
- [ ] Return safe range owners whose requested length excludes padding while reservation statistics include padding.
- [ ] Mutation probe: temporarily advance the cursor before the bounds check; require the OOM/overflow focused tests to fail, then restore the correct code.
- [ ] Run all six tests and the existing `memory_ffi::tests` module GREEN.

### Task 2: Alignment, mixed sizes, and address invariants

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

- [ ] Add RED tests `cpp_parity_exact_six_size_matrix_is_64_aligned_and_fully_writable`, `cpp_parity_eighty_mixed_store_allocation_attempts_validate_every_success`, `cpp_parity_one_byte_then_two_mib_aligned_reservation_orders_addresses`, `cpp_parity_registered_allocation_rejects_alignment_100`, and `cpp_parity_exact_power_of_two_alignment_matrix_is_accepted`.
- [ ] Implement requested-alignment offset planning against a backing aligned to at least 2 MiB and at least the configured default, and complete `Deref`/`DerefMut` range access.
- [ ] Retain all owners while checking literal size/alignment matrices, full writes, base/length/content identity, checked range ends, ordering, and non-overlap.
- [ ] Mutation probe: ignore the requested alignment and require the 2-MiB mixed-alignment test to fail, then restore it.
- [ ] Run the five exact tests and the full module GREEN.

### Task 3: OOM, concurrency, and statistics

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

- [ ] Add RED tests `cpp_parity_repeated_oom_never_advances_reserved_past_capacity`, `cpp_parity_sixteen_thread_oom_accounts_every_attempt`, `cpp_parity_ten_concurrent_store_allocations_return_unique_addresses`, `cpp_parity_eight_threads_attempt_8000_validate_every_success`, `cpp_parity_stats_samples_remain_bounded_during_eight_thread_load`, `cpp_parity_sixty_four_byte_fill_stops_with_bounded_reservation`, `cpp_parity_peak_tracks_512_then_1024_reservations`, `cpp_parity_exact_six_request_reservation_is_monotonic`, `cpp_parity_exact_sixteen_mib_fit_then_one_kib_oom`, and `cpp_parity_concurrent_policy_readers_preserve_four_mib_and_128_alignment`.
- [ ] Use barriers and the literal C++ thread/attempt counts; retain successful owners where uniqueness and writability require lifetime proof.
- [ ] Ensure each completed attempt increments exactly one outcome counter, reserved remains bounded, and immutable capacity/alignment metadata is identical for all readers.
- [ ] Mutation probe: suppress failure increments and require the exact accounting test to fail; restore it and rerun.
- [ ] Run all ten exact tests and the complete client library suite GREEN.

### Task 4: Backing residency, large writes, and fallback

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

- [ ] Add RED tests `cpp_parity_linux_four_mib_segment_is_resident_or_page_readable`, `cpp_parity_first_four_mib_segment_and_optional_second_are_immediately_writable`, and `cpp_parity_injected_legacy_fallback_is_fully_writable_at_three_points`.
- [ ] Prefault every system page before arena publication and implement a test-only Linux residency query using `mincore` with the specified readable fallback.
- [ ] Fully fill and page-sample the large arena ranges, retaining both owners and proving disjointness/independence when the optional second succeeds.
- [ ] Reuse the existing injected non-strict HugeTLB failure hook, retain its real regular fallback buffer, fill 1 MiB with `0xef`, and verify first/middle/last.
- [ ] Mutation probe: skip the prefault touch under tests and require the residency test to fail on the Linux gate, then restore it.
- [ ] Run the three exact tests, the full module, and the complete client library suite GREEN.

### Task 5: Manifest, evidence, review, and commits

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Update after commits: `/home/fy2462/Mooncake/.git/worktrees/ha-chaos-live/codex-parity-handoff.md`

- [ ] Self-review production/test diff for soundness, exact oracle scope, test discoverability, and zero C/C++ changes.
- [ ] Commit production and tests with `[Store]` prefix.
- [ ] Change only the 24 named rows from `missing` to `covered`, attach their exact Rust tests, and append 24 remediation records with the implementation SHA.
- [ ] Run ten consecutive focused rounds into `/tmp/mooncake-mmap-arena-parity-final.typescript`; audit exact round/pass markers and zero failed/zero-test runs.
- [ ] Run the complete client library suite, all-target cargo check, all four manifest validators, validator unit and shell tests, rustfmt, touched-file pre-commit, JSON validation, `git diff --check`, and a zero C/C++ diff audit.
- [ ] Request code review, address every Critical/Important/Minor finding through RED-to-GREEN evidence, and rerun affected plus final gates.
- [ ] Commit the ledger separately, update the external handoff with exact SHAs/evidence/counts, audit the handoff, and report the new aggregate covered/missing totals.
