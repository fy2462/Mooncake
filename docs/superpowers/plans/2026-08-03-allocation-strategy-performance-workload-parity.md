# Allocation Strategy Performance Workload Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cover the three remaining `allocation_strategy_test.cpp` performance-workload parity rows with exact-scale production Rust `SegmentAllocator` tests, without claiming a timing threshold or modifying C/C++.

**Architecture:** Add test-only workload helpers and two integration tests to the existing Store Master allocator target. One shared test executes the exact 512-segment/5,000-allocation Random and FreeRatioFirst workloads; one test executes the exact SSD-aware 64-segment, 200-warmup, 2,000-measured workloads for Random and SsdFreeRatioFirst. Production allocator code remains unchanged unless the red-green cycle exposes a real failure.

**Tech Stack:** Rust 2024, `mooncake-store-master::allocator::SegmentAllocator`, Store parity JSON validators, Python from `/home/fy2462/Mooncake/.venv`, git, pre-commit.

## Global Constraints

- Treat the four manifests in `rust-repo/tools/store-validation` as authoritative.
- Preserve the exact C++ workload counts and allocation sizes.
- Timing is observational only; do not assert latency, speedup, or overhead ratios.
- Do not modify or format C/C++ files.
- Keep `ShmMappingEstablished`, hardware-only, topology-dependent, Kubernetes, CXL, Sunrise, and unrelated rows unchanged.
- Maintain zero C/C++ changes across baseline `186bd256..HEAD`.

---

### Task 1: Exact production allocator workload witnesses

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_allocator.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_allocator.rs`

**Interfaces:**
- Consumes: `SegmentAllocator::new`, `with_strategy`, `add_segment`, `allocate_checked`, `allocate_for_client_with_ssd_metrics`, `release`, `AllocationStrategy`, `SsdUsageMetrics`, `ReplicateConfig`, and the existing `make_seg` helper.
- Produces: `cpp_parity_allocation_strategy_random_and_free_ratio_complete_512_by_5000_workloads` and `cpp_parity_allocation_strategy_ssd_and_random_complete_exact_warmup_and_measured_workloads`.

- [ ] **Step 1: Add the exact-scale Random/FreeRatioFirst test**

Add a test-only helper that constructs a fresh Offset-backed allocator with a chosen strategy and exact logical segment geometry:

```rust
fn allocation_workload_allocator(
    strategy: AllocationStrategy,
    prefix: &str,
    segment_count: usize,
    segment_size: u64,
) -> (SegmentAllocator, Vec<Uuid>) {
    let mut allocator = SegmentAllocator::new().with_strategy(strategy);
    let mut owners = Vec::with_capacity(segment_count);
    for index in 0..segment_count {
        let owner = Uuid::from_u128(index as u128 + 1);
        allocator.add_segment(
            make_seg(&format!("{prefix}-{index}:1"), segment_size),
            0,
            owner,
        );
        owners.push(owner);
    }
    (allocator, owners)
}
```

Add `cpp_parity_allocation_strategy_random_and_free_ratio_complete_512_by_5000_workloads`. For each of `AllocationStrategy::Random` and `AllocationStrategy::FreeRatioFirst`, create a fresh allocator with 512 segments of 64 MiB, record `Instant::now()`, execute exactly 5,000 retained allocations of 4 MiB using unique literal-derived keys, and assert every result has one memory descriptor with exact size. Retain every returned vector until the strategy workload ends, increment `completed` after each successful allocation, assert `completed == 5_000`, and print strategy plus elapsed microseconds. Do not compare the two durations.

- [ ] **Step 2: Mutation-check the exact 5,000 count (RED)**

Temporarily change only the loop bound from `0..5_000` to `0..4_999`, leaving `assert_eq!(completed, 5_000)` unchanged.

Run:

```bash
cargo test -p mooncake-store-master --test test_allocator cpp_parity_allocation_strategy_random_and_free_ratio_complete_512_by_5000_workloads -- --exact --nocapture
```

Expected: FAIL with `left: 4999`, `right: 5000`. Restore the loop bound to `0..5_000` using `apply_patch`.

- [ ] **Step 3: Add the exact SSD-aware test**

Add `cpp_parity_allocation_strategy_ssd_and_random_complete_exact_warmup_and_measured_workloads`. For each of `Random` and `SsdFreeRatioFirst`, create a fresh allocator with exactly 64 segments of 8 MiB and use the returned owner UUIDs to construct literal SSD metrics: total 1,000 MiB and used `(100 + index * 800 / 64)` MiB. Execute exactly 200 warmup allocations of 128 KiB through `allocate_for_client_with_ssd_metrics`; assert one exact memory replica and release it immediately. Then record `Instant::now()`, execute exactly 2,000 retained allocations through the same production method, assert one exact memory replica each, count completions, and print elapsed and per-operation microseconds. Assert literal counts `warmup_completed == 200` and `measured_completed == 2_000`; do not compare durations.

- [ ] **Step 4: Mutation-check the SSD measured count (RED)**

Temporarily change only the measured loop bound from `0..2_000` to `0..1_999` while keeping the literal completion assertion.

Run:

```bash
cargo test -p mooncake-store-master --test test_allocator cpp_parity_allocation_strategy_ssd_and_random_complete_exact_warmup_and_measured_workloads -- --exact --nocapture
```

Expected: FAIL with `left: 1999`, `right: 2000`. Restore `0..2_000` using `apply_patch`.

- [ ] **Step 5: Verify both tests GREEN and format**

Run each exact test independently with `--exact --nocapture`, then run:

```bash
rustfmt --edition 2024 --check rust-repo/crates/mooncake-store-master/tests/test_allocator.rs
git diff --check
```

Expected: both exact tests pass; formatting and diff checks exit zero.

- [ ] **Step 6: Commit the implementation witness**

```bash
git add rust-repo/crates/mooncake-store-master/tests/test_allocator.rs
git commit -m '[Store] cover allocation strategy workloads'
```

Record the resulting implementation SHA for all three remediation records.

---

### Task 2: Manifest mapping, exact evidence, and remediation ledger

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: the two exact Rust test names and implementation SHA from Task 1.
- Produces: exactly three `covered` rows, exactly three remediation records, and `/tmp/mooncake-allocation-strategy-performance-workloads.typescript`.

- [ ] **Step 1: Update exactly three manifest rows**

Change only these references from `missing` to `covered`:

1. `AllocationStrategyTest.PerformanceComparison`
2. `AllocationStrategyTest.PerformanceTest`
3. `AllocationStrategyTest.SsdFreeRatioFirstVsRandomStrategyPerformance`

Map the first two to `cpp_parity_allocation_strategy_random_and_free_ratio_complete_512_by_5000_workloads`. Map the third to `cpp_parity_allocation_strategy_ssd_and_random_complete_exact_warmup_and_measured_workloads`. State exact geometry and counts, and explicitly state that elapsed values are recorded without a performance threshold or cross-language comparison.

- [ ] **Step 2: Generate ten-round exact evidence**

Use `script -q -e` to run both exact tests independently for ten rounds and retain the transcript at `/tmp/mooncake-allocation-strategy-performance-workloads.typescript`.

Audit the transcript with anchored round markers and exact result summaries. Expected evidence: 10 rounds, 20 occurrences of `test result: ok. 1 passed; 0 failed`, no `FAILED`, no `0 passed; 0 failed`, and `COMMAND_EXIT_CODE="0"`.

- [ ] **Step 3: Append exactly three remediation records**

Each record must contain the exact C++ reference, exact Rust test, first divergence, Task 1 implementation SHA, an exact `--exact --nocapture` focused command, the full allocator target command, and the new transcript path. Do not use a shared prefix filter.

- [ ] **Step 4: Validate and commit the ledger**

Run JSON parsing and all four validators before committing:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m json.tool rust-repo/tools/store-validation/parity-map.json >/dev/null
/home/fy2462/Mooncake/.venv/bin/python -m json.tool rust-repo/tools/store-validation/remediation-log.json >/dev/null
```

Then commit:

```bash
git add rust-repo/tools/store-validation/parity-map.json rust-repo/tools/store-validation/remediation-log.json
git commit -m '[Store] record allocation workload parity'
```

---

### Task 3: Broad gates, independent review, and handoff

**Files:**
- Modify outside repository: `.git/worktrees/ha-chaos-live/codex-parity-handoff.md`
- Verify only: all files changed since Task 1

**Interfaces:**
- Consumes: committed implementation, manifest, ledger, and retained evidence from Tasks 1-2.
- Produces: independently reviewed, fully gated clean HEAD with current handoff.

- [ ] **Step 1: Run broad Rust gates**

Run:

```bash
cargo test -p mooncake-store-master --test test_allocator
cargo test -p mooncake-store-master --lib
cargo check -p mooncake-store-master --all-targets
```

Expected: every command exits zero. Record exact passed/failed counts from the fresh output.

- [ ] **Step 2: Run validation toolchain gates**

Run all four manifests through `validate_parity.py`, then run the 44 Python validator self-tests and both shell contracts. Run Rust 2024 formatting on the exact Rust test file and pre-commit on only the design, plan, Rust test, parity map, and remediation log with `mooncake-code-format,codespell` skipped.

- [ ] **Step 3: Request independent review**

Ask the reviewer to compare all three C++ tests against the two Rust witnesses, verify exact scale, warmup/release/retention semantics, timing non-claims, shared-test honesty, manifest wording, evidence reproducibility, remediation SHA correctness, and C/C++ zero changes. Resolve every Critical or Important finding and re-run affected gates.

- [ ] **Step 4: Update handoff and perform final audit**

Update the external handoff HEAD, exact manifest counts, latest completed batch, gate counts, review result, and transcript path. Verify:

```bash
git status --porcelain=v1
git diff --name-only 186bd256..HEAD -- '*.c' '*.cc' '*.cpp' '*.cxx' '*.h' '*.hh' '*.hpp' '*.hxx'
git diff --check
```

Expected: clean worktree, no C/C++ paths, no whitespace errors, three exact covered rows, three exact remediation records using the implementation SHA, `PENDING=0`, and handoff HEAD equal to repository HEAD.

- [ ] **Step 5: Close the active goal**

Only after every requirement and gate above has current evidence, mark the active goal complete and report the three newly covered rows plus updated global remaining count.
