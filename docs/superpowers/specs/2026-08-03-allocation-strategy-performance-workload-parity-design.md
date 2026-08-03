# Allocation Strategy Performance Workload Parity Design

## Context

The Store C++ parity manifest has three remaining rows from
`allocation_strategy_test.cpp`: `PerformanceComparison`, `PerformanceTest`,
and `SsdFreeRatioFirstVsRandomStrategyPerformance`. These gtests execute fixed
allocation workloads, assert that every measured allocation succeeds, and
print elapsed timing. They do not enforce a latency, speedup, or overhead
threshold.

Rust already exposes the production `SegmentAllocator` strategies needed for
an honest replacement boundary: `Random`, `FreeRatioFirst`, and
`SsdFreeRatioFirst`. The missing boundary is exact-scale workload execution,
not new allocator behavior.

## Design

Add two tests to `rust-repo/crates/mooncake-store-master/tests/test_allocator.rs`.

The first test creates a fresh production `SegmentAllocator` for each of
`Random` and `FreeRatioFirst`, mounts exactly 512 logical 64-MiB Offset
segments, and performs exactly 5,000 retained 4-MiB allocations. Every call
must return exactly one memory replica of the requested size. It records and
prints each elapsed duration without comparing them. This one test is the
shared Rust witness for both C++ `PerformanceComparison` and
`PerformanceTest`: it contains the complete Random workload required by the
latter and both workloads required by the former, without paying for the
same 512-by-5,000 Random workload twice.

The second test creates exactly 64 logical 8-MiB Offset segments with
deterministic owner UUIDs and SSD metrics matching the C++ gradient. For both
`Random` and `SsdFreeRatioFirst`, it executes exactly 200 warmup allocations,
releasing each warmup allocation immediately, followed by exactly 2,000
retained 128-KiB measured allocations. Every measured call must return exactly
one memory replica of the requested size. The Random measured allocations are
released before starting the SSD-aware workload so both strategies begin from
equivalent free-memory state. The test records and prints per-strategy elapsed
durations and derived per-operation values, but imposes no performance ratio.

No production code is planned. If an exact workload exposes a production
failure, the failure must first remain captured by the test and then receive
the smallest production fix through a separate red-green cycle.

## Manifest and Evidence

Mark exactly the three named rows covered. The first two rows reference the
shared Random/FreeRatioFirst test; the SSD row references the SSD-aware test.
Each reason states the exact segment, warmup, measured-allocation, and size
counts and explicitly says that timing is observational rather than a
cross-language performance claim.

Append exactly three remediation records after the implementation commit is
known. Each record uses an exact Rust test filter, the full
`mooncake-store-master --test test_allocator` gate, and one retained transcript
containing ten rounds of both exact tests. No record uses a broad prefix that
could select future tests.

## Verification

Before completion:

1. Mutation-check the new tests by temporarily reducing a measured loop bound;
   an independent literal completion-count assertion must fail, then the
   production-scale bound is restored and passes.
2. Run both exact tests for ten rounds and audit 20 one-test passes, zero
   failures, zero zero-test runs, and command exit zero.
3. Run the full allocator integration target, the Store Master library suite,
   and `cargo check -p mooncake-store-master --all-targets`.
4. Run all four parity validators, 44 validator self-tests, both shell contract
   tests, Rust 2024 formatting, relevant pre-commit hooks, JSON parsing, and
   `git diff --check`.
5. Obtain an independent review of the C++-to-Rust semantic mapping and exact
   workload counts.
6. Verify the worktree is clean after commits and that C/C++ changes from
   baseline `186bd256..HEAD` remain zero.

## Non-Goals

- No latency, speedup, or overhead threshold.
- No claim that Rust and C++ allocator timings are directly comparable.
- No Criterion or other benchmark framework.
- No C/C++ source or formatting changes.
- No hardware-only, CXL, Sunrise, NUMA, Kubernetes, or shared-memory coverage.
