# Summary Window Metrics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the production Rust summary window and cumulative eviction metrics required by the C++ summary oracle.

**Architecture:** Keep Prometheus counters authoritative, serialize summary baselines in one mutex, and separate deterministic elapsed-time formatting from the production monotonic-clock wrapper. Wire Memory and NoF eviction counters at their real cycle boundaries.

**Tech Stack:** Rust, Prometheus, lazy_static, `std::time`, Tokio integration tests, JSON parity manifest.

## Global Constraints

- Follow strict RED-GREEN TDD.
- Use no correctness sleeps.
- Do not copy unrelated C++ summary sections.
- Preserve cumulative eviction counters across snapshot updates.
- Run focused low-memory tests with `--test-threads=1`.

---

### Task 1: Deterministic Summary Witness

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_metrics.rs`

**Interfaces:**
- Consumes: `metrics::summary_at(elapsed, update_snapshot)` and live counters.
- Produces: `cpp_parity_master_metrics_test_cpp_mastermetricstest_summaryuseswindowratesandcumulativeeviction`.

- [ ] Add an isolated fresh-child test using elapsed times 0, 20 ms, and 40 ms.
- [ ] Increment PutStart by 4/failures by 1, batch PutStart by 5/partial by 2, generic and Memory eviction by success 1/attempts 2/keys 3/bytes 4096, and NoF by success 1/attempts 2/keys 1/bytes 2048.
- [ ] Assert exact required strings and forbidden `/s` and `PutStart=3/4` forms.
- [ ] Run the exact test and verify RED because the summary API/counters do not exist.

### Task 2: Production Formatter and Counters

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/metrics.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/background_ops.rs`

**Interfaces:**
- Produces: `summary_at(Duration, bool) -> String` and `summary(bool) -> String`.
- Produces: cumulative `MEM_*` and `NOF_*` eviction counters.

- [ ] Add counter families and register them.
- [ ] Add counter snapshot, mutex-protected baseline, deterministic formatter, production clock wrapper, rate and byte helpers.
- [ ] Record Memory and NoF cycle attempts/results without changing eviction decisions.
- [ ] Run the exact witness, metrics test binary, and focused eviction tests.

### Task 3: Manifest, Review, and Commit

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Produces: one exact covered C++ row.

- [ ] Map only `MasterMetricsTest.SummaryUsesWindowRatesAndCumulativeEviction` to the exact Rust witness.
- [ ] Run fmt, all-target check, JSON validation, parity validation, and diff checks.
- [ ] Require independent review with no Critical/Important findings.
- [ ] Commit as `test(store): cover summary window metrics` and recount missing cases.

