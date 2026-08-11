# NUMA HugeTLB Population Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Linux NUMA-region HugeTLB population and add a discoverable Rust witness for `PopulateNumaHugetlbMappingTouchesEveryRegion`.

**Architecture:** Separate deterministic CPU-list/region planning from Linux affinity and unsafe memory touching. Region workers make a best-effort node-affinity attempt, then always populate every assigned page.

**Tech Stack:** Rust 2024, standard library scoped threads, existing `libc` crate, Linux sysfs, JSON parity validation.

## Global Constraints

- Do not add a `libnuma` dependency.
- Invalid pointer/layout inputs fail before any memory write.
- Affinity lookup or binding failure warns but never suppresses page population.
- Non-Linux builds keep compiling through target gating.
- Preserve unrelated working-tree edits and stage only intended files or exact JSON hunks.

---

### Task 1: Add Compile-Red Planner and Population Tests

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

**Interfaces:**
- Consumes: existing `populate_hugetlb_pages` conventions and `HUGEPAGE_2_MIB`.
- Produces: compile-time requirements for `parse_linux_id_list`, `numa_page_ranges`, `online_numa_nodes`, and `populate_hugetlb_numa_pages`.

- [ ] **Step 1: Add pure CPU/node-list parser tests**

Assert:

```rust
assert_eq!(parse_linux_id_list("0-3,8,10-11\n").unwrap(), vec![0,1,2,3,8,10,11]);
assert!(parse_linux_id_list("").is_err());
assert!(parse_linux_id_list("3-1").is_err());
assert!(parse_linux_id_list("1,,2").is_err());
```

- [ ] **Step 2: Add pure region-plan test**

For eight pages, nodes `[0, 2]`, and six available workers, assert every returned range stays inside its four-page node region and flattened pages equal `0..8` exactly once. Also reject zero inputs and page counts not divisible by nodes.

- [ ] **Step 3: Add the exact parity witness**

Add `cpp_parity_numa_hugetlb_population_touches_every_region` under Linux. Read online nodes, take two, and explicitly skip with `eprintln!` if unavailable/empty. Allocate `2 * node_count * HUGEPAGE_2_MIB` bytes filled with `0xCD`, overwrite every hugepage boundary with `0xAB`, invoke the absent population helper, then assert all boundaries are zero and boundary+1 remains `0xCD`.

- [ ] **Step 4: Run exact test and capture compile red**

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib \
memory_ffi::tests::cpp_parity_numa_hugetlb_population_touches_every_region \
-- --exact --nocapture
```

Expected: compilation fails because the parser/planner/population helpers do not exist.

### Task 2: Implement Parsing and Region Planning

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

**Interfaces:**
- Produces: `parse_linux_id_list(&str) -> io::Result<Vec<usize>>` and `numa_page_ranges(usize, &[usize], usize) -> io::Result<Vec<(usize, Range<usize>)>>`.

- [ ] **Step 1: Implement strict Linux list parsing**

Trim once, reject empty input and empty comma components. Parse each component as either one decimal ID or an inclusive ascending `start-end` range. Use checked range expansion, sort, deduplicate, and reject malformed or descending ranges with `InvalidData`.

- [ ] **Step 2: Implement C++-equivalent worker distribution**

Validate non-empty/divisible inputs. Compute:

```rust
let worker_count = numa_nodes
    .len()
    .max(available_workers.min(16).min(page_count));
```

Distribute workers with quotient/remainder, divide each equal node region using ceiling pages per worker, and omit empty ranges. Return node IDs rather than node indexes.

- [ ] **Step 3: Run the two pure tests**

Run each parser/planner test exactly. Expected: both pass without native linking or NUMA hardware assumptions.

### Task 3: Implement Linux Affinity and Population

**Files:**
- Modify and test: `rust-repo/crates/mooncake-store-client/src/memory_ffi.rs`

**Interfaces:**
- Consumes: parser and region planner.
- Produces: `online_numa_nodes() -> io::Result<Vec<usize>>`, best-effort `bind_current_thread_to_numa_node(usize)`, and unsafe `populate_hugetlb_numa_pages(*mut u8, usize, usize, &[usize]) -> io::Result<()>` under Linux.

- [ ] **Step 1: Add sysfs node/CPU discovery**

Read `/sys/devices/system/node/online` through `parse_linux_id_list`. For one node's affinity, read `/sys/devices/system/node/node{node}/cpulist`, parse IDs, reject an empty result, and ignore CPUs greater than or equal to `libc::CPU_SETSIZE as usize` with a warning.

- [ ] **Step 2: Add best-effort pthread affinity**

Zero a `libc::cpu_set_t`, add CPUs with `libc::CPU_SET`, then call:

```rust
libc::pthread_setaffinity_np(
    libc::pthread_self(),
    std::mem::size_of::<libc::cpu_set_t>(),
    &set,
)
```

Convert nonzero return codes to `io::Error::from_raw_os_error`. The population worker catches and warns on this result, then continues.

- [ ] **Step 3: Add layout validation and scoped workers**

Reject null pointers, zero sizes, empty nodes, `len % nodes != 0`, or `(len / nodes) % page_size != 0`. Obtain available parallelism, call `numa_page_ranges`, spawn one scoped thread per plan entry, attempt affinity, and volatile-write zero at each assigned page boundary. Convert thread panic to `io::Error::other`.

- [ ] **Step 4: Run the exact parity witness**

Run the Task 1 exact command. Expected: `1 passed`, or a documented test-level skip only when online NUMA sysfs is unavailable/empty. On ordinary Linux with node0 it must execute and pass.

- [ ] **Step 5: Run related memory tests and full client lib**

Run the parser/planner test names, `population_`, `page_ranges_`, and the exact parity witness, then:

```bash
LD_LIBRARY_PATH=/home/fy2462/Mooncake/build/mooncake-transfer-engine/src:/home/fy2462/Mooncake/build/mooncake-common/src \
cargo test -p mooncake-store-client --features link-native --lib
```

Expected full client count: 390 tests if the parser and planner are separate new tests, with zero failures.

- [ ] **Step 6: Run formatting and commit code**

Run `rustfmt --edition 2024 --check` and `git diff --check` on `memory_ffi.rs`. Stage only that file and commit:

```bash
git commit -m "feat(store): populate HugeTLB pages by NUMA region"
```

### Task 4: Publish Parity Evidence

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`

**Interfaces:**
- Consumes: code repair SHA and exact/full verification outputs.
- Produces: one missing-to-covered transition and one remediation record.

- [ ] **Step 1: Update the exact manifest row**

Reference `mooncake-store-client/src/memory_ffi.rs:cpp_parity_numa_hugetlb_population_touches_every_region`. Explain equal node regions, real sysfs node selection/affinity attempt, page-boundary writes, and best-effort binding semantics.

- [ ] **Step 2: Append remediation evidence**

Record compile-red cause, repair SHA, exact and full commands/counts, actual execution versus skip result, parser/planner coverage, and formatting result.

- [ ] **Step 3: Run all validators**

```bash
python3 rust-repo/tools/store-validation/validate_parity.py --repo-root . \
  --manifest rust-repo/tools/store-validation/parity-map.json \
  --manifest rust-repo/tools/store-validation/tent-parity-map.json \
  --manifest rust-repo/tools/store-validation/transfer-engine-parity-map.json \
  --manifest rust-repo/tools/store-validation/wheel-store-parity-map.json
PYTHONPATH=rust-repo/tools/store-validation \
python3 -m unittest discover -s rust-repo/tools/store-validation/tests -p 'test_*.py'
```

Expected active store count: `1119 covered / 164 missing / 116 not-applicable`; validator unit tests: 44 pass.

- [ ] **Step 4: Commit exact JSON hunks**

Parse staged blobs as JSON, run `git diff --cached --check`, and commit:

```bash
git commit -m "docs(store): cover NUMA HugeTLB population"
```

### Task 5: Independent Review

**Files:**
- Review only: Task 3 and Task 4 commits.

**Interfaces:**
- Consumes: C++ oracle, planner, affinity/population code, witness, manifests, and verification evidence.
- Produces: Ready without Critical/Important findings, or a focused repair loop.

- [ ] **Step 1: Request focused review**

Ask the existing reviewer to check C++ worker distribution parity, layout safety, sysfs parsing, CPU-set bounds, best-effort affinity, exact page coverage, skip honesty, refs/SHA, and counts.

- [ ] **Step 2: Resolve findings**

Reproduce and repair every Critical/Important issue, rerun proportional verification, update repair SHA if code changes, and repeat review until Ready.
