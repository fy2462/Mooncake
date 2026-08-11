# NUMA HugeTLB Population Parity Design

## Scope

Cover `MmapArenaFallbackTest.PopulateNumaHugetlbMappingTouchesEveryRegion` by adding a Linux Rust helper that partitions a mapping into equal NUMA regions, assigns node-local worker threads, and touches the first byte of every configured hugepage.

The behavior mirrors the C++ helper and test boundary. It does not add a `libnuma` build dependency or redesign the existing segment allocator.

## Considered Approaches

1. Read Linux NUMA CPU lists from sysfs and bind each region worker with `pthread_setaffinity_np`. This is selected because it provides real best-effort NUMA awareness using the existing `libc` dependency.
2. Link against `libnuma` and call `numa_run_on_node`. This most directly copies C++, but introduces a system-library dependency that is not currently required by the Rust client.
3. Use only an injectable fake binder. This can test partitioning but does not implement the production NUMA worker-affinity behavior represented by the missing row.

## Region Planning

Add a pure helper that accepts page count, ordered NUMA node IDs, and an available worker count. Valid input requires:

- at least one page, node, and worker;
- page count divisible by node count, because every node receives an equal region;
- at least one page per region.

The total worker count matches C++: `max(node_count, min(available_workers, 16, page_count))`. Workers are distributed across nodes using quotient and remainder. Each node's region is then divided into bounded, non-overlapping page ranges. The plan returns `(node_id, Range<usize>)` entries whose flattened ranges cover `0..page_count` exactly once.

The mapping helper additionally requires `len` to be divisible by node count and each region length to be divisible by `page_size`. Invalid layouts return `io::ErrorKind::InvalidInput` before any writes.

## Linux Node Affinity

For node `N`, read `/sys/devices/system/node/nodeN/cpulist`. Parse Linux CPU-list syntax such as `0-3,8,10-11` into unique CPU IDs. Initialize `libc::cpu_set_t`, add every representable CPU with `CPU_SET`, and call `pthread_setaffinity_np(pthread_self(), ...)` inside the worker thread before touching pages.

Node-list parsing, sysfs reads, or affinity calls are best-effort at population time. Failure emits a warning and the worker still touches its assigned range, matching C++ behavior when `numa_run_on_node` fails. Layout and memory-safety errors remain hard errors.

No affinity lock or process-global state is introduced. Each worker terminates after its range, so its thread-local affinity requires no restoration.

## Population Helper

Add Linux-only unsafe `populate_hugetlb_numa_pages(ptr, len, page_size, numa_nodes)` beside `populate_hugetlb_pages`.

The helper validates pointer, sizes, nodes, and equal-region layout; obtains available parallelism with the existing 16-worker cap; computes the region plan; then uses scoped threads. Each worker attempts node affinity and performs volatile zero writes at `ptr + page_index * page_size`. All joins must succeed or return an `io::Error`.

The function is unsafe because the caller must provide a uniquely writable mapping covering the full rounded page boundary range. The implementation never writes outside `ptr..ptr+len` for a valid divisible layout.

## Witnesses

Add the discoverable test `cpp_parity_numa_hugetlb_population_touches_every_region`.

On Linux, read the online node list from `/sys/devices/system/node/online`, select at most two nodes, and skip with an explicit stderr message if NUMA sysfs is unavailable or empty. Allocate an anonymous writable Rust buffer representing two 2 MiB pages per selected region, fill it with `0xCD`, and set the first byte of every page to `0xAB`. Call the production helper, then assert every page boundary is zero and neighboring bytes remain `0xCD`.

Add pure tests for CPU-list parsing and the region planner. These tests establish multi-node partition behavior even on single-node CI, while the parity test uses the actual online node IDs and real affinity attempt available on its host.

## Verification and Accounting

Use TDD: first add the parity and planner/parser tests and capture compilation failure for absent helpers. Implement the planner/parser, then the Linux affinity/population helper. Run the exact parity test, related memory population tests, full client library suite, and `rustfmt --check`/`git diff --check`.

Commit code before documentation. Mark only the target manifest row covered with the exact Rust test reference, append a remediation record with immutable repair SHA and truthful skip semantics, run all four manifest validators plus validator unit tests, and obtain independent review with no Critical/Important findings.
