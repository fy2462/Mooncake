# Rust Store Full Validation and Performance Design

## Purpose

Validate that the Rust Store Client and Master reproduce the functional and
logical behavior specified by the C++ Store, then improve the performance of
TENT, accelerator DLPack, the shared-memory hot cache, and `io_uring` without
regressing correctness.

The C++ Store is the semantic reference, not a system under test. Its tests and
implementation are read to derive expected behavior and Rust test cases. The
validation gates build and execute Transfer Engine and the Rust Store only;
they do not build, execute, or report results for the C++ Store.

## Scope

The tested production components are:

- Transfer Engine, including the TENT boundary needed by Rust Store;
- `transfer-engine-ffi` and its ownership and lifecycle contracts;
- Rust Store Client;
- Rust Store Master; and
- single-node and multi-node flows composed from those components.

The following work is in scope:

1. inventorying C++ Store tests and mapping their behavioral assertions to
   Rust tests;
2. filling missing Rust Client and Master test coverage;
3. running module, integration, three-node, and fault-recovery tests;
4. fixing reproducible Rust differences according to the C++ behavior;
5. establishing reproducible performance baselines for the four named
   capabilities; and
6. optimizing each capability and proving correctness did not regress.

The following work is out of scope:

- testing or improving the C++ Store itself;
- treating C++ Store build or test results as an acceptance gate;
- changing externally visible Store semantics for performance; and
- unrelated refactoring discovered during validation.

## Delivery Order

Work proceeds through a strict correctness gate before performance work:

1. construct the C++-test-to-Rust-test coverage map;
2. make the Transfer Engine and Rust module gates reproducible;
3. add or complete Rust parity tests;
4. run single-node, multi-node, and fault-recovery gates;
5. fix every confirmed difference and repeat the full correctness gate;
6. establish a baseline for each performance capability;
7. optimize one capability at a time; and
8. run capability-specific benchmarks plus the full correctness gate after
   each optimization.

Performance changes must not enter the correctness repair phase. This keeps
failures attributable to either semantic remediation or a measured
optimization.

## Behavioral Coverage Map

A version-controlled coverage matrix is the source of truth for parity work.
Each row records:

- the C++ test file and test name;
- the behavior or invariant asserted by the test;
- the relevant Rust Client, Master, or TE boundary;
- the Rust test file and test name;
- coverage status: `covered`, `missing`, `not-applicable`, or `blocked`;
- the reason for `not-applicable` or `blocked`; and
- the latest verified command or gate containing the Rust test.

Mapping is semantic rather than line-for-line. Multiple C++ cases may map to
one parameterized Rust case when all inputs and assertions remain visible. A
single C++ case may map to several Rust module and integration cases when the
Rust architecture separates Client and Master responsibilities.

`not-applicable` requires a concrete reason such as a C++-only implementation
detail outside the Rust Store boundary. Missing hardware is `blocked`, not
`covered`, and a skipped test never satisfies a parity row.

The initial matrix covers at least:

- Client and Master lifecycle, configuration, reconnect, and cleanup;
- object put, get, remove, upsert, range, and batch operations;
- allocation, replica selection, eviction, offload, and promotion;
- local storage, persistence, restart, and recovery;
- HA, oplog, snapshots, leases, and failover;
- tenant identity, isolation, and quota behavior;
- hot-cache admission, invalidation, and coherence;
- task, timeout, cancellation, and background-worker behavior;
- memory registration, transfer ownership, and error propagation; and
- metrics or administrative state when it is part of observable behavior.

## Correctness Gates

### Module gate

Run the complete Rust workspace test surface with feature combinations needed
by Store. This includes focused tests for Store Core, Client, Master, Python
bindings used by Client workflows, and `transfer-engine-ffi`. Native TE tests
are included where their libraries and transports are available.

Every command records toolchain versions, enabled features, exit status,
duration, and a log path. Tests that require unavailable capabilities report a
machine-readable skip or block reason; they do not silently disappear.

### Rust parity gate

Run every `covered` row from the behavioral coverage map. The tests assert the
same externally meaningful facts as the C++ reference:

- return values and error classes;
- Client-visible data and metadata;
- Master state transitions and placement decisions;
- persistence and recovery outcomes;
- replica, eviction, promotion, and quota effects; and
- resource ownership and cleanup after success, failure, timeout, or cancel.

The gate fails if a mapped test fails, a `covered` row names no discoverable
Rust test, or an in-scope row remains `missing` without an explicit active
disposition.

### Multi-node and fault gate

Use `rust-repo/tools/rdma-multinode` as the standard single-host three-node
acceptance environment. It validates verbs, Transfer Engine, and Rust Store in
that order, and it must reject TCP fallback as an RDMA pass.

The Store scenarios cover normal three-replica operation, degraded reads,
disk fallback and promotion, Store-node restart, Master and etcd restart, RDMA
link interruption and recovery, one-owner reads, memory and disk watermark
eviction, mixed-size stress, and a clean repeated standard run. Failures retain
the first failing result and preserve logs while cleanup remains idempotent.

## Difference Handling

A suspected difference is not repaired until a focused Rust test reproduces
it. Investigation follows this sequence:

1. identify the exact C++ test assertion and implementation path defining the
   expected behavior;
2. reduce the Rust failure to the smallest relevant Client, Master, or TE
   boundary;
3. add a failing Rust regression test;
4. implement the smallest semantic correction in Rust or its TE integration;
5. run the focused test and the owning module suite; and
6. run the complete correctness gate before closing the difference.

If the C++ test exposes undefined, contradictory, or platform-specific
behavior, the row remains explicitly disposed rather than inventing a Rust
rule. Any intentional deviation requires user approval and documentation; it
cannot be labeled parity.

## Performance Work

Each capability is a separate optimization unit with its own benchmark,
profile, change set, and acceptance evidence. The common metrics are throughput,
P50/P95/P99 latency, CPU time, resident memory, allocation count where
observable, bytes copied, and relevant system-call or device counters.

Benchmarks include warmup, fixed dataset and request distributions, repeated
samples, environment metadata, raw results, and a summarized comparison.
Results must distinguish noise from improvement and must retain the pre-change
baseline.

### TENT

Measure submission, scheduling, polling/completion, cancellation, and
multi-request throughput across the Rust FFI boundary. Profiles should separate
Rust marshaling and ownership overhead from native TENT scheduling and
transport time. Optimizations preserve request priority, policy, deadline,
intent, cancellation, status, and registered-memory lifetime semantics.

### Accelerator DLPack

Measure capsule validation, producer synchronization, memory registration,
transfer submission, and teardown for supported accelerator tensors. The
correctness contract proves device ordinal, pointer location, stream/event
ordering, capsule consumption rules, and DMA lifetime before enabling a fast
path. CPU-only mocks can validate rejection and ownership logic but cannot
satisfy accelerator performance acceptance.

### Shared-memory hot cache

Measure acquire/release, lookup, admission, cross-process hit latency,
invalidation, contention, and effective bandwidth. The implementation must use
owner-bearing mappings and explicit IPC lifecycle, preserve tenant and object
coherence, and keep TE registration alive for every in-flight transfer.
Process crash and stale-owner recovery are correctness cases, not benchmark
exceptions.

### `io_uring`

Measure sequential and random reads and writes across object sizes, queue
depths, batching levels, and direct-I/O modes. Compare against the existing
POSIX path under the same workload. Optimizations preserve exact Store
persistence, partial-I/O, cancellation, error, fsync/durability, and shutdown
semantics.

## Hardware Validation Layers

The current development machine is an eight-core AArch64 virtual machine with
one NUMA node, a virtual SSD, Docker, Soft-RoCE support through the existing
suite, and an enabled `io_uring` syscall. It has no visible accelerator,
physical RDMA device, multiple NUMA nodes, or physical NVMe device.

It can therefore provide authoritative evidence for Rust module correctness,
Soft-RoCE multi-node behavior, fault orchestration, benchmark reproducibility,
and functional `io_uring` behavior. It cannot provide final accelerator,
physical-RDMA, NUMA-locality, or NVMe performance evidence.

Hardware-dependent benchmarks must emit a clear capability result. Final
performance acceptance for DLPack requires a supported accelerator; TENT
transport conclusions require physical RDMA; locality conclusions require a
multi-NUMA host; and storage conclusions require the target NVMe class. Until
those results exist, the implementation may progress but the corresponding
performance item remains unsigned.

## Result and Failure Model

Every gate produces a stable result with `PASS`, `FAIL`, `SKIP`, or `BLOCKED`,
plus the command, environment fingerprint, timestamps, duration, and log or raw
benchmark artifact. `SKIP` and `BLOCKED` never roll up to `PASS` for a required
capability.

Test runners preserve the first product failure, attempt independent reporting
and safe cleanup, and do not destroy artifacts needed for diagnosis. Repeated
runs use isolated paths and explicit ownership markers for temporary services,
links, devices, shared memory, and storage.

## Acceptance Criteria

Correctness is accepted only when:

1. every in-scope C++ behavioral test has a reviewed matrix disposition;
2. every `covered` row has a passing Rust test;
3. all required TE, Rust Client, and Rust Master module and integration tests
   pass;
4. the standard and resilience three-node gates pass without transport
   fallback;
5. every observed semantic difference has a passing regression test and a
   complete-gate result; and
6. no C++ Store binary, library, or test result is used as tested-product
   evidence.

The performance phase is accepted only when each of TENT, accelerator DLPack,
shared-memory hot cache, and `io_uring` has:

1. a reproducible pre-change baseline;
2. a documented profile and bottleneck hypothesis;
3. measured post-change results using the same workload;
4. a material improvement or an explicit evidence-backed no-change
   disposition;
5. passing capability-specific correctness tests; and
6. a passing complete Rust correctness gate after the optimization.

The overall goal remains incomplete while any required real-hardware
performance result is missing or any in-scope parity row is unresolved.
