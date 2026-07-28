# Rust Store Three-Master HA Chaos Gate Design

## Goal

Add a reproducible live gate that proves the Rust Store retains the C++ chaos
test contract across repeated crashes and restarts of a three-Master HA group.
The gate uses etcd for leader election and the shared ordered oplog, exercises
real Master processes and TCP data transfers, and validates object bytes rather
than only coordinator or metadata state.

## Scope

The gate covers the two C++ references in
`mooncake-store/tests/e2e/chaos_rand_test.cpp`:

1. `ChaosRandTest.RandomMasterCrashWithSmallValue`;
2. `ChaosRandTest.RandomMasterCrashWithLargeValue`.

It starts one isolated etcd container, three Rust `mooncake-master` processes,
and multiple Rust Store clients. A fixed seed controls every Master stop/start
decision and client/key selection. The small-object workload avoids eviction;
the large-object workload intentionally exceeds aggregate mounted memory and
enables eviction pressure.

The gate is independent of RDMA and accelerator hardware. It uses the native
Transfer Engine TCP transport so that it verifies the real Store data path on
ordinary development and CI hosts. The existing Soft-RoCE suite remains the
authority for RDMA-specific resilience.

## Architecture

### Canonical entrypoint

`rust-repo/tools/store-validation/run-ha-chaos-live.sh` is the canonical
entrypoint. It:

1. validates Docker, the etcd image, Cargo, and native Transfer Engine
   libraries;
2. builds the Rust Master and the dedicated live-test binary;
3. reserves an unused loopback client port for etcd and starts an isolated,
   suite-owned etcd container;
4. invokes the live-test binary with the etcd endpoint, Master binary path,
   artifact directory, seed, and bounded timing parameters;
5. validates the emitted JSON result;
6. always stops and removes only its own etcd container.

The entrypoint never reuses the fixed `mc-rdma-etcd` name or port. A unique run
identifier namespaces the container, etcd keys, object keys, temporary
directories, logs, and result files, allowing concurrent and repeated runs.

### Live-test binary

A dedicated integration test target under `mooncake-store-client` owns the
process and client lifecycle. It is opt-in and receives all external paths and
endpoints through environment variables set by the shell entrypoint. A normal
`cargo test` run reports the test as an exact capability skip unless the live
gate opt-in variable is set; an opted-in run treats every missing dependency as
a failure.

The harness launches three `mooncake-master` children on separately reserved
loopback ports with identical:

- etcd endpoint;
- unique cluster namespace;
- HA lease TTL;
- Store runtime and eviction configuration.

Each child has a separate snapshot directory and log file. The shared etcd
oplog is the authoritative replication mechanism; local snapshot directories
must not be shared between Master processes.

The clients use TCP Transfer Engine endpoints and the ordered list of all three
Master addresses. They retain the same mounted segments for the full scenario.
After an RPC failure or leader change, the harness invokes the client's normal
candidate failover and health/remount path. It does not create a new client to
hide remount or leader-switch defects.

### Leader observation

The test reads the current etcd Master view through the Rust coordinator API.
A stabilization helper waits until:

1. etcd reports a nonempty leader view;
2. the advertised address is one of the three configured Masters;
3. a client can switch to that address and complete health/remount;
4. the same view version remains observable for a bounded quiet interval.

Process liveness alone is not evidence of a stable service. Each recovery
checkpoint requires the coordinator view and a successful Store operation.

## Scenario model

### Determinism

The default seed is `0x4d4f_4f4e_4841_4348` (`MOONHA CH`), printed in logs
and recorded in JSON. The seed, number of rounds, and timeouts may be overridden
for diagnosis. The default acceptance profile is short enough for a live CI
gate but performs at least four failure/recovery rounds per workload.

Every round preserves at least one running Master before the stability check.
The selected victim set rotates across all three Master indices over the full
run. Restart decisions reuse the original command line, port, snapshot path,
and log identity.

### Small-object scenario

At least three clients mount memory segments large enough to hold the complete
small-object set without eviction. For each round:

1. stop one or two seeded Master victims;
2. during instability, attempt cross-client puts and gets; operation failures
   caused by leadership loss are allowed;
3. for every put reported as successful or already present, require an exact
   cross-client read of the expected bytes once a surviving leader stabilizes;
4. after stabilization, put or confirm every key, then read every key from a
   different selected client and compare exact bytes;
5. restart selected stopped Masters and require them to rejoin the next
   failure/recovery cycle.

The scenario fails if no unstable-period operation succeeds, if any successful
write later returns different bytes, or if the stable cluster cannot serve the
entire key set.

### Large-object scenario

The large-object profile uses values close to each client segment's capacity,
with a logical dataset larger than aggregate mounted memory. It enables short
eviction intervals and fixed high/low watermarks. Each value contains a
key-derived deterministic pattern and digest.

The same crash/restart cycle applies, with these assertions:

- every successful read has the exact expected length, digest, and bytes;
- transient not-found/capacity/unavailable errors are allowed only before the
  leader stabilization checkpoint;
- after stabilization, each key is either read exactly or rewritten exactly
  and then read from another client;
- the workload records evidence that eviction pressure occurred, rather than
  inferring pressure only from configured capacity.

## Failure handling and cleanup

Every process and external resource has an owner guard. On success, assertion
failure, timeout, panic, `INT`, or `TERM`, cleanup:

1. terminates all live Master children with a bounded graceful wait followed by
   a forced kill only for the exact recorded PIDs;
2. removes the uniquely named etcd container;
3. preserves Master logs, client logs, seeds, commands, and JSON results;
4. removes only ephemeral runtime directories owned by the current run.

No broad process-name kill, wildcard container deletion, fixed shared port, or
workspace cleanup is permitted.

The final result is `PASS`, `FAIL`, or `SKIP`. `SKIP` is valid only before
opt-in when Docker or native TCP capability is absent. Once the canonical
entrypoint opts in, missing dependencies, startup failures, timeouts, and
insufficient failure/eviction evidence are `FAIL`.

## Machine-readable evidence

The result JSON includes:

- schema version, status, seed, start/end time, and duration;
- etcd endpoint, cluster namespace, and three Master addresses;
- per-scenario operation counts, successful unstable operations, stable reads,
  exact-byte comparisons, eviction evidence, crash count, restart count, and
  observed leader view versions;
- first failure stage and diagnostic message;
- paths to each retained Master/client log.

The gate succeeds only when both small- and large-object scenarios pass and all
three Master indices have been stopped and restarted at least once.

## Test strategy

The implementation follows test-driven development at two levels.

Contract tests exercise the shell runner with fake Docker/Cargo commands and
verify unique resource naming, exact capability behavior, argument propagation,
result validation, signal cleanup, and preservation of diagnostics.

The live integration test first proves that the current implementation fails
the intended crash/restart behavior for the correct reason, then any product
fix is made narrowly against that failure. Focused Rust tests cover deterministic
victim selection, stability deadlines, expected-value bookkeeping, and result
serialization without replacing the real live scenario with mocks.

Acceptance requires two consecutive canonical live-gate passes with different
fixed seeds. The parity manifest may mark the two C++ chaos rows `covered` only
after those live passes exist and the referenced Rust test names are stable.

## Non-goals

This gate does not:

- validate RDMA, TENT, accelerator DLPack, CXL, Redis, or Kubernetes backends;
- claim availability when all three Masters are stopped;
- introduce a second HA protocol or a test-only Master replication path;
- weaken the C++ oracle to metadata-only or single-process simulation;
- make performance claims.

Those capabilities retain separate correctness gates and benchmarks in the
broader Rust Store audit.
