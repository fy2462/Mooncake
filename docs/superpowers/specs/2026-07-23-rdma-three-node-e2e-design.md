# Three-Node RDMA E2E Design

## Goal

Make `rust-repo/tools/rdma-multinode` the standard privileged acceptance suite
for a three-node software-RDMA topology. The suite must prove direct Transfer
Engine connectivity, Rust Store replication, multi-level cache behavior, and
failure recovery without loading the C++ Store implementation.

## Topology

The host setup creates three isolated veth-backed RXE devices and records all
suite-owned resources in the host manifest. Compose starts three TE containers
and three Rust Store node containers, named A, B, and C. Each Store node owns a
distinct RDMA endpoint and its own ephemeral test storage under container
`/tmp`. Build outputs and retained evidence remain under
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode`.

The direct TE gate exercises every unordered node pair in both directions:
A to B, B to A, B to C, C to B, C to A, and A to C. Every transfer must report
RDMA as the selected protocol and compare the received bytes successfully.

## Store Replica Profiles

The Store acceptance suite uses three explicit replica profiles:

- Three replicas for baseline object correctness. Small, large, sliced,
  concurrent, overwrite, and delete cases must observe complete RDMA memory
  replicas on A, B, and C.
- Two replicas for degraded-read behavior. Placement must span two distinct
  Store nodes; after one owner is stopped, the surviving owner must return
  byte-identical data.
- One replica for multi-level cache behavior. Memory pressure must exceed the
  aggregate high-watermark capacity of all three Store nodes, produce a
  disk-only `LocalDisk` replica, return byte-identical fallback data, and
  promote the object back to its original RDMA endpoint.

Replica expectations are scenario-specific and must not be weakened to
"at least one reachable copy" in the baseline gate.

## Resilience Scenarios

Node-failure coverage rotates across A, B, and C instead of treating A/B as a
fixed pair. The suite covers Store-node restart, Master and etcd restart, RXE
disconnect and reconnect, two-replica degraded reads, memory and SSD watermark
eviction, mixed-size stress, and a second clean standard pass. Each scenario
must emit structured evidence identifying affected nodes, observed failure,
recovery, replica counts, protocol, and byte-integrity result.

The Rust HA Master must support an empty-cluster first-leader election. Its
etcd oplog notifier receives a bounded startup grace period before an initially
false health bit is treated as a broken watch. A regression test must reproduce
delayed notifier health initialization.

## Orchestration and Cleanup

`run.sh all` remains the canonical entry point. It builds with
`CARGO_BUILD_JOBS=5`, provisions host RDMA, starts Compose, runs verbs, direct
TE, standard Store, resilience Store, and the independent Open-RDMA mock check,
then renders one report. Product gates fail closed; the independent mock result
is classified separately.

Signal and error cleanup must stop all A/B/C test processes and containers,
remove only manifest-owned RXE/veth resources, and leave an empty suite
inventory. Cleanup commands use RXE device names rather than `NAME/PORT` link
display strings.

## Verification

Implementation follows red-green-refactor cycles for topology contracts,
three-node orchestration, replica profiles, resilience evidence, and HA notifier
startup. Completion requires:

1. All `rust-repo/tools/rdma-multinode/tests` shell and Python tests pass.
2. Relevant Rust workspace tests, formatting, and dependency checks pass with
   build output in the shared artifact root.
3. A privileged `run.sh all` run records PASS for verbs, all six direct TE
   directions, standard Store, and every resilience scenario.
4. The standard result proves three-replica RDMA placement, the degraded result
   proves two-replica survival, and the multi-level result proves disk fallback
   plus promotion.
5. No C++ Store shared library is loaded, and final Docker/RXE/veth inventory is
   empty.

## Non-Goals

This work does not modify the external Open-RDMA checkout, add physical RDMA
hardware requirements, persist test runtime data outside the containers, or
change production Store replication defaults. The 3/2/1 profiles are E2E
scenario inputs only.
