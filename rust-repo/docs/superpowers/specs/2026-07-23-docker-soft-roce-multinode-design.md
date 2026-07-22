# Docker Soft-RoCE Multi-Node Validation Design

## Goal

Create a repeatable Docker environment that gives two logical Mooncake nodes
independent Linux Soft-RoCE devices, proves the Transfer Engine RDMA data path
first, and only then runs Rust Master/Store cross-node tests. Run the external
Open-RDMA driver's pure mock suite as complementary verbs-provider evidence,
not as evidence of cross-node transport.

## Current environment

The host is Ubuntu ARM64 with Linux 7.0.0-27. Docker 29.1.3 is available to
the current user. `ib_core` is loaded, no RDMA link is configured, and the
matching in-tree `rdma_rxe.ko` module is installed. `rdma system show` reports
`netns shared`, so the implementation must explicitly verify whether RXE links
created against container veth devices remain usable from their owning
containers.

The repository already has a TCP-oriented `scripts/run-docker-e2e.sh`, but it
does not create RDMA devices or prove that the Transfer Engine selected RDMA.
The external checkout at
`/home/fy2462/workspace/PFS/open-rdma-driver` supports `mock`, `sim`, and `hw`
modes. Its own documentation defines mock mode as a hardware-free interface
stub; therefore mock success cannot replace an RXE data-transfer test.

## Selected architecture

Use Docker bridge networking and create one RXE device per data-plane
container:

- `mc-rdma-etcd` provides Transfer Engine metadata.
- `mc-rdma-te-a` and `mc-rdma-te-b` each receive a stable bridge IP and an RXE
  device bound to that container's `eth0`.
- `mc-rdma-master` runs the Rust Store master after the TE gate passes.
- `mc-rdma-store-a` and `mc-rdma-store-b` each register a distinct memory
  segment and force the RDMA transport/device selected for that node.
- `mc-rdma-client` performs Store writes and reads whose selected replica is on
  the other data node.

The containers need `CAP_NET_ADMIN`, `CAP_SYS_ADMIN`, `IPC_LOCK`, access to
`/dev/infiniband`, and sufficient `memlock`. The host loads `rdma_rxe` before
containers start. RXE devices and Docker resources are ephemeral; no module or
network configuration is persisted across reboot.

All generated source checkouts, image caches that can be redirected, build
trees, logs, and test artifacts live below
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode`. Cargo commands use
`CARGO_BUILD_JOBS=5`.

## Isolation fallback

The primary topology is accepted only if both containers see separate RXE
device names and GIDs and verbs traffic crosses their Docker bridge IPs.

If the host's shared RDMA namespace prevents an RXE link from being created or
used inside a container network namespace, fall back to host-created RXE links
and host-network containers. The fallback must still use separate server/RPC
ports and must pass the same verbs, TE, and Store content checks. Results must
label this topology as shared-RDMA-device process isolation, not independent
RDMA-node isolation.

Do not silently switch TE or Store to TCP. A TCP fallback is a failed RDMA
test, even when the payload happens to arrive correctly.

## Validation gates

### Gate 0: host and image prerequisites

Record kernel version, `rdma system show`, module identity, Docker version,
hugepage/memlock state, existing RDMA devices, and repository revisions.
Verify that the container image contains `rdma-core`, `ibverbs-providers`,
`perftest` or `rping`, the built TE binaries/libraries, and Rust Store
binaries. Missing packages are image-build failures, not skipped tests.

### Gate 1: independent RXE devices and verbs

For each TE container:

1. create the RXE link against its own `eth0`;
2. verify `rdma link`, `ibv_devices`, and `ibv_devinfo` report an active port;
3. record the device, GID, link IP, and MTU;
4. run an `rping` or `ib_write_bw` server/client pair across the Docker bridge;
5. require successful connection, non-zero transferred bytes, and identical
   payload verification where the tool supports it.

This gate proves kernel verbs and RDMA-CM connectivity independently of
Mooncake.

### Gate 2: Transfer Engine two-node RDMA

Run the repository's `rdma_transport_test` with one target and one initiator
against the same etcd service. Force `--protocol=rdma` and select the intended
RXE device on each node. Exercise both RDMA Read and Write over host memory,
including memory registration, remote-segment discovery/open, transfer status,
content validation, close, deregistration, and clean shutdown.

Gate 2 passes only if:

- both processes exit zero;
- logs show RDMA transport installation and the RXE device/GID;
- no TCP transport or `FORCE_TCP` fallback is active;
- transferred contents match in both directions;
- no failed, canceled, or timed-out transfer remains.

The exact Gate 2 result is the mandatory prerequisite recorded for the Store
test. Gate 3 must not start when Gate 2 fails.

### Gate 3: Rust Master/Store multi-node RDMA

Start etcd and the Rust Master, then start two Rust Store clients/services with
distinct names, RPC ports, RXE selections, and registered memory segments.
The Rust path may depend on native TE/TENT through `transfer-engine-ffi`, but it
must not compile, link, or load `libmooncake_store.so`.

The client scenario must:

- put a small object and read it through a remote memory replica;
- put a multi-chunk object larger than one transfer slice and verify every
  byte;
- repeat reads to cover metadata and segment-cache reuse;
- remove an object and verify the subsequent miss/error contract;
- confirm both Store nodes remain healthy and shut down cleanly.

Acceptance requires logs or API evidence that the selected memory replicas use
protocol `rdma`, the expected remote node, and the configured RXE devices.

### Gate 4: Open-RDMA mock compatibility

Run the external Open-RDMA Rust driver library/mock suite with its existing
working-tree changes preserved. Do not edit or commit that repository as part
of the Mooncake environment work. Report its revision, dirty state, exact test
command, pass/fail counts, and any prerequisite-aware skips separately from
the Soft-RoCE conclusions.

## Implementation units

Keep orchestration separate from test logic:

- a Docker image/Compose definition supplies tools and stable service names;
- a host orchestration script owns module loading, lifecycle, evidence capture,
  fallback selection, and cleanup;
- a container RXE setup script creates and validates only the requested link;
- a TE gate script runs target/initiator tests and emits a machine-readable
  result;
- a Rust Store scenario owns cross-node object assertions;
- a report file records topology, commands, versions, logs, results, and skips.

Every script is idempotent. Cleanup targets only names created by this suite.
On failure, logs are retained under the shared artifact directory while
containers, Docker networks, and RXE links are removed.

## Error handling and safety

- Resolve container, network, interface, and RDMA link names before deletion.
- Never recursively delete `/usr`, `/usr/local`, the workspace root, or a broad
  Docker resource set.
- Do not change persistent module configuration, sysctl files, firewall rules,
  or Docker daemon configuration.
- Treat missing privilege, inactive RXE ports, unavailable `/dev/infiniband`,
  and unsupported network-namespace behavior as explicit failures that may
  trigger only the documented shared-device fallback.
- Preserve the user's dirty Open-RDMA checkout and all unrelated Mooncake
  changes.
- Store no passwords or credentials in scripts, Compose files, images, or
  logs.

## Deliverables and success criteria

The change is complete when the repository contains a reproducible environment
and a fresh report showing:

1. the selected independent or clearly labeled fallback topology;
2. a passing cross-container verbs test;
3. a passing two-node Transfer Engine RDMA Read/Write gate;
4. a passing Rust Master/two-Store-node cross-node object scenario that ran
   only after the TE gate;
5. Open-RDMA mock results reported separately;
6. proof that Rust Store artifacts do not depend on `libmooncake_store.so`;
7. successful cleanup with no suite-owned containers, networks, or RDMA links
   left behind.
