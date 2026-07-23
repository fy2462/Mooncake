# Compose Shared-RXE E2E Design

## Goal

Make the validated `shared-rdma-device` topology the standard, repeatable
end-to-end acceptance test. Docker Compose owns every test service; host-side
scripts own the explicitly named Soft-RoCE and veth lifecycle.

## Scope

The standard test covers:

1. host veth and RXE setup;
2. Compose startup for etcd and all data-plane processes;
3. cross-container verbs transfer;
4. Transfer Engine RDMA Write, Read, and byte comparison;
5. Rust Master with two Rust Store segment owners and one test client;
6. DRAM/SSD multi-level cache offload, fallback, promotion, and consistency;
7. concurrent and mixed-size Store workloads;
8. service restart, link interruption, degraded-read, and recovery scenarios;
9. repeated-run and bounded stress validation;
10. the separately classified Open-RDMA mock run;
11. report generation and idempotent cleanup.

It does not claim two physical hosts. Store nodes are distinct container
processes with distinct logical endpoints and memory segments, sharing one
host RXE device because the validation host reports `rdma system: netns
shared`.

## Architecture

### Host RDMA lifecycle

`setup-host-rdma.sh` runs before Compose startup. It uses normal interactive
`sudo` authentication to load `rdma_rxe`, create an explicitly named veth
pair, configure the suite address, and create exactly one
`mc-rdma-rxe` device. It records ownership and the resulting device/GID/IP in
the artifact directory.

`cleanup-host-rdma.sh` deletes only resources recorded as suite-owned. It is
idempotent and runs for normal completion, command failure, `INT`, and `TERM`.
Passwords are never accepted as arguments, environment variables, Compose
values, or log content.

### Compose services

All data-plane services use `network_mode: host`, privileged device access,
unlimited memlock, `/dev/infiniband`, and the artifact bind mount. The
obsolete bridge addresses and per-container `rxe-a`/`rxe-b` assumptions are
removed.

Compose owns these services:

- `etcd`;
- `te-node-a` and `te-node-b`;
- `rust-master`;
- `store-node-a` and `store-node-b`;
- `store-test-client`.

The services share `mc-rdma-rxe` but use distinct logical endpoints and
ports. The two Store nodes each register a 128 MiB Rust Store memory segment;
the client has no global segment and requests two replicas.

### Gate sequence

The acceptance sequence is fixed:

```text
preflight
host-rdma-setup
build
compose-up
verbs
transfer-engine
rust-store-standard
rust-store-resilience
open-rdma-mock
report
compose-down
host-rdma-cleanup
```

The verbs gate requires non-zero RDMA transfer output. The Transfer Engine
gate requires explicit `protocol: rdma`, Write, Read, and `RDMA compare: OK`
markers. The Rust Store gate cannot start unless the Transfer Engine result is
PASS.

The Rust Store acceptance is split into two mandatory result groups. The
`standard` group requires two complete RDMA replicas on different logical
Store segments, deterministic small/large/cross-slice hashes, repeated reads,
concurrent put/get, overwrite and delete behavior, successful forced removal,
and no dependency on `libmooncake_store.so`. It also enables the Rust Store
SSD backend and proves the multi-level cache loop: a value is written through
the memory tier, offloaded or evicted to SSD, read back with the same hash,
then promoted or restored to memory without losing replica correctness.

The `resilience` group exercises Store-node restart, Rust Master and etcd
restart/recovery, bounded RDMA link interruption and reconnection, degraded
read with one replica owner unavailable, memory/SSD watermark eviction,
concurrent mixed-size load, and a second complete suite execution. Every
scenario has a timeout and machine-readable PASS/FAIL/SKIP result. SKIP is
permitted only for a capability that preflight proves unavailable; a skipped
scenario never satisfies the overall full acceptance target.

Open-RDMA mock remains independent. Its result is recorded as mock-only and
does not change the Soft-RoCE, Transfer Engine, or Rust Store gate results.

## Orchestration and failure handling

`run.sh all` is the canonical entrypoint. Individual stages remain available
for diagnosis, but they consume the same Compose service names and shared RXE
manifest as the full run.

Any failure preserves logs and machine-readable result files. Verbs failure
blocks Transfer Engine and Store. Transfer Engine failure blocks both Store
groups. A standard Store failure blocks resilience scenarios that depend on a
healthy baseline.
Open-RDMA still runs after a product-gate failure when safe, and report plus
cleanup always run. Cleanup never removes an RXE or network link not recorded
as owned by the current suite run.

## Automated tests

Contract tests use fake `sudo`, `ip`, `rdma`, and Docker commands to verify:

- exact owned host-resource creation and idempotent deletion;
- no password handling in repository files;
- Compose uses host networking and the standard service names;
- all gates consume `mc-rdma-rxe` and Compose-owned containers;
- fixed orchestration order and failure short-circuiting;
- Store never starts without a passing RDMA Transfer Engine result;
- multi-level DRAM/SSD transitions preserve hashes and replica metadata;
- restart, link interruption, degraded-read, watermark, concurrency, stress,
  and repeated-run scenarios are bounded and reported independently;
- cleanup runs on failures and signals.

The final acceptance run starts from an empty suite inventory, executes
`CARGO_BUILD_JOBS=5 RDMA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/rdma-multinode bash rust-repo/tools/rdma-multinode/run.sh all`, and finishes with no `mc-rdma-*` container, network, RXE, or veth resource.
