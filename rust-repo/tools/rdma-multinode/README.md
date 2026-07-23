# Docker Soft-RoCE validation

This suite validates verbs, Transfer Engine, and the Rust Store standard gate
in that order. The Store gate refuses to run unless `te.result` records
`status=PASS` and `protocol=rdma`. Compose starts `etcd`, three Transfer Engine
nodes, `rust-master`, three Rust Store nodes, and `store-test-client`.

Retained E2E binaries, logs, and result files belong under
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode`; container runtime data
uses `/tmp`. Normal Cargo development and unit tests use `rust-repo/target`.
Builds use `CARGO_BUILD_JOBS=5`. The only required privileged host operations
are loading `rdma_rxe`, creating/deleting explicitly named RXE/veth/bridge
links, and interrupting the owned C-node link during the resilience gate.

The Compose topology is limited to this physical host. On kernels reporting
`rdma system: netns shared`, host-network containers use three suite-owned RXE
devices (`mc-rdma-rxe-a`, `-b`, and `-c`) connected by the manifest-owned
`mc-rdma-br` software-RoCE bridge. A TCP fallback is never accepted as an RDMA
PASS. The TE gate covers A↔B, B↔C, and C↔A (all six directed transfers).

Run the acceptance sequence with:

```bash
rust-repo/tools/rdma-multinode/run.sh all
```

It executes `preflight host-rdma-setup build compose-up verbs te
store-standard store-resilience open-rdma report compose-down
host-rdma-cleanup`. Host setup and cleanup prompt through interactive `sudo`
when needed; the scripts never read a password. Cleanup only delegates to the
manifest-owned `mc-rdma-*` host RDMA lifecycle scripts.

Diagnostic subcommands use those same names; `store` remains an alias for
`store-standard`, and `cleanup` runs the two cleanup stages. `run.sh all`
preserves the first failing status while still attempting independent
Open-RDMA/report stages and cleanup.

Result files are `verbs.result`, `te.result`, `store.result` (legacy Store
gate output), `store-standard.result`, `store-resilience.result`, and
`open-rdma.result`. The standard and resilience gates enforce three-replica
baseline placement, two-replica degraded reads, and one-replica disk
fallback/promotion. Resilience rotates failures across A/B/C and covers Store
node restart, Master/etcd restart, C-link RDMA reconnection followed by node
restart, one-owner degraded reads, memory and SSD watermark eviction,
mixed-size stress, and a second full standard run. It publishes a group status
plus per-scenario JSON and logs, preserving the first failure. When the
standard Store gate fails, resilience is recorded as `status=BLOCKED` with
`reason=store-standard-gate`.

The Markdown report separates product evidence (verbs, Transfer Engine, and
standard Rust Store), resilience evidence, and Open-RDMA mock-only evidence.
