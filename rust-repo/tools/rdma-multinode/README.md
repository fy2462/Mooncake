# Docker Soft-RoCE validation

This suite validates verbs, Transfer Engine, and the Rust Store standard gate
in that order. The Store gate refuses to run unless `te.result` records
`status=PASS` and `protocol=rdma`. The Compose services are `etcd`, the two
Transfer Engine nodes, `rust-master`, two Store nodes, and `store-test-client`.

All generated data belongs under
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode`. Builds use
`CARGO_BUILD_JOBS=5`. The only required privileged host operations are loading
`rdma_rxe` and creating/deleting explicitly named RXE/veth links.

The Compose topology is limited to this physical host. On kernels reporting
`rdma system: netns shared`, it uses the labeled `shared-rdma-device` topology:
host-network containers share the suite-owned `mc-rdma-rxe` device. A TCP
fallback is never accepted as an RDMA PASS.

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
`open-rdma.result`. The resilience gate covers Store-node restart,
Master/etcd restart, RDMA reconnection, one-owner degraded reads, memory and
SSD watermark eviction, mixed-size stress, and a second full standard run.
It publishes a group status plus per-scenario JSON and logs, preserving the
first failure. When the standard Store gate fails, resilience is recorded as
`status=BLOCKED` with `reason=store-standard-gate`.

The Markdown report separates product evidence (verbs, Transfer Engine, and
standard Rust Store), resilience evidence, and Open-RDMA mock-only evidence.
