# Docker Soft-RoCE validation

This suite validates verbs, Transfer Engine, and the Rust Store in that order.
The Store gate refuses to run unless `te.result` records `status=PASS` and
`protocol=rdma`.

All generated data belongs under
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode`. Builds use
`CARGO_BUILD_JOBS=5`. The only required privileged host operations are loading
`rdma_rxe` and creating/deleting explicitly named RXE/veth links.

The preferred topology creates `rxe-a` and `rxe-b` in separate containers. On
kernels reporting `rdma system: netns shared`, use the labeled
`shared-rdma-device` fallback: two host-network containers use the same owned
RXE device. A TCP fallback is never accepted as an RDMA PASS.

Run individual stages with `run.sh preflight`, `build`, `verbs`, `te`, `store`,
`open-rdma`, `report`, or `cleanup`. `run.sh all` preserves logs and always
cleans containers, networks, and suite-owned RDMA links. Result files are
`verbs.result`, `te.result`, `store.result`, and `open-rdma.result`.
