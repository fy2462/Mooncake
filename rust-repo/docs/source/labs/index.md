# 分步实验

本节通过请求追踪、三节点 RXE E2E 和 RDMA 故障排查，把文档中的架构模型
落实为可观察的运行行为。每个特权操作都会说明前置条件和清理步骤。

```{toctree}
:maxdepth: 1

tracing-a-request
running-three-node-e2e
debugging-rdma
```

先完成无需特权的源码追踪，再运行完整集群；RDMA 排障实验既可在失败时使用，也可在
成功环境中主动核对每一层证据。
