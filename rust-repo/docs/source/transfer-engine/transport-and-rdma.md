# Transport 与 RDMA 实现

## 前置章节

- “Rust FFI 边界”

## 本章目标

沿 RDMA transport 找到 device/GID、MR、endpoint/QP、WR 提交和 completion 的具体
实现位置，并理解链路中断为何可能需要重建连接。

## 源码地图

| 阶段 | 主要文件 |
| --- | --- |
| transport 入口/切片准备 | `mooncake-transfer-engine/src/transport/rdma_transport/rdma_transport.cpp` |
| device、MR 与上下文 | `mooncake-transfer-engine/src/transport/rdma_transport/rdma_context.cpp` |
| endpoint、QP 与 WR | `mooncake-transfer-engine/src/transport/rdma_transport/rdma_endpoint.cpp` |
| worker/CQ 与重试 | `mooncake-transfer-engine/src/transport/rdma_transport/worker_pool.cpp` |
| endpoint 缓存 | `mooncake-transfer-engine/src/transport/rdma_transport/endpoint_store.cpp` |

## 从请求到 WR

`RdmaTransport::submitTransfer` 校验 batch/request，依据本地地址找到注册 buffer/lkey，
依据远端 Segment metadata 计算 remote address/rkey，把大请求切成 slice 并交给 worker。
endpoint 把 slice 转为 SGE/WR，选择 QP 后 post；worker 消费 CQ completion 并更新每个
task 的 transferred bytes 和 terminal status。

```{mermaid}
flowchart LR
  R[TransferRequest] --> V[validate local/remote ranges]
  V --> SL[slice request]
  SL --> EP[get/create endpoint]
  EP --> WR[post RDMA WR]
  WR --> CQ[poll CQ]
  CQ --> ST[update task status]
  CQ -->|retryable| EP
  CQ -->|terminal error| ST
```

## device 与 GID

RdmaContext 绑定具体 device，并通过 GID probe/配置选择 GID index。多 NIC 时 topology
和调度器决定 slice 使用哪条 rail。远端 metadata 中的协议和 buffer 必须与 RDMA
transport 匹配，不能把 TCP 注册描述误绑为 RDMA MR。

## 链路中断

CQ error 不都意味着本地 RNIC 故障。代码区分可重试、endpoint/QP 失效与终态错误，
并维护重试上限。链路恢复后旧 QP 不一定自动可用；endpoint store/worker 必须丢弃或
重建连接。E2E 因此既观察中断期间失败，又在 link 恢复和节点重启后重新做字节校验。

## TCP 的意义

TCP transport 实现相同 batch/status 接口，可用于其他部署，但 RDMA 验收不允许静默
TCP fallback。日志出现 `protocol: tcp` 时，即使字节一致也不能记为 RDMA PASS。

## 自检问题

1. lkey 与 rkey 分别保护哪一侧？
2. 为什么链路恢复不保证旧 QP 立即可用？
3. TCP 字节校验通过为什么不能替代 RDMA 门禁？

## 下一步

进入“一次传输的生命周期”，把所有 API 与失败清理串成完整时序。
