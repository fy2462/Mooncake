# 面向 Rust 开发者的 RDMA

## 前置章节

- “系统全景”
- “控制面与数据面”
- “部署拓扑”

## 本章目标

建立足以读懂 Mooncake TE 的心智模型：端点如何发现、内存为何要注册、远端地址代表
什么，以及异步传输完成前哪些资源必须保持有效。

## 从普通 I/O 到 RDMA

普通 socket 通常让内核在应用缓冲区和网络栈间搬运字节。RDMA 让 RNIC 在权限允许
时直接访问已注册内存。应用提交 work request 后还要等待 completion；“提交成功”
不等于“传输完成”。

| Rust/系统概念 | RDMA 概念 | 在 TE 中的意义 |
| --- | --- | --- |
| 地址稳定的 buffer | Memory Region（MR） | 注册后才能被 RNIC 访问 |
| 网络连接能力 | Queue Pair（QP） | 承载 RDMA 请求 |
| 异步任务 handle | Batch / Work Request | 追踪一组传输 |
| `Future` 完成 | Completion | 设备已完成或失败 |
| 地址 + capability | remote address + rkey | 对指定 MR 的访问授权 |

注册 MR 会映射页并产生访问 key。因此 buffer 生命周期必须覆盖注册、提交、完成确认
和注销。若 Rust owner 提前 drop，即使裸指针数值还在，也可能产生 use-after-free。

```{mermaid}
sequenceDiagram
  participant R as Rust owner
  participant T as Transfer Engine
  participant N as RNIC
  R->>T: register_local_memory(ptr, len)
  T->>N: register MR
  R->>T: submit_transfer(batch, requests)
  T->>N: post work requests
  loop until terminal
    R->>T: get_transfer_status(batch)
  end
  N-->>T: completion / error
  R->>T: free_batch + unregister
```

## Segment、device 与 GID

Segment 是一块可分配区域及其 TE 可访问信息，不是 RDMA 连接本身。`open_segment`
让本地 TE 发现远端 Segment；连接和 transport 资源由 TE 管理。

- **device** 标识 RDMA 设备，例如测试中的 `mc-rdma-rxe-a`。
- **GID** 用于 RoCE 寻址，同一设备可能有多个 GID index。
- **Soft-RoCE/RXE** 用软件实现 verbs/RDMA 语义，适合功能验收，不代表 RNIC 性能。

控制面可能完全健康，但 device/GID 配错会让数据面超时。因此测试必须同时断言
`protocol=rdma` 和字节一致。

## 源码入口与安全检查

- `rust-repo/crates/transfer-engine-ffi/src/lib.rs`：handle、Segment 和 batch 包装；
- `rust-repo/crates/transfer-engine-ffi/src/transfer.rs`：操作码、请求与状态；
- `mooncake-transfer-engine/src/transfer_engine.cpp`：native TE 顶层；
- `mooncake-transfer-engine/src/transport/rdma_transport/rdma_transport.cpp`：RDMA transport。

第一遍重点检查 raw handle 是否唯一释放，`unsafe impl Send/Sync` 的 native 前提，指针
和长度是否活到 completion，以及错误路径是否释放 batch、注销内存和关闭 Segment。

## 自检问题

1. 为什么提交成功后不能立即释放源 buffer？
2. 元数据 RPC 正常为什么不能证明 RDMA 正常？
3. Soft-RoCE PASS 能证明什么，又不能证明什么？

## 下一步

进入“Object、Replica 与 Segment”，把 RDMA 可访问区域映射到 Store 放置模型。
