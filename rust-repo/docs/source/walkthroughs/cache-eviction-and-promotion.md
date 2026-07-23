# 缓存驱逐与提升

## 前置章节

- “Remove 与覆盖写”“多级缓存”

## 本章目标

从 Master 心跳任务追踪 Memory→LocalDisk→Memory，并映射到 E2E evidence。

```{mermaid}
sequenceDiagram
  participant M as Master
  participant S as Store Node
  participant D as RustFilePerKey
  M-->>S: offload heartbeat task
  S->>S: read Complete Memory
  S->>D: reserve + write
  S->>M: notify_offload_success
  M->>M: publish disk + evict memory
  M-->>S: promotion task after Get
  S->>M: promotion_alloc_start
  S->>S: TE write Memory
  S->>M: notify_promotion_success
```

Client 入口是 `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`；Master
入口在 `service/cluster/offload.rs` 和 `service/cluster/promotion.rs`。写盘失败要报告失败
并回滚未发布对象；promotion 分配或 TE 写失败也不能暴露半完成副本。

`rust-repo/tools/rdma-multinode/store-e2e.py` 制造超过高水位的压力，选择真正只剩
LocalDisk 的对象，校验 fallback 字节，再等待 Complete Memory。证据应包含 selected
key、压力对象数、fallback 类型/数量与 promotion 内存副本数。

磁盘水位回收从 high 回收到 low，并通知 Master 删除 disk replica；它与内存 offload
方向不同。

## 自检问题

1. 为什么测试要等到没有 Complete Memory？
2. promotion 分配成功但 TE 写失败应怎样？
3. SSD eviction 与 Memory offload 有何不同？

## 下一步

进入“节点故障与恢复”。
