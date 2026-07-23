# 多级缓存

## 前置章节

- “Object、Replica 与 Segment”

## 本章目标

理解 offload、eviction 和 promotion 的职责，以及为何测试必须同时检查状态与字节。

Memory 是高性能层，LocalDisk 是容量层。内存达到高水位后，Master 通过心跳下发
offload task；Store Client 读取内存对象、写入本地存储、发布磁盘元数据，随后内存
副本才可释放。Get 命中磁盘后触发 promotion，将数据写入新 Memory 副本。

```{mermaid}
sequenceDiagram
  participant M as Master
  participant S as Store Client
  participant D as LocalDisk
  M-->>S: heartbeat: offload task
  S->>S: read Memory replica
  S->>D: reserve + write
  S->>M: publish LocalDisk metadata
  M->>M: release Memory replica
  Note over M,D: later Get
  M-->>S: promotion task
  S->>D: read bytes
  S->>M: allocate Memory replica
  S->>S: write via TE
  S->>M: promotion success
```

## 水位与正确性顺序

内存水位控制 offload，磁盘水位控制删除旧磁盘对象。高水位是触发点，低水位是回收
目标，必须满足 `0 < low < high <= 1`。

安全 offload 顺序是：验证内存副本 → 预留并写完磁盘 → 发布成功元数据 → 释放内存。
任何步骤失败都要报告失败并清理未发布对象。promotion 也只能在 Memory 数据写完后
报告成功。

源码入口：

- `rust-repo/crates/mooncake-store-client/src/client/storage_local.rs`：offload、磁盘水位与 promotion；
- `rust-repo/crates/mooncake-store-master/src/service/`：任务和副本状态；
- `rust-repo/tools/rdma-multinode/store-e2e.py`：fallback/promotion 验收。

## 有效测试证据

有效测试要观察：初始 Complete Memory → 压力超过高水位 → LocalDisk Complete 且
无 Memory Complete → Get 字节一致 → 新 Complete Memory 出现。只验证最终 Get 会
漏掉“对象从未离开内存”的假阳性。

## 自检问题

1. 为什么必须先发布磁盘元数据再释放 Memory？
2. Get 成功为什么不能单独证明 promotion？
3. 内存驱逐和 SSD 驱逐解决的是同一个容量问题吗？

## 下一步

进入 Store Client 章节，沿生命周期、Put/Get/Delete 和后台任务阅读实现。
