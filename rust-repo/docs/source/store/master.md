# Master

## 前置章节

- “Store Client”
- “控制面与数据面”

## 本章目标

理解 tonic handler 如何进入共享状态、allocator 如何生成副本计划，以及 Master 为何
不转发对象字节。

## 请求入口

二进制入口是 `rust-repo/crates/mooncake-store-master/src/main.rs`。standalone 与 HA
模式构造 `MasterServiceImpl` 并启动服务。`service/grpc_trait.rs` 是 tonic 适配层，
把 wire request 分发给领域模块。

```{mermaid}
flowchart LR
  G[tonic handler] --> P[grpc_objects_put]
  G --> Q[grpc_objects_query]
  G --> S[cluster/segment]
  P --> ST[service state]
  Q --> ST
  S --> ST
  ST --> A[allocator]
  ST --> O[oplog]
  ST --> B[background ops]
```

## Put 控制面

`service/grpc_objects_put.rs` 处理 PutStart/PutEnd/PutRevoke。PutStart 校验请求、配额，
创建对象占位并让 allocator 分配 ReplicaDescriptor；响应只携带 Segment、offset、size
和传输信息，payload 由 Client 直接写目标。

PutEnd 检查副本状态与 handle，将有效副本推进为 Complete、更新可见性并记录 oplog。
PutRevoke 是失败补偿路径，撤销未完成对象并回收预留空间。

## 查询、删除与 Segment

`service/grpc_objects_query.rs` 返回读取候选和处理 remove，不替 Client 读取 payload。
删除要协调 busy/refcnt、force、allocator 释放和 LocalDisk 元数据。
`service/cluster/segment.rs` 管理 mount、remount、graceful unmount 与 unavailable；
Segment 状态变化会使相关副本不可选并触发清理或迁移。

异步 handler 通过 `Arc` 共享对象、Segment、任务和 client 状态。读锁范围时关注：不要
持锁等待网络 I/O；allocator 与目录更新必须保持可回滚顺序。

## 推荐阅读顺序

1. `main.rs::main`
2. `service/grpc_trait.rs` 的 put/query/remove
3. `service/grpc_objects_put.rs`
4. `allocator/mod.rs` 与 `allocator/strategies.rs`
5. `service/cluster/segment.rs` 与 `service/background_ops/`

## 自检问题

1. Master 返回地址后，payload 是否经过 Master？
2. PutStart 与 PutEnd 为什么不能合成一个 RPC？
3. Segment unavailable 会怎样影响候选副本？

## 下一步

进入“Store Node”，看 Client、TE、Memory 与 LocalDisk 如何装配。
