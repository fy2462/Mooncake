# Get：选择副本与读取

## 前置章节

- “Put：从分配到提交”

## 本章目标

追踪 hot cache、Master 查询、Memory/NoF、LocalDisk 和 miss handler 的选择路径。

```{mermaid}
flowchart TD
  G[get] --> H{hot cache?}
  H -->|hit| R[return]
  H -->|miss| Q[Master replica list]
  Q --> S{best Complete replica}
  S -->|Memory/NoF| T[TE/local read]
  S -->|LocalDisk| D[offload read]
  S -->|none| X{miss handler?}
  X -->|yes| O[S3/FS]
  X -->|no| N[KeyNotFound]
  T --> C[cache + return]
  D --> C
  D -.-> P[promotion]
  O --> C
```

入口是 `rust-repo/crates/mooncake-store-client/src/client/read.rs` 的 `Client::get`。
tenant-scoped hot cache 未命中后，Client 从 Master 获取可见候选并调用
`select_best_replica`。`read_from_replica` 按介质进入本地复制、TE Read 或 LocalDisk
offload server；成功结果写回 hot cache。

LocalDisk 读取触发 promotion，但读取成功不代表 promotion 已完成。allocator 可能把
新 Memory 副本放到另一个健康 Segment。`get_into` 可直接写调用者注册 buffer，减少
内部复制，但仍需 Master 查询、足够容量、buffer 生命周期和 TE completion。

## 自检问题

1. hot cache 命中是否访问 Master？
2. LocalDisk Get 成功为何不等于 promotion 完成？
3. promotion 为什么可能换节点？

## 下一步

进入“Remove 与覆盖写”。
