# 元数据、快照与 oplog

## 前置章节

- “Master”
- “Store Node”

## 本章目标

区分运行时目录、持久化 backend、oplog 与 snapshot，理解 mutation 如何可恢复。

| 状态 | 作用 | 入口 |
| --- | --- | --- |
| 运行时目录 | 查询 object/replica/segment/task | `service/state.rs` |
| StorageBackend | 持久对象与容量状态 | `storage_backend.rs` |
| OpLog | 按 sequence 记录 mutation | `oplog/oplog_manager.rs` |
| Snapshot | 某一点的完整 catalog | `ha/snapshot.rs` |

OpLogManager 为事件附加单调 sequence 和 view version，再写入 OpLogStore。PutEnd、
remove、revoke、mount/unmount 都有记录；只更新内存而不留下可恢复记录会让 standby
或重启 Master 缺失变更。

```{mermaid}
flowchart LR
  S[latest snapshot @ N] --> L[restore catalog]
  O[oplog N+1..latest] --> A[ordered replay]
  L --> A
  A --> R[standby caught up]
```

恢复先加载最新有效 snapshot，再从下一个 sequence 重放 oplog。旧日志清理必须晚于
snapshot 安全持久化和消费者确认。sequence 不得因重启回退；view version 隔离旧
leader；runtime-only 队列应重建，不应盲目序列化。

推荐阅读 `rust-repo/crates/mooncake-store-master/src/oplog.rs`、
`rust-repo/crates/mooncake-store-master/src/oplog/oplog_manager.rs`、具体 store、
`rust-repo/crates/mooncake-store-master/src/ha/snapshot.rs`，最后是 `ha/oplog_applier.rs`。

## 自检问题

1. 有 StorageBackend 为什么还需要 oplog？
2. snapshot sequence 与重放起点差一位会怎样？
3. runtime-only 队列为何需要重建？

## 下一步

进入“HA 与故障恢复”。
