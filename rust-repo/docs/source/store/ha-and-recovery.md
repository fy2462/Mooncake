# HA 与故障恢复

## 前置章节

- “元数据、快照与 oplog”

## 本章目标

理解 leader 任期、standby 追平、promotion 门槛，以及不同故障的恢复责任。

HA 主循环在 `rust-repo/crates/mooncake-store-master/src/main_ha.rs`。coordinator 基于
etcd、Redis 或 Kubernetes backend 竞争带 view/version 的 leadership。获得租约后
准备 oplog 与状态再服务；失去租约必须停止 mutation 并清理本任期资源。

```{mermaid}
stateDiagram-v2
  [*] --> Stopped
  Stopped --> Connecting: start
  Connecting --> Syncing: watch connected
  Syncing --> Ready: snapshot + oplog caught up
  Ready --> Promoted: leadership and lag=0
  Connecting --> Failed: error limit
  Syncing --> Failed: replay failure
  Ready --> Connecting: watch lost
  Failed --> Connecting: restart
  Promoted --> Stopped: term lost
```

transition 在 `ha/state_machine.rs`，controller 在 `ha/standby.rs`。standby 必须加载
兼容 snapshot、持续跟随 oplog、保持 watch 健康并让 `lag_entries=0`；有 lag 时拒绝
promotion，避免旧 catalog 成为新事实来源。

| 故障 | 恢复责任 | 核心证据 |
| --- | --- | --- |
| Store 重启 | Segment 重挂载 + Master 状态 | 重启后仍可读 |
| Master 重启 | snapshot/backend/oplog | catalog 与读写恢复 |
| etcd 重启 | coordinator 重连/lease | leader 重新稳定 |
| RDMA link 中断 | TE endpoint/连接重建 | 恢复后字节一致 |
| replica owner 下线 | Client 副本选择 | 从剩余副本读 |

`rust-repo/tools/rdma-multinode/store-resilience-e2e.py` 覆盖这些场景，并记录每项
evidence 与 `first_failure`。oplog 不修复 RDMA 链路，TE 重连也不恢复 Master catalog。

推荐按 `ha/types.rs` → `ha/state_machine.rs` → `ha/coordinator.rs` → `ha/standby.rs` →
`ha/oplog_applier.rs` → `main_ha.rs` 阅读。

## 自检问题

1. watch 健康但 lag 非零为什么仍不能提升？
2. 旧 leader 丢失 lease 后继续服务会怎样？
3. Store 与 Master 重启分别依赖哪些恢复信息？

## 下一步

进入 Transfer Engine，深入 Rust FFI 以下的数据传输。
