# Crate、进程与入口

## 前置章节

- “环境与构建”

## 本章目标

区分 crate、进程与 native 边界，并为每个组件找到第一份应读源码。

## 先分清 crate 和进程

一个 crate 不一定对应独立进程。`mooncake-store-client` 既可被应用嵌入，
也可在测试 Store Node 进程中挂载一块 Segment。Master 是独立服务进程；
Transfer Engine 是被各数据节点嵌入的 native runtime。

| 组件 | 运行位置 | 主要职责 | 第一入口 | 关键符号 |
|---|---|---|---|---|
| Store core | Rust 库 | 共享领域类型与错误 | `rust-repo/crates/mooncake-store-core/src/types.rs` | `Segment`, `ReplicaDescriptor`, `ReplicaStatus`, `ReplicaType`, `ReplicateConfig` |
| Store Client | 应用或 Store Node 内 | 对象 API、Master RPC、Replica 选择、TE 调用 | `rust-repo/crates/mooncake-store-client/src/client/mod.rs` | `MooncakeClient` |
| Master | `mooncake-master` 进程 | 元数据、分配、状态机、后台任务、HA | `rust-repo/crates/mooncake-store-master/src/main.rs` | `MasterServiceImpl` |
| TE FFI | Rust 库 | 安全句柄、错误映射、C ABI 生命周期 | `rust-repo/crates/transfer-engine-ffi/src/lib.rs` | `TransferEngine`, `SegmentId`, `BatchId`, `TransferRequest` |
| C++ TE | 每个数据参与者内 | 内存注册、peer/segment、transport 与传输完成 | `mooncake-transfer-engine/src/transfer_engine.cpp` | `TransferEngine` |
| RDMA transport | C++ TE 内 | RNIC context、endpoint/QP、WR/CQ | `mooncake-transfer-engine/src/transport/rdma_transport/rdma_transport.cpp` | `RdmaTransport`, `RdmaContext`, `RdmaEndpoint` |
| Python binding | Python 进程内 | 把 asyncio API 映射到 Rust Client | `rust-repo/python/src/client.rs` | `MooncakeClient` Python class |
| E2E Store Node | Docker 容器内 | 挂载 RDMA Segment 和 RustFilePerKey 本地层 | `rust-repo/tools/rdma-multinode/store-node.py` | `main` |
| 三节点编排 | Host + Docker | A/B/C RXE、TE、Store 与故障注入 | `rust-repo/tools/rdma-multinode/run.sh` | `all`, `store_resilience` |

## Workspace 依赖方向

```{mermaid}
flowchart LR
    PY[mooncake-store-py] --> CLIENT[mooncake-store-client]
    PY --> CORE[mooncake-store-core]
    CLIENT --> CORE
    CLIENT --> FFI[transfer-engine-ffi]
    MASTER[mooncake-store-master] --> CORE
    FFI --> CABI[C ABI]
    CABI --> TE[C++ Transfer Engine]
    TE --> RDMA[RDMA/TCP/TENT]
```

依赖图中没有 C++ Store。Master 和 Client 共享协议/领域语义，但 Master 不依赖
Client，也不通过 TE 搬运对象主体。

## 请求入口

### Client

`MooncakeClient` 的字段定义在
`rust-repo/crates/mooncake-store-client/src/client/mod.rs`，实现按职责拆分：

- 创建与资源生命周期：`client/lifecycle.rs`
- Put：`client/write.rs`、`write_parts.rs`、`transfer_write.rs`
- Get：`client/read.rs`、`read_meta.rs`、`transfer_read.rs`
- Remove：`client/remove.rs`
- Replica 选择：`client/replica_selection.rs`
- 多级存储：`client/storage_*.rs` 与 `local_storage_backend/`

不要期待在 `mod.rs` 中看到所有方法；Rust 允许多个文件中的
`impl MooncakeClient` 共同构成一个类型的行为。

### Master

二进制入口 `rust-repo/crates/mooncake-store-master/src/main.rs` 在 standalone
和 HA loop 间分流。tonic trait 适配器在 `service/grpc_trait.rs`，业务实现继续
拆到：

- `service/grpc_objects_put.rs`：`put_start_impl`、`put_end_impl`
- `service/grpc_objects_query.rs`：查询和 Remove
- `service/cluster/segment.rs`：Segment mount/unmount/status
- `allocator/`：空间与 Replica 放置计划
- `oplog/` 和 `ha/`：持久化变更、snapshot、standby、promotion

### Transfer Engine

Rust 从 `rust-repo/crates/transfer-engine-ffi/src/lib.rs` 的 owning handle 开始。
它通过 build-time bindgen 生成的 C ABI 调用 native TE。传输值类型位于
`transfer-engine-ffi/src/transfer.rs`，Segment 类型位于 `segment.rs`。

进入 C++ 后先看 public façade，再按 transport 下钻；不要从 verbs 调用开始
逆向猜测整个 TE。

## 协议定义

Master RPC 与共享消息来自：

- `rust-repo/proto/mooncake_store_grpc.proto`
- `rust-repo/proto/mooncake_store_types.proto`
- `rust-repo/proto/mooncake_offload_rpc.proto`

生成的 Rust 客户端和服务端代码通过 `tonic::include_proto!` 引入。阅读 RPC
时先看 proto 字段，再看 Client 构造请求、Master 转换类型的两端实现。

## 自检问题

1. Store Node 是否对应一个独立 production crate？
2. Rust Store 唯一的主要 native 数据传输边界在哪里？

## 下一步

进入“推荐源码阅读顺序”，按依赖逐步下钻。
