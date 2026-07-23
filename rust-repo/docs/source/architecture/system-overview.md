# 系统总览

## 前置章节

- {doc}`../getting-started/learning-roadmap`
- 熟悉 Rust 的 `Arc`、async/await 和基本 client/server 概念即可。

## 本章目标

建立 Store Client、Master、Store Node、Transfer Engine 和 etcd 的顶层模型，
并能指出对象字节与元数据分别经过哪里。

```{image} ../_static/diagrams/system-overview.svg
:alt: Mooncake Rust Store 控制面与数据面总览
:class: architecture-figure
:width: 100%
```

{download}`下载可编辑的 Draw.io 源文件 <../../diagrams/system-overview.drawio>`。

## 五个角色

### 1. 应用与 Rust Client

应用从 Python binding 或 Rust API 调用 `MooncakeClient`。Client 同时持有：

- 到 Master 的 tonic gRPC channel；
- 一个 `Arc<transfer_engine_ffi::TransferEngine>`；
- 用于 staging 或零拷贝的已注册 buffer；
- 可选 hot cache、LocalDisk backend 与后台 offload/promotion 任务。

字段总览在 `rust-repo/crates/mooncake-store-client/src/client/mod.rs`。Client
是唯一同时看见“业务 key/Replica 元数据”和“本地数据 buffer”的核心角色，
因此它把控制面结果转换成 TE 请求。

### 2. Rust Master

Master 保存“对象在哪里”和“空间怎样分配”的权威状态。主要职责包括：

- 注册 Segment 及其容量、endpoint 和 protocol；
- 为 Put 生成 Replica 放置计划；
- 在 Put 完成后把 Replica 标为可读；
- 为 Get 返回候选 Replica；
- 驱动删除、淘汰、promotion、后台 task 与租户配额；
- 在 HA 模式下记录 oplog、snapshot 并执行 leader/standby 切换。

入口是 `rust-repo/crates/mooncake-store-master/src/main.rs` 和
`rust-repo/crates/mooncake-store-master/src/service/mod.rs`。Master 的进程
内没有用户对象的传输 buffer，所以它不应出现在 RDMA payload 路径中。

### 3. Store Node

Store Node 是提供容量的 Client 实例，而不是另一套 C++ Store 服务。它创建
并注册一块长生命周期内存作为 Segment，并可挂载 Rust LocalStorageBackend。
三节点教程中的进程装配位于
`rust-repo/tools/rdma-multinode/store-node.py`。

Store Node A/B/C 分别发布独立 endpoint。Master 分配的是这些 endpoint 上的
offset/size；真正写入由发起请求的 Client 通过 TE 完成。

### 4. Transfer Engine

TE 是数据搬运 runtime。Rust 通过
`rust-repo/crates/transfer-engine-ffi/src/lib.rs` 使用它，native 实现在
`mooncake-transfer-engine/`。它负责：

- 注册本地内存并让 RNIC 可访问；
- 发布/打开 Segment；
- 把 `TransferRequest` 组成 batch；
- 选择 RDMA、TCP 或其他 transport；
- 报告每个 task 的 completion/error。

TE 不决定业务 Replica 数量，也不定义 key 的可见性；这些属于 Store/Master。

### 5. etcd

etcd 同时出现在两个不同语境：TE 可用它发现 peer/segment 元数据，Master HA
用它协调 leader 并持久化 oplog 等状态。两者都叫“metadata”，但内容和所有者
不同，阅读日志时应先辨认调用方。

## 一次 Put 的鸟瞰

```{mermaid}
sequenceDiagram
    participant A as Application
    participant C as Rust Client
    participant M as Master
    participant T as Transfer Engine
    participant S as Store A/B/C
    A->>C: put(key, bytes, replica=3)
    C->>M: put_start(key, size, policy)
    M-->>C: 3 个 Segment + offset
    C->>T: submit RDMA Write batch
    T->>S: 对象字节直写 3 个 Segment
    S-->>T: CQ completion
    T-->>C: batch terminal=Completed
    C->>M: put_end(key, replica status)
    M-->>C: 对象对 Get 可见
    C-->>A: success
```

关键不变量是：`put_start` 只预留位置，TE completion 只证明字节搬运完成，
`put_end` 才把控制面状态推进到可读。三者不可合并为一个模糊的“写入”。

## 常见误解

| 误解 | 正确模型 |
|---|---|
| Master 是数据代理 | Master 只处理控制信息；payload 由 TE 直达 Store Node |
| Store Node 是独立 C++ Store | Rust E2E Store Node 是挂载 Segment 和 Rust 本地层的 Rust Client 进程 |
| Replica 等于一整台机器 | Replica 是对象的一个存储位置；Segment 才属于具体 endpoint |
| RDMA 成功等于 Put 成功 | 还需要 Master finalize/可见性状态成功 |
| etcd 保存对象字节 | etcd 保存协调/元数据，不承载对象 payload |

## 自检问题

1. 为什么 Client 同时需要 Master channel 和 TransferEngine？
2. Store Node 的内存由谁拥有，Master 保存的是地址还是字节？
3. `put_start`、RDMA completion、`put_end` 分别证明什么？
4. 为什么 C++ Store 不属于当前 Rust Store 的运行依赖？

## 下一步

继续阅读 {doc}`control-plane-and-data-plane`，把顶层角色映射为实际请求阶段和
错误边界。
