# Store Client

## 前置章节

- “Object、Replica 与 Segment”
- “多级缓存”

## 本章目标

理解 `Client` 如何协调 Master RPC、Transfer Engine、本地 buffer 与后台存储任务，
并找到 Put/Get/Delete 的源码入口。

## Client 是编排者

Client 不保存全局目录，也不实现 verbs。它向 Master 请求放置元数据，再把数据移动
交给 `transfer-engine-ffi`。

```{mermaid}
flowchart LR
  API[Client API] --> RPC[Master tonic client]
  API --> BUF[local buffer]
  API --> XFER[transfer helpers]
  XFER --> FFI[transfer-engine-ffi]
  BG[background workers] --> RPC
  BG --> CACHE[hot cache / LocalDisk]
```

## 创建与资源所有权

从 `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs` 的 `Client::create`
开始。调用链归一化配置、连接 Master、创建 TE、分配 buffer、注册内存、打开并挂载
Segment，最后组装 Client。失败路径要逆序释放已经取得的资源；TE 注册记录不能延长
Rust buffer 的真实生命周期。

`create_with_master_candidates*` 支持 HA 地址候选，`create_for_tenant` 注入 tenant。
`lifecycle_state.rs` 保存需要显式停止的服务和任务。

## 写、读、删入口

- `write.rs::Client::put`：PutStart 取得计划，写全部目标，PutEnd 提交；失败则 revoke。
- `read.rs::Client::get`：查询并选择 Complete 副本，必要时读取 LocalDisk。
- `remove.rs::Client::remove`：删除 Master 元数据并使本地缓存/存储条目失效。

第一遍先读单对象路径，再比较 `batches.rs`、`write_batch.rs` 和 `read_batch.rs` 的
部分失败、容量与每 key 状态组织。

`replica_selection.rs` 优先本地 Memory/NoF，再选择可用远端副本，最后磁盘 fallback。
可选 scorer 会偏好 RDMA，并保留 Master 顺序作为稳定 tie-break。

## 后台任务

`background.rs` 启动 health、storage 和 task 三类循环。storage worker 调用
`storage_local.rs` 的 `offload_objects`、`promote_objects` 和磁盘水位回收。
`ClientBackgroundHandle::shutdown` 先让循环退出再释放共享资源。

## 推荐阅读顺序

1. `lifecycle.rs::Client::create_with_config`
2. `lifecycle.rs::Client::create_with_connected_master`
3. `write.rs::Client::put` 与 `read.rs::Client::get`
4. `replica_selection.rs::ReplicaSelectionPolicy`
5. `storage_local.rs::offload_objects` 与 `promote_objects`

## 自检问题

1. Client 为什么不能只取得地址后绕开 PutEnd？
2. TE 注册内存后，Rust buffer owner 为什么仍要存活？
3. 后台任务 shutdown 为什么属于资源正确性？

## 下一步

进入“Master”，理解副本计划、提交与后台回收。
