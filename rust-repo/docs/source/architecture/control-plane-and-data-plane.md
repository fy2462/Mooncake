# 控制面与数据面

## 前置章节

- {doc}`system-overview`

## 本章目标

能把任意日志或函数归类为控制面、数据面或两者的衔接点，并理解每个边界的
超时、重试和清理责任。

## 控制面：决定“在哪里、何时可见”

控制面以 gRPC 与 Master 状态为中心：

```text
Client API
  → tonic MasterServiceClient
  → generated MasterService trait
  → MasterServiceImpl::*_impl
  → allocator / object metadata / task / oplog
```

Client 的 proto module 在 `rust-repo/crates/mooncake-store-client/src/lib.rs`
通过 `tonic::include_proto!` 引入。服务端适配器在
`rust-repo/crates/mooncake-store-master/src/service/grpc_trait.rs`，它把 wire
request 转给按职责拆分的业务实现。

控制面的典型值是 key、size、tenant、Replica policy、Segment ID、offset、
status 和 task ID。即使出现 `base`/`offset`，它们也是描述数据位置的元数据，
不是对象字节本身。

## 数据面：执行“把这些字节搬过去”

数据面从 Client 拿到 buffer 和 Master 返回的 ReplicaDescriptor 后开始：

```text
MooncakeClient
  → transfer-engine-ffi::TransferEngine
  → generated C ABI
  → C++ TransferEngine
  → RdmaTransport / TcpTransport / TENT
  → Store Node Segment
```

数据面的典型值是本地地址、远端 SegmentId、远端 offset、length、opcode、
BatchId、task ID 和 completion status。

`rust-repo/crates/transfer-engine-ffi/src/transfer.rs` 的 `TransferRequest` 是非常
好的边界观察点：到这里，业务 key 已经被解析成可执行的数据搬运描述。

## 衔接点才是最容易出错的地方

Put 的衔接点在“Master 分配结果 → TE write request”和“TE completion →
Master finalize”。Get 的衔接点在“Master Replica list → Client 选择 → TE read
或 LocalDisk read”。错误责任如下：

| 失败位置 | 已发生的状态 | 主要责任 |
|---|---|---|
| `put_start` 前 | 尚未分配 | Client 返回控制面错误 |
| 分配后、传输前 | 有临时 Replica/空间 | Client/Master revoke 或超时回收 |
| 部分 TE task 失败 | 可能部分写入 | 不得把对象标为 Complete；清理 batch 和 Replica |
| TE 完成、`put_end` 失败 | 字节存在但未可靠发布 | 不能仅凭数据面成功向用户报告成功 |
| Get 的首选 Replica 失败 | 元数据可能仍有其他候选 | 依据选择/重试策略换候选或返回错误 |
| LocalDisk read 成功 | 字节可返回但 Memory 不一定存在 | 可触发独立 promotion 流程 |

## 两种 metadata 不要混淆

1. **Store metadata**：对象、Replica、Segment、状态和 task，由 Master 负责。
2. **TE metadata**：peer endpoint、transport 可达性和 Segment publication，由
   TE runtime 用于建连。

它们可能都借助 etcd，但数据模型和生命周期不同。调试时看到 etcd 错误，先
确认是 Master HA/oplog 还是 TE peer discovery。

## 用代码搜索验证分类

```bash
cd /home/fy2462/Mooncake

# 控制面
rg -n 'put_start|put_end|get_replica_list|remove' \
  rust-repo/crates/mooncake-store-client/src/client \
  rust-repo/crates/mooncake-store-master/src/service

# 数据面
rg -n 'TransferRequest|submit_transfer|get_transfer_status' \
  rust-repo/crates/mooncake-store-client \
  rust-repo/crates/transfer-engine-ffi
```

## 自检问题

1. `ReplicaDescriptor` 属于哪一面？它何时被转换成 `TransferRequest`？
2. 为什么 TE 不应该自己把对象标为 Complete？
3. 数据面成功、控制面 finalize 失败时可以向用户返回成功吗？
4. TE metadata 与 Master metadata 的 owner 分别是谁？

## 下一步

阅读 {doc}`deployment-topology`，理解单机 host-network Compose 如何模拟三个
逻辑 RDMA 节点，以及哪些结论可以推广到物理多机。
