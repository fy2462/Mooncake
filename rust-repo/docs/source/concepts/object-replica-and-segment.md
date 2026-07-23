# Object、Replica 与 Segment

## 前置章节

- “面向 Rust 开发者的 RDMA”

## 本章目标

区分对象、副本和承载副本的 Segment，并理解 Put 的状态机与 3/2/1 副本断言。

```{mermaid}
flowchart TB
  O[Object: tenant + key] --> R1[Replica 1]
  O --> R2[Replica 2]
  O --> R3[Replica 3]
  R1 --> S1[Segment A]
  R2 --> S2[Segment B]
  R3 --> S3[Segment C]
```

- **Object** 是用户以 tenant/key 访问的逻辑数据。
- **Replica** 是对象的一份物理副本，记录介质、Segment、offset、size、状态与 handle。
- **Segment** 是节点注册给 Master 的容量区域，包含基地址、大小、TE endpoint 和协议。

定义集中在 `rust-repo/crates/mooncake-store-core/src/types.rs`。Master allocator 选择
Segment 并分配 offset；Client 根据 ReplicaDescriptor 计算传输目标。

## 状态与介质

```text
Undefined → Allocating → Written → Complete
                         ↘ Failed
```

只有 `Complete` 且 handle 有效的副本可作为可靠读源。`Written` 不表示对象已提交；
Master 仍需处理 PutEnd。`refcnt` 保护 copy、move 或 promotion 的在途副本，busy 时不得
驱逐。克隆 descriptor 会重置 refcnt，因为快照不应继承在途计数。

| ReplicaType | 位置 | 典型路径 |
| --- | --- | --- |
| `Memory` | DRAM | TE RDMA/TCP |
| `Disk` | 文件或块设备 | 存储后端 |
| `LocalDisk` | 节点本地 SSD | offload + promotion |
| `NoFSsd` | NVMe-over-Fabrics | NoF/TE |
| `All` | 查询通配符 | 不是实际位置 |

## 3/2/1 副本

- **3 副本基线**：A/B/C 三个不同 Segment 都有 Complete RDMA Memory 副本。
- **2 副本降级起点**：先放到两个不同 owner，再使一个不可用。
- **1 存活副本读**：读选择跳过故障 owner，字节仍一致。
- **1 LocalDisk 副本**：驱逐后只保留磁盘副本，Get 再触发 promotion。

“E2E 对象副本”不是额外服务，而是测试对象被放到 Store Segment 的物理拷贝。
`ReplicateConfig.replica_num` 控制 Memory 副本数；默认值为 1，所以三个 Store 节点
并不意味着每个对象自动拥有三副本。

## 自检问题

1. 三个 Store 节点存在时，为什么对象仍可能只有一个副本？
2. Complete 但 `handle_valid=false` 的副本能否读取？
3. 为什么三副本还要检查三个不同 Segment？

## 下一步

进入“多级缓存”，理解 Memory 与 LocalDisk 副本如何迁移。
