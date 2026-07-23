# Put：从分配到提交

## 前置章节

- “Store Client”“Master”“一次传输的生命周期”

## 本章目标

追踪三副本 Put 的控制面、数据面与失败补偿，理解 PutEnd 是可见性提交点。

```{mermaid}
sequenceDiagram
  participant C as Client
  participant M as Master
  participant A as Store A
  participant B as Store B
  participant D as Store C
  C->>M: PutStart(key,size,replica_num=3)
  M->>M: reserve + allocate A/B/C
  M-->>C: ReplicaDescriptors
  par TE writes
    C->>A: RDMA Write
    C->>B: RDMA Write
    C->>D: RDMA Write
  end
  C->>C: wait all tasks terminal
  alt required writes succeeded
    C->>M: PutEnd
    M->>M: Complete + oplog
  else failure
    C->>M: PutRevoke / partial policy
    M->>M: release allocations
  end
```

从 `rust-repo/crates/mooncake-store-client/src/client/write.rs` 的 `Client::put` 开始：
参数与 config 转 proto；再到 Master `service/grpc_objects_put.rs` 看 PutStart 创建占位、
检查配额并调用 allocator。返回 Client 后，`write_to_replica` 对同节点走 memcpy，对远端
Memory/NoF 走 TE。最后 `finalize_put_for_key` 根据可靠/灵活模式决定 end 或 revoke，
Master PutEnd 校验 handle/status、推进 Complete 并记录 oplog。

三副本不能只看 descriptor 数量：目标应是三个不同 Segment、协议为 RDMA、handle
有效，三个写任务成功且最终 Complete。某次 write 失败时，要先终止/清理其他在途
batch，再 revoke；TE 清理解决传输资源，revoke 解决 Master/allocator 状态。

## 自检问题

1. PutStart 成功是否意味着 Get 可见？
2. 为什么 TE 清理与 PutRevoke 都需要？
3. 三个 descriptor 为何不足以证明三副本完成？

## 下一步

进入“Get：选择副本与读取”。
