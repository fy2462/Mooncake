# Remove 与覆盖写

## 前置章节

- “Get：选择副本与读取”

## 本章目标

区分目录删除、allocator 回收、本地缓存失效与物理清理，并理解 force。

`rust-repo/crates/mooncake-store-client/src/client/remove.rs` 的 `Client::remove` 发送
tenant/force。Master 在 `service/grpc_objects_query.rs` 校验对象，撤销可见性、释放
replica/allocator 资源并记录 oplog；RPC 成功后 Client 使本地 hot cache 失效。

```{mermaid}
sequenceDiagram
  participant C as Client
  participant M as Master
  participant S as Store/local cache
  C->>M: Remove(key,force)
  M->>M: validate + remove metadata
  M->>M: release replicas + oplog
  M-->>C: success
  C->>S: invalidate hot cache
  Note over M,S: physical cleanup may continue
```

`force=false` 保留 pin/busy 等保护；`force=true` 不能使仍在执行的 RDMA 突然安全，
仍需协调 refcnt 和 handle。`remove_all` 还清理 attached local storage；batch remove 的
每项状态与输入对齐。

覆盖写应把新副本提交与旧副本退役视为受控阶段，不能先暴露半写数据，也不能泄漏旧
副本。E2E overwrite-delete 同时检查最新字节、删除后不可读与状态。

## 自检问题

1. Remove 成功后为什么仍可能有后台物理清理？
2. force 是否允许释放 RNIC 正在使用的 buffer？
3. 覆盖写为何不等同于先 remove 再 put？

## 下一步

进入“缓存驱逐与提升”。
