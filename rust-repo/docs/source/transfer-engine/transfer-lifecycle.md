# 一次传输的生命周期

## 前置章节

- “Transport 与 RDMA 实现”

## 本章目标

能从 Rust 调用点追踪到 terminal status，并在每个失败点说明应释放哪些资源。

## 标准 batch 时序

```{mermaid}
sequenceDiagram
  participant C as Store Client
  participant F as Rust FFI
  participant T as Native TE
  C->>F: open_segment(name)
  F->>T: openSegment
  T-->>F: SegmentId
  C->>F: allocate_batch_id(N)
  F->>T: allocateBatchID
  C->>F: submit_transfer(batch, requests)
  F->>T: submitTransfer
  loop each task until terminal
    C->>F: get_transfer_status(batch, task_id)
    F->>T: getTransferStatus
    T-->>C: Waiting/Pending/Completed/Failed
  end
  C->>F: free_batch_id
  C->>F: close_segment
```

推荐直接对照 `rust-repo/crates/transfer-engine-ffi/src/lib.rs` 的 `open_segment`、
`allocate_batch_id`、`submit_transfer`、`get_transfer_status`、`free_batch_id` 和
`close_segment`。

## Read 与 Write 的地址含义

`TransferRequest.source` 始终是本进程 buffer 地址。Write 表示 local source → remote
target；Read 表示 remote target → local source。`target_offset` 是远端 Segment 地址
空间中的目标偏移/地址语义，调用者还必须保证 length 不越过两侧有效范围。

## 状态与清理

每个 request 的 task_id 等于它在提交切片中的索引。只有全部 task 到达 Completed 或
Failed 等终态后才能 free batch。提交部分失败也要查询/终止已接受任务，再释放 batch；
不能因为一个 task 失败就让其余在途任务引用已释放 buffer。

失败清理按取得顺序逆序执行：

```text
requests terminal → free batch → close remote Segment
                  → unregister local memory → drop buffer → destroy engine
```

Store 的 Put 还要在 TE 失败后调用 PutRevoke；TE 清理解决传输资源，Master revoke
解决对象和 allocator 状态，二者缺一不可。

## TENT 请求

TENT 路径把普通请求加上 priority、transport hint、policy name、deadline 和 intent。
兼容选项使用 legacy ABI，扩展字段使用 V2 ABI；提交后的 batch/status/cleanup 原则不变。

```{mermaid}
flowchart LR
  R[TransferRequest] --> O[TentRequestOptions]
  O --> A{legacy-compatible?}
  A -->|yes| L[legacy request ABI]
  A -->|no| V[V2 request ABI]
  L --> S[TENT submit/status]
  V --> S
```

## 自检问题

1. batch 中 task 0 失败时，为何不能立即 drop 所有 buffer？
2. TE 失败后为什么还需要 Store PutRevoke？
3. TENT V2 增加调度字段后，哪条内存安全前提没有改变？

## 下一步

进入端到端请求走读，用 Put/Get/Delete 把 Client、Master 与 TE 三层连接起来。
