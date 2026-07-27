# Rust FFI 边界

## 前置章节

- “Transfer Engine 总览”

## 本章目标

能逐项说明 `TransferEngine`、`SegmentId`、legacy `BatchId`、Store 使用的
`OwnedBatchId` 和 `TransferRequest` 的所有权，并识别 safe wrapper 无法替调用者
证明的内存生命周期。

## 调用层次

`rust-repo/crates/transfer-engine-ffi/src/lib.rs` 用 `NonNull<c_void>` 保存 opaque native
handle。bindgen 输出仅在私有 `ffi` 模块使用，产品代码通过 Rust 类型调用。创建返回
null 会转成 `NullHandle`，整数返回码转成 `TransferEngineError`。

| Rust 类型 | 表示 | 是否拥有 native 资源 |
| --- | --- | --- |
| `TransferEngine` | 引擎 handle | 是；Drop destroy |
| `SegmentId` | 已打开远端 Segment | 否；需显式 close |
| `BatchId` | 范围外 caller 的 legacy copyable batch token | 否；需显式 free |
| `OwnedBatchId` | Store 使用的 engine-bound batch allocation | 是；需显式 free，成功后 token 失效 |
| `TransferRequest` | 指针与传输描述 | 否；不拥有 source buffer |
| `TransferStatus` | 状态快照 | 否 |

`unsafe impl Send/Sync` 依赖 C++ 引擎内部同步；Rust wrapper 从不解引用 handle。这是需要
native 实现持续保证的契约，不是 `NonNull` 自动带来的线程安全。

## 内存注册的 unsafe 前提

`register_local_memory` 是 unsafe，因为编译器无法验证：指针至少有 length 字节、地址
适合声明的 location、内存在 unregister 前有效，且所有在途 batch 已终止。注销也
必须使用原始注册地址。

```{mermaid}
flowchart TD
  O[buffer owner alive] --> R[register]
  R --> S[submit]
  S --> C{all tasks terminal?}
  C -->|no| S
  C -->|yes| F[free batch]
  F --> U[unregister]
  U --> D[drop buffer]
```

## 字符串和切片转换

Rust 先用 `CString` 拒绝内部 NUL，再在 FFI 调用期间借用指针。批量 request 会转换成
C layout 数组；native API 必须在调用返回前复制描述，或 Rust 必须保证数组活到 native
不再使用。source payload 的生命周期则始终跨越异步完成。

## Drop 不替代有序清理

`TransferEngine::drop` 最终销毁 native 引擎，但正常路径仍应先完成 batch、free、close
Segment、unregister memory。把所有清理都推给 Drop 会丢失可报告错误，也可能在仍有
外部 buffer/任务时破坏顺序。

## TENT wrapper

`rust-repo/crates/transfer-engine-ffi/src/tent.rs` 的 `TentEngine` 同样拥有 opaque
handle。`TentTransferRequest` 的 Send/Sync 前提仍是调用者保持 source 已注册且活到
终态；V2 request 还要让 policy CString 在 native 调用期间有效。

## 自检问题

1. `NonNull` 能证明指向的 C++ 对象仍存活吗？
2. `OwnedBatchId` 为什么仍要求 reaper 显式证明 quiescence 后 free，而不能在
   普通 Drop 中直接释放？
3. `TransferRequest: Send` 为什么不代表 source buffer 可提前释放？

## 下一步

进入“Transport 与 RDMA 实现”，把抽象请求映射到 MR、endpoint、WR 和 CQ。
