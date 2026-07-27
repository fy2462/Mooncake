# Transfer Engine 总览

## 前置章节

- “HA 与故障恢复”
- “面向 Rust 开发者的 RDMA”

## 本章目标

理解 TE 如何把“本地 buffer 与远端 Segment 间移动字节”抽象成统一请求，以及元数据
发现、transport 与 Store 的边界。

## 稳定概念模型

TE 的输入由本地地址、远端 SegmentId/offset、长度和 Read/Write opcode 组成。它负责
发现远端 Segment 描述、选择已安装 transport、拆分/调度请求并汇报异步状态。

```{mermaid}
flowchart LR
  STORE[Rust Store Client] --> FFI[transfer-engine-ffi]
  FFI --> CABI[C ABI]
  CABI --> TE[C++ TransferEngine]
  TE --> META[transfer metadata]
  TE --> RDMA[RDMA transport]
  TE --> TCP[TCP transport]
  TE -. optional .-> TENT[TENT policy/scheduler]
```

TE 元数据记录 Segment endpoint、buffer 和 protocol，使发起端能把 Segment 名转换为
本地 handle。它与 Store Master 的对象目录不同：Master 知道 key 对应哪些 Replica，
TE 知道如何访问某个 Segment。

## 典型初始化

1. create engine 并连接 metadata service；
2. install `rdma`、`tcp` 等 transport；
3. discover NIC/GPU topology；
4. register local memory；
5. publish/open Segment；
6. 执行 batch transfer；
7. close、unregister、destroy。

native 入口为 `mooncake-transfer-engine/src/transfer_engine.cpp` 与
`mooncake-transfer-engine/src/transfer_engine_impl.cpp`。transport 公共接口位于
`mooncake-transfer-engine/include/transport/transport.h`。

## TENT 的位置

原生 TransferEngine 可以在设置 `MC_USE_TENT` 或 `MC_USE_TEV1` 后委托给
TENT，但当前 Rust Store 只打平 classic C ABI，并会显式拒绝该模式。现有顶层
`transfer_request_t` 没有贯通 priority、transport hint、policy、deadline 与
intent；这些能力只有在版本化 capability/request/status ABI 落地后才能计为
Rust Store 支持。它们仍属于 TE native 边界，不是 C++ Store 回归依赖。

## 自检问题

1. Store Master 元数据与 TE Segment 元数据分别回答什么问题？
2. install transport 与 open Segment 的职责有何不同？
3. TENT 是否改变 Rust Store 不依赖 C++ Store 的边界？

## 下一步

进入“Rust FFI 边界”，审视 opaque handle、裸指针和 Drop。
