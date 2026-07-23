# 三阶段学习路线

## 先记住一条主线

Mooncake Store 的一次对象操作同时经过两条路径：

- **控制面**向 Master 询问“对象在哪里、可以放在哪里、状态是否可见”。
- **数据面**让 Transfer Engine（TE）直接在 Client 与 Store Node 的内存之间搬运字节。

Master 不转发对象主体。后续阅读时只要不断问“这段代码在决定位置，还是在
搬运字节”，大部分模块就会自然归位。

## 阶段一：建立模型

按以下顺序阅读：

1. “系统总览”
2. “控制面与数据面”
3. “面向 Rust 开发者的 RDMA”
4. “Object、Replica 与 Segment”
5. “多级缓存”

这一阶段先不追函数。完成标志是你能够不看文档解释：

- 为什么 Master 知道 Replica，却不接触对象字节；
- 为什么 RDMA 前必须注册内存；
- Segment、Slice 和 Replica 分别描述什么；
- LocalDisk fallback 与重新 promotion 的区别。

## 阶段二：追踪路径

依次追踪 Put、Get 和 Remove 端到端章节。每一页都把阶段分成 Client、Master、
TE 和后台动作，并给出 `rg` 搜索词。

完成标志是你能在代码中找到：

- `MooncakeClient::put` 如何把 `put_start` 与数据写入连接起来；
- `put_end` 为什么必须晚于所有目标 Replica 的数据完成；
- `get` 如何选择 Replica，并在 Memory 与 LocalDisk 路径间分流；
- Remove 的元数据删除与物理空间回收为何不是同一个动作。

## 阶段三：进入源码和运行系统

先用 {doc}`../code-map/recommended-reading-order` 阅读关键文件，再完成：

1. “追踪一次请求”：非特权环境下追踪一次请求。
2. “运行三节点 E2E”：运行三节点 RXE/TE/Store 验收。
3. “调试 RDMA”：根据 device、GID、TE 和 Store 证据排错。

完成标志不是“读完所有文件”，而是能从一条失败日志逆向定位到控制面、
FFI 或 RDMA transport 的具体边界。

## 两条专题捷径

### 只想先学 Master/HA

阅读 Master、metadata/oplog、HA 三章，然后运行：

```bash
cd /home/fy2462/Mooncake/rust-repo
CARGO_BUILD_JOBS=5 cargo test -p mooncake-store-master --lib
```

重点搜索 `MasterServiceImpl`、`OpLogManager`、`HotStandbyService` 和
`MasterServiceSupervisor`。

### 只想先学 TE/RDMA

阅读 RDMA 基础、FFI 边界、transport/RDMA 和传输生命周期。重点搜索：

```bash
cd /home/fy2462/Mooncake
rg -n 'register_local_memory|open_segment|submit_transfer|get_transfer_status' \
  rust-repo/crates/transfer-engine-ffi
rg -n 'RdmaContext|RdmaEndpoint|poll_cq|post_send' \
  mooncake-transfer-engine
```
