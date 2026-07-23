# Store Node

## 前置章节

- “Store Client”
- “Master”

## 本章目标

理解当前 E2E Store Node 的真实组成，避免把它误认为 Master、纯 TE 容器或 C++ Store。

入口 `rust-repo/tools/rdma-multinode/store-node.py` 通过 PyO3 `_mooncake_store` 创建
Rust `MooncakeClient`，配置节点地址、RDMA device、Master、metadata server、全局
Segment 和本地 buffer。随后它：

1. attach `RustFilePerKey` local storage；
2. mount LocalDisk Segment，启动 offload server；
3. 发布 ready JSON；
4. 循环 health、offload、promotion 与 SSD 水位回收；
5. 处理测试命令并发布 stats；
6. 收到信号后关闭 Client。

```{mermaid}
flowchart TB
  PY[store-node.py] --> PYO3[_mooncake_store]
  PYO3 --> C[Rust Client]
  C --> TE[Transfer Engine]
  C --> MEM[registered Memory]
  C --> D[RustFilePerKey LocalDisk]
  C --> M[Master gRPC]
```

`store-node-a/b/c` 是 Store 产品路径的测试节点，每个自身加载 TE；`te-node-a/b/c`
只为 `rdma_transport_test` 隔离验证六方向矩阵。两组数量相同但职责不同。

ready 文件和 `/proc/*/maps` 检查共同证明 `cpp_store_loaded=false`。Rust Store 不借用
C++ Store 补齐能力，native 依赖仅在 TE/TENT 边界。`store-node.py` 是验收装配器，
不是额外 production crate；其他部署仍须保留 Segment 注册、后台循环和有序关闭。

## 自检问题

1. Store Node 与 Master 分别拥有什么状态？
2. TE 独立门禁通过后，Store 节点为何仍需加载 TE？
3. `cpp_store_loaded=false` 证明什么？

## 下一步

进入“元数据、快照与 oplog”。
