# 核心概念

本节用 Rust 开发者熟悉的所有权和生命周期直觉解释 RDMA，并定义后续章节
统一使用的 Object、Slice、Replica、Segment 与多级缓存术语。

请按顺序阅读：RDMA 解释数据如何跨节点移动，Segment/Replica 解释数据放在哪里，
多级缓存解释数据为何在不同介质间迁移。

```{toctree}
:maxdepth: 1

rdma-for-rust-developers
object-replica-and-segment
multilevel-cache
```
