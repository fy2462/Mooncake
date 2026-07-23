# Mooncake Rust Store 与 Transfer Engine 学习指南

这套文档面向熟悉 Rust、但尚未系统学习 RDMA 的开发者。目标不是罗列
文件，而是先建立 Store、Master 与 Transfer Engine 的整体模型，再沿真实
Put/Get/Remove 调用链逐步深入源码。

:::{admonition} 推荐方式
:class: tip
第一次阅读请从“开始学习”进入并按三阶段路线前进。已经熟悉 Mooncake 的
读者可以直接打开“源码地图”或“端到端链路”。
:::

```{toctree}
:maxdepth: 2
:caption: 学习路径

getting-started/index
architecture/index
concepts/index
store/index
transfer-engine/index
walkthroughs/index
code-map/index
labs/index
```

## 你最终应该获得什么

- 能画出 Client、Master、Store Node、TE 与 etcd 的控制面和数据面关系。
- 能解释注册内存、Segment、Replica、批量传输和 RDMA completion。
- 能从一次 Put/Get/Remove 进入对应 Rust 与 C++ TE 源码。
- 能解释 3/2/1 副本、多级缓存、HA 和节点故障恢复的实际实现。
- 能运行三节点 RXE E2E，并根据结果文件与日志定位问题。
