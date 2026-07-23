# 整体架构

本节建立顶层心智模型：谁负责元数据，谁保存对象字节，谁真正执行跨节点
传输，以及控制面和数据面为何必须分开理解。

核心架构图将同时提供可编辑 Draw.io 源文件和浏览器可显示的 SVG。

```{toctree}
:maxdepth: 1

system-overview
control-plane-and-data-plane
deployment-topology
```

推荐严格按顺序阅读：总览先回答“有哪些角色”，控制面/数据面再回答“请求
怎样穿过角色”，部署拓扑最后解释“这些角色如何落到进程、容器和网卡”。
