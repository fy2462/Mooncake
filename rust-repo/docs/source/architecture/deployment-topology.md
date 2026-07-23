# 部署拓扑：从进程到三节点 RXE

## 前置章节

- {doc}`system-overview`
- {doc}`control-plane-and-data-plane`

## 本章目标

把逻辑组件映射到真实进程、端口、容器和 RXE device，并明确软件三节点测试
与物理多机部署之间的相同点和不同点。

## 逻辑生产拓扑

一个最小多节点系统包含：

- 一个可用的 Master leader（HA 时另有 standby）；
- 多个提供 Segment 的 Store Node；
- 一个或多个调用 Store API 的业务 Client；
- 每个参与数据面的进程各自初始化 TE；
- etcd 或其他配置的 metadata/HA backend。

业务 Client 和 Store Node 可以位于不同物理机。Master RPC 可走普通 TCP；
对象 payload 通过 TE 选择的 RDMA/TCP transport。

## 本地三节点 E2E 如何映射

`rust-repo/tools/rdma-multinode/compose.yaml` 启动：

| 服务 | 数量 | 作用 |
|---|---:|---|
| etcd | 1 | Master HA/oplog 与 TE metadata 测试依赖 |
| te-node-a/b/c | 3 | 六方向直接 TE 数据面验证 |
| rust-master | 1 | Rust Master leader |
| store-node-a/b/c | 3 | 三个独立 Segment 与 LocalDisk backend |
| store-test-client | 1 | 发起标准与韧性场景 |

host setup 创建 `mc-rdma-br`，以及三组 veth/RXE：

```{mermaid}
flowchart TB
    BR[mc-rdma-br 软件 bridge]
    A[mc-rdma-net-a<br/>10.90.0.1<br/>mc-rdma-rxe-a]
    B[mc-rdma-net-b<br/>10.90.0.2<br/>mc-rdma-rxe-b]
    C[mc-rdma-net-c<br/>10.90.0.3<br/>mc-rdma-rxe-c]
    BR --- A
    BR --- B
    BR --- C
    A -->|A↔B| B
    B -->|B↔C| C
    C -->|C↔A| A
```

内核报告 RDMA namespace 为 shared 时，容器使用 host network 并看到这些
host-owned device。每个 Client/Store 仍显式选择自己的 device/GID，因此测试
的是三个独立 TE endpoint，而不是一张 device 上的三个进程名字。

## 端口与 Segment 名

测试中的 Store endpoint 是 `127.0.0.1:12401`、`:12402`、`:12403`。在同一
host 上用不同端口区分逻辑节点；物理多机通常由不同 IP 加端口区分。Master
返回的 `segment_name` 必须与 TE 发布/打开时使用的 endpoint 一致。

## 本地模拟能证明什么

它可以证明：

- verbs 确实在 RXE 上完成，而不是 TCP fallback；
- 三个 TE endpoint 的六个有向路径均能传输并比较字节；
- Master 能分配 3/2/1 Replica 到三个 Store endpoint；
- link down、进程重启、Master/etcd 重启后行为符合预期；
- 最终 manifest-owned 资源被清理。

它不能证明真实硬件的带宽、PCIe/NIC 拓扑、RoCE PFC/ECN 配置、跨交换机
路由或生产规模拥塞行为。RXE 是功能与编排验收，不是硬件性能基准。

## 自检问题

1. 为什么 Compose 有三个 TE service 和三个 Store service？
2. host network 是否意味着三个 Store 共用同一个逻辑 Segment？
3. 六方向 TE gate 比只测 A→B 多证明了什么？
4. RXE PASS 为什么不能推出物理 RNIC 性能达标？

## 下一步

进入“概念基础”中的“面向 Rust 开发者的 RDMA”，学习 device/GID 背后的 MR、
QP、WR 与 CQ，再回来看这张拓扑会更具体。
