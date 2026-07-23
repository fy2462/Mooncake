# 实验：RDMA 分层排障

## 前置章节

- “实验：运行三节点 Soft-RoCE E2E”

## 本章目标

按照 host→verbs→TE→Store 的顺序定位问题，避免同时修改多个层次。

## 第一层：设备与 GID

```bash
rdma link
ibv_devices
ibv_devinfo -v -d mc-rdma-rxe-a
ip -br link show mc-rdma-br
ip -br addr show mc-rdma-net-a
```

确认 A/B/C 都是 ACTIVE/PORT_ACTIVE，GID 非空，veth 与 bridge 为 UP。再查看
`host-rdma.json`，把 device、address 和 GID 与真实输出逐项对应。设备缺失属于 host
setup 问题，不要先改 TE。

## 第二层：verbs

```bash
rust-repo/tools/rdma-multinode/run.sh verbs
sed -n '1,160p' \
  /home/fy2462/workspace/tmp/mooncake/rdma-multinode/verbs.result
```

`ib_write_bw` 失败时检查 GID index、port state、MTU 和 bridge 连通；此时 TE 应判为
BLOCKED。

## 第三层：TE

```bash
rust-repo/tools/rdma-multinode/run.sh te
rg -n 'protocol|Stage|compare|timeout|failed' \
  /home/fy2462/workspace/tmp/mooncake/rdma-multinode/te-*.log
```

先找到首个失败方向，再区分 target 是否启动、open Segment 是否成功、是否明确报告
RDMA、Write/Read 是否都执行、compare 是否 OK。出现 TCP 即协议门禁失败。

## 第四层：Store

```bash
rg -n 'error|FAILED|timeout|replica|offload|promotion' \
  /home/fy2462/workspace/tmp/mooncake/rdma-multinode/{master,store-a,store-b,store-c,store-client}-*.log
```

检查三个 ready 文件的 device/protocol/backend 与 `cpp_store_loaded=false`，再读 Store
JSON 的顶层 status 和 evidence。副本不足查 allocator/Segment；字节错误查 TE 和源/目标
长度；fallback 超时查水位、storage stats 与任务通知。

```{mermaid}
flowchart TD
  H{A/B/C active + GID?} -->|no| HF[host setup]
  H -->|yes| V{ib_write_bw PASS?}
  V -->|no| VF[verbs/network]
  V -->|yes| T{TE six directions?}
  T -->|no| TF[segment/QP/CQ/log]
  T -->|yes| S{Store standard?}
  S -->|no| SF[Master/replica/cache]
  S -->|yes| R[resilience scenarios]
```

## 自检问题

1. `rdma link` 正常但 verbs 失败时应先查什么？
2. 五个 TE 方向通过、一个失败，能否运行 Store 门禁？
3. Store 返回两个而非三个副本时应优先查 TE 还是 allocator/Segment？

## 下一步

回到“源码地图”，选择失败层的推荐入口进行第二轮源码学习。
