# 如何阅读测试证据

## 前置章节

- “单节点测试方法”
- “多节点测试方法”

## 本章目标

不要只看命令退出码或日志最后一行。本章给出一套固定判读顺序，用结果文件回答：
哪个层次通过、通过依据是什么、下一层是否有资格运行。

## 结果文件

默认位于 `/home/fy2462/workspace/tmp/mooncake/rdma-multinode`：

| 文件 | 必要证据 |
| --- | --- |
| `verbs.result` | 首行为 `PASS`，并记录 `tool=ib_write_bw` |
| `te.result` | 顶层 `status=PASS`、`protocol=rdma`，六方向均 PASS 且 `compare=OK` |
| `store-standard.result` | 顶层 JSON `status` 为 PASS，并具有三副本和多级缓存证据 |
| `store-resilience.result` | 七个必需场景全部 PASS，`first_failure` 为空 |
| `open-rdma.result` | 仅代表 mock 测试结果，不属于真实 RDMA 产品证据 |
| `report.md` | 汇总上述证据，便于评审；不能脱离原始结果独立使用 |

## PASS、FAIL 与 BLOCKED

- **PASS**：该层的操作和强制证据均满足。例如 TE 不仅退出成功，还必须明确报告
  RDMA、六个方向和字节一致。
- **FAIL**：该层已经获得运行条件，但行为或证据不满足。例如传输超时、只创建两个
  完整副本，或回读字节不一致。
- **BLOCKED**：该层没有资格产生产品结论。例如 verbs 未通过时 TE 被阻塞，或
  Store 标准门禁失败时韧性门禁被阻塞。

`BLOCKED` 不是 PASS，也不应被写成“测试大致可用”；它要求修复前置环境或前一层
产品问题后重新运行。

## 推荐判读顺序

```{mermaid}
flowchart TD
  V{verbs PASS?} -->|否| VB[TE: BLOCKED]
  V -->|是| T{TE RDMA 六方向 PASS?}
  T -->|否| SB[Store: BLOCKED]
  T -->|是| S{Store standard PASS?}
  S -->|否| RB[resilience: BLOCKED]
  S -->|是| R{七个 resilience 场景 PASS?}
  R -->|否| F[产品验收 FAIL]
  R -->|是| P[产品 E2E PASS]
  O[Open-RDMA mock] --> X[mock-only 附加证据]
```

1. 先读 `verbs.result`，确认基础数据面可用。
2. 再读 `te.result`，逐项核对协议、方向和字节比较。
3. 再读标准 Store JSON，不接受嵌套字段中的偶然 `"status":"PASS"` 代替顶层状态。
4. 最后读韧性 JSON，确认场景集合完整且每项具有规定 evidence。
5. 单独记录 Open-RDMA mock，不把它合并成产品 RDMA 结论。

## 多级缓存的有效证据

只看到 Put/Get 成功不足以证明多级缓存工作。标准验收至少要求同一个对象经历：

```text
Memory Complete → 水位压力 → LocalDisk Complete 且无 Memory Complete
                → Get → Memory Complete（promotion）→ 字节摘要一致
```

`store-standard.result` 应记录 fallback 的 `replica_type=LocalDisk` 和副本数，以及
promotion 后的内存副本数。韧性门禁还要证明内存 offload 与 SSD eviction 都真实
发生，并满足 `0 < low_ratio < high_ratio <= 1`。

## 失败时从哪里开始

| 首个失败层 | 优先检查 |
| --- | --- |
| host setup / verbs | 内核模块、RXE device、GID、veth/bridge manifest、sudo 权限 |
| TE | 对应方向的 initiator/target 日志、协议行、超时、compare 结果 |
| Store standard | Master 日志、三个 Store 日志、客户端 JSON、副本描述 |
| Store resilience | `first_failure`、对应场景 evidence、故障注入前后的节点状态 |
| report | 原始 result 是否缺失或格式不满足严格合同 |

修改代码前先定位“第一个失败层”。下游 BLOCKED 往往只是上游失败的结果，不是第二个
独立 bug。

## 自检问题

1. `open-rdma.result` 为 PASS、`te.result` 为 BLOCKED 时，最终能否宣称 RDMA PASS？
2. Store 客户端成功读回对象，为什么还不足以证明三副本和多级缓存通过？
3. 韧性结果顶层 PASS，但缺少 `rdma_reconnect` 场景，应如何判定？

## 下一步

回到“实践实验”，按实验清单亲自运行单节点与多节点测试；遇到失败时，沿本章的
证据链回溯到第一个失败层。
