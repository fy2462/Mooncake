# 节点故障与恢复

## 前置章节

- “缓存驱逐与提升”“HA 与故障恢复”

## 本章目标

区分 Store、Master/etcd、RDMA link 与 replica owner 故障的恢复证据。

`rust-repo/tools/rdma-multinode/store-resilience-e2e.py` 保存每个场景的故障、恢复动作
和业务 evidence。排障应从 `first_failure` 进入相应场景。

| 场景 | 必要证据 |
| --- | --- |
| Store restart | restart + post-restart read |
| Master/etcd restart | master/etcd recovered + read |
| RDMA reconnect | link down、失败、restored、read |
| degraded read | initial owners≥2、remaining=1、bytes valid |
| watermark eviction | memory offloaded + SSD evicted |
| mixed stress | 多尺寸 operations + bytes identical |
| second standard | standard PASS + complete |

```{mermaid}
flowchart LR
  B[baseline] --> I[inject fault]
  I --> O[observe interruption]
  O --> R[restore]
  R --> V[read + byte validation]
  V --> E[record evidence]
```

两副本降级读不证明副本已经重建。C link 恢复后重启 Store 是为了重建 TE/Segment
生命周期。第二次标准门禁发现故障注入留下的污染。若 standard 未通过，resilience
只能是 BLOCKED，因为缺少合格 baseline。

## 自检问题

1. 降级读 PASS 是否说明副本数已恢复？
2. link 场景为何同时验证中断和恢复？
3. second standard 能发现什么污染？

## 下一步

进入“测试方法”和“实践实验”，运行并判读这些链路。
