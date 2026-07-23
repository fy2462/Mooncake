# 多节点测试方法

## 前置章节

- “单节点测试方法”中的测试分层与编排契约
- “部署拓扑”中的三 TE、三 Store、Master 与 etcd
- Transfer Engine 数据通路和多级缓存章节

## 本章目标

使用 `rust-repo/tools/rdma-multinode/compose.yaml` 建立单机模拟的多节点环境，按
依赖顺序验证 verbs、Transfer Engine 和 Rust Store。这里的“多节点”是多个隔离
进程/容器和三个 Soft-RoCE 设备，不等同于三台物理服务器；它验证协议和编排，不能
替代真实交换机、RNIC 和跨主机性能测试。

## 拓扑与职责

Compose 启动以下服务：

| 服务 | 数量 | 作用 |
| --- | ---: | --- |
| etcd | 1 | 元数据服务与 HA 协调基础设施 |
| Rust Master | 1 | 副本分配、对象元数据和生命周期控制 |
| TE node | 3 | 在 A/B/C 三个 RXE 设备间验证六个有向传输 |
| Store node | 3 | 提供三个独立 Segment、内存容量和本地磁盘层 |
| test client | 1 | 发起 Put/Get/Delete、压力和故障场景 |

Store 节点“依赖 TE”不意味着 Store 与 TE 必须拆成两组同数量的生产服务。这里的
三个 `te-node-*` 是隔离验证 TE 传输矩阵的测试容器；三个 `store-node-*` 进程自身
也加载 TE，通过各自的 RXE 设备暴露数据面。两组角色的测试目的不同。

```{mermaid}
flowchart TB
  C[store-test-client] --> M[Rust Master]
  M --> E[etcd]
  C --> A[Store A / TE]
  C --> B[Store B / TE]
  C --> D[Store C / TE]
  A <-->|RDMA| B
  B <-->|RDMA| D
  D <-->|RDMA| A
```

## 前置检查

完整流程需要 Docker、Docker Compose、`rdma` 工具、可加载的 `rdma_rxe` 模块，
以及创建指定 bridge/veth/RXE 设备的 sudo 权限。脚本在需要提权时会由 sudo 正常
提示输入密码；不要把密码写入脚本、环境变量或日志。

```bash
command -v docker
docker compose version
command -v rdma
sudo modprobe rdma_rxe
```

构建产物和长期保留的日志默认写入
`/home/fy2462/workspace/tmp/mooncake/rdma-multinode`；容器运行时数据使用 `/tmp`。
Cargo 构建仍使用 `rust-repo/target`，并发数由构建脚本固定或通过
`CARGO_BUILD_JOBS=5` 传入。

## 一键标准验收

从仓库根目录运行：

```bash
export CARGO_BUILD_JOBS=5
rust-repo/tools/rdma-multinode/run.sh all
```

`all` 按以下依赖链执行，并在退出时清理由本套件创建的 Compose 服务和主机 RDMA
设备：

```text
preflight → host-rdma-setup → build → compose-up
          → verbs → te → store-standard → store-resilience
          → open-rdma → report → compose-down → host-rdma-cleanup
```

不要跳过前置门禁直接解释 Store 结果：Store 标准门禁要求 `te.result` 同时包含
`status=PASS` 和 `protocol=rdma`。

## 分阶段诊断

完整流程耗时较长时，可以逐层执行，以定位首次失败：

```bash
rust-repo/tools/rdma-multinode/run.sh preflight
rust-repo/tools/rdma-multinode/run.sh host-rdma-setup
rust-repo/tools/rdma-multinode/run.sh build
rust-repo/tools/rdma-multinode/run.sh compose-up
rust-repo/tools/rdma-multinode/run.sh verbs
rust-repo/tools/rdma-multinode/run.sh te
rust-repo/tools/rdma-multinode/run.sh store-standard
rust-repo/tools/rdma-multinode/run.sh store-resilience
rust-repo/tools/rdma-multinode/run.sh report
rust-repo/tools/rdma-multinode/run.sh cleanup
```

阶段之间有状态依赖，因此诊断时仍应保持顺序。`cleanup` 只清理由 manifest 记录、
名称以 `mc-rdma-*` 标识的资源。

## 各门禁验证什么

### 1. verbs 门禁

在 RXE A/B 间运行 `ib_write_bw`。它证明内核 RDMA/Soft-RoCE、设备、GID 和基础
verbs 路径可工作，但还没有运行 Transfer Engine。

### 2. TE 门禁

依次验证 A→B、B→A、B→C、C→B、C→A、A→C 六个方向。每个方向必须出现：

- 远端 Segment 协议为 `rdma`；
- Write 和 Read 两个阶段均执行；
- 字节比较为 `OK`；
- 日志中没有 TCP 回退、超时或 transfer failed。

这就是后续合入 master 前的 TE 双端/多节点前置条件。

### 3. Store 标准门禁

启动一个 Rust Master、三个 Rust Store 节点和测试客户端，验证：

- Put/Get/Delete 与覆盖写；
- 小对象、大对象、并发和跨 Segment 访问；
- 三个完整且互异的 RDMA 内存副本；
- 多级缓存压力下从 Memory 回落到 LocalDisk；
- 读取磁盘副本后重新提升到 Memory；
- 运行进程没有加载 C++ `libmooncake_store.so`。

### 4. Store 韧性门禁

标准门禁通过后才会运行，覆盖 Store 节点重启、Master/etcd 重启、C 链路断开与
恢复、两副本降级为单存活副本读取、内存/SSD 水位驱逐、混合对象压力，以及恢复后
第二次完整标准验收。故障会轮换覆盖 A/B/C，避免只验证固定节点。

### 5. Open-RDMA mock

该阶段验证 Open-RDMA 的 mock feature 和 API 行为，结果单独记录为 mock-only
证据。它不能替代 verbs 或 TE 的真实 `protocol=rdma` 结果。

## 从单机模拟走向真实三机

当前 Compose 使用 `network_mode: host` 和主机上的三个 suite-owned RXE 设备，适合
可重复的功能验收。迁移到真实三机时，应保持相同逻辑角色和六方向矩阵，但还要：

- 将 A/B/C 分布到不同主机并使用各自 RNIC；
- 为每台主机选择正确 device、GID index 和 routable 地址；
- 验证防火墙、MTU、RoCE PFC/ECN 或 InfiniBand fabric 配置；
- 额外记录带宽、延迟、NUMA 绑定和长时间稳定性。

真实三机的性能结果不能与 Soft-RoCE 数值直接比较，但功能断言应保持一致。

## 自检问题

1. 为什么三个 TE 测试容器和三个 Store 节点不是重复部署？
2. TE 为什么需要六个有向传输，而不是只测 A→B？
3. Store 标准门禁失败时，为什么韧性门禁应标记为 `BLOCKED`？
4. 多级缓存场景必须观察哪些副本状态变化，才能证明回落与提升真实发生？

## 下一步

进入“如何阅读测试证据”，学习从 result、JSON 和日志中判断是产品失败、环境阻塞
还是仅 mock 通过。
