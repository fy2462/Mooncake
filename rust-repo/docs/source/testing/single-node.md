# 单节点测试方法

## 前置章节

- “代码地图”中的 crate 边界与源码入口
- “整体架构”中的控制面与数据面
- “概念基础”中的 Replica、Segment 和多级缓存

## 本章目标

建立从小到大的单节点测试顺序，并明确每一层测试能够证明什么。单节点测试适合
开发时快速定位问题，但不能证明跨主机 RDMA、远端故障或多副本放置正确。

## 统一构建约定

从仓库根目录执行。Cargo 增量产物保留在项目目录，编译并发固定为 5；测试进程
产生的短期数据可以使用系统 `/tmp`。

```bash
cd rust-repo
export CARGO_BUILD_JOBS=5
export CARGO_TARGET_DIR="$PWD/target"
```

不要把 `CARGO_TARGET_DIR` 指向共享目录。共享目录
`/home/fy2462/workspace/tmp/mooncake` 只用于容量较大的构建输入、保留日志和 E2E
产物；短生命周期测试文件放在 `/tmp` 更合适。

## 第一层：纯 Rust 单元测试

按依赖方向运行，而不是一开始就运行整个 workspace：

```bash
cargo test -p mooncake-store-core --lib
cargo test -p mooncake-store-client --lib
cargo test -p mooncake-store-master --lib
```

这层主要验证：

- Core 中的数据类型、序列化和状态约束；
- Client 的副本选择、请求编排和错误处理；
- Master 的元数据、分配、操作日志与 HA 状态逻辑。

它不能证明 TE 动态库可以加载，也不能证明 RDMA 设备、GID、MR 注册或远端传输
可用。失败时先用测试名缩小范围，例如：

```bash
cargo test -p mooncake-store-client replica --lib -- --nocapture
```

## 第二层：FFI 边界测试

系统已经安装与当前工程匹配的 Transfer Engine 头文件和共享库后，运行：

```bash
cargo test -p transfer-engine-ffi --lib -- --nocapture
```

这层关注 Rust 包装能否正确创建和释放 native handle，以及参数、状态码和生命周期
能否跨 C ABI 边界传播。若链接阶段报错，先检查：

```bash
ldconfig -p | rg 'libtransfer_engine|libtent'
```

FFI 测试通过仍不等于 RDMA 数据面通过：测试可能没有注册真实网卡内存，也可能只
覆盖失败路径或本机路径。

## 第三层：多节点脚本的本地契约测试

E2E 编排本身也需要测试。以下测试不会创建真实 RXE 拓扑，适合在修改脚本后快速
检查门禁、结果格式、三节点约束和多级缓存断言：

```bash
cd tools/rdma-multinode
bash tests/test_common.sh
bash tests/test_orchestration.sh
bash tests/test_standard_gate_contract.sh
bash tests/test_store_gate_signal.sh
bash tests/test_host_rdma_lifecycle.sh
bash tests/test_report.sh
/home/fy2462/Mooncake/.venv/bin/python -m pytest -q tests
```

若虚拟环境位于仓库根目录，应从 `rust-repo/tools/rdma-multinode` 使用
`../../../.venv/bin/python`；上面的绝对路径与其指向同一个环境。这些测试证明“编排合同”正确，例如 TE
未通过时 Store 必须是 `BLOCKED`，但它们不会证明真实 RDMA 字节传输成功。

## 推荐的开发反馈环

```{mermaid}
flowchart LR
  E[修改代码] --> F[cargo fmt --check]
  F --> U[相关 crate 单元测试]
  U --> C[编排契约测试]
  C --> M[多节点 E2E]
  U -->|失败| E
  C -->|失败| E
  M -->|失败| E
```

先运行离改动最近、反馈最快的测试。只有这些测试通过后，才值得付出构建镜像、创建
RXE 设备和启动完整集群的成本。

## 自检问题

1. `mooncake-store-client` 单元测试通过，为什么不能说明远端内存读写已经通过？
2. FFI 链接失败与 RDMA verbs 运行失败分别发生在哪一层？
3. 编排契约测试中的 PASS 为什么不能作为产品 RDMA PASS？

## 下一步

进入“多节点测试方法”，把本章的快速反馈升级为真实 RXE、TE 和 Rust Store 的
端到端证据。
