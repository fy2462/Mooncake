# 环境与构建

## 前置章节

- “三阶段学习路线”

## 本章目标

复用正确的 Python/Cargo 环境，并区分构建产物、测试临时数据和 E2E 保留产物。

## 路径约定

本教程以 `/home/fy2462/Mooncake` 为仓库根目录：

| 内容 | 路径 | 用途 |
|---|---|---|
| Rust workspace | `rust-repo/` | Store Client、Master、core、FFI 与 Python 扩展 |
| Cargo target | `rust-repo/target` | 日常增量编译与单元测试 |
| 原生 TE | `mooncake-transfer-engine/` | C++ Transfer Engine、TENT 和 transports |
| E2E 工具 | `rust-repo/tools/rdma-multinode/` | 三节点 RXE/TE/Rust Store 验收 |
| 保留证据 | `/home/fy2462/workspace/tmp/mooncake/rdma-multinode` | E2E 二进制、日志与结果 |
| Python 环境 | `/home/fy2462/Mooncake/.venv` | Python binding、测试与 Sphinx |

`rust-repo/.cargo/config.toml` 把 Cargo 输出设为本地 `target`，并把默认
并发数限制为 5。命令中仍显式写 `CARGO_BUILD_JOBS=5`，这样学习记录不会
依赖某台机器的隐式配置。

## Rust 基线

```bash
cd /home/fy2462/Mooncake/rust-repo
CARGO_BUILD_JOBS=5 cargo check --workspace
CARGO_BUILD_JOBS=5 cargo test -p mooncake-store-client --lib
CARGO_BUILD_JOBS=5 cargo test -p mooncake-store-master --lib
```

启用真实 TE 的 crate 需要 native 共享库。边界是
`libtransfer_engine.so`，启用 TENT 时还包括 `libtent_shared.so`；Rust Store
不应链接 `libmooncake_store.so`。

## SPDK/DPDK 的位置

当前 NoF/SPDK 集成从 `/usr/local` 使用匹配版本的头文件与库。它属于可选
功能，不是理解 Memory Replica 和基本 RDMA Put/Get 的前置条件。初学阶段
不要用 SPDK 编译失败解释普通 TE 数据路径。

## 文档环境

```bash
cd /home/fy2462/Mooncake/rust-repo/docs
uv pip install --python /home/fy2462/Mooncake/.venv/bin/python \
  -r requirements-docs.txt
make html
make serve
```

浏览器打开 <http://127.0.0.1:8000>。严格构建使用 `-W --keep-going`，任何
断链或未知指令都会使构建失败。

## 三节点实验的权限边界

软件 RoCE 实验需要 Docker、`rdma-core`、`rdma_rxe` 内核模块和交互式
`sudo`。脚本只创建带 `mc-rdma-*` 前缀且写入 ownership manifest 的 bridge、
veth 和 RXE device；`run.sh all` 退出时会清理它们。密码不得写进命令、脚本
或文档。

不具备特权时仍然可以完成概念阅读、Rust 单元测试、文档构建和大部分源码
追踪实验。

## 自检问题

1. Cargo target、长期 E2E artifact 和运行时临时文件分别应放在哪里？
2. 哪些实验需要 sudo，哪些不需要？

## 下一步

进入“Crate、进程与入口”，建立源码地图。
