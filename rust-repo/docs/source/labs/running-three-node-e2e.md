# 实验：运行三节点 Soft-RoCE E2E

## 前置章节

- “多节点测试方法”
- “如何阅读测试证据”

## 本章目标

创建三个 RXE 端点和完整 Compose 拓扑，得到 verbs、TE、Store standard/resilience 的
可审计证据，并安全清理由本套件创建的资源。

## 前置检查

```bash
cd /home/fy2462/Mooncake
docker compose version
command -v rdma
command -v ibv_devinfo
git status --short
```

需要 Docker 与加载 `rdma_rxe`、创建 `mc-rdma-*` bridge/veth/device 的 sudo 权限。
sudo 会交互提示；不要把密码写进命令。长期产物默认在共享目录，容器运行数据在 `/tmp`。

## 分层运行

```bash
export CARGO_BUILD_JOBS=5
export RDMA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/rdma-multinode
rust-repo/tools/rdma-multinode/run.sh preflight
rust-repo/tools/rdma-multinode/run.sh host-rdma-setup
rust-repo/tools/rdma-multinode/run.sh build
rust-repo/tools/rdma-multinode/run.sh compose-up
rust-repo/tools/rdma-multinode/run.sh verbs
rust-repo/tools/rdma-multinode/run.sh te
rust-repo/tools/rdma-multinode/run.sh store-standard
rust-repo/tools/rdma-multinode/run.sh store-resilience
rust-repo/tools/rdma-multinode/run.sh report
```

每一步成功后再进入下一步。首次使用建议分层运行；熟悉后可用：

```bash
rust-repo/tools/rdma-multinode/run.sh all
```

`all` 即使失败也尝试生成报告并清理，但仍保留第一个非零状态。

## 检查结果

```bash
sed -n '1,120p' "$RDMA_ARTIFACT_ROOT/verbs.result"
sed -n '1,160p' "$RDMA_ARTIFACT_ROOT/te.result"
/home/fy2462/Mooncake/.venv/bin/python -m json.tool \
  "$RDMA_ARTIFACT_ROOT/store-standard.result"
/home/fy2462/Mooncake/.venv/bin/python -m json.tool \
  "$RDMA_ARTIFACT_ROOT/store-resilience.result"
sed -n '1,240p' "$RDMA_ARTIFACT_ROOT/report.md"
```

TE 必须是 RDMA 六方向 PASS/compare OK；Store standard 必须证明三个不同 RDMA 副本
与 LocalDisk fallback/promotion；resilience 必须七场景完整且无 first_failure。

## 清理

```bash
rust-repo/tools/rdma-multinode/run.sh cleanup
```

脚本只删除 manifest 所有的 `mc-rdma-*` 资源。清理后用 `docker ps -a` 和 `rdma link`
确认没有套件容器/设备残留；保留 artifact 供评审。

## 自检问题

1. 为什么 verbs、TE、Store 必须顺序执行？
2. 三个 Store 与三个 TE test node 分别验证什么？
3. `run.sh all` 失败后首先查看哪个结果？

## 下一步

进入“RDMA 分层排障”，练习定位第一个失败层。
