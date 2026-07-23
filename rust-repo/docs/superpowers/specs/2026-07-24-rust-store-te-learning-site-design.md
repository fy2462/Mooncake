# Rust Store 与 Transfer Engine 学习站点设计

## 目标

在 `rust-repo/docs` 建立一套面向“熟悉 Rust、尚未系统学习 RDMA”的开发者的中文学习站点。站点必须以当前 `rust_repo_main` 的真实实现为准，从顶层系统模型逐步深入 Rust Store Client、Master、Store Node、Transfer Engine 和 RDMA 数据通路，使读者能够根据文档独立定位入口、追踪调用链、运行实验并继续阅读源码。

最终产物通过 Sphinx 和 MyST Markdown 构建为静态 HTML，可在本地浏览器中完整导航和搜索。核心架构图同时提供可编辑的 Draw.io 源文件与适合 HTML 展示的 SVG；局部时序、状态和流程图使用 Mermaid。

## 目标读者与讲解边界

目标读者已经掌握 Rust 语法、trait、所有权和基本异步编程，不重复编写通用 Rust 入门教程。文档重点补充：

- RDMA 的 MR、QP、CQ、WR、GID、RNIC、注册内存与零拷贝概念。
- Rust Store 的对象、分片、Replica、Segment、Master 元数据和 Store Node 存储职责。
- 控制面与数据面的分工，以及 Rust/C FFI 到 C++ Transfer Engine 的边界。
- 多副本、多级缓存、淘汰、回读提升、HA 日志恢复和节点故障处理。
- 如何从可运行命令、日志和测试逐步进入对应源码。

C++ Store 只作为迁移背景或行为参考，不作为运行依赖，也不作为本学习站点的主要阅读对象。Transfer Engine 则按照真实 native 依赖边界讲解。

## 内容组织方案

站点采用“顶层架构 → 核心概念 → 请求链路 → 子系统源码 → 动手实验”的学习路径，而不是按文件名逐个罗列。

```text
rust-repo/docs/
├── Makefile
├── requirements-docs.txt
├── README.md
├── source/
│   ├── conf.py
│   ├── index.md
│   ├── getting-started/
│   │   ├── index.md
│   │   ├── learning-roadmap.md
│   │   └── environment-and-build.md
│   ├── architecture/
│   │   ├── index.md
│   │   ├── system-overview.md
│   │   ├── control-plane-and-data-plane.md
│   │   └── deployment-topology.md
│   ├── concepts/
│   │   ├── index.md
│   │   ├── rdma-for-rust-developers.md
│   │   ├── object-replica-and-segment.md
│   │   └── multilevel-cache.md
│   ├── store/
│   │   ├── index.md
│   │   ├── client.md
│   │   ├── master.md
│   │   ├── store-node.md
│   │   ├── metadata-and-oplog.md
│   │   └── ha-and-recovery.md
│   ├── transfer-engine/
│   │   ├── index.md
│   │   ├── overview.md
│   │   ├── ffi-boundary.md
│   │   ├── transport-and-rdma.md
│   │   └── transfer-lifecycle.md
│   ├── walkthroughs/
│   │   ├── index.md
│   │   ├── put-object.md
│   │   ├── get-object.md
│   │   ├── remove-object.md
│   │   ├── cache-eviction-and-promotion.md
│   │   └── node-failure-recovery.md
│   ├── code-map/
│   │   ├── index.md
│   │   ├── crates-and-entrypoints.md
│   │   └── recommended-reading-order.md
│   ├── labs/
│   │   ├── index.md
│   │   ├── tracing-a-request.md
│   │   ├── running-three-node-e2e.md
│   │   └── debugging-rdma.md
│   └── _static/
│       └── diagrams/
│           ├── system-overview.svg
│           └── store-master-te.svg
└── diagrams/
    ├── system-overview.drawio
    └── store-master-te.drawio
```

每个一级主题用 `index.md` 汇总章节目的、前置知识和推荐顺序。首页提供顺序阅读入口，也允许熟悉系统的读者直接跳转到源码地图或具体请求链路。

## 页面模板

源码讲解页根据内容选择以下固定栏目，保持整套教程的阅读节奏一致：

1. 本章要解决的问题。
2. 阅读前需要知道的概念。
3. 组件职责与边界。
4. 关键类型、trait、线程和异步任务。
5. 调用链及数据结构变化。
6. 精确到仓库相对路径和符号名的源码导航。
7. 常见误解、失败模式和日志观察点。
8. 可执行的小实验。
9. “读完后应该能回答”的自检问题。

源码位置使用 Sphinx/MyST 可解析的仓库相对链接。行号只在确实稳定且有帮助时使用；主要导航依赖类型名、函数名和搜索命令，避免源码更新后所有链接失效。

## 顶层教学模型

第一张 Draw.io 总图必须建立以下心智模型：

- 应用通过 Rust Store Client 发起对象操作。
- Master 负责元数据、分配、Replica 状态、租约/事务和 HA，不搬运对象主体数据。
- Store Node 提供注册内存 Segment 与本地磁盘层，执行卸载、淘汰和提升。
- Transfer Engine 位于客户端和 Store Node 的数据通路中，通过 FFI 暴露注册、批量传输和状态接口。
- etcd 承担协调、选主和持久化 oplog/snapshot 相关元数据。
- Put/Get 的控制 RPC 和 RDMA 数据传输必须用不同颜色与箭头表示。

第二张 Draw.io 图聚焦 Store、Master 与 TE 的代码边界，明确 Rust crate、C ABI、C++ TE/TENT 和底层 transports 的关系，并标出“不链接 C++ Store”的约束。

## 图形策略

### Draw.io 与 SVG

核心图在 `docs/diagrams` 保存 `.drawio` XML 源文件，在 `source/_static/diagrams` 保存同步导出的 SVG。SVG 必须包含清晰中文标签、图例和方向箭头，并能在浅色 HTML 页面中阅读。文档嵌入 SVG，同时链接到 `.drawio` 源文件，便于后续编辑。

### Mermaid

以下内容使用 Mermaid：

- Put、Get、Remove 的时序图。
- 内存注册与批量传输生命周期。
- Replica 状态变化。
- 内存 → LocalDisk → 内存的淘汰与提升流程。
- Master/etcd 重启和 oplog 恢复流程。
- 三节点 A/B/C 故障场景。

Sphinx 使用支持 Mermaid 的扩展在构建时渲染，不依赖浏览器访问外部 CDN。

## 源码调研与真实性要求

写作前必须从当前工作树建立符号清单，而不是沿用历史印象。至少核对：

- `mooncake-store-client` 的公开 API、请求入口与 TE 使用点。
- `mooncake-store-master` 的 service、allocator、oplog、snapshot、hot standby 与 storage backend。
- `mooncake-store-core` 的共享类型和协议模型。
- `transfer-engine-ffi` 的安全封装、C ABI 声明、资源生命周期和错误映射。
- `mooncake-transfer-engine` 的 TransferEngine、Transport、RDMA context、endpoint、worker/CQ 逻辑。
- Python binding 和三节点 E2E 如何装配以上组件。

每个重要结论至少由实际类型/函数、测试或运行脚本之一支撑。对于仍在迁移或尚未实现的能力，文档必须明确标为当前限制，不能把规划写成现状。

## 主学习路线代码注释

教程引用的关键 Rust 路径需要补充紧贴代码的学习型注释，但不对整个仓库逐行注释。范围限定为：

- Client 初始化、Put/Get/Remove、Replica 选择和传输提交。
- Master 分配、Put 提交、查询、删除及关键后台任务。
- Replica/Segment 状态转换、多级缓存淘汰与提升。
- oplog、snapshot、leader/standby 恢复的关键状态边界。
- `transfer-engine-ffi` 的 unsafe 内存注册、Segment 和 batch 生命周期。

注释优先解释代码本身无法直接表达的“为什么”：状态不变量、所有权/生命周期约束、控制面到数据面的切换、unsafe 调用的前置条件、失败时为何按特定顺序清理。已有清晰注释不重复添加；不把每行语句翻译成自然语言，不改变任何产品行为。注释以中文为主，公共 API 的既有英文 rustdoc 保持兼容。

## Sphinx 构建方案

参考 RLinf 的最小 Sphinx 结构，但只引入本地学习站点必需部分：

- `Makefile` 使用标准 `sphinx-build -M`。
- MyST 同时解析 Markdown、交叉引用和 admonition。
- 使用 `pydata-sphinx-theme` 提供侧边导航与站内搜索。
- 使用 Mermaid 扩展离线生成流程图。
- `requirements-docs.txt` 固定最低依赖范围。
- 优先使用 `/home/fy2462/Mooncake/.venv` 安装和构建，不在共享目录创建新的 Python 虚拟环境。
- HTML 输出到 `rust-repo/docs/build/html`；构建产物不提交 Git。

`README.md` 记录环境安装、构建、清理和本地 HTTP 服务命令。正式验收命令为：

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/sphinx-build -W --keep-going -b html source build/html
```

## 学习路线

教程提供三个递进阶段：

1. **建立模型**：RDMA 基础、整体架构、控制面/数据面和部署拓扑。
2. **追踪路径**：Put、Get、Remove、多级缓存、HA 与恢复。
3. **进入源码**：按 crate/模块阅读、运行单元测试、添加日志、执行三节点 RXE E2E。

每阶段给出预计阅读顺序、需要运行的命令和完成标志。学习实验只使用已有安全测试入口；涉及 RXE、Docker 或 sudo 的实验明确标记权限和清理行为。

## 验证与验收

完成条件如下：

1. 目录树中的页面全部存在并进入 Sphinx toctree，不产生 orphan 页面。
2. 两张核心 Draw.io 图均有可编辑源文件和同步 SVG，SVG 可在 HTML 中正常展示。
3. Mermaid 图在离线构建中成功渲染。
4. `sphinx-build -W --keep-going` 退出 0，无断链、重复标题或未知指令警告。
5. 自动检查所有文档中引用的仓库相对源码路径均存在。
6. 抽查首页、整体架构、RDMA 基础、Put、Master、TE/FFI 和实验页面的生成 HTML，确认导航与中文显示正常。
7. 文档明确覆盖 Store/Master/TE 顶层设计、Put/Get/Delete、3/2/1 Replica、多级缓存、HA、A/B/C 故障恢复和推荐源码阅读顺序。
8. `git diff --check` 和针对文档文件的可用 pre-commit hooks 通过，不包含 HTML 构建产物或无关改动。
9. 主学习路线关键 Rust 文件包含与教程一致的行内注释，`cargo fmt --check`、受影响 crate 测试和 clippy/check 均通过，且代码 diff 不含行为修改。

## 非目标

- 不在本任务中重构 Store、Master 或 TE 产品行为；只允许补充主学习路线所需的注释。
- 不自动生成完整 Rust API reference；教程以概念、链路和源码导航为主。
- 不复制 RLinf 的在线搜索服务、版本切换器、AI 问答组件或部署配置。
- 不提交 `build/html`、Python 虚拟环境或大型构建缓存。
- 不把 Open-RDMA mock 当作 Transfer Engine 数据面的实现或证明。
