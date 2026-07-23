# Rust Store 与 Transfer Engine 学习站点实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `rust-repo/docs` 建立基于当前真实源码、可构建为 HTML 的中文 Rust Store、Master 与 Transfer Engine 分层教程。

**Architecture:** 使用 Sphinx、MyST 和 pydata-sphinx-theme 组织“架构 → 概念 → 子系统 → 请求链路 → 源码地图 → 实验”六层内容。核心架构使用 Draw.io 源文件和同步 SVG，局部时序与状态流使用 Mermaid；独立校验脚本检查源码链接、导航覆盖和图形配对。

**Tech Stack:** Sphinx, MyST Markdown, sphinxcontrib-mermaid, pydata-sphinx-theme, Draw.io XML/SVG, Bash, Python 3.

## Global Constraints

- 内容面向熟悉 Rust、尚未系统学习 RDMA 的开发者，重点讲解 RDMA 与实际代码映射。
- 所有架构和功能结论必须由当前 `rust_repo_main` 的类型、函数、测试或运行脚本支撑。
- Rust Store 不得被描述为依赖或调用 C++ Store；native 边界是 `transfer-engine-ffi` 到 C++ Transfer Engine/TENT。
- 核心图必须同时保存 `.drawio` 可编辑源文件和 SVG；局部图使用 Mermaid。
- 使用 `/home/fy2462/Mooncake/.venv` 安装文档依赖，不在共享目录创建 Python 虚拟环境。
- HTML 输出到 `rust-repo/docs/build/html`，构建产物不提交 Git。
- 修改范围仅限 `rust-repo/docs` 及必要的 `.gitignore` 文档构建条目，不修改产品实现。

---

### Task 1: 建立可验证的 Sphinx/MyST 站点骨架

**Files:**
- Create: `rust-repo/docs/Makefile`
- Create: `rust-repo/docs/requirements-docs.txt`
- Create: `rust-repo/docs/README.md`
- Create: `rust-repo/docs/source/conf.py`
- Create: `rust-repo/docs/source/index.md`
- Create: `rust-repo/docs/source/_static/css/custom.css`
- Create: `rust-repo/docs/tools/check_docs.py`

**Interfaces:**
- Consumes: Markdown documents under `rust-repo/docs/source` and repository root resolved from `conf.py`.
- Produces: `make html`, `make clean`, and `python tools/check_docs.py` as stable verification entry points.

- [ ] **Step 1: Write the site contract checker first**

Create `tools/check_docs.py` to require the planned section indexes, verify every Markdown source path written as `` `rust-repo/...` `` or repository-relative code link exists, require every `.drawio` stem to have a matching SVG, and reject `TBD`/`TODO` placeholders.

- [ ] **Step 2: Run the checker and verify it fails on the absent site**

Run:

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
```

Expected: non-zero with missing `source/index.md`, section indexes, and diagram pairs.

- [ ] **Step 3: Add the minimal Sphinx configuration**

Configure `myst_parser`, `sphinxcontrib.mermaid`, `sphinx_copybutton`, `pydata_sphinx_theme`, Chinese search, local `_static`, and strict reference handling. Use standard `sphinx-build -M` in `Makefile`; list exact Python packages in `requirements-docs.txt`; add build and preview commands to `README.md`.

- [ ] **Step 4: Add the root navigation page and section placeholders as real index pages**

Create the root `index.md` with a MyST toctree for `getting-started`, `architecture`, `concepts`, `store`, `transfer-engine`, `walkthroughs`, `code-map`, and `labs`. Each section index must explain its purpose and list its child pages; do not use empty placeholders.

- [ ] **Step 5: Install dependencies and build the initial site**

Run:

```bash
cd rust-repo/docs
uv pip install --python /home/fy2462/Mooncake/.venv/bin/python -r requirements-docs.txt
/home/fy2462/Mooncake/.venv/bin/sphinx-build -W --keep-going -b html source build/html
```

Expected: exit 0 and `build/html/index.html` exists.

- [ ] **Step 6: Commit the site skeleton**

```bash
git add rust-repo/docs/Makefile rust-repo/docs/requirements-docs.txt \
  rust-repo/docs/README.md rust-repo/docs/source rust-repo/docs/tools/check_docs.py
git commit -m '[Doc] scaffold Rust Store learning site'
```

### Task 2: 建立真实源码清单和阅读路线

**Files:**
- Create: `rust-repo/docs/source/code-map/index.md`
- Create: `rust-repo/docs/source/code-map/crates-and-entrypoints.md`
- Create: `rust-repo/docs/source/code-map/recommended-reading-order.md`
- Create: `rust-repo/docs/source/getting-started/index.md`
- Create: `rust-repo/docs/source/getting-started/learning-roadmap.md`
- Create: `rust-repo/docs/source/getting-started/environment-and-build.md`

**Interfaces:**
- Consumes: Cargo manifests, `lib.rs`/`main.rs`, Python bindings, `tools/rdma-multinode`, and native TE entry points.
- Produces: authoritative crate/component map reused by all later pages.

- [ ] **Step 1: Inventory public entry points and cross-crate dependencies**

Record concrete symbols including `mooncake_store_client::Client`, Master gRPC service implementation, `StorageBackend`, `HotStandbyService`, `transfer_engine_ffi::TransferEngine`, `TransferRequest`, `BatchId`, `SegmentId`, and Python `MooncakeStore` bindings. Confirm each path with `rg` before documenting it.

- [ ] **Step 2: Write the crate and process map**

Explain `mooncake-store-core`, `mooncake-store-client`, `mooncake-store-master`, `transfer-engine-ffi`, Python bindings, C++ TE/TENT, etcd, and three-node E2E assembly. Include a table with responsibility, process ownership, key symbols, dependencies, and first file to read.

- [ ] **Step 3: Write the three-stage learning roadmap**

Define “建立模型、追踪路径、进入源码” stages, exact page order, commands to run, and observable completion criteria. Include alternative fast tracks for Master/HA and TE/RDMA.

- [ ] **Step 4: Write environment and build instructions**

Document local Cargo target, `CARGO_BUILD_JOBS=5`, `/usr/local` SPDK/DPDK boundary, Python venv, native shared libraries, Sphinx commands, and privileged RXE test requirements without embedding credentials.

- [ ] **Step 5: Validate source paths and commit**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
git add source/code-map source/getting-started
git commit -m '[Doc] map Rust Store source reading path'
```

### Task 3: 绘制整体架构和控制面/数据面

**Files:**
- Create: `rust-repo/docs/diagrams/system-overview.drawio`
- Create: `rust-repo/docs/diagrams/store-master-te.drawio`
- Create: `rust-repo/docs/source/_static/diagrams/system-overview.svg`
- Create: `rust-repo/docs/source/_static/diagrams/store-master-te.svg`
- Create: `rust-repo/docs/source/architecture/index.md`
- Create: `rust-repo/docs/source/architecture/system-overview.md`
- Create: `rust-repo/docs/source/architecture/control-plane-and-data-plane.md`
- Create: `rust-repo/docs/source/architecture/deployment-topology.md`

**Interfaces:**
- Consumes: Task 2 component map and actual three-node Compose/E2E topology.
- Produces: shared top-level mental model and reusable SVG assets.

- [ ] **Step 1: Create a valid editable Draw.io system diagram**

Use native mxGraph XML with layers for Application/Python, Rust Client, Master, Store A/B/C, TE/FFI, etcd, memory, LocalDisk, and network. Use blue control-plane arrows and orange data-plane arrows with an explicit legend.

- [ ] **Step 2: Create the Store/Master/TE boundary diagram**

Show Rust crate boundaries, tonic gRPC, shared protocol types, unsafe FFI boundary, C ABI, C++ TransferEngine/TENT, RDMA/TCP transports, and explicitly mark that C++ Store is not linked.

- [ ] **Step 3: Export matching SVGs and verify XML/SVG validity**

Use Draw.io CLI if available; otherwise generate semantically equivalent SVG from the approved node/edge model and validate both XML and SVG with Python XML parsing. Keep source and rendered labels synchronized.

- [ ] **Step 4: Write the architecture chapters**

Explain process topology, ownership, control/data paths, three RXE endpoints, metadata versus object bytes, and why Master does not relay payloads. Embed the SVGs and add a Mermaid deployment flow where useful.

- [ ] **Step 5: Run diagram and HTML checks, then commit**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
/home/fy2462/Mooncake/.venv/bin/sphinx-build -W --keep-going -b html source build/html
git add diagrams source/_static/diagrams source/architecture
git commit -m '[Doc] explain Store Master and TE architecture'
```

### Task 4: 编写 RDMA、Replica 与多级缓存概念层

**Files:**
- Create: `rust-repo/docs/source/concepts/index.md`
- Create: `rust-repo/docs/source/concepts/rdma-for-rust-developers.md`
- Create: `rust-repo/docs/source/concepts/object-replica-and-segment.md`
- Create: `rust-repo/docs/source/concepts/multilevel-cache.md`

**Interfaces:**
- Consumes: TE FFI types, Store core replica types, allocator/storage backend behavior.
- Produces: terminology and invariants referenced by subsystem and walkthrough chapters.

- [ ] **Step 1: Explain RDMA from a Rust developer perspective**

Map ownership/lifetime intuition to registered memory, MR, RNIC, QP, CQ, WR, GID, completion polling, local/remote address and rkey. Explain what zero-copy does and does not mean, and why unsafe registration is isolated in FFI.

- [ ] **Step 2: Explain object, slice, replica and segment semantics**

Use actual `ReplicaType`, `ReplicaStatus`, `ReplicaInfo`, `Slice`, segment naming and handle rules. Include a Mermaid state diagram for allocation → pending → complete → failed/removed.

- [ ] **Step 3: Explain 3/2/1 profiles and multi-level caching**

Describe baseline three-replica correctness, two-owner degraded reads, one-replica LocalDisk fallback, watermarks, eviction, fallback read, and promotion. Distinguish E2E scenario inputs from production defaults.

- [ ] **Step 4: Validate terminology against symbols and commit**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
git add source/concepts
git commit -m '[Doc] teach RDMA and Store data concepts'
```

### Task 5: 深入 Rust Store Client、Master、Store Node 和 HA

**Files:**
- Create: `rust-repo/docs/source/store/index.md`
- Create: `rust-repo/docs/source/store/client.md`
- Create: `rust-repo/docs/source/store/master.md`
- Create: `rust-repo/docs/source/store/store-node.md`
- Create: `rust-repo/docs/source/store/metadata-and-oplog.md`
- Create: `rust-repo/docs/source/store/ha-and-recovery.md`

**Interfaces:**
- Consumes: Client modules, Master services/allocator/backend, E2E Store node wrapper, HA coordinator/oplog/snapshot code.
- Produces: subsystem-level source guide and background-task model.

- [ ] **Step 1: Document Client structure and lifecycle**

Trace construction, TE initialization, buffer allocation, public operations, batching, background tasks, replica selection, local hot cache, offload and promotion modules. Explain `Arc`, mutex/channel and async task ownership only where it affects behavior.

- [ ] **Step 2: Document Master request and allocation logic**

Trace tonic handlers into service state, allocator plans, object/replica metadata, segment registration, finalize/revoke, removal and background cleanup. Include a Mermaid component-flow diagram.

- [ ] **Step 3: Document Store Node behavior**

Explain that the current E2E Store node is a Rust-backed process assembled by `store-node.py`, how it publishes memory segments, uses `RustFilePerKey`, processes commands/stats, and differs from Master and Client.

- [ ] **Step 4: Document metadata, oplog, snapshots and HA**

Explain leader election, notifier startup, inclusive oplog replay, snapshot bootstrap, standby catch-up, promotion, and Master/etcd restart. Include a Mermaid state diagram and recovery sequence.

- [ ] **Step 5: Build and commit**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
/home/fy2462/Mooncake/.venv/bin/sphinx-build -W --keep-going -b html source build/html
git add source/store
git commit -m '[Doc] explain Rust Store and Master internals'
```

### Task 6: 深入 Transfer Engine 与 FFI

**Files:**
- Create: `rust-repo/docs/source/transfer-engine/index.md`
- Create: `rust-repo/docs/source/transfer-engine/overview.md`
- Create: `rust-repo/docs/source/transfer-engine/ffi-boundary.md`
- Create: `rust-repo/docs/source/transfer-engine/transport-and-rdma.md`
- Create: `rust-repo/docs/source/transfer-engine/transfer-lifecycle.md`

**Interfaces:**
- Consumes: `transfer-engine-ffi`, C headers/API, `TransferEngine`, Transport implementations, RDMA context/endpoint/workers and TENT code.
- Produces: exact Rust-to-native call map and transfer lifecycle.

- [ ] **Step 1: Document TE architecture and transport selection**

Explain metadata discovery, local segments, remote segments, RDMA/TCP transports, topology, endpoint store and TENT boundary. Separate stable conceptual API from implementation details.

- [ ] **Step 2: Document FFI safety and ownership**

Trace Rust wrappers to C ABI declarations. Explain `TransferEngine`, `SegmentId`, `BatchId`, `TransferRequest`, register/unregister safety contracts, `Drop`, error conversion, and thread-safety assumptions.

- [ ] **Step 3: Document native RDMA path**

Map device selection, context lookup by device name, GID index, memory registration, endpoint/QP setup, WR submission, CQ polling, retry/error state and link interruption behavior to concrete C++ symbols.

- [ ] **Step 4: Document one transfer lifecycle with Mermaid**

Show allocate batch → open segment → create requests → submit → poll terminal state → free batch, including failure and cleanup branches. Add a second compact sequence for TENT-enabled requests.

- [ ] **Step 5: Validate and commit**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
git add source/transfer-engine
git commit -m '[Doc] explain Transfer Engine and Rust FFI'
```

### Task 7: 编写端到端请求链路教程

**Files:**
- Create: `rust-repo/docs/source/walkthroughs/index.md`
- Create: `rust-repo/docs/source/walkthroughs/put-object.md`
- Create: `rust-repo/docs/source/walkthroughs/get-object.md`
- Create: `rust-repo/docs/source/walkthroughs/remove-object.md`
- Create: `rust-repo/docs/source/walkthroughs/cache-eviction-and-promotion.md`
- Create: `rust-repo/docs/source/walkthroughs/node-failure-recovery.md`

**Interfaces:**
- Consumes: Tasks 4–6 terminology and source maps.
- Produces: request-oriented entry points connecting user actions to modules and network operations.

- [ ] **Step 1: Trace Put end to end**

Cover buffer preparation, Master allocation, three replica locations, segment opening, RDMA write requests, completion, finalize and failure rollback. Include a detailed Mermaid sequence diagram and source-reading checkpoints.

- [ ] **Step 2: Trace Get end to end**

Cover metadata lookup, candidate selection, RDMA read, LocalDisk fallback, byte validation and optional promotion. Explain why promotion may relocate to another healthy endpoint.

- [ ] **Step 3: Trace Remove and overwrite**

Cover logical metadata changes, replica cleanup, force semantics, background effects, idempotency and observable errors.

- [ ] **Step 4: Trace cache and failure paths**

Write focused walkthroughs for watermark eviction/promotion and A/B/C Store restart, Master/etcd restart, and C RXE reconnect plus Store restart. Map each phase to retained E2E evidence.

- [ ] **Step 5: Validate and commit**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
git add source/walkthroughs
git commit -m '[Doc] trace Store operations end to end'
```

### Task 8: 编写分步实验并完成浏览器验收

**Files:**
- Create: `rust-repo/docs/source/labs/index.md`
- Create: `rust-repo/docs/source/labs/tracing-a-request.md`
- Create: `rust-repo/docs/source/labs/running-three-node-e2e.md`
- Create: `rust-repo/docs/source/labs/debugging-rdma.md`
- Modify: `rust-repo/docs/README.md`
- Modify: `rust-repo/docs/tools/check_docs.py`

**Interfaces:**
- Consumes: existing Cargo tests, tracing/log settings, and `tools/rdma-multinode/run.sh` stages/results.
- Produces: reproducible learning exercises and final acceptance evidence.

- [ ] **Step 1: Write a non-privileged request-tracing lab**

Give exact `rg`, `cargo test`, `RUST_LOG`, source breakpoint and expected-observation steps. The learner must trace one Put or Get without requiring RDMA hardware.

- [ ] **Step 2: Write the three-node RXE lab**

Explain prerequisites, `CARGO_BUILD_JOBS=5`, artifact placement, interactive sudo, individual gate commands, full `run.sh all`, result interpretation and manifest-owned cleanup. Never embed a password.

- [ ] **Step 3: Write the RDMA debugging lab**

Use `rdma link`, `ibv_devinfo`, GID/device mapping, TE result/logs, Store ready/stats and common failure signatures. Provide a decision flow from preflight through QP/data integrity.

- [ ] **Step 4: Strengthen automated coverage checks**

Require all named topics, at least two `.drawio`/SVG pairs, multiple Mermaid blocks, all source links, and all toctree targets. Print actionable failures with file paths.

- [ ] **Step 5: Run final strict build and inspect representative HTML**

```bash
cd rust-repo/docs
/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py
rm -rf build
/home/fy2462/Mooncake/.venv/bin/sphinx-build -W --keep-going -b html source build/html
test -f build/html/index.html
test -f build/html/architecture/system-overview.html
test -f build/html/concepts/rdma-for-rust-developers.html
test -f build/html/store/master.html
test -f build/html/transfer-engine/ffi-boundary.html
test -f build/html/walkthroughs/put-object.html
test -f build/html/labs/running-three-node-e2e.html
```

Open the site locally with:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m http.server 8000 -d build/html
```

Inspect the homepage, navigation, two SVG diagrams, Mermaid output, Chinese search assets and representative pages in a browser.

- [ ] **Step 6: Run hygiene checks and commit**

```bash
git diff --check
/home/fy2462/Mooncake/.venv/bin/pre-commit run --files $(git diff --name-only)
git status --short
git add rust-repo/docs
git commit -m '[Doc] complete Rust Store learning guide'
```
