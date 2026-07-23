# Rust Store 与 Transfer Engine 学习文档

这里是基于当前 `rust_repo_main` 源码编写的中文学习站点。内容从 RDMA
基础和整体架构开始，逐步进入 Rust Store Client、Master、Store Node、
Transfer Engine、FFI、多级缓存和 HA，再通过端到端调用链与实验回到源码。

## 安装依赖

复用项目本地虚拟环境，不在共享目录创建 Python 环境：

```bash
cd /home/fy2462/Mooncake/rust-repo/docs
uv pip install --python /home/fy2462/Mooncake/.venv/bin/python \
  -r requirements-docs.txt
```

## 构建与浏览

```bash
make html
make serve
```

浏览器访问 <http://127.0.0.1:8000>。HTML 位于 `build/html`，它是可重新
生成的本地产物，不应提交到 Git。

严格构建命令为：

```bash
/home/fy2462/Mooncake/.venv/bin/sphinx-build \
  -W --keep-going -b html source build/html
```

`make check` 会检查章节入口、学习依赖标题、toctree 目标、关键主题、源码路径、
Mermaid 数量、占位符以及 Draw.io/SVG 配对。
`make clean` 清理已生成的 HTML。
