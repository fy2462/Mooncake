# 实验：追踪一次 Put

## 前置章节

- “Put：从分配到提交”
- “单节点测试方法”

## 本章目标

不用 RDMA 设备，通过搜索、测试和日志把 Put 的 Client→Master→TE 边界定位到源码。

## 步骤一：建立入口清单

```bash
cd /home/fy2462/Mooncake
rg -n 'pub async fn put\(' rust-repo/crates/mooncake-store-client/src
rg -n 'async fn put_(start|end|revoke)' rust-repo/crates/mooncake-store-master/src
rg -n 'pub fn (submit_transfer|get_transfer_status)' \
  rust-repo/crates/transfer-engine-ffi/src
```

预期依次找到 Client public API、Master tonic/内部实现和 FFI batch 操作。把结果按控制面
与数据面分成两列：PutStart/End/Revoke 属于控制面，submit/status 属于数据面。

## 步骤二：运行最小测试

```bash
cd rust-repo
export CARGO_BUILD_JOBS=5
export CARGO_TARGET_DIR="$PWD/target"
cargo test -p mooncake-store-client put --lib -- --nocapture
cargo test -p mooncake-store-master put --lib -- --nocapture
```

若过滤结果为 0 个测试，先用 `cargo test -p <crate> --lib -- --list | rg -i put` 选择当前
真实测试名。不要为得到绿色结果而跳到无关测试。

## 步骤三：观察日志与断点

```bash
RUST_LOG=te_debug=trace,mooncake_store_master=debug \
  cargo test -p mooncake-store-client put --lib -- --nocapture
```

在 IDE 中依次设置断点：`Client::put`、Master PutStart 内部实现、
`write_to_replica`、`finalize_put_for_key`、Master PutEnd。mock/dummy 测试可能不会进入
native RDMA，这是本实验的预期边界。

## 完成标准

你应能画出调用链，指出副本何时 Allocating、数据何时传输、何时 Complete，以及
任一 write 失败时由谁 revoke。把答案与 Put 走读页对照。

## 自检问题

1. 哪些断点属于控制面，哪些属于数据面？
2. 单元测试未进入 native TE 能证明哪些逻辑？
3. 第一个必须在所有写完成后执行的控制面动作是什么？

## 下一步

进入“三节点 Soft-RoCE E2E”，把同一链路放到真实 RDMA 协议门禁中。
