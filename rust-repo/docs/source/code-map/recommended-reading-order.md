# 推荐源码阅读顺序

## 第一遍：只读 12 个入口

不要一开始遍历所有模块。按顺序阅读下列文件，并用旁边的问题约束注意力。

1. `rust-repo/crates/mooncake-store-core/src/types.rs`<br>
   Segment、Replica 和状态如何表达？
2. `rust-repo/proto/mooncake_store_grpc.proto`<br>
   哪些 RPC 属于元数据控制？
3. `rust-repo/crates/mooncake-store-client/src/client/mod.rs`<br>
   一个 Client 长期持有哪些资源？
4. `rust-repo/crates/mooncake-store-client/src/client/lifecycle.rs`<br>
   TE、buffer、Master channel 和后台任务按什么顺序建立？
5. `rust-repo/crates/mooncake-store-client/src/client/write.rs`<br>
   Put 的控制面和数据面在哪里分开？
6. `rust-repo/crates/mooncake-store-client/src/client/read.rs`<br>
   Get 怎样选择读取路径？
7. `rust-repo/crates/mooncake-store-master/src/service/grpc_trait.rs`<br>
   tonic handler 如何转发到业务实现？
8. `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_put.rs`<br>
   分配和完成状态如何改变？
9. `rust-repo/crates/transfer-engine-ffi/src/lib.rs`<br>
   raw handle 和 native 资源由谁释放？
10. `rust-repo/crates/transfer-engine-ffi/src/transfer.rs`<br>
    一个 batch/request/status 在 Rust 中如何表示？
11. `mooncake-transfer-engine/src/transfer_engine.cpp`<br>
    façade 如何选择 transport？
12. `mooncake-transfer-engine/src/transport/rdma_transport/rdma_transport.cpp`<br>
    RDMA 请求怎样进入 worker 和 completion 路径？

第一遍只记入口和边界，不追每个 helper。

## 第二遍：沿 Put/Get 分支阅读

打开两个终端：一个用 `rg` 查定义，一个保留调用方。推荐命令：

```bash
cd /home/fy2462/Mooncake
rg -n 'pub async fn put|put_start|put_end|write_to_replica' \
  rust-repo/crates/mooncake-store-client \
  rust-repo/crates/mooncake-store-master
rg -n 'pub async fn get|select_best_replica|read_from_replica' \
  rust-repo/crates/mooncake-store-client
```

每跨一次 gRPC 或 FFI 边界就在笔记中画一条横线。边界比 helper 的细节更
重要，因为它决定错误、超时和资源清理由谁负责。

## 第三遍：选择专题

### Master 与 HA

```text
main.rs
  → main_ha.rs / main_server.rs
  → ha/supervisor.rs
  → hot_standby.rs
  → oplog/oplog_manager.rs
  → ha/oplog_applier.rs
  → ha/snapshot.rs
```

关注 leader view、oplog sequence、snapshot watermark 和 promotion fence，
不要先陷入 etcd/Kubernetes/Redis coordinator 的协议细节。

### 多级缓存

```text
client/storage.rs
  → client/storage_offload.rs
  → client/storage_promotion.rs
  → local_storage_backend/
  → master/service/cluster/offload.rs
  → master/service/cluster/promotion.rs
  → master/eviction.rs
```

始终记录对象当前的 ReplicaType、ReplicaStatus 和实际字节所在位置。

### TE/RDMA

```text
transfer-engine-ffi/src/lib.rs
  → transfer-engine-ffi/src/transfer.rs
  → transfer_engine_c.cpp
  → transfer_engine.cpp / transfer_engine_impl.cpp
  → transport/rdma_transport/rdma_transport.cpp
  → rdma_context.cpp / rdma_endpoint.cpp / worker_pool.cpp
```

在 native 路径中区分“配置/建连”和“每次数据请求”。前者创建 context、QP、
endpoint；后者提交 WR 并等待 CQ completion。

## 如何判断已经读懂一个函数

对每个关键函数回答五个问题：

1. 输入代表控制信息还是数据 buffer？
2. 它读取或改变了哪个持久/共享状态？
3. 它跨越了 gRPC、FFI 或线程/async task 边界吗？
4. 成功的可见性条件是什么？
5. 中途失败时谁释放已分配的空间、batch、Segment 或内存注册？

如果只能复述语句，却回答不了这五点，就还没有掌握该函数的系统职责。
