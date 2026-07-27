# C++ / Rust Mooncake Store 功能对齐审计

日期：2026-07-25
审计基线：`d7b1544c`
范围：`mooncake-store`（C++）与 `rust-repo`（Rust）Store 实现
方法：功能结论来自静态代码、协议与状态机审计；后续修复项另以定向单元/集成
回归作为实现证据，但测试通过不替代生产环境验证。

## 实现边界（硬约束）

- 本工作的目标是功能与逻辑打平，不是为 C++ Store 新增另一套语言绑定。
- Rust Store 是对 C++ Store 的永久平替，不存在混合部署阶段；验收只考察 Rust
  独立运行时是否复现 C++ 的功能和逻辑。
- 永久不开发 Rust Client→C++ Master、C++ Client→Rust Master、tonic/coro_rpc
  互通、双栈共存、RPC bridge/sidecar 或 endpoint 协商。
- `mooncake-store` C++ 代码只作为行为参考，不在本工作中修改。
- Store 权威数据与状态机优先且原则上落在
  `rust-repo/crates/mooncake-store-master`。
- 分层固定为 `Store/Client -> transfer-engine-ffi -> Transfer Engine`：
  Master 持有对象、副本、并发、恢复与错误语义；Client 使用 Store API 并经
  FFI 委托数据传输；FFI 是传输适配和 `unsafe` 隔离层；TE 只负责注册内存、
  建立传输与提交/完成数据传输。
- `mooncake-store-client` 仅在方案更合理时补充必要的薄逻辑，例如参数校验、
  请求编排及结果/错误转换；不得复制 Master 的权威状态或状态机。
- `mooncake-p2p-store` 不是本次对齐落点；FFI 和 TE 不承载 Store 状态机。
  工作树不修改其源码；共享 FFI 保留原有 copyable `BatchId` API 供该范围外调用方
  使用，Rust Store、registered-memory 与 Python binding 只使用 engine-bound
  `OwnedBatchId` API。
- 原始句柄、裸指针、ABI 调用、边界转换和 native 生命周期规则必须封装在 FFI
  内；Master/Client 等顶层使用方只调用安全 Rust API，不使用或传播 `unsafe`
  语义。
- 不引入 `mooncake-store-c`，也不把 Store 状态机下沉到 FFI 或 TE。
- 当前阶段先以静态代码、协议和状态机审计确认逻辑正确，不以搭建新的本地测试
  环境作为前置条件。

## 结论

按当前静态代码、协议和状态机审计，经典 C++ Store 范围内已没有已知未豁免的
P0/P1 功能差异。最后发现的 delayed/discarded replica HA 缺口已按 Rust-only
边界修复：Upsert preemption/replacement、过期 Put 与 MoveEnd 不再依赖
process-local `tokio::spawn + sleep`，而是把旧 Memory/NoF range、绝对 deadline
和 object image 写入同一 durable oplog record；native v12 snapshot、catalog
extension、C++ `discarded_replicas` importer、standby replay、allocator rebuild
和到期 tombstone/reaper 使用同一权威状态；segment unmount 对跨 segment entry
只剪除命中的 replica，不会提前释放其余 range。过期 Copy/Move、所有 revoke/
source-invalid rollback 与 promotion staged detach 也禁止在 durable metadata
之前归还 allocator；PromotionAllocStart 返回 writable descriptor 前持久化
object range，quota abort mismatch 会 fence 而不是静默继续。Memory、NoF、
GracefulUnmount completion、NoF heartbeat 与 expired-client purge 的 segment
卸载也统一为 durable tombstone 成功后才失效 replica、释放 allocator 和发布
拓扑变更；flush 失败保留 segment 并永久 fence。Memory/NoF Mount 与 ReMount
的新 segment 同样在 durable mount 成功后才进入 segment/allocator/client/HTTP
metadata live state；producer preflight 与 replay decoder 都拒绝 nil client/
segment identity 和无效 geometry，NoF namespace offset 0 按 C++ 语义保持合法，
Cachelib 模式要求 slab 对齐，不能写出 leader 接受但 standby 无法重放的记录。
Memory mount UUID 由完整 request identity 稳定派生，同一请求跨 leader 重试不会
追加第二个 segment；NoF 同 UUID 重试幂等、同 endpoint 异 UUID fail-closed。
Memory/NoF/LocalDisk 的单批 CRUD、Copy/Move、并发、physical quota ledger 及
admission eviction、Drain/HA、恢复、经典 Transfer Engine 生命周期和安全 FFI
边界均已在 Rust 路径落地。allocator release 也已恢复 C++ `AllocatedBuffer`
所有权语义：只接受精确 live offset/size/segment identity，整批 Memory/NoF
预校验后再释放，stale/double-free 或恢复期 UUID/name 分叉均 fail-closed。

这仍不是生产验收完成声明：真实 C++ producer golden、完整 Rust 回归、
Transfer Engine 数据面以及 HF3FS/global DISK/CXL 目标环境证据尚未补齐。
TENT 与 accelerator DLPack native registration 是已记录的经典范围外可选能力，
不应被混同为经典 Store 功能缺口。

### 范围外能力登记

| 能力 | 用户影响 | 当前替代/失败语义 | 重新纳入条件 | 删除日期 |
|---|---|---|---|---|
| TENT priority/policy/deadline/intent | Rust Store 不能启用 TENT 高级调度 | `MC_USE_TENT`/`MC_USE_TEV1` 在创建 native engine 前 fail-fast；经典 TE 正常可用 | 单独评审并落地版本化 capability/request/status ABI，且顶层 TE 生命周期完成审计 | 不适用：按用户规范永久不属于经典 Store 平替 |
| accelerator DLPack native registration | GPU/加速器 tensor 不能直接注册为 Store buffer | CPU TensorMetadata 路径可用；accelerator capsule 在消费前 fail-closed | C ABI 可证明 device ordinal、pointer location、producer synchronization 与 DMA 生命周期 | 不适用：当前平替范围明确排除 |
| SHM hot-cache dummy acquire/release | 多进程不能共享 C++ memfd hot cache | 普通进程内 hot cache 已对齐；显式 SHM 开关 fail-fast | Rust Client/FFI 具备 owner-bearing memfd mapping、IPC acquire/release 和 TE registration 生命周期 | 不适用：仅在目标部署明确要求时另立能力 |
| io_uring/O_DIRECT、NUMA/NIC 拓扑与 SSD quantile time series | 吞吐、时延和观测细节可能不同 | POSIX Store 语义、显式 MR 限制和 Prometheus summary 可用 | 独立性能/硬件验收提出量化门槛；不得以此修改 Store 状态机 | 不适用：性能增强而非功能 parity |

以上项目均给出显式用户影响和退出条件；不存在静默降级。它们没有临时“稍后
删除”的兼容分支，因此删除日期记为不适用，而不是虚构日期。

## 对齐判定标准

“功能打平”要求同时满足：

1. API 能力覆盖一致；
2. 正常路径与失败路径的状态机一致；
3. 并发调用具有相同的原子性和幂等语义；
4. Memory、NoF、LocalDisk 等副本的生命周期一致；
5. 配额、租约、Pin、驱逐、Drain 与恢复后的行为一致；
6. 持久化格式、主备复制协议和对外绑定具备明确的兼容策略；
7. 经典 Transfer Engine 生命周期必须打平；TENT 只有在版本化 capability/request
   ABI 完整贯通后才计入支持范围，当前必须显式拒绝而不能静默误配。

## 功能矩阵

| 模块 | Rust 状态 | 判定 |
|---|---|---|
| Memory 单对象 CRUD | 主路径与 owner-bearing 多 segment 生命周期已静态覆盖：`MC_MAX_MR_SIZE` 分片、UUID identity、HA remount、base-address local fast-path、teardown 及动态 allocate/mount/free 均已接入；Mount 响应丢失时先按 canonical UUID rollback，只有 `OK`/`NotFound` 才释放 owner，其余结果保活 owner | 实现已修复，待回归验证 |
| Batch CRUD | 单/多 buffer、逐 key status/finalize、短传输拒绝与 owner-bearing completion 已静态覆盖 | 实现已修复，待回归验证 |
| Copy / Move | tenant task payload、local-source gate、end-failure revoke、限次重试、native runtime snapshot、payload/key restore fence 与 source-pin/quota 恢复已静态覆盖；`replication_start` leader/standby/snapshot 对 source、allocated target、existing Move target、non-complete ownership 及 checked Memory reservation 使用对称 preflight；无新增 target 的 Copy 与复用既有 target 的 Move 保留 C++ 薄客户端语义 | 实现已修复，待回归验证 |
| NoF | 分配、读写、批量完成与独立水位/分配压力淘汰路径已覆盖 | 实现已修复，待回归验证 |
| LocalDisk offload | offload/promotion、FilePerKey/Offset pending eviction、Master 授权恢复、generation fencing 与 stale-upsert cleanup 已覆盖；FilePerKey/Offset durable accepted-eviction journal 属于两端均未提供的额外 crash hardening | Store parity 已静态修复，待回归验证 |
| Hot cache | 覆盖写前后统一失效、`MC_STORE_LOCAL_HOT_CACHE_SIZE`/block/admission 启动配置、16 MiB 默认 block、固定内存 Count-Min Sketch 频次准入、远端 Memory-only 填充与单对象 block 上限均已补齐；显式 builder 保留首次 miss 准入。共享内存 dummy acquire/release 因 Rust 尚无 memfd cache lifecycle 而 fail-fast | 普通进程内路径已修复；SHM 可选能力未打平 |
| 并发 Put/BatchPut | scoped-key mutation coordinator 覆盖 Put/Upsert quota retry 的 operation stripe、逐 key RPC/后台 mutation、batch offload 的去重有序 `lock_many`、读触发 promotion 的内部重入 gate，以及 quota/automatic eviction 的 global mutation epoch；authored cases 覆盖单 key 竞争、交叉 batch 与 `lock_many` 成员串行化 | 已修复 |
| Tenant quota | physical-replica ledger、尺寸变化 Upsert 双计费窗口、同尺寸原地 Upsert 的 committed-allocation/inflight-generation 区分、所有副本 mutation、snapshot/oplog 重建，以及同 tenant deficit eviction + 两次 admission retry 已静态对齐；外部 admission 与 durable restore 对单对象乘法、tenant 聚合及 `used + reserved` 组合均使用 checked/u128 arithmetic，溢出在 allocator/live-state mutation 前 fail-closed | 实现已修复，待回归验证 |
| Snapshot / allocator recovery | v12 snapshot、typed/legacy oplog completion、segment topology、runtime task 与 delayed replica reservations 从 live state 精确重建；保存 allocator config、deadline/pin/quota、同尺寸原地 Upsert processing generation、尺寸变化 Upsert replacement 及 exact existing Move target 语义，恢复时 live/delayed range 统一做越界、重叠和 UUID/name identity 校验；Offset/Cachelib 保存精确 live allocation ownership，release 拒绝 stale/size mismatch/double-free，Memory+NoF 整批预校验；Cachelib slab count 在 mount/config/restore 校验 u32 identity 表示范围；decode、semantic、allocator 或 quota candidate 失败均发生在唯一 live-state replacement barrier 前，可继续尝试较旧 snapshot | 实现已修复，待回归验证 |
| Drain / graceful failover | Drain terminal、GracefulUnmount deadline、HA role/lifecycle、atomic baseline、durability fence、启动 preflight 与 bounded reconnect 已完成静态修复；恢复会撤销没有 runtime job authority 的 active Drain task，finished-task reaper 会保留仍被 Drain job 引用的终态 task 直到 job 消费结果，所有 Memory/NoF 自动与显式卸载都在 durable tombstone 后才释放本地状态 | 实现已修复，待回归验证 |
| Redis/Kubernetes HA | capability matrix 在生产 HA 启动前拒绝无 shared-oplog 组合 | 已修复（fail-fast） |
| TENT | 为保持经典 Store parity 零改 TE C++，当前 Rust Store 对 `MC_USE_TENT`/`MC_USE_TEV1` fail-fast；顶层 request v2/capability/status ABI 尚未实现 | 可选能力未打平 |
| FFI 安全边界 | Store Client 的 staging、单/批量、range、offload 与 zero-copy 提交已全部改走 owner-bearing `RegisteredMemory`、typed `RegisteredTransferRequest` 和 `SubmittedRegisteredBatch`；Client 业务模块不再构造 raw `TransferRequest` 或调用 unsafe submit。batch 自持 region/in-flight claim，reaper 只有在 native quiescence/free 后才释放；legacy BatchId 仅为范围外 P2P 保持源码兼容；accelerator DLPack 因现有 ABI 无法证明 device ordinal/sync 而 fail-closed | 经典 Store 路径已修复，DLPack 未打平 |
| Python Client lifecycle | 创建路径已绑定 health/storage/task worker；普通调用用 cancellation-safe async mutex guard 原位借用 Client；teardown/close 先停止 worker，Engram 转移/归还时停止/重启 worker | 静态实现已修复，待回归验证 |
| Python BufferPool | 已绑定 Rust Client，提供原生 writable `BufferLease`、identity/export/lifetime、阻塞/超时/max-regions、active-close gate 与 owner-bearing TE registration；全部 region 走安全 overflow owner，不复用独占 staging buffer | 静态功能已修复，性能路径不同，待回归验证 |
| HTTP metadata API | key-scoped GET/PUT/DELETE 已补齐并保留聚合 GET | 已修复 |
| global DISK | Master descriptor、Client 原子读写、durable tenant/key sidecar、quota/FIFO/水位驱逐、逐 key accepted-set、tenant remove-all 与 HA config fence 已完成静态实现 | 实现已修复，待回归验证 |
| CXL | 已补 Master 单一全局 Cachelib-like allocator、client alias mount/remount、HA/oplog allocator 重建、Client 通过现有 FFI 注册 TE CXL mmap、device-relative offset 与写入 preferred alias；未修改 TE C++/C ABI | 静态实现已修复，待真实 CXL 回归 |
| Bucket / Distributed / HF3FS | Client Bucket 与 Distributed 均已接入 `AttachedLocalStorage`。Distributed 已静态覆盖 C++ 环境配置、XXH64 hash bucket/文件名转义、HF3FS fd 注册与 USRBIO 分片 I/O、健康探测、无驱逐语义，并增加 Rust generation envelope、原子发布、严格恢复与 generation fencing；未复用 Master 文件工具，也未修改 TE/C++ | Store 逻辑已静态修复；真实 HF3FS runtime/故障恢复待验证 |

## P0：阻塞替代的正确性问题

### 1. 并发首次 Put 缺少按 key 原子性

状态：已于当前修复分支解决。

C++ master 在对象创建状态机外层使用对象锁串行化同一 key。Rust 当前采用 `contains/get -> allocate -> insert` 的复合流程；这些步骤之间没有同一把 key 级锁。两个并发的首次 `PutStart` 或 `BatchPutStart` 可能同时通过存在性检查并各自分配空间，随后互相覆盖元数据并泄漏或错误归属副本。

证据：

- C++：`mooncake-store/src/master_service.cpp` 的对象锁路径；
- Rust：`crates/mooncake-store-master/src/service/grpc_objects_put.rs`；
- Rust batch：`crates/mooncake-store-master/src/service/grpc_batches/batch_put_start.rs`。

修复要求：对同一 tenant-scoped key 的检查、过期清理、分配、插入和 processing 标记建立单一原子边界；Batch 必须使用稳定锁顺序。

修复证据：

- `MasterState::key_mutations` 使用固定分片的 `KeyMutationCoordinator`；
- 单 key mutation 入口按 tenant-scoped key 串行；
- PutStart/UpsertStart 另持有 tenant-scoped operation stripe，和 C++
  `AcquireObjectOperationLock` 一样跨越 quota eviction 的全部重试；释放 mutation
  guard 进入 exclusive eviction epoch 时，同 key 新请求不能插入；
- BatchPutStart 按 C++ 逐 key 委托 PutStart 的边界逐项获取 operation/mutation
  guard；其他真正 multi-key mutation 对分片编号排序、去重后获取锁；
- Put/Upsert、完成/撤销、Remove、Copy/Move、offload/promotion、eviction/reaper 和失效 handle 清理共享相同边界；
- Exist/Get/BatchGet/regex Get 的 lease 与 group-lease 更新也在相同 key gate
  内重读并校验 Complete/routable 状态；因此不能先返回旧 replica view、再与
  Remove/Upsert 交错补 lease。group refresh 使用 exclusive snapshot mutation
  barrier 冻结 tenant/group membership，并原子刷新全部 Complete 成员，不能让
  group eviction 插入逐成员更新窗口；租约刷新在返回成功前写入单条
  `lease_refresh_batch` durable oplog，其中携带 canonical tenant/key/group
  identity 与绝对 lease/soft-pin deadline。standby 先验证整批 identity 和完整
  group membership，再一次性安装全部 deadline；任一项异常不改变对象或
  `expected_seq`；
- promotion admission 在同一 key gate 内完成 source 重读、refcount pin 与 task
  publish，不能和 Remove/Upsert 交错发布引用已删除 generation 的任务；
- BatchPutStart 逐 entry 复用空 key/tenant delimiter 校验，不允许批量路径绕过
  tenant-scoped key canonicalization；
- UpsertStart 抢占旧 processing replica 时使用新 buffer，并延迟释放旧 buffer；
- 已编写 32 writer 单 key 并发、反序 overlapping batch、Upsert preemption、
  operation stripe 串行/不阻塞 snapshot epoch，以及 lease refresh 等待 key
  mutation guard 的测试；按当前约束未运行。

### 2. 写操作未使 Hot Cache 失效

状态：已于当前修复分支解决。

Rust `Get` 优先命中本地 hot cache，但 Put、BatchPut 和 Upsert 没有在覆盖写前后失效对应缓存项。客户端可能在写成功后继续返回旧值。C++ 在 Put/Upsert/Batch 写路径中显式执行缓存失效。

证据：

- Rust cache-first：`crates/mooncake-store-client/src/client/read.rs`；
- Rust write：`client/write.rs`、`client/write_parts.rs`、`client/write_batch.rs`；
- Rust upsert：`client/upsert.rs`；
- C++：`mooncake-store/src/client_service.cpp`。

修复要求：覆盖写开始前失效；Upsert 成功后再次失效，以覆盖并发读在传输期间重新填充缓存的窗口。

修复证据：

- client 写入、分片写、batch 写与 upsert 共享 tenant-scoped invalidation helper；
- upsert 成功后执行第二次失效，关闭传输期间并发读重新填充的窗口；
- remove 系列不再维护独立失效逻辑。

### 3. Tenant quota 的计费单位和恢复重建不一致

状态：physical-replica ledger、snapshot 与完整 object-image/remove oplog 重建已完成
静态修复，待允许执行回归门后验证。

C++ 按实际 Memory 副本字节计费。Rust PutStart 预留并最终提交单份对象大小，删除也按对象大小回收；多副本与 NoF 组合不能准确反映资源占用。Rust 从 snapshot 恢复对象后没有重建 tenant quota 使用量。

证据：

- Rust：`grpc_objects_put.rs`、`grpc_objects.rs`、`helpers.rs`、`service/mod.rs`；
- C++：`mooncake-store/src/master_service.cpp`。

修复要求：定义并统一“逻辑字节”或“物理副本字节”口径；所有创建、扩缩副本、删除、回滚和恢复路径只通过同一计费接口修改额度。

修复证据：

- `TenantQuotaTable` 分离 metadata object count、reserved bytes、used bytes 和首次 positive commit count；
- PutStart 按请求的 Memory 副本数预留，PutEnd 按实际 Complete Memory 副本数结算并回滚差额；
- NoF-only 与 LocalDisk-only 对象使用零物理字节、非零 metadata object count；
- Copy/Move/promotion 的临时预留、成功结算和 revoke/reaper 回滚已纳入；
- PutEnd、PutRevoke、Copy completion 使用 project-before-mutate，quota 失败不再
  暴露 Complete replica、lease 或 cache metric 半提交；Move 把 target settle
  与 source release 应用于 cloned ledger 后原子安装；
- NoF-only 对象首次通过 Copy/Move/promotion 获得正 Memory charge 时增加一次
  committed count，已有 Memory charge 的对象扩副本不会重复增加；
- 尺寸变化 Upsert 在 replacement 完成前同时保留旧 physical charge 与新
  reservation；PutEnd 事务性 settle 新 charge 并 release 旧 charge，revoke/remove
  事务性回收两者。新分配失败保留旧对象；native snapshot v9 持久化 replacement
  accounting 字段，legacy snapshot 以零值兼容；
- quota release 大于对象权威 committed charge 时 fail-closed，不再静默 clamp；
- eviction、replica clear、失效 handle 与 Remove 按实际被移除 Memory 副本释放；
- eviction、replica clear 与失效 handle 清理使用 project-before-mutate，quota
  mismatch 不再只记 warning 后继续发布副本删除；durable 整对象 remove 后若
  quota invariant 仍失败则 fence service，避免错误 ledger 继续接受新写入，
  恢复时仍由 durable object table 重算；
- snapshot load 从 object/replica 状态重建 quota ledger；
- standby oplog typed `put_end` v3 从完整 replica、hard pin、data type、deadline
  与 quota image 重建对象，remove/revoke 回收账本；
- AddReplica 拒绝非 LocalDisk descriptor，避免未计费 Memory 副本注入；
- Rust 回归覆盖多 Memory 副本、Copy/Move、完整驱逐、LocalDisk-only、promotion 和策略删除保护。

### 4. Master snapshot 丢失 allocator 空洞

状态：snapshot、typed oplog object-image、segment topology 与 native runtime task
恢复已完成静态修复，待允许执行回归门后验证。

Rust snapshot 只保存 segment 的聚合 `used`，恢复时把 `[used, capacity)` 当成唯一空闲区。对象删除产生的空洞和实际分配区间不会被精确重建，可能造成空间永久丢失，极端情况下也可能与恢复后的对象布局冲突。C++ 会序列化精确 allocator 状态。

证据：

- Rust：`storage_backend_snapshot.rs`、`allocator/mod.rs`、`service/mod.rs`；
- C++：`serialize/serializer.cpp`、`segment.cpp`。

修复要求：snapshot 持久化 allocator 的精确 free/allocated ranges，或从所有存活 replica descriptor 验证并重建区间集合。

修复证据：

- Offset/CachelibLike allocator 从所有 live Memory/NoF replica descriptor 重建；
- 恢复拒绝 missing segment、越界、错位、重复/重叠及 usage overflow；
- object/replica contract 额外拒绝 zero-size/empty、`ALL` descriptor、
  replica/object size 漂移、range overflow、Memory/NoF nil segment 与重复
  location；native Copy/Move task 的 source/target 匹配包含 size，不能以同
  segment/offset/type 但错误长度保留 Allocating target。CXL alias 与 runtime
  allocation strategy 双向校验，漂移在替换 live state 前失败；native v10 与
  catalog `rust_allocator_config_v1` 保存 `allocation_strategy` 和
  `memory_allocator_kind`，新快照若与 runtime 配置不同会在状态替换前失败，
  legacy 缺失配置仍保持只读兼容；
- snapshot v2 引入精确 allocator 恢复，v3 增加 native Copy/Move runtime state，
  v4 保存对象 put/lease/soft-pin deadline、正确的 Copy/Move task codec 和实际重试
  次数；缺失格式号按 v1 读取，未来版本拒绝；
- restore 在 replacement barrier 前规范化 default-tenant object/task key，并拒绝
  canonical duplicate、对象 metadata identity 冲突、重复/空 LocalDisk storage
  identity、重复 task UUID、active task 缺对象和 replication source/target
  不一致；Copy/Move task JSON payload 同时按 task type 解析，payload tenant/key
  必须与 canonical `TaskEntry.key` 一致，legacy payload 会重写为显式 tenant +
  user key；native task envelope/payload UUID、assigned client 与时间戳也会
  fail-closed 校验，active task 必须存在 assigned client；Drain 无法解析源 owner
  时保持 blocked 而不创建悬空任务；清空 live state 后只执行不可失败安装；
- hot standby snapshot replacement 复用相同恢复逻辑并清理旧状态；
- catalog bootstrap 合并 latest marker 与全部 published listing；marker 只作
  availability hint，descriptor 去重并按 `(producer view, included sequence,
  snapshot ID)` 降序。这样旧 leader 的在途同步 I/O 即使晚于 successor 覆盖
  marker，也不会压过新 term snapshot。manifest/payload 解码失败以及后续
  allocator/task/quota semantic restore 失败都会继续尝试更早 candidate；全部
  失败时 snapshot-only 模式 fail-closed，启用 oplog following 时才允许从
  sequence 0 进入 oplog-only bootstrap；
- 新写 manifest 使用 C++ `<protocol>|<version>|<snapshot_id>`，reader 校验第三
  字段与 descriptor identity；历史 Rust `|rust` 只读兼容保留，新写入不再生成
  无 snapshot identity 的 manifest；
- standby 在提交 typed object image 前构建候选 allocator，拒绝 missing segment、越界与重叠且不污染已应用状态；
- typed image replacement 与 remove/revoke 同时构建 tenant-quota 候选账本；
  旧对象 removal、replacement restore 任一 accounting mismatch 都不会删除对象、
  修改 allocator/client index 或推进 sequence，成功后才一次性替换 ledger；
- unmount replay 先投影整批 affected object、task reservation 与 quota 变化，再
  提交 object/index/segment/allocator；批内后续对象失败不会留下部分 unmount。
  leader 与 standby 均以 replica type + UUID 失效指定介质，避免 Memory/NoF UUID
  碰撞时跨介质删除。leader 若在关联 object image 持久化期间进入 durability
  fence，最终 segment oplog 成功也只用于收敛 standby，RPC 仍返回 Unavailable；
- leader 的显式 Memory/NoF RPC、GracefulUnmount scheduler、NoF heartbeat 与
  expired-client purge 共用 persist-then-apply helper：持有 global mutation
  barrier 校验 UUID/owner/name，先 flush durable unmount tombstone，再失效
  replica、释放 allocator 和 bump view。flush 失败时本地 segment、allocator 与
  graceful intent 保持原状并 fence；durable tombstone 后的本地 apply 若违背
  已验证 invariant 也会 fence；
- Memory/NoF Mount 与 ReMount 的新 segment 先完成可重放 preflight 和 durable
  mount，再插入 live segment/allocator、同步 client index、发布 HTTP metadata
  与 bump view；flush 失败不会让 snapshot 或 allocator 观察到新 segment。
  mount producer/replay 同时拒绝 nil client/segment UUID、空 segment name、
  无效 Memory base/size、NoF size/endpoint 与 Cachelib 非对齐范围；NoF base 是
  namespace offset，0 保持合法。Memory UUID 由 client 与完整 runtime identity
  稳定派生，mount oplog 的可选 `identity_version=1` 让 standby 校验 fingerprint，
  legacy 缺失字段仍可读取；Memory/NoF 同 UUID 都支持恢复后的安全 runtime
  rebind。NoF 同 UUID retry 不重复写 oplog，同 endpoint 异 UUID 拒绝，避免
  durable replay poison 与重试重复 mount；
- stable Memory UUID 同时下沉到 `mooncake-store-core` 供 Client rollback 使用。
  Mount RPC 报错、缺 UUID 或返回非 canonical UUID 时，Client 在释放 owner 前
  对 expected/returned UUID 执行清理；`OK`/`NotFound` 都确认目标已不存在，
  无法确认删除时返回
  `SegmentMountOutcomeAmbiguous` 并保留/泄漏 owner。启动 owned segment、动态
  allocate-and-mount 与 CXL registration 都遵守这一失败语义，native unregister
  失败也不再 drop CXL owner；
- segment mount oplog v1 可恢复完整拓扑，unmount 同步移除关联副本与 allocator segment。
- mount/unmount/remount/local-disk mount 与 client purge 进入独占 global mutation barrier，snapshot 不再观察这些路径的中间态。
- local snapshot v3+ 与可选 catalog sidecar 保存 native Copy/Move
  source/targets/reservation/task age；恢复重建 source pin、quota reservation 与
  allocator 占用；
- snapshot capture 在短 mutation barrier 内复制不可变 DTO，持久化 I/O 在 barrier
  外执行；超时或取消不会允许第二个 writer 与仍在运行的 writer 重叠；
- 无 runtime task 归属的旧快照 `Allocating` replica 会在恢复时撤销。
- snapshot baseline、后续 oplog following 与 promotion 的组合测试验证 applied sequence 和 object table 收敛。

### 5. 远端 LocalDisk 读取缺少边界检查

状态：已于当前修复分支解决。

Rust 远端 offload read 直接使用 `result.pointers[0]`，并按远端 metadata 的 size 构造写入本地固定 buffer 的 TE 请求。恶意或损坏的 metadata/response 可导致 panic，或让 native transport 写越界。

证据：`crates/mooncake-store-client/src/client/offload_read.rs`。

修复要求：在 RPC 前验证 size 可转换且不超过本地 buffer；验证响应指针数量；所有 RPC 后错误路径释放远端 batch。

修复证据：

- 远端 size 在转换与 allocate 前校验；
- response pointer 数量与本地 buffer capacity 明确校验；
- RPC 成功后的 TE open/allocate/submit/poll 失败路径统一释放远端 batch。

### 6. Upsert 与 LocalDisk/ALL 完成语义错误

状态：已于当前修复分支解决。

同尺寸 Upsert 会把已有 Complete 副本全部改成 Allocating；客户端却只应重写 Memory/NoF 数据。Batch 客户端跳过 LocalDisk，但 master 的 `ReplicaType::All` 会把所有类型标记为 Complete，导致旧 LocalDisk 内容被重新宣告为有效。单对象 Upsert 还会把 LocalDisk descriptor 当成普通 TE 目标。

证据：

- master：`grpc_objects_upsert.rs`、`grpc_objects.rs`；
- client：`client/upsert.rs`；
- C++ target type 语义：`mooncake-store/src/master_service.cpp`。

修复要求：`All` 只代表 Memory + NoF；Upsert 必须明确淘汰或刷新旧 LocalDisk 副本，不能让 stale disk replica 重新变成 Complete；单/批量路径使用相同 finalize 决策。

修复证据：

- `ReplicaType::All` 只 finalize Memory/NoF；
- client 单/批量 upsert 均不向 TE 提交 LocalDisk descriptor；
- 覆盖写使旧 LocalDisk replica 失效，并进入重新 offload 生命周期。

### 7. Drain 可错误报告成功并重新激活源节点

状态：已于当前修复分支解决。

Rust Drain 在部分失败路径可能仍完成任务，并把源节点恢复为 active。该行为也可在 C++ 参考实现中找到，属于共享缺陷；它不应被当作“Rust 与 C++ 一致即可接受”的功能证据。

证据：Rust `drain.rs`、`background_drain.rs`；C++ `master_service.cpp`。

修复要求：成功条件必须由所有对象迁移结果共同决定；失败节点保持不可分配状态，直到显式恢复。

修复证据：

- 暂无目标时 job 保持 Running，源 segment 保持 Draining；
- terminal failure 标记 Failed，不自动恢复 Active；
- 全量迁移成功后源 segment 标记 Unavailable；
- 新分配排除 Draining/Unavailable Memory 与 NoF segment；
- create 的唯一 source/target 解析、Active 校验与 Draining 转换在一个 global
  mutation epoch 内完成；同名不同 UUID 的 name-only Drain 请求 fail-closed，
  并发请求不能重复认领同一 source；
- active move task 存在时拒绝 cancel，避免 source 提前恢复 Active；
- 多 segment 状态使用 durable `segment_status_batch` oplog 原子复制；成功后的
  Unavailable 能跨 baseline 晋升保留。runtime-only Drain job 在 snapshot restore
  或 promotion 丢失时，orphan Draining 会按中止语义恢复 Active，不会永久卡住；
- standby 对 durable segment status 执行 `Active -> Draining`、
  `Draining -> Active/Unavailable` 的显式迁移图，同状态重放保持幂等，
  `Unavailable -> Active` 等终态复活记录 fail-closed 且不推进 sequence；
- source 上 Allocating 等非 Complete replica 仍属于 remaining，只能形成 blocked
  unit，不能触发错误成功。

### 8. HA backend 配置与实际 oplog 能力不一致

状态：启动能力校验已于当前修复分支解决。

Rust 配置接受 Redis/Kubernetes discovery，但 leader oplog 初始化只完整支持 Etcd；无法建立 leader oplog 时会释放 leadership，使“配置可解析”不等于“集群可服务”。

证据：`main_config.rs`、`main.rs`、`main_ha.rs`。

修复要求：不支持的组合在启动前 fail-fast，或为每个宣称支持的 backend 实现等价 oplog。

修复证据：

- backend 显式声明 discovery、leader election、shared oplog capability；
- 生产 serving path 在连接 backend 前执行组合校验；
- Redis/Kubernetes 当前以明确的 unavailable 错误拒绝，Etcd 作为唯一完整组合。

### 9. TENT Store 安装链不兼容

状态：不属于经典 Store parity；当前显式拒绝，尚未实现。

原问题是 Rust client 生命周期无条件安装 transport、FFI 把空句柄视为失败，
而 native TENT 分支有意返回空句柄；C++ Store 在 TENT 模式会跳过该安装路径。

证据：

- Rust：`client/lifecycle.rs`、`transfer-engine-ffi/src/lib.rs`；
- C++：`mooncake-transfer-engine/src/transfer_engine.cpp`、`mooncake-store/src/client_service.cpp`。

边界决定：

- 经典 Store 的 create/register/open/submit/status/free 已由现有 C ABI 覆盖，
  不为 Store parity 修改 TE C++；
- Rust Store 在发现 `MC_USE_TENT` 或 `MC_USE_TEV1` 时于创建 TE 前 fail-fast，
  避免把 TENT 实例误走 classic install/discover；
- `isUsingTent` 单一布尔查询不足以证明完整能力。TENT 重新纳入范围前，需要
  版本化 capability query、request v2，以及不会丢失 Busy/terminal 语义的状态 ABI；
- priority、transport hint、policy、deadline 与 intent 当前没有穿过顶层
  `transfer_request_t`/`submitTransfer`，不得宣称已由 Rust Store 支持。

### 10. HA promotion、standby role 与 durability

状态：2026-07-26 替代 blocker 已完成静态修复，待允许执行回归门后验证。

已正确落地的局部语义包括：GracefulUnmount intent durable-before-publish、
snapshot/catalog/oplog 的绝对 deadline、catalog descriptor 最后发布，以及恢复
成功前 service gate 保持关闭。promotion/follower lifecycle 与 standby
mutation-worker gate 已完成静态修复：

- promotion 成功执行 `Promoted -> Stopped`，并覆盖下一 leader term 再次
  `Start`；外层 cleanup/retry 路径会显式记录 `enter_standby_mode` 失败；
- reaper、eviction、client purge、Drain 和 NoF heartbeat 统一持有 background
  mutation lease；demotion 关闭 service gate 后独占并排空该 lease，standby
  不再运行这些权威 mutation。
- snapshot/oplog restore 清除 Memory/NoF runtime 地址并使 allocator segment
  runtime-unbound；ReMount 以 durable identity/owner/size 为 fence，原子重绑
  segment、allocator、对象副本和在途 Copy/Move descriptor。Client 自动保存并
  重挂当前 NoF descriptor；重绑前副本不可路由、不可新分配、不可被 heartbeat
  或自动 eviction 当作在线副本处理。
- native snapshot v7 在同一 payload 中持久化 `last_included_seq`；v8 继续在
  同一 authoritative payload 中增加 Memory Segment `host_id`，旧版本缺失字段
  按空值读取并在 remount 时恢复。state clone 与
  sequence 在同一 mutation barrier 内捕获并做前后不变校验，最后随 msgpack
  原子 rename。LocalSnapshotProvider 不再拼接 catalog descriptor；旧 native
  snapshot 缺少 baseline 时按 0 重放。
- PutEnd、PutRevoke、Remove/BatchRemove、Copy/Move、offload/promotion、
  Memory/NoF Mount/Unmount、LocalDisk recovery cleanup 与到期
  GracefulUnmount，以及 Get/Exist/Batch/regex Get 已确认返回的单对象/整组
  lease refresh，均已统一改为 durable recorder。前台 RPC 在 append/flush
  失败时返回 `Unavailable`，后台 mutation 立即停止当前 worker；两者都会设置
  不可逆 `service_fenced` 并关闭 service gate。服务层已无 best-effort object/
  segment recorder 调用，KV stored/removed 事件也排在对应 durable image/remove
  之后发布。
- `OpLogApplier` 使用 replay mutex 串行化 recover、sequence advance 与 pending
  drain，避免两个 follower 回调同时应用相同 `expected_seq`；单条 oplog record
  的对象、segment、allocator、quota 与二级索引变更统一持有 snapshot global
  mutation barrier。`graceful_unmount_segment` 不再嵌套获取同一屏障，避免
  非可重入写锁死锁。
- legacy C++ `put_end` 会把无租户 key 规范化为 default tenant scoped key；
  legacy quota settle 使用 validate/project-before-mutate，失败不会留下已完成
  replica 或放大的 size。Memory/NoF Mount replay 对重复 UUID 校验 durable
  identity 与 allocator presence，拒绝冲突且首次安装失败会回滚 allocator。

- snapshot backend 名称与 backend/directory 组合改为 fallible 解析；native
  writer 在 leader publish 前执行 create/write/fsync/remove 探测，catalog
  provider 同期探测 catalog read 与 object-store write/delete。任一失败都会释放
  leadership，不开放 service gate 或 leader label；
- notifier/watch 断开后最多执行三次 polling reconnect；成功恢复
  `Watching`，连续失败进入 `Failed` 并终止 follower。Controller 已删除独立的
  `standby_running` 缓存，直接使用状态机判断运行和 promotion 资格。

## P1：兼容性与完整性差异

- **Tenant quota admission eviction 已静态修复。** C++
  `PutStart`/`UpsertStart` 捕获 `TENANT_QUOTA_EXCEEDED` 后调用
  `ComputeDeficit`，按同 tenant、Complete、非 busy、非 hard-pin、lease 已过期
  的 Memory replica 驱逐，并在允许时执行 soft-pin 第二轮及 offload 策略，最多
  重试两次。Rust 现以 `compute_deficit`、`run_tenant_quota_eviction` 和
  `reserve_tenant_quota_with_eviction` 实现同一状态机；Put/BatchPut 在 quota
  admission 前释放并重新获取完整 mutation lock set，Upsert 每次失败尝试返回后
  再驱逐，避免固定 stripe 的递归死锁。exclusive snapshot barrier 保护动态 group
  membership、replica、allocator 与 ledger 的原子视图；protected request key
  防止尺寸变化 Upsert 为腾额度驱逐自己的旧对象。hard/soft pin、lease、busy、
  group、offload defer/force/cap、同 tenant 隔离及实际释放字节均已接入。该实现
  只修改 `mooncake-store-master`，未修改 C++ Store、TE C++ 或 FFI。
- **普通 Memory 自动驱逐的 group/offload 策略已静态修复。** Rust
  `run_eviction_cycle` 现在与 C++ `BatchEvict` 一样，在 LRU 候选命中 group 后
  扩展同 tenant 全组；任一成员 live lease 时跳过全组，否则仅移除非 hard-pin、
  符合 soft-pin pass 且非 busy 的成员。soft-pin 第二轮使用独立
  `allow_evict_soft_pinned_objects` 配置，`offload_force_evict` 仅控制排队失败或
  cap 达到后的强制驱逐；quota invariant failure 会 fence 并立即中止循环，而非
  继续扫描。已补 authored tests 覆盖整组扩展、live-lease 全组保护与 hard-pin
  成员保留，按当前约束未运行。
- stale Put 清理的 DashMap guard/remove 自死锁已修复；
- Put/BatchPut/Upsert start 的 stale-handle 清理现统一进入 tenant-scoped
  project-before-mutate helper；部分失效 Memory replica 会同步释放 physical
  quota 并持久化剩余 image，账本 mismatch 会 fence。过期 PutStart 与 Upsert
  preemption 删除未提交 metadata 时先写 durable remove，再尝试新 reservation，
  避免 quota 失败后 standby/snapshot 仍保留已删除 generation；
- NoF 已使用独立 allocator usage、0.90 高水位和 0.05 eviction ratio；分配不足
  会设置瞬态压力信号，后台 worker 与 Memory eviction 分开计算目标。淘汰只删除
  Complete、handle-valid、非 busy 的 NoF 副本，并复用 hard pin、lease、soft pin
  与 scoped-key mutation gate；若仍有 Memory 副本则保留对象，NoF-only 最后副本
  被删时同步清理对象、quota/client/task 索引并持久化最终 remove；
- `transfer-engine-ffi` 为 Rust Store 提供不可伪造、不可复制且绑定 engine 的
  `OwnedBatchId`；范围外的 P2P caller 继续使用原有 legacy `BatchId`，因此无需
  修改 P2P 源码。Owned classic/TENT raw submit 与 foreign registration 为显式
  `unsafe`；Store 只经 `ClientTransferEngine` 的 owner-bearing 安全接口提交，
  `SubmittedRegisteredBatch` 自持 native batch、request descriptors、typed
  region lease 与 per-registration in-flight claim；Client 只把该 batch、
  segment 和业务 payload 移交给 cancellation-safe background reaper，native
  `TIMEOUT` 和部分 submit failure 不再导致提前释放。
  Engram 已迁移到 safe slice/copy API。Client registry 已增加 range/overlap
  校验、read/write permission type、registration generation 与 `Arc` in-flight
  lease；这些语义现已下沉到 FFI `RegisteredMemory`，由它直接持有
  `StableMemoryOwner` 并在 native 注销失败时 leak owner + tombstone range。
  Python/Dummy 使用不可伪造的 generation identity；Python registration 持有
  writable C-contiguous `PyBuffer` export lease 以阻止 resize，并校验 raw
  address 与 owner base/容量。共享 scratch buffer 使用不可复制的 staging lease；
  LocalDisk offload 的远端 batch release 也被排序在本地 native quiescence 之后。
  Rust Client ownerless address-only registration API 已删除；CUDA/ROCm DLPack
  validator 能校验 capsule、shape/stride/base/capacity/read-only，但现有 C ABI
  不能证明 native vendor/ordinal，也不能为外部 DMA 建立 producer synchronization，
  因此 accelerator registration 当前在消费 capsule 前 fail-closed。zero-copy Store Client raw
  address 入口只做 owner-bearing typed region lookup，已改成 safe API，Python/
  Dummy 顶层不再承接 unsafe Store 语义。通用 FFI 另提供
  `RegisteredTransferRequest` 与 `SubmittedRegisteredBatch`：Read/Write 分别要求
  writable/readable region，提交前校验 engine、范围和 target overflow，并对每个
  registration 建立独占 in-flight claim；native reject、Busy 或调用方丢弃均继续
  持有或 fail-closed 泄漏 lease，只有 `freeBatchID` 成功才释放。
- Rust oplog legacy `MCOPMETA1/2` 只读兼容，新 mutation 写入 `MCOPMETA3`
  完整 object image，并拒绝未来版本；Copy/Move/offload/promotion success、
  background mutation 与 revoke/failure cleanup 均记录最终 object image/remove。
  v3 对 zero-size/empty、`ALL`、replica size/range/nil segment、duplicate location
  和 quota charge 漂移 fail-closed，失败不推进 sequence；legacy typed image
  也执行相同 geometry/identity 基线校验，非法 UUID 或畸形 replica 不会静默
  恢复。leader 写入执行同一 preflight，不能生成 standby 必然拒绝的 poison
  record；standby 从未提交 quota 或未完成 Memory/NoF/Disk 副本重建
  `processing_keys`。无 typed image 的旧 `put_end` 只推进 snapshot 中既有对象
  的完成态；
- remove/revoke/unmount/put-start 的新记录固定 schema v1；缺失 schema 仅作为
  legacy reader 路径，未来版本 fail-closed。unmount 对现存 segment 同时校验
  durable UUID/name；mount、graceful-unmount、unmount 与 status batch 拒绝 nil
  UUID，graceful-unmount 还校验 live segment 的 UUID/name/client 三元身份。
  Rust Etcd writer 不把 versioned remove/revoke 降级到无 schema 的 C++ wire，
  也不把 legacy completion marker 编造成空 typed image；
- 过期 PutStart 的 partial survivor 会投影 settle reservation、精确提交剩余
  Memory charge、释放 size-changing replacement charge，再原子替换对象与
  ledger 并持久化 v3 image；保留下来的全 Complete 对象恢复 client index。
  snapshot restore 无论 quota enforcement 是否启用都会规范化对象内 charge，
  避免 legacy state 在下一次 durable mutation 时毒化 oplog；
- catalog descriptor 已固定 C++ v1 三字段 shape；payload 的跨版本 fixture 仍不足；
- catalog metadata expiry cleanup 会先解析 optional `hard_pinned`，只丢弃 lease/
  soft-pin 均过期的非 hard-pinned Complete object。C++ 当前 post-restore cleanup
  漏掉 hard-pin gate；Rust 按公开 hard-pin 不可驱逐语义修正该恢复期数据丢失，
  不复制 C++ 缺陷；
- catalog Segment reader 仅接受 C++ 已定义的 status 0–5，并把
  `UNDEFINED/DRAINED/UNMOUNTING` 安全归一为不可分配；未来或损坏的 status 不再
  被静默猜成 `Unavailable`，而是拒绝当前 snapshot 以便尝试更早版本；
- catalog 的可选 `cs`、`ld` 与 `discarded_replicas` 字段仅允许缺失，重复字段、
  错误类型不再被当作“旧版本未提供”而静默忽略。Segment ownership 拒绝重复
  client 及指向未知 Segment 的悬空引用；metadata shard ID 必须唯一且位于 C++
  的 0–1023 范围。Replica status 按 C++ `UNDEFINED/INITIALIZED/PROCESSING/
  COMPLETE/REMOVED/FAILED` 显式转换：`INITIALIZED/PROCESSING` 均归一为 Rust
  `Allocating`，两个不可读终态统一为 Rust `Failed`，且不会构造 C++ catalog
  中不存在的独立 Rust `Written` 阶段；Replica ID 和 Memory offset-handle
  flag/shape 也按 C++ wire 校验，不再接受未解析的畸形占位字段；
- HTTP metadata key-scoped GET/PUT/DELETE 已补齐；
- `rpc_only` 已跳过 TE 创建、transport 安装与 buffer 注册；
- global DISK 已补 Master-issued tenant-scoped descriptor、Client 原子 data/sidecar
  发布、跨进程 mutation lock、启动严格扫描、quota/FIFO/水位驱逐、逐 key batch
  status 与 accepted-set 重试、tenant-scoped remove-all 及 HA storage-config fence。
  无 sidecar 的裸 data、sidecar/data 不一致、超 quota 启动和 failover 配置漂移均
  fail-closed；代码级失败窗口和多租户测试已落地，当前尚未执行。CXL 已按 C++
  的单一物理 allocator/多 client alias 语义补齐 Master、HA 和 Client 静态路径，
  catalog reader 会在解码 Memory replica 前用 Rust versioned allocator-config
  extension 恢复 wire 中缺失的 `protocol=cxl`；不会再把 Rust 自己发布的 CXL
  alias 猜成 `tcp` 并在 allocator rebuild 时自相矛盾地拒绝快照。C++ catalog
  的 SegmentSerializer 本身不支持 Cachelib/CXL，缺失 extension 时不会按名称或
  地址臆测 CXL；
  复用现有 `getBaseAddr/registerLocalMemory` FFI，未修改 TE C++/C ABI；真实 DAX
  设备上的 mmap、跨进程读写与故障恢复证据尚未执行；
  Master `storage_backend` 的同名文件工具不在生产 LocalDisk 调用链，不能作为
  C++ Client backend parity 证据。Bucket 现已在正确的
  `mooncake-store-client::AttachedLocalStorage` 层实现：持久化多 key bucket
  文件、自描述严格恢复、稳定 storage identity、单 namespace owner、每条记录
  durable generation、bucket size/key 与总容量限制、实际磁盘 free-space gate、
  FIFO/LRU、watermark eviction、Master accepted-set 部分提交和全局互斥的
  in-flight read/delete。Master 已接受的 victim 在改写前写 durable eviction
  journal；重启先完成 journal 再开放 inventory，运行期未完成 journal 的 key
  也不会被 scan/read 重新暴露。offload、promotion、remove-all 与 HA inventory
  复用原有统一调用链；不做 C++ 目录格式共享。Distributed 也已落在同一 Client
  接口：生产配置只接受 `hf3fs`，使用与 C++ 一致的 XXH64(seed=0) bucket 和
  percent filename codec；native symbol、C layout、fd register/deregister、IOV/IOR
  及 CQE 均封装在内部安全适配层。数据以同目录 temporary + rename 发布，记录
  envelope 固定 logical key、value size 和非 nil generation；启动扫描验证 bucket、
  filename、envelope 与 storage identity，读/删再次校验 generation，水位驱逐按
  C++ 语义返回空集合。offload、promotion、remove-all 与 HA inventory 已通过
  `AttachedLocalStorage::Distributed` 接入；真实 3FS library、FUSE/USRBIO
  namespace、崩溃窗口与吞吐仍需目标环境验证。
  C++ `io_uring` 只是在同一 `StorageBackendInterface` 下替换 POSIX file I/O：
  Store API、落盘格式、原子发布、恢复与错误状态不变；未编译 `USE_URING` 时，
  `MOONCAKE_OFFLOAD_USE_URING` 即使开启也回落到 `PosixFile`。Rust Client 当前
  使用等价 POSIX file I/O，因此这属于性能实现选项而不是 Store 功能 parity
  缺口；若未来要求相同 NVMe queue-depth/固定 buffer 性能，再作为独立性能能力
  实现和验收；
- FilePerKey 已把记录内容升级为与 C++ `struct_pb::KVEntry` wire shape 一致的
  protobuf key/value envelope，并提供显式 persistent lifecycle；由于两端 path hash
  仍不同，不能直接共享目录，但 C++ FilePerKey 可通过离线 preflight、staging
  回读校验和同文件系统原子 rename 无损迁入 Rust canonical layout，源目录不修改；
- FilePerKey prepare/commit 以 backend id、reservation token、record generation 和
  expected encoded size 绑定；victim/target reservation 使用 RAII，在取消或失败时
  自动释放。并发写会预留 quota growth，commit 只接受同代 victim，rename 后的
  durability error 仍按已经可见的新 generation 更新权威状态；
- FilePerKey 运行期若 data directory/ownership marker 或其 `(dev, ino)` namespace
  identity 变化会 fail-closed，不会静默重建空目录；外部 unlink 会单次对账，已被
  master 接受但本地删除失败的文件进入不再发布的 orphan 状态；
- 上述 generation/orphan 状态仍是单进程内权威状态：没有跨进程 journal 或
  victim quarantine 时，外部进程可在校验与 unlink 之间替换同路径文件；进程在
  master 已接受、unlink 尚未完成的窗口崩溃，也可能在下一次扫描时重新注册该
  文件。C++ 当前同样没有这层事务日志，因此这是共享加固项，不作为 Rust 独有
  parity blocker，但生产部署必须限制外部写入；
- eviction notification 按 tenant 稳定分组并有限幂等重试；跨租户部分成功时只
  commit master 已接受的 victim，rollback 其余 victim 并中止目标写；FilePerKey
  与 Offset 均使用 accepted-set 语义。Offset pending 通过 backend-local mutation
  reservation、write-key/expected-size/replaced-entry 与完整 victim
  `OffsetEntry`（含 generation）比较，阻止 stale commit 和 extent 提前复用；
- persistent LocalDisk mount 使用 `Mount(false) -> scan/preflight ->
  NotifyOffloadSuccess -> Mount(true)`；Master 仅接受已签发 task completion 或
  同 holder、同尺寸、Complete 的已知副本重关联，拒绝缺失对象、Allocating 和
  size mismatch；回归已改为显式 recovery transaction，并验证空 Master 不能被
  本地盘 inventory 反向重建；
- Offset 已增加 C++ v3 `kv_cache.data/meta` 到 Rust canonical layout 的严格
  离线 importer：按真实 `struct_pb + native raw allocator ABI + v3 record`
  调用链解析，校验 allocator partition、checkpoint seq、CRC、duplicate winner
  与 tombstone，并按 C++ Phase A/B 的 stale/missing/duplicate FIFO repair 语义
  生成稳定写入顺序；post-checkpoint、torn、unknown record 与 CRC 失败节点按
  C++ recovery 逐节点跳过并进入报告计数。随后使用独立 staging 回读后
  no-replace 原子发布且不修改源目录。由于 C++ raw allocator 没有 endian/ABI marker，
  importer 只接受当前已知 little-endian、28-byte Node ABI 和 canonical v3
  protobuf；跨 ABI 输入 fail-closed，不宣称为通用共享格式；
- remote-source fallback 已允许 master `NotFound` 进入回源；
- promotion 的生产默认参数已静态对齐：C++ 与 Rust 均为默认关闭、
  admission threshold `2`、全局 queue limit `50000`、单次 heartbeat 上限 `1`；
  Rust CLI、runtime config 与参数传播回归已固定这些值，当前仅待重新执行回归门；
- task 创建/领取/完成/超时已进入 snapshot barrier，native Copy/Move reservation
  与 source-pin 可从 Rust v3+ snapshot 恢复；v4 额外恢复对象 runtime deadline、
  task 类型和重试次数；C++ v1 catalog 通过可选 Rust sidecar 扩展，不改变 C++
  payload；
- GracefulUnmount 使用绝对 epoch deadline 并持久化到 native snapshot、catalog
  sidecar 和 oplog；standby 不执行，到 promotion 时从权威状态重建调度。旧快照
  只有 `GracefullyUnmounting` 状态而没有 deadline 时按已到期处理，避免永久卡住。

## 不计为 Rust 独有差异的共享问题

审计发现的 C++ 既有缺陷不能作为 Rust 功能正确的依据，例如 Drain 错误成功以及部分单 Upsert finalize 类型问题。修复优先级仍按用户可见影响评估，但验收目标应是正确语义，而不是机械复刻缺陷。

## 当前修复边界

当前分支已完成：

1. 写路径 hot-cache invalidation；
2. 远端 LocalDisk 读取边界与响应校验；
3. Upsert 的 `All`/LocalDisk 状态机；
4. stale Put 的 DashMap guard/remove 顺序；
5. scoped-key mutation coordinator；
6. tenant physical-replica quota ledger；
7. 精确 allocator snapshot restore 与 v1/v2/v3/v4 边界；
8. Drain terminal 状态机；
9. HA backend capability fail-fast；
10. HTTP metadata、remote fallback 与 `rpc_only`；
11. background/failure mutation oplog 收敛、task snapshot barrier；
12. native Copy/Move snapshot reservation/source-pin 恢复；
13. FilePerKey canonical protobuf envelope、显式 persistent lifecycle、
    C++→Rust 离线迁移，以及 Offset C++/mixed-layout 破坏前 fail-fast；
14. FilePerKey generation/reservation 两阶段并发写与运行期 namespace fail-closed；
15. 跨 tenant eviction partial-accept 的 commit/rollback 原子性；
16. FilePerKey/Offset persistent LocalDisk 重启扫描与严格 tenant-key preflight；
17. Master 授权的 LocalDisk 重关联、native snapshot v4 runtime deadline/task
    codec，以及 GracefulUnmount snapshot/catalog/oplog/promotion 恢复实现；
18. OwnedBatchId engine-bound 非复制所有权、raw submit unsafe 边界、Store native
    quiescence 屏障、本地 memcpy checked arithmetic 与 Engram safe slice/copy
    适配；
19. Client typed read/write region、registration generation/in-flight lease、
    overlap fencing、Python/Dummy 精确 generation 注销，以及 Python owner 随
    Engram ownership 转移；Python registration 同时校验 writable、contiguous
    与容量并持有 exporter lease；
20. FFI owner-bearing `RegisteredMemory`、engine-bound generation、read/write
    capability、RAII unregister、busy lease fencing 与 failed-unregister
    fail-closed tombstone；Python/Dummy owner 已直接移入该 FFI handle。
21. `ClientTransferEngine` background reaper、可转移独占 staging lease、全 submit
    路径 owner handoff 与 LocalDisk remote-batch release ordering；future
    cancellation 不再提前复用或释放 native payload。
22. CUDA/ROCm DLPack validator：已有 managed capsule、base/capacity/contiguity/
    read-only 校验逻辑，但 accelerator registration 当前整体 fail-closed。精确
    `cuda:N/hip:N` native identity 与 DMA-safe producer synchronization 拆为独立
    可选增强，不计入经典 Store parity，也不通过修改 TE C++ 假装完成。
23. Rust Client ownerless address-only register/unregister 已删除；15 个 zero-copy
    operation 入口只在地址解析成 live owner-bearing typed region 后使用，公开 API
    改为 safe，Python/Dummy 顶层不再调用 unsafe Store operation。
24. C++ OffsetAllocator v3 离线 importer：只读解析 native allocator，按 C++
    recovery 选择 live record、修复 FIFO，重建 Rust canonical Offset 后原子
    发布；不共享目录、不修改 C++ Store，skipped 与无 CRC 的合法 record 均在
    报告中单独计数。调用方必须在整个调用期间停写 C++ 源目录。
25. catalog Segment decoder 已兼容当前 C++ LocalDisk offloading task
    `[tenant_id, key, size]`，校验 task identity 与外层 storage key 一致，同时
    保留旧 `storage_key -> size` reader；现有 C++ payload fixture 已切换到当前
    task/capacity 形态。
26. classic TE reaper 已移除把 `TIMEOUT` 排除在 terminal 之外的错误特判；
    Busy batch 继续保活轮询，不可证明的永久错误泄漏完整 native job，Client
    析构不再无限 join 活跃 worker。
27. NoF 独立水位/分配压力淘汰已落地，并增加 NoF-only 水位触发、混合对象保留
    Memory 副本、hard pin 跳过及 CLI 参数边界的静态测试证据。
28. global DISK 数据面与容量事务：Master 为实际 resolved tenant 生成稳定路径；
    Client 使用 raw data + versioned tenant/key sidecar、同进程 mutex 与跨进程文件
    锁串行 mutation。写入/水位驱逐都先稳定选择 FIFO victim，再按 tenant 向
    Master 获取逐 key status；`0`/NotFound 才进入 accepted-set，本地只删除已接受
    victim。quota-disabled 保持 C++ 的不限制语义，quota-enabled 支持显式容量或
    文件系统 90% 容量；重启恢复、tenant remove-all 与 HA config fence 已补静态
    测试和 fail-closed 边界。
29. Batch CRUD 全数据面统一校验精确 completion 长度；TE 返回 terminal
    `Completed` 但 byte count 小于请求长度时，单/多 buffer、parts、range 与
    remote LocalDisk read 均失败，不再 finalize 不完整对象。
30. Copy/Move task worker 使用 payload tenant（legacy payload 缺省
    `default`），拒绝非本机 owned Memory source；transfer/End 失败 revoke，
    `NO_AVAILABLE_HANDLE` 有界退避重试，显式 tenant Client API 已补齐。
31. Query/BatchQuery/QueryByRegex 已按 C++ Client 语义收口；regex 路径使用
    `GetReplicaListByRegex`，只返回 Complete/routable replica、刷新 lease，并
    校验 tenant、user key 与 scoped key 三者一致。
32. catalog metadata/Segment/Task decoder 已改为 fail-closed：拒绝 trailing
    MessagePack、重复字段/对象/segment/task、allocator identity/usage 错配与
    非法 task payload；current task payload 保留 tenant-scoped identity。
    native/catalog restore 在 replacement barrier 前统一核对
    `TaskEntry.key` 与 payload tenant/key，并按 Copy/Move 类型验证 targets/target；
    legacy default-tenant payload 被 canonicalize，冲突不会污染 live state。
    native task UUID envelope、assigned client 和时间戳损坏也不会被静默修正；
    active task 缺 assigned client 会被拒绝，Drain 不再生成这种悬空任务。
33. C++ `host_id` 已贯穿 Rust Client、ReplicateConfig/Mount/ReMount proto、
    Master Segment/allocator、native snapshot v8、oplog、catalog 与管理查询。
    `LOCAL_FIRST` 优先使用请求 host_id，缺失时才回退到已挂载 client segment；
    segment logical name、TE endpoint 与 physical host identity 不再混用。
34. 通用 `transfer-engine-ffi` 已增加 owner-bearing safe registered submission
    状态机；raw pointer/unsafe 接口继续只作为底层 expert adapter，不要求修改
    Transfer Engine C++ 或 C ABI。
35. Client metrics 反向 API 审计发现并修复了原先每次 `/metrics` 请求只临时创建
    healthy/closed gauge 的差异。Rust Client 现在持有独立持久 registry，支持
    `MC_STORE_CLIENT_METRIC`、`MC_STORE_CLIENT_METRIC_BANDWIDTH` 与
    `MC_STORE_CLUSTER_ID`，并提供安全的 `serialize_metrics`、
    `summary_metrics`、`/metrics` 和 `/metrics/summary`。所有 tonic Master RPC
    由透明 channel wrapper 统一记录 count/latency，避免业务调用点遗漏或重复；
    Get/Put/Batch、zero-copy/multi-buffer/range/upsert 接口记录成功字节与延迟，
    TE/local-copy 成功路径记录物理传输字节，LocalDisk offload/promotion 记录
    SSD bytes/ops/latency。指标只观察已完成的成功 Store 操作，RPC 则按实际完成
    的调用统一计数。
36. Client hot-cache 反向配置审计补齐 C++ 默认关闭、正数总容量启用、16 MiB
    默认 block、1..255 admission threshold、固定内存 Count-Min Sketch 计数、
    单对象不超过 block，以及本地 Memory 只计频次而不重复复制的语义。Rust
    Client 创建时自动读取对应环境变量，并提供 enabled/block-count/admission-count
    查询；显式 `with_hot_cache` 为兼容旧 Rust 调用保持 threshold=1。C++
    `MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1` 依赖 memfd、TE memory registration、
    dummy acquire/release 与 IPC 映射，不能用普通 `Vec` 冒充，Rust 当前明确
    fail-fast 并将其保留为独立可选部署能力。
37. Client endpoint 反向配置审计补齐 hostname-only 自动端口预留、
    `MC_STORE_CLIENT_MIN_PORT`/`MAX_PORT`/`SETUP_RETRIES`、C++ 默认
    `12300..14300` 与有限 bind attempts。显式 hostname/IPv4/`[IPv6]:port`
    会严格校验，raw IPv6 作为无端口 host 进入自动分配，TE、Master segment、
    local endpoint 与 host identity 统一使用 canonical endpoint。Master
    `port_from_segment_name` 同时改为识别 bracketed IPv6，不再把 IPv6 的第二段
    错当端口；自动选择的 bound-but-not-listening socket 由 Client 持有到 teardown，
    对齐 C++ `AutoPortBinder` 生命周期。
38. Client HugeTLB 配置审计补齐 C++ `MC_STORE_USE_HUGEPAGE`（存在即启用）和
    `MC_STORE_HUGEPAGE_SIZE`（2 MiB/1 GiB）语义。local staging buffer、启动
    Store segment 与动态 segment 均经同一 memory adapter 分配；显式 C++ 开关
    下 mmap 失败或 mapping 不满足 allocator alignment 会返回错误，不再静默降级。
    旧 Rust `MOONCAKE_USE_HUGEPAGE`/`MOONCAKE_HUGEPAGE_SIZE` 仍作为兼容入口，
    保持原有 permissive fallback，但 C++ 名称优先并采用 strict failure。裸 mmap、
    page flags 与 pointer 生命周期继续封装在 `memory_ffi`，Client 顶层只调用安全
    `io::Result` API。
39. HTTP metadata timeout cleanup 已按 C++ 语义补齐。`MetadataState` 在 Master
    创建时一次读取 `MC_METADATA_CLUSTER_ID`，形成 `mooncake/` 或
    `mooncake/<cluster>/` 前缀；Client monitor 清理过期 Memory segment 时，除
    aggregate node 外同步删除精确 `ram/<segment>` 和
    `rpc_meta/<segment>` key。删除只作用于当前 cluster 与当前 canonical segment，
    不会误删其他 cluster/segment；Rust 当前只提供 co-located HTTP metadata
    server，不虚构尚不存在的 remote cleanup deployment。
40. Client metrics 周期输出已补 `MC_STORE_CLIENT_METRIC_INTERVAL`：缺失、`0`
    或非法值保持只采集不输出；正整数按秒启动 Client-owned Tokio reporter，
    teardown/Drop 会终止任务。每次报告包含持久 summary，并以相邻报告 snapshot
    计算 read/write interval delta、elapsed time 与吞吐；bandwidth 开关关闭时不
    输出 interval throughput。reporter 只持有 `Arc<ClientMetrics>`，不获取 Client
    业务锁，也不把指标状态下沉到 Master 或 FFI。
41. Python Client 创建入口已补默认 `tenant_id="default"`，空值按 C++
    `TenantId` 语义归一为 default；tenant 与 HTTP 配置现在从最初的
    `ClientConfig` 一次进入生命周期，不再先创建空 tenant Client 再事后修改
    字段。REST service 的 JSON config 同时透传 `tenant_id`，Python 层不持有
    tenant-scoped key 或 quota 业务逻辑。
42. 动态 owned segment 的公共调用链已补齐：Python
    `allocate_and_mount_segments(size)` 直接返回权威 UUID 列表与 allocator
    对齐后的实际容量，`unmount_and_free_segments(ids, grace_period_ms)` 在进入
    async Client 前严格解析 UUID。分配、`MC_MAX_MR_SIZE` 分片、TE registration、
    Master mount/graceful-unmount、native quiescence、unregister 与 owner free
    仍全部由 `mooncake-store-client` 承担，Python 不接触裸地址或 `unsafe`。
43. Rust Python 已补 C++ `TensorMetadata` v1 基础 codec：
    `_tensor_metadata_size` 固定返回当前 native ABI 的 304 bytes，
    `_serialize_tensor` 为 CPU contiguous tensor 生成相同 magic/version/header、
    dtype ordinal、global/local shape、data offset/size，并返回持有 contiguous
    tensor 的 owner；`_deserialize_tensor` 严格校验 metadata、shape/dtype
    expected bytes、layout axis 与 payload 边界后通过 PyTorch buffer owner
    materialize。big-endian 和 accelerator tensor 明确 fail-closed，后者继续属于
    已排除的 DLPack/device synchronization 能力。Rust package root 已导出三个
    helper，structured-object 不再因 helper 缺失退化为 `torch.save` 或跳过基础
    codec 路径。`MooncakeClient.put_tensor/get_tensor/upsert_tensor` 也已作为薄
    adapter 接入现有 Put/Get/Upsert 状态机；编码只在持有 contiguous CPU owner
    时立即复制，异步 Client 不持有 Python 裸地址。静态 Rust layout/corruption
    测试与 Python CPU scalar/empty/round-trip 测试代码已落地，当前未执行。
44. Tensor batch 与预注册 buffer API 已静态补齐：
    `batch_put_tensor/batch_get_tensor/batch_upsert_tensor` 复用 Client 的真正
    batch start/transfer/finalize 状态机，逐 key 保留 status/`None`，无效 tensor
    不阻塞同批合法 entry。新增 owned-slice `batch_upsert` 后，Python 不再以逐
    key loop 假装 batch；同时修复 BatchUpsert 把整组 `group_ids` 塞进每个
    single-key entry 的错误，现在先校验整批长度，再为每个 entry 选择对应
    group id。`put/get/upsert_tensor_from/into` 及 batch 变体只接受
    owner-bearing、C-contiguous、已注册 Python buffer，在 Store mutation 前
    严格验证 TensorMetadata；GetInto 返回引用原 buffer owner 的 PyTorch view。
    raw integer address 在 Tensor 专用入口 fail-closed，region lease 与传输
    quiescence 仍由 Client/FFI 保证。structured-object 已改为传 owner 而不是
    ownerless pointer。以上测试代码已落地，当前未执行。
45. legacy Tensor Parallelism 公共面已静态补齐：
    `put/get/batch_*_tensor_with_tp` 与对应 owner-bearing `from/into` 使用 C++
    相同的 `{base}_tp_{rank}` key；写入严格要求 uniform shard，逐 rank 调用
    PyTorch `narrow(...).contiguous()`，并编码 `layout_kind=SHARD`、global/local
    shape 及 TP axis 的 rank/count/split_dim/local extent。读路径只解析当前 rank
    的 shard，不错误拼成 full tensor；TP batch 只有全部 shard 成功才把 base key
    标为成功，无效 base tensor 不阻塞同批合法项。`tp_rank/tp_size/split_dim`
    在进入 Store 前完成边界校验，accelerator 继续 fail-closed。该 legacy
    路径与第 47 项 general parallelism manifest/writer-partition 多轴路由并存，
    不用 legacy key 冒充新的通用布局。
46. configurable tensor publish 与 safetensor 公共面已静态补齐：
    `pub_tensor/batch_pub_tensor`、legacy TP publish 及
    `upsert_pub_tensor/batch_upsert_pub_tensor` 复用已有 Client
    put/upsert 状态机，并按 C++ 规则校验 `preferred_segments` 与
    `replica_num`；TP batch 会把每个 base key 的 `group_id` 重复映射到其全部
    shard。`save_tensor_to_safetensor/load_tensor_from_safetensor` 复用
    `safetensors.torch` 与已审计 TensorMetadata codec；同步 Python 方法通过
    PyO3 管理的 Tokio runtime 驱动 Client future，载入结果先收窄为
    `PyDict`，再从其 list-valued `keys()` 提取条目，避免把 `dict_keys` 错当
    Python sequence。以上仍未执行运行回归。
47. general parallelism 公共面已完成静态对齐：
    Rust Python 已暴露 `ParallelAxis`、`TensorParallelism`、`ReadTarget` 与
    `WriterPartition`，按 C++ 规则验证 DP/TP/EP/PP、rank/size、split_dim、
    expert/stage id 及 canonical axis order。key codec 已覆盖 legacy single-TP、
    canonical multi-axis suffix、writer/parallelism manifest key 与 writer shard
    key。96-byte writer/parallelism manifest v1 codec 保留 C++ struct padding、
    header size 与八维 shape；TensorMetadata shard encoder 可写全部四类 axis。
    `put/upsert_tensor_with_parallelism` 的 single/batch 路径已接入：TP 请求把
    输入视为当前 rank 的 local shard 并推导 global extent，writer partition
    从 full tensor 提取当前 writer shard；需要时先写 shard 再写 manifest，与
    C++ 顺序一致。batch 保留逐 base-key status，并用 `ForSingleKey` 等价的
    group-id 投影执行 C++ 同样的逐请求路由。single
    `get_tensor_with_parallelism` 及 batch 变体已覆盖 as-stored、精确 shard
    （含 writer fallback）和 writer/parallelism full reconstruction。读取先走
    manifest；manifest 缺失、损坏或对应 shard 不完整时，按 C++ 顺序从 legacy
    TP 或 canonical multi-axis key 与 shard metadata 推导 stored shard count、
    global shape 和 split dimension。请求 TP size 与存储 size 不同时可从完整
    sources 均匀重分片；直接 requested shard 缺失时也可经 full reconstruction
    物化目标 shard。每个 source 的 canonical non-TP axes、TP rank/count、
    split_dim 和 global shape 均在拼接前验证。
    `put/upsert_tensor_with_parallelism_from` 的 single/batch owner-bearing 路径
    已补齐，其中 full TensorMetadata buffer 按 C++ contract 拆成所有 TP shard；
    `get_tensor_with_parallelism_into` 的 single/batch 路径只接受属于当前 Client
    的已注册、可写 owner range，重构结果写入调用方 buffer 并返回引用该 owner
    的 PyTorch view。C++ Tensor 公开方法名反向清单已归零；以上仍未执行运行
    回归，不能把静态对齐表述为生产验收完成。
48. Drain identity 已从 name-based 调度收紧为 exact UUID/type：
    active task 同时保存 source/target segment UUID、replica type、tenant-scoped
    task key 与 assigned client；MoveStart 按这些 durable/runtime authority
    校验 owner/name/status，并经 exact-ID allocator 分配。后挂载的同名 target
    不能劫持任务，NoF owner 也不再由 name 猜测；相关 authored tests 尚未执行。
49. 远端 LocalDisk offload read buffer 已改为 owner-bearing 有界 lease：
    pool 容量、GC TTL/interval 对齐 C++ 配置，reservation/commit/release 以精确
    bytes 和 RAII 管理；server 校验磁盘实际读取长度，registration owner 随 batch
    生命周期释放；client 校验 response identity/shape/endpoint/TTL，并拒绝超过
    lease 的 RPC+传输。相关 authored tests 尚未执行。
50. GracefulUnmount/segment-status HA producer 与 replay validator 已对称：
    producer 不再写出空 identity、nil UUID、零 deadline、空/重复/非法状态批次；
    replay 在任何 segment mutation 前验证旧 intent owner，snapshot restore 在
    replacement barrier 前拒绝 malformed sidecar。拒绝路径 mutation-atomic 的
    authored tests 已落地，尚未执行。
51. 通用 Copy/Move（非 Drain）也已移除 name-first authority：
    CopyStart 将每个 legacy target name 唯一解析成 Active UUID/type 后按 exact-ID
    allocator 分配，并校验 source 的 exact owner；CreateCopyTask/CreateMoveTask
    只接受唯一、Complete、handle-valid 的 Memory/NoF source 与跨 Memory/NoF
    唯一 target。重复/同名歧义只会 fail-closed，不会把任务交给错误 client。
    offload/eviction 的 source owner fallback 也改为 UUID/type/name 精确匹配。
    非 owner Copy 与同名 target 的 authored tests 已落地，尚未执行。
52. Copy/Move 终态与 runtime task durability 已完成反向复核：
    C++ 合法的空 target/no-new-target Copy 继续创建零 reservation task，并由薄 Client
    直接 `CopyEnd`；existing-target Move 在 wire 上返回 `target=None` 跳过数据传输，
    但 Master 内部以 exact descriptor 保存既有 target。该字段由
    `replication_start` v2、native snapshot v12 与 catalog
    `rust_replication_tasks_v2` 一致持久化，reader 对旧 v1 继续只读兼容。
    CopyEnd 会把 invalid allocated target 从 object image 脱离，并在 allocator
    range 可复用前完成 reservation/tombstone 持久化；MoveEnd 只有在 newly
    allocated 或 existing exact target
    仍 Complete/handle-valid 时才移除 source，否则只回滚本次新 target、释放 source
    pin 并保留源副本。事务性 object clone 显式保留其他 runtime refcnt。oplog
    replay、native/catalog snapshot 与终态失败 authored cases 已落地，尚未执行。
53. Master live-object 事务投影已统一保留 runtime replica refcnt：
    `ReplicaDescriptor::clone` 为 response、task 与 durable image 有意把运行时
    refcnt 清零，但此前部分 clone/validate/commit 路径会把该 clone 写回权威对象，
    从而丢失与本次 mutation 无关的 Copy/Move/promotion/offload pin；tenant quota
    eviction 甚至可能在候选扫描确认“存在 idle replica”后把 busy replica 也当成
    idle 删除。现由专用 `clone_object_for_mutation` 恢复逐 replica refcnt，并用于
    PutEnd、PutRevoke、BatchReplicaClear、stale-handle cleanup、expired Put reaper、
    tenant quota eviction、Copy/Move End、legacy put-end replay、lease-refresh replay
    及 segment-unmount replay。普通 response/durable clone 仍保持 refcnt 清零。
    克隆契约与“busy 保留、idle 驱逐” authored tests 已落地，尚未执行。
54. Drain terminal failure 已恢复 C++ 的 source reopening 语义：
    过去 Rust 在 unit 重试耗尽后把 job 标记为 `Failed`，但永久保留 source
    `Draining`，导致 allocator 容量和后续 Drain 都被无期限阻塞；C++
    `MaybeCompleteDrainJob` 会把仍存在且未卸载的 source 恢复为 `OK`。Rust 现在
    先按持久化的 exact UUID/type/name authority 验证全部 source，以单条
    `segment_status_batch` durable 记录恢复 Memory/NoF source 为 `Active`，然后
    才发布 job `Failed`。仍有 replica 引用的 source 缺失或 identity/status 漂移
    会 fence；已经显式卸载且为空的 source 不按名称重建。Memory 失败后重新创建
    Drain 与 NoF 三次失败恢复的 authored tests 已落地，尚未执行。
55. HA oplog backend 的 commit boundary 与损坏恢复已改为 fail-closed：
    LocalFS 新写入使用 `MCOPLG02` 帧，完整保存 `u64 sequence`、
    `producer_view_version`、payload length 与 xxh32；历史
    `[u32 sequence, u32 length, payload]` v1 仍只读兼容，但截断 header/payload、
    非法 UTF-8、checksum 错误、文件名/首记录不一致、段内/跨段 sequence gap
    均拒绝启动。所有 segment 都会在恢复时校验；v2 segment 必须受原子
    `latest` commit pointer 覆盖，不能把“segment rename 成功、latest 失败”的
    记录误当成已提交 mutation。满 segment flush 已从丢错的后台线程改为同步
    file fsync + rename + directory fsync；`latest` 与 snapshot sequence 同样先
    写临时文件、fsync、rename。任一不确定持久化失败会 poison 当前 store，
    后续读写不再替另一调用者提交先前已报错的记录。
    Etcd startup 不再把连接、UTF-8 或 sequence parse 错误降级为 0；entry 与
    `/latest` 改为单个 transaction 原子提交，range/watch 同时校验
    key/value sequence，range gap 与 orphan entry 会失败。promotion 最终追平
    使用可返回错误的 backend max/commit 查询；读取失败、无进展、超时或未追平
    都进入 `PromotionFailed`，不再 `break` 后发布成功。u64 边界、legacy v1、
    corruption/commit-pointer 与 promotion read failure 的 authored tests 已
    落地，尚未执行。
56. Snapshot object/catalog 的本地发布与 listing 错误边界已收紧：
    `LocalFileSnapshotObjectStore` 不再直接覆盖最终对象；manifest、descriptor、
    payload 与 `latest.txt` 统一在同目录临时文件完成 write + file fsync 后
    rename，并逐级 fsync 到 snapshot root。prefix delete 同样在删除后同步目录，
    native snapshot history copy 在裁剪旧副本前先同步新副本和 history 目录，
    裁剪后再次同步。Embedded 空 latest marker 现在视为损坏；catalog listing
    只对明确 NotFound 或 descriptor decode 失败跳过单个 candidate 并记录原因，
    object-store I/O/连接错误直接传播，不能伪装成“没有 snapshot”。删除当前
    Embedded latest 时也不再吞掉 marker 删除错误。原子本地 publication 与空
    marker 的 authored tests 已落地，尚未执行。
57. Standby continuous replay 的 semantic failure 不再伪装成健康连接：
    notifier 收到正好等于 expected sequence、但 payload/schema/状态前置条件无法
    apply 的记录时立即触发 `FatalError`；polling fallback 若首记录不是 expected，
    或非空 batch apply 后 expected 完全不推进，同样进入 `Failed` 并停止 follower。
    `Failed` notifier 的后续 close/error callback 不再把同步状态覆盖回
    `Reconnecting`。这使未知 future op、损坏 payload 与不可恢复 gap 在日常跟随
    阶段即 fail-closed，而不是只在 promotion 时才暴露。polling semantic reject
    authored test 已落地，尚未执行。
58. Etcd leadership view 的读取与 acquire 后确认已去除 fabricated fallback：
    非 UTF-8/空 leader address、零或负 mod revision 直接拒绝；空申请地址和非正
    lease TTL 在 grant 前拒绝。CAS transaction 成功后必须线性读回同一 owner
    且存在的 master view，不能以请求地址和固定 version 1 伪造成功结果；transport
    error、读回缺失/owner mismatch/损坏都会先 revoke 本次 lease，以覆盖“请求
    已落地但响应丢失”的不确定结果，再返回错误。纯 decoder authored test 已
    落地，尚未执行。
59. Leadership session/keepalive 与 service mutation gate 已形成同一 term fence：
    acquire 成功即登记 active owner session 并发布本地 Leader role，重复 acquire、
    stale renew/release/start-keepalive 均拒绝，result view 必须与 session view
    一致。keepalive 已前移到 standby promotion/final catch-up 之前，长时间追平
    不再暴露无续租窗口。Etcd 后台 keepalive 与 warmup renewal 都会消费有界 ACK，
    校验 stream lease id、response lease id 与正 TTL；仅把请求写入 sender 不再算
    续租成功。ACK timeout、stream close、错误、id mismatch 或 TTL<=0 都清除
    active token 并发布 Standby。LeadershipMonitor 观察到 Standby 或 role channel
    close 时先同步关闭 `service_available`，排空 mutation gate，再请求 gRPC
    shutdown，不等待 graceful server 完全退出才停止写入。远端 revoke 失败也
    无条件完成本地 demotion/owner 清理，只允许远端 lease 自然过期。session
    ownership 与纯 ACK/interval authored tests 已落地，尚未执行。
60. HA snapshot scheduler 已纳入同一 serving gate：
    leader 定时任务优先消费 shutdown signal，失租或 demotion 后不会再启动 native
    save 或 catalog publish；即使 tick 已经触发，native writer 会在调度、global
    mutation barrier 内和 backend commit 前重复检查 `service_available`，catalog
    capture 也会在 capture 前后检查 gate。这样 graceful gRPC shutdown 期间不再由
    已降级 standby 发起新的 snapshot publication。非 serving native-save 的
    authored test 已落地，尚未执行。
61. Snapshot retention/delete 的异常顺序已改为 catalog-first：
    Embedded 先删除 latest marker（仅删除当前项时）和 descriptor，再清理 snapshot
    payload prefix；Redis 先以单个 atomic pipeline 移除 sorted index 与可选 latest
    key，再删除 object-store payload。后半段失败只留下恢复路径不可见的孤儿对象，
    不再留下 catalog 可枚举但 payload 已被删除的坏 candidate。Embedded payload
    cleanup 注入失败的 authored test 已落地，尚未执行。
62. Snapshot restore/retention 已增加跨 term 单调顺序：
    `latest` 不再被当作跨 leader 的排序 oracle；provider 合并 marker 与 published
    listing 后按 `producer_view_version`、`last_included_seq`、`snapshot_id`
    依次降序，retention 使用同一顺序。旧 term 已进入同步 object-store I/O 的
    snapshot 即使最后覆盖 marker，也只能成为较早 fallback，不能隐藏或裁掉
    successor baseline。late old-term marker 的 authored restore/prune test 已
    落地，尚未执行。
63. Admin HTTP tenant-quota policy 不再只依赖 handler 的瞬时 gate 检查：
    `upsert/delete_tenant_quota_policy_for_tenant` 在 Service 层进入
    `begin_background_mutation` epoch，demotion 会等待已进入 connector save 的
    policy mutation 完成，关闭 gate 后的新写直接返回 Unavailable。这样 handler
    检查与实际 connector/ledger commit 之间的竞态不能让 standby 改写 policy。
    demotion 后 upsert/delete 不改变内存 ledger 的 authored test 已落地，尚未执行。
64. Tenant-quota policy connector failure 已纳入 durability fence：
    file connector 的 temp write/file fsync/rename 后不再吞掉 parent directory fsync
    错误，相对路径也同步当前目录；Etcd put 或 file publication 任一错误都按
    commit outcome 不确定处理，关闭 `service_available` 并标记当前 Master
    durability-fenced，内存 ledger 不提交候选 policy。下一任只能从 connector
    重建，不能让“外部 policy 可能已更新、当前内存仍为旧值”的 term 继续服务。
    policy save failure 的 authored test 已更新为验证 Unavailable + fenced，尚未执行。
65. 生产 HTTP metadata server 已绑定 Store serving gate：
    standalone 与 HA listener 的 GET/PUT/DELETE 都在 handler 读取或改写 TE bootstrap/
    routing metadata 前取得与 tonic 共用的 owned foreground request guard，跨越
    完整 async handler future；PUT/DELETE 在取得 metadata lock 后还会进入
    background mutation epoch，使 demotion 同时排空读取和已进入的修改并拒绝之后
    的请求。因此 tonic graceful shutdown 尚未返回的窗口内，旧 leader 端口返回
    HTTP 503，不再暴露或接受 `rpc_meta`/RAM metadata。无 gate listener 仅作为
    显式低层测试 API 保留。gate 关闭后的 GET/PUT authored integration test 已落地，
    尚未执行。
66. HA tenant-quota policy connector 已完成跨 term 单调 commit：
    policy v1 YAML 增加可选 `producer_view_version`，legacy C++/Rust policy 缺失时
    读取为 0；Service 使用独立、只在 leadership acquire 时设置的 term 字段生成
    snapshot，不复用会因 segment topology 变化而递增的 Client-visible
    `view_version`。file connector 使用持久 sibling lock file 的
    `flock` 独占区，在锁内重读当前 policy、拒绝较小 view，再执行原子 write/fsync/
    rename/directory-fsync。Etcd connector 线性读取 value/mod_revision，严格解析
    当前 view，以 `mod_revision == observed`（不存在时 `version == 0`）transaction
    CAS；竞争会重读，八次无进展或任何 transport ambiguity 都返回错误并由 Service
    durability-fence。因而旧 leader 的慢写即使晚到也不能覆盖 successor policy。
    legacy view=0、leadership/topology view 隔离、file stale-writer 与纯
    monotonicity authored tests 已落地，尚未执行。
67. Tonic interceptor 与实际 handler mutation 之间的排队竞态已关闭：
    仅在 interceptor 检查 `service_available` 不足以覆盖“请求已进入、随后等待
    operation/key stripe”的窗口。全部 74 个 `MasterService` trait 方法现在在委托
    `*_impl` 前取得 owned foreground request guard，guard 跨越完整 async future，
    不会随 `Request::into_inner` 提前释放。demotion 先原子关闭 serving gate，
    再等待 foreground counter 归零，之后才排空 background mutation 与 global
    key barrier；gate 关闭后无法登记新 guard。因此所有已通过 interceptor 的读写
    RPC 要么在 demotion 返回前完整结束，要么在登记时返回 Unavailable，不能在
    standby term 获得 stripe 后继续。foreground/background 登记同时检查
    `service_fenced`，覆盖 fence flag 与 availability store 之间的极短窗口；LocalDisk
    object disappearance 也已统一走 invariant-fence helper。纯 gate reject/drain/
    fenced authored test 已落地，尚未执行。
68. Irreversible durability fence 现在会终止 server/leader term：
    过去 fence 只关闭 `service_available`，coordinator keepalive 仍可无限续租，
    形成永久 Unavailable 却阻止 standby 接管的 leader。standalone 与 HA 的 tonic
    shutdown future 现在同时观察 process watch 和 `service_fenced`，最多 100 ms
    发现 fence 后同步排空 foreground/background/key mutation epochs，再停止接收
    并退出 server；HA 随即沿现有 cleanup 路径关闭 keepalive、revoke/release
    session 并回到 candidacy。server shutdown predicate 的 authored test 已落地，
    尚未执行。
69. Etcd oplog 的 writer 已绑定 leadership election revision：
    standby 使用的 `EtcdOpLogStore::new` 现在是显式 reader-only，任何 mutation
    都拒绝；promotion 不再复用这个 reader，而是以 acquired
    `MasterView.view_version` 和同一 `master_view` key 构造 leader writer。
    append transaction 同时比较 election key `mod_revision`、`/latest` 的期望前序
    值和每个新 entry key 的 `version == 0`，再原子写入 records 与 commit pointer；
    batch 必须 sequence 连续、末项等于本地 latest，且所有 record 的
    `producer_view_version` 与 writer term 完全相等。`UpdateLatestSequenceId`、
    snapshot-sequence publication 与 cleanup range 也改为 election-fenced
    transaction。CAS 失败或 transport outcome 不确定会 poison Store，继而触发
    已有 durability fence/leader-term shutdown；lease 删除重建后，旧 leader
    即使网络恢复也不能覆盖、推进或清理 successor 的 oplog。producer-view 与
    contiguous-sequence、snapshot sequence corruption 的纯 authored tests 已
    落地，尚未执行。
70. Tenant-quota connector 已补 leader acquire 后、serve 前的 term barrier：
    单靠 policy value 中的单调 `producer_view_version` 仍存在窗口——successor 已
    获得 election，但尚未进行首次 quota mutation 时，旧 leader 的延迟写仍可能
    看到 connector 中的旧 term 并成功。Rust HA 现在在 standby final catch-up
    完成后、打开 service gate 前，先在 file 的同一 `flock` 或 Etcd 的同一
    mod-revision CAS 中读取 winning policy、保留其 quota 内容并只把 term 推进到
    acquired view。旧写因此要么排在 barrier 前并包含在返回 snapshot 中，要么
    排在 barrier 后因 stale term 被拒绝；新 leader 再以返回 snapshot 替换内存
    policy layer，同时保留 standby 已恢复的 used/reserved/object accounting。
    barrier 失败不会进入 serving，会释放 leadership 后重试。file barrier、
    runtime-ledger preservation 与 prior-winner absorption 的 authored tests 已
    落地，尚未执行。
71. allocator release 已补齐 C++ `AllocatedBuffer` 所代表的精确所有权语义：
    Offset allocator 不再只维护可合并 free range，而是按起始 offset 记录每个
    live allocation 的精确请求大小；Cachelib allocation 同样保存 requested
    size。释放必须匹配 live offset、精确 size、segment identity，并拒绝 stale、
    double-free、同批重复 target。Memory 与 NoF 混合释放会按固定锁序同时取得
    allocator，在修改任一 allocator 前预校验完整批次，避免半释放；usage 使用
    checked subtraction。snapshot/oplog allocator rebuild 同时核对普通 segment
    与共享 CXL alias 的 UUID/name identity，损坏 descriptor 在安装恢复状态前
    fail-closed。运行期权威 metadata 与 allocator 不一致时不再制造空闲空间或
    继续回放，而是返回 Unavailable、durability-fence 当前 Master，等待重启从
    durable metadata 重建。Offset stale/size mismatch、批次原子性、restore
    identity 和 Service fence 的 authored tests 已落地，尚未执行。
72. HA replay 的 remove/legacy completion 已改为真正 fail-closed：
    remove/put-revoke 在修改 live metadata 前已经从 post-record object 与 delayed
    reservation 集合构造并验证完整 allocator candidate，因此 replay 不再对旧
    runtime allocator 做冗余 release。旧 allocator 即使与权威 metadata 分叉，
    也不会出现“对象已删除、release 失败、sequence 未推进”的非幂等半提交；replay
    会清理 runtime offload/promotion 索引并直接安装 candidate。legacy `put_end`
    不携带 replica image，只允许完成 snapshot 中已存在的对象；对象缺失时保持
    expected sequence 和 processing marker，不再把无法重建的对象静默跳过。无
    重建字段的 legacy Memory/NoF mount 也只有在 snapshot 已包含唯一同名 topology
    且 allocator 存在时才作为 informational record 推进，否则等待可用 baseline。
    NoF ReMount proto 缺失 UUID 时不再生成随机 identity，而是保留 nil 并由入口
    返回 InvalidArgument。stale-runtime remove、missing-object legacy put-end、
    legacy mount baseline 与 missing NoF UUID 的 authored tests 已落地，尚未执行。
73. Store Client 的 Transfer Engine 顶层调用已移除残留 unsafe/raw submit：
    过去 `ClientTransferEngine::submit_transfer` 虽然是 safe crate-private 方法，
    内部仍直接调用 `unsafe submit_owned_transfer`，正确性依赖每个业务调用点把
    raw pointer owner 另行塞进 reaper payload，未由类型系统证明。现在内部
    staging allocation 在创建时直接消费进 FFI `RegisteredMemory` owner；
    本地填充/读取要求当前不可复制 staging lease，且 `RegisteredMemory` 只有在
    没有 region/native in-flight lease 时才允许 bounded local copy。普通 read/
    write、multi-buffer、put-parts、batch-get、range-read 与 LocalDisk offload
    全部构造 typed `RegisteredTransferRequest`，zero-copy 只能使用 registry
    解析出的 readable/writable generation capability。提交返回的
    `SubmittedRegisteredBatch` 同时持有 batch、requests、registration claims
    和 owner，native reject 也必须交给 reaper；只有 `try_release` 证明 quiescence
    后才释放 capability 并关闭 segment。Client 业务目录已无 unsafe block、
    raw `TransferRequest` 或 `OwnedBatchId` 提交语义；unsafe 只保留在
    `transfer-engine-ffi` 与 Client 的 memory/accelerator adapter。replica
    base/offset/relative address 现使用 checked arithmetic；typed request 构造在
    batch 产生前失败时会立即关闭已打开 segment，避免安全校验新增资源泄漏。
    staging exclusive-lease 与 address-overflow authored tests 已落地，尚未执行。
74. Memory/NoF mount/remount 的 topology/allocator identity 已改为全局
    fail-closed：live mount 与批量 remount 不再允许 Memory 与 NoF 使用同一个
    segment UUID；正反两个方向都会同时检查对方 topology 与 allocator。若本类型
    UUID 只存在于 allocator，或已有 topology 却缺少 allocator，Master 会在写
    oplog 或 rebind 前返回 Unavailable 并 fence，既不覆盖 allocator 中的
    live/quarantined range，也不把内部状态损坏伪装成普通客户端冲突。remount
    会先预检整批，再提交任一新 segment；同时拒绝同批 Memory runtime range
    重复及 NoF endpoint 重复。legacy Memory remount 未携带 `segment_ids` 且没有
    snapshot topology baseline 时，不再生成随机 UUID，而是复用普通 Mount 的
    canonical `stable_memory_segment_id` 完整 runtime fingerprint；相同请求重试
    因而解析到同一 identity。Memory/NoF remount 也复用普通 Mount 的 Cachelib
    `u32` slab-count 上限，不允许批量重挂绕过 allocator identity 表示范围。
    HA replay 使用相同的跨类型 UUID 排他规则，并重新验证 follower 的
    `enable_cxl`/`cxl_size`、`enable_nof`、Cachelib alignment 与 slab-count
    capability；配置漂移或损坏记录不能恢复出当前 Master 无法发布的 segment。
    allocator-only、topology-only 或跨类型冲突均不推进 expected sequence；
    尤其不会通过 `add_segment` 把已有 used bytes 重置为零。Memory/NoF
    allocator-only、topology-only、跨类型 live mount/remount、legacy stable
    identity、Cachelib capacity、整批 preflight、replay capability drift 与
    collision authored tests 已落地，尚未执行。
75. Snapshot segment topology 安装前校验已与 live/replay identity contract
    收敛：Memory/NoF segment 必须具备 non-nil UUID/client、非空名称与非零容量，
    各类型内 UUID 唯一且两类 topology 不得共享 UUID；`enable_nof=false` 不再
    恢复 NoF topology，NoF 也拒绝没有对应 scheduler/state machine 的
    `GracefullyUnmounting` 状态。Cachelib snapshot 在创建 layout 前校验 slab
    alignment 与 `u32` slab-count 上限，避免先执行截断 cast/构造 allocator、再由
    descriptor rebuild 迟到报错。所有失败均发生在唯一 live-state replacement
    barrier 前，原有 live object/segment 保持不变。cross-type UUID、disabled
    NoF、unsupported NoF status 与 Cachelib preflight authored tests 已落地，
    尚未执行。
76. 同尺寸原地 Upsert 的 durable generation/quota 语义已收敛：
    此路径复用原有 Memory allocation，所以 replica 暂时从 Complete 回到
    Allocating 时，`quota_committed=true` 与原 committed physical charge 必须
    保持不变；它不是一笔新的 reservation。过去 typed oplog 只按 Complete
    Memory 反算 committed charge，导致 UpsertStart image 在 append 前被当成
    quota corruption 并 fence。现在 durable charge 根据 `put_start_time` 与
    Put/Upsert write-target 状态区分原地 generation 和无 PutStart authority 的
    Copy/Move target；PutEnd、Revoke 与过期 reaper 在终态清除
    `put_start_time`。snapshot 与 typed replay 会恢复 processing marker、既有
    committed charge 和 allocator ownership，但不会把 inflight generation
    提前加入 readable client index；无 durable task authority 的 promotion
    staged target 仍按 orphan 处理。snapshot 对未提交对象的 reservation 改为
    始终从 Allocating Memory descriptor checked 重算，不再信任任意非零持久
    账值。legacy `put_end` 也可完成 snapshot 中已经 committed 的原地 Upsert，
    同时拒绝 record size 与 authoritative object size 漂移。helper、PutEnd、
    snapshot、typed replay 与 legacy replay authored tests 已落地，尚未执行。
77. mutation RPC 的 `replica_type` selector 已改为 fail-closed：
    过去 request conversion 会把未知 i32 静默回退为 Memory，畸形
    PutEnd/UpsertEnd、PutRevoke、BatchRevoke 或 Disk eviction 因而可能完成、撤销
    或删除错误的 Memory replica。现在 descriptor decode 仍保留历史 wire fallback
    以读取 legacy metadata，但所有 client mutation selector 使用独立 checked
    conversion。单请求与整批共享 selector 返回 InvalidArgument；逐 entry
    Put/Upsert batch 保留既有 per-item `InvalidState`，坏 entry 不进入 key mutation
    或对象状态机。selector authored test 已落地，尚未执行。
78. legacy HA `put_end.size` 的缺失语义与畸形输入已分离：
    缺失或 Null 仍表示旧 completion marker 没有携带 size，可使用 snapshot
    authoritative object；字段存在时则必须是 u64。过去字符串/对象等错误类型会
    经 `unwrap_or(0)` 伪装成缺失并绕过 size drift 校验。现在畸形 record 不修改
    object/processing marker，也不推进 expected sequence。authored replay test
    已落地，尚未执行。
79. Redis/K8s leader coordinator 的 term 与 session fencing 已补齐：
    Redis acquire 不再把缺少 term 的 `{1}` 响应伪造成 view 1，只接受精确
    `{0}` 或 `{1, positive_view}`；leader address、TTL 乘法/转换、持久 view=0
    与 renew/release 返回码均 fail-closed。keepalive 使用 TTL/3 的 bounded
    interval，不再固定等待 3 秒；首次连接或后续重连失败会同步 demote 并清除
    active owner token。K8s held Lease 必须带正数 `lease_transitions`，任何新
    acquisition（包括相同 advertise address 的进程重启）都会 checked 增加 term；
    未过期 Lease 即使 holder address 相同也不能被重新获取。renew/release 同时
    比较 holder address 与 session term，并拒绝过期 session 续约；API 成功响应
    不再用本地 fallback view 掩盖缺失/错误 Lease。Redis result/interval 与 K8s
    view/term/same-address/session authored tests 已落地，尚未执行。
80. task completion mutation 的未知状态已使用 checked request decoder：
    durable C++ catalog task decoder 原本已经严格拒绝未知 type/status；gRPC
    `MarkTaskToComplete` 过去却先把未知 i32 回退为 Pending，再依赖后续
    terminal-only 检查间接拒绝。现在 request 边界直接返回 InvalidArgument，
    不再把畸形协议值伪装成合法内部状态。pure conversion 与 RPC authored tests
    已落地，尚未执行。
81. Etcd 竞选失败后的 discovery view 已改为事务后权威读取：
    过去通用 coordinator 会在 campaign 前先读取 current view，并在 Etcd
    create-only transaction compare 失败后优先返回该缓存。若 leader 正好在两者
    之间切换，调用方可能得到已经失效的地址/term。现在 acquire 不再接收
    pre-campaign view；失败 compare 会先撤销本次未获胜 lease，再在事务提交点
    之后重新读取 election key。成功路径仍要求 key 存在、地址与本次 contender
    精确一致、key 绑定的 lease ID 与本次 grant 精确一致且 mod revision 为正数，
    不用本地 fallback 伪造 view。discovery 同时拒绝 lease ID=0 的持久 election
    key；session token 也不能解析为 0。这样旧 lease 在成功事务后、回读前失效，
    且同地址 successor 接管时，旧进程不能把 successor 的新 term 绑定到自己的
    已失效 session。address+lease pure authored tests 已落地；事务后读取的并发
    顺序由 Etcd transaction/read call chain 静态审查覆盖。仓库没有现成 fake
    Etcd，按当前“不构建、不启动测试环境”约束未添加伪运行时证据。
82. 任务队列已重新以 C++ `TaskManager` 为直接 oracle 逐分支收敛：
    `FetchTasks(batch_size=0)` 按 C++ `result.size() < batch_size` 返回空，不再
    解释为无限；pending 上限检查、UUID 未占用检查与任务插入由同一全局
    mutation barrier 串行，所有 Copy/Move/Drain task producer 都复用
    `unique_task_id`，对应 C++ write access 内的 UUID 碰撞重试。新任务 message
    保持空字符串，只有完成/超时写入；任务已经终态后的任意 completion retry
    都与 C++ 一样返回成功并保留第一次结果，不再额外比较重复请求的
    status/message。未知 completion enum 仍在边界拒绝，因为 C++
    `is_finished_status` 同样只接受 SUCCESS/FAILED。对应 authored tests 已更新，
    尚未执行。
83. Upsert 抢占与 delayed-release HA image 已按 C++ 锁内最终状态修复：
    C++ 移走 PROCESSING replica 后，若没有 COMPLETE survivor，会在同一对象锁
    中删除 metadata。Rust 过去先持久化 `Some(empty/incomplete object)`，再删除
    并写第二条 record；leader 在两条之间退出会恢复出 C++ 不会提交的中间对象。
    现在所有旧 allocator replica 与 `object_image=null` 合并进一条
    `object_delayed_release_batch`。若 authoritative object 是 committed
    same-size Upsert generation，回放使用 `object_has_inflight_write`，不能仅凭
    `quota_committed=true` 提前放入 readable client index。atomic producer 与
    committed-inflight replay authored tests 已落地，尚未执行。
84. Drain 公开状态与后台 task refresh 已按 C++ 状态机收敛：创建、运行、成功、
    失败和取消 message 分别使用 C++ 文本；空转 refresh 不再无条件推进
    `last_updated_at`。active task 从权威任务表消失时立即把 unit 标为 terminal
    failed，而不是重新调度一个已经没有结果 authority 的任务；Created job 在
    scheduling 进入 Planning，满并发时仍推进 Running。成功源 segment 的 C++
    `DRAINED` 在 Rust proto 中由既有 `Unavailable` 表示，这是已记录的枚举模型
    适配，不改变其不可再分配语义。
85. Copy/Drain 重复 target 行为已恢复 C++ 规则：C++ 只要求 Drain source
    唯一，不拒绝重复 target；CopyStart 也逐 target 计数和分配。Rust 不再在
    CreateCopyTask、CopyStart 或 CreateDrainJob 提前施加 target 唯一性规则。
    Rust 因同名 segment 可对应多个 UUID 而保留的“name 必须解析到唯一 active
    physical segment”检查单独属于 identity 适配，不冒充 C++ 原始分支。
86. 本轮重新分类审计证据：Redis/K8s/Etcd coordinator fencing、Remote Pull
    ownership/order 与 Rust oplog durability 是 Rust replacement/HA hardening，
    不再作为 C++ Store 某个直接入口已经对齐的证据。经典 parity 结论只引用
    C++ `MasterService`/Client/TaskManager 的可定位分支；Rust-only 增强仍需
    自身 producer/replay 对称性，但不能替代 C++→Rust 功能证明。

下一阶段剩余工作：

1. Client 公共 API/config 反向清单的生产逻辑已静态闭环；剩余部署证据与条件
   差异为：
   - 普通 KV Client 的 tenant-aware create 与动态 owned segment Python 薄绑定
     已完成静态修复；TensorMetadata CPU 基础 codec helper 已补齐。
     Tensor-specific batch 与预注册 buffer from/into 已完成静态修复。
     legacy TP、safetensor、configurable publish 与 general
     parallelism/writer manifest 已完成静态修复；该独立 Python integration
     surface 已使用专用 key/metadata/manifest/from/into 路径，而不是以 generic
     bytes fallback 代替；
   - hostname-only 自动端口、有限重试与 IPv6 canonical endpoint 已完成静态
     修复；当前只缺按用户约束延后的运行时 bind/TE registration 证据；
   - hot-cache SHM/dummy acquire-release 若属于目标部署，需在 Rust Client/FFI
     安全适配层设计 owner-bearing memfd mapping 与注册生命周期，不修改 C++
     Store 或 TE C++。普通进程内 hot cache 已不再是功能缺口；
   - HugeTLB 的 C++ 环境变量名、2 MiB/1 GiB page-size 选择与 strict failure
     已完成静态修复；真实 HugeTLB reservation/population 证据按当前约束延后；
   - HTTP metadata 的 key-scoped CRUD、cluster prefix 与过期 Client segment
     cleanup 已完成静态修复；
   - `MC_STORE_MEMCPY`、`MC_STORE_NUMA_SOCKET_ID`、mmap arena 与 NoF worker/
     queue 环境变量是性能/拓扑调优项，不改变 Store 对象状态机，列为 P2 或目标
     硬件部署验收；Redis username/password/db 可通过 Rust Redis URI 表达，不是
     能力缺口。
2. 补 Offset importer 的真实 C++ v3 fixture，以及由 C++ producer 一次
   生成并以 `include_bytes!` 固定读取的 catalog golden：metadata
   v1/v2-data-type/v2-hard-pin/v3/current-v4、legacy/current Segment。当前
   LocalDisk task decoder blocker 已修，但现有 fixture 仍由 Rust 运行时拼装，
   不能作为独立 C++ wire 证据；Offset 反向 exporter 不属于“Rust 平替 C++”
   必需路径，除非另行明确要求，不再作为 parity blocker。当前测试中的 helper
   已统一显式命名为 `synthetic_cpp_*`，只作为 audited wire-shape decoder
   coverage，禁止把它们报告成 C++ producer golden。仓库内未发现可复用的真实
   C++ 二进制产物；按当前“不构建”约束，该证据保持未完成而不伪造 provenance；
3. global DISK 运行时/故障注入证据、CXL 真实 DAX 运行时/故障恢复证据，以及
   Client Distributed/HF3FS 的真实 library/USRBIO/FUSE runtime 与故障恢复证据。
   Bucket 与 Distributed 均已落在生产 `AttachedLocalStorage` 路径并完成静态
   修复；现有 Master `storage_backend` 同名实现仍不作为该结论的证据。
   CXL 的 Store 逻辑已静态补齐并复用现有
   FFI，TENT 属于经典 parity 排除项，继续保持 fail-fast。HA durability、启动 preflight、
   watch reconnect、promotion/follower lifecycle、standby role gate、
   Memory/NoF restore-remount 与 atomic baseline 已完成静态修复。
4. Client 启动与动态 segment 生命周期已完成静态修复：总容量严格按
   `MC_MAX_MR_SIZE` 拆成 owner-bearing MR 集合；同名 chunk 以 UUID/base address
   区分，Rust remount 协议携带 UUID，Master 不再错误地按名称唯一；HA、local
   fast-path、teardown 与动态 `allocate_and_mount_segments`/
   `unmount_and_free_segments` 均按 owner 生命周期接入。Master storage config
   同时发布 offset/cachelib 所需 base/size alignment，Client 使用真正 aligned
   owner 并在启动时拒绝不对齐总容量。经典 parity 保持零 TE C++/C ABI 修改。
   现有 FFI 不暴露 device clamp 与 NIC NUMA topology，因此
   RDMA/EFA/CXI 要求显式 `MC_MAX_MR_SIZE`，不能假装已自动发现；graceful
   unmount-and-free 会轮询 `GetSegmentsDetail`，只有精确 UUID 已从 Master 消失
   才注销 TE registration 并释放 owner，超时则保留 owner。以上代码尚未执行
   运行时回归。
5. Client metrics 的核心公开 API、Prometheus family 与
   `MC_STORE_CLIENT_METRIC_INTERVAL` 周期 summary/interval bandwidth 已完成
   静态实现；仍可作为 P2 运维增强补齐 C++ SSD streaming-summary quantile 的
   独立 time series。当前 human-readable
   summary 已从持久 histogram 输出 count/avg/p95/max；该项不影响 Store
   对象、副本、持久化或传输正确性，但在要求逐指标完全相同的监控面板前仍需验收。

reaper 的落地协议不是只接管 `BatchId`：job 同时持有 engine、segment、request
descriptors、staging lease 或 typed region lease。async caller 取消 oneshot
receiver 后，worker 仍会等待 native quiescence、free batch、close segment，再
归还或释放 payload。`TIMEOUT` 按公开状态模型作为 terminal，随后仍由
`freeBatchID` 的 Busy 状态证明真实静默；非 Busy 错误无法证明静默时，reaper
向调用方返回 `QuiescenceUnproven` 并故意泄漏完整 job（engine、segment、batch
和 payload）。Client Drop 只 join 已结束 worker，活跃 Busy job 安全 detach，
避免析构永久挂起或提前释放 native 仍可能访问的内存。

## 历史回归基线（待重跑）

- `cargo check --workspace`：通过；仅保留两个既有 `memory_ffi` dead-code warning；
- Client lib：176/176；FilePerKey integration：33/33；Offset integration：9/9；
- 跨 tenant eviction 与 persistent restart/control-plane 测试：10/10，其中真实
  tonic Master 测试证明重启后只存在一个可路由的 Complete LocalDisk replica；
- Master lib：101/101；snapshot storage：23/23；mount/graceful：12/12；
  catalog snapshot：12/12；
- `cargo fmt --all -- --check` 与 `git diff --check`：通过。

这些结果产生于本轮 LocalDisk fail-closed、snapshot 构造错误传播与 durable
GracefulUnmount 修改之前，不能作为当前工作树已完成的证据。重启网络测试也仍需
迁移为 Master snapshot 权威语义；其 `--no-default-features` 配置没有链接 native
TE offload handler，因此不能替代 production Transfer Engine 联调。
