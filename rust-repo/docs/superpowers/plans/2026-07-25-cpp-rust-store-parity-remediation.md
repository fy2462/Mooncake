# C++ / Rust Store 对齐修复计划

关联审计：[`../specs/2026-07-25-cpp-rust-store-full-parity-audit.md`](../specs/2026-07-25-cpp-rust-store-full-parity-audit.md)

## 目标

在不引入 C++ Store 运行时依赖的前提下，使 Rust Store 在公开 API、对象状态机、副本生命周期、资源计费、持久化恢复和可选 TE 模式上达到可验证的功能等价。

实现边界：

- C++ `mooncake-store` 仅作为只读行为参考，不修改原实现；
- Rust Store 永久平替 C++ Store，不存在混合部署阶段；
- 不开发 Rust Client→C++ Master、C++ Client→Rust Master、tonic/coro_rpc
  互通、双栈共存、RPC bridge/sidecar 或 endpoint 协商；
- Store 权威数据与状态机优先且原则上在
  `rust-repo/crates/mooncake-store-master` 实现；
- 严格保持 `Store/Client -> transfer-engine-ffi -> Transfer Engine` 分层；
- Master 承载对象、副本、并发与恢复状态机；Client 只使用 Store API 并委托
  数据传输；FFI 是传输适配和 `unsafe` 隔离层，TE 仅负责数据传输；
- `mooncake-store-client` 只补充参数校验、请求编排、结果/错误转换等必要薄
  逻辑，不复制 Master 权威状态或状态机；
- `mooncake-p2p-store` 不是本次对齐落点，FFI/TE 不承载 Store 语义；
- 原始句柄、裸指针、ABI 调用、边界转换和 native 生命周期只存在于 FFI
  内；Master/Client 等顶层使用方只能依赖安全 Rust API，不传播 `unsafe` 语义；
- 不创建或恢复 `mooncake-store-c`；
- 当前先完成静态代码、协议和状态机审计，不搭建新的本地测试环境。

每个阶段均要求：

- 先提交最小状态机或协议设计；
- 对新增行为补充 Rust 回归测试；
- 保持旧 snapshot/protocol 的显式版本识别；
- 不以“C++ 也有同样缺陷”作为验收标准；
- 修改后更新功能矩阵和剩余差异。

## Phase 0：立即修复的数据正确性与安全问题

### 0.1 Hot cache 一致性

- 将 tenant-scoped cache invalidation 提升为 client 公共内部 helper；
- Put/PutFrom/PutParts 与 BatchPut 在写开始前失效；
- Upsert/BatchUpsert 在开始前失效，并对成功 key 在结束后再次失效；
- Remove 系列复用同一 helper。
- Client 创建时按 C++ 规则解析 `MC_STORE_LOCAL_HOT_CACHE_SIZE`、
  `MC_STORE_LOCAL_HOT_BLOCK_SIZE` 与
  `MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD`；默认关闭，启用后默认 16 MiB
  block、准入阈值 2；
- 使用固定内存 Count-Min Sketch，仅在成功的非 cache read 后增加频次；普通
  进程内 cache 只复制远端 Memory replica，拒绝超过单 block 的对象；
- `MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1` 当前 fail-fast：该模式要求 memfd、
  owner-bearing TE registration、dummy acquire/release 和 IPC mapping，必须
  作为独立可选能力实现，不能用普通 `Vec` cache 静默降级。

验收：缓存旧值后覆盖写，后续 Get 不得返回旧值；batch 部分失败只对成功完成项执行 post-invalidation。

状态：普通进程内 hot cache 的静态逻辑已补齐；SHM/dummy 模式已登记为经典
Store parity 范围外的可选能力，只有目标部署另行明确要求时才重新纳入。
若后续启用该能力，当前架构决策优先采用
`memfd + mmap + Unix Socket/SCM_RIGHTS` 的原生 owner/FD 传递方案，不引入
Zenoh 或 iceoryx2 作为经典 Store 依赖。
按当前约束仅编写测试，不运行测试。

### 0.2 远端 LocalDisk read 边界

状态：静态实现已完成，测试代码已落地但未执行。

- 检查 `replica.size -> usize/i64` 的安全转换；
- 在发起远端分配前检查本地 buffer 容量；
- 检查响应 pointer 数量；
- RPC 成功后的所有退出路径释放远端 batch。

实现结果：

- server 按 C++ 环境变量/default 构造有界 buffer pool，reserve/commit/release
  使用精确 aggregate bytes，失败和取消由 RAII 回滚；
- 每个 batch 持有 owner-bearing `RemoteReadableRegistration`，TTL/显式 release
  后在锁外 drop，不把裸注册地址或 unregister 责任上移到 Client；
- disk 实际读取长度必须等于 Master 声明长度，注册与文件 I/O 放入
  `spawn_blocking`；GC 与 server 同生命周期，不遗留 detached task；
- client 校验非零 batch id、pointer 数量、TE endpoint、TTL，并把 RPC+传输总耗时
  与远端 lease 比较；超时按 C++ `OBJECT_HAS_LEASE` 语义失败。

验收：超大 size、空 pointers、TE open/allocate/submit/poll 失败均返回 StoreError，不 panic、不提交越界请求。

### 0.3 Upsert 副本类型状态机

- `ReplicaType::All` 仅匹配 Memory/NoF；
- 单 Upsert 过滤不可由 TE 覆盖的 LocalDisk descriptor；
- 同尺寸 Upsert 明确处理旧 LocalDisk：失效并重新 offload，或删除该副本；
- 单/批量 Upsert 复用相同 finalize decision。

验收：旧 LocalDisk 内容不会在新值写入后保持 Complete；Memory-only、NoF-only 和混合配置均能结束 processing 状态。

### 0.4 stale Put 锁顺序

- 在 DashMap `remove` 前释放 `get_mut` guard；
- 添加过期 allocating object 被新 Put 替换的回归测试。

验收：timeout/stale handle 清理不会自死锁，资源只释放一次。

### 0.5 FFI `unsafe` 隔离

状态：静态实现已完成，当前工作树的构建、GPU runtime 与回归证据待补。原生
batch 所有权、Store 传输 quiescence、owner-bearing host/device capability、
cancellation-safe background reaper 与顶层 safe Store Client API 已封闭。

- 在 `transfer-engine-ffi` 内封装 native handle、裸指针、ABI 调用及注册生命周期；
- 用携带地址范围、长度和注册状态的安全 Rust 类型表达传输 buffer；
- Master/Client 只接收和调用安全接口，不直接调用 `unsafe` TE 方法；
- FFI 负责整数/指针转换、空句柄、越界和 native 错误码校验。

验收：Store/Master/Client 的 TE 调用路径不再需要 `unsafe` block，也不向其上层
暴露裸句柄或未验证的裸指针语义。

已完成：

- Rust Store 使用的 `OwnedBatchId` 原生值私有、不可 `Copy/Clone`，绑定创建它的
  engine instance，成功 free 后不可复用；共享 FFI 的原有 copyable `BatchId`
  仅为范围外 P2P caller 保留，工作树无需修改 P2P 源码。Python TENT binding
  通过内部 `OwnedBatchId` 句柄表解析 token，不再从用户返回的整数重新构造
  native batch pointer；
- classic/TENT raw submit 与 TENT foreign-memory 注册改为显式 `unsafe` FFI
  边界；Store 只经 `ClientTransferEngine` 适配层提交；
- Store 的所有 native submit 路径在首次 async cancellation point 前，把
  `OwnedBatchId`、segment、request descriptors 与 typed payload owner/region lease
  同步移交给 `ClientTransferEngine` 持有的 background reaper；10 秒只触发告警，
  不提前释放 payload。native `TIMEOUT` 是公开 terminal 状态，但仍必须由
  `freeBatchID` 成功证明静默；Busy 持续保活轮询，非 Busy 的不可证明错误返回
  `QuiescenceUnproven` 并泄漏完整 job。提交失败路径不探测 classic TE 可能遗留
  的未初始化 task slot；Client Drop 不 join 仍活跃的 Busy worker；
- 共享 scratch buffer 已封装为不可复制的独占 `StagingBufferLease`；future
  取消时 reaper 继续持有 allocation，后续调用在 lease 回收前不能复用。
  LocalDisk offload 的远端 batch release guard 也随同 payload 移交，确保远端
  source buffer 只在本地 native transfer 静默后释放；
- Store-owned buffer 注销已集中到 `memory_ffi`，本地 memcpy 的 `u64 -> usize`
  和 range end 使用 checked conversion/arithmetic；native notification buffer
  对负 count、空字符串指针和 UTF-8 失败使用 RAII 释放；
- Engram 删除 detached `register_buffer(&[u8]) -> ()` 生命周期语义，写入使用
  safe slice batch API，范围读取使用 copy-based safe API；
- Client 外部 buffer registry 已从地址元组改为精确 registration generation；
  注册拒绝与 Store buffer 或现有 registration 重叠，read source/write destination
  使用不同 typed region，region 的 `Arc` lease 会让并发注销返回 busy；
- `transfer-engine-ffi::RegisteredMemory` 直接持有 `StableMemoryOwner`，并绑定
  engine instance、base/length、permission、location 与 generation；显式注销
  只在 region lease 清空后执行，handle 被遗忘时由 RAII 注销。native 注销失败
  时 owner 被安全泄漏且 range 被 fail-closed tombstone 阻止复用；
- 新的 `BufferRegistrationId` 在注销时同时校验 base 与 generation；Rust
  `MooncakeClient` 的 ownerless address-only register/unregister 已删除，只保留
  owner-bearing register 与 generation-aware unregister；
- Python 与 Dummy binding 保存并使用精确 registration id；Python foreign owner
  以 writable C-contiguous `PyBuffer` export lease 固定地址，拒绝 readonly、
  non-contiguous、容量不足或 base 不匹配的注册。raw integer address 的 owner
  也必须暴露可验证 Python buffer；任意 Python object 不再被当作安全 owner。
  owner 已移入 FFI handle，因此自动随 MooncakeClient 转入/转出 Engram。

- CUDA/ROCm DLPack 的 capsule、shape/stride/base/capacity/read-only validator
  已实现，但现有 TE C ABI 无法验证 native device ordinal，也无法为 RDMA/外部
  transport 建立 producer synchronization。为保持经典 Store parity 零改 TE C++，
  accelerator registration 当前在消费 capsule 前 fail-closed；完整 DLPack owner
  capability 拆为独立可选增强，不计入本阶段完成项；
- 15 个 zero-copy get/put/upsert/range/batch Rust Client 入口改为 safe：裸地址只
  被用作 registration lookup key，只有完整范围解析成 live owner-bearing typed
  region 后才进入本地 copy/TE，region lease 随 reaper 保持到 native quiescence；
  Python/Dummy 顶层不再调用 unsafe Store Client operation。

background reaper 已完成的所有权协议：

- reaper job 共同拥有 `Arc<TransferEngine>`、`BatchId`、segment、request
  descriptors、staging lease 或 typed registered-memory region；
- submitted batch 在所有 task（包括 `TIMEOUT`）terminal 后仍须 native free
  成功才释放 payload；Busy 保活重试，其他不可证明错误泄漏完整 job；
- failed submission 不读取可能未初始化的 task slot，只根据 free 的
  success/Busy/permanent-error 状态推进；
- async caller 仅等待 oneshot result；取消 receiver 不会取消 reaper job。

## Phase 1：并发原子性与资源计费

### 1.1 Per-key mutation coordinator

状态：已完成。

- 为 tenant-scoped key 建立共享 mutation coordinator；
- 为 PutStart/UpsertStart 增加独立 operation stripe，跨 quota eviction 全部重试
  保持同一请求串行；实际状态变更仍使用 mutation/snapshot guard；
- PutStart、BatchPutStart、UpsertStart、Remove、Move/Copy 的冲突阶段进入同一原子边界；
- C++ 本身逐 key 调用 PutStart 的 BatchPutStart 同样逐 key 获取 operation/mutation
  guard；真正的 multi-key mutation 仍按稳定 scoped-key stripe 顺序获取锁；
- 明确 leader oplog 记录点位于状态提交之后。

验收：高并发首次 Put 只有一个成功；无覆盖插入、重复分配或 processing/quota 泄漏。

实现结果：

- 固定 1024 分片，避免 per-key lock map 无界增长；
- operation stripe 不进入 snapshot barrier，因此请求可临时释放 mutation guard
  让 tenant quota eviction 获取 exclusive snapshot epoch，而同 key 的另一个
  Put/Upsert 仍无法插入重试窗口；
- multi-key mutation lock 按分片编号排序并去重；
- 写入、删除、副本变更、后台 offload/promotion/eviction/reaper 使用同一 coordinator；
- Upsert processing preemption 不复用仍在写入的 buffer；
- BatchPutStart 逐 entry 执行与单 key PutStart 相同的空 key 与 tenant delimiter
  校验，禁止批量入口构造伪造 scoped key；
- 已有 32 writer 单 key 与反序 overlapping batch authored tests；新增 operation
  stripe 串行及“不阻塞 quota eviction snapshot epoch”测试。按当前约束未运行。

### 1.2 Quota ledger

状态：核心对象/副本账本已完成；HA standby/oplog 晋升后的重建并入 Phase 2.2。

- 先确定 quota 是逻辑对象字节还是实际 Memory 副本字节；
- reserve/commit/abort/release 使用同一 ledger；
- 扩缩副本、Upsert 换尺寸、Remove、故障清理和 snapshot restore 全部纳入；
- 增加一致性断言与恢复重算工具。

验收：任意失败注入后 `reserved + committed` 与对象/副本表可重算结果一致。

实现结果：

- 与 C++ 统一为“实际 Complete Memory 副本字节”，NoF/LocalDisk 只计元数据对象数；
- metadata object、reservation、physical usage 和首次 committed charge 分账；
- Put/BatchPut/Upsert、Copy、Move、promotion、eviction、Remove、reaper、失效 handle 与 snapshot restore 均接入统一 ledger；
- PutEnd、PutRevoke 与 Copy completion 先在对象 clone 上构造目标状态，quota
  校验成功后才发布 replica/lease/cache-accounting 变化；quota mismatch 保留
  原对象、processing key 与 task，允许安全诊断或重试；
- Move 的 reservation settle 与 source charge release 在 cloned quota table
  上组成单一事务，第二阶段失败不会留下第一阶段账本；NoF-only 对象第一次通过
  Copy/Move/promotion 获得 Memory charge 时只登记一次 committed object；
- 尺寸变化 Upsert 在新副本提交或撤销前保留旧 Memory buffer 的 physical
  charge，并同时预留新副本 charge；提交在 cloned quota table 上结算新 charge
  并释放旧 charge，撤销则同时 abort 新 reservation、释放旧 charge 和注销唯一
  metadata object。新分配失败时旧对象继续可读，不会先删对象再丢失数据；
- quota release 超过对象权威 charge 时返回 accounting mismatch，不再用
  `min()` 静默截断并掩盖副本/ledger 漂移；
- Put/BatchPut/Upsert 的 tenant quota admission 在超限时计算包含
  `used + reserved + incoming` 的 deficit，在释放请求 key mutation guard 后进入
  exclusive snapshot-consistent eviction epoch，只扫描同 tenant 的 Complete、
  非 busy Memory replica；hard pin 和 live lease 始终保护，soft pin 按独立
  `allow_evict_soft_pinned_objects` 第二轮策略处理，group 任一 live lease 会保护
  全组。offload defer/force/cap 与 C++ 语义一致，按实际释放字节停止并最多重试
  两次；尺寸变化 Upsert 的旧对象作为 protected key，不会被自己的准入驱逐；
- 普通 Memory 自动驱逐复用同一副本移除状态机：候选命中 group 后扩展全部同
  tenant 成员，任一 live lease 保护全组，hard/soft pin 与 busy 成员逐个保留；
  `allow_evict_soft_pinned_objects` 不再与 `offload_force_evict` 混用。offload cap
  只在 force 模式达到阈值后触发直接驱逐，非 force 模式继续尝试排队，和 C++
  `BatchEvict` 一致；
- eviction、replica-clear 与 stale-handle cleanup 先在对象投影上释放 quota，
  mismatch 时不发布 replica/cache-metric 变化；整对象 durable remove 已无法
  安全回滚时，任何 quota invariant failure 会立即 fence Master 并终止本轮驱逐，
  禁止 warning-only 状态继续接受写入，重启后从权威 snapshot/oplog 对象表重建
  ledger；
- PutStart、BatchPutStart 与 UpsertStart 不再各自内联 mutation-first stale
  cleanup，而是在 tenant-scoped mutation epoch 内复用同一个
  project-before-mutate helper；多 Memory 副本只清掉部分失效 handle 时同步减少
  physical charge 并持久化 object image。quota mismatch fail-closed，过期或被
  preempt 的 in-flight metadata 在新 reservation 失败前先记录 durable remove；
- Put/BatchPut/Upsert/Replication admission 对
  `object.size * Memory replica count` 使用 checked arithmetic，溢出在分配或
  quota mutation 前 fail-closed；snapshot 与 typed/legacy oplog object image
  同样拒绝单对象乘法溢出。snapshot restore 在接触 live state 前，以 checked
  multiplication 和 checked aggregate addition 构造完整候选 ledger，单对象或
  tenant 聚合溢出都会使当前 candidate 失败并允许尝试更旧 snapshot；
  `used_bytes + reserved_bytes` 也作为同一 u64 physical-ledger 不变量在 reserve、
  settle 与 checked restore 中预先校验，不能由两个分别合法的计数器组成不可表示
  的总量；`over_quota` 使用 u128 比较，不会在 effective quota 为 `u64::MAX` 时
  因饱和加法把真实超额误报为 false；
- live-object clone/validate/commit 统一使用保留 runtime replica refcnt 的
  mutation projection；`ReplicaDescriptor::clone` 继续只用于 response、task 与
  durable image 的无 pin 副本。该约束覆盖前台 Put/Copy/Move、batch replica
  clear、stale handle、quota eviction、reaper 和 HA replay，避免事务写回清零
  无关在途 pin；
- AddReplica 只接受 LocalDisk，关闭绕过分配与配额注入 Memory descriptor 的入口；
- 元数据在 PutStart/LocalDisk object 创建时立即计数，不再延迟到 PutEnd；
- 增加多 Memory 副本、Copy/Move、完整驱逐、LocalDisk-only、promotion 及删除回收测试。

## Phase 2：持久化、Drain 与 HA

### 2.1 精确 allocator snapshot

状态：已完成 snapshot 恢复主路径；oplog 增量副本变更后的 allocator 收敛仍归入 Phase 2.2。

- snapshot schema 升级，记录 allocator ranges 或可重建的完整 replica allocation；
- 恢复时校验重叠、越界和 usage 汇总；
- 支持旧 schema 的只读迁移或明确拒绝。

实现结果：

- 从所有 Memory/NoF live replica descriptor 精确重建空洞，而不是信任聚合 `used`；
- Offset 与 CachelibLike 均校验 segment identity、越界、对齐、重叠和 usage overflow；
- snapshot 格式先以 v2 引入精确 allocator 恢复、再以 v3 引入 native Copy/Move
  runtime state，并以 v4 保存对象 runtime deadline、Copy/Move task codec 和实际
  retry count；v10 保存 `allocation_strategy` 与 `memory_allocator_kind`；v11
  保存 delayed/discarded Memory/NoF replica reservation 与绝对 release
  deadline；v12 保存 MoveStart 复用的 exact existing target，恢复 allocator 时
  live 与 delayed range 一起校验和占用；
  无版本字段的 JSON/msgpack 按 v1 兼容读取，未来版本 fail-fast；
- hot standby snapshot replacement 使用同一精确恢复路径。

### 2.2 Snapshot consistency barrier

状态：已完成核心一致性闭环。

- 在对象表、segment allocator、任务表和 quota ledger 之间建立一致性快照点；
- 恢复后保留 Copy/Move 的原始语义；
- hot standby apply snapshot 前清理旧状态。

已完成：

- 所有纳入 scoped-key mutation coordinator 的对象、副本、任务与 quota 变更受 snapshot 写屏障保护；
- snapshot replacement 会清空旧 objects、segments、tasks、client index、临时 promotion 状态及旧 quota usage；
- replacement 前完成对象 default-tenant key 规范化、canonical duplicate/metadata
  identity、LocalDisk storage identity、task UUID/active-object 引用及 native
  replication source/target 校验；进入 global replacement barrier 后不再存在
  `Result` 失败出口，非法 snapshot 保留完整 live state；
- standby oplog 的完整 `put_end` v3 image 可重建 replica、hard pin、data type、
  runtime deadline 与精确 tenant quota 状态，remove/revoke 可回收 quota；
- leader 在追加 v3 image 前执行与 standby 对称的 canonical key、replica
  geometry 和 quota-charge preflight，拒绝会毒化回放流的内存状态；standby
  从 v3 的 quota/replica completion 重建 `processing_keys`，不会把部分
  `put_end` 误当成终态；
- native `replication_start` producer 与 standby consumer 对 source Complete、
  allocated target 的 Allocating/唯一性、exact existing Move target、全部
  non-complete replica ownership、非空 client 以及 Memory target reservation
  使用同一 checked preflight；Copy 可按 C++ 语义持有零个新增 target，Move 必须
  恰好持有一个 allocated target 或一个 existing target；target
  总量溢出或声明 reservation 不一致不能写入 oplog，也不能推进 standby
  sequence。native snapshot restore 执行相同校验，并在 live-state replacement
  barrier 前拒绝损坏 candidate；
- Cachelib/CachelibLike 的 slab identity 是 `u32`；Memory、NoF、CXL mount/config
  及 descriptor-driven restore 显式拒绝超过该 slab-index 表示范围的 capacity，
  不再把 snapshot 的 `u64` slab count 截断为零或别名到已有 slab。普通 segment
  与 CXL global oversized authored cases 均要求 restore fail-closed；
- CopyEnd、MoveEnd、offload success 与 promotion success 提交完整 durable object image；
- segment mount oplog v1 携带 base/size/endpoint/protocol/client，standby 可恢复拓扑；unmount 会清理关联副本；
- object image 在提交前先构建候选 allocator，缺失 segment、越界或重叠时不推进 sequence；成功后原子替换 allocator、usage、quota 与 client index。
- standby replace/remove 在触碰 object、allocator、cache metric 或 client index
  前先克隆 tenant ledger，并在投影上完成旧对象 removal 与新 image restore；
  quota mismatch 保留全部 live state 和 `expected_seq`，成功路径最后一次性提交
  projected ledger；
- segment unmount 对全部 affected objects 使用同一个 projected ledger 和变更
  batch；任一对象或 task reservation accounting mismatch 时，不删除任何对象、
  segment 或 allocator entry。Memory/NoF 以 `(replica_type, segment UUID)` 区分，
  同 UUID 的另一介质副本不会被误删；leader 内层 object-image durability 已
  fence 时，即使最终 segment record 写入成功，Unmount RPC 也不会错误返回成功；
- 显式 Memory/NoF RPC、GracefulUnmount completion、NoF heartbeat 与
  expired-client purge 共用 persist-then-apply 卸载 helper。global mutation
  barrier 内先验证 UUID/owner/name 并 flush durable tombstone，成功后才失效
  replica、释放 allocator 和 bump view；flush 失败保留 segment/allocator/
  graceful intent 并永久 fence。finished-task 与 expired-client task tombstone
  也改为 durable 成功后才从 runtime task table 删除；
- generic finished-task reaper 不再删除仍被 Drain job `active_tasks` 引用的
  Success/Failed task；Drain 先消费 terminal status 并更新 unit 计数，解除引用后
  下一轮 reaper 才写 durable tombstone 并删除 task，避免 retention 上限为 0 或
  finished task 过量时把成功迁移误判为 missing/failed；
- Memory/NoF Mount 与 ReMount 新 segment 也改为 persist-then-publish：producer
  在 flush 前完成 nil identity、name、base/size/endpoint 校验，standby decoder
  使用相同 fail-closed 约束；durable mount 成功后才更新 segment/allocator、
  client index、view 与 HTTP metadata。NoF base 作为 namespace offset 可为 0，
  Cachelib 路径仍强制 slab 对齐。Memory mount UUID 从完整 request identity 稳定
  派生，mount record 使用可选 `identity_version=1` 让新 standby 验证 fingerprint，
  legacy record 仍可读取；Memory/NoF 同 UUID 可在恢复后安全 rebind。NoF 同
  endpoint 异 UUID fail-closed。flush 失败不会留下 leader-only topology；
- stable Memory UUID 放在 shared core，Client 因 Mount RPC error、缺失 response
  UUID 或 non-canonical UUID 进入回滚时，可先按 expected/returned UUID 幂等
  unmount；`OK`/`NotFound` 均视为确认已删除。只有确认 Master 已删除后才
  unregister/free；无法确认时用
  `SegmentMountOutcomeAmbiguous` 把“必须保留 owner”传给动态分配调用方，启动
  owned segment/CXL 路径直接安全泄漏 owner，避免响应丢失后形成 Master 指向已
  释放地址的 segment；
- restore 还会验证对象非零 size、非空 replica 集、stored replica type、
  `replica.size == object.size`、range overflow、Memory/NoF 非 nil segment 与重复
  location；Copy/Move source/target identity 包含 size，错误长度的 task 不能认领
  同 offset 的 Allocating range。CXL snapshot alias 要求 runtime
  `allocation_strategy=cxl`，CXL runtime 也拒绝普通 Memory segment。native v10
  与 catalog `rust_allocator_config_v1` sidecar 保存 allocator kind/placement
  strategy；新快照在替换 live state 前拒绝配置漂移，缺失字段的 legacy 快照仍兼容；
- standby oplog 的 batch apply/recover 由单一 replay lock 串行化，避免并发 follower
  回调重复提交同一 `expected_seq`；每条 durable record 的对象、segment、
  allocator、quota 与二级索引变更整体进入 global mutation barrier，因此
  snapshot 只能看到该记录之前或之后的完整状态；
- legacy C++ `put_end` 的无租户 key 统一规范化到 default tenant scoped key；
  quota settle 先在对象副本上构造完成态，ledger 校验成功后才提交对象，失败时
  sequence、replica status、size、processing key 与 quota 均保持不变；
- 过期 PutStart 若保留已完成的 NoF/Disk/Memory 副本，会先以投影 ledger 原子
  settle 剩余 physical Memory charge、释放 replacement charge，再提交 survivor
  image；不会留下无 processing owner 的 reservation。legacy snapshot 即使关闭
  quota enforcement，也会规范化对象内 committed/reserved charge，保证后续 v3
  durability preflight 与 promotion 语义一致；
- replay Mount 的重复 UUID 只有在 durable name/size/host/client identity 和
  allocator entry 同时一致时才视为幂等；冲突记录不推进 sequence，首次 Mount
  的 allocator runtime invalidation 失败会回滚刚插入的 allocator segment；
- mount/unmount/remount/local-disk mount 与 client purge 使用 coordinator 的独占 global mutation barrier，不会被 snapshot 截取到 segment/object/allocator 的中间态。
- background eviction、PutStart/Copy/Move/offload/promotion reaper、失效 handle 及前台 revoke/failure cleanup 在状态提交后记录完整 object image；删除最后副本时记录 remove；
- 普通 Copy/Move 与 task 创建的 legacy name-only segment 输入统一 fail-closed：
  target 必须跨 Memory/NoF 唯一解析为 Active UUID/type，Copy 按 exact ID 分配；
  source 必须是唯一、Complete、handle-valid 的 Memory/NoF replica，并按 exact
  UUID/type/name 校验请求 client owner。后台 offload/eviction 的 source owner
  fallback 同样不再使用首个同名 segment；
- RPC task 创建进入 key mutation coordinator，Fetch/Complete/timeout/reap 与 Drain 跨表 task 变更进入独占 snapshot barrier；
- local snapshot v3+ 持久化 native Copy/Move 的
  kind/source/targets/quota reservation/task age；恢复时重建 source refcnt pin、
  reservation 与 allocator 占用；
- native snapshot v4 持久化 put start、lease 与 soft-pin 的绝对时间，修正
  Copy=0/Move=1 task codec，未知 task type fail-fast，并恢复真实
  `max_retry_attempts`；
- snapshot 在短 global mutation barrier 内捕获不可变 DTO，文件 I/O 在 barrier 外
  完成；超时/取消后仍保持 single-flight，直到 detached writer 实际退出；
- C++ catalog v1 payload 保持不变，Rust 当前以可选
  `rust_replication_tasks_v2` sidecar 保存同一 runtime state，并只读回退
  `rust_replication_tasks_v1`；缺失 sidecar 兼容为空。v2 新增 exact existing
  Move target，不用于开发 C++/Rust Master 混合部署；
- 旧快照中没有 task 归属的孤儿 `Allocating` replica 在恢复时安全撤销，避免 allocator 永久泄漏。
- snapshot baseline + 后续 oplog following + promotion 组合测试验证最终 object table 与 applied sequence 一致。

### 2.3 Persistent LocalDisk 与 eviction 原子性

状态：核心原子性与 Master-authoritative 回归迁移已完成；跨进程外部写入和
accepted-before-unlink crash window 仍属于 durable journal 后续项。

- FilePerKey pending 绑定 backend id、reservation token、record generation、
  expected encoded size 和 write target previous generation；
- victim 不在 prepare 阶段移出 FIFO，而以 generation+reservation 标记；pending
  的 RAII Drop 覆盖 RPC await 期间的 future cancellation；
- 并发不同 key 写预留各自 quota growth，同 key overwrite/delete 在 pending
  期间被拒绝；read 保持可用；
- commit 校验 victim 的 canonical path、regular-file identity、size、envelope key
  和 generation；rename 是可见性点，rename 后 fsync 失败仍安装新 generation；
- data directory/marker 消失或 namespace identity 被替换时 fail-closed；已接受但
  unlink 失败的 victim 转为 orphan 并保持计费，直到 cleanup 成功；
- generation/reservation/orphan ledger 当前是进程内状态；跨进程外部写入的
  validate/unlink TOCTOU 以及 accepted-before-unlink crash window 仍需通过 durable
  journal 或 victim quarantine 进一步加固。C++ 也存在该窗口，因此列为共享后续项；
- eviction RPC 按 tenant 分组并有限幂等重试，部分成功时只 commit accepted set，
  rollback unaccepted set；FilePerKey 与 Offset 均使用 accepted-set 语义，Offset
  额外用 backend-local reservation、write-key/expected-size/replaced-entry 和
  完整 victim generation 比较阻止 stale commit 与 extent 提前复用；
- FilePerKey/Offset persistent restart 先全量 scan 和 tenant-key preflight，再以
  20k batch 重发 `NotifyOffloadSuccess`；mount 顺序固定为
  `Mount(false) -> Notify -> Mount(true)`，非空恢复缺少数据面时 fail-closed；
  Master 只允许完成已签发的 offload task，或重关联同 holder、同尺寸且原状态为
  Complete 的已知 LocalDisk 副本，不允许磁盘扫描创建对象或覆盖 object size。
- Offset pending eviction 现在按 reservation token/generation 保留 victim，
  accepted set 提交与 unaccepted set 回滚不会提前复用 extent；旧的成功型
  AddReplica fixture 已迁移为 Begin/Commit mount、Put、heartbeat task、
  NotifyOffloadSuccess 和临时 Memory unmount 的真实权威链。

验收：跨 tenant A 成功/B 失败只删除 A，B 与目标写保持不变且 reservation 全释放；
真实 persistent FilePerKey + tonic Master 重启恢复后只有一个可路由 Complete
LocalDisk replica，重复恢复幂等。

### 2.4 Drain

状态：已完成正确性修复。

- 失败传播到 task terminal status；
- 源 segment 只在全量成功后进入 drained/unmounted；
- terminal failure 与显式取消一样恢复仍挂载的 source segment，允许修复故障后
  重新创建 Drain。

实现结果：

- 无目标时保持 `Running + Draining`，等待后续可用目标；
- terminal failure 先以单条 durable `segment_status_batch` 把仍挂载的精确
  Memory/NoF source UUID 恢复为 `Active`，随后才发布 `Failed`；仍被对象引用但
  已缺失、同 UUID 换名或状态漂移会 fence，不能按名称恢复错误 segment；
- 全部对象迁移成功后源 segment 进入 `Unavailable`；
- allocator 分配显式排除非 Active Memory/NoF segment；
- CreateDrainJob 在同一 global mutation epoch 内完成唯一名称解析、Active
  校验与 Draining 转换，两个并发 job 不能认领同一个源；Rust 允许同名不同 UUID
  segment，因此 name-only Drain API 遇到歧义会 fail-closed；
- CancelDrainJob 在仍有 active move task 时拒绝恢复源 segment，避免任务继续写入
  已重新开放分配的 source；
- Drain 的多 segment 状态转换以单条 durable `segment_status_batch` oplog
  复制；成功终态 `Unavailable` 可跨 snapshot baseline 晋升保留。Drain job
  scheduler 仍是 runtime-only，snapshot restore 或 standby promotion 会把没有
  scheduler 的 orphan `Draining` 安全视为已中止并恢复为 `Active`；
- completion 会把 source 上任意状态的 replica 都视为 remaining；临时
  `Allocating`/非 Complete replica 只能阻塞 job，不能错误报告成功。
- Drain task 现在固化 source/target 的 UUID、replica type 与 task key；MoveStart
  按精确 UUID 验证 owner/name/status，并通过 exact-ID allocator 分配，不能被
  后挂载的同名 segment 劫持。NoF source owner 同样从精确 UUID 解析；重复 target
  name 在排程时拒绝。
- 已编写同名 target 后挂载、非 owner MoveStart、exact allocator、Memory/NoF
  terminal failure 恢复与 NoF retry 回归；按当前约束未运行。

### 2.5 GracefulUnmount HA

状态：GracefulUnmount deadline、intent durable-before-publish、snapshot
baseline、地址重绑定、统一 durability fence、启动 preflight 与 watch reconnect
均已完成静态修复；待允许执行回归门后验证。

- GracefulUnmount 权威状态保存 `segment_id/client_id/deadline_epoch_ms`；
- RPC、native snapshot v4、catalog `rust_graceful_unmounts_v1` sidecar 与 oplog
  共享绝对 deadline；
- standby 暂停执行，promotion 和 snapshot restore 从权威表重建 scheduler；
- 到期卸载在 global mutation barrier 内先持久化 unmount，再应用本地删除并
  bump view；非 durable completion 不会让 owner 误判 segment 已可释放；
- 旧 snapshot 只有 GracefullyUnmounting status 而无 deadline 时按立即到期恢复。
- Drain job 仍是 runtime-only；snapshot restore/promotion 会 durable tombstone
  无法恢复 job authority 的 active Drain move task，再把 orphan Draining segment
  原子恢复为 Active，避免新 leader 执行已中止 term 的任务。
- leader producer 与 standby replay 对 GracefulUnmount 的非空 name、非 nil
  segment/client UUID、非零绝对 deadline 使用同一校验；已有 intent owner 冲突
  会在修改 segment status 前拒绝。`segment_status_batch` producer 同样拒绝空批、
  nil/重复 identity 和非 Active/Draining/Unavailable 状态。
- snapshot restore 在替换 live state 前拒绝 malformed GracefulUnmount sidecar；
  malformed replay/batch 的 authored tests 断言 segment、intent 与 sequence 均不
  发生部分提交。按当前约束未运行。

本轮已完成：

- promotion 成功后显式执行 `Promoted -> Stopped`，下一 leader term 可重新
  `Start`；leader cleanup/retry 路径不再静默忽略 `enter_standby_mode` 错误；
- `MasterState` 增加可排空的 background mutation gate。reaper、eviction、
  client purge、Drain 与 NoF heartbeat 仅在 service available 时持有共享
  mutation lease；demotion 先关闭原子门，再独占 gate 等待在途 worker 退出，
  防止已通过一次检查的 worker 在 standby term 泄漏 mutation。
- native snapshot 与 standby oplog replay 均把 Memory/NoF process address、
  endpoint、protocol 和 `handle_valid` 视为非持久 runtime state；restore 后
  allocator segment 也标记为 runtime-unbound，不能承接新分配。ReMount 按
  durable segment id/name、owner 与 size 完整预检后，统一重绑定 segment table、
  allocator、对象副本和在途 Copy/Move descriptor；Rust Client 保存当前 NoF
  descriptors 并在 `NeedRemount` 时自动提交。NoF heartbeat 和自动 eviction
  不处理尚未重绑定的副本。
- native snapshot v7 把 `last_included_seq` 直接写入同一 msgpack payload；
  状态 DTO 与 oplog baseline 在同一个 global mutation barrier 内捕获，并校验
  capture 前后 sequence 未变化，随后随 payload 一次原子 rename 发布。
  `LocalSnapshotProvider` 不再读取无关联的 catalog descriptor；v1-v6 缺少该
  字段时 baseline 固定为 0，从 oplog 起点安全重放。
- native snapshot v8 在 v7 atomic baseline 上增加 Memory Segment `host_id`；
  v1-v7 缺失字段按空值读取，Client remount 再提交当前稳定 identity。catalog
  current Segment 的第九字段与 mount oplog 同时保存该值。

统一 durability fence 已完成静态修复：PutEnd、PutRevoke、Remove/BatchRemove、
Copy/Move、offload/promotion、Memory/NoF Mount/Unmount、LocalDisk recovery
cleanup 与到期 GracefulUnmount 均使用 durable recorder；append/flush 失败会返回
`Unavailable` 或停止后台 worker，并不可逆关闭当前 Master term。服务层已无
best-effort object/segment recorder，KV stored/removed 发布排在 durable 记录之后。

本轮补齐 backend 自身的 commit boundary：LocalFS 使用带 u64 sequence/view 与
checksum 的 `MCOPLG02`，保留严格 v1 reader；所有 segment、`latest` 和 snapshot
sequence 都执行原子 rename 与可传播的 fsync，v2 segment 超过或缺少 commit
pointer 时拒绝恢复，持久化失败后 poison 当前实例。Etcd entry 与 `/latest`
由同一 transaction 提交，startup/latest/key-value/gap/orphan 校验均不再降级；
promotion final catch-up 的 backend error、空读、无进展、超时和未追平统一转入
`PromotionFailed`。相关 authored tests 已落地，按当前约束未运行。

Snapshot 的底层本地 object store 也已纳入同一 durability contract：
upload 使用同目录临时文件、file fsync、rename 与逐级 directory fsync，delete
与 native history retention 在删除前后同步目录；Embedded/Redis catalog listing
仅跳过明确消失或单个损坏的 candidate，I/O/连接错误直接传播，空 latest marker
和 latest 删除失败不再降级。相关 authored tests 已落地，按当前约束未运行。

Continuous replay 也已补齐 semantic fence：watch/poll 对 expected record apply
失败、不可解释的首序号 gap 或非空 batch 无进展直接进入 `Failed`，notifier 的
后续断开回调不能再把 `Failed` 覆盖为 `Reconnecting`。相关 authored test 已
落地，按当前约束未运行。

Etcd leadership view 同样改为严格解析与 acquire 后线性确认：非法 address/
revision/TTL 拒绝，CAS 成功后缺失或 owner mismatch 不再 fabricated fallback；
transaction 结果不确定或读回失败会 revoke 本次 lease 后返回错误。相关 authored
test 已落地，按当前约束未运行。

Leadership term fence 进一步统一：active session 在 acquire 成功时登记，重复/
stale session 操作拒绝；keepalive 在 promotion 追平前启动并以 TTL-derived 周期
等待、校验 Etcd ACK。lease loss/channel close 会先关闭并排空 Store mutation
gate，再触发 gRPC shutdown；远端 revoke 失败仍无条件完成本地 demotion。相关
authored tests 已落地，按当前约束未运行。

HA snapshot scheduler 也已接入 serving gate：shutdown signal 在 timer 同时
ready 时优先，demotion 后不再启动 native/catalog snapshot；native writer 在
调度、全局 mutation barrier 内与 backend commit 前重复检查 gate，catalog
capture 在 capture 前后检查 gate。相关非 serving authored test 已落地，按当前
约束未运行。

Snapshot retention/delete 改为先撤销 catalog 可见性、再清理 payload：
Embedded 先移除 latest marker/descriptor，Redis 以 atomic pipeline 先移除
index/可选 latest，后续 object-store 失败因此只产生不可见 orphan，而不会制造
descriptor 指向缺失 payload 的恢复候选。相关注入失败 authored test 已落地，
按当前约束未运行。

Admin HTTP 的 tenant-quota policy mutation 也已在 Service 层进入 background
mutation epoch，而非只依赖 handler 的瞬时 availability 检查；demotion 排空在途
connector save，关闭 gate 后的 upsert/delete 返回 Unavailable。相关 authored
test 已落地，按当前约束未运行。

Tenant-quota connector 的 commit ambiguity 同样 fail-closed：file connector
传播 rename 后 directory fsync 错误，Etcd/file 任一 save 错误都会 durability
fence 当前 term 且不提交内存候选 ledger，下一任从 connector 重建。相关 authored
failure test 已更新，按当前约束未运行。

跨 term policy 单调提交已补齐：policy YAML 携带可选 producer view，legacy
缺失按 0 读取；Service 使用独立 election-issued leadership term，不复用会被
segment topology bump 的 Client-visible view。file connector 在持久 sibling lock
file 的跨进程独占区内重读并拒绝旧 view，再原子发布；Etcd 对 observed
mod_revision（不存在时 version 0）执行 transaction CAS，竞争重读，transport
ambiguity/重试耗尽继续触发 durability fence。旧 term 的晚到写不能覆盖 successor。
相关 authored tests 已落地，按当前约束未运行。

生产 HTTP metadata listener 已绑定同一 Store serving gate；失租后 tonic
graceful shutdown 尚未结束时，旧 leader 的 GET/PUT/DELETE 统一返回 503，不再
暴露或改写 TE bootstrap/routing metadata；PUT/DELETE 在 metadata lock 后进入
background mutation epoch，demotion 会排空在途修改。相关 authored integration
test 已落地，按当前约束未运行。

Tonic service 也不再只依赖 interceptor 的瞬时检查：全部 MasterService trait
入口取得跨完整 async handler future 的 owned foreground request guard。demotion
关闭 gate 后先 drain 该 counter，再排空 background/key barrier；已通过
interceptor 但等待 per-key stripe 的 RPC 因而不能在 standby term 继续。相关纯
gate authored test 已落地，按当前约束未运行。

Irreversible durability fence 也会终止 standalone/HA tonic server；HA server
退出后复用 leader cleanup 关闭 keepalive、释放 session 并重新进入 candidacy，
不再由一个永久 Unavailable 的 fenced process 无限占有 lease。相关 shutdown
predicate authored test 已落地，按当前约束未运行。

启动与 follower fail-closed 也已完成静态修复：未知 snapshot backend 和不完整
backend/directory 组合直接拒绝；native writer 与 catalog/object-store 在 service
gate、leader label 发布前完成实际探测。watch 断开后执行最多三次 polling
reconnect，失败进入 `Failed` 并终止 follower；Controller 不再维护可能陈旧的
`standby_running` 缓存。catalog restore 合并 latest marker 与全部 published
listing，去重后按 producer view、included sequence、snapshot ID 降序；marker
只作 availability hint，旧 term 的晚到 I/O 不能隐藏 successor。manifest/payload decode 和后续
allocator/task/quota semantic apply 任一失败都会尝试更早 snapshot。新 manifest
第三字段写入并校验 snapshot ID，旧 Rust `|rust` 仅保留只读兼容。snapshot-only
在所有 candidate 失败后保持 fail-closed，只有已配置 oplog following 才从
sequence 0 回退 oplog-only bootstrap。

验收：future/expired native restart、legacy snapshot、catalog roundtrip、oplog
replay 和 standby promotion 测试通过。

### 2.6 HA backend capability matrix

状态：后端 capability fail-fast、standby lifecycle 与 role gate 已完成静态
修复；完整 durability/replay 闭环仍归入 Phase 2.5。

- 启动前验证 discovery、leader election、oplog backend 的合法组合；
- 未实现组合 fail-fast；
- 为支持组合增加 promotion/replay 一致性测试。

实现结果：

- `HABackendType` 显式声明 discovery、leader election、shared oplog 三项能力；
- 当前生产 HA 仅允许三项能力完整的 Etcd；
- Redis/Kubernetes 在启动连接前以 `UnavailableInCurrentMode` 拒绝，不再出现“配置可解析但无法服务”。

## Phase 3：Rust 跨版本协议与公开 API

状态：经典 Store 范围内的静态协议/API 逻辑已完成；真实 C++ producer golden
与按当前约束禁止执行的运行回归仍缺证据。范围外能力继续 fail-closed，不计为
本阶段未完成的经典 Store 代码。

- oplog 与 catalog schema 增加版本、兼容 reader 和迁移策略；
- 对齐 HTTP metadata 的 GET/PUT/DELETE 能力；
- 修复 remote-source miss fallback；
- 让 `rpc_only` 真正跳过 TE；
- 为本地磁盘格式提供版本探测和迁移工具。

验收：每个 Rust 持久协议都有版本边界和升级策略，不再依赖“同一提交版本同时升级”的隐式假设。

已完成：

- Rust oplog 的 legacy `MCOPMETA1/2` 保持只读兼容；新写入使用
  `MCOPMETA3` typed object image，完整保存 replica、hard pin、data type、
  put/lease/soft-pin deadline 和 committed/reserved/replacement quota。未知 schema
  显式拒绝且不推进 sequence；legacy typed image 同样校验非零 size、非空
  replica、合法 client UUID、stored replica type、range/segment/location identity，
  不能把畸形旧记录降级为可用对象。无 image 的旧 `put_end` 仅保留为已有
  snapshot 对象的 completion marker；
- 新 remove/revoke/unmount/put-start control records 写入 `schema_version=1`；
  reader 仅对缺失版本的历史记录兼容，显式未来版本不执行、不推进 sequence。
  unmount 还要求 live segment 的 UUID/name identity 同时一致。Etcd serializer
  对这些 versioned Rust records 保留 generic `OpLogRecord`，不再降级成会丢失
  schema 的 C++ wire；无 replica 的 legacy `put_end` marker 同样保持 untyped；
- catalog descriptor 固定并测试 C++ v1 三字段 wire shape；
- HTTP metadata 提供 key-scoped GET/PUT/DELETE，并保留 Rust 聚合 GET；
- master `NotFound` 不再截断 remote-source miss fallback；
- `rpc_only` 不创建、不安装 Transfer Engine，也不注册本地 buffer；
- gRPC status 映射补齐 NotFound、AlreadyExists、Unavailable 与 DeadlineExceeded。
- FilePerKey 在清理前验证 Rust format/ownership marker；非空无 marker 或 foreign
  marker 目录 fail-fast 且不修改内容；
- FilePerKey canonical record 使用与 C++ `struct_pb::KVEntry` 相同的 protobuf
  field 1 key / field 2 value wire shape；persistent lifecycle 在重启时从 envelope
  恢复精确 key 和 value size，不再从清洗文件名猜测；
- 新增 C++ FilePerKey → Rust canonical 的离线迁移：全量 preflight 解码、tenant
  scoping、重复 key/quota 检查、源内容 SHA-256 身份记录、staging 前复核、
  staging 写后回读与最终发布前第三次源身份复核，最后在同一 root 下 no-replace
  rename 原子发布；源目录始终保持不变；
- OffsetAllocator 在创建 lock/arena 前识别 C++ `kv_cache.data/meta` 与 mixed
  layout，明确拒绝共享目录，避免静默生成第二套空状态。
- 新增 C++ OffsetAllocator v3 → Rust canonical 的严格只读离线 importer：
  外层只接受 canonical struct_pb v3，allocator_state 按生产 `serialize_to`
  的 little-endian native raw ABI 解码（不是旁路 msgpack serializer），校验
  active-node partition、free/allocated accounting、extent/neighbor、record
  seq/flags/CRC、duplicate/tombstone；stale/missing/duplicate FIFO 按 C++ Phase
  A/B repair，post-checkpoint/torn/unknown/CRC-failed node 按 C++ recovery
  逐节点跳过并计数。随后在独立 strict persistence staging 中按修复后的 FIFO
  顺序重建、逐 key 回读、复核源 inode/size/
  mtime、metadata hash 与每个 live record hash，最后 no-replace rename 发布。
  源目录始终不变；未知 ABI、非 canonical metadata 和无法确定 live winner 的
  歧义状态 fail-closed，skipped 与无 CRC record 在迁移报告中显式计数。C++ 没有
  可互操作目录锁，因此调用方停写源目录是明确前置条件，identity/hash 复核不是
  ownership lock 的替代品。
- catalog Segment reader 已补当前 C++ LocalDisk task array
  `[tenant_id, key, size]`，并校验其 scoped identity；legacy `key -> size` 分支
  继续保留。现有 C++ fixture 改为 current task 加 SSD capacity，避免测试只覆盖
  旧整数分支。另新增 v1、v2-data-type、v2-hard-pin、v3 与 current-v4 五种
  metadata shape 的表驱动 reader 回归（按当前约束只落代码、尚未执行）。
- Client Batch CRUD 的 native completion 现在同时要求 terminal `Completed`
  与 `transferred_bytes == request.length`；短读/短写不再被提交为完整对象。
  单 buffer、多 buffer、parts、range 与 remote LocalDisk read 使用同一精确长度
  语义。
- Copy/Move worker 现在反序列化并使用 task payload 中的 tenant（legacy payload
  缺省为 `default`），拒绝非本机 owned Memory source；transfer 或 End 失败均
  revoke，`NO_AVAILABLE_HANDLE` 按 assignment 上限退避重试。显式 tenant 的
  Create/Copy/Move Client API 也已补齐。
- QueryByRegex 已切换到 C++ Client 对应的 `GetReplicaListByRegex`，并验证
  response tenant/user/scoped identity；diagnostic `QueryByRegex` 不再被 Client
  公共查询路径误用。
- catalog reader 现在拒绝 trailing bytes、重复字段/identity、allocator mismatch
  与 malformed/duplicate task；current task payload 恢复 tenant-scoped key，
  active task restore 还会验证对象存在。native/catalog task restore 在替换
  live state 前统一解析 Copy/Move JSON payload，验证 payload tenant/key 与
  `TaskEntry.key` 的 canonical identity 及 task-type shape；legacy 缺省 tenant
  或 scoped payload key 会重写为显式 tenant + user key，冲突则 fail-closed。
  native snapshot task 还会拒绝 nil/mismatched envelope UUID、nil assigned
  client、active task 缺失 assigned client、非法或逆序时间戳，不再用当前时间
  掩盖损坏；前台与后台 Drain 在源 segment owner 缺失或 payload 序列化失败时
  将 unit 保持 blocked，不再发布永远无法被 worker 获取的 Pending task；
  已补 payload identity/type 冲突不替换 live state 与 legacy canonicalization
  的静态测试，按当前约束尚未执行。
- `host_id` 已从 Rust Client 贯穿 mount/remount、ReplicateConfig、Master
  allocator、native snapshot v8、oplog、catalog 与 segment detail；LOCAL_FIRST
  不再把逻辑 segment name 当作唯一物理主机身份。
- `transfer-engine-ffi` 已增加 safe owner-bearing registered submission：
  typed Read/Write request、exclusive in-flight claim 与 quiescence-owned batch
  guard。native reject/Busy/drop 不会提前释放 payload，且未修改 TE C++/C ABI。
- Client metrics 已从 request-local healthy/closed gauge 改为 client-owned
  persistent registry；安全导出 API、`/metrics/summary`、cluster label、开关与
  bandwidth summary 已接入。透明 tonic channel 统一采集所有 Master RPC，
  顶层 Get/Put/Batch/zero-copy/multi-buffer/range/upsert 与 LocalDisk
  offload/promotion 成功路径分别采集 interface/transfer/SSD 指标，避免把指标
  状态散落到 Master 权威逻辑或 FFI unsafe 边界。
- Python Client 已把 `tenant_id` 从创建入口直接写入初始 `ClientConfig`，REST
  service config 同步透传；动态 owned segment 也已用薄绑定暴露
  `allocate_and_mount_segments`/`unmount_and_free_segments`。UUID 解析留在
  Python adapter，owner、TE registration、Master lifecycle 与释放仍由 Client
  统一管理。
- C++ `TensorMetadata` v1 的 304-byte CPU codec 已在 Rust PyO3 adapter
  独立实现并从 package root 导出。serializer 持有 contiguous tensor owner，
  deserializer 严格校验 magic/version/header、dtype、shape、layout 与 payload
  边界；accelerator tensor 在没有 native device identity/synchronization
  capability 时 fail-closed，不借裸地址伪造支持。普通
  `put_tensor/get_tensor/upsert_tensor` 已薄接现有 Client 状态机。
- Tensor batch 与预注册 buffer API 已静态接通。普通 batch tensor 复用
  Client 的 multi-key start/transfer/finalize；owned-slice `batch_upsert`
  不以 Python 单 key loop 替代 batch RPC，并修复每个 BatchUpsert entry 错带
  整组 `group_ids` 的问题。from/into 只接受 owner-bearing、已注册 Python
  buffer，metadata 校验发生在 mutation 前，GetInto materialize 为引用调用方
  owner 的 PyTorch view；raw integer address fail-closed。structured-object
  同步改为向 Tensor 专用入口传 owner。
- legacy TP 已按 C++ `{base}_tp_{rank}` 路由补齐 put/get/batch 及 owner-bearing
  from/into。serializer 只接受 CPU uniform shard，生成 SHARD metadata 的
  global/local shape 与 TP axis；batch 以 base key 聚合所有 shard status。
  该项不替代 general parallelism manifest、writer partition 或 full tensor
  reconstruction。
- configurable tensor publish 已薄接普通/legacy TP put 与 upsert 状态机，
  覆盖 single/batch、`preferred_segments` 校验和 TP shard 的 `group_id`
  重复映射。safetensor save/load 已复用审计后的 TensorMetadata codec 与
  `safetensors.torch`；同步入口使用 PyO3 管理的 Tokio runtime，load 将返回值
  收窄为 `PyDict` 后从 list-valued `keys()` 提取条目，不依赖 `dict_keys`
  sequence extraction。
- general parallelism 已建立 Python request types、DP/TP/EP/PP
  validation、canonical key codec、96-byte C++ manifest v1 codec 与多轴
  TensorMetadata shard encoder；single/batch put/upsert 已覆盖 requested
  shard 和 writer partition，并保留逐 key status/group-id 投影。
  single/batch ReadTarget 已覆盖 as-stored、metadata-matched shard、
  manifest-backed full reconstruction、manifest 失败后的 legacy/canonical
  metadata probing，以及从 stored TP layout 重分片 requested shard。
  owner-bearing single/batch put/upsert-from 与 get-into 均已接入；into 严格要求
  当前 Client 的已注册可写 owner range。C++ Tensor 公开方法名反向清单已归零，
  当前状态为静态实现完成、运行回归按约束延后。

仍需补充的外部证据、运行验收与明确范围外能力：

- Master mutation 反向审计发现的 HA start/task 缺口已按 Rust-only 边界收口：
  Put/BatchPut/Upsert Start 在返回 descriptor 前保存完整 object image；
  Copy/Move Start 用单条 `replication_start` 同时保存 object、allocator/quota
  所需 replica image 与 native replication task，completion/revoke 回放会清除
  source pin、task reservation 与二级索引。通用 Copy/Move task 的
  create/claim/complete/timeout/remove 现使用原子 `task_state_batch`，Drain 调度、
  Client TTL purge 与 finished-task reaper 同样进入该 durable 状态机。NoF
  heartbeat 与过期 Client 自动摘除 segment 也会写 durable tombstone，失败即
  fence。上述结论为源码与 authored replay case 的静态证据，未执行测试；
- Client 托管生命周期已静态收口：生产 Python 创建路径把
  `start_background_workers` 与 Client 所有权绑定，worker handle 与共享
  `Arc<tokio::sync::Mutex<Option<MooncakeClient>>>` 同生命周期；普通异步调用持有
  原位 owned mutex guard，不再 `Option::take()`/put-back。并发调用等待同一
  async mutex，future 取消只释放 guard，不会丢失 Client。`tear_down_all()` 与
  `close()` 先请求并等待 worker 退出，再串行执行 teardown；成功关闭后才从
  lifecycle slot 移除 Client。Engram 消费 Client 时请求 worker 退出，归还 Client
  时重新启动 worker。以上为静态源码证据，未执行运行回归；
- Python `BufferPool`/`RegisteredBufferPool` 的静态 P1 已收口：构造必须绑定
  Rust Store/Client，`RegisteredBufferPool` 与 C++ 一样是 `BufferPool` 别名；
  `acquire()` 返回原生 writable-buffer-protocol `BufferLease`，提供 `ptr`、
  requested `size`、幂等 release、export-count 保护、阻塞/超时、
  `max_regions`、容量 reservation 和关闭时 active-lease 拒绝。每个 allocation
  使用显式 alignment 的 Rust owner，并通过 Client generation-aware registry
  注册到 TE；release 只有在精确 registration 注销成功后才释放容量，in-flight
  DMA 会由既有 region lease 返回 busy。Rust 没有把 Client 的独占 staging
  buffer 同时暴露给 pool，而是对全部 region 使用 C++ overflow 等价路径；因此
  注册次数/性能不同，但 owner、zero-copy 与释放语义一致。已补纯状态 reservation、
  exhaustion、close 与 timeout authored tests，以及 Rust Python binding 的 alias、
  alignment、writable view、export gate、blocking/timeout、active-close 和析构注销
  回归向量，按当前约束未运行；
- Python `health_check` 已返回明确 bool，Store service/adapter 会等待
  `close()`；
- Client 反向 API/config 清单中的普通 KV 部署 P1 已静态收口：tenant-aware
  create、动态 owned segment、hostname-only 自动
  端口绑定/有限重试、IPv6 canonical endpoint、HugeTLB 的 C++ 环境变量名/
  page-size/strict failure，以及 HTTP metadata timeout 按 cluster prefix 清理
  `ram/`/`rpc_meta/` key 均已落地。hot-cache
  普通进程内路径已补齐，SHM/dummy acquire-release 作为目标部署启用时的独立
  可选能力保留；
- C++ Python Tensor 基础 CPU codec helper、普通 put/get/upsert、batch、
  owner-bearing 预注册 buffer into/from、legacy TP、general parallelism
  manifest/writer partition、safetensor 与 configurable publish 已完成静态
  实现，但尚未按当前约束执行运行回归。Rust
  structured-object generic payload 可复用新 codec 覆盖普通值传输，但不能据此
  宣称整个独立 integration surface 已打平；后续实现仍应保持 Python 薄、
  owner/region lease 下沉 Client/FFI；
- FilePerKey 与已知 ABI 的 C++ Offset v3 已具备 C++→Rust 离线迁移；两端格式
  不同，因此都是 importer 式重写而不是共享目录或原地打开。Offset 真实 C++ v3
  golden fixture 尚未补齐；旧 Rust FilePerKey 在没有权威 key 清单时仍无法从
  裸 value/清洗文件名保证无损恢复；
- C++ catalog snapshot payload 的跨版本 fixture 覆盖仍需扩展。
  具体缺口是固定 C++ 生产 bytes，而不是继续用 Rust `rmpv + zstd` 运行时生成：
  metadata 的 v1/v2a/v2b/v3/current-v4，以及 legacy/current Segment。manifest、
  descriptor 和 TaskManager 没有发现多版本分支，无需伪造版本矩阵；现有手工
  bytes/helper 已重命名为 `synthetic_cpp_*`，明确只作为 decoder shape coverage，
  不再形成虚假的 C++ fixture 完成证据；
- catalog decoder 的 expired-object cleanup 已补 hard-pin gate：先完成 v1-v4
  optional field 解码，再只移除 lease/soft-pin 均过期的非 hard-pinned Complete
  object。C++ 当前恢复 cleanup 同样遗漏该 gate，但按本计划“不以 C++ 同类缺陷
  作为验收标准”的原则，Rust 保持 hard pin 跨恢复不可驱逐；
- catalog Segment status decoder 显式覆盖 C++ 历史枚举 0–5：
  `OK/DRAINING/GRACEFULLY_UNMOUNTING` 保留对应语义，
  `UNDEFINED/DRAINED/UNMOUNTING` 归一为不可分配；其他数值 fail-closed，不再
  接受未知未来协议并静默丢失关联对象；
- catalog optional-field reader 现在区分“字段缺失”和“字段损坏”：`cs`、`ld`
  与 `discarded_replicas` 缺失仍兼容旧 snapshot，但重复字段、错误类型直接拒绝。
  client ownership 同时拒绝重复 client 和指向未知 Segment 的引用，metadata
  shard ID 校验 C++ 0–1023 范围及唯一性。Replica status 不再把 C++ 的
  `REMOVED/FAILED` 复活成 `Allocating`，而是统一映射到 Rust `Failed`；其余
  `UNDEFINED` 映射为 Rust `Undefined`，`INITIALIZED/PROCESSING` 均归一为
  Master 权威的 Rust `Allocating`，`COMPLETE` 映射为 Rust `Complete`。C++
  catalog 没有可恢复为独立 Rust `Written` 的阶段；Replica ID 以及 Memory
  offset-handle flag/array 也执行完整类型与 shape 校验；
- 按当前“不构建、不测试”约束延后的运行证据：真实 HF3FS shared library 上的
  fd register/deregister、32 MiB USRBIO 分片、FUSE namespace rename/fsync、
  restart/corruption/partial-I/O 故障恢复；global DISK 故障注入及 CXL DAX 设备
  回归。
- Client metrics 的 `MC_STORE_CLIENT_METRIC_INTERVAL` 周期日志与 interval
  read/write bandwidth 已静态实现；C++ SSD streaming-summary quantile time
  series 尚未实现。持久 Prometheus histogram 与 human-readable
  count/avg/p95/max summary 已覆盖核心运维 API。剩余 quantile time series 列为
  P2 监控输出细节，不阻塞经典 Store 数据语义平替。

## Phase 4：可选传输与存储后端

- TENT 生命周期、版本化 capability/request/status ABI（未实现；Rust Store 当前
  对 `MC_USE_TENT`/`MC_USE_TEV1` fail-fast，经典 Store parity 不修改 TE C++）；
- C++ LocalDisk `io_uring` 是 `StorageFile` 的可选性能实现，与 `PosixFile`
  共享 Store API、磁盘格式和恢复语义，且未编译 `USE_URING` 时配置会回落
  POSIX。Rust 的 POSIX 实现已覆盖功能语义；相同 NVMe queue-depth、O_DIRECT
  与 fixed-buffer 性能不计为 Store 功能 parity，若需要应单独立性能验收项；
- global DISK 核心数据面与容量事务已完成静态实现：tenant-scoped descriptor、
  raw data + durable identity sidecar、跨进程 mutation lock、quota/FIFO/水位驱逐、
  逐 key accepted-set、tenant remove-all 和 HA storage-config fence 均已接入；
  运行时、故障注入与回归证据待补。CXL 已完成静态 Store 路径：Master 使用
  单一全局 Cachelib-like allocator 并把每个 client mount 作为 alias，snapshot/
  oplog 从全部 live CXL replica 重建一次物理布局。catalog Segment legacy wire
  不含 protocol；reader 现在先读取 Rust allocator-config extension，并在
  `allocation_strategy=cxl` 时把所有 alias 恢复为 `protocol=cxl`，再解码 replica
  和重建 allocator。缺少 extension 的 C++ catalog 不猜测 CXL，因为 C++
  SegmentSerializer 原本就拒绝 Cachelib/CXL。Client 通过现有
  `getBaseAddr/registerLocalMemory` FFI 注册 TE 持有的 mmap，使用 device-relative
  offset，并强制写入本机 preferred alias。没有修改 TE C++ 或 C ABI；真实 DAX
  设备回归仍待执行；
- Bucket/Distributed/HF3FS 的局部 Master 文件工具不能证明生产 parity；调用链
  审计确认 LocalDisk offload/promotion 走 Client `AttachedLocalStorage`。Bucket
  已在该正确层级
  静态实现并复用现有 prepare/commit eviction、generation fencing、inventory
  recovery、promotion 与 remove-all 接口；包含多 key bucket、FIFO/LRU、
  accepted-set 部分提交、accepted-eviction durable journal、严格重启扫描与
  namespace lock。Distributed/HF3FS 现也已接入该接口：生产只接受 HF3FS，
  按 C++ 规则实现 XXH64 bucket/percent filename、健康探测与无驱逐语义；unsafe
  native ABI 隔离在 adapter 内，顶层使用安全 API。Rust generation envelope、
  temporary+rename 发布、严格 scan/recovery、generation-aware delete、稳定
  storage identity 与 HA inventory 已静态落地。真实 USRBIO/FUSE runtime 证据
  仍待执行；
- NoF 压力驱逐与 promotion 参数对齐（NoF 独立 allocator usage、高水位、
  allocation-pressure signal、pin/lease/busy gate 与最后副本清理已完成静态实现；
  promotion 参数已接入，均待回归验证）。

## 执行顺序

device-owner、ownerless registration API 删除、safe zero-copy Store Client
边界、HA promotion/follower lifecycle、standby mutation gate 与 Memory/NoF
restore-remount rebind 与 native snapshot v12（v7 atomic baseline、v8 Segment
host identity、v9 size-changing Upsert pending old charge、v10 allocator config
fence、v11 delayed replica reservation、v12 exact existing Move target）的静态
实现已经落地。统一 durability
fence、启动 preflight、bounded
watch reconnect、NoF
独立水位/分配压力淘汰、global DISK 容量事务与 CXL Store 逻辑也已完成静态收口。
Offset v3 离线迁移实现已落地；Bucket/Distributed/HF3FS 的层级审计已确认必须
落在 Client `AttachedLocalStorage`，而不是继续扩张 Master 文件工具。Client
Bucket 与 Distributed(HF3FS adapter) 均已完成静态实现。当前剩余真实
Offset/catalog golden fixture，以及按本阶段约束延后的完整 Rust 回归和
HF3FS/global DISK/CXL runtime 与故障注入证据。Batch CRUD 与 Copy/Move 的
CRUD 及 HA start/replay 静态功能缺口已收口；通用 task 也已有 durable
create/claim/terminal/remove 状态机，不再单独列为“部分打平”。Client 多
segment 已完成静态实现：以 UUID
而不是非唯一名称作为身份，补齐 `MC_MAX_MR_SIZE` 分片、全量 remount/teardown、
base-address local fast-path，以及动态 allocate/mount/free 对称 API；Master
发布 allocator alignment，Client 的 CachelibLike segment 使用 aligned owner。
现有 FFI 不暴露设备 clamp 和 NIC NUMA topology，RDMA/EFA/CXI 因此要求显式
`MC_MAX_MR_SIZE`；graceful free 通过 `GetSegmentsDetail` 轮询精确 UUID 的消失，
只有 Master 完成卸载后才释放 owner，超时保留 owner。不在 Store 顶层引入
unsafe。运行时/故障注入证据按当前“不构建、不测试”约束延后。
跨语言 Store RPC 互通永久不在执行范围内。

2026-07-27 delayed/discarded replica HA 复核补充：

- `MasterState::delayed_replica_releases` 取代 Upsert/Move 的进程内 sleep task；
- 过期 Put 沿用 C++ 的原始 `put_start_time + put_start_release_timeout`
  deadline；Upsert/Move 才使用 mutation 当前时间加 release timeout，避免在扫描
  发现过期时错误地重新开始完整 grace period；
- object mutation 与 delayed reservation 通过单条
  `object_delayed_release_batch` oplog 原子发布，到期先 durable tombstone、再释放
  allocator range；
- C++ `discarded_replicas` 的五类生产语义已逐路径反向核对：过期 Put、
  Upsert preemption、size-changing Upsert、MoveEnd source、过期 Copy/Move
  targets。最后一类原先在 Rust 中先释放 allocator、后写 oplog，现已改为先发布
  object/task/quota/reservation，再以同一轮 durable tombstone 释放；
- Put/BatchPut/BatchUpsert/Copy/Move revoke、source-invalid rollback 与 promotion
  failure 也统一遵守“metadata detach durable 后 allocator 才可复用”；普通
  Memory/NoF eviction 则在 durable object image 成功后才执行立即释放；
- PromotionAllocStart 在返回 writable descriptor 前写入 authoritative object
  image。promotion task 仍是可丢弃的本机调度状态，但 promoted range 不再能在
  failover 后无记录地被复用；
- tenant quota abort 不再静默吞掉 ledger mismatch；任何不可能的 abort 失败会
  fence Master，过期 replication/promotion 使用 projected ledger 后再提交状态；
- native snapshot 先以 v11 加入 delayed reservation，随后以 v12 加入 exact
  existing Move target；catalog 使用版本化 Rust extension；读取 C++ catalog 时
  解析顶层 `discarded_replicas`，不能再只读取 `shards`；
- snapshot/oplog allocator rebuild 同时纳入 live 与 delayed Memory/NoF range，
  missing segment、非 allocator replica、重复 ID、越界和重叠均 fail-closed；
- segment unmount 只剪除 delayed entry 中属于该 segment 的 replica；同一 entry
  跨多个 segment 时，其余 range 继续保留，不能因卸载其中一个 segment 而提前
  失去 HA reservation；
- 有 `put_start_time` 的 Put/Upsert `Allocating` replica 不再被误判为无 task
  orphan；只有既无 Put/Upsert authority、也无 Copy/Move task 的 legacy target
  才会在恢复时丢弃；
- 已补 native restore range 保留/重叠拒绝、catalog extension round-trip、
  oplog upsert/tombstone allocator 原子重放、跨 segment unmount 剪枝、过期
  Copy durable reservation/tombstone、revoke reservation/tombstone、quota abort
  mismatch fence，以及 synthetic C++ discarded payload 回归代码；按当前约束
  未运行。

FilePerKey/Offset 的 accepted-eviction durable journal 仍可作为额外 crash
hardening 实施，但 C++ Store 同样只保存进程内 pending eviction，因此它不是
Rust 平替 C++ 的独有功能缺口。若完成标准提升为“Master 已接受 eviction 后，
任意进程崩溃都必须自动收敛本地文件与 metadata”，则需把该 journal 重新列为
独立正确性任务。

2026-07-27 tenant-scoped mutation guard 复核：

| mutation surface | identity | serialization boundary |
| --- | --- | --- |
| Put/Upsert/Remove、Batch、Copy/Move | `TenantId::make_scoped_key` | 单 key guard；批量 lease 使用排序后的 multi-key guard |
| lease/group mutation | scoped key + authoritative tenant/group | 单 key 快路径；跨成员更新升级到 snapshot-wide guard |
| offload/promotion admission 与 completion | scoped task key | admission 内部或 RPC 单 key guard |
| eviction、tenant quota eviction | object 中的 authoritative tenant | 单对象 key guard，或整个 eviction epoch 的 snapshot-wide guard |
| expired Put/task、client purge、segment recovery/unmount | persisted scoped key | 单 key guard；跨 segment/全局清理使用 snapshot-wide guard |
| oplog replay、snapshot restore/rebuild | durable scoped identity | replay entry 与 restore epoch 使用 snapshot-wide guard |

逐 mutation-site 反向检查没有发现 raw user key 直接写入 `objects`、
`processing_keys`、replication/promotion/offload task map 的生产路径。辅助函数
`push_offloading_queue`、`clear_*_task`、staged promotion detach 只在上述 guard
覆盖的调用链中使用；promotion admission 自行获取 scoped key guard。

本次复核另发现 quota reservation rollback 的错误传播不完整：账本 mismatch
虽然会 fence Master，部分 allocation/revoke 路径仍可能返回普通业务错误，batch
还可能继续处理后续 item。`abort_tenant_quota` 现返回强制处理的
`Result<(), Status>`；Put/Upsert/Batch/Copy/Move/Promotion/segment recovery
全部使用 `?` 传播 `Unavailable`。authored invariant test 同时断言 RPC status 与
Master fence，防止以后重新静默吞掉 rollback 失败。

后续 quota/HA 复核又关闭三处同类分叉：

- `account_removed_object_quota` 不再只记录 warning 并 fence；它返回显式错误，
  Remove、Revoke、Evict、reaper 与 segment cleanup 在 mismatch 后不会继续写
  durable remove、释放 allocator 或发布成功；
- settle/additional-settle/move settle+release 的 `AccountingMismatch` 统一转换为
  fail-closed `Unavailable` 并 fence，不能再退化成普通 `FailedPrecondition`；
- committed 对象上的 promotion staged replica 不再在 snapshot/oplog restore 后
  误设 object-level processing gate。没有 durable promotion task 的 staged
  Memory/NoF range 会从仍可读的 Complete 对象上摘除，并通过
  `object_delayed_release_batch` quarantine 一个完整 grace period后回收。

segment unmount replay 也会按 Copy/Move source、targets、promotion staged
segment 与 offload source 精确清理受影响 task。即使对象仍有其他副本，也会
回滚 task reservation、解除 surviving source pin 并清理 runtime queue；其余
Allocating target 由上述 orphan quarantine 收敛。authored tests 覆盖恢复后对象
保持可读且 staged range 被 quarantine，以及 unmount target 后对象保留、
replication task/reservation/refcount 同步清零。

2026-07-27 Etcd oplog term-fencing 复核：

- standby reader 与 leader writer 已拆分；promotion 必须用 acquired Etcd
  `master_view` mod revision 重建 writer，不能复用无 fencing token 的 reader；
- append 以单个 transaction 同时校验 election revision、前序 latest 与目标 entry
  不存在，再提交 records + latest，关闭双 leader 从相同 sequence 覆盖写入的窗口；
- latest 初始化、snapshot sequence 与 range cleanup 也统一使用 election compare；
  reader-only 实例拒绝全部 mutation；
- 任意 CAS/transport failure poison oplog，交由现有 durability fence 关闭服务并
  结束 leader term。纯 producer-view/sequence validation 测试已编写，按当前约束
  未运行。

2026-07-27 tenant-quota acquire barrier 复核：

- 新 leader 不能只等待第一次 admin quota mutation 才把 connector term 推进；
  final catch-up 后、serve gate 打开前必须执行 connector-side term barrier；
- file barrier 与普通 save 共用 sibling `flock`，Etcd barrier 共用
  mod-revision CAS；两者都保留 barrier 前最后获胜的 policy 内容，仅推进
  `producer_view_version`；
- barrier 返回的 policy 会替换内存 explicit-policy layer，但不清空 standby
  已重建的 used/reserved/committed/object counters；失败则不进入 serving 并释放
  leadership；
- 因而 old-term write 与 barrier 形成全序：前者在前则被 successor 吸收，在后则
  被 stale-term check 拒绝。相关 authored tests 按当前约束未运行。

2026-07-27 allocator release ownership 复核：

- Offset allocator 已从“只有 free range”改为同时保存精确 live
  `offset -> requested_size`，Cachelib allocation 也保存 requested size；伪造
  size、过期 descriptor 与 double-free 均不能再把 live range 变为空闲；
- `SegmentAllocator::release` 在任何修改前校验完整 batch，包括 segment identity、
  CXL alias/global ownership 与重复 physical target，释放后的 usage 使用 checked
  subtraction；
- snapshot/oplog rebuild 会在安装恢复状态前核对 replica 与普通 segment/共享
  CXL alias 的 UUID/name identity；不再允许损坏 descriptor 延迟到首次 release
  才触发错误；
- Service 同时释放 Memory/NoF 时使用固定锁序并先校验两个 allocator，禁止一个
  成功、另一个失败的半提交；allocator/authoritative metadata mismatch 会
  durability-fence Master，并显式终止 RPC 或 oplog replay；
- object auxiliary task 只在 allocator release 成功后清理，失败时保留诊断/
  重建线索。相关 authored invariant tests 按当前约束未运行。

2026-07-27 HA replay transaction/identity 复核：

- remove/put-revoke replay 已在 mutation 前构造完整 post-record allocator
  candidate；commit 不再释放旧 runtime allocator，避免 stale allocator 在对象
  已删除后失败、同一 sequence 重试观察不同状态；
- runtime offload/promotion 辅助索引通过 `service` 的显式 crate-visible
  re-export 清理，HA 不再越过私有 module 路径；
- 无完整 replica image 的 legacy `put_end` 必须命中 snapshot 已恢复对象，否则
  不推进 expected sequence，也不清除 processing marker；
- 无重建字段的 legacy Memory/NoF mount 只有在 snapshot 已存在唯一同名 segment
  且 allocator 存在时才作为幂等 informational record 推进；
- NoF ReMount 缺失 UUID 保留 nil 并在 RPC validation 返回 InvalidArgument，
  不再为持久 identity 生成随机值。相关 authored tests 按当前约束未运行。

2026-07-27 mount topology/allocator identity 复核：

- Memory 与 NoF segment UUID 是同一全局 identity namespace；live mount 与
  批量 remount 在两个方向都检查对方 topology 和 allocator，禁止仅依赖单侧 map
  顺序产生不对称 collision；
- 本类型 allocator-only identity 会在写 oplog 前 fence，保留已有 used bytes；
  topology-only identity 会在 rebind 前 fence，不再作为客户端
  FailedPrecondition 后继续服务；
- remount 在提交第一个新 segment 前预检完整 batch，拒绝重复 Memory runtime
  range、重复 NoF endpoint 及任一 allocator/topology mismatch，后部冲突不会
  留下前部半批发布；
- legacy Memory remount 缺失 `segment_ids` 且无 snapshot topology baseline 时，
  使用与普通 Mount 相同的 canonical runtime fingerprint 派生稳定 UUID，不再以
  随机 UUID 破坏请求重试的 identity 幂等性；
- Memory/NoF remount 与普通 Mount 使用相同的 Cachelib `u32` slab-count 容量
  上限，不能通过批量重挂创建 allocator 无法精确表示的 segment；
- HA replay 在 `add_segment` 前同时排除本类型 allocator-only 和跨类型
  topology/allocator collision，失败时不推进 expected sequence，也不覆盖
  live/quarantined range；
- HA replay 复验 follower 的 CXL enable/size、NoF enable、Cachelib alignment
  与 slab-count capability，配置漂移或损坏 oplog 不能绕过 leader producer
  validation；
- snapshot topology 在 live-state replacement 前校验 Memory/NoF durable
  identity、各类型 UUID 唯一、跨类型 UUID 排他、NoF enable/status capability
  及 Cachelib 表示范围；allocator restore 在构造 Cachelib layout 前完成
  slab-count checked conversion；
- Memory/NoF live mount、remount preflight 与 replay collision authored tests
  已编写，按当前约束未运行。

2026-07-27 Store Client owner-bearing submit 边界复核：

- `ClientTransferEngine` 不再用 safe 外观包裹 `unsafe submit_owned_transfer`；
  Client 业务层不再分配/传播 raw `OwnedBatchId`，也不构造 raw
  `TransferRequest`；
- scratch staging allocation 在创建时消费进 FFI `RegisteredMemory` owner，
  bounded CPU copy 必须持有当前独占 staging lease，且 outstanding typed region
  或 native in-flight claim 会拒绝 local mutation；
- 单对象、multi-buffer、put-parts、batch-get、range-read、LocalDisk offload
  与 zero-copy 路径统一构造 `RegisteredTransferRequest`；read 需要 writable
  region，write 需要 readable region，engine/range/target-overflow 在 native
  submission 前验证；
- `SubmittedRegisteredBatch` 自持 native batch、requests、region leases 与
  registration claims；native reject 同样交给 reaper，只有 `try_release`
  证明 quiescence 后才释放 owner capability 并关闭 segment。无法证明时继续
  fail-closed 泄漏完整 job；
- replica base/offset/relative target address 统一 checked-add；typed request
  在 native batch 创建前失败时立即关闭 segment，不把新增 validation 变成句柄
  泄漏；
- unsafe 只存在于 `transfer-engine-ffi` 以及 Client memory/accelerator adapter，
  未修改 TE C++/C ABI。staging exclusive-lease 与 address-overflow authored
  tests 已编写，按当前约束未运行。

2026-07-27 同尺寸原地 Upsert durable generation/quota 复核：

- 同尺寸 Upsert 复用既有 Memory allocation；descriptor 从 Complete 暂时回到
  Allocating 不代表新增 reservation，已有 committed physical charge 在整个
  generation 中保持不变；
- durable object-image 校验不再机械地只统计 Complete Memory，而是用
  `put_start_time` 和 Put/Upsert write-target 状态识别原地 inflight generation；
  没有 PutStart authority 的 Copy/Move Allocating target 仍由 replication-task
  reservation 记账，不能混入 object committed charge；
- PutEnd、直接/批量 Revoke 与过期 Put reaper 在 write generation 终止时清除
  `put_start_time`；已 committed 的原地 Upsert 完成不会重复 settle quota，但会
  关闭 processing gate；
- snapshot restore 从 replica descriptor checked 重建 committed/reserved
  Memory charge，恢复原地 generation 的 processing marker、allocator ownership
  与不可读状态；终态或无 Put/Upsert authority 的对象不会被误设 processing；
- typed replay 使用同一 lifecycle-aware 校验并保持 inflight generation 不进入
  client-readable index；legacy `put_end` 可完成 snapshot 中已 committed 的
  原地 generation，同时 fail-closed 拒绝 size drift；
- helper、PutEnd、snapshot、typed replay 与 legacy replay authored tests 已
  编写，按当前约束未运行。

2026-07-27 mutation replica selector 协议复核：

- concrete replica descriptor 的 legacy wire fallback 与 client mutation
  selector 不再共用同一个转换函数；前者继续兼容历史 metadata，后者必须
  `ReplicaType::try_from` 成功；
- 未知 selector 不再回退为 Memory，避免畸形 PutEnd/UpsertEnd、Revoke 与
  Disk eviction 完成或删除错误介质；
- 单请求及共享 batch selector 返回 InvalidArgument；逐 entry batch 将其映射为
  `InvalidState` 并继续其他 entry，不取得坏 entry 的 key mutation guard；
- checked selector authored test 已编写，按当前约束未运行。

同轮 legacy HA completion 输入复核：

- `put_end.size` 仅在字段缺失或 Null 时解释为 legacy unspecified；
- 字段存在但不是 u64 时 fail-closed，不再用 0 绕过 authoritative object size
  校验；
- 拒绝记录时保留 object、processing marker 与 expected sequence。相关 authored
  replay test 已编写，按当前约束未运行。

2026-07-27 Redis/K8s coordinator term/session fencing 复核：

- Redis Lua acquire 响应必须是精确 `{0}` 或 `{1, positive_view}`；不再为缺失 term
  生成 view 1，持久 view 0、畸形 renew/release code、空地址及 TTL overflow
  全部 fail-closed；
- Redis keepalive interval 从固定 3 秒改为 TTL/3（100 ms–3 s bounded）；首次
  connect 或任一 reconnect/renew 失败都会 demote 并清除 active owner token；
- K8s held Lease 的 `lease_transitions` 必须存在且为正数，API replace/create
  成功响应必须返回精确预期 address+term，不再用调用方本地值 fallback；
- 同地址进程不能获取仍存活的 Lease；Lease 过期/释放后的每次 acquisition 都
  checked 增加 transition，避免 pod/address 复用重复旧 term；
- K8s renew/release 同时 fence holder address 与 session view version，旧 session
  不能续约或删除同地址 successor，已经过期的 session 也不能自行复活；
- Redis result/TTL interval 和 K8s view/transition/acquisition/session authored
  tests 已编写，按当前约束未运行。

task completion request status 同轮改为 checked decoder：未知 i32 在 RPC 边界直接
返回 InvalidArgument；catalog durable task type/status 继续使用既有严格 decoder。
pure/RPC authored tests 已编写，按当前约束未运行。

Etcd losing-campaign discovery 同轮完成并发顺序收敛：

- campaign 前不再读取并向 backend 传递可过期的 current view；
- create-only transaction compare 失败证明事务提交点存在其他 owner，调用方撤销
  本次 lease 后再读取 election key，只返回 post-transaction authoritative view；
- transaction 成功仍从 Etcd 读取实际地址、key lease ID 与 mod revision，并要求
  address+lease ID 精确匹配本次 contender/session；因此同地址 successor 不能被
  旧 session 冒认。缺失 key、unleased key、owner/lease mismatch、空地址、非正
  revision 或 zero session token 均 fail-closed，不使用本地合成 view；
- address+lease identity 与 zero-token authored tests 已编写，按当前约束未运行；
- 此项只有真实 Etcd 并发 fixture 才能提供独立运行时证据；当前按约束仅保留
  transaction/read 顺序的代码审查证据，不伪造 mock 结论。

2026-07-27 C++ direct-oracle 收口复核：

- 审计结论重新限定为 C++ 公开入口、状态分支、错误与副作用的直接映射；
  coordinator、Remote Pull 和 oplog durability 作为 Rust-only HA hardening
  单列，不能替代 classic parity 证据；
- TaskManager 的 batch-size-zero、全局 pending admission、UUID collision
  retry、初始 message 与 terminal retry 已恢复 C++ 语义；未知 completion enum
  继续按 C++ terminal-status predicate fail-closed；
- Upsert preemption 在没有 COMPLETE survivor 时，以单条
  `object_delayed_release_batch` 同时持久化 authoritative absence 与 retired
  allocator ranges；delayed-release replay 使用 write-generation 状态而不是
  quota ownership 判断可读性；
- Drain missing-task terminal failure、Created/Planning/Running 推进、
  `last_updated_at` 与公开 message 已按 C++ 收敛；Rust UUID-bound segment
  identity 和 durable status record 是 replacement 所需适配；
- CopyStart 与 Drain request 不再添加 C++ 没有的 target 去重规则；重复 target
  仍按请求逐项进入 quota/allocation 或 target selection。

## 静态门禁与待执行回归

- 本轮允许的静态门禁：`cargo fmt --all -- --check`、
  `cargo metadata --no-deps --format-version 1`、`git diff --check`；三项已在
  本轮 HA replay、mount/remount identity、owner-bearing Client submit 与同尺寸
  原地 Upsert durable generation/quota、coordinator fencing 与 task request
  decoder 修改后重新执行并通过；
- 额外边界门禁已通过：C++ Store/TE C++、`mooncake-p2p-store` 与
  `mooncake-store-c` 零 diff；Client 业务目录不存在 unsafe block、raw
  `TransferRequest`/`OwnedBatchId` 或 `submit_owned_transfer`；9 个生产 submit
  site 全部走 typed registered submission；Master tonic trait 仍为 74/74
  foreground guards；
- 当前明确禁止执行的类型检查/运行回归：`cargo check --workspace`；
- Client lib 176/176，FilePerKey integration 33/33，Offset integration 9/9；
- Master lib 101/101，snapshot storage 23/23，mount/graceful 12/12，catalog 12/12。

以上计数是本轮 LocalDisk fail-closed、snapshot 构造错误传播与 durable
GracefulUnmount 修改之前的历史基线，不是当前工作树的完成证据。按当前阶段约束
先完成静态逻辑审查；之后必须重新执行这些门禁。production Transfer Engine 仍
必须单独验收，不能由 Rust 控制面回归代替。

## 完成定义

只有在以下条件全部满足后，才可把 Rust Store 标记为“与 C++ Store 功能打平”：

1. 审计矩阵不存在未豁免的 P0/P1；
2. 所有豁免项记录用户影响、替代路径和删除日期；
3. 单元、集成、故障注入、恢复和跨版本测试覆盖关键状态机；
4. Rust Store 不链接或运行时依赖 C++ Store；
5. 文档准确描述实际支持的 backend、协议与构建模式。
