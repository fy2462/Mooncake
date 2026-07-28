//! # MasterState — Master 核心状态机 / Core State Machine
//!
//! MasterState 是 master 服务的全局状态容器，所有数据结构均为并发安全类型。
//! DashMap 用于高并发读写（分段锁），RwLock 用于需要事务性一致性的操作。
//!
//! MasterState is the global state container of the Master service.
//! All data structures are concurrency-safe types.
//! DashMap is used for high-concurrency reads/writes (sharded locks),
//! while RwLock is used for operations requiring transactional consistency.
//!
//! ## 核心数据结构 / Core Data Structures
//!
//! | 字段 / Field | 类型 / Type | 用途 / Purpose |
//! |--------------|-------------|----------------|
//! | `clients` | `DashMap<Uuid, ClientEntry>` | 客户端注册表：记录客户端信息、地址、心跳时间 |
//! | `objects` | `DashMap<String, ObjectEntry>` | 对象元数据表：key → 副本列表、大小、租约、pin 状态 |
//! | `processing_keys` | `DashMap<String, ()>` | 进行中的 PutStart key 集合：防止并发写冲突 |
//! | `client_objects` | `DashMap<Uuid, HashSet<String>>` | 每个客户端拥有的对象索引：加速客户端下线时批量清理 |
//! | `segments` | `DashMap<Uuid, SegmentEntry>` | Memory segment 注册表：容量、使用量、所属客户端 |
//! | `nof_segments` | `DashMap<Uuid, NoFSegmentEntry>` | NoF (NVMe-oF) segment 注册表：远程 SSD 存储分配 |
//! | `local_disk_segments` | `DashMap<Uuid, LocalDiskSegmentEntry>` | 本地磁盘 segment：offload/promotion 队列、SSD 容量 |
//! | `tasks` | `DashMap<Uuid, TaskEntry>` | 任务队列：Copy/Move 任务的创建、分配、执行 |
//! | `replication_tasks` | `DashMap<String, ReplicationTaskEntry>` | 进行中的复制任务：按 key 索引 |
//! | `offloading_tasks` | `DashMap<String, OffloadingTaskEntry>` | 进行中的下沉任务：内存 → 本地磁盘 |
//! | `promotion_tasks` | `DashMap<String, PromotionTaskEntry>` | 进行中的提升任务：本地磁盘 → 内存 |
//! | `promotion_sketch` | `RwLock<CountMinSketch>` | Count-Min Sketch 频率统计：promotion 准入控制 |
//! | `promotion_candidates` | `DashMap<String, PromotionCandidate>` | 因瞬时限制等待后台重试的提升候选 |
//! | `drain_jobs` | `DashMap<Uuid, DrainJobEntry>` | Drain 任务：segment 下线前的数据迁移 |
//! | `allocator` | `RwLock<SegmentAllocator>` | Memory segment 分配器：管理内存段的空间分配 |
//! | `nof_allocator` | `RwLock<SegmentAllocator>` | NoF segment 分配器：管理远程 SSD 段的空间分配 |
//! | `storage_backend` | `RwLock<Option<StorageBackend>>` | 快照持久化后端：状态的定期备份与恢复 |
//! | `promotion_in_flight` | `AtomicUsize` | 全局在途晋升计数：防止并发晋升超过队列限制 |
//! | `view_version` | `AtomicI64` | 全局视图版本号：通知客户端拓扑变更 |
//! | `runtime_config` | `MasterRuntimeConfig` | 运行时配置：所有可调参数的汇总 |
//! | `pending_remote_pulls` | `DashMap<String, RemotePullEntry>` | 进行中的远端拉取：S3 等远端源的回源协调 |

use crate::TenantId;
use crate::allocator::{AllocationStrategy, MemoryAllocatorKind, SegmentAllocator};
use crate::count_min_sketch::CountMinSketch;
use crate::kv_event::KvEventPublisher;
use crate::storage_backend::StorageBackend;
use crate::tenant_quota::TenantQuotaTable;
use dashmap::DashMap;
use mooncake_store_core::{NoFSegment, ObjectDataType, ReplicaDescriptor, TaskInfo};
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

mod state_config;
mod state_drain;
pub use state_config::MasterRuntimeConfig;
pub(crate) use state_drain::{ActiveDrainTask, DrainJobEntry, DrainSourceSegment};

const KEY_MUTATION_STRIPES: usize = 1024;

/// Serializes compound metadata mutations for the same tenant-scoped key.
///
/// A fixed stripe table avoids an unbounded per-key lock map. Different keys
/// may occasionally share a stripe, which only reduces concurrency; it does
/// not weaken correctness. Multi-key callers acquire unique stripes in sorted
/// order so overlapping batches cannot deadlock.
pub(crate) struct KeyMutationCoordinator {
    snapshot_barrier: RwLock<()>,
    stripes: Vec<Mutex<()>>,
    operation_stripes: Vec<Mutex<()>>,
}

pub(crate) struct KeyOperationGuard<'a> {
    _stripe: MutexGuard<'a, ()>,
}

pub(crate) struct KeyMutationGuard<'a> {
    _snapshot: RwLockReadGuard<'a, ()>,
    _stripe: MutexGuard<'a, ()>,
}

pub(crate) struct KeyMutationGuards<'a> {
    _snapshot: RwLockReadGuard<'a, ()>,
    _stripes: Vec<MutexGuard<'a, ()>>,
}

pub(crate) struct ForegroundRequestGate {
    active: Mutex<usize>,
    drained: Condvar,
}

pub(crate) struct ForegroundRequestGuard {
    gate: Arc<ForegroundRequestGate>,
}

impl ForegroundRequestGate {
    pub(crate) fn new() -> Self {
        Self {
            active: Mutex::new(0),
            drained: Condvar::new(),
        }
    }

    fn drain(&self) {
        let mut active = self.active.lock();
        while *active != 0 {
            self.drained.wait(&mut active);
        }
    }
}

impl Drop for ForegroundRequestGuard {
    fn drop(&mut self) {
        let mut active = self.gate.active.lock();
        debug_assert!(*active > 0, "foreground request count underflow");
        *active = active.saturating_sub(1);
        if *active == 0 {
            self.gate.drained.notify_all();
        }
    }
}

impl Default for KeyMutationCoordinator {
    fn default() -> Self {
        Self {
            snapshot_barrier: RwLock::new(()),
            stripes: (0..KEY_MUTATION_STRIPES).map(|_| Mutex::new(())).collect(),
            operation_stripes: (0..KEY_MUTATION_STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }
}

impl KeyMutationCoordinator {
    fn stripe_index(&self, scoped_key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        scoped_key.hash(&mut hasher);
        hasher.finish() as usize % self.stripes.len()
    }

    pub(crate) fn lock(&self, scoped_key: &str) -> KeyMutationGuard<'_> {
        let snapshot = self.snapshot_barrier.read();
        let stripe = self.stripes[self.stripe_index(scoped_key)].lock();
        KeyMutationGuard {
            _snapshot: snapshot,
            _stripe: stripe,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_lock_stripe_for_test(&self, scoped_key: &str) -> Option<MutexGuard<'_, ()>> {
        self.stripes[self.stripe_index(scoped_key)].try_lock()
    }

    /// Serialize a complete PutStart/UpsertStart operation, including quota
    /// eviction retries that must temporarily release the mutation stripe.
    ///
    /// This deliberately does not enter the snapshot barrier: the operation
    /// guard protects request identity, while each actual state mutation still
    /// takes `lock`/`lock_many`. Quota eviction may therefore acquire the
    /// exclusive snapshot barrier without recursively waiting on this request.
    pub(crate) fn lock_operation(&self, scoped_key: &str) -> KeyOperationGuard<'_> {
        KeyOperationGuard {
            _stripe: self.operation_stripes[self.stripe_index(scoped_key)].lock(),
        }
    }

    pub(crate) fn lock_many<'a, I>(&'a self, scoped_keys: I) -> KeyMutationGuards<'a>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let snapshot = self.snapshot_barrier.read();
        let mut indices = scoped_keys
            .into_iter()
            .map(|key| self.stripe_index(key))
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        let stripes = indices
            .into_iter()
            .map(|index| self.stripes[index].lock())
            .collect();
        KeyMutationGuards {
            _snapshot: snapshot,
            _stripes: stripes,
        }
    }

    pub(crate) fn lock_snapshot(&self) -> RwLockWriteGuard<'_, ()> {
        self.snapshot_barrier.write()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromotionCandidateReason {
    Watermark,
    QueueCap,
    PushFailed,
}

#[derive(Debug, Clone)]
pub(crate) struct PromotionCandidate {
    pub(crate) sketch_score: u8,
    pub(crate) first_seen: Instant,
    pub(crate) last_seen: Instant,
    pub(crate) retry_after: Instant,
    pub(crate) last_reason: PromotionCandidateReason,
    pub(crate) last_error_code: Option<i32>,
    pub(crate) retry_count: u32,
}

/// Master 服务的全局状态容器。
/// processing_keys 跟踪正在 PutStart 但未 Complete 的 key，防止并发冲突。
/// client_objects 维护每个客户端的对象索引，加速客户端下线时批量清理。
///
/// Global state container for the Master service.
/// `processing_keys` tracks keys that are in PutStart but not yet Complete, preventing concurrent conflicts.
/// `client_objects` maintains per-client object indices for fast batch cleanup on client disconnection.
pub(crate) struct MasterState {
    /// 客户端注册表 / Client registry: client_id → address, last_ping, etc.
    pub(crate) clients: DashMap<Uuid, ClientEntry>,
    /// 已完成 remount 的客户端 / Clients that completed remount and may receive OK on Ping.
    pub(crate) ok_clients: DashMap<Uuid, ()>,
    /// 对象元数据表 / Object metadata store: key → replicas, size, lease, pin status.
    pub(crate) objects: DashMap<String, ObjectEntry>,
    /// Serializes compound object mutations that span multiple concurrent maps.
    pub(crate) key_mutations: KeyMutationCoordinator,
    /// 进行中的 key 集合 / In-flight key set: keys currently in PutStart (not yet PutEnd).
    pub(crate) processing_keys: DashMap<String, ()>,
    /// 每个客户端的对象索引 / Per-client object index: client_id → set of owned keys.
    /// 加速客户端下线时的 O(N_keys) 查找 / Enables O(N_keys) lookup on client disconnection.
    pub(crate) client_objects: DashMap<Uuid, HashSet<String>>,
    /// Memory segment 注册表 / Memory segment registry: segment_id → capacity, usage, owner.
    pub(crate) segments: DashMap<Uuid, SegmentEntry>,
    /// Replica ranges removed from object metadata but intentionally retained
    /// until their RDMA grace period expires.
    pub(crate) delayed_replica_releases: DashMap<Uuid, DelayedReplicaReleaseEntry>,
    /// Durable graceful-unmount intents keyed by memory segment ID.
    ///
    /// Deadlines use Unix epoch milliseconds so native/catalog snapshots and
    /// oplog replay can preserve the remaining grace period across processes.
    pub(crate) graceful_unmounts: DashMap<Uuid, GracefulUnmountSnapshotEntry>,
    /// NoF (NVMe-oF) segment 注册表 / NoF segment registry: segment_id → remote SSD storage.
    pub(crate) nof_segments: DashMap<Uuid, NoFSegmentEntry>,
    /// 本地磁盘 segment 注册表 / Local disk segment registry:
    /// durable storage identity → active session and work queues.
    pub(crate) local_disk_segments: DashMap<Uuid, LocalDiskSegmentEntry>,
    /// Ephemeral process client ID → durable LocalDisk storage identity.
    pub(crate) local_disk_client_sessions: DashMap<Uuid, Uuid>,
    /// 任务队列 / Task queue: task_id → TaskEntry (copy/move tasks).
    pub(crate) tasks: DashMap<Uuid, TaskEntry>,
    /// 进行中的复制任务 / In-flight replication tasks: key → ReplicationTaskEntry.
    pub(crate) replication_tasks: DashMap<String, ReplicationTaskEntry>,
    /// 进行中的下沉任务 / In-flight offloading tasks: key → OffloadingTaskEntry.
    pub(crate) offloading_tasks: DashMap<String, OffloadingTaskEntry>,
    /// 进行中的提升任务 / In-flight promotion tasks: key → PromotionTaskEntry.
    pub(crate) promotion_tasks: DashMap<String, PromotionTaskEntry>,
    /// Count-Min Sketch 频率统计 / Frequency sketch: approximate access counts for promotion gating.
    pub(crate) promotion_sketch: RwLock<CountMinSketch>,
    /// Transient promotion candidates waiting for bounded background retry.
    pub(crate) promotion_candidates: DashMap<String, PromotionCandidate>,
    /// Drain 任务注册表 / Drain job registry: job_id → DrainJobEntry.
    pub(crate) drain_jobs: DashMap<Uuid, DrainJobEntry>,
    /// Memory 分配器 / Memory segment allocator: manages space allocation within Memory segments.
    pub(crate) allocator: RwLock<SegmentAllocator>,
    /// NoF 分配器 / NoF segment allocator: manages space allocation within NoF segments.
    pub(crate) nof_allocator: RwLock<SegmentAllocator>,
    /// Set after NoF allocation pressure so the eviction worker runs even
    /// when allocator fragmentation is not reflected by the byte watermark.
    pub(crate) nof_eviction_requested: AtomicBool,
    /// 快照存储后端 / Snapshot storage backend: periodic backup and restore.
    pub(crate) storage_backend: RwLock<Option<StorageBackend>>,
    /// Shared oplog sink used by RPC handlers and background mutation workers.
    ///
    /// Keeping one handle in `MasterState` prevents background eviction,
    /// reaping, and stale-handle cleanup from mutating the authoritative
    /// object table without emitting the same object image as foreground RPCs.
    pub(crate) oplog_manager: Arc<crate::oplog::OpLogManager>,
    /// 全局在途晋升计数 / Global in-flight promotion counter: prevents exceeding queue limit.
    pub(crate) promotion_in_flight: AtomicUsize,
    /// Exact number of entries reserved in `promotion_candidates`.
    pub(crate) promotion_candidate_count: AtomicUsize,
    /// Fair partition cursor used by promotion retry scanning.
    pub(crate) promotion_retry_cursor: AtomicUsize,
    /// 全局视图版本号 / Global view version: notifies clients of topology changes.
    pub(crate) view_version: AtomicI64,
    /// Election-issued leadership term. Unlike `view_version`, topology
    /// updates inside one leader term never change this value.
    pub(crate) leadership_view_version: AtomicU64,
    /// 运行时配置 / Runtime configuration: all tunable parameters.
    pub(crate) runtime_config: MasterRuntimeConfig,
    /// Whether the service plane should accept client/admin traffic.
    pub(crate) service_available: AtomicBool,
    /// Owned guards span full tonic handler futures. Demotion closes
    /// `service_available` and drains this counter before returning, including
    /// requests that passed the interceptor but were waiting for a key stripe.
    pub(crate) foreground_request_gate: Arc<ForegroundRequestGate>,
    /// Drains and excludes authoritative background mutations while this
    /// process is a standby. Workers take a read guard immediately before
    /// mutating state; demotion closes `service_available` first and then
    /// takes the write guard, so no pre-checked worker can leak a mutation
    /// into the standby term.
    pub(crate) background_mutation_gate: RwLock<()>,
    /// Irreversible process-lifetime fence raised after an ambiguous durable
    /// mutation failure. A leadership callback cannot reopen the serving gate;
    /// recovery requires rebuilding the service from durable state.
    pub(crate) service_fenced: AtomicBool,
    /// Per-tenant quota admission/accounting table.
    pub(crate) tenant_quotas: RwLock<TenantQuotaTable>,
    /// Tracks in-flight remote source pulls so only one node fetches a given key.
    /// 远端回源协调表：确保同一 key 只有一个节点从远端（如 S3）拉取数据。
    pub(crate) pending_remote_pulls: DashMap<String, RemotePullEntry>,
    /// NoF 心跳状态表 / NoF heartbeat state table: segment_id → heartbeat tracking state.
    pub(crate) nof_heartbeat_states: DashMap<Uuid, NoFHeartbeatState>,
    /// Optional KV events publisher shared with background workers.
    pub(crate) kv_event_publisher: Arc<KvEventPublisher>,
}

impl MasterState {
    pub(crate) fn begin_foreground_request(self: &Arc<Self>) -> Option<ForegroundRequestGuard> {
        if !self.service_available.load(Ordering::Acquire)
            || self.service_fenced.load(Ordering::Acquire)
        {
            return None;
        }
        let mut active = self.foreground_request_gate.active.lock();
        if !self.service_available.load(Ordering::Acquire)
            || self.service_fenced.load(Ordering::Acquire)
        {
            return None;
        }
        *active = active.checked_add(1)?;
        Some(ForegroundRequestGuard {
            gate: Arc::clone(&self.foreground_request_gate),
        })
    }

    pub(crate) fn drain_foreground_requests(&self) {
        self.foreground_request_gate.drain();
    }

    /// Irreversibly close this process after an authoritative in-memory
    /// mutation could not be made durable. Leadership changes must not reopen
    /// the gate; recovery requires reconstructing state from durable inputs.
    pub(crate) fn fence_after_durability_failure(
        &self,
        operation: &str,
        error: &crate::ha::HaError,
    ) {
        self.service_fenced.store(true, Ordering::Release);
        self.service_available.store(false, Ordering::Release);
        tracing::error!(
            operation,
            %error,
            "fenced Master after authoritative mutation persistence failure"
        );
    }

    /// Stop serving mutations after an authoritative state invariant fails.
    /// The durable object image/remove remains the recovery source of truth;
    /// continuing with a divergent runtime ledger would compound corruption.
    pub(crate) fn fence_after_invariant_failure(&self, operation: &str, detail: &str) {
        self.service_fenced.store(true, Ordering::Release);
        self.service_available.store(false, Ordering::Release);
        tracing::error!(
            operation,
            detail,
            "fenced Master after authoritative state invariant failure"
        );
    }

    /// Enter one authoritative background-worker mutation epoch.
    ///
    /// The availability check is deliberately repeated after acquiring the
    /// read guard. A demotion may close the atomic gate while this worker is
    /// waiting behind the exclusive drain guard.
    pub(crate) fn begin_background_mutation(&self) -> Option<RwLockReadGuard<'_, ()>> {
        if !self.service_available.load(Ordering::Acquire)
            || self.service_fenced.load(Ordering::Acquire)
        {
            return None;
        }
        let guard = self.background_mutation_gate.read();
        if !self.service_available.load(Ordering::Acquire)
            || self.service_fenced.load(Ordering::Acquire)
        {
            return None;
        }
        Some(guard)
    }

    /// Durable variant used when acknowledging an externally visible replica
    /// generation change. `Ok(false)` means the object disappeared before its
    /// image could be captured.
    pub(crate) fn record_current_object_image_durable(
        &self,
        scoped_key: &str,
    ) -> Result<bool, crate::ha::HaError> {
        let Some(object) = self.objects.get(scoped_key) else {
            return Ok(false);
        };
        let object_image = object.clone();
        drop(object);
        self.oplog_manager
            .record_object_image_durable(scoped_key, &object_image)?;
        Ok(true)
    }

    pub(crate) fn record_object_image_or_remove_durable(
        &self,
        scoped_key: &str,
    ) -> Result<(), crate::ha::HaError> {
        if !self.record_current_object_image_durable(scoped_key)? {
            self.oplog_manager.record_remove_durable(scoped_key)?;
        }
        Ok(())
    }

    pub(crate) fn persist_object_image_or_remove_or_fence(
        &self,
        scoped_key: &str,
        operation: &str,
    ) -> Result<(), crate::ha::HaError> {
        if let Err(error) = self.record_object_image_or_remove_durable(scoped_key) {
            self.fence_after_durability_failure(operation, &error);
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn persist_replication_start_or_fence(
        &self,
        scoped_key: &str,
        operation: &str,
    ) -> Result<(), crate::ha::HaError> {
        let result = (|| {
            let object = self.objects.get(scoped_key).ok_or_else(|| {
                crate::ha::HaError::InvalidBackend(format!(
                    "{operation} lost its authoritative object image"
                ))
            })?;
            let object_image = object.clone();
            drop(object);
            let task = self.replication_tasks.get(scoped_key).ok_or_else(|| {
                crate::ha::HaError::InvalidBackend(format!(
                    "{operation} lost its authoritative replication task"
                ))
            })?;
            let task_image = task.clone();
            drop(task);
            self.oplog_manager.record_replication_start_durable(
                scoped_key,
                &object_image,
                &task_image,
            )?;
            Ok(())
        })();
        if let Err(error) = result {
            self.fence_after_durability_failure(operation, &error);
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn persist_task_state_batch_or_fence(
        &self,
        upsert_ids: &[Uuid],
        remove_ids: &[Uuid],
        operation: &str,
    ) -> Result<(), crate::ha::HaError> {
        let result = (|| {
            let mut seen = HashSet::with_capacity(upsert_ids.len() + remove_ids.len());
            let mut upserts = Vec::with_capacity(upsert_ids.len());
            for task_id in upsert_ids {
                if task_id.is_nil() || !seen.insert(*task_id) {
                    return Err(crate::ha::HaError::InvalidBackend(format!(
                        "{operation} contains an invalid or duplicate task id"
                    )));
                }
                let task = self.tasks.get(task_id).ok_or_else(|| {
                    crate::ha::HaError::InvalidBackend(format!(
                        "{operation} lost authoritative task {task_id}"
                    ))
                })?;
                upserts.push(task.clone());
            }
            for task_id in remove_ids {
                if task_id.is_nil() || !seen.insert(*task_id) {
                    return Err(crate::ha::HaError::InvalidBackend(format!(
                        "{operation} contains an invalid, duplicate, or overlapping task id"
                    )));
                }
            }
            self.oplog_manager
                .record_task_state_batch_durable(&upserts, remove_ids)?;
            Ok(())
        })();
        if let Err(error) = result {
            self.fence_after_durability_failure(operation, &error);
            return Err(error);
        }
        Ok(())
    }

    /// Move allocator-backed replicas out of the routable object image while
    /// retaining their ranges durably until the transfer grace period expires.
    ///
    /// The current object image (or its absence) and the new reservation share
    /// one oplog record, so standby replay cannot expose either half alone.
    pub(crate) fn schedule_delayed_replica_release_or_fence(
        &self,
        scoped_key: &str,
        authoritative_object: Option<ObjectEntry>,
        replicas: Vec<ReplicaDescriptor>,
        deadline: Option<SystemTime>,
        operation: &str,
    ) -> Result<Option<Uuid>, crate::ha::HaError> {
        let replicas = replicas
            .into_iter()
            .filter(|replica| {
                matches!(
                    replica.replica_type,
                    mooncake_store_core::ReplicaType::Memory
                        | mooncake_store_core::ReplicaType::NoFSsd
                )
            })
            .collect::<Vec<_>>();
        if replicas.is_empty() {
            return Ok(None);
        }
        let deadline = match deadline {
            Some(deadline) => deadline,
            None => SystemTime::now()
                .checked_add(self.runtime_config.put_start_release_timeout)
                .ok_or_else(|| {
                    crate::ha::HaError::InvalidBackend(format!(
                        "{operation} delayed release deadline overflow"
                    ))
                })?,
        };
        let deadline_epoch_ms = u64::try_from(
            deadline
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|error| {
                    crate::ha::HaError::InvalidBackend(format!(
                        "{operation} delayed release deadline precedes epoch: {error}"
                    ))
                })?
                .as_millis(),
        )
        .map_err(|_| {
            crate::ha::HaError::InvalidBackend(format!(
                "{operation} delayed release deadline exceeds u64"
            ))
        })?;
        let entry = DelayedReplicaReleaseEntry {
            id: Uuid::new_v4(),
            scoped_key: scoped_key.to_string(),
            deadline_epoch_ms,
            replicas,
        };
        self.delayed_replica_releases
            .insert(entry.id, entry.clone());
        let result = self
            .oplog_manager
            .record_object_delayed_release_batch_durable(
                scoped_key,
                authoritative_object.as_ref(),
                std::slice::from_ref(&entry),
                &[],
            );
        if let Err(error) = result {
            self.fence_after_durability_failure(operation, &error);
            return Err(error);
        }
        Ok(Some(entry.id))
    }

    /// Durably retire an expired delayed-release reservation before returning
    /// its ranges to the allocator.
    pub(crate) fn persist_delayed_replica_release_removal_or_fence(
        &self,
        entry: &DelayedReplicaReleaseEntry,
        operation: &str,
    ) -> Result<(), crate::ha::HaError> {
        let object = self
            .objects
            .get(&entry.scoped_key)
            .map(|object| object.clone());
        if let Err(error) = self
            .oplog_manager
            .record_object_delayed_release_batch_durable(
                &entry.scoped_key,
                object.as_ref(),
                &[],
                &[entry.id],
            )
        {
            self.fence_after_durability_failure(operation, &error);
            return Err(error);
        }
        Ok(())
    }

    /// Clear retry-only promotion state that is intentionally absent from snapshots/oplogs.
    pub(crate) fn clear_transient_promotion_candidates(&self) {
        self.promotion_candidates.clear();
        self.promotion_candidate_count.store(0, Ordering::Relaxed);
        self.promotion_retry_cursor.store(0, Ordering::Relaxed);
    }

    /// Create a completely empty MasterState (for standby bootstrap).
    pub(crate) fn empty() -> Self {
        use crate::allocator::SegmentAllocator;
        use crate::count_min_sketch::CountMinSketch;
        use parking_lot::RwLock;
        use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize};
        Self {
            clients: DashMap::new(),
            ok_clients: DashMap::new(),
            objects: DashMap::new(),
            key_mutations: KeyMutationCoordinator::default(),
            processing_keys: DashMap::new(),
            client_objects: DashMap::new(),
            segments: DashMap::new(),
            delayed_replica_releases: DashMap::new(),
            graceful_unmounts: DashMap::new(),
            nof_segments: DashMap::new(),
            local_disk_segments: DashMap::new(),
            local_disk_client_sessions: DashMap::new(),
            tasks: DashMap::new(),
            replication_tasks: DashMap::new(),
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_sketch: RwLock::new(CountMinSketch::new()),
            promotion_candidates: DashMap::new(),
            drain_jobs: DashMap::new(),
            allocator: RwLock::new(SegmentAllocator::new()),
            nof_allocator: RwLock::new(SegmentAllocator::new()),
            nof_eviction_requested: AtomicBool::new(false),
            storage_backend: RwLock::new(None),
            oplog_manager: Arc::new(crate::oplog::OpLogManager::new(None, 0)),
            promotion_in_flight: AtomicUsize::new(0),
            promotion_candidate_count: AtomicUsize::new(0),
            promotion_retry_cursor: AtomicUsize::new(0),
            view_version: AtomicI64::new(0),
            leadership_view_version: AtomicU64::new(0),
            runtime_config: MasterRuntimeConfig::default(),
            service_available: AtomicBool::new(true),
            foreground_request_gate: Arc::new(ForegroundRequestGate::new()),
            background_mutation_gate: RwLock::new(()),
            service_fenced: AtomicBool::new(false),
            tenant_quotas: RwLock::new(TenantQuotaTable::new(0)),
            pending_remote_pulls: DashMap::new(),
            nof_heartbeat_states: DashMap::new(),
            kv_event_publisher: Arc::new(KvEventPublisher::new(Default::default())),
        }
    }
}

/// Persisted scheduling state for a delayed memory-segment unmount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GracefulUnmountSnapshotEntry {
    pub segment_id: Uuid,
    pub client_id: Uuid,
    pub deadline_epoch_ms: u64,
}

/// Durable allocator reservation for replicas that are no longer routable
/// through object metadata but may still be referenced by in-flight transfers.
///
/// The absolute epoch deadline preserves elapsed grace time across snapshot
/// restore and standby promotion. Only Memory and NoF descriptors are stored;
/// metadata-only replica kinds do not own allocator ranges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelayedReplicaReleaseEntry {
    pub id: Uuid,
    pub scoped_key: String,
    pub deadline_epoch_ms: u64,
    pub replicas: Vec<ReplicaDescriptor>,
}

impl DelayedReplicaReleaseEntry {
    pub(crate) fn same_durable_reservation(&self, other: &Self) -> bool {
        self.id == other.id
            && self.scoped_key == other.scoped_key
            && self.deadline_epoch_ms == other.deadline_epoch_ms
            && self.replicas.len() == other.replicas.len()
            && self
                .replicas
                .iter()
                .zip(&other.replicas)
                .all(|(left, right)| {
                    left.segment_id == right.segment_id
                        && left.offset == right.offset
                        && left.size == right.size
                        && left.status == right.status
                        && left.replica_type == right.replica_type
                })
    }
}

/// Tracks the start time of a remote fetch for a key.
/// 记录某个 key 的回源拉取开始时间。
#[derive(Debug, Clone)]
pub(crate) struct RemotePullEntry {
    /// 拉取开始时间，用于 TTL 过期判断 / Pull start time for TTL expiry.
    pub(crate) started_at: Instant,
    /// Only the client that acquired this entry may complete or release it.
    pub(crate) owner_client_id: Uuid,
}

/// 客户端条目：包含客户端信息和最后心跳时间戳。
/// Client entry: contains client info and last ping timestamp.
pub(crate) struct ClientEntry {
    pub(crate) info: mooncake_store_core::ClientInfo,
    /// 最后心跳时间 / Last ping time, used for liveness detection.
    pub(crate) last_ping: SystemTime,
}

/// ObjectEntry: 对象的完整元数据。
/// put_start_time/lease_timeout/soft_pin_timeout 在通用 serde 表示中跳过；
/// native master snapshot 通过版本化 DTO 显式持久化这些运行时截止时间。
///
/// ObjectEntry: complete metadata for an object.
/// `put_start_time`/`lease_timeout`/`soft_pin_timeout` are marked `serde(skip)`
/// in the general-purpose representation. The native master snapshot persists
/// them explicitly in its versioned DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    /// 副本列表 / Replica list: describes each copy's location, status, type.
    pub replicas: Vec<ReplicaDescriptor>,
    /// 对象大小（字节）/ Object size in bytes.
    pub size: u64,
    /// 最后访问时间 / Last access time (for LRU eviction ranking).
    pub last_access: SystemTime,
    /// 硬固定状态：禁止驱逐 / Hard-pinned: never evicted.
    #[serde(default)]
    pub hard_pinned: bool,
    /// 数据类型：KV cache / Tensor / Weight 等 / Data type categorization.
    #[serde(default)]
    pub data_type: ObjectDataType,
    /// 对象所有者客户端 ID / Owning client ID.
    #[serde(default)]
    pub client_id: Uuid,
    /// PutStart 时间戳（通用 serde 不序列化）/ PutStart timestamp (skipped by general serde).
    #[serde(skip)]
    pub put_start_time: Option<SystemTime>,
    /// 租约超时时间（运行时）/ Lease timeout (runtime).
    #[serde(skip)]
    pub lease_timeout: Option<SystemTime>,
    /// 软锁定超时时间（运行时）/ Soft-pin timeout (runtime).
    #[serde(skip)]
    pub soft_pin_timeout: Option<SystemTime>,
    /// 租户标识符（规范化后，未设置时默认为 "default"）。
    /// Tenant identifier (normalized; defaults to "default" when not set).
    /// C++ equivalent: ObjectMetadata::tenant_id
    #[serde(default)]
    pub tenant_id: TenantId,
    /// Optional group id for grouped lease/routing semantics.
    /// 分组租约/路由语义使用的可选 group id。
    #[serde(default)]
    pub group_id: String,
    /// Whether this object has already moved from reserved quota to used quota.
    #[serde(default)]
    pub quota_committed: bool,
    /// Memory-replica bytes currently reserved for this object's in-flight mutation.
    #[serde(default)]
    pub reserved_quota_charge_bytes: u64,
    /// Memory-replica bytes currently charged to this object.
    #[serde(default)]
    pub committed_quota_charge_bytes: u64,
    /// Charge retained for the old buffers of a size-changing Upsert until the
    /// replacement commits or is revoked.
    /// C++ equivalent: ObjectMetadata::pending_replaced_quota_charge_bytes.
    #[serde(default)]
    pub pending_replaced_quota_charge_bytes: u64,
    /// Whether this object is currently counted in the memory-cache inventory gauge.
    #[serde(skip)]
    pub memory_cache_total_accounted: bool,
    /// Whether this object is currently counted in the file-cache inventory gauge.
    #[serde(skip)]
    pub disk_cache_total_accounted: bool,
    /// 用户提供的原始 key（不包含租户作用域前缀）。
    /// Original user-provided key (without tenant scope prefix).
    /// C++ equivalent: ObjectMetadata::user_key
    #[serde(default)]
    pub user_key: String,
}

impl ObjectEntry {
    pub(crate) fn user_key_for_event<'a>(&'a self, scoped_key: &'a str) -> &'a str {
        if self.user_key.is_empty() {
            scoped_key
        } else {
            &self.user_key
        }
    }

    /// 授予对象租约，刷新 lease 和 soft-pin 超时。
    /// C++ equivalent: ObjectMetadata::GrantLease(key_ttl, soft_ttl)
    ///
    /// Grant a lease on this object. Extends `lease_timeout` to
    /// `max(lease_timeout, now + key_ttl)`. If `soft_pin_timeout` is set,
    /// also extends it to `max(soft_pin_timeout, now + soft_ttl)`.
    pub fn grant_lease(&mut self, key_ttl: Duration, soft_ttl: Duration) {
        let now = SystemTime::now();
        let next_lease = now + key_ttl;
        self.lease_timeout = Some(match self.lease_timeout {
            Some(t) if t > next_lease => t,
            _ => next_lease,
        });
        if let Some(ref mut soft) = self.soft_pin_timeout {
            let next_soft = now + soft_ttl;
            if next_soft > *soft {
                *soft = next_soft;
            }
        }
    }

    /// 检查软锁定是否有效（超时未过期）。
    /// C++ equivalent: ObjectMetadata::IsSoftPinned()
    ///
    /// Returns true if `soft_pin_timeout` is set and the current time
    /// has not yet exceeded it.
    pub fn is_soft_pinned(&self) -> bool {
        self.soft_pin_timeout
            .map_or(false, |t| SystemTime::now() < t)
    }

    /// 检查软锁定是否有效（相对于给定时间点）。
    /// C++ equivalent: ObjectMetadata::IsSoftPinned(now)
    pub fn is_soft_pinned_at(&self, now: SystemTime) -> bool {
        self.soft_pin_timeout.map_or(false, |t| now < t)
    }
}

/// Segment 条目：Memory segment 的注册信息。
/// Segment entry: registration info for a Memory segment.
#[derive(Debug, Clone)]
pub struct SegmentEntry {
    /// Segment 的静态属性（ID、名称、基地址、大小、端点）
    pub segment: mooncake_store_core::Segment,
    /// 已使用的字节数 / Bytes currently used.
    pub used: u64,
    /// 所属客户端 ID / Owning client ID.
    pub client_id: Uuid,
    /// Segment 状态 / Segment status: Active, Draining, etc.
    pub status: crate::proto::SegmentStatus,
}

/// NoF (NVMe-oF) Segment 条目：远程 SSD segment 的注册信息。
/// NoF segment entry: registration info for a remote NVMe-oF SSD segment.
#[derive(Debug, Clone)]
pub struct NoFSegmentEntry {
    /// NoF segment 属性（含传输端点）/ NoF segment properties (includes transport endpoint).
    pub segment: NoFSegment,
    /// 已使用字节数 / Bytes used.
    pub used: u64,
    /// Segment 状态 / Segment status.
    pub status: crate::proto::SegmentStatus,
}

/// NoF segment 心跳追踪状态 / NoF segment heartbeat tracking state.
/// C++ equivalent: `NoFHeartbeatState` in master_service.h.
#[derive(Debug, Clone)]
pub(crate) struct NoFHeartbeatState {
    pub(crate) segment_id: Uuid,
    pub(crate) segment_name: String,
    pub(crate) te_endpoint: String,
    /// 下次探测时间 / Next probe time.
    pub(crate) next_probe_at: Instant,
    /// 最后一次成功探测的时间 / Last successful probe time.
    pub(crate) last_success_at: Instant,
    /// 连续失败次数 / Consecutive probe failures.
    pub(crate) consecutive_failures: u32,
}

/// 本地磁盘 segment 条目：管理每个客户端的 SSD 存储和 offload/promotion 队列。
/// Local disk segment entry: manages per-client SSD storage and offload/promotion queues.
pub(crate) struct LocalDiskSegmentEntry {
    /// Currently bound process session. Restored snapshots start offline and
    /// cannot route reads until a client completes inventory recovery.
    pub(crate) active_client_id: Option<Uuid>,
    /// Whether the current session has committed a complete disk inventory.
    pub(crate) recovery_complete: bool,
    /// Token of the currently authorized inventory transaction. The token is
    /// retained after Commit so that a retried Commit is idempotent.
    pub(crate) recovery_session_id: Option<Uuid>,
    /// Master-known keys reported during the current recovery epoch.
    pub(crate) recovered_objects: HashSet<String>,
    /// 是否启用 offload（下沉）/ Whether offloading is enabled.
    pub(crate) enable_offloading: bool,
    /// 待 offload 的对象队列 / Queue of objects pending offload: key → size.
    pub(crate) offloading_objects: HashMap<String, i64>,
    /// 待 promotion 的对象队列 / Queue of objects pending promotion: key → size.
    pub(crate) promotion_objects: HashMap<String, i64>,
    /// 本地 SSD 总容量（字节）/ Total local SSD capacity in bytes.
    pub(crate) ssd_total_capacity_bytes: i64,
}

/// 任务条目：Copy/Move 任务的完整信息。
/// Task entry: complete info for a Copy/Move task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEntry {
    /// 任务基本信息 / Task metadata: ID, type, status, assigned_client, timestamps.
    pub info: TaskInfo,
    /// 关联的 key / Associated object key.
    pub key: String,
    /// 序列化后的任务 payload（JSON）/ Serialized task payload (JSON).
    pub payload: String,
    /// 最大重试次数 / Maximum retry attempts.
    pub max_retry_attempts: u32,
}

/// ReplicationTaskKind: 区分 Copy（保留源副本）和 Move（完成后删除源副本）两种复制语义。
/// Distinguishes between Copy (keep source replica) and Move (delete source replica after completion).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationTaskKind {
    Copy,
    Move,
}

/// ReplicationTaskEntry: 记录进行中的复制任务，包含源和目标副本描述符，用于 CopyEnd/MoveEnd 验证。
/// Records an in-flight replication task with source and target descriptors, used for CopyEnd/MoveEnd validation.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationTaskEntry {
    /// 发起任务的客户端 / Client that initiated the replication.
    pub(crate) client_id: Uuid,
    /// 任务开始时间 / Task start time (for TTL expiry).
    pub(crate) start_time: Instant,
    /// 复制类型 / Replication kind: Copy or Move.
    pub(crate) kind: ReplicationTaskKind,
    /// 源副本描述符 / Source replica descriptor.
    pub(crate) source: ReplicaDescriptor,
    /// 目标副本描述符列表 / Target replica descriptor list.
    pub(crate) targets: Vec<ReplicaDescriptor>,
    /// Exact pre-existing target reused by MoveStart. Newly allocated targets
    /// remain in `targets` because they own reservation/rollback state.
    pub(crate) existing_move_target: Option<ReplicaDescriptor>,
    /// Memory bytes reserved for newly allocated replication targets.
    pub(crate) reserved_quota_charge_bytes: u64,
}

/// Serializable representation of an in-flight native Copy/Move operation.
///
/// `Instant` cannot cross a process boundary, so snapshots store the task age
/// and reconstruct a monotonic start time relative to the restore instant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationTaskSnapshotEntry {
    pub key: String,
    pub client_id: Uuid,
    pub start_age_millis: u64,
    pub kind: ReplicationTaskKind,
    pub source: ReplicaDescriptor,
    pub targets: Vec<ReplicaDescriptor>,
    #[serde(default)]
    pub existing_move_target: Option<ReplicaDescriptor>,
    pub reserved_quota_charge_bytes: u64,
}

impl ReplicationTaskSnapshotEntry {
    pub(crate) fn capture(
        key: &str,
        task: &ReplicationTaskEntry,
        snapshot_instant: Instant,
    ) -> Self {
        Self {
            key: key.to_string(),
            client_id: task.client_id,
            start_age_millis: snapshot_instant
                .saturating_duration_since(task.start_time)
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            kind: task.kind,
            source: task.source.clone(),
            targets: task.targets.clone(),
            existing_move_target: task.existing_move_target.clone(),
            reserved_quota_charge_bytes: task.reserved_quota_charge_bytes,
        }
    }

    pub(crate) fn into_runtime(self, restore_instant: Instant) -> (String, ReplicationTaskEntry) {
        let age = Duration::from_millis(self.start_age_millis);
        let start_time = restore_instant.checked_sub(age).unwrap_or(restore_instant);
        (
            self.key,
            ReplicationTaskEntry {
                client_id: self.client_id,
                start_time,
                kind: self.kind,
                source: self.source,
                targets: self.targets,
                existing_move_target: self.existing_move_target,
                reserved_quota_charge_bytes: self.reserved_quota_charge_bytes,
            },
        )
    }
}

/// Offloading 任务条目：追踪一次内存→本地磁盘下沉任务。
/// Offloading task entry: tracks a single memory→local-disk offload operation.
#[derive(Debug, Clone)]
pub(crate) struct OffloadingTaskEntry {
    /// 下沉目标客户端 / Client where the offload is happening.
    pub(crate) client_id: Uuid,
    /// Durable LocalDisk namespace targeted by this task.
    pub(crate) storage_id: Uuid,
    /// Master-issued identity that must be persisted with the offloaded bytes.
    pub(crate) generation_id: Uuid,
    /// 被 offload 固定的源 Memory 副本 / Source memory replica pinned during offload.
    pub(crate) source: ReplicaDescriptor,
    /// 任务开始时间 / Task start time (for TTL expiry).
    pub(crate) start_time: Instant,
}

/// Promotion 任务条目：追踪一次本地磁盘→内存提升任务。
/// Promotion task entry: tracks a single local-disk→memory promotion operation.
#[derive(Debug, Clone)]
pub(crate) struct PromotionTaskEntry {
    /// 持有磁盘副本的客户端 / Client holding the disk replica.
    pub(crate) holder_id: Uuid,
    /// Durable LocalDisk namespace containing the source replica.
    pub(crate) storage_id: Uuid,
    /// 对象大小 / Object size in bytes.
    pub(crate) object_size: u64,
    /// 被 promotion 固定的源 LocalDisk 副本 / Source LocalDisk replica pinned during promotion.
    pub(crate) source: ReplicaDescriptor,
    /// 暂存的 segment ID（分配后）/ Staged segment ID (after allocation).
    pub(crate) staged_segment_id: Option<Uuid>,
    /// 暂存的偏移量（分配后）/ Staged offset (after allocation).
    pub(crate) staged_offset: Option<u64>,
    /// Memory bytes reserved for the staged promotion replica.
    pub(crate) reserved_quota_charge_bytes: u64,
    /// 任务开始时间 / Task start time.
    pub(crate) start_time: Instant,
}

#[cfg(test)]
mod tests {
    use super::{KeyMutationCoordinator, MasterState};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn foreground_request_gate_rejects_new_work_and_drains_inflight_work() {
        let state = Arc::new(MasterState::empty());
        let request_guard = state.begin_foreground_request().unwrap();
        state.service_available.store(false, Ordering::Release);
        assert!(state.begin_foreground_request().is_none());

        let drain_state = Arc::clone(&state);
        let (tx, rx) = std::sync::mpsc::channel();
        let drain_thread = std::thread::spawn(move || {
            drain_state.drain_foreground_requests();
            tx.send(()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(request_guard);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drain_thread.join().unwrap();

        state.service_available.store(true, Ordering::Release);
        state.service_fenced.store(true, Ordering::Release);
        assert!(state.begin_foreground_request().is_none());
        assert!(state.begin_background_mutation().is_none());
    }

    #[test]
    fn snapshot_barrier_waits_for_inflight_key_mutation() {
        let coordinator = Arc::new(KeyMutationCoordinator::default());
        let mutation_guard = coordinator.lock("tenant\0key");
        let snapshot_coordinator = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let snapshot_thread = std::thread::spawn(move || {
            let _snapshot_guard = snapshot_coordinator.lock_snapshot();
            tx.send(()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(mutation_guard);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        snapshot_thread.join().unwrap();
    }

    #[test]
    fn operation_stripe_serializes_the_same_tenant_scoped_key() {
        let coordinator = Arc::new(KeyMutationCoordinator::default());
        let first = coordinator.lock_operation("tenant-a\0key");
        let second_coordinator = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let second_thread = std::thread::spawn(move || {
            let _second = second_coordinator.lock_operation("tenant-a\0key");
            tx.send(()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(first);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        second_thread.join().unwrap();
    }

    #[test]
    fn operation_stripe_does_not_block_quota_eviction_snapshot_epoch() {
        let coordinator = Arc::new(KeyMutationCoordinator::default());
        let _operation = coordinator.lock_operation("tenant-a\0key");
        let snapshot_coordinator = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let snapshot_thread = std::thread::spawn(move || {
            let _snapshot = snapshot_coordinator.lock_snapshot();
            tx.send(()).unwrap();
        });

        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        snapshot_thread.join().unwrap();
    }

    #[test]
    fn lock_many_deduplicates_keys_and_serializes_each_member() {
        let coordinator = Arc::new(KeyMutationCoordinator::default());
        let batch =
            coordinator.lock_many(["tenant-a\0first", "tenant-a\0first", "tenant-a\0second"]);
        let member_coordinator = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let member_thread = std::thread::spawn(move || {
            let _member = member_coordinator.lock("tenant-a\0second");
            tx.send(()).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(25)).is_err(),
            "a key mutation bypassed the batch mutation epoch"
        );
        drop(batch);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        member_thread.join().unwrap();
    }
}
