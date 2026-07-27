// =============================================================================
// Operation Log (OpLog) — 操作日志存储
// =============================================================================
// Provides operation log persistence for HA consistency and standby recovery.
// 为 HA 一致性和备用恢复提供操作日志持久化。
//
// This module implements a replay log for master mutations. The leader appends
// each state-changing operation (put, remove, mount, unmount) as a JSON record
// with a monotonically increasing sequence number. Standby nodes poll and replay
// these records to stay synchronized with the leader.
// 本模块实现 master 变更的重放日志。Leader 将每个状态变更操作（put、remove、
// mount、unmount）以 JSON 记录形式追加，每条记录带有单调递增的序列号。
// Standby 节点轮询并重放这些记录以保持与 leader 同步。
//
// Architecture / 架构:
// ┌──────────────────────────────────────────────────────────┐
// │  OpLogManager (high-level)                                │
// │  record_put_end / record_remove / record_mount / ...     │
// │  ┌──────────────────────┐                                │
// │  │ OpLogStore (trait)    │ ← abstract backend            │
// │  │ append / read_since / │                                │
// │  │ latest_sequence /     │                                │
// │  │ poll_from             │                                │
// │  └──────┬───────────────┘                                │
// │         │                                                 │
// │  ┌──────┼───────────────┬───────────────┐                │
// │  │ InMemoryOpLog │ LocalFsOpLogStore│ EtcdOpLogStore     │
// │  │ (VecDeque)    │ (segment files)  │ (etcd key-value)   │
// │  └──────────────┴───────────────────┴───────────────────┘ │
// └──────────────────────────────────────────────────────────┘
//
// Backend implementations / 后端实现:
// - InMemoryOpLog: VecDeque-based FIFO buffer (lightweight, no persistence).
//   InMemoryOpLog: 基于 VecDeque 的 FIFO 缓冲区（轻量，无持久化）。
// - LocalFsOpLogStore: segment files on local disk with atomic write.
//   LocalFsOpLogStore: 本地磁盘的分段文件，带原子写入保证。
// - EtcdOpLogStore: etcd key-value store, suitable for distributed deployment.
//   EtcdOpLogStore: etcd 键值存储，适合分布式部署。

use crate::TenantId;
use crate::ha::{HaError, OpLogPollResult, OpLogRecord};
use crate::metrics;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use mooncake_store_core::{ObjectDataType, ReplicaDescriptor, ReplicaType};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;
use uuid::Uuid;
use xxhash_rust::xxh32::xxh32;

const CPP_OP_PUT_END: u8 = 1;
const CPP_OP_PUT_REVOKE: u8 = 2;
const CPP_OP_REMOVE: u8 = 3;
const MAX_OBJECT_KEY_SIZE: usize = 4096;
const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
/// Stable prefix followed by the decimal payload schema version.
const PUT_END_MSGPACK_MAGIC_PREFIX: &[u8] = b"MCOPMETA";
/// Version 2 persists the exact LocalDisk storage and byte-generation identity
/// carried by ReplicaDescriptor. Version 1 remains readable for upgrade, but
/// any LocalDisk replica without a generation is restored offline.
const PUT_END_MSGPACK_MAGIC_V1: &[u8] = b"MCOPMETA1";
const PUT_END_MSGPACK_MAGIC_V2: &[u8] = b"MCOPMETA2";
const PUT_END_MSGPACK_MAGIC: &[u8] = b"MCOPMETA3";
const OPLOG_MSGPACK_RECORD_PREFIX: &str = "msgpack:";
const ETCD_WATCH_SYNC_BATCH_SIZE: usize = 1000;
const ETCD_WATCH_MAX_CONSECUTIVE_ERRORS: usize = 10;
const ETCD_WATCH_RECONNECT_DELAY_MS: u64 = 1000;
const ETCD_WATCH_MAX_RECONNECT_DELAY_MS: u64 = 30_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CppOpLogWireEntry {
    sequence_id: u64,
    timestamp_ms: u64,
    op_type: u8,
    object_key: String,
    payload: String,
    checksum: u32,
    prefix_hash: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PutEndMetadataPayloadV2 {
    pub(crate) op: String,
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) client_id: Option<String>,
    pub(crate) tenant_id: String,
    pub(crate) group_id: String,
    pub(crate) user_key: String,
    pub(crate) replicas: Vec<ReplicaDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableObjectImagePayloadV3 {
    pub(crate) size: u64,
    pub(crate) client_id: String,
    pub(crate) group_id: String,
    pub(crate) replicas: Vec<ReplicaDescriptor>,
    pub(crate) hard_pinned: bool,
    pub(crate) data_type: ObjectDataType,
    #[serde(default)]
    pub(crate) last_access_ms: Option<i64>,
    pub(crate) put_start_time_ms: Option<i64>,
    pub(crate) lease_timeout_ms: Option<i64>,
    pub(crate) soft_pin_timeout_ms: Option<i64>,
    pub(crate) quota_committed: bool,
    pub(crate) reserved_quota_charge_bytes: u64,
    pub(crate) committed_quota_charge_bytes: u64,
    pub(crate) pending_replaced_quota_charge_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PutEndMetadataPayloadV3 {
    pub(crate) op: String,
    pub(crate) schema_version: u32,
    pub(crate) key: String,
    pub(crate) tenant_id: String,
    pub(crate) user_key: String,
    pub(crate) object: DurableObjectImagePayloadV3,
}

fn system_time_to_epoch_millis(value: SystemTime) -> i64 {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

impl PutEndMetadataPayloadV3 {
    fn try_new(key: &str, object: &crate::service::ObjectEntry) -> Result<Self, HaError> {
        if key.len() > MAX_OBJECT_KEY_SIZE {
            return Err(HaError::InvalidBackend(
                "put_end v3 object key exceeds maximum length".into(),
            ));
        }
        let identity =
            recover_object_identity(key, object.tenant_id.as_str(), object.user_key.as_str())?;
        if identity.scoped_key != key {
            return Err(HaError::InvalidBackend(
                "put_end v3 object key is not canonical".into(),
            ));
        }
        if object.size == 0 || object.replicas.is_empty() {
            return Err(HaError::InvalidBackend(
                "put_end v3 object image requires non-zero size and replicas".into(),
            ));
        }
        let mut locations = HashSet::with_capacity(object.replicas.len());
        for replica in &object.replicas {
            if replica.replica_type == ReplicaType::All
                || replica.size != object.size
                || replica.offset.checked_add(replica.size).is_none()
                || (matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                ) && replica.segment_id.is_nil())
                || !locations.insert((
                    replica.segment_id,
                    replica.offset,
                    replica.size,
                    replica.replica_type,
                    replica.local_disk_storage_id,
                    replica.local_disk_generation_id,
                ))
            {
                return Err(HaError::InvalidBackend(
                    "put_end v3 object image has invalid replica geometry".into(),
                ));
            }
        }
        let completed_charge =
            crate::service::helpers::checked_durable_committed_memory_quota_charge(object)
                .map_err(|_| {
                    HaError::InvalidBackend(
                        "put_end v3 committed Memory quota charge overflows uint64".into(),
                    )
                })?;
        let allocating_charge = crate::service::helpers::checked_allocating_memory_quota_charge(
            object,
        )
        .map_err(|_| {
            HaError::InvalidBackend(
                "put_end v3 allocating Memory quota charge overflows uint64".into(),
            )
        })?;
        if (object.quota_committed
            && (object.reserved_quota_charge_bytes != 0
                || object.pending_replaced_quota_charge_bytes != 0
                || object.committed_quota_charge_bytes != completed_charge))
            || (!object.quota_committed
                && (object.committed_quota_charge_bytes != 0
                    || object.reserved_quota_charge_bytes != allocating_charge))
        {
            return Err(HaError::InvalidBackend(
                "put_end v3 object image has inconsistent quota charges".into(),
            ));
        }
        Ok(Self {
            op: "put_end".to_string(),
            schema_version: 3,
            key: key.to_string(),
            tenant_id: object.tenant_id.as_str().to_string(),
            user_key: object.user_key.clone(),
            object: DurableObjectImagePayloadV3 {
                size: object.size,
                client_id: object.client_id.to_string(),
                group_id: object.group_id.clone(),
                replicas: object.replicas.clone(),
                hard_pinned: object.hard_pinned,
                data_type: object.data_type,
                last_access_ms: Some(system_time_to_epoch_millis(object.last_access)),
                put_start_time_ms: object.put_start_time.map(system_time_to_epoch_millis),
                lease_timeout_ms: object.lease_timeout.map(system_time_to_epoch_millis),
                soft_pin_timeout_ms: object.soft_pin_timeout.map(system_time_to_epoch_millis),
                quota_committed: object.quota_committed,
                reserved_quota_charge_bytes: object.reserved_quota_charge_bytes,
                committed_quota_charge_bytes: object.committed_quota_charge_bytes,
                pending_replaced_quota_charge_bytes: object.pending_replaced_quota_charge_bytes,
            },
        })
    }
}

pub(crate) struct RecoveredObjectIdentity {
    pub(crate) tenant_id: TenantId,
    pub(crate) user_key: String,
    pub(crate) scoped_key: String,
}

pub(crate) fn recover_object_identity(
    durable_key: &str,
    metadata_tenant: &str,
    metadata_user_key: &str,
) -> Result<RecoveredObjectIdentity, HaError> {
    let durable_identity = if durable_key.contains('\0') {
        Some(TenantId::parse_scoped_key(durable_key).map_err(|error| {
            HaError::InvalidBackend(format!(
                "oplog put_end has invalid scoped tenant id: {error}"
            ))
        })?)
    } else {
        None
    };
    let tenant_id = if metadata_tenant.is_empty() {
        durable_identity
            .as_ref()
            .map(|(tenant_id, _)| tenant_id.clone())
            .unwrap_or_default()
    } else {
        TenantId::new(metadata_tenant.to_string()).map_err(|error| {
            HaError::InvalidBackend(format!("oplog put_end has invalid tenant id: {error}"))
        })?
    };
    let user_key = if let Some((durable_tenant, durable_user_key)) = durable_identity {
        if durable_tenant != tenant_id {
            return Err(HaError::InvalidBackend(format!(
                "oplog put_end tenant mismatch: scoped={durable_tenant}, metadata={tenant_id}"
            )));
        }
        if !metadata_user_key.is_empty() && metadata_user_key != durable_user_key {
            return Err(HaError::InvalidBackend(
                "oplog put_end user key mismatch between scoped key and metadata".to_string(),
            ));
        }
        durable_user_key
    } else if metadata_user_key.is_empty() {
        durable_key.to_string()
    } else {
        metadata_user_key.to_string()
    };
    let scoped_key = tenant_id.make_scoped_key(&user_key);
    Ok(RecoveredObjectIdentity {
        tenant_id,
        user_key,
        scoped_key,
    })
}

pub(crate) fn recover_object_identity_from_payload(
    payload: &serde_json::Value,
) -> Result<Option<RecoveredObjectIdentity>, HaError> {
    let Some(durable_key) = payload.get("key").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    if payload.get("tenant_id").is_none()
        && payload.get("user_key").is_none()
        && payload.get("replicas").is_none()
    {
        return Ok(None);
    }
    let string_field = |name| match payload.get(name) {
        None => Ok(""),
        Some(serde_json::Value::String(value)) => Ok(value.as_str()),
        Some(_) => Err(HaError::InvalidBackend(format!(
            "oplog put_end {name} must be a string"
        ))),
    };
    recover_object_identity(
        durable_key,
        string_field("tenant_id")?,
        string_field("user_key")?,
    )
    .map(Some)
}

// =============================================================================
// OpLogStore Trait — unified abstraction for oplog backends
// =============================================================================

/// Operation log storage abstraction. Different backends (memory, local files,
/// etcd) provide append, read, and poll capabilities through this trait.
/// 操作日志存储的统一抽象接口。
/// 不同后端（内存、本地文件、etcd）均通过此 trait 提供追加、读取、轮询能力。
/// 用于 HA 场景中 standby 节点从 leader 的 oplog 中同步状态。
pub trait OpLogStore: Send + Sync {
    /// Append a record and return its assigned sequence number.
    /// 追加一条记录，返回分配的序列号。
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError>;

    /// Read up to max_count records starting from since_seq (inclusive).
    /// 从 since_seq（含）开始读取最多 max_count 条记录。
    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError>;

    /// Get the latest committed sequence number.
    /// 获取最新已提交的序列号。
    fn latest_sequence(&self) -> u64;

    /// Get the maximum sequence number currently present in the backend.
    fn max_sequence_id(&self) -> Result<u64, HaError>;

    /// Update the latest sequence pointer without appending a new entry.
    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError>;

    /// Persist the sequence id associated with a published snapshot.
    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError>;

    /// Read the sequence id associated with a published snapshot.
    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError>;

    /// Remove oplog entries strictly before before_sequence_id.
    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError>;

    /// Flush pending entries that were appended but not durably persisted yet.
    fn flush_durable(&mut self) -> Result<(), HaError>;

    /// Poll for records from since_seq, returning records, next_seq, and timeout flag.
    /// 从 since_seq 开始轮询记录，返回记录列表、next_seq 和是否超时。
    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult;

    /// Create a change notifier for backends that support push-based updates.
    /// 创建后端支持的变更通知器；不支持时返回 None 并由调用方回退轮询。
    fn create_change_notifier(&self) -> Option<Box<dyn OpLogChangeNotifier>> {
        None
    }
}

pub type OpLogEntryCallback = Box<dyn FnMut(OpLogRecord) + Send + 'static>;
pub type OpLogErrorCallback = Box<dyn FnMut(HaError) + Send + 'static>;

pub trait OpLogChangeNotifier: Send {
    fn start(
        &mut self,
        start_seq_id: u64,
        on_entry: OpLogEntryCallback,
        on_error: OpLogErrorCallback,
    ) -> Result<(), HaError>;

    fn stop(&mut self);

    fn is_healthy(&self) -> bool;
}

// =============================================================================
// InMemoryOpLog — FIFO in-memory oplog (no persistence)
// =============================================================================

/// In-memory oplog backed by a bounded VecDeque.
/// 基于 VecDeque 的内存 oplog，容量有上限（FIFO 淘汰旧条目）。
/// 用于没有配置持久化后端时的轻量级替代方案。
///
/// When the buffer exceeds max_entries, the oldest entries are evicted (FIFO).
/// Suitable for testing and single-node deployments without persistence.
/// 当 buffer 超过 max_entries 时，最旧的条目被淘汰（FIFO）。
/// 适合测试和无持久化需求的单节点部署。
pub struct InMemoryOpLog {
    /// Ring buffer of records (FIFO eviction when full).
    /// 记录的环形缓冲区（满时 FIFO 淘汰）。
    buffer: VecDeque<OpLogRecord>,
    /// Monotonically increasing sequence counter.
    /// 单调递增的序列计数器。
    last_seq: u64,
    /// Maximum number of entries to retain.
    /// 最大保留条目数。
    max_entries: usize,
    snapshot_sequences: HashMap<String, u64>,
}

impl InMemoryOpLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            last_seq: 0,
            max_entries: max_entries.max(1),
            snapshot_sequences: HashMap::new(),
        }
    }
}

impl InMemoryOpLog {
    /// Convenience: append a payload with `producer_view_version` and return the assigned seq.
    pub fn append_payload(
        &mut self,
        producer_view_version: u64,
        payload: impl Into<String>,
    ) -> u64 {
        self.append(&OpLogRecord {
            seq: 0,
            producer_view_version,
            payload: payload.into(),
        })
        .unwrap_or_default()
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

impl OpLogStore for InMemoryOpLog {
    /// Append entry: increments sequence counter, evicts oldest if at capacity.
    /// 追加条目：自增序列号，FIFO 淘汰超出容量的旧数据。
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq = self.last_seq.checked_add(1).ok_or_else(|| {
            HaError::InvalidBackend("oplog sequence exhausted at u64::MAX".into())
        })?;
        if self.buffer.len() >= self.max_entries {
            self.buffer.pop_front();
        }
        self.buffer.push_back(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        Ok(self.last_seq)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        Ok(self
            .buffer
            .iter()
            .filter(|r| r.seq >= since_seq)
            .take(max_count)
            .cloned()
            .collect())
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        Ok(self.last_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        if sequence_id < self.last_seq || !self.buffer.is_empty() {
            return Err(HaError::InvalidBackend(
                "oplog latest sequence cannot move backwards or bypass buffered entries".into(),
            ));
        }
        self.last_seq = sequence_id;
        Ok(())
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        self.snapshot_sequences
            .insert(snapshot_id.to_string(), sequence_id);
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        validate_snapshot_id(snapshot_id)?;
        self.snapshot_sequences
            .get(snapshot_id)
            .copied()
            .ok_or_else(|| HaError::InvalidBackend(format!("snapshot not found: {snapshot_id}")))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.buffer.retain(|entry| entry.seq >= before_sequence_id);
        Ok(())
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        Ok(())
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records
            .last()
            .map(|r| r.seq.saturating_add(1))
            .unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}

// =============================================================================
// LocalFsOpLogStore — segment-file-based persistent oplog
// =============================================================================

/// Segment-based persistent oplog on the local filesystem.
/// 基于本地文件系统的分段持久化 oplog。
///
/// Each segment file is named `oplog_<start_seq:020>.bin`. Entries use a
/// length-prefixed binary frame format:
/// 每个分段文件命名为 `oplog_<start_seq:020>.bin`。条目采用长度前缀帧格式：
/// ```text
/// [4 bytes seq LE][4 bytes payload_len LE][payload bytes]
/// ```
///
/// Writes are made atomic via "write .tmp → rename → fsync directory".
/// 通过「写入 .tmp 文件 → rename → fsync 目录」实现原子写和持久性保证。
///
/// A background flush thread asynchronously persists full segment buffers,
/// preventing the append hot-path from being blocked by disk I/O.
/// 后台线程异步刷盘，避免 append 时阻塞业务路径。
mod oplog_etcd;
mod oplog_local;
mod oplog_manager;
mod oplog_wire;
#[doc(hidden)]
pub mod test_support;

pub use oplog_etcd::EtcdOpLogStore;
pub use oplog_local::LocalFsOpLogStore;
pub(crate) use oplog_manager::LeaseRefreshEntry;
pub use oplog_manager::OpLogManager;
pub(crate) use oplog_wire::decode_record_payload_value;

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), HaError> {
    if snapshot_id.is_empty()
        || snapshot_id.contains('/')
        || snapshot_id.contains('\\')
        || snapshot_id.contains("..")
    {
        return Err(HaError::InvalidBackend(format!(
            "invalid snapshot id {snapshot_id:?}"
        )));
    }
    Ok(())
}
