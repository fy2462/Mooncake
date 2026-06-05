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

use crate::ha::{HaError, OpLogPollResult, OpLogRecord};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use mooncake_store_core::ReplicaDescriptor;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use uuid::Uuid;
use xxhash_rust::xxh32::xxh32;

const CPP_OP_PUT_END: u8 = 1;
const CPP_OP_PUT_REVOKE: u8 = 2;
const CPP_OP_REMOVE: u8 = 3;
const MAX_OBJECT_KEY_SIZE: usize = 4096;
const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
const PUT_END_MSGPACK_MAGIC: &[u8] = b"MCOPMETA1";
const OPLOG_MSGPACK_RECORD_PREFIX: &str = "msgpack:";

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
pub(crate) struct PutEndMetadataPayloadV1 {
    pub(crate) op: String,
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) client_id: Option<String>,
    pub(crate) tenant_id: String,
    pub(crate) group_id: String,
    pub(crate) user_key: String,
    pub(crate) replicas: Vec<ReplicaDescriptor>,
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
        self.last_seq += 1;
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
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
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
pub struct LocalFsOpLogStore {
    /// Root directory for segment files.
    /// 分段文件的根目录。
    dir: PathBuf,
    /// Maximum entries per segment file.
    max_entries_per_segment: usize,
    /// Entries accumulated in the current (not yet flushed) segment.
    buffer: Vec<OpLogRecord>,
    /// Monotonically increasing sequence counter.
    last_seq: u64,
    /// Starting sequence of the current segment.
    current_segment_seq: u64,
    /// Channel to send full buffers to the background flush thread.
    flush_tx: mpsc::Sender<Vec<OpLogRecord>>,
    /// Background flush thread handle — joined on drop.
    _flush_handle: Option<std::thread::JoinHandle<()>>,
}

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), HaError> {
    if snapshot_id.is_empty()
        || snapshot_id.contains('/')
        || snapshot_id.contains("..")
        || snapshot_id.as_bytes().contains(&0)
    {
        return Err(HaError::InvalidBackend(format!(
            "invalid snapshot id: {snapshot_id}"
        )));
    }
    Ok(())
}

impl LocalFsOpLogStore {
    /// Create a local filesystem oplog store.
    /// 创建本地文件 oplog store。
    /// - Creates the directory if it doesn't exist. / 创建目录（如不存在）
    /// - Spawns a background flush thread via mpsc channel. / 启动后台 flush 线程（通过 mpsc channel 接收待刷盘的 buffer）
    /// - Recovers last_seq from existing segment files. / 从已有分段文件恢复 last_seq
    pub fn new(dir: &Path, max_entries_per_segment: usize) -> Result<Self, HaError> {
        fs::create_dir_all(dir)
            .map_err(|e| HaError::InvalidBackend(format!("oplog dir create: {e}")))?;

        let dir_buf = dir.to_path_buf();
        let (flush_tx, flush_rx) = mpsc::channel::<Vec<OpLogRecord>>();

        // Background thread: asynchronously writes full buffers to disk.
        // 后台线程：异步将满 buffer 写入磁盘，不阻塞 append 调用方。
        let flush_dir = dir_buf.clone();
        let flush_handle = std::thread::spawn(move || {
            for entries in flush_rx {
                if let Err(e) = Self::flush_inner(&flush_dir, &entries) {
                    warn!("oplog background flush failed: {e}");
                }
            }
        });

        let mut store = Self {
            dir: dir_buf,
            max_entries_per_segment: max_entries_per_segment.max(1000),
            buffer: Vec::new(),
            last_seq: 0,
            current_segment_seq: 0,
            flush_tx,
            _flush_handle: Some(flush_handle),
        };
        // Recover latest sequence from existing segment files
        store.recover()?;
        Ok(store)
    }

    /// Recover last_seq from existing segment files: scan for the highest-
    /// numbered segment file and parse its last record's seq.
    /// 从磁盘恢复 last_seq：扫描编号最大的分段文件，解析其中最后一条记录的 seq。
    fn recover(&mut self) -> Result<(), HaError> {
        let persisted_latest = fs::read_to_string(self.latest_path())
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok());
        let mut segments: Vec<u64> = self.list_segment_files()?;
        segments.sort();
        if let Some(&highest_start) = segments.last() {
            let path = self.segment_path(highest_start);
            let data = fs::read(&path)
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            let entries = Self::parse_entries(&data);
            if let Some(last) = entries.last() {
                self.last_seq = last.seq;
                self.current_segment_seq = highest_start;
            }
        }
        if let Some(persisted_latest) = persisted_latest {
            self.last_seq = self.last_seq.max(persisted_latest);
        }
        Ok(())
    }

    /// List segment files in the oplog directory, returning their start_seq.
    /// 列出 oplog 目录中的分段文件，返回其 start_seq。
    fn list_segment_files(&self) -> Result<Vec<u64>, HaError> {
        let mut segments = Vec::new();
        let dir_entries = fs::read_dir(&self.dir)
            .map_err(|e| HaError::InvalidBackend(format!("oplog read dir: {e}")))?;
        for entry in dir_entries {
            let entry =
                entry.map_err(|e| HaError::InvalidBackend(format!("oplog dir entry: {e}")))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str
                .strip_prefix("oplog_")
                .and_then(|s| s.strip_suffix(".bin"))
            {
                if let Ok(seq) = rest.parse::<u64>() {
                    segments.push(seq);
                }
            }
        }
        Ok(segments)
    }

    /// Build the path for a segment file given its start sequence number.
    /// 根据起始序列号构建分段文件的路径。
    fn segment_path(&self, start_seq: u64) -> PathBuf {
        self.dir.join(format!("oplog_{:020}.bin", start_seq))
    }

    fn latest_path(&self) -> PathBuf {
        self.dir.join("latest")
    }

    fn snapshots_dir(&self) -> PathBuf {
        self.dir.join("snapshots")
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.snapshots_dir().join(snapshot_id)
    }

    /// Flush entries to a segment file atomically (synchronous — used by both
    /// the background thread and explicit flush() calls).
    /// 将 buffer 原子写入分段文件（同步操作，由后台线程和显式 flush() 调用）。
    ///
    /// Writes to .tmp, renames to final, then fsyncs the parent directory for durability.
    /// 写入 .tmp 文件后 rename 到最终文件名，并 fsync 父目录保证持久性。
    fn flush_inner(dir: &Path, entries: &[OpLogRecord]) -> Result<(), HaError> {
        if entries.is_empty() {
            return Ok(());
        }
        let start_seq = entries.first().unwrap().seq;
        let tmp_path = dir.join(format!("oplog_{:020}.tmp", start_seq));
        let final_path = dir.join(format!("oplog_{:020}.bin", start_seq));

        // Serialize entries in binary frame format
        // 以二进制帧格式序列化条目
        let mut data = Vec::with_capacity(entries.len() * 128);
        for entry in entries {
            let payload = entry.payload.as_bytes();
            data.extend_from_slice(&(entry.seq as u32).to_le_bytes());
            data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            data.extend_from_slice(payload);
        }

        fs::write(&tmp_path, &data)
            .map_err(|e| HaError::InvalidBackend(format!("oplog write segment: {e}")))?;
        fs::rename(&tmp_path, &final_path)
            .map_err(|e| HaError::InvalidBackend(format!("oplog rename segment: {e}")))?;
        // fsync parent directory for durability
        if let Ok(f) = fs::File::open(dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    /// Write the current buffer to a segment file atomically.
    fn flush(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        Self::flush_inner(&self.dir, &self.buffer)?;
        self.current_segment_seq = self.buffer.first().map(|e| e.seq).unwrap_or(0);
        self.write_latest(self.last_seq)?;
        self.buffer.clear();
        Ok(())
    }

    fn write_latest(&self, sequence_id: u64) -> Result<(), HaError> {
        fs::write(self.latest_path(), sequence_id.to_string())
            .map_err(|e| HaError::InvalidBackend(format!("oplog write latest: {e}")))?;
        if let Ok(f) = fs::File::open(&self.dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    /// Parse entries from binary frame format: [4B seq LE][4B payload_len LE][payload].
    /// 解析二进制帧格式的数据：每帧 [4B seq LE][4B payload_len LE][payload]。
    /// 遇到不完整帧时停止解析，保证容错性。
    ///
    /// Stops parsing on incomplete frames for fault tolerance.
    fn parse_entries(data: &[u8]) -> Vec<OpLogRecord> {
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset + 8 <= data.len() {
            let seq = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as u64;
            let payload_len = u32::from_le_bytes([
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]) as usize;
            offset += 8;
            if offset + payload_len > data.len() {
                break;
            }
            let payload = String::from_utf8_lossy(&data[offset..offset + payload_len]).into_owned();
            entries.push(OpLogRecord {
                seq,
                producer_view_version: 0,
                payload,
            });
            offset += payload_len;
        }
        entries
    }
}

impl OpLogStore for LocalFsOpLogStore {
    /// Append entry: buffer in memory, flush via background thread when buffer is full.
    /// 追加条目：先写入内存 buffer，buffer 满时通过 channel 发送给后台线程异步刷盘。
    ///
    /// If the flush channel is closed (background thread exited abnormally),
    /// falls back to a synchronous flush to prevent data loss.
    /// 若 channel 已关闭（后台线程异常退出），回退到同步 flush 保证数据不丢失。
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        // When buffer is full: send to background thread for async flush
        // buffer 满时触发异步刷盘：swap 空 buffer 后将旧数据发给后台线程
        if self.buffer.len() >= self.max_entries_per_segment {
            let to_flush = std::mem::take(&mut self.buffer);
            match self.flush_tx.send(to_flush) {
                Ok(()) => {} // Background thread handles the flush / 后台线程接管刷盘
                Err(mpsc::SendError(entries)) => {
                    // Channel closed — fallback to synchronous flush to prevent data loss
                    // Channel 关闭，回退到同步 flush 兜底（防止数据丢失）
                    warn!("oplog flush channel closed, falling back to sync flush");
                    self.buffer = entries;
                    self.flush()?;
                }
            }
        }
        Ok(self.last_seq)
    }

    /// Read entries starting from since_seq.
    /// Reads both on-disk segment files and in-memory (unflushed) buffer.
    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        let segments = self.list_segment_files()?;
        let mut all_entries = Vec::new();

        // Read from on-disk segment files
        let mut sorted_segs = segments;
        sorted_segs.sort();
        for start_seq in sorted_segs {
            if start_seq > since_seq + 100_000 {
                break; // optimization: don't read far-ahead segments
            }
            let data = fs::read(self.segment_path(start_seq))
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            for entry in Self::parse_entries(&data) {
                if entry.seq >= since_seq {
                    all_entries.push(entry);
                    if all_entries.len() >= max_count {
                        return Ok(all_entries);
                    }
                }
            }
        }

        // Also include buffered (unflushed) entries
        for entry in &self.buffer {
            if entry.seq >= since_seq {
                all_entries.push(entry.clone());
                if all_entries.len() >= max_count {
                    break;
                }
            }
        }

        all_entries.truncate(max_count);
        Ok(all_entries)
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        let mut max_seq = self.last_seq;
        for start_seq in self.list_segment_files()? {
            let data = fs::read(self.segment_path(start_seq))
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            if let Some(entry) = Self::parse_entries(&data).last() {
                max_seq = max_seq.max(entry.seq);
            }
        }
        for entry in &self.buffer {
            max_seq = max_seq.max(entry.seq);
        }
        Ok(max_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.last_seq = sequence_id;
        self.write_latest(sequence_id)
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        fs::create_dir_all(self.snapshots_dir())
            .map_err(|e| HaError::InvalidBackend(format!("oplog create snapshots dir: {e}")))?;
        fs::write(self.snapshot_path(snapshot_id), sequence_id.to_string())
            .map_err(|e| HaError::InvalidBackend(format!("oplog write snapshot seq: {e}")))?;
        if let Ok(f) = fs::File::open(self.snapshots_dir()) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        validate_snapshot_id(snapshot_id)?;
        let value = fs::read_to_string(self.snapshot_path(snapshot_id))
            .map_err(|e| HaError::InvalidBackend(format!("oplog read snapshot seq: {e}")))?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|e| HaError::InvalidBackend(format!("oplog parse snapshot seq: {e}")))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.buffer.retain(|entry| entry.seq >= before_sequence_id);
        for start_seq in self.list_segment_files()? {
            let path = self.segment_path(start_seq);
            let data = fs::read(&path)
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            let entries = Self::parse_entries(&data);
            let retained = entries
                .iter()
                .filter(|entry| entry.seq >= before_sequence_id)
                .cloned()
                .collect::<Vec<_>>();
            if retained.is_empty() {
                fs::remove_file(&path)
                    .map_err(|e| HaError::InvalidBackend(format!("oplog cleanup segment: {e}")))?;
            } else if retained.len() != entries.len() {
                fs::remove_file(&path)
                    .map_err(|e| HaError::InvalidBackend(format!("oplog rewrite segment: {e}")))?;
                Self::flush_inner(&self.dir, &retained)?;
            }
        }
        if let Ok(f) = fs::File::open(&self.dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        self.flush()
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}

fn serialize_etcd_oplog_value(entry: &OpLogRecord) -> Result<String, HaError> {
    validate_record_size(entry)?;
    if let Some(wire) = cpp_wire_entry_from_record(entry) {
        serde_json::to_string(&wire)
            .map_err(|e| HaError::InvalidBackend(format!("oplog wire serialize: {e}")))
    } else {
        serde_json::to_string(entry)
            .map_err(|e| HaError::InvalidBackend(format!("oplog serialize: {e}")))
    }
}

fn deserialize_etcd_oplog_value(value: &str) -> Result<OpLogRecord, HaError> {
    if let Ok(entry) = serde_json::from_str::<OpLogRecord>(value) {
        return Ok(entry);
    }

    let wire: CppOpLogWireEntry = serde_json::from_str(value)
        .map_err(|e| HaError::InvalidBackend(format!("oplog wire deserialize: {e}")))?;
    validate_wire_entry_size(&wire)?;
    let payload = rust_payload_from_cpp_wire_entry(&wire)?;
    Ok(OpLogRecord {
        seq: wire.sequence_id,
        producer_view_version: 0,
        payload,
    })
}

fn validate_record_size(entry: &OpLogRecord) -> Result<(), HaError> {
    if entry.payload.len() > MAX_PAYLOAD_SIZE {
        return Err(HaError::InvalidBackend(format!(
            "oplog payload too large: {}",
            entry.payload.len()
        )));
    }
    if let Ok(payload_json) = decode_record_payload_value(&entry.payload) {
        if let Some(key) = payload_json.get("key").and_then(|key| key.as_str()) {
            if key.len() > MAX_OBJECT_KEY_SIZE {
                return Err(HaError::InvalidBackend(format!(
                    "oplog object key too large: {}",
                    key.len()
                )));
            }
        }
    }
    Ok(())
}

fn validate_wire_entry_size(wire: &CppOpLogWireEntry) -> Result<(), HaError> {
    if wire.object_key.len() > MAX_OBJECT_KEY_SIZE {
        return Err(HaError::InvalidBackend(format!(
            "oplog object key too large: {}",
            wire.object_key.len()
        )));
    }
    let max_base64_payload = MAX_PAYLOAD_SIZE.div_ceil(3) * 4;
    if wire.payload.len() > max_base64_payload {
        return Err(HaError::InvalidBackend(format!(
            "oplog payload too large: {}",
            wire.payload.len()
        )));
    }
    Ok(())
}

fn cpp_wire_entry_from_record(entry: &OpLogRecord) -> Option<CppOpLogWireEntry> {
    if let Some(payload_bytes) = decode_msgpack_record_payload_bytes(&entry.payload).ok()? {
        if payload_bytes.starts_with(PUT_END_MSGPACK_MAGIC) {
            let payload = decode_put_end_msgpack_typed(&payload_bytes).ok()?;
            let object_key = payload.key;
            let checksum = compute_cpp_checksum(&payload_bytes);
            let prefix_hash = compute_cpp_prefix_hash(&object_key);
            return Some(CppOpLogWireEntry {
                sequence_id: entry.seq,
                timestamp_ms: unix_timestamp_ms(),
                op_type: CPP_OP_PUT_END,
                object_key,
                payload: BASE64_STANDARD.encode(payload_bytes),
                checksum,
                prefix_hash,
            });
        }
        let payload_json: serde_json::Value = rmp_serde::from_slice(&payload_bytes).ok()?;
        return cpp_wire_entry_from_payload_json(entry, &payload_json);
    }

    let payload_json = serde_json::from_str::<serde_json::Value>(&entry.payload).ok()?;
    cpp_wire_entry_from_payload_json(entry, &payload_json)
}

fn cpp_wire_entry_from_payload_json(
    entry: &OpLogRecord,
    payload_json: &serde_json::Value,
) -> Option<CppOpLogWireEntry> {
    let op = payload_json.get("op")?.as_str()?;
    let (op_type, object_key, payload_bytes) = match op {
        "put_end" => (
            CPP_OP_PUT_END,
            payload_json.get("key")?.as_str()?.to_string(),
            encode_put_end_msgpack_from_json(&payload_json).ok()?,
        ),
        "put_revoke" => (
            CPP_OP_PUT_REVOKE,
            payload_json.get("key")?.as_str()?.to_string(),
            Vec::new(),
        ),
        "remove" => (
            CPP_OP_REMOVE,
            payload_json.get("key")?.as_str()?.to_string(),
            Vec::new(),
        ),
        _ => return None,
    };

    let checksum = compute_cpp_checksum(&payload_bytes);
    let prefix_hash = compute_cpp_prefix_hash(&object_key);
    Some(CppOpLogWireEntry {
        sequence_id: entry.seq,
        timestamp_ms: unix_timestamp_ms(),
        op_type,
        object_key,
        payload: BASE64_STANDARD.encode(payload_bytes),
        checksum,
        prefix_hash,
    })
}

fn encode_msgpack_record_payload_value(payload: &serde_json::Value) -> Result<String, HaError> {
    let bytes = rmp_serde::to_vec_named(payload)
        .map_err(|e| HaError::InvalidBackend(format!("oplog msgpack encode: {e}")))?;
    Ok(format!(
        "{}{}",
        OPLOG_MSGPACK_RECORD_PREFIX,
        BASE64_STANDARD.encode(bytes)
    ))
}

fn encode_put_end_record_payload(payload: &PutEndMetadataPayloadV1) -> Result<String, HaError> {
    let bytes = encode_put_end_msgpack(payload)?;
    Ok(format!(
        "{}{}",
        OPLOG_MSGPACK_RECORD_PREFIX,
        BASE64_STANDARD.encode(bytes)
    ))
}

fn decode_msgpack_record_payload_bytes(payload: &str) -> Result<Option<Vec<u8>>, HaError> {
    let Some(encoded) = payload.strip_prefix(OPLOG_MSGPACK_RECORD_PREFIX) else {
        return Ok(None);
    };
    BASE64_STANDARD
        .decode(encoded)
        .map(Some)
        .map_err(|e| HaError::InvalidBackend(format!("oplog msgpack base64 decode: {e}")))
}

fn encode_put_end_msgpack(payload: &PutEndMetadataPayloadV1) -> Result<Vec<u8>, HaError> {
    let mut bytes = Vec::from(PUT_END_MSGPACK_MAGIC);
    let body = rmp_serde::to_vec_named(payload)
        .map_err(|e| HaError::InvalidBackend(format!("put_end msgpack encode: {e}")))?;
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

fn encode_put_end_msgpack_from_json(payload_json: &serde_json::Value) -> Result<Vec<u8>, HaError> {
    let payload = PutEndMetadataPayloadV1 {
        op: "put_end".to_string(),
        key: payload_json
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        size: payload_json
            .get("size")
            .and_then(|v| v.as_u64())
            .unwrap_or_default(),
        client_id: payload_json
            .get("client_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        tenant_id: payload_json
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        group_id: payload_json
            .get("group_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        user_key: payload_json
            .get("user_key")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        replicas: payload_json
            .get("replicas")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| HaError::InvalidBackend(format!("put_end replicas decode: {e}")))?
            .unwrap_or_default(),
    };
    encode_put_end_msgpack(&payload)
}

fn decode_put_end_msgpack_typed(bytes: &[u8]) -> Result<PutEndMetadataPayloadV1, HaError> {
    let body = bytes
        .strip_prefix(PUT_END_MSGPACK_MAGIC)
        .ok_or_else(|| HaError::InvalidBackend("put_end msgpack magic mismatch".into()))?;
    rmp_serde::from_slice(body)
        .map_err(|e| HaError::InvalidBackend(format!("put_end msgpack decode: {e}")))
}

pub(crate) fn decode_put_end_msgpack(bytes: &[u8]) -> Result<serde_json::Value, HaError> {
    serde_json::to_value(decode_put_end_msgpack_typed(bytes)?)
        .map_err(|e| HaError::InvalidBackend(format!("put_end msgpack to json: {e}")))
}

pub(crate) fn decode_record_payload_value(payload: &str) -> Result<serde_json::Value, HaError> {
    if let Some(bytes) = decode_msgpack_record_payload_bytes(payload)? {
        if bytes.starts_with(PUT_END_MSGPACK_MAGIC) {
            return decode_put_end_msgpack(&bytes);
        }
        return rmp_serde::from_slice(&bytes)
            .map_err(|e| HaError::InvalidBackend(format!("oplog msgpack decode: {e}")));
    }
    serde_json::from_str(payload)
        .map_err(|e| HaError::InvalidBackend(format!("oplog json decode: {e}")))
}

fn rust_payload_from_cpp_wire_entry(wire: &CppOpLogWireEntry) -> Result<String, HaError> {
    let decoded_payload = if wire.payload.is_empty() {
        Vec::new()
    } else {
        BASE64_STANDARD
            .decode(&wire.payload)
            .map_err(|e| HaError::InvalidBackend(format!("oplog payload base64 decode: {e}")))?
    };
    if wire.checksum != 0 && compute_cpp_checksum(&decoded_payload) != wire.checksum {
        return Err(HaError::InvalidBackend(format!(
            "oplog checksum mismatch for seq={}",
            wire.sequence_id
        )));
    }

    match wire.op_type {
        CPP_OP_PUT_END => {
            let metadata_payload_base64 = BASE64_STANDARD.encode(&decoded_payload);
            if decoded_payload.starts_with(PUT_END_MSGPACK_MAGIC) {
                return Ok(format!(
                    "{}{}",
                    OPLOG_MSGPACK_RECORD_PREFIX,
                    BASE64_STANDARD.encode(decoded_payload)
                ));
            }
            if let Ok(payload) = String::from_utf8(decoded_payload) {
                if serde_json::from_str::<serde_json::Value>(&payload)
                    .ok()
                    .and_then(|v| v.get("op").and_then(|op| op.as_str()).map(str::to_string))
                    .as_deref()
                    == Some("put_end")
                {
                    return Ok(payload);
                }
            }
            Ok(json!({
                "op": "put_end",
                "key": wire.object_key,
                "size": 0,
                "metadata_payload_base64": metadata_payload_base64
            })
            .to_string())
        }
        CPP_OP_PUT_REVOKE => Ok(json!({"op": "put_revoke", "key": wire.object_key}).to_string()),
        CPP_OP_REMOVE => Ok(json!({"op": "remove", "key": wire.object_key}).to_string()),
        other => Err(HaError::InvalidBackend(format!(
            "unsupported C++ oplog op_type: {other}"
        ))),
    }
}

fn compute_cpp_checksum(payload: &[u8]) -> u32 {
    xxh32(payload, 0)
}

fn compute_cpp_prefix_hash(key: &str) -> u32 {
    if key.is_empty() {
        0
    } else {
        xxh32(key.as_bytes(), 0)
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// =============================================================================
// Etcd-backed OpLog Store / 基于 etcd 的 OpLog 存储
// =============================================================================

/// Persistent oplog backed by etcd.
/// 基于 etcd 的持久化 oplog。
///
/// Each record is stored as `/oplog/<cluster>/<seq:020>` with zero-padded
/// sequence numbers to match the C++ EtcdOpLogStore key format.
/// 每条记录存储为 `/oplog/<cluster>/<seq:020>`，零填充序列号并匹配 C++ key 格式。
///
/// A separate `/oplog/<prefix>/latest` key holds the latest sequence number
/// for fast recovery without scanning all keys.
/// 独立的 `/oplog/<prefix>/latest` key 记录最新序列号，加速恢复。
pub struct EtcdOpLogStore {
    client: etcd_client::Client,
    key_prefix: String,
    last_seq: u64,
    /// Entries accumulated for batch write.
    buffer: Vec<OpLogRecord>,
}

impl EtcdOpLogStore {
    /// Create an etcd-backed oplog store, recovering last_seq from the /latest key.
    /// 创建 etcd oplog store，通过读取 `/latest` key 恢复 last_seq。
    pub async fn new(client: etcd_client::Client, key_prefix: &str) -> Result<Self, HaError> {
        let prefix = key_prefix.trim_end_matches('/').to_string();
        let mut store = Self {
            client,
            key_prefix: prefix,
            last_seq: 0,
            buffer: Vec::new(),
        };
        store.recover().await?;
        Ok(store)
    }

    fn reader_clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            key_prefix: self.key_prefix.clone(),
            last_seq: self.last_seq,
            buffer: Vec::new(),
        }
    }

    /// Recover `last_seq` from the `/latest` key.
    async fn recover(&mut self) -> Result<(), HaError> {
        let latest_key = format!("{}/latest", self.key_prefix);
        let c = self.client.clone();
        match c.kv_client().get(latest_key.as_bytes(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        self.last_seq = val.parse::<u64>().unwrap_or(0);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read oplog latest key: {}", e);
            }
        }
        Ok(())
    }

    /// Build the etcd key for a given sequence number.
    fn entry_key(&self, seq: u64) -> String {
        Self::format_entry_key(&self.key_prefix, seq)
    }

    fn format_entry_key(key_prefix: &str, seq: u64) -> String {
        format!("{}/{:020}", key_prefix.trim_end_matches('/'), seq)
    }

    /// Build the etcd key for the latest sequence pointer.
    fn latest_key(&self) -> String {
        format!("{}/latest", self.key_prefix)
    }

    fn snapshot_key(&self, snapshot_id: &str) -> String {
        format!("{}/snapshot/{}", self.key_prefix, snapshot_id)
    }

    /// Flush buffered entries to etcd: put each record, then update /latest.
    /// 将 buffer 批量写入 etcd：逐个 put 每条记录，最后更新 `/latest` 指针。
    async fn flush(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let c = self.client.clone();
        for entry in &self.buffer {
            let key = self.entry_key(entry.seq);
            let value = serialize_etcd_oplog_value(entry)?;
            c.kv_client()
                .put(key.as_bytes(), value.as_bytes(), None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd put oplog: {e}")))?;
        }
        // Update latest pointer
        let max_seq = self.buffer.last().unwrap().seq;
        let latest_val = max_seq.to_string();
        c.kv_client()
            .put(self.latest_key().as_bytes(), latest_val.as_bytes(), None)
            .await
            .map_err(|e| HaError::InvalidBackend(format!("etcd put oplog latest: {e}")))?;

        self.buffer.clear();
        Ok(())
    }
}

impl OpLogStore for EtcdOpLogStore {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        block_on_runtime(self.flush())?;
        Ok(self.last_seq)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        block_on_runtime(self.read_since_async(since_seq, max_count))
    }

    fn latest_sequence(&self) -> u64 {
        let c = self.client.clone();
        let latest_key = self.latest_key();
        block_on_runtime(async move {
            match c.kv_client().get(latest_key.as_bytes(), None).await {
                Ok(resp) => resp
                    .kvs()
                    .first()
                    .and_then(|kv| String::from_utf8(kv.value().to_vec()).ok())
                    .and_then(|v| v.parse::<u64>().ok()),
                Err(_) => None,
            }
        })
        .unwrap_or(self.last_seq)
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        let mut max_seq = self.last_seq;
        let c = self.client.clone();
        let range_start = self.entry_key(0);
        let range_end = self.entry_key(u64::MAX);
        let backend_max = block_on_runtime(async move {
            c.kv_client()
                .get(
                    range_start.as_bytes(),
                    Some(
                        etcd_client::GetOptions::new()
                            .with_range(range_end.as_bytes())
                            .with_sort(
                                etcd_client::SortTarget::Key,
                                etcd_client::SortOrder::Descend,
                            )
                            .with_limit(1),
                    ),
                )
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd get max oplog: {e}")))
        })?
        .kvs()
        .first()
        .and_then(|kv| String::from_utf8(kv.key().to_vec()).ok())
        .and_then(|key| {
            key.rsplit('/')
                .next()
                .and_then(|seq| seq.parse::<u64>().ok())
        });
        if let Some(backend_max) = backend_max {
            max_seq = max_seq.max(backend_max);
        }
        Ok(max_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.last_seq = sequence_id;
        let c = self.client.clone();
        let latest_key = self.latest_key();
        block_on_runtime(async move {
            c.kv_client()
                .put(
                    latest_key.as_bytes(),
                    sequence_id.to_string().as_bytes(),
                    None,
                )
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd put oplog latest: {e}")))
        })?;
        Ok(())
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        let c = self.client.clone();
        let key = self.snapshot_key(snapshot_id);
        block_on_runtime(async move {
            c.kv_client()
                .put(key.as_bytes(), sequence_id.to_string().as_bytes(), None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd put snapshot seq: {e}")))
        })?;
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        validate_snapshot_id(snapshot_id)?;
        let c = self.client.clone();
        let key = self.snapshot_key(snapshot_id);
        let value = block_on_runtime(async move {
            c.kv_client()
                .get(key.as_bytes(), None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd get snapshot seq: {e}")))
        })?
        .kvs()
        .first()
        .and_then(|kv| String::from_utf8(kv.value().to_vec()).ok())
        .ok_or_else(|| HaError::InvalidBackend(format!("snapshot not found: {snapshot_id}")))?;
        value
            .parse::<u64>()
            .map_err(|e| HaError::InvalidBackend(format!("etcd parse snapshot seq: {e}")))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        let c = self.client.clone();
        let range_start = self.entry_key(0);
        let range_end = self.entry_key(before_sequence_id);
        block_on_runtime(async move {
            c.kv_client()
                .delete(
                    range_start.as_bytes(),
                    Some(etcd_client::DeleteOptions::new().with_range(range_end.as_bytes())),
                )
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd cleanup oplog: {e}")))
        })?;
        Ok(())
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        block_on_runtime(self.flush())
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }

    fn create_change_notifier(&self) -> Option<Box<dyn OpLogChangeNotifier>> {
        Some(Box::new(EtcdOpLogChangeNotifier::new(self.reader_clone())))
    }
}

struct EtcdOpLogChangeNotifier {
    store: EtcdOpLogStore,
    shutdown_tx: Option<tokio::sync::watch::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    healthy: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl EtcdOpLogChangeNotifier {
    fn new(store: EtcdOpLogStore) -> Self {
        Self {
            store,
            shutdown_tx: None,
            thread: None,
            healthy: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

impl OpLogChangeNotifier for EtcdOpLogChangeNotifier {
    fn start(
        &mut self,
        start_seq_id: u64,
        mut on_entry: OpLogEntryCallback,
        mut on_error: OpLogErrorCallback,
    ) -> Result<(), HaError> {
        if self.thread.is_some() {
            return Ok(());
        }
        let store = self.store.reader_clone();
        let healthy = self.healthy.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        self.shutdown_tx = Some(shutdown_tx);
        self.thread = Some(std::thread::spawn(move || {
            healthy.store(true, std::sync::atomic::Ordering::Release);
            let _ = block_on_runtime(store.watch_entries_from_until(
                start_seq_id,
                1000,
                shutdown_rx,
                move |entry| on_entry(entry),
                move |err| on_error(err),
            ));
            healthy.store(false, std::sync::atomic::Ordering::Release);
        }));
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.healthy
            .store(false, std::sync::atomic::Ordering::Release);
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(std::sync::atomic::Ordering::Acquire)
    }
}

fn block_on_runtime<F: Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create temporary tokio runtime")
            .block_on(future)
    }
}

// =============================================================================
// EtcdOpLogStore — async extension methods
// =============================================================================

/// Async extension for etcd-backed stores that need full range queries.
impl EtcdOpLogStore {
    /// Read entries from etcd starting from `since_seq` (async).
    /// 从 etcd 异步读取条目（起始序列号 since_seq）。
    pub async fn read_since_async(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        self.read_since_with_revision_async(since_seq, max_count)
            .await
            .map(|(entries, _)| entries)
    }

    pub async fn read_since_with_revision_async(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<(Vec<OpLogRecord>, i64), HaError> {
        let mut entries = Vec::new();
        let c = self.client.clone();

        // Range query: from seq_N to seq_MAX across the key prefix
        let range_end = self.entry_key(u64::MAX);
        let range_start = self.entry_key(since_seq);

        let revision = match c
            .kv_client()
            .get(
                range_start.as_bytes(),
                Some(etcd_client::GetOptions::new().with_range(range_end.as_bytes())),
            )
            .await
        {
            Ok(resp) => {
                let revision = resp.header().map(|h| h.revision()).unwrap_or_default();
                for kv in resp.kvs().iter().take(max_count) {
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        if let Ok(entry) = deserialize_etcd_oplog_value(&val) {
                            entries.push(entry);
                            if entries.len() >= max_count {
                                break;
                            }
                        }
                    }
                }
                revision
            }
            Err(e) => {
                return Err(HaError::InvalidBackend(format!(
                    "etcd range query for oplog failed: {e}"
                )));
            }
        };

        // Supplement with buffered (not yet flushed) entries
        for entry in &self.buffer {
            if entry.seq >= since_seq
                && entries.len() < max_count
                && !entries.iter().any(|e| e.seq == entry.seq)
            {
                entries.push(entry.clone());
            }
        }

        entries.sort_by_key(|e| e.seq);
        entries.truncate(max_count);
        Ok((entries, revision))
    }

    /// Flush buffered entries to etcd (async). Call periodically or before shutdown.
    pub async fn flush_async(&mut self) -> Result<(), HaError> {
        self.flush().await
    }

    pub async fn watch_entries_from<F, E>(
        &self,
        start_seq_id: u64,
        max_historical_batch: usize,
        mut on_entry: F,
        mut on_error: E,
    ) -> Result<(), HaError>
    where
        F: FnMut(OpLogRecord) + Send,
        E: FnMut(HaError) + Send,
    {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        self.watch_entries_from_until(
            start_seq_id,
            max_historical_batch,
            shutdown_rx,
            &mut on_entry,
            &mut on_error,
        )
        .await
    }

    pub async fn watch_entries_from_until<F, E>(
        &self,
        start_seq_id: u64,
        max_historical_batch: usize,
        mut shutdown_rx: tokio::sync::watch::Receiver<()>,
        mut on_entry: F,
        mut on_error: E,
    ) -> Result<(), HaError>
    where
        F: FnMut(OpLogRecord) + Send,
        E: FnMut(HaError) + Send,
    {
        let batch_size = max_historical_batch.max(1);
        let mut read_seq = start_seq_id;
        let mut last_seq = start_seq_id.saturating_sub(1);
        let mut revision = 0;
        loop {
            if shutdown_rx.has_changed().unwrap_or(true) {
                return Ok(());
            }
            let (historical, read_revision) = self
                .read_since_with_revision_async(read_seq, batch_size)
                .await?;
            if read_revision > 0 {
                revision = read_revision;
            }
            let delivered = historical.len();
            for entry in historical {
                last_seq = last_seq.max(entry.seq);
                read_seq = entry.seq.saturating_add(1);
                on_entry(entry);
            }
            if delivered < batch_size {
                break;
            }
        }

        let watch_prefix = format!("{}/", self.key_prefix.trim_end_matches('/'));
        let mut watch_client = self.client.clone().watch_client();
        let options = etcd_client::WatchOptions::new()
            .with_prefix()
            .with_start_revision(revision.saturating_add(1));
        let (_watcher, mut stream) = watch_client
            .watch(watch_prefix.as_bytes(), Some(options))
            .await
            .map_err(|e| HaError::InvalidBackend(format!("etcd watch oplog: {e}")))?;

        loop {
            let message = tokio::select! {
                changed = shutdown_rx.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                message = stream.message() => message,
            };
            match message {
                Ok(Some(response)) => {
                    if response.canceled() {
                        return Err(HaError::InvalidBackend(format!(
                            "etcd watch canceled: {}",
                            response.cancel_reason()
                        )));
                    }
                    for event in response.events() {
                        let Some(kv) = event.kv() else {
                            continue;
                        };
                        let key = String::from_utf8_lossy(kv.key());
                        if key.ends_with("/latest") || key.contains("/snapshot/") {
                            continue;
                        }
                        if event.event_type() == etcd_client::EventType::Delete {
                            continue;
                        }
                        let value = match String::from_utf8(kv.value().to_vec()) {
                            Ok(value) => value,
                            Err(e) => {
                                let err =
                                    HaError::InvalidBackend(format!("etcd watch oplog utf8: {e}"));
                                on_error(err.clone());
                                continue;
                            }
                        };
                        match deserialize_etcd_oplog_value(&value) {
                            Ok(entry) => {
                                if entry.seq > last_seq {
                                    last_seq = entry.seq;
                                    on_entry(entry);
                                }
                            }
                            Err(e) => on_error(e),
                        }
                    }
                }
                Ok(None) => {
                    return Err(HaError::InvalidBackend(
                        "etcd watch stream closed".to_string(),
                    ));
                }
                Err(e) => {
                    let err = HaError::InvalidBackend(format!("etcd watch stream: {e}"));
                    on_error(err.clone());
                    return Err(err);
                }
            }
        }
    }
}

// =============================================================================
// OpLogManager — high-level wrapper that records mutations into the oplog
// =============================================================================

/// High-level manager that serializes business operations (put, remove, mount,
/// etc.) as JSON and appends them to the underlying OpLogStore.
/// OpLog 管理器：将业务操作（put/remove/mount 等）序列化为 JSON 并追加到底层 OpLogStore。
///
/// Best-effort record_* helpers keep the old non-blocking behavior. The
/// append_and_persist / record_*_durable helpers are used for mutations where
/// stale standby state would be unsafe after promotion.
pub struct OpLogManager {
    store: Option<Box<dyn OpLogStore + Send>>,
    view_version: u64,
}

impl OpLogManager {
    pub fn new(store: Option<Box<dyn OpLogStore + Send>>, view_version: u64) -> Self {
        Self {
            store,
            view_version,
        }
    }

    pub fn latest_sequence(&self) -> u64 {
        self.store
            .as_ref()
            .map(|s| s.latest_sequence())
            .unwrap_or(0)
    }

    pub fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.store
            .as_ref()
            .map(|s| s.max_sequence_id())
            .unwrap_or(Ok(0))
    }

    pub fn set_view_version(&mut self, version: u64) {
        self.view_version = version;
    }

    pub fn store(&self) -> Option<&(dyn OpLogStore + Send)> {
        self.store.as_deref()
    }

    pub fn into_store(self) -> Option<Box<dyn OpLogStore + Send>> {
        self.store
    }

    fn append_payload(&mut self, payload: String) -> Result<u64, HaError> {
        let Some(store) = &mut self.store else {
            return Ok(0);
        };
        let record = OpLogRecord {
            seq: 0,
            producer_view_version: self.view_version,
            payload,
        };
        validate_record_size(&record)?;
        store.append(&record)
    }

    pub fn append_and_persist(&mut self, payload: String) -> Result<u64, HaError> {
        let Some(store) = &mut self.store else {
            return Ok(0);
        };
        let record = OpLogRecord {
            seq: 0,
            producer_view_version: self.view_version,
            payload,
        };
        validate_record_size(&record)?;
        let seq = store.append(&record)?;
        store.flush_durable()?;
        Ok(seq)
    }

    pub fn set_initial_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        if let Some(store) = &mut self.store {
            store.update_latest_sequence_id(sequence_id)?;
        }
        Ok(())
    }

    pub fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        if let Some(store) = &mut self.store {
            store.cleanup_before(before_sequence_id)?;
        }
        Ok(())
    }

    pub fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        if let Some(store) = &mut self.store {
            store.record_snapshot_sequence_id(snapshot_id, sequence_id)?;
        }
        Ok(())
    }

    pub fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.store
            .as_ref()
            .map(|s| s.get_snapshot_sequence_id(snapshot_id))
            .unwrap_or(Ok(0))
    }

    pub fn record_put_end(&mut self, key: &str, size: u64) {
        self.record_put_end_with_metadata(key, size, None, "", "", "", &[]);
    }

    /// Record a put_end mutation with enough metadata for standby replay to
    /// recreate an object that was created after the latest snapshot.
    pub fn record_put_end_with_metadata(
        &mut self,
        key: &str,
        size: u64,
        client_id: Option<Uuid>,
        tenant_id: &str,
        group_id: &str,
        user_key: &str,
        replicas: &[ReplicaDescriptor],
    ) {
        if let Some(store) = &mut self.store {
            let payload = PutEndMetadataPayloadV1 {
                op: "put_end".to_string(),
                key: key.to_string(),
                size,
                client_id: client_id.map(|id| id.to_string()),
                tenant_id: tenant_id.to_string(),
                group_id: group_id.to_string(),
                user_key: user_key.to_string(),
                replicas: replicas.to_vec(),
            };
            let record = OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload: match encode_put_end_record_payload(&payload) {
                    Ok(payload) => payload,
                    Err(e) => {
                        warn!("OpLogManager: failed to encode put_end for key={key}: {e}");
                        return;
                    }
                },
            };
            if let Err(e) = validate_record_size(&record).and_then(|_| store.append(&record)) {
                warn!("OpLogManager: failed to record put_end for key={key}: {e}");
            }
        }
    }

    /// Record a remove mutation: { "op": "remove", "key": "..." }
    pub fn record_remove(&mut self, key: &str) {
        match encode_msgpack_record_payload_value(&json!({"op": "remove", "key": key})) {
            Ok(payload) => {
                if let Err(e) = self.append_payload(payload) {
                    warn!("OpLogManager: failed to record remove for key={key}: {e}");
                }
            }
            Err(e) => warn!("OpLogManager: failed to encode remove for key={key}: {e}"),
        }
    }

    pub fn record_remove_durable(&mut self, key: &str) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(
            &json!({"op": "remove", "key": key}),
        )?)
    }

    /// Record a put_revoke mutation that fully removes an unfinished object.
    pub fn record_put_revoke(&mut self, key: &str) {
        match encode_msgpack_record_payload_value(&json!({"op": "put_revoke", "key": key})) {
            Ok(payload) => {
                if let Err(e) = self.append_payload(payload) {
                    warn!("OpLogManager: failed to record put_revoke for key={key}: {e}");
                }
            }
            Err(e) => warn!("OpLogManager: failed to encode put_revoke for key={key}: {e}"),
        }
    }

    pub fn record_put_revoke_durable(&mut self, key: &str) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(
            &json!({"op": "put_revoke", "key": key}),
        )?)
    }

    /// Record a mount-segment mutation.
    pub fn record_mount_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "mount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("OpLogManager: failed to encode mount_segment for {segment_name}: {e}");
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record mount_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record an unmount-segment mutation.
    pub fn record_unmount_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "unmount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("OpLogManager: failed to encode unmount_segment for {segment_name}: {e}");
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record unmount_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record a mount-nof-segment mutation.
    pub fn record_mount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "mount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!(
                        "OpLogManager: failed to encode mount_nof_segment for {segment_name}: {e}"
                    );
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record mount_nof_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record an unmount-nof-segment mutation.
    pub fn record_unmount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "unmount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!(
                        "OpLogManager: failed to encode unmount_nof_segment for {segment_name}: {e}"
                    );
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record unmount_nof_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record a put-start mutation: { "op": "put_start", "key": "...", "client_id": "..." }
    pub fn record_put_start(&mut self, key: &str, client_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "put_start",
                "key": key,
                "client_id": client_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("OpLogManager: failed to encode put_start for key={key}: {e}");
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record put_start for key={key}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64) -> OpLogRecord {
        OpLogRecord {
            seq,
            producer_view_version: 1,
            payload: format!("entry-{}", seq),
        }
    }

    #[test]
    fn test_in_memory_append_and_poll() {
        let mut oplog = InMemoryOpLog::new(1000);
        oplog.append(&make_entry(0)).unwrap();
        oplog.append(&make_entry(0)).unwrap();
        oplog.append(&make_entry(0)).unwrap();
        assert_eq!(oplog.latest_sequence(), 3);

        let result = oplog.poll_from(1, 10);
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.next_seq, 4);
    }

    #[test]
    fn test_in_memory_poll_empty() {
        let oplog = InMemoryOpLog::new(1000);
        let result = oplog.poll_from(1, 10);
        assert!(result.records.is_empty());
        assert_eq!(result.next_seq, 1);
    }

    #[test]
    fn test_oplog_manager_records_put_revoke() {
        let store = InMemoryOpLog::new(1000);
        let mut manager = OpLogManager::new(Some(Box::new(store)), 7);

        manager.record_put_revoke("k1");

        let store = manager.into_store().unwrap();
        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].producer_view_version, 7);
        assert_eq!(
            decode_record_payload_value(&entries[0].payload).unwrap(),
            json!({"key":"k1","op":"put_revoke"})
        );
    }

    #[test]
    fn test_local_fs_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();

        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap();
        assert_eq!(store.latest_sequence(), 2);

        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[1].seq, 2);
    }

    #[test]
    fn test_local_fs_flush_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

        // Appending 3 entries with max=2 triggers flush of first 2
        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap(); // flush triggered for entries 1-2
                                               // Manually flush remaining buffer so entry 3 is on disk
        store.flush().unwrap();

        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 3);

        // Re-open recovers the state
        let store2 = LocalFsOpLogStore::new(dir.path(), 2).unwrap();
        assert_eq!(store2.latest_sequence(), 3);
    }

    #[test]
    fn test_local_fs_poll_from() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();

        for _ in 0..10 {
            store.append(&make_entry(0)).unwrap();
        }
        let result = store.poll_from(5, 3);
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].seq, 5);
        assert_eq!(result.next_seq, 8);
    }

    #[test]
    fn test_local_fs_async_flush_readable_without_explicit_flush() {
        let dir = tempfile::tempdir().unwrap();
        // max_entries_per_segment=3 triggers async flush on every 3rd append.
        let mut store = LocalFsOpLogStore::new(dir.path(), 3).unwrap();

        // Append 7 entries: triggers async flush at append #3 and #6.
        for _ in 0..7 {
            store.append(&make_entry(0)).unwrap();
        }
        assert_eq!(store.latest_sequence(), 7);

        // Give the background thread time to flush segments to disk.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // read_since reads from both on-disk segments and in-memory buffer.
        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(
            entries.len(),
            7,
            "all 7 entries should be readable after async flush"
        );
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[6].seq, 7);
    }

    #[test]
    fn test_local_fs_async_flush_fallback_on_channel_close() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

        // Drop the store's flush channel receiver by replacing the sender
        // with one whose receiver is immediately dropped.
        let (dead_tx, dead_rx) = std::sync::mpsc::channel::<Vec<OpLogRecord>>();
        drop(dead_rx);
        let _old_tx = std::mem::replace(&mut store.flush_tx, dead_tx);

        // Append should trigger the fallback: mpsc::send fails → inline sync flush.
        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap();

        // Verify data was flushed synchronously via the fallback.
        assert_eq!(store.latest_sequence(), 3);
        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_local_fs_snapshot_sequence_and_cleanup_parity() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

        for _ in 0..5 {
            store.append(&make_entry(0)).unwrap();
        }
        store.flush_durable().unwrap();
        assert_eq!(store.max_sequence_id().unwrap(), 5);

        store.record_snapshot_sequence_id("snap1", 3).unwrap();
        assert_eq!(store.get_snapshot_sequence_id("snap1").unwrap(), 3);
        assert!(store.record_snapshot_sequence_id("../bad", 1).is_err());

        store.cleanup_before(4).unwrap();
        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 4);

        let reopened = LocalFsOpLogStore::new(dir.path(), 2).unwrap();
        assert_eq!(reopened.latest_sequence(), 5);
        assert_eq!(reopened.get_snapshot_sequence_id("snap1").unwrap(), 3);
    }

    #[test]
    fn test_manager_append_and_persist_flushes_local_fs() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();
        let mut manager = OpLogManager::new(Some(Box::new(store)), 7);

        let seq = manager.record_remove_durable("k1").unwrap();
        assert_eq!(seq, 1);

        let store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();
        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            decode_record_payload_value(&entries[0].payload).unwrap(),
            json!({"op": "remove", "key": "k1"})
        );
    }

    #[test]
    fn test_oplog_size_validation_matches_cpp_limits() {
        let long_key = "k".repeat(MAX_OBJECT_KEY_SIZE + 1);
        let entry = OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: json!({"op": "remove", "key": long_key}).to_string(),
        };
        assert!(validate_record_size(&entry).is_err());

        let wire = CppOpLogWireEntry {
            sequence_id: 1,
            timestamp_ms: 1,
            op_type: CPP_OP_PUT_END,
            object_key: "k".to_string(),
            payload: "A".repeat((MAX_PAYLOAD_SIZE.div_ceil(3) * 4) + 1),
            checksum: 0,
            prefix_hash: 0,
        };
        assert!(validate_wire_entry_size(&wire).is_err());
    }

    #[test]
    fn test_etcd_oplog_entry_key_matches_cpp_format() {
        let key = EtcdOpLogStore::format_entry_key("/oplog/cluster-a", 42);
        assert_eq!(key, "/oplog/cluster-a/00000000000000000042");
        assert!(!key.contains("seq_"));
    }

    #[test]
    fn test_etcd_oplog_value_writes_cpp_outer_json_for_put_end() {
        let replica = ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "seg-a:1234".to_string(),
            offset: 16,
            size: 100,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: mooncake_store_core::ReplicaType::Memory,
            holder_client_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
        };
        let entry = OpLogRecord {
            seq: 12,
            producer_view_version: 7,
            payload: json!({
                "op": "put_end",
                "key": "k1",
                "size": 100,
                "client_id": null,
                "tenant_id": "default",
                "group_id": "",
                "user_key": "k1",
                "replicas": [replica],
            })
            .to_string(),
        };

        let value = serialize_etcd_oplog_value(&entry).unwrap();
        let wire: CppOpLogWireEntry = serde_json::from_str(&value).unwrap();
        assert_eq!(wire.sequence_id, 12);
        assert_eq!(wire.op_type, CPP_OP_PUT_END);
        assert_eq!(wire.object_key, "k1");
        let decoded = BASE64_STANDARD.decode(&wire.payload).unwrap();
        assert!(decoded.starts_with(PUT_END_MSGPACK_MAGIC));
        assert_eq!(wire.checksum, compute_cpp_checksum(&decoded));
        assert_eq!(wire.prefix_hash, compute_cpp_prefix_hash("k1"));

        let parsed = deserialize_etcd_oplog_value(&value).unwrap();
        assert_eq!(parsed.seq, 12);
        assert_eq!(
            decode_record_payload_value(&parsed.payload).unwrap(),
            decode_record_payload_value(&entry.payload).unwrap()
        );
    }

    #[test]
    fn test_etcd_oplog_value_reads_versioned_msgpack_put_end() {
        let payload = PutEndMetadataPayloadV1 {
            op: "put_end".to_string(),
            key: "k-msgpack".to_string(),
            size: 42,
            client_id: Some(Uuid::new_v4().to_string()),
            tenant_id: "tenant-a".to_string(),
            group_id: "group-a".to_string(),
            user_key: "k-msgpack".to_string(),
            replicas: Vec::new(),
        };
        let bytes = encode_put_end_msgpack(&payload).unwrap();
        let wire = CppOpLogWireEntry {
            sequence_id: 21,
            timestamp_ms: 1,
            op_type: CPP_OP_PUT_END,
            object_key: "k-msgpack".to_string(),
            payload: BASE64_STANDARD.encode(&bytes),
            checksum: compute_cpp_checksum(&bytes),
            prefix_hash: compute_cpp_prefix_hash("k-msgpack"),
        };

        let parsed = deserialize_etcd_oplog_value(&serde_json::to_string(&wire).unwrap()).unwrap();
        assert_eq!(parsed.seq, 21);
        let value = decode_record_payload_value(&parsed.payload).unwrap();
        assert_eq!(value["op"], "put_end");
        assert_eq!(value["key"], "k-msgpack");
        assert_eq!(value["size"], 42);
        assert_eq!(value["tenant_id"], "tenant-a");
        assert_eq!(value["group_id"], "group-a");
    }

    #[test]
    fn test_etcd_oplog_value_rejects_cpp_checksum_mismatch() {
        let wire = CppOpLogWireEntry {
            sequence_id: 8,
            timestamp_ms: 1,
            op_type: CPP_OP_REMOVE,
            object_key: "k-bad".to_string(),
            payload: String::new(),
            checksum: compute_cpp_checksum(b"not-empty"),
            prefix_hash: compute_cpp_prefix_hash("k-bad"),
        };
        let value = serde_json::to_string(&wire).unwrap();

        assert!(matches!(
            deserialize_etcd_oplog_value(&value),
            Err(HaError::InvalidBackend(_))
        ));
    }

    #[test]
    fn test_etcd_oplog_value_reads_cpp_binary_put_end_payload() {
        let binary_payload = vec![0, 159, 146, 1, 2, 3, 255];
        let wire = CppOpLogWireEntry {
            sequence_id: 9,
            timestamp_ms: 1,
            op_type: CPP_OP_PUT_END,
            object_key: "k-binary".to_string(),
            payload: BASE64_STANDARD.encode(&binary_payload),
            checksum: compute_cpp_checksum(&binary_payload),
            prefix_hash: compute_cpp_prefix_hash("k-binary"),
        };
        let value = serde_json::to_string(&wire).unwrap();

        let parsed = deserialize_etcd_oplog_value(&value).unwrap();
        assert_eq!(parsed.seq, 9);
        let payload: serde_json::Value = serde_json::from_str(&parsed.payload).unwrap();
        assert_eq!(payload["op"], "put_end");
        assert_eq!(payload["key"], "k-binary");
        assert_eq!(payload["metadata_payload_base64"], wire.payload);
    }

    #[test]
    fn test_etcd_oplog_value_reads_cpp_remove_and_put_revoke() {
        let remove_wire = CppOpLogWireEntry {
            sequence_id: 3,
            timestamp_ms: 1,
            op_type: CPP_OP_REMOVE,
            object_key: "k-remove".to_string(),
            payload: String::new(),
            checksum: 0,
            prefix_hash: 0,
        };
        let remove_value = serde_json::to_string(&remove_wire).unwrap();
        let remove = deserialize_etcd_oplog_value(&remove_value).unwrap();
        assert_eq!(remove.seq, 3);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&remove.payload).unwrap(),
            json!({"op": "remove", "key": "k-remove"})
        );

        let revoke_wire = CppOpLogWireEntry {
            sequence_id: 4,
            op_type: CPP_OP_PUT_REVOKE,
            object_key: "k-revoke".to_string(),
            ..remove_wire
        };
        let revoke_value = serde_json::to_string(&revoke_wire).unwrap();
        let revoke = deserialize_etcd_oplog_value(&revoke_value).unwrap();
        assert_eq!(revoke.seq, 4);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&revoke.payload).unwrap(),
            json!({"op": "put_revoke", "key": "k-revoke"})
        );
    }
}
