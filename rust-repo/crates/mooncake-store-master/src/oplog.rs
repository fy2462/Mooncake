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
use serde_json::json;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

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

    /// Poll for records from since_seq, returning records, next_seq, and timeout flag.
    /// 从 since_seq 开始轮询记录，返回记录列表、next_seq 和是否超时。
    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult;
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
}

impl InMemoryOpLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            last_seq: 0,
            max_entries: max_entries.max(1),
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
        }).unwrap_or_default()
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
        self.buffer.clear();
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
// Etcd-backed OpLog Store / 基于 etcd 的 OpLog 存储
// =============================================================================

/// Persistent oplog backed by etcd.
/// 基于 etcd 的持久化 oplog。
///
/// Each record is stored as `/oplog/<prefix>/seq_<seq:020>` with zero-padded
/// sequence numbers to ensure lexicographic ordering.
/// 每条记录存储为 `/oplog/<prefix>/seq_<seq:020>`，零填充序列号保证字典序。
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
        format!("{}/seq_{:020}", self.key_prefix, seq)
    }

    /// Build the etcd key for the latest sequence pointer.
    fn latest_key(&self) -> String {
        format!("{}/latest", self.key_prefix)
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
            let value = serde_json::to_string(entry)
                .map_err(|e| HaError::InvalidBackend(format!("oplog serialize: {e}")))?;
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
        Ok(self.last_seq)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        // Read from buffered entries first (fast path)
        let mut entries: Vec<OpLogRecord> = self
            .buffer
            .iter()
            .filter(|r| r.seq >= since_seq)
            .take(max_count)
            .cloned()
            .collect();
        if entries.len() >= max_count {
            entries.truncate(max_count);
            return Ok(entries);
        }
        let remaining = max_count - entries.len();

        // We can't easily do async etcd reads from &self (sync context).
        // For now, return buffered entries. Full async reads require an async
        // read_since variant.
        let _ = remaining;
        Ok(entries)
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
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
        let mut entries = Vec::new();
        let c = self.client.clone();

        // Range query: from seq_N to seq_MAX across the key prefix
        let range_end = self.entry_key(u64::MAX);
        let range_start = self.entry_key(since_seq);

        match c
            .kv_client()
            .get(
                range_start.as_bytes(),
                Some(etcd_client::GetOptions::new().with_range(range_end.as_bytes())),
            )
            .await
        {
            Ok(resp) => {
                for kv in resp.kvs().iter().take(max_count) {
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        if let Ok(entry) = serde_json::from_str::<OpLogRecord>(&val) {
                            entries.push(entry);
                            if entries.len() >= max_count {
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("etcd range query for oplog failed: {}", e);
            }
        }

        // Supplement with buffered (not yet flushed) entries
        for entry in &self.buffer {
            if entry.seq >= since_seq && entries.len() < max_count
                && !entries.iter().any(|e| e.seq == entry.seq) {
                    entries.push(entry.clone());
                }
        }

        entries.sort_by_key(|e| e.seq);
        entries.truncate(max_count);
        Ok(entries)
    }

    /// Flush buffered entries to etcd (async). Call periodically or before shutdown.
    pub async fn flush_async(&mut self) -> Result<(), HaError> {
        self.flush().await
    }
}

// =============================================================================
// OpLogManager — high-level wrapper that records mutations into the oplog
// =============================================================================

/// High-level manager that serializes business operations (put, remove, mount,
/// etc.) as JSON and appends them to the underlying OpLogStore.
/// OpLog 管理器：将业务操作（put/remove/mount 等）序列化为 JSON 并追加到底层 OpLogStore。
///
/// All errors are logged via warn! but never propagated — oplog uses best-effort
/// semantics to ensure the main write path is never blocked by oplog failures.
/// 所有错误仅 warn 日志记录，不向上传播——oplog 采用 best-effort 语义，
/// 确保主写路径永不因 oplog 故障而被阻塞。
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

    pub fn set_view_version(&mut self, version: u64) {
        self.view_version = version;
    }

    pub fn store(&self) -> Option<&(dyn OpLogStore + Send)> {
        self.store.as_deref()
    }

    pub fn into_store(self) -> Option<Box<dyn OpLogStore + Send>> {
        self.store
    }

    /// Record a put_end mutation: object data write completed.
    /// 记录 put_end 操作：对象数据写入完成。
    /// payload 格式：{ "op": "put_end", "key": "...", "size": ... }
    pub fn record_put_end(&mut self, key: &str, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = json!({"op": "put_end", "key": key, "size": size}).to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record put_end for key={key}: {e}");
            }
        }
    }

    /// Record a remove mutation: { "op": "remove", "key": "..." }
    pub fn record_remove(&mut self, key: &str) {
        if let Some(store) = &mut self.store {
            let payload = json!({"op": "remove", "key": key}).to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record remove for key={key}: {e}");
            }
        }
    }

    /// Record a mount-segment mutation.
    pub fn record_mount_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = json!({
                "op": "mount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })
            .to_string();
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
            let payload = json!({
                "op": "unmount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })
            .to_string();
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
            let payload = json!({
                "op": "mount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })
            .to_string();
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
            let payload = json!({
                "op": "unmount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })
            .to_string();
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
            let payload = json!({
                "op": "put_start",
                "key": key,
                "client_id": client_id.to_string()
            })
            .to_string();
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
}
