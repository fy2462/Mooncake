// =============================================================================
// Storage Backend — 存储后端抽象
// =============================================================================
// Provides snapshot persistence and per-key file storage with support for
// different storage backends: local disk, HF3FS (3FS distributed filesystem),
// and FilePerKey (offload storage).
// 提供快照持久化和按 key 的文件存储，支持不同的存储后端：
// 本地磁盘、HF3FS（3FS 分布式文件系统）和 FilePerKey（offload 存储）。
//
// Architecture / 架构:
// ┌──────────────────────────────────────────────────────┐
// │  StorageBackend                                       │
// │  ┌──────────────┐  ┌────────────────────────────────┐ │
// │  │ Snapshot      │  │ FilePerKey                     │ │
// │  │ (msgpack/json)│  │ (key → file on disk)           │ │
// │  │ save/load     │  │ batch_offload / batch_load     │ │
// │  │ Segments,     │  │ remove / remove_by_regex       │ │
// │  │ Objects,      │  │ scan_meta                      │ │
// │  │ Tasks         │  │                                │ │
// │  └──────────────┘  └────────────────────────────────┘ │
// └──────────────────────────────────────────────────────┘
//
// Backend Types / 后端类型:
// - LocalDisk: plain local filesystem, no special handling needed.
//   LocalDisk：普通本地磁盘，无需额外注册。
// - Hf3fs: 3FS distributed filesystem, requires fd registration via hf3fs API.
//   Hf3fs：3FS 分布式文件系统，需要通过 hf3fs API 注册文件描述符。
// - FilePerKey: each key stored as a separate file (used for memory offloading).
//   FilePerKey：每个 key 独立文件存储（用于 offload 场景）。

use crate::hf3fs;
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Snapshot storage backend types.
/// 快照存储后端类型：
/// - LocalDisk：普通本地磁盘，无需额外注册
/// - Hf3fs：3FS 分布式文件系统，需要通过 hf3fs::register_fd 注册文件描述符
/// - FilePerKey：每个 key 独立文件存储（用于 offload 场景）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendType {
    /// Standard local filesystem — no special handling needed.
    /// 标准本地文件系统 —— 无需特殊处理。
    LocalDisk,
    /// HF3FS (3FS) distributed filesystem — requires fd registration.
    /// HF3FS（3FS）分布式文件系统 —— 需要 fd 注册。
    Hf3fs,
    /// File-per-key mode: each key is stored as a separate file.
    /// 每个 key 独立文件模式：每个 key 存储为单独的文件。
    FilePerKey,
}

// =============================================================================
// Snapshot Serialization Types / 快照序列化类型
// =============================================================================
// These types mirror the domain types but use serialization-friendly
// representations (e.g., String IDs instead of Uuid, i32 enums, etc.).
// 这些类型镜像领域类型，但使用利于序列化的表示（如 String ID 代替 Uuid，i32 枚举等）。

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotSegment {
    id: String,
    name: String,
    size: u64,
    used: u64,
    client_id: String,
    #[serde(default)]
    status: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotNoFSegment {
    id: String,
    name: String,
    base: u64,
    size: u64,
    te_endpoint: String,
    client_id: String,
    used: u64,
    #[serde(default)]
    status: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotObject {
    object: crate::service::ObjectEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotTask {
    id: String,
    task_type: i32,
    status: i32,
    created_at_ms: i64,
    last_updated_at_ms: i64,
    assigned_client: Option<String>,
    message: String,
    key: String,
    payload: String,
}

/// Top-level snapshot structure for serialization.
/// 顶层快照结构，用于序列化。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    segments: Vec<SnapshotSegment>,
    nof_segments: Vec<SnapshotNoFSegment>,
    objects: Vec<(String, SnapshotObject)>,
    tasks: Vec<(String, SnapshotTask)>,
}

// =============================================================================
// BackendFile — RAII wrapper with optional HF3FS registration
// =============================================================================

/// The main storage backend for snapshot persistence and key-based file operations.
/// 用于快照持久化和基于 key 的文件操作的主存储后端。
pub struct StorageBackend {
    backend_type: StorageBackendType,
    disk_dir: PathBuf,
}

/// Wraps fs::File with optional HF3FS fd registration for RAII cleanup.
/// 封装 fs::File，针对 Hf3fs 后端额外持有 fd 注册句柄，防止文件被 3FS 提前回收。
///
/// For HF3FS backend, holds an Hf3fsRegistration to keep the fd valid with
/// the 3FS client. For LocalDisk/FilePerKey, registration is None.
/// 对于 HF3FS 后端，持有 Hf3fsRegistration 以保持 fd 对 3FS 客户端有效。
/// 对于 LocalDisk/FilePerKey，registration 为 None。
struct BackendFile {
    file: fs::File,
    /// RAII guard: holding this keeps the 3FS fd valid until drop.
    /// RAII 守卫：持有此对象保证 3FS fd 在 drop 前有效。
    _hf3fs_registration: Option<hf3fs::Hf3fsRegistration>,
}

impl BackendFile {
    /// Create a new file for writing, registering with HF3FS if needed.
    /// 创建新文件用于写入，必要时注册 HF3FS。
    fn create(
        path: &Path,
        backend_type: StorageBackendType,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = fs::File::create(path)?;
        let registration = match backend_type {
            StorageBackendType::LocalDisk | StorageBackendType::FilePerKey => None,
            StorageBackendType::Hf3fs => Some(hf3fs::register_fd(file.as_raw_fd())?),
        };
        Ok(Self {
            file,
            _hf3fs_registration: registration,
        })
    }

    /// Open an existing file for reading, registering with HF3FS if needed.
    /// 打开已有文件用于读取，必要时注册 HF3FS。
    fn open(
        path: &Path,
        backend_type: StorageBackendType,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = fs::File::open(path)?;
        let registration = match backend_type {
            StorageBackendType::LocalDisk | StorageBackendType::FilePerKey => None,
            StorageBackendType::Hf3fs => Some(hf3fs::register_fd(file.as_raw_fd())?),
        };
        Ok(Self {
            file,
            _hf3fs_registration: registration,
        })
    }

    /// Flush buffered data and fsync to disk.
    /// 刷新缓冲数据并 fsync 到磁盘。
    fn sync_all(&self) -> Result<(), std::io::Error> {
        self.file.sync_all()
    }
}

impl Read for BackendFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.file.read(buf)
    }
}

impl Write for BackendFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.file.flush()
    }
}

// =============================================================================
// StorageBackend Methods / StorageBackend 方法
// =============================================================================

impl StorageBackend {
    /// Create a new StorageBackend, ensuring the base directory exists.
    /// 创建新的 StorageBackend，确保基础目录存在。
    pub fn new(backend_type: StorageBackendType, disk_dir: &Path) -> Self {
        fs::create_dir_all(disk_dir).ok();
        Self {
            backend_type,
            disk_dir: disk_dir.to_path_buf(),
        }
    }

    /// Save a snapshot of the current master state.
    /// 保存当前 master 状态的快照。
    ///
    /// Serializes segments, nof_segments, objects, and tasks into msgpack format.
    /// 将 segments、nof_segments、objects 和 tasks 序列化为 msgpack 格式。
    ///
    /// Uses atomic write pattern:
    /// 使用原子写模式：
    /// 1. Write to .tmp file with buffered I/O.
    ///    写入 .tmp 文件（带缓冲 I/O）。
    /// 2. Flush + fsync the data.
    ///    刷新 + fsync 数据。
    /// 3. Atomically rename .tmp → final filename.
    ///    原子 rename .tmp → 最终文件名。
    /// 使用原子写模式：先写 .tmp 文件，sync + rename 到最终文件名，
    /// 防止写过程中崩溃导致数据损坏。
    pub fn save(
        &self,
        segments: &DashMap<Uuid, crate::service::SegmentEntry>,
        nof_segments: &DashMap<Uuid, crate::service::NoFSegmentEntry>,
        objects: &DashMap<String, crate::service::ObjectEntry>,
        tasks: &DashMap<Uuid, crate::service::TaskEntry>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Convert domain types to serializable snapshot types
        // 将领域类型转换为可序列化的快照类型
        let snap = Snapshot {
            segments: segments
                .iter()
                .map(|entry| SnapshotSegment {
                    id: entry.segment.id.to_string(),
                    name: entry.segment.name.clone(),
                    size: entry.segment.size,
                    used: entry.used,
                    client_id: entry.client_id.to_string(),
                    status: entry.status as i32,
                })
                .collect(),
            nof_segments: nof_segments
                .iter()
                .map(|entry| SnapshotNoFSegment {
                    id: entry.segment.id.to_string(),
                    name: entry.segment.name.clone(),
                    base: entry.segment.base,
                    size: entry.segment.size,
                    te_endpoint: entry.segment.te_endpoint.clone(),
                    client_id: entry.segment.client_id.to_string(),
                    used: entry.used,
                    status: entry.status as i32,
                })
                .collect(),
            objects: objects
                .iter()
                .map(|entry| {
                    let obj = SnapshotObject {
                        object: entry.clone(),
                    };
                    (entry.key().clone(), obj)
                })
                .collect(),
            tasks: tasks
                .iter()
                .map(|entry| {
                    let info = &entry.info;
                    let id_str = info.id.to_string();
                    let task = SnapshotTask {
                        id: id_str.clone(),
                        task_type: info.task_type as i32,
                        status: info.status as i32,
                        created_at_ms: info.created_at.timestamp_millis(),
                        last_updated_at_ms: info.last_updated_at.timestamp_millis(),
                        assigned_client: info.assigned_client.map(|id| id.to_string()),
                        message: info.message.clone(),
                        key: entry.key.clone(),
                        payload: entry.payload.clone(),
                    };
                    (id_str, task)
                })
                .collect(),
        };

        let path = self.disk_dir.join("master_snapshot.msgpack");
        let tmp = self.disk_dir.join("master_snapshot.msgpack.tmp");

        // Phase 1: write to temporary file
        // 先写入临时文件，完成后再原子 rename（避免中途崩溃产生损坏的快照）
        let writer = BackendFile::create(&tmp, self.backend_type)?;
        let mut writer = BufWriter::new(writer);
        rmp_serde::encode::write_named(&mut writer, &snap)?;
        writer.flush()?;
        // Phase 2: fsync to guarantee durability
        // fsync 保证数据落盘
        writer.get_ref().sync_all()?;
        drop(writer); // Close the file handle / 关闭文件句柄
        // Phase 3: atomic rename
        // 原子替换
        fs::rename(&tmp, &path)?;

        tracing::info!(
            "Snapshot saved to {} via {:?} ({} segments, {} objects)",
            path.display(),
            self.backend_type,
            snap.segments.len() + snap.nof_segments.len(),
            snap.objects.len()
        );
        Ok(())
    }

    /// Convert deserialized Snapshot types back to domain types.
    /// 将反序列化的 Snapshot 转换为领域类型（SegmentEntry / NoFSegmentEntry / ObjectEntry / TaskEntry）。
    ///
    /// Handles type differences between serialized and domain representations:
    /// 处理序列化/反序列化之间的类型差异：
    /// - UUID string ↔ Uuid
    /// - i32 ↔ enum (SegmentStatus, TaskType, TaskStatus)
    /// - millisecond timestamps ↔ chrono::DateTime
    fn build_loaded_state(
        snap: Snapshot,
        backend_type: StorageBackendType,
        path: &Path,
    ) -> (
        Vec<crate::service::SegmentEntry>,
        Vec<crate::service::NoFSegmentEntry>,
        Vec<(String, crate::service::ObjectEntry)>,
        Vec<crate::service::TaskEntry>,
    ) {
        // Deserialize memory segments
        let segments: Vec<crate::service::SegmentEntry> = snap
            .segments
            .into_iter()
            .map(|s| {
                let status = match s.status {
                    1 => crate::proto::SegmentStatus::Active,
                    2 => crate::proto::SegmentStatus::Draining,
                    3 => crate::proto::SegmentStatus::Unavailable,
                    _ => crate::proto::SegmentStatus::Active,
                };
                crate::service::SegmentEntry {
                    segment: mooncake_store_core::Segment {
                        id: Uuid::parse_str(&s.id).unwrap_or_else(|_| Uuid::new_v4()),
                        name: s.name,
                        base: 0,
                        size: s.size,
                        te_endpoint: String::new(),
                        protocol: String::new(),
                    },
                    used: s.used,
                    client_id: Uuid::parse_str(&s.client_id).unwrap_or_else(|_| Uuid::new_v4()),
                    status,
                }
            })
            .collect();

        // Deserialize NoF (file) segments
        let nof_segments: Vec<crate::service::NoFSegmentEntry> = snap
            .nof_segments
            .into_iter()
            .map(|s| crate::service::NoFSegmentEntry {
                segment: mooncake_store_core::NoFSegment {
                    id: Uuid::parse_str(&s.id).unwrap_or_else(|_| Uuid::new_v4()),
                    name: s.name,
                    base: s.base,
                    size: s.size,
                    te_endpoint: s.te_endpoint,
                    client_id: Uuid::parse_str(&s.client_id).unwrap_or_else(|_| Uuid::new_v4()),
                },
                used: s.used,
                status: match s.status {
                    1 => crate::proto::SegmentStatus::Active,
                    2 => crate::proto::SegmentStatus::Draining,
                    3 => crate::proto::SegmentStatus::Unavailable,
                    _ => crate::proto::SegmentStatus::Active,
                },
            })
            .collect();

        // Deserialize objects
        let objects: Vec<(String, crate::service::ObjectEntry)> = snap
            .objects
            .into_iter()
            .map(|(key, obj)| (key, obj.object))
            .collect();

        // Deserialize tasks
        let tasks: Vec<crate::service::TaskEntry> = snap
            .tasks
            .into_iter()
            .map(|(_id_str, t)| crate::service::TaskEntry {
                info: TaskInfo {
                    id: Uuid::parse_str(&t.id).unwrap_or_else(|_| Uuid::new_v4()),
                    task_type: match t.task_type {
                        1 => TaskType::ReplicaCopy,
                        2 => TaskType::ReplicaMove,
                        _ => TaskType::ReplicaCopy,
                    },
                    status: match t.status {
                        1 => TaskStatus::Processing,
                        2 => TaskStatus::Success,
                        3 => TaskStatus::Failed,
                        _ => TaskStatus::Pending,
                    },
                    created_at: chrono::DateTime::from_timestamp_millis(t.created_at_ms)
                        .unwrap_or_else(Utc::now),
                    last_updated_at: chrono::DateTime::from_timestamp_millis(t.last_updated_at_ms)
                        .unwrap_or_else(Utc::now),
                    assigned_client: t.assigned_client.and_then(|id| Uuid::parse_str(&id).ok()),
                    message: t.message,
                },
                key: t.key,
                payload: t.payload,
                max_retry_attempts: 3,
            })
            .collect();

        tracing::info!(
            "Snapshot loaded from {} via {:?} ({} segments, {} objects, {} tasks)",
            path.display(),
            backend_type,
            segments.len() + nof_segments.len(),
            objects.len(),
            tasks.len()
        );

        (segments, nof_segments, objects, tasks)
    }

    /// Load a snapshot from disk.
    /// 加载快照：优先尝试 msgpack 格式（新），不存在时回退到 JSON 格式（旧版兼容）。
    ///
    /// Tries msgpack format first (new format), falls back to JSON (legacy compatibility).
    /// Returns restored segments, nof_segments, objects, and tasks.
    /// 返回恢复的 segments、nof_segments、objects 和 tasks。
    ///
    /// Returns None if no snapshot file exists.
    /// 若不存在快照文件，返回 None。
    pub fn load(
        &self,
    ) -> Result<
        Option<(
            Vec<crate::service::SegmentEntry>,
            Vec<crate::service::NoFSegmentEntry>,
            Vec<(String, crate::service::ObjectEntry)>,
            Vec<crate::service::TaskEntry>,
        )>,
        Box<dyn std::error::Error>,
    > {
        // Try msgpack first (new format), then fall back to JSON (legacy)
        // 优先尝试 msgpack（新格式），不存在则回退到 JSON（旧版兼容）
        let msgpack_path = self.disk_dir.join("master_snapshot.msgpack");
        if msgpack_path.exists() {
            let reader = BufReader::new(BackendFile::open(&msgpack_path, self.backend_type)?);
            let snap: Snapshot = rmp_serde::decode::from_read(reader)?;
            let (segments, nof_segments, objects, tasks) =
                Self::build_loaded_state(snap, self.backend_type, &msgpack_path);
            return Ok(Some((segments, nof_segments, objects, tasks)));
        }

        // Fall back to legacy JSON format
        // 回退到旧版 JSON 格式
        let json_path = self.disk_dir.join("master_snapshot.json");
        if !json_path.exists() {
            return Ok(None);
        }

        let reader = BufReader::new(BackendFile::open(&json_path, self.backend_type)?);
        let snap: Snapshot = serde_json::from_reader(reader)?;
        let (segments, nof_segments, objects, tasks) =
            Self::build_loaded_state(snap, self.backend_type, &json_path);
        Ok(Some((segments, nof_segments, objects, tasks)))
    }

    /// Clear all snapshot files.
    /// 清除所有快照文件。
    pub fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        for filename in &["master_snapshot.msgpack", "master_snapshot.json"] {
            let path = self.disk_dir.join(filename);
            if path.exists() {
                fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    // =========================================================================
    // FilePerKey Methods — per-key file operations
    // FilePerKey 方法 —— 按 key 的文件操作
    // =========================================================================

    /// Directory for per-key files ("keys" subdirectory).
    /// 按 key 的文件目录（"keys" 子目录）。
    fn key_dir(&self) -> PathBuf {
        self.disk_dir.join("keys")
    }

    /// Compute the safe file path for a given key.
    /// 计算给定 key 的安全文件路径。
    ///
    /// Replaces '/' with '_' to prevent path traversal attacks.
    /// 编码 key：将 '/' 替换为 '_'，防止路径穿越攻击。
    fn key_path(&self, key: &str) -> PathBuf {
        let safe_key = key.replace('/', "_");
        self.key_dir().join(safe_key)
    }

    /// Batch offload: write multiple key-value pairs to disk as individual files.
    /// 批量下沉：将多个 key 的二进制数据写入独立文件。
    ///
    /// Used for offloading hot data from memory to local disk.
    /// 用于将热数据从内存 offload 到本地磁盘。
    pub fn batch_offload(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = self.key_dir();
        std::fs::create_dir_all(&dir)?;
        for (key, value) in entries {
            let path = self.key_path(key);
            let mut f = std::fs::File::create(&path)?;
            f.write_all(value)?;
        }
        Ok(())
    }

    /// Batch load: read multiple keys from disk.
    /// 批量加载：从磁盘读取多个 key 的二进制数据。
    ///
    /// Missing keys are silently skipped (no error).
    /// 不存在的 key 直接跳过，不报错。
    pub fn batch_load(
        &self,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();
        for key in keys {
            let path = self.key_path(key);
            if path.exists() {
                let mut f = std::fs::File::open(&path)?;
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                results.push((key.clone(), buf));
            }
        }
        Ok(results)
    }

    /// Remove specific keys from disk.
    /// 从磁盘删除特定 key。
    pub fn remove_keys(&self, keys: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        for key in keys {
            let path = self.key_path(key);
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    /// Check if a key exists on disk.
    /// 检查 key 是否存在于磁盘上。
    pub fn is_exist(&self, key: &str) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.key_path(key).exists())
    }

    /// Remove all keys matching a regex pattern.
    /// 删除所有匹配正则表达式的 key。
    pub fn remove_by_regex(&self, pattern: &str) -> Result<usize, Box<dyn std::error::Error>> {
        let dir = self.key_dir();
        if !dir.exists() {
            return Ok(0);
        }
        let re = regex::Regex::new(pattern).map_err(|e| format!("invalid regex: {e}"))?;
        let mut count = 0;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if re.is_match(&name) {
                std::fs::remove_file(entry.path())?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Remove all per-key files.
    /// 删除所有按 key 的文件。
    pub fn remove_all(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let dir = self.key_dir();
        if !dir.exists() {
            return Ok(0);
        }
        let count = std::fs::read_dir(&dir)?.count();
        std::fs::remove_dir_all(&dir)?;
        std::fs::create_dir_all(&dir)?;
        Ok(count)
    }

    /// Scan metadata for all per-key files: return (key, size) pairs.
    /// 扫描所有按 key 的文件的元数据：返回 (key, size) 对。
    pub fn scan_meta(&self) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        let dir = self.key_dir();
        if !dir.exists() {
            return Ok(vec![]);
        }
        let mut results = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let meta = entry.metadata()?;
            results.push((name, meta.len()));
        }
        Ok(results)
    }
}
