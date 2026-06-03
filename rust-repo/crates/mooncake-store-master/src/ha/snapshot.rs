use super::types::HaError;
use crate::service::NoFSegmentEntry;
use crate::service::ObjectEntry;
use crate::service::TaskEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use mooncake_store_core::Segment;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const SNAPSHOT_CATALOG_ROOT: &str = "mooncake_master_snapshot";
const SNAPSHOT_LATEST_FILE: &str = "latest.txt";
const SNAPSHOT_DESCRIPTOR_FILE: &str = "descriptor.txt";
const SNAPSHOT_MANIFEST_FILE: &str = "manifest.txt";

// ----------------------------------------------------------------------------
// LoadedSnapshot — snapshot data loaded during standby recovery
// LoadedSnapshot —— standby 恢复期间加载的快照数据
// ----------------------------------------------------------------------------

/// A fully-loaded snapshot containing all state needed to bootstrap a standby.
/// 已完全加载的快照，包含引导 standby 所需的所有状态。
#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    /// Human-readable snapshot identifier (e.g. "snapshot-1712345678000").
    /// 人类可读的快照标识符。
    pub snapshot_id: String,
    /// Sequence ID at the time the snapshot was taken. / 快照拍摄时的序列 ID。
    pub snapshot_sequence_id: u64,
    /// Memory segments at snapshot time. / 快照时的内存 segment。
    pub segments: Vec<Segment>,
    /// NVMe-oF segments at snapshot time. / 快照时的 NVMe-oF segment。
    pub nof_segments: Vec<NoFSegmentEntry>,
    /// Objects and their replicas at snapshot time. / 快照时的对象及其副本。
    pub objects: Vec<(String, ObjectEntry)>,
    /// Pending tasks at snapshot time. / 快照时的待处理任务。
    pub tasks: Vec<TaskEntry>,
}

/// Catalog descriptor for a published snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDescriptor {
    pub snapshot_id: String,
    pub last_included_seq: u64,
    pub producer_view_version: u64,
    pub manifest_key: String,
    pub object_prefix: String,
    pub created_at_ms: i64,
}

impl SnapshotDescriptor {
    pub fn new(snapshot_id: impl Into<String>) -> Self {
        let snapshot_id = snapshot_id.into();
        let object_prefix = build_snapshot_prefix(&snapshot_id);
        Self {
            manifest_key: format!("{object_prefix}{SNAPSHOT_MANIFEST_FILE}"),
            object_prefix,
            snapshot_id,
            last_included_seq: 0,
            producer_view_version: 0,
            created_at_ms: current_time_ms(),
        }
    }
}

/// C++-compatible snapshot catalog operations.
pub trait SnapshotCatalogStore: Send + Sync {
    fn publish(&self, snapshot: &SnapshotDescriptor) -> Result<(), HaError>;
    fn get_latest(&self) -> Result<Option<SnapshotDescriptor>, HaError>;
    fn list(&self, limit: usize) -> Result<Vec<SnapshotDescriptor>, HaError>;
    fn delete(&self, snapshot_id: &str) -> Result<(), HaError>;
}

/// Embedded catalog store backed by files under the snapshot root.
pub struct EmbeddedSnapshotCatalogStore {
    root_dir: PathBuf,
}

impl EmbeddedSnapshotCatalogStore {
    pub fn new(root_dir: PathBuf) -> Self {
        Self { root_dir }
    }

    fn catalog_path(&self, key: impl AsRef<str>) -> PathBuf {
        self.root_dir.join(key.as_ref())
    }

    fn descriptor_path(&self, snapshot_id: &str) -> PathBuf {
        self.catalog_path(build_descriptor_key(snapshot_id))
    }

    fn latest_path(&self) -> PathBuf {
        self.catalog_path(build_latest_key())
    }
}

impl SnapshotCatalogStore for EmbeddedSnapshotCatalogStore {
    fn publish(&self, snapshot: &SnapshotDescriptor) -> Result<(), HaError> {
        validate_snapshot_id(&snapshot.snapshot_id)?;
        let descriptor_path = self.descriptor_path(&snapshot.snapshot_id);
        if let Some(parent) = descriptor_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| HaError::Snapshot(e.to_string()))?;
        }
        std::fs::write(&descriptor_path, serialize_snapshot_descriptor(snapshot))
            .map_err(|e| HaError::Snapshot(e.to_string()))?;
        if let Some(parent) = self.latest_path().parent() {
            std::fs::create_dir_all(parent).map_err(|e| HaError::Snapshot(e.to_string()))?;
        }
        std::fs::write(self.latest_path(), &snapshot.snapshot_id)
            .map_err(|e| HaError::Snapshot(e.to_string()))?;
        Ok(())
    }

    fn get_latest(&self) -> Result<Option<SnapshotDescriptor>, HaError> {
        let latest_path = self.latest_path();
        if !latest_path.exists() {
            return Ok(None);
        }
        let snapshot_id = std::fs::read_to_string(latest_path)
            .map_err(|e| HaError::Snapshot(e.to_string()))?
            .trim()
            .to_string();
        if snapshot_id.is_empty() {
            return Ok(None);
        }
        validate_snapshot_id(&snapshot_id)?;
        let payload = std::fs::read_to_string(self.descriptor_path(&snapshot_id))
            .map_err(|e| HaError::Snapshot(e.to_string()))?;
        deserialize_snapshot_descriptor(&snapshot_id, &payload).map(Some)
    }

    fn list(&self, limit: usize) -> Result<Vec<SnapshotDescriptor>, HaError> {
        let root = self.catalog_path(SNAPSHOT_CATALOG_ROOT);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(root).map_err(|e| HaError::Snapshot(e.to_string()))? {
            let entry = entry.map_err(|e| HaError::Snapshot(e.to_string()))?;
            if !entry
                .file_type()
                .map_err(|e| HaError::Snapshot(e.to_string()))?
                .is_dir()
            {
                continue;
            }
            let id = entry.file_name().to_string_lossy().to_string();
            if is_valid_snapshot_id(&id) {
                ids.push(id);
            }
        }
        ids.sort_by(|a, b| b.cmp(a));
        if limit != 0 {
            ids.truncate(limit);
        }

        let mut snapshots = Vec::new();
        for id in ids {
            if let Ok(payload) = std::fs::read_to_string(self.descriptor_path(&id)) {
                if let Ok(descriptor) = deserialize_snapshot_descriptor(&id, &payload) {
                    snapshots.push(descriptor);
                }
            }
        }
        Ok(snapshots)
    }

    fn delete(&self, snapshot_id: &str) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        let deletes_latest = self
            .get_latest()?
            .as_ref()
            .map(|latest| latest.snapshot_id.as_str() == snapshot_id)
            .unwrap_or(false);
        let prefix = self.catalog_path(build_snapshot_prefix(snapshot_id));
        if prefix.exists() {
            std::fs::remove_dir_all(prefix).map_err(|e| HaError::Snapshot(e.to_string()))?;
        }
        if deletes_latest {
            let _ = std::fs::remove_file(self.latest_path());
        }
        Ok(())
    }
}

fn build_snapshot_prefix(snapshot_id: &str) -> String {
    format!("{SNAPSHOT_CATALOG_ROOT}/{snapshot_id}/")
}

fn build_descriptor_key(snapshot_id: &str) -> String {
    format!(
        "{}{SNAPSHOT_DESCRIPTOR_FILE}",
        build_snapshot_prefix(snapshot_id)
    )
}

fn build_latest_key() -> String {
    format!("{SNAPSHOT_CATALOG_ROOT}/{SNAPSHOT_LATEST_FILE}")
}

fn serialize_snapshot_descriptor(descriptor: &SnapshotDescriptor) -> String {
    format!(
        "{}|{}|{}",
        descriptor.last_included_seq, descriptor.producer_view_version, descriptor.created_at_ms
    )
}

fn deserialize_snapshot_descriptor(
    snapshot_id: &str,
    payload: &str,
) -> Result<SnapshotDescriptor, HaError> {
    let mut parts = payload.trim().split('|');
    let last_included_seq = parts
        .next()
        .ok_or_else(|| HaError::Snapshot("snapshot descriptor missing sequence".into()))?
        .parse::<u64>()
        .map_err(|e| HaError::Snapshot(format!("invalid snapshot sequence: {e}")))?;
    let producer_view_version = parts
        .next()
        .ok_or_else(|| HaError::Snapshot("snapshot descriptor missing view version".into()))?
        .parse::<u64>()
        .map_err(|e| HaError::Snapshot(format!("invalid snapshot view version: {e}")))?;
    let created_at_ms = parts
        .next()
        .ok_or_else(|| HaError::Snapshot("snapshot descriptor missing creation time".into()))?
        .parse::<i64>()
        .map_err(|e| HaError::Snapshot(format!("invalid snapshot creation time: {e}")))?;
    if parts.next().is_some() {
        return Err(HaError::Snapshot(
            "snapshot descriptor has too many fields".into(),
        ));
    }

    let mut descriptor = SnapshotDescriptor::new(snapshot_id);
    descriptor.last_included_seq = last_included_seq;
    descriptor.producer_view_version = producer_view_version;
    descriptor.created_at_ms = created_at_ms;
    Ok(descriptor)
}

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), HaError> {
    if is_valid_snapshot_id(snapshot_id) {
        Ok(())
    } else {
        Err(HaError::InvalidParams(format!(
            "invalid snapshot id: {snapshot_id}"
        )))
    }
}

fn is_valid_snapshot_id(snapshot_id: &str) -> bool {
    let bytes = snapshot_id.as_bytes();
    if bytes.len() != 19 {
        return false;
    }
    bytes.iter().enumerate().all(|(i, ch)| {
        if i == 8 || i == 15 {
            *ch == b'_'
        } else {
            ch.is_ascii_digit()
        }
    })
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ----------------------------------------------------------------------------
// SnapshotProvider — trait for loading snapshots
// SnapshotProvider —— 加载快照的 trait
//
// Different backends implement snapshot storage differently:
// - NoopSnapshotProvider: always returns None (no snapshots).
// - LocalSnapshotProvider: reads from local disk via StorageBackend.
//
// 不同的后端以不同方式实现快照存储：
// - NoopSnapshotProvider 始终返回 None（无快照）。
// - LocalSnapshotProvider 通过 StorageBackend 从本地磁盘读取。
// ----------------------------------------------------------------------------

/// Trait for loading snapshots during standby bootstrap.
/// standby 引导期间加载快照的 trait。
pub trait SnapshotProvider: Send + Sync {
    /// Load the latest snapshot for the given cluster.
    /// 加载给定集群的最新快照。
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError>;
}

/// No-op snapshot provider: always returns None.
/// 空操作快照提供者：始终返回 None。
pub struct NoopSnapshotProvider;

impl SnapshotProvider for NoopSnapshotProvider {
    fn load_latest_snapshot(&self, _cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        Ok(None)
    }
}

/// Local disk snapshot provider: reads master state from a directory.
/// 本地磁盘快照提供者：从目录读取 master 状态。
pub struct LocalSnapshotProvider {
    /// Root directory for snapshot files. / 快照文件的根目录。
    root_dir: PathBuf,
    /// Storage format backend type (e.g. JSON, binary). / 存储格式后端类型。
    backend_type: StorageBackendType,
}

impl LocalSnapshotProvider {
    pub fn new(root_dir: PathBuf, backend_type: StorageBackendType) -> Self {
        Self {
            root_dir,
            backend_type,
        }
    }
}

impl SnapshotProvider for LocalSnapshotProvider {
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        let mut dirs = Vec::new();
        if cluster_id.is_empty() {
            dirs.push(self.root_dir.clone());
        } else {
            dirs.push(self.root_dir.join(cluster_id));
            dirs.push(self.root_dir.clone());
        }

        for dir in dirs {
            // Load segments, NOF segments, objects, and tasks from the backend.
            // 从后端加载 segments、NOF segments、objects 和 tasks。
            let backend = StorageBackend::new(self.backend_type, &dir);
            let Some((segments, nof_segments, objects, tasks)) = backend
                .load()
                .map_err(|error| HaError::Snapshot(error.to_string()))?
            else {
                continue;
            };

            // Derive snapshot_id from the file modification time if available.
            // 如果可用，从文件修改时间推导 snapshot_id。
            let snapshot_path = ["master_snapshot.msgpack", "master_snapshot.json"]
                .iter()
                .map(|name| dir.join(name))
                .find(|path| path.exists());
            let snapshot_id = snapshot_path
                .as_ref()
                .and_then(|path| std::fs::metadata(path).ok())
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
                .map(|ts| format!("snapshot-{}", ts.as_millis()))
                .unwrap_or_else(|| "snapshot-latest".to_string());
            let descriptor = EmbeddedSnapshotCatalogStore::new(dir.clone())
                .get_latest()
                .ok()
                .flatten();
            let snapshot_sequence_id = descriptor
                .as_ref()
                .map(|descriptor| descriptor.last_included_seq)
                .unwrap_or(0);
            let snapshot_id = descriptor
                .map(|descriptor| descriptor.snapshot_id)
                .unwrap_or(snapshot_id);

            return Ok(Some(LoadedSnapshot {
                snapshot_id,
                snapshot_sequence_id,
                // Extract the Segment domain object from each SegmentEntry wrapper.
                // 从每个 SegmentEntry 封装中提取 Segment 领域对象。
                segments: segments
                    .into_iter()
                    .map(|s: crate::service::SegmentEntry| s.segment)
                    .collect(),
                nof_segments,
                objects,
                tasks,
            }));
        }

        Ok(None)
    }
}
