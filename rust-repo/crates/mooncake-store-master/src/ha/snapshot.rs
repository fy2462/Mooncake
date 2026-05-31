use super::types::HaError;
use crate::service::NoFSegmentEntry;
use crate::service::ObjectEntry;
use crate::service::TaskEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use mooncake_store_core::Segment;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

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
        // If cluster_id is specified, use a cluster-specific subdirectory.
        // 如果指定了 cluster_id，使用集群特定的子目录。
        let dir = if cluster_id.is_empty() {
            self.root_dir.clone()
        } else {
            self.root_dir.join(cluster_id)
        };

        // Load segments, NOF segments, objects, and tasks from the backend.
        // 从后端加载 segments、NOF segments、objects 和 tasks。
        let backend = StorageBackend::new(self.backend_type, &dir);
        let Some((segments, nof_segments, objects, tasks)) = backend
            .load()
            .map_err(|error| HaError::Snapshot(error.to_string()))?
        else {
            return Ok(None);
        };

        // Derive snapshot_id from the file modification time if available.
        // 如果可用，从文件修改时间推导 snapshot_id。
        let snapshot_path = dir.join("master_snapshot.json");
        let snapshot_id = std::fs::metadata(&snapshot_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
            .map(|ts| format!("snapshot-{}", ts.as_millis()))
            .unwrap_or_else(|| "snapshot-latest".to_string());

        Ok(Some(LoadedSnapshot {
            snapshot_id,
            snapshot_sequence_id: 0,
            // Extract the Segment domain object from each SegmentEntry wrapper.
            // 从每个 SegmentEntry 封装中提取 Segment 领域对象。
            segments: segments
                .into_iter()
                .map(|s: crate::service::SegmentEntry| s.segment)
                .collect(),
            nof_segments,
            objects,
            tasks,
        }))
    }
}
