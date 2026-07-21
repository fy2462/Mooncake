use super::storage_backend_file::BackendFile;
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufReader;

fn invalid_snapshot_data(message: impl Into<String>) -> Box<dyn std::error::Error> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn parse_snapshot_uuid(value: &str, field: &str) -> Result<Uuid, Box<dyn std::error::Error>> {
    Uuid::parse_str(value)
        .map_err(|error| invalid_snapshot_data(format!("invalid {field} '{value}': {error}")))
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
    #[serde(default)]
    base: u64,
    size: u64,
    #[serde(default)]
    te_endpoint: String,
    #[serde(default)]
    protocol: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDiskSnapshotEntry {
    pub client_id: Uuid,
    pub enable_offloading: bool,
    pub offloading_objects: HashMap<String, i64>,
    pub ssd_total_capacity_bytes: i64,
}

/// Top-level snapshot structure for serialization.
/// 顶层快照结构，用于序列化。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    segments: Vec<SnapshotSegment>,
    nof_segments: Vec<SnapshotNoFSegment>,
    objects: Vec<(String, SnapshotObject)>,
    tasks: Vec<(String, SnapshotTask)>,
    #[serde(default)]
    local_disk_segments: Vec<LocalDiskSnapshotEntry>,
}

// =============================================================================
// BackendFile — RAII wrapper with optional HF3FS registration
// =============================================================================

impl StorageBackend {
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
        self.save_with_local_disk(segments, nof_segments, objects, tasks, &DashMap::new())
    }

    pub fn save_with_local_disk(
        &self,
        segments: &DashMap<Uuid, crate::service::SegmentEntry>,
        nof_segments: &DashMap<Uuid, crate::service::NoFSegmentEntry>,
        objects: &DashMap<String, crate::service::ObjectEntry>,
        tasks: &DashMap<Uuid, crate::service::TaskEntry>,
        local_disk_segments: &DashMap<Uuid, LocalDiskSnapshotEntry>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Convert domain types to serializable snapshot types
        // 将领域类型转换为可序列化的快照类型
        let snap = Snapshot {
            segments: segments
                .iter()
                .map(|entry| SnapshotSegment {
                    id: entry.segment.id.to_string(),
                    name: entry.segment.name.clone(),
                    base: entry.segment.base,
                    size: entry.segment.size,
                    te_endpoint: entry.segment.te_endpoint.clone(),
                    protocol: entry.segment.protocol.clone(),
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
            local_disk_segments: local_disk_segments
                .iter()
                .map(|entry| entry.value().clone())
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

    /// Copy the latest snapshot into a bounded history directory and prune old
    /// retained files. Restore still reads `master_snapshot.msgpack`.
    pub fn retain_latest_snapshot(
        &self,
        retention_count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if retention_count == 0 {
            return Ok(());
        }

        let latest = self.disk_dir.join("master_snapshot.msgpack");
        if !latest.exists() {
            return Ok(());
        }

        let history_dir = self.disk_dir.join("snapshots");
        fs::create_dir_all(&history_dir)?;
        let retained = history_dir.join(format!(
            "master_snapshot_{}_{}.msgpack",
            Utc::now().timestamp_millis(),
            Uuid::new_v4()
        ));
        fs::copy(&latest, &retained)?;

        let mut entries = fs::read_dir(&history_dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                let filename = path.file_name()?.to_str()?;
                if !filename.starts_with("master_snapshot_") || !filename.ends_with(".msgpack") {
                    return None;
                }
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .ok()?;
                Some((modified, path))
            })
            .collect::<Vec<_>>();
        entries.sort_by(|(left_time, left_path), (right_time, right_path)| {
            left_time
                .cmp(right_time)
                .then_with(|| left_path.cmp(right_path))
        });

        let remove_count = entries.len().saturating_sub(retention_count);
        for (_modified, path) in entries.into_iter().take(remove_count) {
            fs::remove_file(path)?;
        }

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
    ) -> Result<
        (
            Vec<crate::service::SegmentEntry>,
            Vec<crate::service::NoFSegmentEntry>,
            Vec<(String, crate::service::ObjectEntry)>,
            Vec<crate::service::TaskEntry>,
            Vec<LocalDiskSnapshotEntry>,
        ),
        Box<dyn std::error::Error>,
    > {
        // Deserialize memory segments
        let segments: Vec<crate::service::SegmentEntry> = snap
            .segments
            .into_iter()
            .map(|s| -> Result<_, Box<dyn std::error::Error>> {
                let status = match s.status {
                    0 | 1 => crate::proto::SegmentStatus::Active,
                    2 => crate::proto::SegmentStatus::Draining,
                    3 => crate::proto::SegmentStatus::Unavailable,
                    value => {
                        return Err(invalid_snapshot_data(format!(
                            "invalid memory segment status: {value}"
                        )))
                    }
                };
                Ok(crate::service::SegmentEntry {
                    segment: mooncake_store_core::Segment {
                        id: parse_snapshot_uuid(&s.id, "memory segment id")?,
                        name: s.name,
                        base: s.base,
                        size: s.size,
                        te_endpoint: s.te_endpoint,
                        protocol: s.protocol,
                    },
                    used: s.used,
                    client_id: parse_snapshot_uuid(&s.client_id, "memory segment client id")?,
                    status,
                })
            })
            .collect::<Result<_, _>>()?;

        // Deserialize NoF (file) segments
        let nof_segments: Vec<crate::service::NoFSegmentEntry> = snap
            .nof_segments
            .into_iter()
            .map(|s| -> Result<_, Box<dyn std::error::Error>> {
                let status = match s.status {
                    0 | 1 => crate::proto::SegmentStatus::Active,
                    2 => crate::proto::SegmentStatus::Draining,
                    3 => crate::proto::SegmentStatus::Unavailable,
                    value => {
                        return Err(invalid_snapshot_data(format!(
                            "invalid NoF segment status: {value}"
                        )))
                    }
                };
                Ok(crate::service::NoFSegmentEntry {
                    segment: mooncake_store_core::NoFSegment {
                        id: parse_snapshot_uuid(&s.id, "NoF segment id")?,
                        name: s.name,
                        base: s.base,
                        size: s.size,
                        te_endpoint: s.te_endpoint,
                        client_id: parse_snapshot_uuid(&s.client_id, "NoF segment client id")?,
                    },
                    used: s.used,
                    status,
                })
            })
            .collect::<Result<_, _>>()?;

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
            .map(|(_id_str, t)| -> Result<_, Box<dyn std::error::Error>> {
                let assigned_client = t
                    .assigned_client
                    .map(|id| parse_snapshot_uuid(&id, "task assigned client id"))
                    .transpose()?;
                Ok(crate::service::TaskEntry {
                    info: TaskInfo {
                        id: parse_snapshot_uuid(&t.id, "task id")?,
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
                        last_updated_at: chrono::DateTime::from_timestamp_millis(
                            t.last_updated_at_ms,
                        )
                        .unwrap_or_else(Utc::now),
                        assigned_client,
                        message: t.message,
                    },
                    key: t.key,
                    payload: t.payload,
                    max_retry_attempts: 3,
                })
            })
            .collect::<Result<_, _>>()?;

        tracing::info!(
            "Snapshot loaded from {} via {:?} ({} segments, {} objects, {} tasks)",
            path.display(),
            backend_type,
            segments.len() + nof_segments.len(),
            objects.len(),
            tasks.len()
        );

        Ok((
            segments,
            nof_segments,
            objects,
            tasks,
            snap.local_disk_segments,
        ))
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
        Ok(self
            .load_with_local_disk()?
            .map(|(segments, nof_segments, objects, tasks, _)| {
                (segments, nof_segments, objects, tasks)
            }))
    }

    pub fn load_with_local_disk(
        &self,
    ) -> Result<
        Option<(
            Vec<crate::service::SegmentEntry>,
            Vec<crate::service::NoFSegmentEntry>,
            Vec<(String, crate::service::ObjectEntry)>,
            Vec<crate::service::TaskEntry>,
            Vec<LocalDiskSnapshotEntry>,
        )>,
        Box<dyn std::error::Error>,
    > {
        // Try msgpack first (new format), then fall back to JSON (legacy)
        // 优先尝试 msgpack（新格式），不存在则回退到 JSON（旧版兼容）
        let msgpack_path = self.disk_dir.join("master_snapshot.msgpack");
        if msgpack_path.exists() {
            let reader = BufReader::new(BackendFile::open(&msgpack_path, self.backend_type)?);
            let snap: Snapshot = rmp_serde::decode::from_read(reader)?;
            let (segments, nof_segments, objects, tasks, local_disk_segments) =
                Self::build_loaded_state(snap, self.backend_type, &msgpack_path)?;
            return Ok(Some((
                segments,
                nof_segments,
                objects,
                tasks,
                local_disk_segments,
            )));
        }

        // Fall back to legacy JSON format
        // 回退到旧版 JSON 格式
        let json_path = self.disk_dir.join("master_snapshot.json");
        if !json_path.exists() {
            return Ok(None);
        }

        let reader = BufReader::new(BackendFile::open(&json_path, self.backend_type)?);
        let snap: Snapshot = serde_json::from_reader(reader)?;
        let (segments, nof_segments, objects, tasks, local_disk_segments) =
            Self::build_loaded_state(snap, self.backend_type, &json_path)?;
        Ok(Some((
            segments,
            nof_segments,
            objects,
            tasks,
            local_disk_segments,
        )))
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
}
