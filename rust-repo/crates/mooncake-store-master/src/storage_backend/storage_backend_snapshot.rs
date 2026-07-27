use super::storage_backend_file::BackendFile;
use super::*;
use crate::allocator::AllocatorSnapshotConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufReader;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

const LEGACY_SNAPSHOT_FORMAT_VERSION: u32 = 1;
const CURRENT_SNAPSHOT_FORMAT_VERSION: u32 = 12;

fn invalid_snapshot_data(message: impl Into<String>) -> Box<dyn std::error::Error> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

const fn legacy_snapshot_format_version() -> u32 {
    LEGACY_SNAPSHOT_FORMAT_VERSION
}

fn validate_snapshot_format_version(version: u32) -> Result<(), Box<dyn std::error::Error>> {
    if (LEGACY_SNAPSHOT_FORMAT_VERSION..=CURRENT_SNAPSHOT_FORMAT_VERSION).contains(&version) {
        Ok(())
    } else {
        Err(invalid_snapshot_data(format!(
            "unsupported master snapshot format version {version}; supported versions are \
             {LEGACY_SNAPSHOT_FORMAT_VERSION}..={CURRENT_SNAPSHOT_FORMAT_VERSION}"
        )))
    }
}

fn parse_snapshot_uuid(value: &str, field: &str) -> Result<Uuid, Box<dyn std::error::Error>> {
    Uuid::parse_str(value)
        .map_err(|error| invalid_snapshot_data(format!("invalid {field} '{value}': {error}")))
}

fn system_time_to_epoch_millis(value: SystemTime) -> i64 {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

fn system_time_from_epoch_millis(
    value: i64,
    field: &str,
) -> Result<SystemTime, Box<dyn std::error::Error>> {
    let duration = Duration::from_millis(value.unsigned_abs());
    if value >= 0 {
        SystemTime::UNIX_EPOCH
            .checked_add(duration)
            .ok_or_else(|| invalid_snapshot_data(format!("{field} timestamp overflow")))
    } else {
        SystemTime::UNIX_EPOCH
            .checked_sub(duration)
            .ok_or_else(|| invalid_snapshot_data(format!("{field} timestamp underflow")))
    }
}

fn optional_system_time_from_epoch_millis(
    value: Option<i64>,
    field: &str,
) -> Result<Option<SystemTime>, Box<dyn std::error::Error>> {
    value
        .map(|timestamp| system_time_from_epoch_millis(timestamp, field))
        .transpose()
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
    #[serde(default)]
    host_id: String,
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
    /// Runtime timestamps are skipped by ObjectEntry's general-purpose serde
    /// representation, so the native snapshot owns an explicit, versioned copy.
    /// Missing fields in v1-v3 snapshots intentionally restore as None.
    #[serde(default)]
    put_start_time_ms: Option<i64>,
    #[serde(default)]
    lease_timeout_ms: Option<i64>,
    #[serde(default)]
    soft_pin_timeout_ms: Option<i64>,
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
    #[serde(default = "default_snapshot_task_max_retry_attempts")]
    max_retry_attempts: u32,
}

const fn default_snapshot_task_max_retry_attempts() -> u32 {
    3
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDiskSnapshotEntry {
    #[serde(default)]
    pub storage_id: Uuid,
    /// Last active process at capture time. It is audit metadata only; restored
    /// LocalDisk namespaces start offline and require a new recovery binding.
    pub client_id: Uuid,
    pub enable_offloading: bool,
    pub offloading_objects: HashMap<String, i64>,
    /// Legacy wire field retained for snapshot decoding. New authoritative
    /// captures write zero because capacity belongs to a live process session.
    pub ssd_total_capacity_bytes: i64,
}

/// Top-level snapshot structure for serialization.
/// 顶层快照结构，用于序列化。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    /// Version 1 snapshots predate this field and deserialize through the
    /// serde default. Version 2 adds precise local-disk/allocator restoration;
    /// version 3 adds native Copy/Move reservation and source-pin recovery;
    /// version 4 persists object runtime deadlines and task retry metadata;
    /// version 5 separates durable LocalDisk storage identity from process sessions;
    /// version 6 persists Master-issued LocalDisk byte-generation identities;
    /// version 7 atomically embeds the oplog sequence covered by this payload;
    /// version 8 persists stable Memory Segment host identities.
    /// version 9 persists the old physical quota charge retained by a
    /// size-changing Upsert until its replacement commits or is revoked.
    /// version 10 persists allocator choices whose drift would change restored
    /// range reuse, alignment, or future placement semantics.
    /// version 11 persists removed replica ranges through their RDMA grace
    /// deadline so restart cannot make them allocatable early.
    /// version 12 preserves the exact pre-existing target reused by MoveStart.
    #[serde(default = "legacy_snapshot_format_version")]
    format_version: u32,
    /// Highest oplog sequence represented by this exact snapshot payload.
    /// Legacy files default to zero so recovery replays from the beginning
    /// rather than skipping an unproven gap.
    #[serde(default)]
    last_included_seq: u64,
    #[serde(default)]
    allocator_config: Option<AllocatorSnapshotConfig>,
    segments: Vec<SnapshotSegment>,
    nof_segments: Vec<SnapshotNoFSegment>,
    objects: Vec<(String, SnapshotObject)>,
    tasks: Vec<(String, SnapshotTask)>,
    #[serde(default)]
    replication_tasks: Vec<crate::service::ReplicationTaskSnapshotEntry>,
    #[serde(default)]
    graceful_unmounts: Vec<crate::service::GracefulUnmountSnapshotEntry>,
    #[serde(default)]
    delayed_replica_releases: Vec<crate::service::state::DelayedReplicaReleaseEntry>,
    #[serde(default)]
    local_disk_segments: Vec<LocalDiskSnapshotEntry>,
}

/// Fully-owned native snapshot captured while the metadata mutation barrier is
/// held. Persisting this value performs no reads from live MasterState maps.
pub(crate) struct CapturedNativeSnapshot(Snapshot);

/// Owns either a complete capture or all partially cloned data from a cancelled
/// capture. Call `into_snapshot` only after releasing the mutation barrier so a
/// large partial capture is also deallocated outside the critical section.
pub(crate) struct NativeSnapshotCaptureAttempt {
    snapshot: CapturedNativeSnapshot,
    cancelled: bool,
}

impl NativeSnapshotCaptureAttempt {
    pub(crate) fn into_snapshot(self) -> Option<CapturedNativeSnapshot> {
        (!self.cancelled).then_some(self.snapshot)
    }
}

// =============================================================================
// BackendFile — RAII wrapper with optional HF3FS registration
// =============================================================================

impl StorageBackend {
    /// Verify that the configured native snapshot backend can create, flush,
    /// fsync, and remove a file before a leader advertises itself as serving.
    pub fn preflight_snapshot_writer(&self) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all(&self.disk_dir)?;
        if !self.disk_dir.is_dir() {
            return Err(format!(
                "snapshot backup path is not a directory: {}",
                self.disk_dir.display()
            )
            .into());
        }
        if self.backend_type == StorageBackendType::Distributed
            && self.distributed_adapter.is_none()
        {
            return Err(
                "distributed snapshot backend failed to initialize its filesystem adapter".into(),
            );
        }

        let probe_path = self.disk_dir.join(format!(
            ".mooncake_snapshot_preflight_{}_{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let probe_result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let mut writer = BackendFile::create(&probe_path, self.backend_type)?;
            writer.write_all(b"mooncake-snapshot-preflight-v1")?;
            writer.flush()?;
            writer.sync_all()?;
            drop(writer);
            fs::remove_file(&probe_path)?;
            fs::File::open(&self.disk_dir)?.sync_all()?;
            Ok(())
        })();
        if probe_result.is_err() {
            let _ = fs::remove_file(&probe_path);
        }
        probe_result
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
        self.save_with_runtime_state(
            segments,
            nof_segments,
            objects,
            tasks,
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            local_disk_segments,
        )
    }

    pub(crate) fn save_with_runtime_state(
        &self,
        segments: &DashMap<Uuid, crate::service::SegmentEntry>,
        nof_segments: &DashMap<Uuid, crate::service::NoFSegmentEntry>,
        objects: &DashMap<String, crate::service::ObjectEntry>,
        tasks: &DashMap<Uuid, crate::service::TaskEntry>,
        replication_tasks: &DashMap<String, crate::service::state::ReplicationTaskEntry>,
        graceful_unmounts: &DashMap<Uuid, crate::service::GracefulUnmountSnapshotEntry>,
        delayed_replica_releases: &DashMap<Uuid, crate::service::state::DelayedReplicaReleaseEntry>,
        local_disk_segments: &DashMap<Uuid, LocalDiskSnapshotEntry>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let captured = Self::capture_runtime_state(
            segments,
            nof_segments,
            objects,
            tasks,
            replication_tasks,
            graceful_unmounts,
            delayed_replica_releases,
            local_disk_segments,
        );
        self.save_captured_snapshot(captured)
    }

    /// Clone every live map into an owned serialization DTO.
    ///
    /// Callers that need a point-in-time view should hold the metadata mutation
    /// barrier only for this method. The returned value no longer borrows or
    /// reads live state, so encoding and filesystem I/O can happen after the
    /// barrier is released.
    pub(crate) fn capture_runtime_state(
        segments: &DashMap<Uuid, crate::service::SegmentEntry>,
        nof_segments: &DashMap<Uuid, crate::service::NoFSegmentEntry>,
        objects: &DashMap<String, crate::service::ObjectEntry>,
        tasks: &DashMap<Uuid, crate::service::TaskEntry>,
        replication_tasks: &DashMap<String, crate::service::state::ReplicationTaskEntry>,
        graceful_unmounts: &DashMap<Uuid, crate::service::GracefulUnmountSnapshotEntry>,
        delayed_replica_releases: &DashMap<Uuid, crate::service::state::DelayedReplicaReleaseEntry>,
        local_disk_segments: &DashMap<Uuid, LocalDiskSnapshotEntry>,
    ) -> CapturedNativeSnapshot {
        let never_cancelled = AtomicBool::new(false);
        let local_disk_segments = local_disk_segments
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        Self::capture_runtime_state_cancellable(
            segments,
            nof_segments,
            objects,
            tasks,
            replication_tasks,
            graceful_unmounts,
            delayed_replica_releases,
            local_disk_segments,
            None,
            0,
            &never_cancelled,
        )
        .into_snapshot()
        .expect("capture without cancellation cannot be cancelled")
    }

    /// Cancellation-aware variant used by the asynchronous snapshot worker.
    ///
    /// The flag is checked between individual records. A timed-out worker can
    /// therefore leave the global mutation barrier after at most one record
    /// clone instead of continuing through the complete state and disk write.
    pub(crate) fn capture_runtime_state_cancellable(
        segments: &DashMap<Uuid, crate::service::SegmentEntry>,
        nof_segments: &DashMap<Uuid, crate::service::NoFSegmentEntry>,
        objects: &DashMap<String, crate::service::ObjectEntry>,
        tasks: &DashMap<Uuid, crate::service::TaskEntry>,
        replication_tasks: &DashMap<String, crate::service::state::ReplicationTaskEntry>,
        graceful_unmounts: &DashMap<Uuid, crate::service::GracefulUnmountSnapshotEntry>,
        delayed_replica_releases: &DashMap<Uuid, crate::service::state::DelayedReplicaReleaseEntry>,
        local_disk_segments: Vec<LocalDiskSnapshotEntry>,
        allocator_config: Option<AllocatorSnapshotConfig>,
        last_included_seq: u64,
        cancelled: &AtomicBool,
    ) -> NativeSnapshotCaptureAttempt {
        // Convert domain types to serializable snapshot types
        // 将领域类型转换为可序列化的快照类型
        let snapshot_instant = std::time::Instant::now();
        let mut capture_cancelled = cancelled.load(Ordering::Acquire);

        let mut captured_segments = Vec::with_capacity(segments.len());
        if !capture_cancelled {
            for entry in segments {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                captured_segments.push(SnapshotSegment {
                    id: entry.segment.id.to_string(),
                    name: entry.segment.name.clone(),
                    base: entry.segment.base,
                    size: entry.segment.size,
                    te_endpoint: entry.segment.te_endpoint.clone(),
                    protocol: entry.segment.protocol.clone(),
                    host_id: entry.segment.host_id.clone(),
                    used: entry.used,
                    client_id: entry.client_id.to_string(),
                    status: entry.status as i32,
                });
            }
        }

        let mut captured_nof_segments = Vec::with_capacity(nof_segments.len());
        if !capture_cancelled {
            for entry in nof_segments {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                captured_nof_segments.push(SnapshotNoFSegment {
                    id: entry.segment.id.to_string(),
                    name: entry.segment.name.clone(),
                    base: entry.segment.base,
                    size: entry.segment.size,
                    te_endpoint: entry.segment.te_endpoint.clone(),
                    client_id: entry.segment.client_id.to_string(),
                    used: entry.used,
                    status: entry.status as i32,
                });
            }
        }

        let mut captured_objects = Vec::with_capacity(objects.len());
        if !capture_cancelled {
            for entry in objects {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                let object = entry.value().clone();
                captured_objects.push((
                    entry.key().clone(),
                    SnapshotObject {
                        put_start_time_ms: object.put_start_time.map(system_time_to_epoch_millis),
                        lease_timeout_ms: object.lease_timeout.map(system_time_to_epoch_millis),
                        soft_pin_timeout_ms: object
                            .soft_pin_timeout
                            .map(system_time_to_epoch_millis),
                        object,
                    },
                ));
            }
        }

        let mut captured_tasks = Vec::with_capacity(tasks.len());
        if !capture_cancelled {
            for entry in tasks {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                let info = &entry.info;
                let id_str = info.id.to_string();
                captured_tasks.push((
                    id_str.clone(),
                    SnapshotTask {
                        id: id_str,
                        task_type: info.task_type as i32,
                        status: info.status as i32,
                        created_at_ms: info.created_at.timestamp_millis(),
                        last_updated_at_ms: info.last_updated_at.timestamp_millis(),
                        assigned_client: info.assigned_client.map(|id| id.to_string()),
                        message: info.message.clone(),
                        key: entry.key.clone(),
                        payload: entry.payload.clone(),
                        max_retry_attempts: entry.max_retry_attempts,
                    },
                ));
            }
        }

        let mut captured_replication_tasks = Vec::with_capacity(replication_tasks.len());
        if !capture_cancelled {
            for entry in replication_tasks {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                captured_replication_tasks.push(
                    crate::service::ReplicationTaskSnapshotEntry::capture(
                        entry.key(),
                        entry.value(),
                        snapshot_instant,
                    ),
                );
            }
        }

        let mut captured_graceful_unmounts = Vec::with_capacity(graceful_unmounts.len());
        if !capture_cancelled {
            for entry in graceful_unmounts {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                captured_graceful_unmounts.push(entry.value().clone());
            }
        }

        let mut captured_delayed_replica_releases =
            Vec::with_capacity(delayed_replica_releases.len());
        if !capture_cancelled {
            for entry in delayed_replica_releases {
                if cancelled.load(Ordering::Acquire) {
                    capture_cancelled = true;
                    break;
                }
                captured_delayed_replica_releases.push(entry.value().clone());
            }
        }

        capture_cancelled |= cancelled.load(Ordering::Acquire);
        let snap = Snapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            last_included_seq,
            allocator_config,
            segments: captured_segments,
            nof_segments: captured_nof_segments,
            objects: captured_objects,
            tasks: captured_tasks,
            replication_tasks: captured_replication_tasks,
            graceful_unmounts: captured_graceful_unmounts,
            delayed_replica_releases: captured_delayed_replica_releases,
            local_disk_segments,
        };
        NativeSnapshotCaptureAttempt {
            snapshot: CapturedNativeSnapshot(snap),
            cancelled: capture_cancelled,
        }
    }

    /// Persist a previously captured DTO without consulting live master state.
    pub(crate) fn save_captured_snapshot(
        &self,
        captured: CapturedNativeSnapshot,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let snap = captured.0;
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
        fs::File::open(&retained)?.sync_all()?;
        fs::File::open(&history_dir)?.sync_all()?;

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
        fs::File::open(&history_dir)?.sync_all()?;

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
            Vec<crate::service::ReplicationTaskSnapshotEntry>,
            Vec<crate::service::GracefulUnmountSnapshotEntry>,
            Vec<crate::service::state::DelayedReplicaReleaseEntry>,
            Vec<LocalDiskSnapshotEntry>,
            Option<AllocatorSnapshotConfig>,
        ),
        Box<dyn std::error::Error>,
    > {
        validate_snapshot_format_version(snap.format_version)?;

        // Deserialize memory segments
        let segments: Vec<crate::service::SegmentEntry> = snap
            .segments
            .into_iter()
            .map(|s| -> Result<_, Box<dyn std::error::Error>> {
                let status = match s.status {
                    0 | 1 => crate::proto::SegmentStatus::Active,
                    2 => crate::proto::SegmentStatus::Draining,
                    3 => crate::proto::SegmentStatus::Unavailable,
                    4 => crate::proto::SegmentStatus::GracefullyUnmounting,
                    value => {
                        return Err(invalid_snapshot_data(format!(
                            "invalid memory segment status: {value}"
                        )));
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
                        host_id: s.host_id,
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
                    4 => crate::proto::SegmentStatus::GracefullyUnmounting,
                    value => {
                        return Err(invalid_snapshot_data(format!(
                            "invalid NoF segment status: {value}"
                        )));
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
            .map(|(key, obj)| -> Result<_, Box<dyn std::error::Error>> {
                let mut object = obj.object;
                object.put_start_time = optional_system_time_from_epoch_millis(
                    obj.put_start_time_ms,
                    "object put_start_time",
                )?;
                object.lease_timeout = optional_system_time_from_epoch_millis(
                    obj.lease_timeout_ms,
                    "object lease_timeout",
                )?;
                object.soft_pin_timeout = optional_system_time_from_epoch_millis(
                    obj.soft_pin_timeout_ms,
                    "object soft_pin_timeout",
                )?;
                Ok((key, object))
            })
            .collect::<Result<_, _>>()?;

        // Deserialize tasks
        let tasks: Vec<crate::service::TaskEntry> = snap
            .tasks
            .into_iter()
            .map(|(id_str, t)| -> Result<_, Box<dyn std::error::Error>> {
                let envelope_id = parse_snapshot_uuid(&id_str, "task envelope id")?;
                let task_id = parse_snapshot_uuid(&t.id, "task id")?;
                if task_id.is_nil() {
                    return Err(invalid_snapshot_data("task id is nil"));
                }
                if envelope_id != task_id {
                    return Err(invalid_snapshot_data(format!(
                        "task envelope id {envelope_id} does not match payload id {task_id}"
                    )));
                }
                let assigned_client = t
                    .assigned_client
                    .map(|id| {
                        let client_id = parse_snapshot_uuid(&id, "task assigned client id")?;
                        if client_id.is_nil() {
                            return Err(invalid_snapshot_data("task assigned client id is nil"));
                        }
                        Ok(client_id)
                    })
                    .transpose()?;
                let created_at = chrono::DateTime::from_timestamp_millis(t.created_at_ms)
                    .ok_or_else(|| invalid_snapshot_data("task created timestamp is invalid"))?;
                let last_updated_at = chrono::DateTime::from_timestamp_millis(t.last_updated_at_ms)
                    .ok_or_else(|| invalid_snapshot_data("task update timestamp is invalid"))?;
                if last_updated_at < created_at {
                    return Err(invalid_snapshot_data(
                        "task update timestamp precedes creation timestamp",
                    ));
                }
                Ok(crate::service::TaskEntry {
                    info: TaskInfo {
                        id: task_id,
                        task_type: match t.task_type {
                            0 => TaskType::ReplicaCopy,
                            1 => TaskType::ReplicaMove,
                            value => {
                                return Err(invalid_snapshot_data(format!(
                                    "invalid task type: {value}"
                                )));
                            }
                        },
                        status: match t.status {
                            0 => TaskStatus::Pending,
                            1 => TaskStatus::Processing,
                            2 => TaskStatus::Success,
                            3 => TaskStatus::Failed,
                            value => {
                                return Err(invalid_snapshot_data(format!(
                                    "invalid task status: {value}"
                                )));
                            }
                        },
                        created_at,
                        last_updated_at,
                        assigned_client,
                        message: t.message,
                    },
                    key: t.key,
                    payload: t.payload,
                    max_retry_attempts: t.max_retry_attempts,
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
            snap.replication_tasks,
            snap.graceful_unmounts,
            snap.delayed_replica_releases,
            snap.local_disk_segments,
            snap.allocator_config,
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
        Ok(self.load_with_runtime_state()?.map(
            |(
                segments,
                nof_segments,
                objects,
                tasks,
                _replication_tasks,
                _graceful_unmounts,
                _delayed_replica_releases,
                local_disk_segments,
                _allocator_config,
            )| (segments, nof_segments, objects, tasks, local_disk_segments),
        ))
    }

    pub(crate) fn load_with_runtime_state(
        &self,
    ) -> Result<
        Option<(
            Vec<crate::service::SegmentEntry>,
            Vec<crate::service::NoFSegmentEntry>,
            Vec<(String, crate::service::ObjectEntry)>,
            Vec<crate::service::TaskEntry>,
            Vec<crate::service::ReplicationTaskSnapshotEntry>,
            Vec<crate::service::GracefulUnmountSnapshotEntry>,
            Vec<crate::service::state::DelayedReplicaReleaseEntry>,
            Vec<LocalDiskSnapshotEntry>,
            Option<AllocatorSnapshotConfig>,
        )>,
        Box<dyn std::error::Error>,
    > {
        Ok(self.load_with_runtime_state_and_sequence()?.map(
            |(
                _last_included_seq,
                segments,
                nof_segments,
                objects,
                tasks,
                replication_tasks,
                graceful_unmounts,
                delayed_replica_releases,
                local_disk_segments,
                allocator_config,
            )| {
                (
                    segments,
                    nof_segments,
                    objects,
                    tasks,
                    replication_tasks,
                    graceful_unmounts,
                    delayed_replica_releases,
                    local_disk_segments,
                    allocator_config,
                )
            },
        ))
    }

    pub(crate) fn load_with_runtime_state_and_sequence(
        &self,
    ) -> Result<
        Option<(
            u64,
            Vec<crate::service::SegmentEntry>,
            Vec<crate::service::NoFSegmentEntry>,
            Vec<(String, crate::service::ObjectEntry)>,
            Vec<crate::service::TaskEntry>,
            Vec<crate::service::ReplicationTaskSnapshotEntry>,
            Vec<crate::service::GracefulUnmountSnapshotEntry>,
            Vec<crate::service::state::DelayedReplicaReleaseEntry>,
            Vec<LocalDiskSnapshotEntry>,
            Option<AllocatorSnapshotConfig>,
        )>,
        Box<dyn std::error::Error>,
    > {
        // Try msgpack first (new format), then fall back to JSON (legacy)
        // 优先尝试 msgpack（新格式），不存在则回退到 JSON（旧版兼容）
        let msgpack_path = self.disk_dir.join("master_snapshot.msgpack");
        if msgpack_path.exists() {
            let reader = BufReader::new(BackendFile::open(&msgpack_path, self.backend_type)?);
            let snap: Snapshot = rmp_serde::decode::from_read(reader)?;
            let last_included_seq = snap.last_included_seq;
            let (
                segments,
                nof_segments,
                objects,
                tasks,
                replication_tasks,
                graceful_unmounts,
                delayed_replica_releases,
                local_disk_segments,
                allocator_config,
            ) = Self::build_loaded_state(snap, self.backend_type, &msgpack_path)?;
            return Ok(Some((
                last_included_seq,
                segments,
                nof_segments,
                objects,
                tasks,
                replication_tasks,
                graceful_unmounts,
                delayed_replica_releases,
                local_disk_segments,
                allocator_config,
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
        let last_included_seq = snap.last_included_seq;
        let (
            segments,
            nof_segments,
            objects,
            tasks,
            replication_tasks,
            graceful_unmounts,
            delayed_replica_releases,
            local_disk_segments,
            allocator_config,
        ) = Self::build_loaded_state(snap, self.backend_type, &json_path)?;
        Ok(Some((
            last_included_seq,
            segments,
            nof_segments,
            objects,
            tasks,
            replication_tasks,
            graceful_unmounts,
            delayed_replica_releases,
            local_disk_segments,
            allocator_config,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn empty_snapshot(format_version: u32) -> Snapshot {
        Snapshot {
            format_version,
            last_included_seq: 0,
            allocator_config: None,
            segments: Vec::new(),
            nof_segments: Vec::new(),
            objects: Vec::new(),
            tasks: Vec::new(),
            replication_tasks: Vec::new(),
            graceful_unmounts: Vec::new(),
            delayed_replica_releases: Vec::new(),
            local_disk_segments: Vec::new(),
        }
    }

    fn object_with_runtime_deadlines(
        put_start_time: Option<SystemTime>,
        lease_timeout: Option<SystemTime>,
        soft_pin_timeout: Option<SystemTime>,
    ) -> crate::service::ObjectEntry {
        crate::service::ObjectEntry {
            replicas: Vec::new(),
            size: 17,
            last_access: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time,
            lease_timeout,
            soft_pin_timeout,
            tenant_id: crate::TenantId::default(),
            group_id: String::new(),
            quota_committed: false,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "object-key".to_owned(),
        }
    }

    fn snapshot_task(task_type: i32) -> SnapshotTask {
        SnapshotTask {
            id: Uuid::new_v4().to_string(),
            task_type,
            status: TaskStatus::Pending as i32,
            created_at_ms: 1_700_000_000_000,
            last_updated_at_ms: 1_700_000_000_001,
            assigned_client: None,
            message: String::new(),
            key: "task-key".to_owned(),
            payload: "{}".to_owned(),
            max_retry_attempts: 9,
        }
    }

    fn legacy_empty_snapshot() -> serde_json::Value {
        json!({
            "segments": [],
            "nof_segments": [],
            "objects": [],
            "tasks": [],
            "local_disk_segments": []
        })
    }

    #[test]
    fn legacy_json_and_msgpack_snapshots_default_to_version_one() {
        let json_snapshot: Snapshot =
            serde_json::from_value(legacy_empty_snapshot()).expect("decode legacy JSON snapshot");
        assert_eq!(json_snapshot.format_version, LEGACY_SNAPSHOT_FORMAT_VERSION);
        assert_eq!(json_snapshot.last_included_seq, 0);

        let encoded = rmp_serde::to_vec_named(&legacy_empty_snapshot())
            .expect("encode legacy msgpack snapshot");
        let msgpack_snapshot: Snapshot =
            rmp_serde::from_slice(&encoded).expect("decode legacy msgpack snapshot");
        assert_eq!(
            msgpack_snapshot.format_version,
            LEGACY_SNAPSHOT_FORMAT_VERSION
        );
        assert_eq!(msgpack_snapshot.last_included_seq, 0);
    }

    #[test]
    fn snapshot_version_validation_rejects_unknown_versions() {
        validate_snapshot_format_version(LEGACY_SNAPSHOT_FORMAT_VERSION)
            .expect("legacy version must remain readable");
        validate_snapshot_format_version(CURRENT_SNAPSHOT_FORMAT_VERSION)
            .expect("current version must be readable");

        let error = validate_snapshot_format_version(CURRENT_SNAPSHOT_FORMAT_VERSION + 1)
            .expect_err("future versions must not be silently accepted");
        assert!(
            error
                .to_string()
                .contains("unsupported master snapshot format version")
        );
    }

    #[test]
    fn newly_serialized_snapshot_declares_current_version() {
        let snapshot = empty_snapshot(CURRENT_SNAPSHOT_FORMAT_VERSION);

        let value = serde_json::to_value(snapshot).expect("serialize current snapshot");
        assert_eq!(
            value.get("format_version").and_then(|value| value.as_u64()),
            Some(u64::from(CURRENT_SNAPSHOT_FORMAT_VERSION))
        );
    }

    #[test]
    fn native_snapshot_roundtrips_object_runtime_deadlines() {
        let temp = tempfile::tempdir().expect("create snapshot directory");
        let backend = StorageBackend::new(StorageBackendType::LocalDisk, temp.path());
        let objects = DashMap::new();
        let put_start_time = SystemTime::UNIX_EPOCH - Duration::from_millis(250);
        let lease_timeout = SystemTime::UNIX_EPOCH + Duration::from_millis(1_234);
        let soft_pin_timeout = SystemTime::UNIX_EPOCH + Duration::from_millis(9_876);
        let mut object = object_with_runtime_deadlines(
            Some(put_start_time),
            Some(lease_timeout),
            Some(soft_pin_timeout),
        );
        object.quota_committed = false;
        object.reserved_quota_charge_bytes = 150;
        object.pending_replaced_quota_charge_bytes = 100;
        objects.insert("object-key".to_owned(), object);

        backend
            .save(&DashMap::new(), &DashMap::new(), &objects, &DashMap::new())
            .expect("save native snapshot");
        let (_, _, objects, _) = backend
            .load()
            .expect("load native snapshot")
            .expect("snapshot exists");

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1.put_start_time, Some(put_start_time));
        assert_eq!(objects[0].1.lease_timeout, Some(lease_timeout));
        assert_eq!(objects[0].1.soft_pin_timeout, Some(soft_pin_timeout));
        assert_eq!(objects[0].1.reserved_quota_charge_bytes, 150);
        assert_eq!(objects[0].1.pending_replaced_quota_charge_bytes, 100);
    }

    #[test]
    fn pre_v4_snapshot_objects_default_runtime_deadlines_to_none() {
        let object = object_with_runtime_deadlines(
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2)),
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(3)),
        );
        let legacy_object: SnapshotObject = serde_json::from_value(json!({ "object": object }))
            .expect("decode object DTO without v4 deadline fields");
        assert_eq!(legacy_object.put_start_time_ms, None);
        assert_eq!(legacy_object.lease_timeout_ms, None);
        assert_eq!(legacy_object.soft_pin_timeout_ms, None);

        let mut snapshot = empty_snapshot(3);
        snapshot
            .objects
            .push(("object-key".to_owned(), legacy_object));
        let (_, _, objects, _, _, _, _) = StorageBackend::build_loaded_state(
            snapshot,
            StorageBackendType::LocalDisk,
            Path::new("legacy-v3.msgpack"),
        )
        .expect("load v3 object snapshot");
        assert_eq!(objects[0].1.put_start_time, None);
        assert_eq!(objects[0].1.lease_timeout, None);
        assert_eq!(objects[0].1.soft_pin_timeout, None);
    }

    #[test]
    fn native_snapshot_roundtrips_zero_and_one_task_type_codecs() {
        let temp = tempfile::tempdir().expect("create snapshot directory");
        let backend = StorageBackend::new(StorageBackendType::LocalDisk, temp.path());
        let tasks = DashMap::new();
        let created_at =
            chrono::DateTime::from_timestamp_millis(1_700_000_000_000).expect("valid timestamp");
        let copy_id = Uuid::new_v4();
        let move_id = Uuid::new_v4();
        for (id, task_type, max_retry_attempts) in [
            (copy_id, TaskType::ReplicaCopy, 7),
            (move_id, TaskType::ReplicaMove, 11),
        ] {
            tasks.insert(
                id,
                crate::service::TaskEntry {
                    info: TaskInfo {
                        id,
                        task_type,
                        status: TaskStatus::Pending,
                        created_at,
                        last_updated_at: created_at,
                        assigned_client: None,
                        message: String::new(),
                    },
                    key: format!("key-{id}"),
                    payload: "{}".to_owned(),
                    max_retry_attempts,
                },
            );
        }

        backend
            .save(&DashMap::new(), &DashMap::new(), &DashMap::new(), &tasks)
            .expect("save task snapshot");
        let (_, _, _, loaded_tasks) = backend
            .load()
            .expect("load task snapshot")
            .expect("snapshot exists");
        let loaded_tasks = loaded_tasks
            .into_iter()
            .map(|task| (task.info.id, task))
            .collect::<HashMap<_, _>>();

        assert_eq!(loaded_tasks[&copy_id].info.task_type, TaskType::ReplicaCopy);
        assert_eq!(loaded_tasks[&move_id].info.task_type, TaskType::ReplicaMove);
        assert_eq!(loaded_tasks[&copy_id].max_retry_attempts, 7);
        assert_eq!(loaded_tasks[&move_id].max_retry_attempts, 11);
    }

    #[test]
    fn snapshot_task_unknown_type_fails_fast() {
        let mut snapshot = empty_snapshot(CURRENT_SNAPSHOT_FORMAT_VERSION);
        let task = snapshot_task(99);
        snapshot.tasks.push((task.id.clone(), task));

        let error = StorageBackend::build_loaded_state(
            snapshot,
            StorageBackendType::LocalDisk,
            Path::new("invalid-task.msgpack"),
        )
        .expect_err("unknown task type must not silently become Copy");
        assert!(error.to_string().contains("invalid task type: 99"));
    }

    #[test]
    fn snapshot_task_envelope_and_timestamps_fail_closed() {
        let mut mismatched = empty_snapshot(CURRENT_SNAPSHOT_FORMAT_VERSION);
        let task = snapshot_task(TaskType::ReplicaCopy as i32);
        mismatched.tasks.push((Uuid::new_v4().to_string(), task));
        let error = StorageBackend::build_loaded_state(
            mismatched,
            StorageBackendType::LocalDisk,
            Path::new("mismatched-task-id.msgpack"),
        )
        .expect_err("mismatched task envelope must fail");
        assert!(error.to_string().contains("does not match payload id"));

        let mut reversed_time = empty_snapshot(CURRENT_SNAPSHOT_FORMAT_VERSION);
        let mut task = snapshot_task(TaskType::ReplicaMove as i32);
        task.last_updated_at_ms = task.created_at_ms - 1;
        reversed_time.tasks.push((task.id.clone(), task));
        let error = StorageBackend::build_loaded_state(
            reversed_time,
            StorageBackendType::LocalDisk,
            Path::new("reversed-task-time.msgpack"),
        )
        .expect_err("reversed task timestamps must fail");
        assert!(
            error
                .to_string()
                .contains("update timestamp precedes creation")
        );
    }

    #[test]
    fn legacy_snapshot_task_defaults_retry_attempts() {
        let mut value = serde_json::to_value(snapshot_task(TaskType::ReplicaCopy as i32))
            .expect("serialize task");
        value
            .as_object_mut()
            .expect("task is an object")
            .remove("max_retry_attempts");
        let task: SnapshotTask =
            serde_json::from_value(value).expect("decode legacy task without retry field");
        assert_eq!(
            task.max_retry_attempts,
            default_snapshot_task_max_retry_attempts()
        );
    }

    #[test]
    fn captured_snapshot_is_detached_from_live_maps() {
        let temp = tempfile::tempdir().expect("create snapshot directory");
        let backend = StorageBackend::new(StorageBackendType::LocalDisk, temp.path());
        let objects = DashMap::new();
        objects.insert(
            "object-key".to_owned(),
            object_with_runtime_deadlines(None, None, None),
        );
        let captured = StorageBackend::capture_runtime_state(
            &DashMap::new(),
            &DashMap::new(),
            &objects,
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
        );
        objects.get_mut("object-key").expect("live object").size = 99;

        backend
            .save_captured_snapshot(captured)
            .expect("save detached snapshot");
        let (_, _, loaded_objects, _) = backend
            .load()
            .expect("load detached snapshot")
            .expect("snapshot exists");
        assert_eq!(loaded_objects[0].1.size, 17);
    }

    #[test]
    fn native_snapshot_payload_owns_its_oplog_baseline() {
        let temp = tempfile::tempdir().expect("create snapshot directory");
        let backend = StorageBackend::new(StorageBackendType::LocalDisk, temp.path());
        let cancelled = AtomicBool::new(false);
        let allocator_config = AllocatorSnapshotConfig {
            allocation_strategy: crate::allocator::AllocationStrategy::FreeRatioFirst,
            memory_allocator_kind: crate::allocator::MemoryAllocatorKind::CachelibLike,
        };
        let captured = StorageBackend::capture_runtime_state_cancellable(
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            Vec::new(),
            Some(allocator_config),
            42,
            &cancelled,
        )
        .into_snapshot()
        .expect("capture is not cancelled");
        backend
            .save_captured_snapshot(captured)
            .expect("save native snapshot");

        let (last_included_seq, .., restored_allocator_config) = backend
            .load_with_runtime_state_and_sequence()
            .expect("load native snapshot")
            .expect("native snapshot exists");
        assert_eq!(last_included_seq, 42);
        assert_eq!(restored_allocator_config, Some(allocator_config));
    }

    #[test]
    fn cancelled_capture_stops_before_persistence() {
        let cancelled = AtomicBool::new(true);
        assert!(
            StorageBackend::capture_runtime_state_cancellable(
                &DashMap::new(),
                &DashMap::new(),
                &DashMap::new(),
                &DashMap::new(),
                &DashMap::new(),
                &DashMap::new(),
                &DashMap::new(),
                Vec::new(),
                None,
                0,
                &cancelled,
            )
            .into_snapshot()
            .is_none()
        );
    }
}
