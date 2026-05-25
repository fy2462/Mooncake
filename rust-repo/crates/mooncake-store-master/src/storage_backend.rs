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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendType {
    LocalDisk,
    Hf3fs,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    segments: Vec<SnapshotSegment>,
    nof_segments: Vec<SnapshotNoFSegment>,
    objects: Vec<(String, SnapshotObject)>,
    tasks: Vec<(String, SnapshotTask)>,
}

pub struct StorageBackend {
    backend_type: StorageBackendType,
    disk_dir: PathBuf,
}

struct BackendFile {
    file: fs::File,
    _hf3fs_registration: Option<hf3fs::Hf3fsRegistration>,
}

impl BackendFile {
    fn create(
        path: &Path,
        backend_type: StorageBackendType,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = fs::File::create(path)?;
        let registration = match backend_type {
            StorageBackendType::LocalDisk => None,
            StorageBackendType::Hf3fs => Some(hf3fs::register_fd(file.as_raw_fd())?),
        };
        Ok(Self {
            file,
            _hf3fs_registration: registration,
        })
    }

    fn open(
        path: &Path,
        backend_type: StorageBackendType,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = fs::File::open(path)?;
        let registration = match backend_type {
            StorageBackendType::LocalDisk => None,
            StorageBackendType::Hf3fs => Some(hf3fs::register_fd(file.as_raw_fd())?),
        };
        Ok(Self {
            file,
            _hf3fs_registration: registration,
        })
    }

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

impl StorageBackend {
    pub fn new(backend_type: StorageBackendType, disk_dir: &Path) -> Self {
        fs::create_dir_all(disk_dir).ok();
        Self {
            backend_type,
            disk_dir: disk_dir.to_path_buf(),
        }
    }

    pub fn save(
        &self,
        segments: &DashMap<Uuid, crate::service::SegmentEntry>,
        nof_segments: &DashMap<Uuid, crate::service::NoFSegmentEntry>,
        objects: &DashMap<String, crate::service::ObjectEntry>,
        tasks: &DashMap<Uuid, crate::service::TaskEntry>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let snap = Snapshot {
            segments: segments
                .iter()
                .map(|entry| SnapshotSegment {
                    id: entry.segment.id.to_string(),
                    name: entry.segment.name.clone(),
                    size: entry.segment.size,
                    used: entry.segment.used,
                    client_id: entry.segment.client_id.to_string(),
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

        let path = self.disk_dir.join("master_snapshot.json");
        let tmp = self.disk_dir.join("master_snapshot.json.tmp");
        let writer = BackendFile::create(&tmp, self.backend_type)?;
        let mut writer = BufWriter::new(writer);
        serde_json::to_writer_pretty(&mut writer, &snap)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
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

    pub fn load(
        &self,
    ) -> Result<
        Option<(
            Vec<(mooncake_store_core::Segment, crate::proto::SegmentStatus)>,
            Vec<crate::service::NoFSegmentEntry>,
            Vec<(String, crate::service::ObjectEntry)>,
            Vec<crate::service::TaskEntry>,
        )>,
        Box<dyn std::error::Error>,
    > {
        let path = self.disk_dir.join("master_snapshot.json");
        if !path.exists() {
            return Ok(None);
        }

        let reader = BufReader::new(BackendFile::open(&path, self.backend_type)?);
        let snap: Snapshot = serde_json::from_reader(reader)?;

        let segments: Vec<(mooncake_store_core::Segment, crate::proto::SegmentStatus)> = snap
            .segments
            .into_iter()
            .map(|s| {
                let status = match s.status {
                    1 => crate::proto::SegmentStatus::Active,
                    2 => crate::proto::SegmentStatus::Draining,
                    3 => crate::proto::SegmentStatus::Unavailable,
                    _ => crate::proto::SegmentStatus::Active,
                };
                (mooncake_store_core::Segment {
                    id: Uuid::parse_str(&s.id).unwrap_or_else(|_| Uuid::new_v4()),
                    name: s.name,
                    size: s.size,
                    used: s.used,
                    client_id: Uuid::parse_str(&s.client_id).unwrap_or_else(|_| Uuid::new_v4()),
                }, status)
            })
            .collect();

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

        let objects: Vec<(String, crate::service::ObjectEntry)> = snap
            .objects
            .into_iter()
            .map(|(key, obj)| (key, obj.object))
            .collect();

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
                        .unwrap_or_else(|| Utc::now()),
                    last_updated_at: chrono::DateTime::from_timestamp_millis(t.last_updated_at_ms)
                        .unwrap_or_else(|| Utc::now()),
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
            self.backend_type,
            segments.len() + nof_segments.len(),
            objects.len(),
            tasks.len()
        );

        Ok(Some((segments, nof_segments, objects, tasks)))
    }

    pub fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        let path = self.disk_dir.join("master_snapshot.json");
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }
}
