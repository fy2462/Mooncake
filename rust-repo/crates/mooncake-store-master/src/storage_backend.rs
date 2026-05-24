use crate::hf3fs;
use dashmap::DashMap;
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotObject {
    object: crate::service::ObjectEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    segments: Vec<SnapshotSegment>,
    objects: Vec<(String, SnapshotObject)>,
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
        objects: &DashMap<String, crate::service::ObjectEntry>,
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
            snap.segments.len(),
            snap.objects.len()
        );
        Ok(())
    }

    pub fn load(
        &self,
    ) -> Result<
        Option<(
            Vec<mooncake_store_core::Segment>,
            Vec<(String, crate::service::ObjectEntry)>,
        )>,
        Box<dyn std::error::Error>,
    > {
        let path = self.disk_dir.join("master_snapshot.json");
        if !path.exists() {
            return Ok(None);
        }

        let reader = BufReader::new(BackendFile::open(&path, self.backend_type)?);
        let snap: Snapshot = serde_json::from_reader(reader)?;

        let segments: Vec<mooncake_store_core::Segment> = snap
            .segments
            .into_iter()
            .map(|s| mooncake_store_core::Segment {
                id: Uuid::parse_str(&s.id).unwrap_or_else(|_| Uuid::new_v4()),
                name: s.name,
                size: s.size,
                used: s.used,
                client_id: Uuid::parse_str(&s.client_id).unwrap_or_else(|_| Uuid::new_v4()),
            })
            .collect();

        let objects: Vec<(String, crate::service::ObjectEntry)> = snap
            .objects
            .into_iter()
            .map(|(key, obj)| (key, obj.object))
            .collect();

        tracing::info!(
            "Snapshot loaded from {} via {:?} ({} segments, {} objects)",
            path.display(),
            self.backend_type,
            segments.len(),
            objects.len()
        );

        Ok(Some((segments, objects)))
    }

    pub fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        let path = self.disk_dir.join("master_snapshot.json");
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }
}
