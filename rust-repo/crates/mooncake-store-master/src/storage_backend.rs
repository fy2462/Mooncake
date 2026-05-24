use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendType {
    LocalDisk,
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

        match self.backend_type {
            StorageBackendType::LocalDisk => {
                let path = self.disk_dir.join("master_snapshot.json");
                let tmp = self.disk_dir.join("master_snapshot.json.tmp");
                let json = serde_json::to_string_pretty(&snap)?;
                fs::write(&tmp, &json)?;
                fs::rename(&tmp, &path)?;
                tracing::info!(
                    "Snapshot saved to {} ({} segments, {} objects)",
                    path.display(),
                    snap.segments.len(),
                    snap.objects.len()
                );
            }
        }
        Ok(())
    }

    pub fn load(
        &self,
    ) -> Result<
        Option<(Vec<mooncake_store_core::Segment>, Vec<(String, crate::service::ObjectEntry)>)>,
        Box<dyn std::error::Error>,
    > {
        match self.backend_type {
            StorageBackendType::LocalDisk => {
                let path = self.disk_dir.join("master_snapshot.json");
                if !path.exists() {
                    return Ok(None);
                }

                let reader = BufReader::new(fs::File::open(&path)?);
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
                    "Snapshot loaded from {} ({} segments, {} objects)",
                    path.display(),
                    segments.len(),
                    objects.len()
                );

                Ok(Some((segments, objects)))
            }
        }
    }

    pub fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::LocalDisk => {
                let path = self.disk_dir.join("master_snapshot.json");
                if path.exists() {
                    fs::remove_file(&path)?;
                }
            }
        }
        Ok(())
    }
}
