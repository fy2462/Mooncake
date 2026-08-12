use super::catalog_task::{decode_task_manager, encode_task_manager};
use super::snapshot::{
    EmbeddedSnapshotCatalogStore, LoadedSnapshot, LocalFileSnapshotObjectStore,
    RedisSnapshotCatalogStore, S3SnapshotObjectStore, SnapshotCatalogStore,
    SnapshotCatalogStoreType, SnapshotDescriptor, SnapshotObjectStore, SnapshotObjectStoreType,
    SnapshotProvider,
};
use super::types::HaError;
use crate::TenantId;
use crate::allocator::{AllocationStrategy, AllocatorSnapshotConfig};
use crate::proto::SegmentStatus;
use crate::service::{
    GracefulUnmountSnapshotEntry, ObjectEntry, ReplicationTaskSnapshotEntry, SegmentEntry,
};
use crate::storage_backend::LocalDiskSnapshotEntry;
use chrono::{Datelike, Timelike};
use mooncake_store_core::{ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
use rmpv::Value;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;
const MANIFEST_PROTOCOL: &str = "messagepack";
const MANIFEST_VERSION: &str = "1.0.0";
const CPP_METADATA_SHARD_COUNT: u64 = 1024;
const RUST_REPLICATION_TASKS_EXTENSION: &str = "rust_replication_tasks_v2";
const RUST_REPLICATION_TASKS_EXTENSION_V1: &str = "rust_replication_tasks_v1";
const RUST_GRACEFUL_UNMOUNTS_EXTENSION: &str = "rust_graceful_unmounts_v1";
const RUST_DELAYED_REPLICA_RELEASES_EXTENSION: &str = "rust_delayed_replica_releases_v1";
const RUST_LOCAL_DISK_REPLICA_IDENTITIES_EXTENSION: &str = "rust_local_disk_replica_identities_v1";
const RUST_ALLOCATOR_CONFIG_EXTENSION: &str = "rust_allocator_config_v1";

#[derive(Serialize, Deserialize)]
struct ReplicationTasksExtension {
    schema_version: u32,
    tasks: Vec<ReplicationTaskSnapshotEntry>,
}

#[derive(Serialize, Deserialize)]
struct GracefulUnmountsExtension {
    schema_version: u32,
    entries: Vec<GracefulUnmountSnapshotEntry>,
}

#[derive(Serialize, Deserialize)]
struct DelayedReplicaReleasesExtension {
    schema_version: u32,
    entries: Vec<crate::service::state::DelayedReplicaReleaseEntry>,
}

#[derive(Serialize, Deserialize)]
struct LocalDiskReplicaIdentitiesExtension {
    schema_version: u32,
    entries: Vec<LocalDiskReplicaIdentity>,
}

#[derive(Serialize, Deserialize)]
struct LocalDiskReplicaIdentity {
    scoped_key: String,
    replica_index: u32,
    storage_id: Option<Uuid>,
    generation_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize)]
struct AllocatorConfigExtension {
    schema_version: u32,
    config: AllocatorSnapshotConfig,
}
pub struct CatalogBackedSnapshotProvider {
    cluster_id: String,
    catalog_store: Box<dyn SnapshotCatalogStore>,
    object_store: Arc<dyn SnapshotObjectStore>,
    snapshot_backup_dir: Option<PathBuf>,
}
impl CatalogBackedSnapshotProvider {
    pub fn new(
        cluster_id: impl Into<String>,
        catalog_store: Box<dyn SnapshotCatalogStore>,
        object_store: Arc<dyn SnapshotObjectStore>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            catalog_store,
            object_store,
            snapshot_backup_dir: None,
        }
    }

    pub fn with_snapshot_backup_dir(mut self, backup_dir: Option<PathBuf>) -> Self {
        self.snapshot_backup_dir = backup_dir.filter(|path| !path.as_os_str().is_empty());
        self
    }

    /// Probe both catalog reads and object-store write/delete before the
    /// process publishes a leader label. The object key is unique and never
    /// enters the snapshot catalog.
    pub fn preflight(&self) -> Result<(), HaError> {
        self.catalog_store.get_latest()?;
        let root = self.catalog_store.get_snapshot_root().trim_end_matches('/');
        let probe_suffix = format!(".preflight/{}/{}", self.cluster_id, Uuid::new_v4());
        let probe_prefix = if root.is_empty() {
            probe_suffix
        } else {
            format!("{root}/{probe_suffix}")
        };
        let probe_key = format!("{probe_prefix}/probe");
        self.object_store
            .upload_buffer(&probe_key, b"mooncake-catalog-preflight-v1")?;
        self.object_store
            .delete_objects_with_prefix(&probe_prefix)?;
        Ok(())
    }

    /// Publish a Rust-loaded snapshot using the C++ catalog object layout.
    ///
    /// Objects are written to a unique snapshot prefix before the catalog is
    /// updated, so readers never observe a descriptor that points at a partial
    /// snapshot.
    pub fn publish_loaded_snapshot(
        &self,
        snapshot: &LoadedSnapshot,
        producer_view_version: u64,
    ) -> Result<SnapshotDescriptor, HaError> {
        let snapshot_id = if snapshot.snapshot_id.trim().is_empty() {
            generate_snapshot_id()
        } else {
            snapshot.snapshot_id.clone()
        };
        let mut descriptor = SnapshotDescriptor::new_with_snapshot_root(
            self.catalog_store.get_snapshot_root(),
            snapshot_id,
        );
        descriptor.last_included_seq = snapshot.snapshot_sequence_id;
        descriptor.producer_view_version = producer_view_version;

        let prefix = descriptor.object_prefix.clone();
        let metadata = encode_metadata(snapshot)?;
        let segments = encode_segments(snapshot)?;
        let task_manager = encode_task_manager(&snapshot.tasks)?;
        let manifest = if snapshot.allocator_config.is_some() {
            format!(
                "{MANIFEST_PROTOCOL}|{MANIFEST_VERSION}|{}|{RUST_ALLOCATOR_CONFIG_EXTENSION}",
                descriptor.snapshot_id
            )
        } else {
            format!(
                "{MANIFEST_PROTOCOL}|{MANIFEST_VERSION}|{}",
                descriptor.snapshot_id
            )
        };
        let core_payloads = [
            ("metadata", format!("{prefix}metadata"), metadata.as_slice()),
            ("segments", format!("{prefix}segments"), segments.as_slice()),
            (
                "task_manager",
                format!("{prefix}task_manager"),
                task_manager.as_slice(),
            ),
        ];
        let mut upload_errors = Vec::new();
        for (name, key, payload) in core_payloads {
            if let Err(error) = self.object_store.upload_buffer(&key, payload) {
                if self.snapshot_backup_dir.is_none() {
                    return Err(error);
                }
                self.backup_failed_snapshot_payload(name, payload);
                upload_errors.push(format!("{name}: {error}"));
            }
        }
        if !upload_errors.is_empty() {
            if let Err(error) = self
                .object_store
                .upload_string(&descriptor.manifest_key, &manifest)
            {
                self.backup_failed_snapshot_payload("manifest.txt", manifest.as_bytes());
                upload_errors.push(format!("manifest.txt: {error}"));
            }
            return Err(HaError::Snapshot(upload_errors.join("\n")));
        }
        self.object_store.upload_buffer(
            &format!("{prefix}{RUST_REPLICATION_TASKS_EXTENSION}"),
            &encode_replication_tasks_extension(&snapshot.replication_tasks)?,
        )?;
        self.object_store.upload_buffer(
            &format!("{prefix}{RUST_GRACEFUL_UNMOUNTS_EXTENSION}"),
            &encode_graceful_unmounts_extension(&snapshot.graceful_unmounts)?,
        )?;
        self.object_store.upload_buffer(
            &format!("{prefix}{RUST_DELAYED_REPLICA_RELEASES_EXTENSION}"),
            &encode_delayed_replica_releases_extension(&snapshot.delayed_replica_releases)?,
        )?;
        self.object_store.upload_buffer(
            &format!("{prefix}{RUST_LOCAL_DISK_REPLICA_IDENTITIES_EXTENSION}"),
            &encode_local_disk_replica_identities_extension(snapshot)?,
        )?;
        if let Some(config) = snapshot.allocator_config {
            self.object_store.upload_buffer(
                &format!("{prefix}{RUST_ALLOCATOR_CONFIG_EXTENSION}"),
                &encode_allocator_config_extension(config)?,
            )?;
        }
        if let Err(error) = self
            .object_store
            .upload_string(&descriptor.manifest_key, &manifest)
        {
            if self.snapshot_backup_dir.is_some() {
                self.backup_failed_snapshot_payload("manifest.txt", manifest.as_bytes());
            }
            return Err(error);
        }
        self.catalog_store.publish(&descriptor)?;
        Ok(descriptor)
    }

    fn backup_failed_snapshot_payload(&self, name: &str, payload: &[u8]) {
        let Some(root) = &self.snapshot_backup_dir else {
            return;
        };
        let backup_dir = root.join("mooncake_snapshot_save_backup");
        if let Err(error) = std::fs::create_dir_all(&backup_dir) {
            tracing::warn!(%error, path = %backup_dir.display(), "failed to create snapshot save backup directory");
            return;
        }
        let path = backup_dir.join(name);
        if let Err(error) = std::fs::write(&path, payload) {
            tracing::warn!(%error, path = %path.display(), "failed to write snapshot save backup");
        }
    }

    pub fn prune_snapshots(&self, retention_count: usize) -> Result<(), HaError> {
        if retention_count == 0 {
            return Ok(());
        }
        let mut snapshots = self.catalog_store.list(0)?;
        sort_snapshot_descriptors_newest_first(&mut snapshots);
        for descriptor in snapshots.into_iter().skip(retention_count) {
            self.catalog_store.delete(&descriptor.snapshot_id)?;
        }
        Ok(())
    }
}

fn sort_snapshot_descriptors_newest_first(snapshots: &mut [SnapshotDescriptor]) {
    snapshots.sort_by(|left, right| {
        right
            .producer_view_version
            .cmp(&left.producer_view_version)
            .then_with(|| right.last_included_seq.cmp(&left.last_included_seq))
            .then_with(|| right.snapshot_id.cmp(&left.snapshot_id))
    });
}

fn encode_replication_tasks_extension(
    tasks: &[ReplicationTaskSnapshotEntry],
) -> Result<Vec<u8>, HaError> {
    let payload = ReplicationTasksExtension {
        schema_version: 2,
        tasks: tasks.to_vec(),
    };
    let encoded = rmp_serde::to_vec_named(&payload).map_err(snapshot_io)?;
    zstd::stream::encode_all(Cursor::new(encoded), 3).map_err(snapshot_io)
}

fn load_replication_tasks_extension(
    object_store: &dyn SnapshotObjectStore,
    prefix: &str,
) -> Result<Vec<ReplicationTaskSnapshotEntry>, HaError> {
    for (extension_name, expected_schema) in [
        (RUST_REPLICATION_TASKS_EXTENSION, 2),
        (RUST_REPLICATION_TASKS_EXTENSION_V1, 1),
    ] {
        let key = format!("{prefix}{extension_name}");
        let data = match object_store.download_buffer(&key) {
            Ok(data) => data,
            Err(error) if object_store.is_not_found_error(&error.to_string()) => continue,
            Err(error) => return Err(error),
        };
        let decoded = zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?;
        let extension: ReplicationTasksExtension =
            rmp_serde::from_slice(&decoded).map_err(snapshot_io)?;
        if extension.schema_version != expected_schema {
            return Err(snapshot_error(format!(
                "Rust replication task extension {extension_name} has schema {}, expected {expected_schema}",
                extension.schema_version
            )));
        }
        return Ok(extension.tasks);
    }
    Ok(Vec::new())
}

fn encode_graceful_unmounts_extension(
    entries: &[GracefulUnmountSnapshotEntry],
) -> Result<Vec<u8>, HaError> {
    let payload = GracefulUnmountsExtension {
        schema_version: 1,
        entries: entries.to_vec(),
    };
    let encoded = rmp_serde::to_vec_named(&payload).map_err(snapshot_io)?;
    zstd::stream::encode_all(Cursor::new(encoded), 3).map_err(snapshot_io)
}

fn encode_delayed_replica_releases_extension(
    entries: &[crate::service::state::DelayedReplicaReleaseEntry],
) -> Result<Vec<u8>, HaError> {
    let payload = DelayedReplicaReleasesExtension {
        schema_version: 1,
        entries: entries.to_vec(),
    };
    let encoded = rmp_serde::to_vec_named(&payload).map_err(snapshot_io)?;
    zstd::stream::encode_all(Cursor::new(encoded), 3).map_err(snapshot_io)
}

fn load_delayed_replica_releases_extension(
    object_store: &dyn SnapshotObjectStore,
    prefix: &str,
) -> Result<Vec<crate::service::state::DelayedReplicaReleaseEntry>, HaError> {
    let key = format!("{prefix}{RUST_DELAYED_REPLICA_RELEASES_EXTENSION}");
    let encoded = match object_store.download_buffer(&key) {
        Ok(encoded) => encoded,
        Err(error) if object_store.is_not_found_error(&error.to_string()) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let decoded = zstd::stream::decode_all(Cursor::new(encoded)).map_err(snapshot_io)?;
    let payload: DelayedReplicaReleasesExtension =
        rmp_serde::from_slice(&decoded).map_err(snapshot_io)?;
    if payload.schema_version != 1 {
        return Err(HaError::Snapshot(format!(
            "unsupported delayed replica release extension schema {}",
            payload.schema_version
        )));
    }
    Ok(payload.entries)
}

fn load_graceful_unmounts_extension(
    object_store: &dyn SnapshotObjectStore,
    prefix: &str,
) -> Result<Vec<GracefulUnmountSnapshotEntry>, HaError> {
    let key = format!("{prefix}{RUST_GRACEFUL_UNMOUNTS_EXTENSION}");
    let data = match object_store.download_buffer(&key) {
        Ok(data) => data,
        Err(error) if object_store.is_not_found_error(&error.to_string()) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let decoded = zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?;
    let extension: GracefulUnmountsExtension =
        rmp_serde::from_slice(&decoded).map_err(snapshot_io)?;
    if extension.schema_version != 1 {
        return Err(snapshot_error(format!(
            "unsupported Rust graceful-unmount snapshot schema {}",
            extension.schema_version
        )));
    }
    Ok(extension.entries)
}

fn encode_allocator_config_extension(config: AllocatorSnapshotConfig) -> Result<Vec<u8>, HaError> {
    let payload = AllocatorConfigExtension {
        schema_version: 1,
        config,
    };
    let encoded = rmp_serde::to_vec_named(&payload).map_err(snapshot_io)?;
    zstd::stream::encode_all(Cursor::new(encoded), 3).map_err(snapshot_io)
}

fn load_allocator_config_extension(
    object_store: &dyn SnapshotObjectStore,
    prefix: &str,
) -> Result<Option<AllocatorSnapshotConfig>, HaError> {
    let key = format!("{prefix}{RUST_ALLOCATOR_CONFIG_EXTENSION}");
    let data = match object_store.download_buffer(&key) {
        Ok(data) => data,
        Err(error) if object_store.is_not_found_error(&error.to_string()) => return Ok(None),
        Err(error) => return Err(error),
    };
    let decoded = zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?;
    let extension: AllocatorConfigExtension =
        rmp_serde::from_slice(&decoded).map_err(snapshot_io)?;
    if extension.schema_version != 1 {
        return Err(snapshot_error(format!(
            "unsupported Rust allocator configuration snapshot schema {}",
            extension.schema_version
        )));
    }
    Ok(Some(extension.config))
}

fn encode_local_disk_replica_identities_extension(
    snapshot: &LoadedSnapshot,
) -> Result<Vec<u8>, HaError> {
    let mut entries = Vec::new();
    for (scoped_key, object) in &snapshot.objects {
        for (replica_index, replica) in object.replicas.iter().enumerate() {
            if replica.replica_type != ReplicaType::LocalDisk {
                continue;
            }
            if replica
                .local_disk_storage_id
                .is_some_and(|storage_id| storage_id.is_nil())
                || replica
                    .local_disk_generation_id
                    .is_some_and(|generation_id| generation_id.is_nil())
            {
                return Err(snapshot_error(format!(
                    "LocalDisk replica for {scoped_key:?} has a nil durable identity"
                )));
            }
            entries.push(LocalDiskReplicaIdentity {
                scoped_key: scoped_key.clone(),
                replica_index: u32::try_from(replica_index)
                    .map_err(|_| snapshot_error("LocalDisk replica index exceeds u32"))?,
                storage_id: replica.local_disk_storage_id,
                generation_id: replica.local_disk_generation_id,
            });
        }
    }
    let payload = LocalDiskReplicaIdentitiesExtension {
        schema_version: 1,
        entries,
    };
    let encoded = rmp_serde::to_vec_named(&payload).map_err(snapshot_io)?;
    zstd::stream::encode_all(Cursor::new(encoded), 3).map_err(snapshot_io)
}

fn load_local_disk_replica_identities_extension(
    object_store: &dyn SnapshotObjectStore,
    prefix: &str,
) -> Result<Option<Vec<LocalDiskReplicaIdentity>>, HaError> {
    let key = format!("{prefix}{RUST_LOCAL_DISK_REPLICA_IDENTITIES_EXTENSION}");
    let data = match object_store.download_buffer(&key) {
        Ok(data) => data,
        Err(error) if object_store.is_not_found_error(&error.to_string()) => return Ok(None),
        Err(error) => return Err(error),
    };
    let decoded = zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?;
    let extension: LocalDiskReplicaIdentitiesExtension =
        rmp_serde::from_slice(&decoded).map_err(snapshot_io)?;
    if extension.schema_version != 1 {
        return Err(snapshot_error(format!(
            "unsupported Rust LocalDisk identity snapshot schema {}",
            extension.schema_version
        )));
    }
    Ok(Some(extension.entries))
}

fn apply_local_disk_replica_identities_extension(
    objects: &mut [(String, ObjectEntry)],
    entries: Vec<LocalDiskReplicaIdentity>,
) -> Result<(), HaError> {
    let object_indexes = objects
        .iter()
        .enumerate()
        .map(|(index, (key, _))| (key.clone(), index))
        .collect::<HashMap<_, _>>();
    let mut seen = std::collections::HashSet::with_capacity(entries.len());
    for entry in entries {
        if entry
            .storage_id
            .is_some_and(|storage_id| storage_id.is_nil())
            || entry
                .generation_id
                .is_some_and(|generation_id| generation_id.is_nil())
        {
            return Err(snapshot_error(format!(
                "LocalDisk identity extension contains a nil identity for {:?}",
                entry.scoped_key
            )));
        }
        let replica_index = usize::try_from(entry.replica_index)
            .map_err(|_| snapshot_error("LocalDisk replica index exceeds usize"))?;
        if !seen.insert((entry.scoped_key.clone(), replica_index)) {
            return Err(snapshot_error(format!(
                "duplicate LocalDisk identity for {:?} replica {}",
                entry.scoped_key, replica_index
            )));
        }
        let Some(object_index) = object_indexes.get(&entry.scoped_key).copied() else {
            // Object expiry is evaluated while loading metadata. Its sidecar
            // identity may therefore legitimately outlive the decoded object.
            continue;
        };
        let replica = objects[object_index]
            .1
            .replicas
            .get_mut(replica_index)
            .ok_or_else(|| {
                snapshot_error(format!(
                    "LocalDisk identity references missing replica {} for {:?}",
                    replica_index, entry.scoped_key
                ))
            })?;
        if replica.replica_type != ReplicaType::LocalDisk {
            return Err(snapshot_error(format!(
                "LocalDisk identity references a non-LocalDisk replica for {:?}",
                entry.scoped_key
            )));
        }
        replica.local_disk_storage_id = entry.storage_id;
        replica.local_disk_generation_id = entry.generation_id;
    }

    for (scoped_key, object) in objects {
        for (replica_index, replica) in object.replicas.iter().enumerate() {
            if replica.replica_type == ReplicaType::LocalDisk
                && !seen.contains(&(scoped_key.clone(), replica_index))
            {
                return Err(snapshot_error(format!(
                    "Rust catalog snapshot is missing LocalDisk identity for {scoped_key:?}"
                )));
            }
        }
    }
    Ok(())
}
fn resolve_local_snapshot_root(
    local_root: Option<PathBuf>,
    environment_root: Option<OsString>,
) -> Result<PathBuf, HaError> {
    let root = local_root
        .or_else(|| environment_root.map(PathBuf::from))
        .ok_or_else(|| {
            HaError::InvalidParams(
                "local snapshot object store requires --snapshot-backup-dir or \
                 MOONCAKE_SNAPSHOT_LOCAL_PATH"
                    .into(),
            )
        })?;
    if root.as_os_str().is_empty() {
        return Err(HaError::InvalidParams(
            "local snapshot object store path must not be empty".into(),
        ));
    }
    Ok(root)
}

pub fn create_catalog_backed_snapshot_provider(
    cluster_id: impl Into<String>,
    object_store_type: SnapshotObjectStoreType,
    catalog_store_type: SnapshotCatalogStoreType,
    local_root: Option<PathBuf>,
    catalog_connstring: Option<&str>,
) -> Result<CatalogBackedSnapshotProvider, HaError> {
    let cluster_id = cluster_id.into();
    let snapshot_backup_dir = local_root.clone();
    let object_store: Arc<dyn SnapshotObjectStore> = match object_store_type {
        SnapshotObjectStoreType::Local => {
            let root = resolve_local_snapshot_root(
                local_root,
                std::env::var_os("MOONCAKE_SNAPSHOT_LOCAL_PATH"),
            )?;
            Arc::new(LocalFileSnapshotObjectStore::new(root))
        }
        SnapshotObjectStoreType::S3 => Arc::new(S3SnapshotObjectStore::from_environment()?),
    };
    let catalog_store: Box<dyn SnapshotCatalogStore> = match catalog_store_type {
        SnapshotCatalogStoreType::Embedded => Box::new(
            EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(
                object_store.clone(),
                &cluster_id,
            ),
        ),
        SnapshotCatalogStoreType::Redis => {
            let connstring = catalog_connstring
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    HaError::InvalidParams(
                        "redis snapshot catalog requires a connection string".into(),
                    )
                })?;
            Box::new(RedisSnapshotCatalogStore::new(
                connstring,
                cluster_id.clone(),
                object_store.clone(),
            )?)
        }
    };
    Ok(
        CatalogBackedSnapshotProvider::new(cluster_id, catalog_store, object_store)
            .with_snapshot_backup_dir(snapshot_backup_dir),
    )
}
impl CatalogBackedSnapshotProvider {
    fn validate_cluster_id(&self, cluster_id: &str) -> Result<(), HaError> {
        if !cluster_id.is_empty() && cluster_id != self.cluster_id {
            return Err(HaError::InvalidParams(format!(
                "snapshot provider cluster mismatch: requested={cluster_id}, configured={}",
                self.cluster_id
            )));
        }
        Ok(())
    }

    fn restore_descriptors(&self) -> Result<Vec<SnapshotDescriptor>, HaError> {
        let latest = match self.catalog_store.get_latest() {
            Ok(latest) => latest,
            Err(error) => {
                tracing::warn!(%error, "failed to load latest snapshot marker; falling back to catalog listing");
                None
            }
        };
        let listed = match self.catalog_store.list(0) {
            Ok(listed) => listed,
            Err(error) if latest.is_some() => {
                tracing::warn!(%error, "failed to list snapshot fallbacks; trying latest only");
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        let mut ids = HashSet::new();
        let mut descriptors = Vec::new();
        if let Some(latest) = latest {
            ids.insert(latest.snapshot_id.clone());
            descriptors.push(latest);
        }
        for descriptor in listed {
            if ids.insert(descriptor.snapshot_id.clone()) {
                descriptors.push(descriptor);
            }
        }
        // `latest` is an availability hint, not a cross-term ordering oracle:
        // a demoted leader may finish an already-started synchronous object
        // store publish after the successor has published. The durable view
        // and sequence carried by each descriptor prevent that late old-term
        // marker from hiding the successor's baseline.
        sort_snapshot_descriptors_newest_first(&mut descriptors);
        Ok(descriptors)
    }

    fn load_descriptor(
        &self,
        descriptor: SnapshotDescriptor,
        backup_if_usable: bool,
    ) -> Result<LoadedSnapshot, HaError> {
        let prefix = if descriptor.object_prefix.is_empty() {
            format!(
                "{}{}/",
                self.catalog_store.get_snapshot_root(),
                descriptor.snapshot_id
            )
        } else {
            descriptor.object_prefix.clone()
        };
        let manifest_key = if descriptor.manifest_key.is_empty() {
            format!("{prefix}manifest.txt")
        } else {
            descriptor.manifest_key.clone()
        };
        let manifest = self.object_store.download_string(&manifest_key)?;
        let allocator_config_required = validate_manifest(&manifest)?;
        let allocator_config =
            load_allocator_config_extension(self.object_store.as_ref(), &prefix)?;
        if allocator_config_required && allocator_config.is_none() {
            return Err(snapshot_error(
                "required Rust allocator configuration extension is missing",
            ));
        }
        let segment_payload = self
            .object_store
            .download_buffer(&format!("{prefix}segments"))?;
        let mut decoded_segments = decode_segments(&segment_payload)?;
        // The legacy C++ Segment wire predates the protocol field. Rust's CXL
        // catalog snapshots carry the authoritative allocator strategy in a
        // versioned extension, so restore every alias as CXL before decoding
        // Memory replicas and handing the snapshot to the allocator rebuild.
        // C++ catalog snapshots never supported the Cachelib-backed CXL
        // SegmentSerializer, so a missing extension remains a non-CXL legacy
        // snapshot rather than being guessed from addresses or names.
        if allocator_config
            .is_some_and(|config| config.allocation_strategy == AllocationStrategy::Cxl)
        {
            for segment in decoded_segments.values_mut() {
                segment.entry.segment.protocol = "cxl".to_string();
            }
        }
        let local_disk_segments = decode_local_disk_segments(&segment_payload)?;
        let segments = decoded_segments
            .values()
            .map(|segment| segment.entry.clone())
            .collect();
        let metadata_payload = self
            .object_store
            .download_buffer(&format!("{prefix}metadata"))?;
        let mut objects = decode_metadata(&metadata_payload, &decoded_segments)?;
        if let Some(entries) =
            load_local_disk_replica_identities_extension(self.object_store.as_ref(), &prefix)?
        {
            apply_local_disk_replica_identities_extension(&mut objects, entries)?;
        }
        let (tasks, task_payload) = match self
            .object_store
            .download_buffer(&format!("{prefix}task_manager"))
        {
            Ok(payload) => (decode_task_manager(&payload)?, Some(payload)),
            Err(error) if self.object_store.is_not_found_error(&error.to_string()) => {
                (Vec::new(), None)
            }
            Err(error) => return Err(error),
        };
        let mut delayed_replica_releases =
            load_delayed_replica_releases_extension(self.object_store.as_ref(), &prefix)?;
        if delayed_replica_releases.is_empty() {
            delayed_replica_releases =
                decode_cpp_discarded_replicas(&metadata_payload, &decoded_segments)?;
        }
        let snapshot = LoadedSnapshot {
            snapshot_id: descriptor.snapshot_id,
            snapshot_sequence_id: descriptor.last_included_seq,
            allocator_config,
            segments,
            nof_segments: Vec::new(),
            objects,
            tasks,
            replication_tasks: load_replication_tasks_extension(
                self.object_store.as_ref(),
                &prefix,
            )?,
            graceful_unmounts: load_graceful_unmounts_extension(
                self.object_store.as_ref(),
                &prefix,
            )?,
            delayed_replica_releases,
            local_disk_segments,
        };
        if backup_if_usable {
            self.backup_restored_payloads(
                &manifest,
                &metadata_payload,
                &segment_payload,
                task_payload.as_deref(),
            );
        }
        Ok(snapshot)
    }

    fn backup_restored_payloads(
        &self,
        manifest: &str,
        metadata: &[u8],
        segments: &[u8],
        task_manager: Option<&[u8]>,
    ) {
        let Some(root) = &self.snapshot_backup_dir else {
            return;
        };
        let backup_dir = root.join("mooncake_snapshot_restore_backup");
        if let Err(error) = std::fs::create_dir_all(&backup_dir) {
            tracing::warn!(%error, path = %backup_dir.display(), "failed to create snapshot restore backup directory");
            return;
        }
        for (name, payload) in [
            ("manifest.txt", manifest.as_bytes()),
            ("metadata", metadata),
            ("segments", segments),
        ] {
            if let Err(error) = std::fs::write(backup_dir.join(name), payload) {
                tracing::warn!(%error, file = name, "failed to write restored snapshot backup");
            }
        }
        if let Some(payload) = task_manager
            && let Err(error) = std::fs::write(backup_dir.join("task_manager"), payload)
        {
            tracing::warn!(%error, "failed to write restored task-manager backup");
        } else if task_manager.is_none() {
            let stale = backup_dir.join("task_manager");
            if let Err(error) = std::fs::remove_file(&stale)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(%error, path = %stale.display(), "failed to remove stale task-manager backup");
            }
        }
    }

    fn load_catalog_candidates(&self, cluster_id: &str) -> Result<Vec<LoadedSnapshot>, HaError> {
        self.validate_cluster_id(cluster_id)?;
        let descriptors = self.restore_descriptors()?;
        let mut snapshots = Vec::new();
        let mut first_error = None;
        for descriptor in descriptors {
            let snapshot_id = descriptor.snapshot_id.clone();
            match self.load_descriptor(descriptor, snapshots.is_empty()) {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => {
                    tracing::warn!(snapshot_id, %error, "snapshot candidate is unusable");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if snapshots.is_empty()
            && let Some(error) = first_error
        {
            return Err(error);
        }
        Ok(snapshots)
    }
}

impl SnapshotProvider for CatalogBackedSnapshotProvider {
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        Ok(self.load_catalog_candidates(cluster_id)?.into_iter().next())
    }

    fn load_snapshot_candidates(&self, cluster_id: &str) -> Result<Vec<LoadedSnapshot>, HaError> {
        self.load_catalog_candidates(cluster_id)
    }
}
#[derive(Clone)]
struct DecodedSegment {
    entry: SegmentEntry,
    has_allocator: bool,
}
fn validate_manifest(manifest: &str) -> Result<bool, HaError> {
    let fields: Vec<_> = manifest.trim().split('|').collect();
    // C++ requires two separators and validates only protocol and version;
    // every trailing field is opaque. The third field is normally a snapshot
    // id, while its canonical fixture uses `standby-test`.
    if fields.len() < 3 || fields[0] != MANIFEST_PROTOCOL || fields[1] != MANIFEST_VERSION {
        return Err(snapshot_error("unsupported snapshot manifest"));
    }
    // Rust's known allocator extension opts into an additional required
    // object. Unknown trailing C++ fields retain legacy behavior.
    Ok(fields.get(3) == Some(&RUST_ALLOCATOR_CONFIG_EXTENSION))
}

fn encode_segments(snapshot: &LoadedSnapshot) -> Result<Vec<u8>, HaError> {
    let mut mounted_segments = Vec::new();
    let mut active_names = Vec::new();
    let mut client_segments: HashMap<Uuid, Vec<Uuid>> = HashMap::new();

    for entry in &snapshot.segments {
        let segment = &entry.segment;
        let allocator = Value::Array(vec![
            segment.name.clone().into(),
            segment.base.into(),
            segment.size.into(),
            entry.used.into(),
            segment.te_endpoint.clone().into(),
            Value::Nil,
        ]);
        let status = match entry.status {
            SegmentStatus::Active => 1_i64,
            SegmentStatus::Draining => 2_i64,
            SegmentStatus::Unavailable => 3_i64,
            SegmentStatus::GracefullyUnmounting => 4_i64,
            _ => 3_i64,
        };
        mounted_segments.push((
            cpp_uuid_string(segment.id).into(),
            Value::Array(vec![
                cpp_uuid_string(segment.id).into(),
                segment.name.clone().into(),
                segment.base.into(),
                segment.size.into(),
                segment.te_endpoint.clone().into(),
                status.into(),
                true.into(),
                allocator,
                segment.host_id.clone().into(),
            ]),
        ));
        if entry.status == SegmentStatus::Active {
            active_names.push(Value::String(segment.name.clone().into()));
        }
        if entry.client_id != Uuid::nil() {
            client_segments
                .entry(entry.client_id)
                .or_default()
                .push(segment.id);
        }
    }

    let mut clients: Vec<(Value, Value)> = client_segments
        .into_iter()
        .map(|(client_id, segment_ids)| {
            (
                cpp_uuid_string(client_id).into(),
                Value::Array(
                    segment_ids
                        .into_iter()
                        .map(|id| Value::String(cpp_uuid_string(id).into()))
                        .collect(),
                ),
            )
        })
        .collect::<Vec<_>>();
    clients.sort_by(|left, right| left.0.as_str().cmp(&right.0.as_str()));

    let mut local_disks = snapshot.local_disk_segments.clone();
    local_disks.sort_by_key(|entry| entry.storage_id);
    let local_disks = local_disks
        .into_iter()
        .map(|entry| {
            let mut objects = entry.offloading_objects.into_iter().collect::<Vec<_>>();
            objects.sort_by(|left, right| left.0.cmp(&right.0));
            let mut fields = vec![
                entry.enable_offloading.into(),
                (objects.len() as u64).into(),
            ];
            for (key, size) in objects {
                fields.push(key.into());
                fields.push(size.into());
            }
            fields.push(entry.ssd_total_capacity_bytes.into());
            fields.push(cpp_uuid_string(entry.client_id).into());
            (
                cpp_uuid_string(entry.storage_id).into(),
                Value::Array(fields),
            )
        })
        .collect();

    encode_compressed_value(&Value::Map(vec![
        ("ma".into(), 0.into()),
        ("an".into(), Value::Array(active_names)),
        ("ms".into(), Value::Map(mounted_segments)),
        ("cs".into(), Value::Map(clients)),
        ("ld".into(), Value::Map(local_disks)),
    ]))
}

fn encode_metadata(snapshot: &LoadedSnapshot) -> Result<Vec<u8>, HaError> {
    let segments_by_id = snapshot
        .segments
        .iter()
        .map(|entry| (entry.segment.id, entry))
        .collect::<HashMap<_, _>>();
    let mut metadata = Vec::new();
    for (scoped_key, object) in &snapshot.objects {
        let user_key = validated_user_key(scoped_key, object)?;
        let mut fields = vec![
            cpp_uuid_string(object.client_id).into(),
            system_time_ms(object.put_start_time.unwrap_or(UNIX_EPOCH))?.into(),
            object.size.into(),
            system_time_ms(object.lease_timeout.unwrap_or(UNIX_EPOCH))?.into(),
            object.soft_pin_timeout.is_some().into(),
            system_time_ms(object.soft_pin_timeout.unwrap_or(UNIX_EPOCH))?.into(),
            (object.replicas.len() as u64).into(),
            (object.data_type as i32 as i64).into(),
        ];
        for replica in &object.replicas {
            fields.push(encode_replica(replica, &segments_by_id)?);
        }
        fields.push(object.hard_pinned.into());
        fields.push(object.group_id.clone().into());
        metadata.push(Value::Array(vec![
            object.tenant_id.as_str().into(),
            user_key.as_ref().into(),
            Value::Array(fields),
        ]));
    }

    let shard = Value::Map(vec![("metadata".into(), Value::Array(metadata))]);
    let compressed_shard = encode_compressed_value(&shard)?;
    encode_value(&Value::Map(vec![(
        "shards".into(),
        Value::Map(vec![(0.into(), Value::Binary(compressed_shard))]),
    )]))
}

fn encode_replica(
    replica: &ReplicaDescriptor,
    segments: &HashMap<Uuid, &SegmentEntry>,
) -> Result<Value, HaError> {
    let payload = match replica.replica_type {
        ReplicaType::Memory => {
            let segment = segments.get(&replica.segment_id).ok_or_else(|| {
                snapshot_error(format!(
                    "memory replica references unknown segment: {}",
                    replica.segment_id
                ))
            })?;
            if replica
                .offset
                .checked_add(replica.size)
                .map_or(true, |end| end > segment.segment.size)
            {
                return Err(snapshot_error("memory replica exceeds segment bounds"));
            }
            let base = if replica.base_addr == 0 {
                segment.segment.base
            } else {
                replica.base_addr
            };
            Value::Array(vec![
                replica.size.into(),
                base.checked_add(replica.offset)
                    .ok_or_else(|| snapshot_error("memory replica address overflow"))?
                    .into(),
                cpp_uuid_string(replica.segment_id).into(),
                false.into(),
                Value::Nil,
            ])
        }
        ReplicaType::Disk => Value::Array(vec![
            replica.segment_name.clone().into(),
            replica.size.into(),
        ]),
        ReplicaType::LocalDisk => Value::Array(vec![
            replica
                .holder_client_id
                .map(cpp_uuid_string)
                .unwrap_or_else(|| cpp_uuid_string(Uuid::nil()))
                .into(),
            replica.size.into(),
            replica.segment_name.clone().into(),
        ]),
        ReplicaType::NoFSsd | ReplicaType::All => {
            return Err(snapshot_error(format!(
                "unsupported replica type for C++ catalog snapshot write: {:?}",
                replica.replica_type
            )));
        }
    };
    Ok(Value::Array(vec![
        0_u64.into(),
        (replica.status as i32 as i64).into(),
        (replica.replica_type as i32 as i64).into(),
        payload,
    ]))
}

fn encode_value(value: &Value) -> Result<Vec<u8>, HaError> {
    let mut data = Vec::new();
    rmpv::encode::write_value(&mut data, value).map_err(snapshot_io)?;
    Ok(data)
}

fn encode_compressed_value(value: &Value) -> Result<Vec<u8>, HaError> {
    zstd::stream::encode_all(Cursor::new(encode_value(value)?), 3).map_err(snapshot_io)
}

fn validated_user_key<'a>(
    scoped_key: &'a str,
    object: &'a ObjectEntry,
) -> Result<Cow<'a, str>, HaError> {
    if scoped_key.contains('\0') {
        let (scoped_tenant, scoped_user_key) = TenantId::parse_scoped_key(scoped_key)
            .map_err(|error| snapshot_error(format!("invalid scoped tenant id: {error}")))?;
        if scoped_tenant != object.tenant_id {
            return Err(snapshot_error(format!(
                "object tenant mismatch: scoped={scoped_tenant}, metadata={}",
                object.tenant_id
            )));
        }
        if !object.user_key.is_empty() && object.user_key != scoped_user_key {
            return Err(snapshot_error(
                "object user key mismatch between scoped key and metadata",
            ));
        }
        return Ok(Cow::Owned(scoped_user_key));
    }
    if object.user_key.is_empty() {
        Ok(Cow::Borrowed(scoped_key))
    } else {
        Ok(Cow::Borrowed(object.user_key.as_str()))
    }
}

fn system_time_ms(value: SystemTime) -> Result<u64, HaError> {
    value
        .duration_since(UNIX_EPOCH)
        .map_err(snapshot_io)
        .and_then(|duration| {
            u64::try_from(duration.as_millis())
                .map_err(|_| snapshot_error("snapshot timestamp exceeds u64"))
        })
}

fn generate_snapshot_id() -> String {
    let now = chrono::Utc::now();
    format!(
        "{:04}{:02}{:02}_{:02}{:02}{:02}_{:03}",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis()
    )
}

fn decode_segments(data: &[u8]) -> Result<HashMap<Uuid, DecodedSegment>, HaError> {
    let root = decode_value(&zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?)?;
    let mounted = value_map(map_field(&root, "ms")?, "mounted segments")?;
    let mut owners = HashMap::new();
    if let Some(clients) = optional_map_field(&root, "cs")? {
        let mut client_ids = HashSet::new();
        for (client, ids) in value_map(clients, "client segments")? {
            let client_id = parse_uuid(value_str(client, "client UUID")?)?;
            if !client_ids.insert(client_id) {
                return Err(snapshot_error("duplicate client UUID in segment ownership"));
            }
            for id in value_array(ids, "client segment IDs")? {
                let segment_id = parse_uuid(value_str(id, "segment UUID")?)?;
                if owners.insert(segment_id, client_id).is_some() {
                    return Err(snapshot_error(
                        "segment is assigned to more than one client",
                    ));
                }
            }
        }
    }
    let mut result = HashMap::new();
    for (id, value) in mounted {
        let map_id = parse_uuid(value_str(id, "segment UUID")?)?;
        let fields = value_array(value, "mounted segment")?;
        if !(8..=9).contains(&fields.len()) {
            return Err(snapshot_error("mounted segment has invalid shape"));
        }
        let segment_id = parse_uuid(value_str(&fields[0], "segment UUID")?)?;
        if segment_id != map_id {
            return Err(snapshot_error("mounted segment UUID mismatch"));
        }
        let encoded_status = value_i64(&fields[5], "segment status")?;
        let status = match encoded_status {
            0 | 3 | 5 => SegmentStatus::Unavailable,
            1 => SegmentStatus::Active,
            2 => SegmentStatus::Draining,
            4 => SegmentStatus::GracefullyUnmounting,
            _ => {
                return Err(snapshot_error(format!(
                    "mounted segment has unknown status {encoded_status}"
                )));
            }
        };
        let segment_name = value_str(&fields[1], "segment name")?;
        let segment_base = value_u64(&fields[2], "segment base")?;
        let segment_size = value_u64(&fields[3], "segment size")?;
        let segment_endpoint = value_str(&fields[4], "segment endpoint")?;
        segment_base
            .checked_add(segment_size)
            .ok_or_else(|| snapshot_error("mounted segment address range overflows"))?;
        let segment_host_id = if fields.len() == 9 {
            value_str(&fields[8], "segment host id")?
        } else {
            ""
        };
        let has_allocator = value_bool(&fields[6], "allocator flag")?;
        let used = if has_allocator {
            let allocator = value_array(&fields[7], "offset allocator")?;
            if allocator.len() != 6 {
                return Err(snapshot_error("invalid offset allocator"));
            }
            if value_str(&allocator[0], "allocator segment name")? != segment_name
                || value_u64(&allocator[1], "allocator base")? != segment_base
                || value_u64(&allocator[2], "allocator size")? != segment_size
                || value_str(&allocator[4], "allocator endpoint")? != segment_endpoint
            {
                return Err(snapshot_error(
                    "offset allocator identity differs from mounted segment",
                ));
            }
            let used = value_u64(&allocator[3], "allocator current size")?;
            if used > segment_size {
                return Err(snapshot_error(
                    "offset allocator usage exceeds segment capacity",
                ));
            }
            used
        } else {
            if !fields[7].is_nil() {
                return Err(snapshot_error(
                    "mounted segment without allocator has non-nil allocator state",
                ));
            }
            0
        };
        let entry = SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: segment_name.to_string(),
                base: segment_base,
                size: segment_size,
                te_endpoint: segment_endpoint.to_string(),
                protocol: "tcp".to_string(),
                host_id: segment_host_id.to_string(),
            },
            used,
            client_id: owners.get(&segment_id).copied().unwrap_or_else(Uuid::nil),
            status,
        };
        if result
            .insert(
                segment_id,
                DecodedSegment {
                    entry,
                    has_allocator,
                },
            )
            .is_some()
        {
            return Err(snapshot_error("duplicate mounted segment UUID"));
        }
    }
    if let Some(segment_id) = owners
        .keys()
        .find(|segment_id| !result.contains_key(segment_id))
    {
        return Err(snapshot_error(format!(
            "client ownership references unknown segment {segment_id}"
        )));
    }
    Ok(result)
}

fn decode_local_disk_segments(data: &[u8]) -> Result<Vec<LocalDiskSnapshotEntry>, HaError> {
    let root = decode_value(&zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?)?;
    let Some(local_disks) = optional_map_field(&root, "ld")? else {
        return Ok(Vec::new());
    };
    let mut result = Vec::new();
    let mut storage_ids = HashSet::new();
    for (storage, value) in value_map(local_disks, "local disk segments")? {
        let storage_id = parse_uuid(value_str(storage, "local disk storage UUID")?)?;
        if !storage_ids.insert(storage_id) {
            return Err(snapshot_error("duplicate local disk storage UUID"));
        }
        let fields = value_array(value, "local disk segment")?;
        if fields.len() < 2 {
            return Err(snapshot_error("local disk segment is too short"));
        }
        let enable_offloading = value_bool(&fields[0], "local disk offloading flag")?;
        let count = usize::try_from(value_u64(&fields[1], "local disk object count")?)
            .map_err(|_| snapshot_error("local disk object count exceeds usize"))?;
        let capacity_index = 2_usize
            .checked_add(
                count
                    .checked_mul(2)
                    .ok_or_else(|| snapshot_error("local disk object count overflow"))?,
            )
            .ok_or_else(|| snapshot_error("local disk object count overflow"))?;
        if fields.len() < capacity_index || fields.len() > capacity_index + 2 {
            return Err(snapshot_error("local disk segment has invalid shape"));
        }
        let mut offloading_objects = HashMap::new();
        for pair in fields[2..capacity_index].chunks_exact(2) {
            let storage_key = value_str(&pair[0], "local disk object key")?.to_string();
            let size = if let Some(task) = pair[1].as_array() {
                if task.len() != 3 {
                    return Err(snapshot_error(
                        "local disk offloading task has invalid shape",
                    ));
                }
                let tenant_id =
                    TenantId::new(value_str(&task[0], "local disk task tenant")?.to_string())
                        .map_err(|error| {
                            snapshot_error(format!(
                                "local disk task has invalid tenant id: {error}"
                            ))
                        })?;
                let user_key = value_str(&task[1], "local disk task key")?;
                if tenant_id.make_scoped_key(user_key) != storage_key {
                    return Err(snapshot_error(
                        "local disk task identity does not match its storage key",
                    ));
                }
                value_i64(&task[2], "local disk task size")?
            } else {
                // Legacy C++ and early Rust snapshots stored only key -> size.
                value_i64(&pair[1], "local disk object size")?
            };
            if size < 0 {
                return Err(snapshot_error("local disk object size is negative"));
            }
            if offloading_objects.insert(storage_key, size).is_some() {
                return Err(snapshot_error("duplicate local disk object key"));
            }
        }
        let ssd_total_capacity_bytes = if fields.len() > capacity_index {
            let capacity = value_i64(&fields[capacity_index], "local disk SSD capacity")?;
            if capacity < 0 {
                return Err(snapshot_error("local disk SSD capacity is negative"));
            }
            capacity
        } else {
            0
        };
        let client_id = if fields.len() > capacity_index + 1 {
            parse_uuid(value_str(
                &fields[capacity_index + 1],
                "local disk active client UUID",
            )?)?
        } else {
            // Legacy catalog entries were keyed by the active client and had
            // no independent durable storage identity.
            storage_id
        };
        result.push(LocalDiskSnapshotEntry {
            storage_id,
            client_id,
            enable_offloading,
            offloading_objects,
            ssd_total_capacity_bytes,
        });
    }
    Ok(result)
}
fn decode_metadata(
    data: &[u8],
    segments: &HashMap<Uuid, DecodedSegment>,
) -> Result<Vec<(String, ObjectEntry)>, HaError> {
    let root = decode_value(data)?;
    let shards = value_map(map_field(&root, "shards")?, "metadata shards")?;
    let now = SystemTime::now();
    let mut objects = Vec::new();
    let mut object_keys = HashSet::new();
    let mut shard_ids = HashSet::new();
    for (encoded_shard_id, blob) in shards {
        let shard_id = match encoded_shard_id {
            Value::String(value) => value
                .as_str()
                .ok_or_else(|| snapshot_error("metadata shard id is not valid UTF-8"))?
                .parse::<u64>()
                .map_err(|_| snapshot_error("metadata shard id is not an unsigned integer"))?,
            value => value_u64(value, "metadata shard id")?,
        };
        if shard_id >= CPP_METADATA_SHARD_COUNT {
            return Err(snapshot_error("metadata shard id is out of range"));
        }
        if !shard_ids.insert(shard_id) {
            return Err(snapshot_error("duplicate metadata shard id"));
        }
        let compressed = match blob {
            Value::Binary(value) => value,
            _ => return Err(snapshot_error("metadata shard is not binary")),
        };
        let shard =
            decode_value(&zstd::stream::decode_all(Cursor::new(compressed)).map_err(snapshot_io)?)?;
        for item in value_array(map_field(&shard, "metadata")?, "metadata entries")? {
            let item = value_array(item, "metadata item")?;
            let (tenant_id, user_key, metadata) = match item {
                [key, metadata] => {
                    let key = value_str(key, "object key")?;
                    let (tenant_id, user_key) =
                        TenantId::parse_scoped_key(key).map_err(|error| {
                            snapshot_error(format!(
                                "invalid tenant id in legacy object key: {error}"
                            ))
                        })?;
                    (tenant_id, user_key, metadata)
                }
                [tenant, key, metadata] => {
                    let tenant_id = TenantId::new(value_str(tenant, "tenant id")?.to_string())
                        .map_err(|error| snapshot_error(format!("invalid tenant id: {error}")))?;
                    let user_key = value_str(key, "object key")?.to_string();
                    (tenant_id, user_key, metadata)
                }
                _ => return Err(snapshot_error("metadata item has invalid shape")),
            };
            if let Some(entry) = decode_object(metadata, &tenant_id, &user_key, segments, now)? {
                let scoped_key = tenant_id.make_scoped_key(&user_key);
                if !object_keys.insert(scoped_key.clone()) {
                    return Err(snapshot_error("duplicate object identity in metadata"));
                }
                objects.push((scoped_key, entry));
            }
        }
    }
    Ok(objects)
}

fn decode_cpp_discarded_replicas(
    data: &[u8],
    segments: &HashMap<Uuid, DecodedSegment>,
) -> Result<Vec<crate::service::state::DelayedReplicaReleaseEntry>, HaError> {
    let root = decode_value(data)?;
    let discarded = match optional_map_field(&root, "discarded_replicas")? {
        Some(value) => value_array(value, "discarded replicas")?,
        None => return Ok(Vec::new()),
    };
    let mut result = Vec::with_capacity(discarded.len());
    for encoded in discarded {
        let fields = value_array(encoded, "discarded replica entry")?;
        if fields.len() < 3 {
            return Err(snapshot_error("discarded replica entry is too short"));
        }
        let deadline_epoch_ms = value_u64(&fields[0], "discarded replica deadline")?;
        let _memory_size = value_u64(&fields[1], "discarded replica memory size")?;
        let replica_count = usize::try_from(value_u64(&fields[2], "discarded replica count")?)
            .map_err(|_| snapshot_error("discarded replica count exceeds usize"))?;
        if replica_count == 0 || fields.len() != 3 + replica_count {
            return Err(snapshot_error(
                "discarded replica entry count does not match payload",
            ));
        }
        let mut replicas = Vec::with_capacity(replica_count);
        for encoded_replica in &fields[3..] {
            let Some(replica) = decode_replica(encoded_replica, segments, false)? else {
                return Err(snapshot_error("discarded replica cannot be reconstructed"));
            };
            if replica.replica_type != ReplicaType::Memory {
                return Err(snapshot_error(
                    "discarded replica entry contains a non-memory replica",
                ));
            }
            replicas.push(replica);
        }
        let id = Uuid::new_v4();
        result.push(crate::service::state::DelayedReplicaReleaseEntry {
            id,
            scoped_key: TenantId::default()
                .make_scoped_key(&format!("__cpp_discarded_replica_{id}")),
            deadline_epoch_ms,
            replicas,
        });
    }
    Ok(result)
}

fn decode_object(
    value: &Value,
    tenant_id: &TenantId,
    user_key: &str,
    segments: &HashMap<Uuid, DecodedSegment>,
    now: SystemTime,
) -> Result<Option<ObjectEntry>, HaError> {
    let fields = value_array(value, "object metadata")?;
    if fields.len() < 7 {
        return Err(snapshot_error("object metadata is too short"));
    }
    let client_id = match parse_uuid(value_str(&fields[0], "object client UUID")?) {
        Ok(client_id) => client_id,
        Err(_) => return Ok(None),
    };
    let put_start_ms = value_u64(&fields[1], "put start time")?;
    let size = value_u64(&fields[2], "object size")?;
    let lease_ms = value_u64(&fields[3], "lease timeout")?;
    let has_soft_pin = value_bool(&fields[4], "soft pin flag")?;
    let soft_pin_ms = value_u64(&fields[5], "soft pin timeout")?;
    let replica_count = usize::try_from(value_u64(&fields[6], "replica count")?)
        .map_err(|_| snapshot_error("replica count is too large"))?;
    let min_fields = 7usize
        .checked_add(replica_count)
        .ok_or_else(|| snapshot_error("replica count overflow"))?;
    if fields.len() < min_fields || fields.len() > min_fields + 3 {
        return Err(snapshot_error("object metadata replica count mismatch"));
    }
    let lease_timeout = time_from_ms(lease_ms)?;
    let soft_pin_timeout = if has_soft_pin {
        Some(time_from_ms(soft_pin_ms)?)
    } else {
        None
    };
    if size == 0 {
        return Ok(None);
    }
    let mut index = 7;
    let data_type = if fields.get(index).and_then(Value::as_u64).is_some() {
        let value = value_u64(&fields[index], "object data type")?;
        index += 1;
        let value =
            i32::try_from(value).map_err(|_| snapshot_error("object data type is too large"))?;
        ObjectDataType::try_from(value).map_err(snapshot_error)?
    } else {
        ObjectDataType::Unknown
    };
    let mut replicas = Vec::with_capacity(replica_count);
    for _ in 0..replica_count {
        let replica = decode_replica(
            fields
                .get(index)
                .ok_or_else(|| snapshot_error("truncated replica list"))?,
            segments,
            false,
        )?;
        index += 1;
        let Some(replica) = replica else {
            return Ok(None);
        };
        replicas.push(replica);
    }
    if replicas.is_empty() {
        return Ok(None);
    }
    let hard_pinned = fields.get(index).and_then(Value::as_bool).unwrap_or(false);
    if fields.get(index).and_then(Value::as_bool).is_some() {
        index += 1;
    }
    let group_id = match fields.get(index) {
        Some(value) => {
            let value = value_str(value, "object group id")?.to_string();
            index += 1;
            value
        }
        None => String::new(),
    };
    if index != fields.len() {
        return Err(snapshot_error(
            "object metadata contains unsupported trailing fields",
        ));
    }
    // Hard pin is an eviction override, not another expiring lease. Parse it
    // before applying the C++ catalog's post-restore expiry cleanup so a
    // hard-pinned object survives even when both ordinary pin deadlines have
    // elapsed. The current C++ cleanup omits this check; Rust intentionally
    // preserves the public hard-pin invariant instead of reproducing that
    // recovery-time data-loss bug.
    if !hard_pinned
        && replicas
            .iter()
            .all(|replica| replica.status == ReplicaStatus::Complete)
        && lease_timeout <= now
        && soft_pin_timeout.map_or(true, |timeout| timeout <= now)
    {
        return Ok(None);
    }
    let quota_committed = replicas
        .iter()
        .all(|replica| replica.status == ReplicaStatus::Complete);
    Ok(Some(ObjectEntry {
        replicas,
        size,
        last_access: now,
        hard_pinned,
        data_type,
        client_id,
        put_start_time: Some(time_from_ms(put_start_ms)?),
        lease_timeout: Some(lease_timeout),
        soft_pin_timeout,
        tenant_id: tenant_id.clone(),
        group_id,
        quota_committed,
        reserved_quota_charge_bytes: 0,
        committed_quota_charge_bytes: 0,
        pending_replaced_quota_charge_bytes: 0,
        memory_cache_total_accounted: false,
        disk_cache_total_accounted: false,
        user_key: user_key.to_string(),
    }))
}

fn decode_replica(
    value: &Value,
    segments: &HashMap<Uuid, DecodedSegment>,
    require_complete: bool,
) -> Result<Option<ReplicaDescriptor>, HaError> {
    let fields = value_array(value, "replica")?;
    if fields.len() != 4 {
        return Err(snapshot_error("replica has invalid shape"));
    }
    let _replica_id = value_u64(&fields[0], "replica id")?;
    let encoded_status = value_i64(&fields[1], "replica status")?;
    let status = match encoded_status {
        0 => ReplicaStatus::Undefined,
        1 => ReplicaStatus::Allocating,
        // C++ creates freshly reserved/writing replicas directly in
        // PROCESSING. Rust's corresponding Master-owned state is Allocating;
        // Written is a Rust wire state that has no distinct C++ catalog phase.
        2 => ReplicaStatus::Allocating,
        3 => ReplicaStatus::Complete,
        // C++ has separate REMOVED=4 and FAILED=5 terminal states. Rust has
        // one unreadable terminal state, so both normalize to Failed rather
        // than being revived as an Allocating replica after recovery.
        4 | 5 => ReplicaStatus::Failed,
        _ => return Err(snapshot_error("replica status is invalid")),
    };
    if require_complete && status != ReplicaStatus::Complete {
        return Ok(None);
    }
    let payload = value_array(&fields[3], "replica payload")?;
    let (segment_id, segment_name, offset, size, holder, base_addr, replica_type) =
        match value_i64(&fields[2], "replica type")? {
            0 => {
                if payload.len() != 5 {
                    return Err(snapshot_error("memory replica payload has invalid shape"));
                }
                let size = value_u64(&payload[0], "replica size")?;
                let address = value_u64(&payload[1], "replica address")?;
                let segment_id = parse_uuid(value_str(&payload[2], "replica segment UUID")?)?;
                let has_offset_handle = value_bool(&payload[3], "replica offset handle flag")?;
                if has_offset_handle {
                    let handle = value_array(&payload[4], "replica offset handle")?;
                    if handle.len() != 3 {
                        return Err(snapshot_error("replica offset handle has invalid shape"));
                    }
                    let _real_base = value_u64(&handle[0], "replica handle real base")?;
                    let _requested_size = value_u64(&handle[1], "replica handle requested size")?;
                    let allocation = value_array(&handle[2], "replica handle allocation metadata")?;
                    if allocation.len() != 2 {
                        return Err(snapshot_error(
                            "replica handle allocation metadata has invalid shape",
                        ));
                    }
                    u32::try_from(value_u64(&allocation[0], "replica handle offset")?)
                        .map_err(|_| snapshot_error("replica handle offset exceeds u32"))?;
                    u32::try_from(value_u64(&allocation[1], "replica handle metadata")?)
                        .map_err(|_| snapshot_error("replica handle metadata exceeds u32"))?;
                } else if !payload[4].is_nil() {
                    return Err(snapshot_error(
                        "replica without offset handle has non-nil handle payload",
                    ));
                }
                let segment = segments
                    .get(&segment_id)
                    .ok_or_else(|| snapshot_error("replica references unknown segment"))?;
                if !matches!(
                    segment.entry.status,
                    SegmentStatus::Active | SegmentStatus::GracefullyUnmounting
                ) || !segment.has_allocator
                {
                    return Ok(None);
                }
                let offset = address
                    .checked_sub(segment.entry.segment.base)
                    .ok_or_else(|| snapshot_error("replica address precedes segment base"))?;
                if offset
                    .checked_add(size)
                    .map_or(true, |end| end > segment.entry.segment.size)
                {
                    return Err(snapshot_error("replica exceeds segment bounds"));
                }
                (
                    segment_id,
                    segment.entry.segment.name.clone(),
                    offset,
                    size,
                    Some(segment.entry.client_id),
                    segment.entry.segment.base,
                    ReplicaType::Memory,
                )
            }
            1 => {
                if payload.len() != 2 {
                    return Err(snapshot_error("disk replica payload has invalid shape"));
                }
                (
                    Uuid::nil(),
                    value_str(&payload[0], "disk path")?.to_string(),
                    0,
                    value_u64(&payload[1], "disk object size")?,
                    None,
                    0,
                    ReplicaType::Disk,
                )
            }
            2 => {
                if payload.len() != 3 {
                    return Err(snapshot_error(
                        "local disk replica payload has invalid shape",
                    ));
                }
                (
                    Uuid::nil(),
                    value_str(&payload[2], "local disk endpoint")?.to_string(),
                    0,
                    value_u64(&payload[1], "local disk object size")?,
                    Some(parse_uuid(value_str(
                        &payload[0],
                        "local disk client UUID",
                    )?)?),
                    0,
                    ReplicaType::LocalDisk,
                )
            }
            _ => return Err(snapshot_error("unsupported replica type")),
        };
    Ok(Some(ReplicaDescriptor {
        segment_id,
        segment_name,
        offset,
        size,
        status,
        replica_type,
        holder_client_id: holder,
        // The C++ v1 payload stores only the active client UUID. Durable
        // storage identity is restored from the Rust sidecar when present, or
        // from the LocalDisk segment table by restore_loaded_snapshot_state.
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr,
        protocol: String::new(),
    }))
}

fn decode_value(data: &[u8]) -> Result<Value, HaError> {
    let mut cursor = Cursor::new(data);
    let value = rmpv::decode::read_value(&mut cursor).map_err(snapshot_io)?;
    if cursor.position() != data.len() as u64 {
        return Err(snapshot_error("snapshot payload contains trailing bytes"));
    }
    Ok(value)
}

fn map_field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, HaError> {
    optional_map_field(value, name)?
        .ok_or_else(|| snapshot_error(format!("missing snapshot field: {name}")))
}

fn optional_map_field<'a>(value: &'a Value, name: &str) -> Result<Option<&'a Value>, HaError> {
    let mut result = None;
    for (key, value) in value_map(value, "map")? {
        if key.as_str() != Some(name) {
            continue;
        }
        if result.replace(value).is_some() {
            return Err(snapshot_error(format!("duplicate snapshot field: {name}")));
        }
    }
    Ok(result)
}

fn value_map<'a>(value: &'a Value, name: &str) -> Result<&'a [(Value, Value)], HaError> {
    value
        .as_map()
        .map(Vec::as_slice)
        .ok_or_else(|| snapshot_error(format!("{name} is not a map")))
}

fn value_array<'a>(value: &'a Value, name: &str) -> Result<&'a [Value], HaError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| snapshot_error(format!("{name} is not an array")))
}

fn value_str<'a>(value: &'a Value, name: &str) -> Result<&'a str, HaError> {
    value
        .as_str()
        .ok_or_else(|| snapshot_error(format!("{name} is not a string")))
}

fn value_u64(value: &Value, name: &str) -> Result<u64, HaError> {
    value
        .as_u64()
        .ok_or_else(|| snapshot_error(format!("{name} is not an unsigned integer")))
}

fn value_i64(value: &Value, name: &str) -> Result<i64, HaError> {
    value
        .as_i64()
        .ok_or_else(|| snapshot_error(format!("{name} is not an integer")))
}

fn value_bool(value: &Value, name: &str) -> Result<bool, HaError> {
    value
        .as_bool()
        .ok_or_else(|| snapshot_error(format!("{name} is not a boolean")))
}

/// Parses a UUID string from a C++-compatible master snapshot.
///
/// The C++ mooncake store serializes UUIDs as decimal `{high}-{low}` pairs
/// (see `UuidToString`/`StringToUuid` in `src/types.cpp`), while Rust-native
/// callers may write standard hyphenated hex UUIDs. Both forms must round-trip
/// so a Rust standby can restore a C++-produced snapshot and a C++ standby can
/// restore a Rust-produced snapshot.
pub(super) fn parse_uuid(value: &str) -> Result<Uuid, HaError> {
    if let Ok(uuid) = Uuid::parse_str(value) {
        return Ok(uuid);
    }
    let (high, low) = value
        .split_once('-')
        .ok_or_else(|| snapshot_error(format!("invalid UUID {value}: not hyphenated")))?;
    let high = u64::from_str_radix(high, 10)
        .map_err(|_| snapshot_error(format!("invalid UUID {value}: high half is not decimal")))?;
    let low = u64::from_str_radix(low, 10)
        .map_err(|_| snapshot_error(format!("invalid UUID {value}: low half is not decimal")))?;
    Ok(Uuid::from_u64_pair(high, low))
}

/// Serializes a UUID in the C++ mooncake on-wire `{high}-{low}` decimal-pair
/// format used by `UuidToString` in `src/types.cpp`.
fn cpp_uuid_string(uuid: Uuid) -> String {
    let (high, low) = uuid.as_u64_pair();
    format!("{high}-{low}")
}

fn time_from_ms(value: u64) -> Result<SystemTime, HaError> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(value))
        .ok_or_else(|| snapshot_error("snapshot timestamp is out of range"))
}

fn snapshot_io(error: impl std::fmt::Display) -> HaError {
    snapshot_error(error.to_string())
}

fn snapshot_error(error: impl Into<String>) -> HaError {
    HaError::Snapshot(error.into())
}

#[cfg(test)]
mod tests {
    use super::{
        HaError, LoadedSnapshot, create_catalog_backed_snapshot_provider,
        decode_cpp_discarded_replicas, decode_local_disk_segments, decode_metadata,
        decode_segments, decode_value, encode_compressed_value, encode_segments, encode_value,
        resolve_local_snapshot_root,
    };
    use crate::ha::{SnapshotCatalogStoreType, SnapshotObjectStoreType};
    use crate::proto::SegmentStatus;
    use crate::service::SegmentEntry;
    use mooncake_store_core::{ReplicaStatus, ReplicaType, Segment};
    use rmpv::Value;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    fn future_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000
    }

    #[test]
    fn local_snapshot_provider_factory_rejects_empty_explicit_root_without_unwind() {
        let result = std::panic::catch_unwind(|| {
            create_catalog_backed_snapshot_provider(
                "cluster-a",
                SnapshotObjectStoreType::Local,
                SnapshotCatalogStoreType::Embedded,
                Some(PathBuf::new()),
                None,
            )
        });

        assert!(matches!(result, Ok(Err(HaError::InvalidParams(_)))));
    }

    #[test]
    fn local_snapshot_root_resolution_rejects_empty_environment_value_without_unwind() {
        let result =
            std::panic::catch_unwind(|| resolve_local_snapshot_root(None, Some(OsString::new())));

        assert!(matches!(result, Ok(Err(HaError::InvalidParams(_)))));
    }

    fn disk_metadata_with_status(status: i64, extra_fields: Vec<Value>) -> Value {
        let replica = Value::Array(vec![
            1_u64.into(),
            status.into(),
            (ReplicaType::Disk as i32 as i64).into(),
            Value::Array(vec!["disk/object".into(), 1_u64.into()]),
        ]);
        let mut fields = vec![
            Uuid::nil().to_string().into(),
            0_u64.into(),
            1_u64.into(),
            future_ms().into(),
            false.into(),
            0_u64.into(),
            1_u64.into(),
            replica,
        ];
        fields.extend(extra_fields);
        Value::Array(fields)
    }

    fn disk_metadata(extra_fields: Vec<Value>) -> Value {
        disk_metadata_with_status(ReplicaStatus::Complete as i32 as i64, extra_fields)
    }

    #[test]
    fn cpp_parity_serializer_test_cpp_serializertest_mountedsegmentserializationpreserveshostid_75440f87()
     {
        let segment_id = Uuid::new_v4();
        let snapshot = LoadedSnapshot {
            snapshot_id: "mounted-segment-round-trip".into(),
            snapshot_sequence_id: 0,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: Segment {
                    id: segment_id,
                    name: "segment_host1".into(),
                    base: 0x300000000,
                    size: 1024 * 1024,
                    te_endpoint: "segment_host1".into(),
                    protocol: "tcp".into(),
                    host_id: "host1".into(),
                },
                used: 0,
                client_id: Uuid::nil(),
                status: SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![],
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };

        let encoded = encode_segments(&snapshot).unwrap();
        let restored = decode_segments(&encoded).unwrap();
        let restored = &restored[&segment_id].entry;

        assert_eq!(restored.segment.id, segment_id);
        assert_eq!(restored.segment.name, "segment_host1");
        assert_eq!(restored.segment.host_id, "host1");
        assert_eq!(restored.status, SegmentStatus::Active);
    }

    #[test]
    fn messagepack_decoder_rejects_trailing_bytes() {
        let mut encoded = encode_value(&Value::Map(Vec::new())).unwrap();
        encoded.push(0);
        assert!(decode_value(&encoded).is_err());
    }

    #[test]
    fn segment_decoder_accepts_legacy_mounted_segment_without_host_id() {
        let segment_id = Uuid::new_v4();
        let mounted = Value::Array(vec![
            segment_id.to_string().into(),
            "legacy-segment".into(),
            0x300000000_u64.into(),
            (1024_u64 * 1024).into(),
            "legacy-segment".into(),
            1.into(),
            false.into(),
            Value::Nil,
        ]);
        let payload = encode_compressed_value(&Value::Map(vec![
            (
                "ms".into(),
                Value::Map(vec![(segment_id.to_string().into(), mounted)]),
            ),
            ("cs".into(), Value::Map(Vec::new())),
        ]))
        .unwrap();

        let decoded = decode_segments(&payload).unwrap();
        let segment = &decoded[&segment_id].entry.segment;
        assert_eq!(segment.id, segment_id);
        assert_eq!(segment.name, "legacy-segment");
        assert_eq!(segment.host_id, "");
    }

    #[test]
    fn metadata_decoder_rejects_duplicate_object_identity() {
        let item = Value::Array(vec![
            "tenant-a".into(),
            "key".into(),
            disk_metadata(Vec::new()),
        ]);
        let shard = Value::Map(vec![(
            "metadata".into(),
            Value::Array(vec![item.clone(), item]),
        )]);
        let encoded_shard = encode_compressed_value(&shard).unwrap();
        let payload = encode_value(&Value::Map(vec![(
            "shards".into(),
            Value::Map(vec![(0.into(), Value::Binary(encoded_shard))]),
        )]))
        .unwrap();

        assert!(decode_metadata(&payload, &HashMap::new()).is_err());
    }

    #[test]
    fn metadata_decoder_rejects_unknown_trailing_shape() {
        let item = Value::Array(vec![
            "tenant-a".into(),
            "key".into(),
            disk_metadata(vec![false.into(), "".into(), 7_u64.into()]),
        ]);
        let shard = Value::Map(vec![("metadata".into(), Value::Array(vec![item]))]);
        let encoded_shard = encode_compressed_value(&shard).unwrap();
        let payload = encode_value(&Value::Map(vec![(
            "shards".into(),
            Value::Map(vec![(0.into(), Value::Binary(encoded_shard))]),
        )]))
        .unwrap();

        assert!(decode_metadata(&payload, &HashMap::new()).is_err());
    }

    #[test]
    fn metadata_decoder_rejects_duplicate_shard_identity() {
        let encoded_shard =
            encode_compressed_value(&Value::Map(vec![("metadata".into(), Value::Array(vec![]))]))
                .unwrap();
        let payload = encode_value(&Value::Map(vec![(
            "shards".into(),
            Value::Map(vec![
                (0.into(), Value::Binary(encoded_shard.clone())),
                (0.into(), Value::Binary(encoded_shard)),
            ]),
        )]))
        .unwrap();

        assert!(decode_metadata(&payload, &HashMap::new()).is_err());
    }

    #[test]
    fn metadata_decoder_preserves_cpp_replica_status_semantics() {
        let expected = [
            ReplicaStatus::Undefined,
            ReplicaStatus::Allocating,
            ReplicaStatus::Allocating,
            ReplicaStatus::Complete,
            ReplicaStatus::Failed,
            ReplicaStatus::Failed,
        ];
        for (encoded_status, expected_status) in expected.into_iter().enumerate() {
            let item = Value::Array(vec![
                "tenant-a".into(),
                format!("key-{encoded_status}").into(),
                disk_metadata_with_status(encoded_status as i64, Vec::new()),
            ]);
            let shard = Value::Map(vec![("metadata".into(), Value::Array(vec![item]))]);
            let encoded_shard = encode_compressed_value(&shard).unwrap();
            let payload = encode_value(&Value::Map(vec![(
                "shards".into(),
                Value::Map(vec![(0.into(), Value::Binary(encoded_shard))]),
            )]))
            .unwrap();

            let objects = decode_metadata(&payload, &HashMap::new()).unwrap();
            assert_eq!(objects.len(), 1);
            assert_eq!(objects[0].1.replicas[0].status, expected_status);
        }
    }

    #[test]
    fn segment_decoder_rejects_unknown_future_status() {
        let segment_id = Uuid::new_v4();
        let mounted = Value::Array(vec![
            segment_id.to_string().into(),
            "segment-a".into(),
            0x1000_u64.into(),
            4096_u64.into(),
            "tcp://node-a".into(),
            6_i64.into(),
            false.into(),
            Value::Nil,
        ]);
        let payload = encode_compressed_value(&Value::Map(vec![(
            "ms".into(),
            Value::Map(vec![(segment_id.to_string().into(), mounted)]),
        )]))
        .unwrap();

        assert!(decode_segments(&payload).is_err());
    }

    #[test]
    fn segment_decoder_rejects_duplicate_optional_client_ownership() {
        let payload = encode_compressed_value(&Value::Map(vec![
            ("ms".into(), Value::Map(Vec::new())),
            ("cs".into(), Value::Map(Vec::new())),
            ("cs".into(), Value::Map(Vec::new())),
        ]))
        .unwrap();

        assert!(decode_segments(&payload).is_err());
    }

    #[test]
    fn segment_decoder_rejects_dangling_client_ownership() {
        let segment_id = Uuid::new_v4();
        let payload = encode_compressed_value(&Value::Map(vec![
            ("ms".into(), Value::Map(Vec::new())),
            (
                "cs".into(),
                Value::Map(vec![(
                    Uuid::new_v4().to_string().into(),
                    Value::Array(vec![segment_id.to_string().into()]),
                )]),
            ),
        ]))
        .unwrap();

        assert!(decode_segments(&payload).is_err());
    }

    #[test]
    fn segment_decoder_rejects_duplicate_client_identity() {
        let client_id = Uuid::new_v4().to_string();
        let payload = encode_compressed_value(&Value::Map(vec![
            ("ms".into(), Value::Map(Vec::new())),
            (
                "cs".into(),
                Value::Map(vec![
                    (client_id.clone().into(), Value::Array(Vec::new())),
                    (client_id.into(), Value::Array(Vec::new())),
                ]),
            ),
        ]))
        .unwrap();

        assert!(decode_segments(&payload).is_err());
    }

    #[test]
    fn local_disk_decoder_rejects_duplicate_optional_inventory() {
        let payload = encode_compressed_value(&Value::Map(vec![
            ("ld".into(), Value::Map(Vec::new())),
            ("ld".into(), Value::Map(Vec::new())),
        ]))
        .unwrap();

        assert!(decode_local_disk_segments(&payload).is_err());
    }

    #[test]
    fn discarded_replica_decoder_rejects_duplicate_optional_field() {
        let payload = encode_value(&Value::Map(vec![
            ("discarded_replicas".into(), Value::Array(Vec::new())),
            ("discarded_replicas".into(), Value::Array(Vec::new())),
        ]))
        .unwrap();

        assert!(decode_cpp_discarded_replicas(&payload, &HashMap::new()).is_err());
    }
}
