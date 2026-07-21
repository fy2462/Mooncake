use super::catalog_task::{encode_task_manager, load_task_manager};
use super::snapshot::{
    EmbeddedSnapshotCatalogStore, LoadedSnapshot, LocalFileSnapshotObjectStore,
    RedisSnapshotCatalogStore, S3SnapshotObjectStore, SnapshotCatalogStore,
    SnapshotCatalogStoreType, SnapshotDescriptor, SnapshotObjectStore, SnapshotObjectStoreType,
    SnapshotProvider,
};
use super::types::HaError;
use crate::make_tenant_scoped_key;
use crate::proto::SegmentStatus;
use crate::service::{ObjectEntry, SegmentEntry};
use crate::storage_backend::LocalDiskSnapshotEntry;
use chrono::{Datelike, Timelike};
use mooncake_store_core::{ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
use rmpv::Value;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;
const MANIFEST_PROTOCOL: &str = "messagepack";
const MANIFEST_VERSION: &str = "1.0.0";
pub struct CatalogBackedSnapshotProvider {
    cluster_id: String,
    catalog_store: Box<dyn SnapshotCatalogStore>,
    object_store: Arc<dyn SnapshotObjectStore>,
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
        }
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
        self.object_store
            .upload_buffer(&format!("{prefix}segments"), &encode_segments(snapshot)?)?;
        self.object_store
            .upload_buffer(&format!("{prefix}metadata"), &encode_metadata(snapshot)?)?;
        self.object_store.upload_buffer(
            &format!("{prefix}task_manager"),
            &encode_task_manager(&snapshot.tasks)?,
        )?;
        self.object_store.upload_string(
            &descriptor.manifest_key,
            &format!("{MANIFEST_PROTOCOL}|{MANIFEST_VERSION}|rust"),
        )?;
        self.catalog_store.publish(&descriptor)?;
        Ok(descriptor)
    }

    pub fn prune_snapshots(&self, retention_count: usize) -> Result<(), HaError> {
        if retention_count == 0 {
            return Ok(());
        }
        let snapshots = self.catalog_store.list(0)?;
        for descriptor in snapshots.into_iter().skip(retention_count) {
            self.catalog_store.delete(&descriptor.snapshot_id)?;
        }
        Ok(())
    }
}
pub fn create_catalog_backed_snapshot_provider(
    cluster_id: impl Into<String>,
    object_store_type: SnapshotObjectStoreType,
    catalog_store_type: SnapshotCatalogStoreType,
    local_root: Option<PathBuf>,
    catalog_connstring: Option<&str>,
) -> Result<CatalogBackedSnapshotProvider, HaError> {
    let cluster_id = cluster_id.into();
    let object_store: Arc<dyn SnapshotObjectStore> = match object_store_type {
        SnapshotObjectStoreType::Local => {
            let root = local_root
                .or_else(|| {
                    std::env::var("MOONCAKE_SNAPSHOT_LOCAL_PATH")
                        .ok()
                        .map(Into::into)
                })
                .ok_or_else(|| {
                    HaError::InvalidParams(
                        "local snapshot object store requires --snapshot-backup-dir or \
                         MOONCAKE_SNAPSHOT_LOCAL_PATH"
                            .into(),
                    )
                })?;
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
    Ok(CatalogBackedSnapshotProvider::new(
        cluster_id,
        catalog_store,
        object_store,
    ))
}
impl SnapshotProvider for CatalogBackedSnapshotProvider {
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        if !cluster_id.is_empty() && cluster_id != self.cluster_id {
            return Err(HaError::InvalidParams(format!(
                "snapshot provider cluster mismatch: requested={cluster_id}, configured={}",
                self.cluster_id
            )));
        }
        let Some(descriptor) = self.catalog_store.get_latest()? else {
            return Ok(None);
        };
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
        validate_manifest(&self.object_store.download_string(&manifest_key)?)?;
        let segment_payload = self
            .object_store
            .download_buffer(&format!("{prefix}segments"))?;
        let decoded_segments = decode_segments(&segment_payload)?;
        let local_disk_segments = decode_local_disk_segments(&segment_payload)?;
        let segments = decoded_segments
            .values()
            .map(|segment| segment.entry.clone())
            .collect();
        let objects = decode_metadata(
            &self
                .object_store
                .download_buffer(&format!("{prefix}metadata"))?,
            &decoded_segments,
        )?;
        let tasks = load_task_manager(self.object_store.as_ref(), &prefix)?;
        Ok(Some(LoadedSnapshot {
            snapshot_id: descriptor.snapshot_id,
            snapshot_sequence_id: descriptor.last_included_seq,
            segments,
            nof_segments: Vec::new(),
            objects,
            tasks,
            local_disk_segments,
        }))
    }
}
#[derive(Clone)]
struct DecodedSegment {
    entry: SegmentEntry,
    has_allocator: bool,
}
fn validate_manifest(manifest: &str) -> Result<(), HaError> {
    let fields: Vec<_> = manifest.trim().split('|').collect();
    if fields.len() != 3 || fields[0] != MANIFEST_PROTOCOL || fields[1] != MANIFEST_VERSION {
        return Err(snapshot_error("unsupported snapshot manifest"));
    }
    Ok(())
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
            _ => 3_i64,
        };
        mounted_segments.push((
            segment.id.to_string().into(),
            Value::Array(vec![
                segment.id.to_string().into(),
                segment.name.clone().into(),
                segment.base.into(),
                segment.size.into(),
                segment.te_endpoint.clone().into(),
                status.into(),
                true.into(),
                allocator,
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
                client_id.to_string().into(),
                Value::Array(
                    segment_ids
                        .into_iter()
                        .map(|id| Value::String(id.to_string().into()))
                        .collect(),
                ),
            )
        })
        .collect::<Vec<_>>();
    clients.sort_by(|left, right| left.0.as_str().cmp(&right.0.as_str()));

    let mut local_disks = snapshot.local_disk_segments.clone();
    local_disks.sort_by_key(|entry| entry.client_id);
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
            (entry.client_id.to_string().into(), Value::Array(fields))
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
        let (tenant_id, user_key) = tenant_and_user_key(scoped_key, object);
        let mut fields = vec![
            object.client_id.to_string().into(),
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
            tenant_id.into(),
            user_key.into(),
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
                replica.segment_id.to_string().into(),
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
                .unwrap_or_else(Uuid::nil)
                .to_string()
                .into(),
            replica.size.into(),
            replica.segment_name.clone().into(),
        ]),
        ReplicaType::NoFSsd | ReplicaType::All => {
            return Err(snapshot_error(format!(
                "unsupported replica type for C++ catalog snapshot write: {:?}",
                replica.replica_type
            )))
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

fn tenant_and_user_key<'a>(scoped_key: &'a str, object: &'a ObjectEntry) -> (&'a str, &'a str) {
    let tenant_id = if object.tenant_id.is_empty() {
        "default"
    } else {
        object.tenant_id.as_str()
    };
    if !object.user_key.is_empty() {
        return (tenant_id, object.user_key.as_str());
    }
    scoped_key
        .split_once('\0')
        .unwrap_or((tenant_id, scoped_key))
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
    if let Ok(clients) = map_field(&root, "cs") {
        for (client, ids) in value_map(clients, "client segments")? {
            let client_id = parse_uuid(value_str(client, "client UUID")?)?;
            for id in value_array(ids, "client segment IDs")? {
                owners.insert(parse_uuid(value_str(id, "segment UUID")?)?, client_id);
            }
        }
    }
    let mut result = HashMap::new();
    for (id, value) in mounted {
        let map_id = parse_uuid(value_str(id, "segment UUID")?)?;
        let fields = value_array(value, "mounted segment")?;
        if fields.len() < 8 {
            return Err(snapshot_error("mounted segment is too short"));
        }
        let segment_id = parse_uuid(value_str(&fields[0], "segment UUID")?)?;
        if segment_id != map_id {
            return Err(snapshot_error("mounted segment UUID mismatch"));
        }
        let status = match value_i64(&fields[5], "segment status")? {
            1 => SegmentStatus::Active,
            2 => SegmentStatus::Draining,
            _ => SegmentStatus::Unavailable,
        };
        let has_allocator = value_bool(&fields[6], "allocator flag")?;
        let used = if has_allocator {
            let allocator = value_array(&fields[7], "offset allocator")?;
            if allocator.len() != 6 {
                return Err(snapshot_error("invalid offset allocator"));
            }
            value_u64(&allocator[3], "allocator current size")?
        } else {
            0
        };
        let entry = SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: value_str(&fields[1], "segment name")?.to_string(),
                base: value_u64(&fields[2], "segment base")?,
                size: value_u64(&fields[3], "segment size")?,
                te_endpoint: value_str(&fields[4], "segment endpoint")?.to_string(),
                protocol: "tcp".to_string(),
            },
            used,
            client_id: owners.get(&segment_id).copied().unwrap_or_else(Uuid::nil),
            status,
        };
        result.insert(
            segment_id,
            DecodedSegment {
                entry,
                has_allocator,
            },
        );
    }
    Ok(result)
}

fn decode_local_disk_segments(data: &[u8]) -> Result<Vec<LocalDiskSnapshotEntry>, HaError> {
    let root = decode_value(&zstd::stream::decode_all(Cursor::new(data)).map_err(snapshot_io)?)?;
    let Ok(local_disks) = map_field(&root, "ld") else {
        return Ok(Vec::new());
    };
    let mut result = Vec::new();
    for (client, value) in value_map(local_disks, "local disk segments")? {
        let client_id = parse_uuid(value_str(client, "local disk client UUID")?)?;
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
        if fields.len() < capacity_index {
            return Err(snapshot_error("local disk object list is truncated"));
        }
        let mut offloading_objects = HashMap::new();
        for pair in fields[2..capacity_index].chunks_exact(2) {
            let key = value_str(&pair[0], "local disk object key")?.to_string();
            let size = value_i64(&pair[1], "local disk object size")?;
            if size < 0 {
                return Err(snapshot_error("local disk object size is negative"));
            }
            offloading_objects.insert(key, size);
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
        result.push(LocalDiskSnapshotEntry {
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
    for (_, blob) in shards {
        let compressed = match blob {
            Value::Binary(value) => value,
            _ => return Err(snapshot_error("metadata shard is not binary")),
        };
        let shard =
            decode_value(&zstd::stream::decode_all(Cursor::new(compressed)).map_err(snapshot_io)?)?;
        for item in value_array(map_field(&shard, "metadata")?, "metadata entries")? {
            let item = value_array(item, "metadata item")?;
            let (tenant_id, user_key, metadata) = match item {
                [key, metadata] => ("default", value_str(key, "object key")?, metadata),
                [tenant, key, metadata] => (
                    value_str(tenant, "tenant id")?,
                    value_str(key, "object key")?,
                    metadata,
                ),
                _ => return Err(snapshot_error("metadata item has invalid shape")),
            };
            if let Some(entry) = decode_object(metadata, tenant_id, user_key, segments, now)? {
                objects.push((make_tenant_scoped_key(tenant_id, user_key), entry));
            }
        }
    }
    Ok(objects)
}

fn decode_object(
    value: &Value,
    tenant_id: &str,
    user_key: &str,
    segments: &HashMap<Uuid, DecodedSegment>,
    now: SystemTime,
) -> Result<Option<ObjectEntry>, HaError> {
    let fields = value_array(value, "object metadata")?;
    if fields.len() < 7 {
        return Err(snapshot_error("object metadata is too short"));
    }
    let client_id = parse_uuid(value_str(&fields[0], "object client UUID")?)?;
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
    if size == 0
        || (lease_timeout <= now && soft_pin_timeout.map_or(true, |timeout| timeout <= now))
    {
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
    let group_id = fields
        .get(index)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
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
        tenant_id: if tenant_id.is_empty() {
            "default"
        } else {
            tenant_id
        }
        .to_string(),
        group_id,
        quota_committed: true,
        memory_cache_total_accounted: false,
        disk_cache_total_accounted: false,
        user_key: user_key.to_string(),
    }))
}

fn decode_replica(
    value: &Value,
    segments: &HashMap<Uuid, DecodedSegment>,
) -> Result<Option<ReplicaDescriptor>, HaError> {
    let fields = value_array(value, "replica")?;
    if fields.len() != 4 {
        return Err(snapshot_error("replica has invalid shape"));
    }
    if value_i64(&fields[1], "replica status")? != ReplicaStatus::Complete as i64 {
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
                let segment = segments
                    .get(&segment_id)
                    .ok_or_else(|| snapshot_error("replica references unknown segment"))?;
                if segment.entry.status != SegmentStatus::Active || !segment.has_allocator {
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
        status: ReplicaStatus::Complete,
        replica_type,
        holder_client_id: holder,
        refcnt: 0,
        handle_valid: true,
        base_addr,
    }))
}

fn decode_value(data: &[u8]) -> Result<Value, HaError> {
    rmpv::decode::read_value(&mut Cursor::new(data)).map_err(snapshot_io)
}

fn map_field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, HaError> {
    value_map(value, "map")?
        .iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
        .ok_or_else(|| snapshot_error(format!("missing snapshot field: {name}")))
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

fn parse_uuid(value: &str) -> Result<Uuid, HaError> {
    Uuid::parse_str(value).map_err(|error| snapshot_error(format!("invalid UUID {value}: {error}")))
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
