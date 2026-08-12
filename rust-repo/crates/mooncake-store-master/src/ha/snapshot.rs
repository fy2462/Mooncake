use super::types::HaError;
use crate::allocator::AllocatorSnapshotConfig;
use crate::service::DelayedReplicaReleaseEntry;
use crate::service::GracefulUnmountSnapshotEntry;
use crate::service::NoFSegmentEntry;
use crate::service::ObjectEntry;
use crate::service::ReplicationTaskSnapshotEntry;
use crate::service::SegmentEntry;
use crate::service::TaskEntry;
use crate::storage_backend::LocalDiskSnapshotEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    timeout::TimeoutConfig,
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SNAPSHOT_CATALOG_ROOT: &str = "mooncake_master_snapshot";
const SNAPSHOT_LATEST_FILE: &str = "latest.txt";
const SNAPSHOT_DESCRIPTOR_FILE: &str = "descriptor.txt";
const SNAPSHOT_MANIFEST_FILE: &str = "manifest.txt";
/// C++ catalog descriptor v1 is exactly three pipe-delimited numeric fields.
/// Keep writing this format until both implementations negotiate a successor.
#[cfg(test)]
const SNAPSHOT_DESCRIPTOR_V1_FIELD_COUNT: usize = 3;
const DEFAULT_S3_CONNECT_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_S3_REQUEST_TIMEOUT_MS: u64 = 30_000;

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
    /// Allocator layout and placement semantics used by the producer.
    /// Missing on legacy snapshots, which remain readable.
    pub allocator_config: Option<AllocatorSnapshotConfig>,
    /// Memory segments at snapshot time. / 快照时的内存 segment。
    pub segments: Vec<SegmentEntry>,
    /// NVMe-oF segments at snapshot time. / 快照时的 NVMe-oF segment。
    pub nof_segments: Vec<NoFSegmentEntry>,
    /// Objects and their replicas at snapshot time. / 快照时的对象及其副本。
    pub objects: Vec<(String, ObjectEntry)>,
    /// Pending tasks at snapshot time. / 快照时的待处理任务。
    pub tasks: Vec<TaskEntry>,
    /// Native Copy/Move reservations and source pins at snapshot time.
    pub replication_tasks: Vec<ReplicationTaskSnapshotEntry>,
    /// Delayed segment-unmount intents with portable epoch deadlines.
    pub graceful_unmounts: Vec<GracefulUnmountSnapshotEntry>,
    /// Removed replica ranges retained until their transfer grace deadline.
    pub delayed_replica_releases: Vec<DelayedReplicaReleaseEntry>,
    /// Persisted per-client local-disk state; promotion queues are runtime-only.
    pub local_disk_segments: Vec<LocalDiskSnapshotEntry>,
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
        Self::new_with_snapshot_root(build_snapshot_root(""), snapshot_id)
    }

    pub fn new_with_snapshot_root(
        snapshot_root: impl AsRef<str>,
        snapshot_id: impl Into<String>,
    ) -> Self {
        let snapshot_id = snapshot_id.into();
        let object_prefix = build_snapshot_prefix(snapshot_root.as_ref(), &snapshot_id);
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
    fn get_snapshot_root(&self) -> &str;
}

pub trait SnapshotObjectStore: Send + Sync {
    fn upload_buffer(&self, key: &str, buffer: &[u8]) -> Result<(), HaError>;
    fn download_buffer(&self, key: &str) -> Result<Vec<u8>, HaError>;

    fn upload_string(&self, key: &str, data: &str) -> Result<(), HaError> {
        self.upload_buffer(key, data.as_bytes())
    }

    fn download_string(&self, key: &str) -> Result<String, HaError> {
        let data = self.download_buffer(key)?;
        String::from_utf8(data).map_err(|e| HaError::Snapshot(e.to_string()))
    }

    fn delete_objects_with_prefix(&self, prefix: &str) -> Result<(), HaError>;
    fn list_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>, HaError>;
    fn is_not_found_error(&self, error: &str) -> bool;
    fn connection_info(&self) -> String;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotObjectStoreType {
    Local,
    S3,
}

pub fn parse_snapshot_object_store_type(value: &str) -> Result<SnapshotObjectStoreType, HaError> {
    match value {
        "local" | "LOCAL" => Ok(SnapshotObjectStoreType::Local),
        "s3" | "S3" => Ok(SnapshotObjectStoreType::S3),
        other => Err(HaError::InvalidParams(format!(
            "unknown snapshot object store type: {other}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCatalogStoreType {
    Embedded,
    Redis,
}

pub fn parse_snapshot_catalog_store_type(value: &str) -> Result<SnapshotCatalogStoreType, HaError> {
    match value {
        "embedded" | "EMBEDDED" => Ok(SnapshotCatalogStoreType::Embedded),
        "redis" | "REDIS" => Ok(SnapshotCatalogStoreType::Redis),
        other => Err(HaError::InvalidParams(format!(
            "unknown snapshot catalog store type: {other}"
        ))),
    }
}

pub struct LocalFileSnapshotObjectStore {
    base_path: PathBuf,
}

impl LocalFileSnapshotObjectStore {
    pub fn new(base_path: PathBuf) -> Self {
        assert!(
            !base_path.as_os_str().is_empty(),
            "LocalFileSnapshotObjectStore base path must not be empty"
        );
        Self { base_path }
    }

    fn key_path(&self, key: &str) -> Result<PathBuf, HaError> {
        let path = PathBuf::from(key);
        if path.is_absolute() || key.split('/').any(|part| part == "..") {
            return Err(HaError::InvalidParams(format!(
                "invalid snapshot object key: {key}"
            )));
        }
        Ok(self.base_path.join(path))
    }

    fn sync_directory_chain(&self, directory: &Path) -> Result<(), HaError> {
        let mut current = Some(directory);
        while let Some(path) = current {
            std::fs::File::open(path)
                .and_then(|file| file.sync_all())
                .map_err(|e| HaError::Snapshot(format!("sync snapshot directory: {e}")))?;
            if path == self.base_path {
                break;
            }
            current = path.parent();
        }
        Ok(())
    }

    fn upload_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), HaError> {
        let path = self.key_path(key)?;
        let parent = path.parent().ok_or_else(|| {
            HaError::Snapshot(format!("snapshot object has no parent directory: {key}"))
        })?;
        std::fs::create_dir_all(parent).map_err(|e| HaError::Snapshot(e.to_string()))?;
        let file_name = path
            .file_name()
            .ok_or_else(|| HaError::Snapshot(format!("snapshot object has no file name: {key}")))?;
        let tmp_path = path.with_file_name(format!(
            ".{}.{}.tmp",
            file_name.to_string_lossy(),
            uuid::Uuid::new_v4()
        ));
        let result = (|| {
            let mut file = std::fs::File::create(&tmp_path)
                .map_err(|e| HaError::Snapshot(format!("create snapshot object: {e}")))?;
            file.write_all(bytes)
                .map_err(|e| HaError::Snapshot(format!("write snapshot object: {e}")))?;
            file.sync_all()
                .map_err(|e| HaError::Snapshot(format!("sync snapshot object: {e}")))?;
            drop(file);
            std::fs::rename(&tmp_path, &path)
                .map_err(|e| HaError::Snapshot(format!("publish snapshot object: {e}")))?;
            self.sync_directory_chain(parent)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        result
    }
}

impl SnapshotObjectStore for LocalFileSnapshotObjectStore {
    fn upload_buffer(&self, key: &str, buffer: &[u8]) -> Result<(), HaError> {
        if buffer.is_empty() {
            return Err(HaError::InvalidParams(
                "snapshot object buffer must not be empty".to_string(),
            ));
        }
        self.upload_bytes(key, buffer)
    }

    fn upload_string(&self, key: &str, data: &str) -> Result<(), HaError> {
        self.upload_bytes(key, data.as_bytes())
    }

    fn download_buffer(&self, key: &str) -> Result<Vec<u8>, HaError> {
        std::fs::read(self.key_path(key)?).map_err(|e| HaError::Snapshot(e.to_string()))
    }

    fn delete_objects_with_prefix(&self, prefix: &str) -> Result<(), HaError> {
        let path = self.key_path(prefix.trim_end_matches('/'))?;
        if path.is_dir() {
            let parent = path.parent().map(Path::to_path_buf);
            std::fs::remove_dir_all(&path).map_err(|e| HaError::Snapshot(e.to_string()))?;
            if let Some(parent) = parent {
                self.sync_directory_chain(&parent)?;
            }
        } else if path.exists() {
            let parent = path.parent().map(Path::to_path_buf);
            std::fs::remove_file(&path).map_err(|e| HaError::Snapshot(e.to_string()))?;
            if let Some(parent) = parent {
                self.sync_directory_chain(&parent)?;
            }
        }
        Ok(())
    }

    fn list_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>, HaError> {
        let root = self.key_path(prefix.trim_end_matches('/'))?;
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut objects = Vec::new();
        let base = self.base_path.clone();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(dir).map_err(|e| HaError::Snapshot(e.to_string()))? {
                let entry = entry.map_err(|e| HaError::Snapshot(e.to_string()))?;
                let path = entry.path();
                if entry
                    .file_type()
                    .map_err(|e| HaError::Snapshot(e.to_string()))?
                    .is_dir()
                {
                    stack.push(path);
                } else {
                    let rel = path
                        .strip_prefix(&base)
                        .map_err(|e| HaError::Snapshot(e.to_string()))?;
                    objects.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        objects.sort();
        Ok(objects)
    }

    fn is_not_found_error(&self, error: &str) -> bool {
        error.contains("No such file") || error.contains("not found")
    }

    fn connection_info(&self) -> String {
        format!("local://{}", self.base_path.display())
    }
}

pub struct S3SnapshotObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3SnapshotObjectStore {
    pub fn from_environment() -> Result<Self, HaError> {
        let region =
            std::env::var("MOONCAKE_AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let bucket = std::env::var("MOONCAKE_AWS_BUCKET_NAME").map_err(|_| {
            HaError::InvalidParams("MOONCAKE_AWS_BUCKET_NAME is required for S3 snapshots".into())
        })?;
        let endpoint = std::env::var("MOONCAKE_AWS_S3_ENDPOINT").ok();
        let access_key = std::env::var("MOONCAKE_AWS_ACCESS_KEY_ID").ok();
        let secret_key = std::env::var("MOONCAKE_AWS_SECRET_ACCESS_KEY").ok();
        let use_https = std::env::var("MOONCAKE_AWS_USE_HTTPS")
            .map(|value| parse_bool_like(&value))
            .unwrap_or(true);
        let force_path_style = s3_force_path_style(
            std::env::var("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING")
                .ok()
                .as_deref(),
        );
        let prefix = std::env::var("MOONCAKE_AWS_S3_PREFIX").unwrap_or_default();
        let endpoint_url = build_s3_endpoint_url(endpoint.as_deref(), &region, use_https);
        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(env_timeout_ms(
                "MOONCAKE_AWS_CONNECT_TIMEOUT_MS",
                DEFAULT_S3_CONNECT_TIMEOUT_MS,
            ))
            .operation_attempt_timeout(env_timeout_ms(
                "MOONCAKE_AWS_REQUEST_TIMEOUT_MS",
                DEFAULT_S3_REQUEST_TIMEOUT_MS,
            ))
            .build();
        let request_checksum = parse_request_checksum_calculation(
            std::env::var("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION")
                .ok()
                .as_deref(),
        );
        let response_checksum = parse_response_checksum_validation(
            std::env::var("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION")
                .ok()
                .as_deref(),
        );

        let client = run_async_sync(async move {
            let mut sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new(region))
                .timeout_config(timeout_config);
            if let (Some(key), Some(secret)) = (access_key, secret_key) {
                sdk_config = sdk_config.credentials_provider(Credentials::new(
                    key,
                    secret,
                    None,
                    None,
                    "mooncake-master-snapshot",
                ));
            }
            if let Some(endpoint_url) = endpoint_url {
                sdk_config = sdk_config.endpoint_url(endpoint_url);
            }
            let sdk_config = sdk_config.load().await;
            let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
            if force_path_style {
                builder = builder.force_path_style(true);
            }
            if let Some(mode) = request_checksum {
                builder = builder.request_checksum_calculation(mode);
            }
            if let Some(mode) = response_checksum {
                builder = builder.response_checksum_validation(mode);
            }
            Ok(aws_sdk_s3::Client::from_conf(builder.build()))
        })?;

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

impl SnapshotObjectStore for S3SnapshotObjectStore {
    fn upload_buffer(&self, key: &str, buffer: &[u8]) -> Result<(), HaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let object_key = self.object_key(key);
        let body = aws_sdk_s3::primitives::ByteStream::from(buffer.to_vec());
        run_async_sync(async move {
            client
                .put_object()
                .bucket(bucket)
                .key(object_key)
                .body(body)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| HaError::Snapshot(e.to_string()))
        })
    }

    fn download_buffer(&self, key: &str) -> Result<Vec<u8>, HaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let object_key = self.object_key(key);
        run_async_sync(async move {
            let output = client
                .get_object()
                .bucket(bucket)
                .key(object_key)
                .send()
                .await
                .map_err(|e| HaError::Snapshot(e.to_string()))?;
            let data = output
                .body
                .collect()
                .await
                .map_err(|e| HaError::Snapshot(e.to_string()))?;
            Ok(data.into_bytes().to_vec())
        })
    }

    fn delete_objects_with_prefix(&self, prefix: &str) -> Result<(), HaError> {
        let objects = self.list_objects_with_prefix(prefix)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_prefix = self.prefix.clone();
        run_async_sync(async move {
            for object_key in objects {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(format!("{key_prefix}{object_key}"))
                    .send()
                    .await
                    .map_err(|e| HaError::Snapshot(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn list_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>, HaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let s3_prefix = self.object_key(prefix);
        let local_prefix_len = self.prefix.len();
        run_async_sync(async move {
            let mut token = None;
            let mut keys = Vec::new();
            loop {
                let mut request = client.list_objects_v2().bucket(&bucket).prefix(&s3_prefix);
                if let Some(ref next_token) = token {
                    request = request.continuation_token(next_token);
                }
                let output = request
                    .send()
                    .await
                    .map_err(|e| HaError::Snapshot(e.to_string()))?;
                for object in output.contents() {
                    if let Some(key) = object.key() {
                        keys.push(key[local_prefix_len..].to_string());
                    }
                }
                token = next_s3_list_token(
                    output.is_truncated().unwrap_or(false),
                    output.next_continuation_token(),
                )?;
                if token.is_none() {
                    break;
                }
            }
            Ok(keys)
        })
    }

    fn is_not_found_error(&self, error: &str) -> bool {
        error.contains("NoSuchKey") || error.contains("NotFound") || error.contains("not found")
    }

    fn connection_info(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }
}

/// Embedded catalog store backed by files under the snapshot root.
pub struct EmbeddedSnapshotCatalogStore {
    object_store: Option<Arc<dyn SnapshotObjectStore>>,
    snapshot_root: String,
}

impl EmbeddedSnapshotCatalogStore {
    pub fn new(root_dir: PathBuf) -> Self {
        Self::with_object_store(Arc::new(LocalFileSnapshotObjectStore::new(root_dir)))
    }

    pub fn with_object_store(object_store: Arc<dyn SnapshotObjectStore>) -> Self {
        Self::with_object_store_and_cluster_id(object_store, "")
    }

    pub fn with_object_store_and_cluster_id(
        object_store: Arc<dyn SnapshotObjectStore>,
        cluster_id: &str,
    ) -> Self {
        Self {
            object_store: Some(object_store),
            snapshot_root: build_snapshot_root(cluster_id),
        }
    }

    pub fn without_object_store(cluster_id: &str) -> Self {
        Self {
            object_store: None,
            snapshot_root: build_snapshot_root(cluster_id),
        }
    }

    fn object_store(&self) -> Result<&Arc<dyn SnapshotObjectStore>, HaError> {
        self.object_store.as_ref().ok_or_else(|| {
            HaError::InvalidParams("embedded snapshot catalog object store is missing".into())
        })
    }
}

impl SnapshotCatalogStore for EmbeddedSnapshotCatalogStore {
    fn publish(&self, snapshot: &SnapshotDescriptor) -> Result<(), HaError> {
        validate_snapshot_id(&snapshot.snapshot_id)?;
        let object_store = self.object_store()?;
        object_store.upload_string(
            &build_descriptor_key(&self.snapshot_root, &snapshot.snapshot_id),
            &serialize_snapshot_descriptor(snapshot),
        )?;
        object_store.upload_string(
            &build_latest_key(&self.snapshot_root),
            &snapshot.snapshot_id,
        )?;
        Ok(())
    }

    fn get_latest(&self) -> Result<Option<SnapshotDescriptor>, HaError> {
        let object_store = self.object_store()?;
        let snapshot_id = match object_store.download_string(&build_latest_key(&self.snapshot_root))
        {
            Ok(value) => value.trim().to_string(),
            Err(error) if object_store.is_not_found_error(&error.to_string()) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if snapshot_id.is_empty() {
            return Err(HaError::Snapshot(
                "embedded snapshot latest marker is empty".into(),
            ));
        }
        validate_snapshot_id(&snapshot_id)?;
        let payload = object_store
            .download_string(&build_descriptor_key(&self.snapshot_root, &snapshot_id))?;
        deserialize_snapshot_descriptor(&self.snapshot_root, &snapshot_id, &payload).map(Some)
    }

    fn list(&self, limit: usize) -> Result<Vec<SnapshotDescriptor>, HaError> {
        let object_store = self.object_store()?;
        let snapshot_root = self.snapshot_root.as_str();
        let mut ids: Vec<String> = object_store
            .list_objects_with_prefix(snapshot_root)?
            .into_iter()
            .filter_map(|key| {
                let trimmed = key.strip_prefix(snapshot_root)?;
                let (snapshot_id, _) = trimmed.split_once('/')?;
                is_valid_snapshot_id(snapshot_id).then(|| snapshot_id.to_string())
            })
            .collect();
        ids.sort_by(|a, b| b.cmp(a));
        ids.dedup();

        let mut snapshots = Vec::new();
        for id in ids {
            if limit != 0 && snapshots.len() >= limit {
                break;
            }
            let payload = match object_store
                .download_string(&build_descriptor_key(&self.snapshot_root, &id))
            {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(snapshot_id = id, %error, "skipping unreadable embedded snapshot descriptor");
                    continue;
                }
            };
            match deserialize_snapshot_descriptor(&self.snapshot_root, &id, &payload) {
                Ok(descriptor) => snapshots.push(descriptor),
                Err(error) => {
                    tracing::warn!(snapshot_id = id, %error, "skipping corrupt embedded snapshot descriptor");
                }
            }
        }
        Ok(snapshots)
    }

    fn delete(&self, snapshot_id: &str) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        let object_store = self.object_store()?;
        let deletes_latest = self
            .get_latest()?
            .is_some_and(|latest| latest.snapshot_id.as_str() == snapshot_id);
        let next_latest = if deletes_latest {
            self.list(0)?
                .into_iter()
                .find(|candidate| candidate.snapshot_id != snapshot_id)
        } else {
            None
        };
        object_store
            .delete_objects_with_prefix(&build_snapshot_prefix(&self.snapshot_root, snapshot_id))?;
        if let Some(next_latest) = next_latest {
            object_store.upload_string(
                &build_latest_key(&self.snapshot_root),
                &next_latest.snapshot_id,
            )?;
        } else if deletes_latest {
            object_store.delete_objects_with_prefix(&build_latest_key(&self.snapshot_root))?;
        }
        Ok(())
    }

    fn get_snapshot_root(&self) -> &str {
        &self.snapshot_root
    }
}

pub struct RedisSnapshotCatalogStore {
    client: redis::Client,
    namespace: String,
    object_store: Arc<dyn SnapshotObjectStore>,
    snapshot_root: String,
}

impl RedisSnapshotCatalogStore {
    pub fn new(
        connstring: &str,
        namespace: impl Into<String>,
        object_store: Arc<dyn SnapshotObjectStore>,
    ) -> Result<Self, HaError> {
        let client = redis::Client::open(connstring)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog: {e}")))?;
        let namespace = namespace.into();
        let snapshot_root = build_snapshot_root(&namespace);
        Ok(Self {
            client,
            namespace,
            object_store,
            snapshot_root,
        })
    }

    fn latest_key(&self) -> String {
        format!(
            "mooncake-store/{{{}}}/snapshot/latest",
            sanitize_redis_hash_tag(&self.namespace)
        )
    }

    fn index_key(&self) -> String {
        format!(
            "mooncake-store/{{{}}}/snapshot/index",
            sanitize_redis_hash_tag(&self.namespace)
        )
    }

    fn connection(&self) -> Result<redis::Connection, HaError> {
        self.client
            .get_connection()
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog: {e}")))
    }
}

impl SnapshotCatalogStore for RedisSnapshotCatalogStore {
    fn publish(&self, snapshot: &SnapshotDescriptor) -> Result<(), HaError> {
        validate_snapshot_id(&snapshot.snapshot_id)?;
        self.object_store.upload_string(
            &build_descriptor_key(&self.snapshot_root, &snapshot.snapshot_id),
            &serialize_snapshot_descriptor(snapshot),
        )?;
        let mut connection = self.connection()?;
        redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(self.latest_key())
            .arg(&snapshot.snapshot_id)
            .ignore()
            .cmd("ZADD")
            .arg(self.index_key())
            .arg(snapshot.created_at_ms)
            .arg(&snapshot.snapshot_id)
            .ignore()
            .query::<()>(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog publish: {e}")))?;
        Ok(())
    }

    fn get_latest(&self) -> Result<Option<SnapshotDescriptor>, HaError> {
        let mut connection = self.connection()?;
        let snapshot_id: Option<String> = redis::cmd("GET")
            .arg(self.latest_key())
            .query(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog get latest: {e}")))?;
        let Some(snapshot_id) = snapshot_id else {
            return Ok(None);
        };
        validate_snapshot_id(&snapshot_id)?;
        match self
            .object_store
            .download_string(&build_descriptor_key(&self.snapshot_root, &snapshot_id))
        {
            Ok(payload) => {
                deserialize_snapshot_descriptor(&self.snapshot_root, &snapshot_id, &payload)
                    .map(Some)
            }
            Err(error) => Err(error),
        }
    }

    fn list(&self, limit: usize) -> Result<Vec<SnapshotDescriptor>, HaError> {
        let mut connection = self.connection()?;
        let stop = if limit == 0 {
            -1
        } else {
            limit.saturating_sub(1) as isize
        };
        let ids: Vec<String> = redis::cmd("ZREVRANGE")
            .arg(self.index_key())
            .arg(0)
            .arg(stop)
            .query(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog list: {e}")))?;
        let mut snapshots = Vec::new();
        for snapshot_id in ids {
            if !is_valid_snapshot_id(&snapshot_id) {
                tracing::warn!(snapshot_id, "skipping invalid redis snapshot catalog id");
                continue;
            }
            let payload = match self
                .object_store
                .download_string(&build_descriptor_key(&self.snapshot_root, &snapshot_id))
            {
                Ok(payload) => payload,
                Err(error) if self.object_store.is_not_found_error(&error.to_string()) => {
                    tracing::warn!(snapshot_id, %error, "snapshot descriptor disappeared from redis catalog listing");
                    continue;
                }
                Err(error) => return Err(error),
            };
            match deserialize_snapshot_descriptor(&self.snapshot_root, &snapshot_id, &payload) {
                Ok(descriptor) => snapshots.push(descriptor),
                Err(error) => {
                    tracing::warn!(snapshot_id, %error, "skipping corrupt redis snapshot descriptor");
                }
            }
        }
        Ok(snapshots)
    }

    fn delete(&self, snapshot_id: &str) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        let mut connection = self.connection()?;
        let latest: Option<String> = redis::cmd("GET")
            .arg(self.latest_key())
            .query(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog get latest: {e}")))?;
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("ZREM")
            .arg(self.index_key())
            .arg(snapshot_id)
            .ignore();
        if latest.as_deref() == Some(snapshot_id) {
            pipeline.cmd("DEL").arg(self.latest_key()).ignore();
        }
        // Commit catalog invisibility atomically before deleting payloads.
        // A later object-store failure leaves only unreachable garbage and
        // never a published descriptor pointing at missing data.
        pipeline
            .query::<()>(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog delete: {e}")))?;
        self.object_store
            .delete_objects_with_prefix(&build_snapshot_prefix(&self.snapshot_root, snapshot_id))?;
        Ok(())
    }

    fn get_snapshot_root(&self) -> &str {
        &self.snapshot_root
    }
}

fn run_async_sync<T, F>(future: F) -> Result<T, HaError>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, HaError>> + Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| HaError::Snapshot(e.to_string()))?
            .block_on(future)
    })
    .join()
    .map_err(|_| HaError::Snapshot("snapshot async bridge panicked".into()))?
}

fn next_s3_list_token(
    is_truncated: bool,
    next_token: Option<&str>,
) -> Result<Option<String>, HaError> {
    if !is_truncated {
        return Ok(None);
    }
    let token = next_token.unwrap_or("").trim();
    if token.is_empty() {
        return Err(HaError::Snapshot(
            "ListObjectsV2 error: truncated response missing next continuation token".into(),
        ));
    }
    Ok(Some(token.to_string()))
}

fn parse_bool_like(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

fn s3_force_path_style(use_virtual_addressing: Option<&str>) -> bool {
    !use_virtual_addressing.map(parse_bool_like).unwrap_or(true)
}

fn build_s3_endpoint_url(endpoint: Option<&str>, region: &str, use_https: bool) -> Option<String> {
    let scheme = if use_https { "https" } else { "http" };
    endpoint
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            let value = value.trim();
            if value.contains("://") {
                value.to_string()
            } else {
                format!("{scheme}://{value}")
            }
        })
        .or_else(|| (!use_https).then(|| format!("http://s3.{region}.amazonaws.com")))
}

fn parse_timeout_ms(value: Option<&str>, default_ms: u64) -> Duration {
    value
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(default_ms))
}

fn env_timeout_ms(name: &str, default_ms: u64) -> Duration {
    parse_timeout_ms(std::env::var(name).ok().as_deref(), default_ms)
}

fn parse_request_checksum_calculation(value: Option<&str>) -> Option<RequestChecksumCalculation> {
    match value?.to_ascii_lowercase().as_str() {
        "when_supported" => Some(RequestChecksumCalculation::WhenSupported),
        "when_required" => Some(RequestChecksumCalculation::WhenRequired),
        _ => None,
    }
}

fn parse_response_checksum_validation(value: Option<&str>) -> Option<ResponseChecksumValidation> {
    match value?.to_ascii_lowercase().as_str() {
        "when_supported" => Some(ResponseChecksumValidation::WhenSupported),
        "when_required" => Some(ResponseChecksumValidation::WhenRequired),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_buffer_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());
        let expected = vec![0, 1, 2, 128, 254, 255];

        store.upload_buffer("test/buf", &expected).unwrap();

        assert_eq!(store.download_buffer("test/buf").unwrap(), expected);
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_string_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());
        let expected = "hello mooncake snapshot";

        store.upload_string("test/str", expected).unwrap();

        assert_eq!(store.download_string("test/str").unwrap(), expected);
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_lists_exact_prefix_cardinality() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());
        store.upload_string("snap/20240101/metadata", "m").unwrap();
        store.upload_string("snap/20240101/segments", "s").unwrap();
        store.upload_string("snap/20240102/metadata", "m2").unwrap();

        assert_eq!(
            store.list_objects_with_prefix("snap/20240101/").unwrap(),
            vec![
                "snap/20240101/metadata".to_string(),
                "snap/20240101/segments".to_string(),
            ]
        );
        assert_eq!(
            store.list_objects_with_prefix("snap/").unwrap(),
            vec![
                "snap/20240101/metadata".to_string(),
                "snap/20240101/segments".to_string(),
                "snap/20240102/metadata".to_string(),
            ]
        );
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_delete_prefix_removes_objects() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());
        store.upload_string("snap/20240101/metadata", "m").unwrap();
        store.upload_string("snap/20240101/segments", "s").unwrap();

        store.delete_objects_with_prefix("snap/20240101/").unwrap();

        let error = store.download_string("snap/20240101/metadata").unwrap_err();
        assert!(matches!(error, HaError::Snapshot(_)));
        assert!(store.is_not_found_error(&error.to_string()));
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_connection_info_contains_base_path() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());

        assert!(
            store
                .connection_info()
                .contains(&root.path().display().to_string())
        );
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_deep_upload_creates_subdirectories() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());

        store.upload_buffer("a/b/c/deep_file", &[42]).unwrap();

        assert!(root.path().join("a/b/c").is_dir());
        assert_eq!(store.download_buffer("a/b/c/deep_file").unwrap(), vec![42]);
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_empty_path_panics() {
        let result = std::panic::catch_unwind(|| LocalFileSnapshotObjectStore::new(PathBuf::new()));

        assert!(result.is_err());
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_missing_buffer_download_errors() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());

        let error = store.download_buffer("no/such/key").unwrap_err();

        assert!(matches!(error, HaError::Snapshot(_)));
        assert!(store.is_not_found_error(&error.to_string()));
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_missing_string_download_errors() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());

        let error = store.download_string("no/such/key").unwrap_err();

        assert!(matches!(error, HaError::Snapshot(_)));
        assert!(store.is_not_found_error(&error.to_string()));
    }

    #[test]
    fn cpp_parity_local_file_snapshot_object_store_empty_buffer_upload_errors() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSnapshotObjectStore::new(root.path().to_path_buf());

        let error = store.upload_buffer("test/empty", &[]).unwrap_err();

        assert!(matches!(error, HaError::InvalidParams(_)));
        assert!(store.download_buffer("test/empty").is_err());
    }

    struct PayloadDeleteFailureStore {
        inner: LocalFileSnapshotObjectStore,
        failed_prefix: String,
    }

    impl SnapshotObjectStore for PayloadDeleteFailureStore {
        fn upload_buffer(&self, key: &str, buffer: &[u8]) -> Result<(), HaError> {
            self.inner.upload_buffer(key, buffer)
        }

        fn download_buffer(&self, key: &str) -> Result<Vec<u8>, HaError> {
            self.inner.download_buffer(key)
        }

        fn delete_objects_with_prefix(&self, prefix: &str) -> Result<(), HaError> {
            if prefix == self.failed_prefix {
                return Err(HaError::Snapshot(
                    "injected snapshot payload delete failure".into(),
                ));
            }
            self.inner.delete_objects_with_prefix(prefix)
        }

        fn list_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>, HaError> {
            self.inner.list_objects_with_prefix(prefix)
        }

        fn is_not_found_error(&self, error: &str) -> bool {
            self.inner.is_not_found_error(error)
        }

        fn connection_info(&self) -> String {
            self.inner.connection_info()
        }
    }

    #[test]
    fn embedded_delete_hides_descriptor_before_payload_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let snapshot_root = build_snapshot_root("");
        let old_id = "20260610_120000_001";
        let old_prefix = build_snapshot_prefix(&snapshot_root, old_id);
        let object_store = Arc::new(PayloadDeleteFailureStore {
            inner: LocalFileSnapshotObjectStore::new(root.path().to_path_buf()),
            failed_prefix: old_prefix.clone(),
        });
        let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());

        let old = SnapshotDescriptor::new_with_snapshot_root(&snapshot_root, old_id);
        catalog.publish(&old).unwrap();
        object_store
            .upload_string(&format!("{old_prefix}payload"), "old")
            .unwrap();
        let latest =
            SnapshotDescriptor::new_with_snapshot_root(&snapshot_root, "20260610_120001_002");
        catalog.publish(&latest).unwrap();

        let error = catalog.delete(old_id).unwrap_err();
        assert!(error.to_string().contains("injected"));
        assert_eq!(
            catalog.get_latest().unwrap().unwrap().snapshot_id,
            latest.snapshot_id
        );
        assert!(
            catalog
                .list(0)
                .unwrap()
                .into_iter()
                .all(|descriptor| descriptor.snapshot_id != old_id),
            "failed payload cleanup must not leave a visible descriptor"
        );
        assert_eq!(
            object_store
                .download_string(&format!("{old_prefix}payload"))
                .unwrap(),
            "old",
            "payload failure may leave unreachable garbage for later cleanup"
        );
    }

    #[test]
    fn test_s3_checksum_mode_parsing_matches_cpp_values() {
        assert_eq!(
            parse_request_checksum_calculation(Some("when_supported")),
            Some(RequestChecksumCalculation::WhenSupported)
        );
        assert_eq!(
            parse_request_checksum_calculation(Some("WHEN_REQUIRED")),
            Some(RequestChecksumCalculation::WhenRequired)
        );
        assert_eq!(parse_request_checksum_calculation(Some("invalid")), None);

        assert_eq!(
            parse_response_checksum_validation(Some("when_supported")),
            Some(ResponseChecksumValidation::WhenSupported)
        );
        assert_eq!(
            parse_response_checksum_validation(Some("WHEN_REQUIRED")),
            Some(ResponseChecksumValidation::WhenRequired)
        );
        assert_eq!(parse_response_checksum_validation(Some("invalid")), None);
    }

    #[test]
    fn test_s3_endpoint_url_honors_https_flag() {
        assert_eq!(
            build_s3_endpoint_url(Some("https://s3.example.com"), "us-east-1", false).as_deref(),
            Some("https://s3.example.com")
        );
        assert_eq!(
            build_s3_endpoint_url(Some("s3.example.com"), "us-east-1", false).as_deref(),
            Some("http://s3.example.com")
        );
        assert_eq!(
            build_s3_endpoint_url(None, "us-west-2", false).as_deref(),
            Some("http://s3.us-west-2.amazonaws.com")
        );
        assert_eq!(build_s3_endpoint_url(None, "us-west-2", true), None);
    }

    #[test]
    fn test_s3_virtual_addressing_defaults_match_cpp() {
        assert!(!s3_force_path_style(None));
        assert!(!s3_force_path_style(Some("true")));
        assert!(!s3_force_path_style(Some("1")));
        assert!(s3_force_path_style(Some("false")));
        assert!(s3_force_path_style(Some("0")));
    }

    #[test]
    fn test_s3_timeout_ms_parsing_uses_cpp_defaults() {
        assert_eq!(
            parse_timeout_ms(Some("5000"), DEFAULT_S3_CONNECT_TIMEOUT_MS),
            Duration::from_millis(5000)
        );
        assert_eq!(
            parse_timeout_ms(Some("bogus"), DEFAULT_S3_REQUEST_TIMEOUT_MS),
            Duration::from_millis(DEFAULT_S3_REQUEST_TIMEOUT_MS)
        );
        assert_eq!(
            parse_timeout_ms(None, DEFAULT_S3_CONNECT_TIMEOUT_MS),
            Duration::from_millis(DEFAULT_S3_CONNECT_TIMEOUT_MS)
        );
    }

    #[test]
    fn test_s3_list_v2_requires_token_when_truncated() {
        assert_eq!(next_s3_list_token(false, None).unwrap(), None);
        assert_eq!(
            next_s3_list_token(true, Some(" next-page ")).unwrap(),
            Some("next-page".to_string())
        );

        let error = next_s3_list_token(true, None).unwrap_err().to_string();
        assert!(error.contains("truncated response missing next continuation token"));
        let empty_error = next_s3_list_token(true, Some("")).unwrap_err().to_string();
        assert!(empty_error.contains("truncated response missing next continuation token"));
    }

    #[test]
    fn snapshot_descriptor_v1_preserves_cpp_wire_shape() {
        let mut descriptor = SnapshotDescriptor::new_with_snapshot_root("root/", "snapshot-id");
        descriptor.last_included_seq = 42;
        descriptor.producer_view_version = 7;
        descriptor.created_at_ms = 1234;

        let encoded = serialize_snapshot_descriptor(&descriptor);
        assert_eq!(encoded, "42|7|1234");
        assert_eq!(
            encoded.split('|').count(),
            SNAPSHOT_DESCRIPTOR_V1_FIELD_COUNT
        );

        let decoded = deserialize_snapshot_descriptor("root/", "snapshot-id", &encoded).unwrap();
        assert_eq!(decoded, descriptor);
        assert!(deserialize_snapshot_descriptor("root/", "snapshot-id", "v2|42|7|1234").is_err());
    }
}

fn sanitize_redis_hash_tag(value: &str) -> String {
    value
        .strip_suffix('/')
        .unwrap_or(value)
        .replace(['{', '}'], "_")
}

fn build_snapshot_root(cluster_id: &str) -> String {
    if cluster_id.is_empty() {
        format!("{SNAPSHOT_CATALOG_ROOT}/")
    } else {
        format!("{SNAPSHOT_CATALOG_ROOT}/{cluster_id}/")
    }
}

fn build_snapshot_prefix(snapshot_root: &str, snapshot_id: &str) -> String {
    format!("{snapshot_root}{snapshot_id}/")
}

fn build_descriptor_key(snapshot_root: &str, snapshot_id: &str) -> String {
    format!(
        "{}{SNAPSHOT_DESCRIPTOR_FILE}",
        build_snapshot_prefix(snapshot_root, snapshot_id)
    )
}

fn build_latest_key(snapshot_root: &str) -> String {
    format!("{snapshot_root}{SNAPSHOT_LATEST_FILE}")
}

fn serialize_snapshot_descriptor(descriptor: &SnapshotDescriptor) -> String {
    // Descriptor v1 intentionally has no textual version prefix. C++ readers
    // consume this exact three-number layout.
    format!(
        "{}|{}|{}",
        descriptor.last_included_seq, descriptor.producer_view_version, descriptor.created_at_ms
    )
}

fn deserialize_snapshot_descriptor(
    snapshot_root: &str,
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

    let mut descriptor = SnapshotDescriptor::new_with_snapshot_root(snapshot_root, snapshot_id);
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

    /// Return restore candidates in newest-first order.
    ///
    /// Providers without a durable catalog expose at most their latest
    /// snapshot. Catalog-backed providers override this to preserve the C++
    /// fallback contract: a corrupt or semantically invalid latest snapshot
    /// must not hide an older usable baseline.
    fn load_snapshot_candidates(&self, cluster_id: &str) -> Result<Vec<LoadedSnapshot>, HaError> {
        Ok(self.load_latest_snapshot(cluster_id)?.into_iter().collect())
    }
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
            let Some((
                snapshot_sequence_id,
                segments,
                nof_segments,
                objects,
                tasks,
                replication_tasks,
                graceful_unmounts,
                delayed_replica_releases,
                local_disk_segments,
                allocator_config,
            )) = backend
                .load_with_runtime_state_and_sequence()
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
            return Ok(Some(LoadedSnapshot {
                snapshot_id,
                snapshot_sequence_id,
                allocator_config,
                segments,
                nof_segments,
                objects,
                tasks,
                replication_tasks,
                graceful_unmounts,
                delayed_replica_releases,
                local_disk_segments,
            }));
        }

        Ok(None)
    }
}
