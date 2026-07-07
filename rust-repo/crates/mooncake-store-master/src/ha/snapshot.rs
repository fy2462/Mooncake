use super::types::HaError;
use crate::service::NoFSegmentEntry;
use crate::service::ObjectEntry;
use crate::service::SegmentEntry;
use crate::service::TaskEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use std::path::PathBuf;
use std::sync::Arc;
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
    pub segments: Vec<SegmentEntry>,
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
}

impl SnapshotObjectStore for LocalFileSnapshotObjectStore {
    fn upload_buffer(&self, key: &str, buffer: &[u8]) -> Result<(), HaError> {
        let path = self.key_path(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| HaError::Snapshot(e.to_string()))?;
        }
        std::fs::write(path, buffer).map_err(|e| HaError::Snapshot(e.to_string()))
    }

    fn download_buffer(&self, key: &str) -> Result<Vec<u8>, HaError> {
        std::fs::read(self.key_path(key)?).map_err(|e| HaError::Snapshot(e.to_string()))
    }

    fn delete_objects_with_prefix(&self, prefix: &str) -> Result<(), HaError> {
        let path = self.key_path(prefix.trim_end_matches('/'))?;
        if path.is_dir() {
            std::fs::remove_dir_all(path).map_err(|e| HaError::Snapshot(e.to_string()))?;
        } else if path.exists() {
            std::fs::remove_file(path).map_err(|e| HaError::Snapshot(e.to_string()))?;
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
        let force_path_style = std::env::var("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING")
            .map(|value| !parse_bool_like(&value))
            .unwrap_or(endpoint.is_some());
        let prefix = std::env::var("MOONCAKE_AWS_S3_PREFIX").unwrap_or_default();
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
                .region(Region::new(region));
            if let (Some(key), Some(secret)) = (access_key, secret_key) {
                sdk_config = sdk_config.credentials_provider(Credentials::new(
                    key,
                    secret,
                    None,
                    None,
                    "mooncake-master-snapshot",
                ));
            }
            if let Some(endpoint) = endpoint {
                sdk_config = sdk_config.endpoint_url(endpoint);
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
                token = output.next_continuation_token().map(ToString::to_string);
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
    object_store: Arc<dyn SnapshotObjectStore>,
}

impl EmbeddedSnapshotCatalogStore {
    pub fn new(root_dir: PathBuf) -> Self {
        Self::with_object_store(Arc::new(LocalFileSnapshotObjectStore::new(root_dir)))
    }

    pub fn with_object_store(object_store: Arc<dyn SnapshotObjectStore>) -> Self {
        Self { object_store }
    }
}

impl SnapshotCatalogStore for EmbeddedSnapshotCatalogStore {
    fn publish(&self, snapshot: &SnapshotDescriptor) -> Result<(), HaError> {
        validate_snapshot_id(&snapshot.snapshot_id)?;
        self.object_store.upload_string(
            &build_descriptor_key(&snapshot.snapshot_id),
            &serialize_snapshot_descriptor(snapshot),
        )?;
        self.object_store
            .upload_string(&build_latest_key(), &snapshot.snapshot_id)?;
        Ok(())
    }

    fn get_latest(&self) -> Result<Option<SnapshotDescriptor>, HaError> {
        let snapshot_id = match self.object_store.download_string(&build_latest_key()) {
            Ok(value) => value.trim().to_string(),
            Err(error) if self.object_store.is_not_found_error(&error.to_string()) => {
                return Ok(None)
            }
            Err(error) => return Err(error),
        };
        if snapshot_id.is_empty() {
            return Ok(None);
        }
        validate_snapshot_id(&snapshot_id)?;
        let payload = self
            .object_store
            .download_string(&build_descriptor_key(&snapshot_id))?;
        deserialize_snapshot_descriptor(&snapshot_id, &payload).map(Some)
    }

    fn list(&self, limit: usize) -> Result<Vec<SnapshotDescriptor>, HaError> {
        let descriptor_suffix = format!("/{SNAPSHOT_DESCRIPTOR_FILE}");
        let mut ids: Vec<String> = self
            .object_store
            .list_objects_with_prefix(SNAPSHOT_CATALOG_ROOT)?
            .into_iter()
            .filter_map(|key| {
                let trimmed = key.strip_prefix(&format!("{SNAPSHOT_CATALOG_ROOT}/"))?;
                let snapshot_id = trimmed.strip_suffix(&descriptor_suffix)?;
                is_valid_snapshot_id(snapshot_id).then(|| snapshot_id.to_string())
            })
            .collect();
        ids.sort_by(|a, b| b.cmp(a));
        if limit != 0 {
            ids.truncate(limit);
        }

        let mut snapshots = Vec::new();
        for id in ids {
            if let Ok(payload) = self
                .object_store
                .download_string(&build_descriptor_key(&id))
            {
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
        self.object_store
            .delete_objects_with_prefix(&build_snapshot_prefix(snapshot_id))?;
        if deletes_latest {
            let _ = self
                .object_store
                .delete_objects_with_prefix(&build_latest_key());
        }
        Ok(())
    }
}

pub struct RedisSnapshotCatalogStore {
    client: redis::Client,
    namespace: String,
    object_store: Arc<dyn SnapshotObjectStore>,
}

impl RedisSnapshotCatalogStore {
    pub fn new(
        connstring: &str,
        namespace: impl Into<String>,
        object_store: Arc<dyn SnapshotObjectStore>,
    ) -> Result<Self, HaError> {
        let client = redis::Client::open(connstring)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog: {e}")))?;
        Ok(Self {
            client,
            namespace: namespace.into(),
            object_store,
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
            &build_descriptor_key(&snapshot.snapshot_id),
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
            .download_string(&build_descriptor_key(&snapshot_id))
        {
            Ok(payload) => deserialize_snapshot_descriptor(&snapshot_id, &payload).map(Some),
            Err(error) if self.object_store.is_not_found_error(&error.to_string()) => Ok(None),
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
                continue;
            }
            if let Ok(payload) = self
                .object_store
                .download_string(&build_descriptor_key(&snapshot_id))
            {
                if let Ok(descriptor) = deserialize_snapshot_descriptor(&snapshot_id, &payload) {
                    snapshots.push(descriptor);
                }
            }
        }
        Ok(snapshots)
    }

    fn delete(&self, snapshot_id: &str) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        self.object_store
            .delete_objects_with_prefix(&build_snapshot_prefix(snapshot_id))?;
        let mut connection = self.connection()?;
        let latest: Option<String> = redis::cmd("GET")
            .arg(self.latest_key())
            .query(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog get latest: {e}")))?;
        redis::cmd("ZREM")
            .arg(self.index_key())
            .arg(snapshot_id)
            .query::<()>(&mut connection)
            .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog delete: {e}")))?;
        if latest.as_deref() == Some(snapshot_id) {
            redis::cmd("DEL")
                .arg(self.latest_key())
                .query::<()>(&mut connection)
                .map_err(|e| HaError::Snapshot(format!("redis snapshot catalog delete: {e}")))?;
        }
        Ok(())
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

fn parse_bool_like(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
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
}

fn sanitize_redis_hash_tag(value: &str) -> String {
    value
        .strip_suffix('/')
        .unwrap_or(value)
        .replace(['{', '}'], "_")
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
                segments,
                nof_segments,
                objects,
                tasks,
            }));
        }

        Ok(None)
    }
}
