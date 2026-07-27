use super::distributed_ffi::{DistributedFileSystem, create_hf3fs_adapter};
use super::{DistributedStorageConfig, LocalStorageRecordMetadata};
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

const DISTRIBUTED_FORMAT_FILE: &str = ".mooncake-distributed-format";
const DISTRIBUTED_STORAGE_ID_FILE: &str = ".mooncake-storage-id";
const DISTRIBUTED_RECORD_MAGIC: &[u8; 8] = b"MCDIST01";
const DISTRIBUTED_RECORD_VERSION: u32 = 1;
const DISTRIBUTED_RECORD_HEADER_SIZE: usize = 8 + 4 + 8 + 8 + 16;
const DISTRIBUTED_TEMP_PREFIX: &str = "%%mooncake-tmp-";

#[derive(Clone, Debug, PartialEq, Eq)]
struct DistributedRecord {
    value_size: u64,
    generation_id: Uuid,
}

#[derive(Debug, Default)]
struct DistributedReservationRegistry {
    keys: Mutex<HashMap<String, u64>>,
}

impl DistributedReservationRegistry {
    fn release(&self, key: &str, token: u64) {
        if token == 0 {
            return;
        }
        let mut keys = self.keys.lock();
        if keys.get(key).copied() == Some(token) {
            keys.remove(key);
        }
    }
}

#[derive(Debug)]
pub(crate) struct PendingDistributedWrite {
    backend_id: Uuid,
    token: u64,
    storage_key: Option<String>,
    required_value_size: Option<u64>,
    previous: Option<DistributedRecord>,
    registry: Option<Arc<DistributedReservationRegistry>>,
}

impl Default for PendingDistributedWrite {
    fn default() -> Self {
        Self {
            backend_id: Uuid::nil(),
            token: 0,
            storage_key: None,
            required_value_size: None,
            previous: None,
            registry: None,
        }
    }
}

impl Drop for PendingDistributedWrite {
    fn drop(&mut self) {
        if let (Some(registry), Some(storage_key)) = (&self.registry, &self.storage_key) {
            registry.release(storage_key, self.token);
        }
    }
}

impl PendingDistributedWrite {
    pub(crate) fn keys(&self) -> Vec<String> {
        Vec::new()
    }

    pub(crate) fn partition_accepted(self, _accepted_keys: &HashSet<String>) -> (Self, Self) {
        (Self::default(), self)
    }
}

struct AtomicPublishFailure {
    installed: bool,
    error: StoreError,
}

/// Client-side Distributed/HF3FS storage used by the real offload/promotion
/// path.
///
/// C++ path parity:
/// - XXH64(key, seed=0) hash buckets;
/// - the same percent-escaped filename mapping;
/// - HF3FS fd registration and USRBIO data I/O;
/// - no capacity eviction.
///
/// Rust adds an authenticated record envelope carrying the logical key and
/// generation UUID. C++/Rust directory interoperability is intentionally not
/// supported because the Rust Store is a replacement, not a mixed-deployment
/// peer.
pub struct DistributedStorageBackend {
    backend_id: Uuid,
    config: DistributedStorageConfig,
    adapter: Arc<dyn DistributedFileSystem>,
    init_lock: Mutex<()>,
    mutation_lock: Mutex<()>,
    initialized: AtomicBool,
    storage_id: RwLock<Option<Uuid>>,
    records: RwLock<HashMap<String, DistributedRecord>>,
    used_bytes: AtomicU64,
    total_bytes: AtomicU64,
    next_token: AtomicU64,
    reservations: Arc<DistributedReservationRegistry>,
}

impl DistributedStorageBackend {
    pub fn new(config: DistributedStorageConfig) -> Self {
        Self::new_with_adapter(config, create_hf3fs_adapter())
    }

    fn new_with_adapter(
        config: DistributedStorageConfig,
        adapter: Arc<dyn DistributedFileSystem>,
    ) -> Self {
        Self {
            backend_id: Uuid::new_v4(),
            config,
            adapter,
            init_lock: Mutex::new(()),
            mutation_lock: Mutex::new(()),
            initialized: AtomicBool::new(false),
            storage_id: RwLock::new(None),
            records: RwLock::new(HashMap::new()),
            used_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            next_token: AtomicU64::new(1),
            reservations: Arc::new(DistributedReservationRegistry::default()),
        }
    }

    #[cfg(test)]
    fn new_for_test(config: DistributedStorageConfig) -> Self {
        Self::new_with_adapter(config, super::distributed_ffi::create_posix_test_adapter())
    }

    pub fn init(&self) -> StoreResult<()> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        let _init_guard = self.init_lock.lock();
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        self.config.validate().map_err(StoreError::InvalidParams)?;
        self.adapter.init(&self.config.fsdir)?;
        self.validate_directory(&self.config.fsdir)?;
        self.install_or_validate_format()?;
        let storage_id = self.load_or_create_storage_id()?;

        for bucket in 0..self.config.hash_bucket_count {
            let directory = self.bucket_directory(bucket);
            match std::fs::symlink_metadata(&directory) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed bucket is not a regular directory: {}",
                        directory.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&directory)?;
                    sync_directory(&self.config.fsdir)?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        if self.config.enable_health_check {
            self.run_health_check()?;
        }

        let records = self.scan_namespace()?;
        let used_bytes = records.values().try_fold(0_u64, |used, record| {
            used.checked_add(record.value_size).ok_or_else(|| {
                StoreError::Internal("distributed storage usage overflow".to_string())
            })
        })?;
        let total_bytes = fs2::total_space(&self.config.fsdir).unwrap_or(0);
        *self.records.write() = records;
        *self.storage_id.write() = Some(storage_id);
        self.used_bytes.store(used_bytes, Ordering::Release);
        self.total_bytes.store(total_bytes, Ordering::Release);
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    fn ensure_initialized(&self) -> StoreResult<()> {
        self.init()
    }

    fn validate_directory(&self, directory: &Path) -> StoreResult<()> {
        let metadata = std::fs::symlink_metadata(directory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StoreError::InvalidParams(format!(
                "distributed root is not a regular directory: {}",
                directory.display()
            )));
        }
        Ok(())
    }

    fn expected_format(&self) -> String {
        format!(
            "backend=distributed\nformat=rust-generation-envelope\nversion={DISTRIBUTED_RECORD_VERSION}\npath_scheme=xxh64-percent-v1\nhash_bucket_count={}\nadapter=hf3fs\n",
            self.config.hash_bucket_count
        )
    }

    fn install_or_validate_format(&self) -> StoreResult<()> {
        let path = self.config.fsdir.join(DISTRIBUTED_FORMAT_FILE);
        let expected = self.expected_format();
        match self.read_metadata_file(&path) {
            Ok(existing) if existing == expected.as_bytes() => Ok(()),
            Ok(_) => Err(StoreError::InvalidParams(format!(
                "distributed format marker does not match configuration: {}",
                path.display()
            ))),
            Err(StoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                self.install_metadata_file(&path, expected.as_bytes())?;
                if self.read_metadata_file(&path)? != expected.as_bytes() {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed format marker raced with an incompatible writer: {}",
                        path.display()
                    )));
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn load_or_create_storage_id(&self) -> StoreResult<Uuid> {
        let path = self.config.fsdir.join(DISTRIBUTED_STORAGE_ID_FILE);
        match self.read_metadata_file(&path) {
            Ok(contents) => parse_storage_id(&path, &contents),
            Err(StoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                let storage_id = Uuid::new_v4();
                let contents = format!("{storage_id}\n");
                self.install_metadata_file(&path, contents.as_bytes())?;
                parse_storage_id(&path, &self.read_metadata_file(&path)?)
            }
            Err(error) => Err(error),
        }
    }

    fn read_metadata_file(&self, path: &Path) -> StoreResult<Vec<u8>> {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StoreError::InvalidParams(format!(
                "distributed metadata path is not a regular file: {}",
                path.display()
            )));
        }
        self.adapter.read_file(path)
    }

    fn install_metadata_file(&self, path: &Path, contents: &[u8]) -> StoreResult<()> {
        match self.adapter.write_file(path, contents) {
            Ok(()) => sync_directory(&self.config.fsdir),
            Err(StoreError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn run_health_check(&self) -> StoreResult<()> {
        let path = self.config.fsdir.join(format!(
            "{DISTRIBUTED_TEMP_PREFIX}health-{}",
            Uuid::new_v4()
        ));
        let expected = b"health_check";
        self.adapter.write_file(&path, expected)?;
        let result = self.adapter.read_file(&path);
        let cleanup = self.adapter.delete_file(&path);
        let actual = result?;
        cleanup?;
        if actual != expected {
            return Err(StoreError::ServiceUnavailable);
        }
        Ok(())
    }

    fn bucket_directory(&self, bucket: usize) -> PathBuf {
        self.config.fsdir.join(format!("{bucket:02x}"))
    }

    fn bucket_for_key(&self, storage_key: &str) -> usize {
        (xxh64(storage_key.as_bytes(), 0) % self.config.hash_bucket_count as u64) as usize
    }

    fn object_path(&self, storage_key: &str) -> PathBuf {
        self.bucket_directory(self.bucket_for_key(storage_key))
            .join(escape_filename(storage_key))
    }

    fn scan_namespace(&self) -> StoreResult<HashMap<String, DistributedRecord>> {
        let mut records = HashMap::new();
        for bucket in 0..self.config.hash_bucket_count {
            let directory = self.bucket_directory(bucket);
            for name in self.adapter.list_files(&directory)? {
                if name.starts_with(DISTRIBUTED_TEMP_PREFIX) {
                    continue;
                }
                let storage_key = unescape_filename(&name)?;
                if storage_key.is_empty() || escape_filename(&storage_key) != name {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed filename is not canonical: {name:?}"
                    )));
                }
                if self.bucket_for_key(&storage_key) != bucket {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed object is in the wrong hash bucket: {name:?}"
                    )));
                }
                let path = directory.join(&name);
                let contents = self.adapter.read_file(&path)?;
                let envelope = decode_record(&contents)?;
                if envelope.storage_key != storage_key {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed record key mismatch at {}",
                        path.display()
                    )));
                }
                if records
                    .insert(
                        storage_key.clone(),
                        DistributedRecord {
                            value_size: envelope.value.len() as u64,
                            generation_id: envelope.generation_id,
                        },
                    )
                    .is_some()
                {
                    return Err(StoreError::InvalidParams(format!(
                        "duplicate distributed record for {storage_key:?}"
                    )));
                }
            }
        }
        Ok(records)
    }

    pub fn storage_id(&self) -> StoreResult<Uuid> {
        self.ensure_initialized()?;
        self.storage_id
            .read()
            .ok_or_else(|| StoreError::Internal("distributed storage id is missing".to_string()))
    }

    /// Return `(used_bytes, filesystem_total_bytes)`.
    pub fn space_usage(&self) -> (u64, u64) {
        if let Err(error) = self.ensure_initialized() {
            tracing::error!(%error, "failed to initialize distributed storage for usage");
            return (0, 0);
        }
        (
            self.used_bytes.load(Ordering::Acquire),
            self.total_bytes.load(Ordering::Acquire),
        )
    }

    pub(crate) fn prepare_write(
        &self,
        storage_key: &str,
        required_value_size: u64,
    ) -> StoreResult<PendingDistributedWrite> {
        self.ensure_initialized()?;
        if storage_key.is_empty() {
            return Err(StoreError::InvalidParams(
                "distributed storage key must not be empty".to_string(),
            ));
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed).max(1);
        let mut reservations = self.reservations.keys.lock();
        if reservations.contains_key(storage_key) {
            return Err(StoreError::NoAvailableHandle);
        }
        let previous = self.records.read().get(storage_key).cloned();
        match &previous {
            Some(record) => {
                let contents = self.adapter.read_file(&self.object_path(storage_key))?;
                let envelope = decode_record(&contents)?;
                validate_envelope(storage_key, record, &envelope)?;
            }
            None if self.adapter.file_exists(&self.object_path(storage_key))? => {
                return Err(StoreError::InvalidParams(format!(
                    "distributed backend refuses to overwrite untracked record {storage_key:?}"
                )));
            }
            None => {}
        }
        reservations.insert(storage_key.to_string(), token);
        Ok(PendingDistributedWrite {
            backend_id: self.backend_id,
            token,
            storage_key: Some(storage_key.to_string()),
            required_value_size: Some(required_value_size),
            previous,
            registry: Some(Arc::clone(&self.reservations)),
        })
    }

    fn validate_pending_write(
        &self,
        storage_key: &str,
        value_size: u64,
        pending: &PendingDistributedWrite,
    ) -> StoreResult<()> {
        if pending.backend_id != self.backend_id
            || pending.storage_key.as_deref() != Some(storage_key)
            || pending.required_value_size != Some(value_size)
            || pending.token == 0
            || self.reservations.keys.lock().get(storage_key).copied() != Some(pending.token)
        {
            return Err(StoreError::InvalidParams(
                "distributed pending write does not match this backend/key/size".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn commit_write_with_generation(
        &self,
        storage_key: &str,
        data: &[u8],
        pending: PendingDistributedWrite,
        generation_id: Uuid,
    ) -> StoreResult<()> {
        self.ensure_initialized()?;
        if generation_id.is_nil() {
            return Err(StoreError::InvalidParams(
                "distributed generation must not be nil".to_string(),
            ));
        }
        let value_size = u64::try_from(data.len()).map_err(|_| {
            StoreError::InvalidParams("distributed value size exceeds u64".to_string())
        })?;
        self.validate_pending_write(storage_key, value_size, &pending)?;
        let _mutation_guard = self.mutation_lock.lock();
        if self.records.read().get(storage_key) != pending.previous.as_ref() {
            return Err(StoreError::InvalidParams(format!(
                "distributed record changed after write reservation for {storage_key:?}"
            )));
        }
        match &pending.previous {
            Some(record) => {
                let contents = self.adapter.read_file(&self.object_path(storage_key))?;
                let envelope = decode_record(&contents)?;
                validate_envelope(storage_key, record, &envelope)?;
            }
            None if self.adapter.file_exists(&self.object_path(storage_key))? => {
                return Err(StoreError::InvalidParams(format!(
                    "untracked distributed record appeared after reservation for {storage_key:?}"
                )));
            }
            None => {}
        }

        let encoded = encode_record(storage_key, data, generation_id)?;
        let path = self.object_path(storage_key);
        let previous_size = pending
            .previous
            .as_ref()
            .map_or(0, |record| record.value_size);
        let new_used = self
            .used_bytes
            .load(Ordering::Acquire)
            .checked_sub(previous_size)
            .ok_or_else(|| StoreError::Internal("distributed usage underflow".to_string()))?
            .checked_add(value_size)
            .ok_or_else(|| StoreError::Internal("distributed usage overflow".to_string()))?;
        let publish_failure = match self.publish_record(&path, &encoded) {
            Ok(()) => None,
            Err(failure) if failure.installed => Some(failure.error),
            Err(failure) => return Err(failure.error),
        };
        self.records.write().insert(
            storage_key.to_string(),
            DistributedRecord {
                value_size,
                generation_id,
            },
        );
        self.used_bytes.store(new_used, Ordering::Release);
        match publish_failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn publish_record(&self, path: &Path, contents: &[u8]) -> Result<(), AtomicPublishFailure> {
        let error = |installed, error| AtomicPublishFailure { installed, error };
        let parent = path.parent().ok_or_else(|| {
            error(
                false,
                StoreError::InvalidParams("distributed object path has no parent".to_string()),
            )
        })?;
        let temporary = parent.join(format!("{DISTRIBUTED_TEMP_PREFIX}{}", Uuid::new_v4()));
        if let Err(write_error) = self.adapter.write_file(&temporary, contents) {
            return Err(error(false, write_error));
        }
        if let Err(rename_error) = std::fs::rename(&temporary, path) {
            let _ = self.adapter.delete_file(&temporary);
            return Err(error(false, rename_error.into()));
        }
        sync_directory(parent).map_err(|failure| error(true, failure))
    }

    pub(crate) fn rollback_eviction(&self, _pending: PendingDistributedWrite) {}

    pub(crate) fn prepare_watermark_eviction(
        &self,
        high: f64,
        low: f64,
    ) -> StoreResult<PendingDistributedWrite> {
        self.ensure_initialized()?;
        if !(0.0 < low && low < high && high <= 1.0) {
            return Err(StoreError::InvalidParams(format!(
                "invalid distributed watermarks: high={high}, low={low}"
            )));
        }
        // C++ DistributedStorageBackend explicitly does not support eviction.
        Ok(PendingDistributedWrite::default())
    }

    pub(crate) fn commit_eviction(&self, pending: PendingDistributedWrite) -> StoreResult<()> {
        if pending.storage_key.is_some() {
            return Err(StoreError::InvalidParams(
                "distributed write reservation cannot be committed as an eviction".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn read_object(&self, storage_key: &str) -> StoreResult<Vec<u8>> {
        self.ensure_initialized()?;
        let _mutation_guard = self.mutation_lock.lock();
        let expected = self
            .records
            .read()
            .get(storage_key)
            .cloned()
            .ok_or_else(|| StoreError::KeyNotFound(storage_key.to_string()))?;
        let contents = self.adapter.read_file(&self.object_path(storage_key))?;
        let envelope = decode_record(&contents)?;
        validate_envelope(storage_key, &expected, &envelope)?;
        Ok(envelope.value.to_vec())
    }

    pub(crate) fn delete_object(&self, storage_key: &str) -> StoreResult<()> {
        self.ensure_initialized()?;
        let _mutation_guard = self.mutation_lock.lock();
        let Some(record) = self.records.read().get(storage_key).cloned() else {
            if self.adapter.file_exists(&self.object_path(storage_key))? {
                return Err(StoreError::InvalidParams(format!(
                    "distributed backend refuses to delete untracked record {storage_key:?}"
                )));
            }
            return Ok(());
        };
        self.delete_record(storage_key, &record)
    }

    pub(crate) fn delete_object_if_generation(
        &self,
        storage_key: &str,
        generation_id: Uuid,
    ) -> StoreResult<bool> {
        if generation_id.is_nil() {
            return Err(StoreError::InvalidParams(
                "distributed generation must not be nil".to_string(),
            ));
        }
        self.ensure_initialized()?;
        let _mutation_guard = self.mutation_lock.lock();
        let Some(record) = self.records.read().get(storage_key).cloned() else {
            return Ok(false);
        };
        if record.generation_id != generation_id {
            return Ok(false);
        }
        self.delete_record(storage_key, &record)?;
        Ok(true)
    }

    fn delete_record(&self, storage_key: &str, expected: &DistributedRecord) -> StoreResult<()> {
        let path = self.object_path(storage_key);
        let contents = self.adapter.read_file(&path)?;
        let envelope = decode_record(&contents)?;
        validate_envelope(storage_key, expected, &envelope)?;
        let new_used = self
            .used_bytes
            .load(Ordering::Acquire)
            .checked_sub(expected.value_size)
            .ok_or_else(|| StoreError::Internal("distributed usage underflow".to_string()))?;
        self.adapter.delete_file(&path)?;
        sync_directory(path.parent().ok_or_else(|| {
            StoreError::InvalidParams("distributed object path has no parent".to_string())
        })?)?;
        self.records.write().remove(storage_key);
        self.used_bytes.store(new_used, Ordering::Release);
        Ok(())
    }

    pub(crate) fn remove_all(&self) -> StoreResult<usize> {
        self.ensure_initialized()?;
        let _mutation_guard = self.mutation_lock.lock();
        let records = self.records.read().clone();
        for (storage_key, record) in &records {
            self.delete_record(storage_key, record)?;
        }
        Ok(records.len())
    }

    pub(crate) fn scan_meta(&self) -> StoreResult<Vec<(String, u64)>> {
        Ok(self
            .scan_records()?
            .into_iter()
            .map(|record| (record.storage_key, record.value_size))
            .collect())
    }

    pub(crate) fn scan_records(&self) -> StoreResult<Vec<LocalStorageRecordMetadata>> {
        self.ensure_initialized()?;
        let mut records = self
            .records
            .read()
            .iter()
            .map(|(storage_key, record)| LocalStorageRecordMetadata {
                storage_key: storage_key.clone(),
                value_size: record.value_size,
                generation_id: record.generation_id,
            })
            .collect::<Vec<_>>();
        records.sort_unstable_by(|left, right| left.storage_key.cmp(&right.storage_key));
        Ok(records)
    }

    #[cfg(test)]
    fn adapter_name(&self) -> &'static str {
        self.adapter.name()
    }
}

fn sync_directory(directory: &Path) -> StoreResult<()> {
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

fn parse_storage_id(path: &Path, contents: &[u8]) -> StoreResult<Uuid> {
    let value = std::str::from_utf8(contents)?.trim();
    let storage_id = Uuid::parse_str(value).map_err(|error| {
        StoreError::InvalidParams(format!(
            "invalid distributed storage id {}: {error}",
            path.display()
        ))
    })?;
    if storage_id.is_nil() {
        return Err(StoreError::InvalidParams(format!(
            "distributed storage id must not be nil: {}",
            path.display()
        )));
    }
    Ok(storage_id)
}

struct DecodedRecord<'a> {
    storage_key: &'a str,
    value: &'a [u8],
    generation_id: Uuid,
}

fn encode_record(storage_key: &str, value: &[u8], generation_id: Uuid) -> StoreResult<Vec<u8>> {
    if storage_key.is_empty() || generation_id.is_nil() {
        return Err(StoreError::InvalidParams(
            "distributed record key/generation must not be empty".to_string(),
        ));
    }
    let key_len = u64::try_from(storage_key.len())
        .map_err(|_| StoreError::InvalidParams("distributed key is too large".to_string()))?;
    let value_len = u64::try_from(value.len())
        .map_err(|_| StoreError::InvalidParams("distributed value is too large".to_string()))?;
    let capacity = DISTRIBUTED_RECORD_HEADER_SIZE
        .checked_add(storage_key.len())
        .and_then(|size| size.checked_add(value.len()))
        .ok_or_else(|| StoreError::InvalidParams("distributed record size overflow".to_string()))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(DISTRIBUTED_RECORD_MAGIC);
    output.extend_from_slice(&DISTRIBUTED_RECORD_VERSION.to_le_bytes());
    output.extend_from_slice(&key_len.to_le_bytes());
    output.extend_from_slice(&value_len.to_le_bytes());
    output.extend_from_slice(generation_id.as_bytes());
    output.extend_from_slice(storage_key.as_bytes());
    output.extend_from_slice(value);
    Ok(output)
}

fn decode_record(input: &[u8]) -> StoreResult<DecodedRecord<'_>> {
    if input.len() < DISTRIBUTED_RECORD_HEADER_SIZE || &input[..8] != DISTRIBUTED_RECORD_MAGIC {
        return Err(StoreError::InvalidParams(
            "invalid distributed record magic/length".to_string(),
        ));
    }
    let version = u32::from_le_bytes(input[8..12].try_into().unwrap());
    if version != DISTRIBUTED_RECORD_VERSION {
        return Err(StoreError::InvalidParams(format!(
            "unsupported distributed record version {version}"
        )));
    }
    let key_len = usize::try_from(u64::from_le_bytes(input[12..20].try_into().unwrap()))
        .map_err(|_| StoreError::InvalidParams("distributed key length overflow".to_string()))?;
    let value_len = usize::try_from(u64::from_le_bytes(input[20..28].try_into().unwrap()))
        .map_err(|_| StoreError::InvalidParams("distributed value length overflow".to_string()))?;
    let generation_id = Uuid::from_slice(&input[28..44]).map_err(|error| {
        StoreError::InvalidParams(format!("invalid distributed generation: {error}"))
    })?;
    if generation_id.is_nil() {
        return Err(StoreError::InvalidParams(
            "distributed record generation must not be nil".to_string(),
        ));
    }
    let key_end = DISTRIBUTED_RECORD_HEADER_SIZE
        .checked_add(key_len)
        .ok_or_else(|| StoreError::InvalidParams("distributed key end overflow".to_string()))?;
    let record_end = key_end
        .checked_add(value_len)
        .ok_or_else(|| StoreError::InvalidParams("distributed value end overflow".to_string()))?;
    if record_end != input.len() {
        return Err(StoreError::InvalidParams(
            "distributed record length does not match envelope".to_string(),
        ));
    }
    let storage_key = std::str::from_utf8(&input[DISTRIBUTED_RECORD_HEADER_SIZE..key_end])?;
    if storage_key.is_empty() {
        return Err(StoreError::InvalidParams(
            "distributed record key must not be empty".to_string(),
        ));
    }
    Ok(DecodedRecord {
        storage_key,
        value: &input[key_end..record_end],
        generation_id,
    })
}

fn validate_envelope(
    storage_key: &str,
    expected: &DistributedRecord,
    envelope: &DecodedRecord<'_>,
) -> StoreResult<()> {
    if envelope.storage_key != storage_key
        || envelope.generation_id != expected.generation_id
        || envelope.value.len() as u64 != expected.value_size
    {
        return Err(StoreError::InvalidParams(format!(
            "distributed record changed outside backend for {storage_key:?}"
        )));
    }
    Ok(())
}

pub(crate) fn escape_filename(storage_key: &str) -> String {
    let mut output = String::with_capacity(storage_key.len() + 16);
    for byte in storage_key.bytes() {
        if matches!(byte, b'@' | b':' | b'/' | b'\\' | b'%') || !(0x20..=0x7e).contains(&byte) {
            let _ = write!(output, "%{byte:02x}");
        } else {
            output.push(byte as char);
        }
    }
    output
}

pub(crate) fn unescape_filename(name: &str) -> StoreResult<String> {
    let bytes = name.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            output.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len()
            || !bytes[index + 1].is_ascii_hexdigit()
            || !bytes[index + 2].is_ascii_hexdigit()
        {
            return Err(StoreError::InvalidParams(format!(
                "invalid distributed percent escape in {name:?}"
            )));
        }
        let value = std::str::from_utf8(&bytes[index + 1..index + 3])
            .ok()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok())
            .ok_or_else(|| {
                StoreError::InvalidParams(format!("invalid distributed percent escape in {name:?}"))
            })?;
        output.push(value);
        index += 3;
    }
    String::from_utf8(output).map_err(|error| {
        StoreError::InvalidParams(format!(
            "distributed filename does not decode as UTF-8: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn config(root: &Path) -> DistributedStorageConfig {
        DistributedStorageConfig {
            fsdir: root.to_path_buf(),
            fs_adapter_type: "hf3fs".to_string(),
            enable_health_check: true,
            hash_bucket_count: 16,
        }
    }

    #[test]
    fn filename_codec_matches_cpp_reserved_byte_rules() {
        let key = "v1:7:tenant/a@b\\c%中";
        let encoded = escape_filename(key);
        assert_eq!(encoded, "v1%3a7%3atenant%2fa%40b%5cc%25%e4%b8%ad");
        assert_eq!(unescape_filename(&encoded).unwrap(), key);
        assert!(unescape_filename("%zz").is_err());
    }

    #[test]
    fn record_envelope_round_trips_generation() {
        let generation = Uuid::new_v4();
        let encoded = encode_record("v1:7:tenant/key", b"value", generation).unwrap();
        let decoded = decode_record(&encoded).unwrap();
        assert_eq!(decoded.storage_key, "v1:7:tenant/key");
        assert_eq!(decoded.value, b"value");
        assert_eq!(decoded.generation_id, generation);
    }

    #[test]
    fn posix_injection_exercises_distributed_lifecycle_and_recovery() {
        let directory = TempDir::new().unwrap();
        let generation = Uuid::new_v4();
        let backend = DistributedStorageBackend::new_for_test(config(directory.path()));
        backend.init().unwrap();
        assert_eq!(backend.adapter_name(), "posix-test");
        let pending = backend.prepare_write("v1:7:tenant/key", 5).unwrap();
        backend
            .commit_write_with_generation("v1:7:tenant/key", b"value", pending, generation)
            .unwrap();
        assert_eq!(backend.read_object("v1:7:tenant/key").unwrap(), b"value");
        assert_eq!(
            backend.scan_records().unwrap(),
            vec![LocalStorageRecordMetadata {
                storage_key: "v1:7:tenant/key".to_string(),
                value_size: 5,
                generation_id: generation,
            }]
        );

        let storage_id = backend.storage_id().unwrap();
        drop(backend);
        let restarted = DistributedStorageBackend::new_for_test(config(directory.path()));
        restarted.init().unwrap();
        assert_eq!(restarted.storage_id().unwrap(), storage_id);
        assert_eq!(restarted.read_object("v1:7:tenant/key").unwrap(), b"value");
        assert!(
            !restarted
                .delete_object_if_generation("v1:7:tenant/key", Uuid::new_v4())
                .unwrap()
        );
        assert!(
            restarted
                .delete_object_if_generation("v1:7:tenant/key", generation)
                .unwrap()
        );
    }

    #[test]
    fn replacement_advances_generation_and_wrong_bucket_is_fail_closed() {
        let directory = TempDir::new().unwrap();
        let backend = DistributedStorageBackend::new_for_test(config(directory.path()));
        backend.init().unwrap();
        let first_generation = Uuid::new_v4();
        let pending = backend.prepare_write("v1:7:tenant/key", 1).unwrap();
        backend
            .commit_write_with_generation("v1:7:tenant/key", b"x", pending, first_generation)
            .unwrap();
        let second_generation = Uuid::new_v4();
        let replacement = backend.prepare_write("v1:7:tenant/key", 2).unwrap();
        backend
            .commit_write_with_generation("v1:7:tenant/key", b"yy", replacement, second_generation)
            .unwrap();
        assert_eq!(backend.read_object("v1:7:tenant/key").unwrap(), b"yy");
        assert!(
            !backend
                .delete_object_if_generation("v1:7:tenant/key", first_generation)
                .unwrap()
        );
        assert_eq!(backend.space_usage().0, 2);

        let source = backend.object_path("v1:7:tenant/key");
        let actual_bucket = backend.bucket_for_key("v1:7:tenant/key");
        let wrong_bucket = (actual_bucket + 1) % backend.config.hash_bucket_count;
        let target = backend
            .bucket_directory(wrong_bucket)
            .join(source.file_name().unwrap());
        std::fs::rename(source, target).unwrap();
        drop(backend);
        let restarted = DistributedStorageBackend::new_for_test(config(directory.path()));
        assert!(restarted.init().is_err());
    }
}
