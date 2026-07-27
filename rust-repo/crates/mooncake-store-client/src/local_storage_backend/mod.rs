mod bucket;
pub mod config;
mod distributed;
mod distributed_ffi;
mod migration;
mod offset;

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub use bucket::BucketStorageBackend;
pub use config::{
    BucketEvictionPolicy, BucketStorageConfig, DistributedStorageConfig, LocalStorageConfig,
    OffsetAllocatorConfig, OffsetEvictionPolicy, OffsetPersistMode, OffsetPersistenceConfig,
};
pub use distributed::DistributedStorageBackend;
pub use migration::{
    FilePerKeyMigrationReport, OffsetAllocatorMigrationReport, migrate_cpp_file_per_key_layout,
    migrate_cpp_offset_allocator_layout,
};
pub use offset::OffsetAllocatorStorageBackend;

const MIN_FREE_SPACE_BYTES: u64 = 256 * 1024 * 1024;
const FORMAT_MARKER_FILE: &str = ".mooncake-storage-format";
const STORAGE_ID_FILE: &str = ".mooncake-storage-id";
const TEMP_DIRECTORY: &str = ".mooncake-tmp";
const FILE_PER_KEY_PERSISTENT_FORMAT_MARKER_V1: &str = concat!(
    "backend=file-per-key\n",
    "format=protobuf-kv\n",
    "version=1\n",
    "wire_compat=cpp-struct-pb\n",
    "path_scheme=sha256-hex-v1\n",
    "lifecycle=persistent\n",
);
const FILE_PER_KEY_EPHEMERAL_FORMAT_MARKER_V1: &str = concat!(
    "backend=file-per-key\n",
    "format=protobuf-kv\n",
    "version=1\n",
    "wire_compat=cpp-struct-pb\n",
    "path_scheme=sha256-hex-v1\n",
    "lifecycle=ephemeral\n",
);
const FILE_PER_KEY_PERSISTENT_FORMAT_MARKER: &str = concat!(
    "backend=file-per-key\n",
    "format=protobuf-kv\n",
    "version=2\n",
    "wire_compat=cpp-struct-pb-plus-generation\n",
    "path_scheme=sha256-hex-v1\n",
    "lifecycle=persistent\n",
);
const FILE_PER_KEY_EPHEMERAL_FORMAT_MARKER: &str = concat!(
    "backend=file-per-key\n",
    "format=protobuf-kv\n",
    "version=2\n",
    "wire_compat=cpp-struct-pb-plus-generation\n",
    "path_scheme=sha256-hex-v1\n",
    "lifecycle=ephemeral\n",
);

type AvailableSpaceProbe = dyn Fn(&Path) -> std::io::Result<u64> + Send + Sync;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FilesystemIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NamespaceIdentity {
    data_directory: FilesystemIdentity,
    format_marker: FilesystemIdentity,
    storage_id: FilesystemIdentity,
}

#[cfg(unix)]
fn filesystem_identity(metadata: &std::fs::Metadata) -> FilesystemIdentity {
    FilesystemIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn filesystem_identity(metadata: &std::fs::Metadata) -> FilesystemIdentity {
    // The supported production platforms are Unix. Keep non-Unix builds
    // fail-closed across replacement by using stable metadata attributes
    // available in std; callers still compare both directory and marker.
    FilesystemIdentity {
        device: metadata.len(),
        inode: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_nanos() as u64),
    }
}

fn sync_directory(path: &Path) -> StoreResult<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
struct AtomicWriteFailure {
    installed: bool,
    error: StoreError,
}

fn write_file_atomically(
    path: &Path,
    contents: &[u8],
    temp_dir: &Path,
) -> Result<(), AtomicWriteFailure> {
    let not_installed = |error| AtomicWriteFailure {
        installed: false,
        error,
    };
    let installed = |error| AtomicWriteFailure {
        installed: true,
        error,
    };
    let parent = path.parent().ok_or_else(|| {
        not_installed(StoreError::InvalidParams(format!(
            "atomic FilePerKey target has no parent: {}",
            path.display()
        )))
    })?;
    match std::fs::symlink_metadata(temp_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(not_installed(StoreError::InvalidParams(format!(
                "FilePerKey temp namespace is not a regular directory: {}",
                temp_dir.display()
            ))));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(temp_dir).map_err(|error| not_installed(error.into()))?;
            sync_directory(temp_dir.parent().ok_or_else(|| {
                not_installed(StoreError::InvalidParams(
                    "FilePerKey temp directory has no parent".to_string(),
                ))
            })?)
            .map_err(not_installed)?;
        }
        Err(error) => return Err(not_installed(error.into())),
    }
    let temporary = temp_dir.join(Uuid::new_v4().to_string());
    let prepare_result = (|| -> StoreResult<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(error) = prepare_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(not_installed(error));
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(not_installed(error.into()));
    }

    let durability_result = (|| -> StoreResult<()> {
        sync_directory(temp_dir)?;
        sync_directory(parent)?;
        match std::fs::remove_dir(temp_dir) {
            Ok(()) => sync_directory(temp_dir.parent().ok_or_else(|| {
                StoreError::InvalidParams("FilePerKey temp directory has no parent".to_string())
            })?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    })();
    durability_result.map_err(installed)
}

fn protobuf_varint_len(mut value: u64) -> u64 {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn append_protobuf_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn file_per_key_record_size(key_len: u64, value_len: u64) -> StoreResult<u64> {
    if key_len == 0 {
        return Err(StoreError::InvalidParams(
            "FilePerKey record key must not be empty".to_string(),
        ));
    }
    let key_field = 1_u64
        .checked_add(protobuf_varint_len(key_len))
        .and_then(|size| size.checked_add(key_len))
        .ok_or_else(|| StoreError::InvalidParams("FilePerKey key size overflow".to_string()))?;
    if value_len == 0 {
        return Ok(key_field);
    }
    key_field
        .checked_add(1)
        .and_then(|size| size.checked_add(protobuf_varint_len(value_len)))
        .and_then(|size| size.checked_add(value_len))
        .ok_or_else(|| StoreError::InvalidParams("FilePerKey value size overflow".to_string()))
}

fn file_per_key_v2_record_size(key_len: u64, value_len: u64) -> StoreResult<u64> {
    file_per_key_record_size(key_len, value_len)?
        .checked_add(18) // field 3 tag + length + 16 UUID bytes
        .ok_or_else(|| StoreError::InvalidParams("FilePerKey v2 size overflow".to_string()))
}

pub(super) fn encode_file_per_key_record(key: &str, value: &[u8]) -> StoreResult<Vec<u8>> {
    let expected_size = file_per_key_record_size(key.len() as u64, value.len() as u64)?;
    let capacity = usize::try_from(expected_size).map_err(|_| {
        StoreError::InvalidParams("FilePerKey record exceeds addressable memory".to_string())
    })?;
    let mut output = Vec::with_capacity(capacity);
    output.push(0x0a); // field 1, length-delimited string: key
    append_protobuf_varint(&mut output, key.len() as u64);
    output.extend_from_slice(key.as_bytes());
    if !value.is_empty() {
        output.push(0x12); // field 2, length-delimited string: value
        append_protobuf_varint(&mut output, value.len() as u64);
        output.extend_from_slice(value);
    }
    Ok(output)
}

fn encode_file_per_key_record_with_generation(
    key: &str,
    value: &[u8],
    generation_id: Uuid,
) -> StoreResult<Vec<u8>> {
    if generation_id.is_nil() {
        return Err(StoreError::InvalidParams(
            "FilePerKey generation must not be nil".to_string(),
        ));
    }
    let expected_size = file_per_key_v2_record_size(key.len() as u64, value.len() as u64)?;
    let mut output = encode_file_per_key_record(key, value)?;
    output.reserve(18);
    output.push(0x1a); // field 3, length-delimited 16-byte generation UUID
    output.push(16);
    output.extend_from_slice(generation_id.as_bytes());
    debug_assert_eq!(output.len() as u64, expected_size);
    Ok(output)
}

fn read_protobuf_varint(input: &[u8], cursor: &mut usize) -> StoreResult<u64> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *input.get(*cursor).ok_or_else(|| {
            StoreError::InvalidParams("truncated FilePerKey protobuf varint".to_string())
        })?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err(StoreError::InvalidParams(
                "overflowing FilePerKey protobuf varint".to_string(),
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(StoreError::InvalidParams(
        "overlong FilePerKey protobuf varint".to_string(),
    ))
}

fn decode_file_per_key_record_with_generation(
    input: &[u8],
) -> StoreResult<(&str, &[u8], Option<Uuid>)> {
    let mut cursor = 0usize;
    let mut key = None;
    let mut value = None;
    let mut generation = None;
    while cursor < input.len() {
        let tag = read_protobuf_varint(input, &mut cursor)?;
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        if field_number == 0 || wire_type != 2 || !matches!(field_number, 1 | 2 | 3) {
            return Err(StoreError::InvalidParams(format!(
                "unsupported FilePerKey protobuf field: field={field_number}, wire={wire_type}"
            )));
        }
        let len = read_protobuf_varint(input, &mut cursor)?;
        let len = usize::try_from(len).map_err(|_| {
            StoreError::InvalidParams("FilePerKey protobuf field is too large".to_string())
        })?;
        let end = cursor.checked_add(len).ok_or_else(|| {
            StoreError::InvalidParams("FilePerKey protobuf field size overflow".to_string())
        })?;
        let bytes = input.get(cursor..end).ok_or_else(|| {
            StoreError::InvalidParams("truncated FilePerKey protobuf field".to_string())
        })?;
        cursor = end;
        match field_number {
            1 if key.replace(bytes).is_some() => {
                return Err(StoreError::InvalidParams(
                    "duplicate FilePerKey protobuf key field".to_string(),
                ));
            }
            2 if value.replace(bytes).is_some() => {
                return Err(StoreError::InvalidParams(
                    "duplicate FilePerKey protobuf value field".to_string(),
                ));
            }
            3 => {
                if bytes.len() != 16 {
                    return Err(StoreError::InvalidParams(
                        "FilePerKey generation field must contain 16 bytes".to_string(),
                    ));
                }
                let generation_id = Uuid::from_slice(bytes).map_err(|error| {
                    StoreError::InvalidParams(format!(
                        "FilePerKey generation field is invalid: {error}"
                    ))
                })?;
                if generation_id.is_nil() {
                    return Err(StoreError::InvalidParams(
                        "FilePerKey generation must not be nil".to_string(),
                    ));
                }
                if generation.replace(generation_id).is_some() {
                    return Err(StoreError::InvalidParams(
                        "duplicate FilePerKey protobuf generation field".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }
    let key = key.ok_or_else(|| {
        StoreError::InvalidParams("FilePerKey protobuf record is missing key".to_string())
    })?;
    let key = std::str::from_utf8(key)?;
    if key.is_empty() {
        return Err(StoreError::InvalidParams(
            "FilePerKey protobuf record has an empty key".to_string(),
        ));
    }
    Ok((key, value.unwrap_or_default(), generation))
}

pub(super) fn decode_file_per_key_record(input: &[u8]) -> StoreResult<(&str, &[u8])> {
    decode_file_per_key_record_with_generation(input).map(|(key, value, _)| (key, value))
}

pub(crate) fn local_storage_key(tenant_id: &str, key: &str) -> String {
    let tenant_id = if tenant_id.is_empty() {
        "default"
    } else {
        tenant_id
    };
    format!("v1:{}:{tenant_id}{key}", tenant_id.len())
}

pub(crate) fn parse_local_storage_key(storage_key: &str) -> (&str, &str) {
    let Some(encoded) = storage_key.strip_prefix("v1:") else {
        return ("default", storage_key);
    };
    let Some((tenant_len, payload)) = encoded.split_once(':') else {
        return ("default", storage_key);
    };
    let Ok(tenant_len) = tenant_len.parse::<usize>() else {
        return ("default", storage_key);
    };
    if tenant_len > payload.len() || !payload.is_char_boundary(tenant_len) {
        return ("default", storage_key);
    }
    payload.split_at(tenant_len)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalStorageRecordMetadata {
    pub(crate) storage_key: String,
    pub(crate) value_size: u64,
    pub(crate) generation_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileRecord {
    storage_key: String,
    relative_path: String,
    size: u64,
    generation: u64,
    durable_generation_id: Uuid,
}

#[derive(Debug)]
struct ScannedFile {
    storage_key: String,
    relative_path: String,
    size: u64,
    value_size: u64,
    durable_generation_id: Option<Uuid>,
    created: std::time::SystemTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilePerKeyLifecycle {
    Ephemeral,
    Persistent,
}

#[derive(Debug)]
struct WriteTargetReservation {
    relative_path: String,
    previous: Option<FileRecord>,
}

#[derive(Debug, Default)]
struct FilePerKeyReservationRegistry {
    paths: Mutex<HashMap<String, u64>>,
    write_growth: Mutex<HashMap<u64, u64>>,
}

impl FilePerKeyReservationRegistry {
    fn release(
        &self,
        token: u64,
        paths: impl IntoIterator<Item = String>,
        release_write_growth: bool,
    ) {
        if token == 0 {
            return;
        }
        if release_write_growth {
            self.write_growth.lock().remove(&token);
        }
        let mut reserved = self.paths.lock();
        for path in paths {
            if reserved.get(&path).copied() == Some(token) {
                reserved.remove(&path);
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PendingLocalEviction {
    backend_id: Uuid,
    token: u64,
    expected_record_size: Option<u64>,
    records: Vec<FileRecord>,
    write_target: Option<WriteTargetReservation>,
    registry: Option<Arc<FilePerKeyReservationRegistry>>,
}

impl Default for PendingLocalEviction {
    fn default() -> Self {
        Self {
            backend_id: Uuid::nil(),
            token: 0,
            expected_record_size: None,
            records: Vec::new(),
            write_target: None,
            registry: None,
        }
    }
}

impl Drop for PendingLocalEviction {
    fn drop(&mut self) {
        let Some(registry) = &self.registry else {
            return;
        };
        let mut paths = Vec::with_capacity(self.records.len() + 1);
        if let Some(target) = &self.write_target {
            paths.push(target.relative_path.clone());
        }
        paths.extend(
            self.records
                .iter()
                .map(|record| record.relative_path.clone()),
        );
        registry.release(self.token, paths, self.write_target.is_some());
    }
}

impl PendingLocalEviction {
    pub(crate) fn keys(&self) -> Vec<String> {
        self.records
            .iter()
            .map(|record| record.storage_key.clone())
            .collect()
    }

    fn has_reservations(&self) -> bool {
        self.token != 0
    }

    pub(crate) fn partition_accepted(
        mut self,
        accepted_keys: &HashSet<String>,
    ) -> (PendingLocalEviction, PendingLocalEviction) {
        let records = std::mem::take(&mut self.records);
        let (accepted, unaccepted): (Vec<_>, Vec<_>) = records
            .into_iter()
            .partition(|record| accepted_keys.contains(&record.storage_key));
        let accepted_pending = PendingLocalEviction {
            backend_id: self.backend_id,
            token: self.token,
            expected_record_size: None,
            records: accepted,
            write_target: None,
            registry: self.registry.clone(),
        };
        self.records = unaccepted;
        (accepted_pending, self)
    }
}

#[derive(Debug)]
pub(crate) enum PendingStorageEviction {
    Bucket(bucket::PendingBucketEviction),
    Distributed(distributed::PendingDistributedWrite),
    FilePerKey(PendingLocalEviction),
    OffsetAllocator(offset::PendingOffsetEviction),
}

impl PendingStorageEviction {
    pub(crate) fn keys(&self) -> Vec<String> {
        match self {
            Self::Bucket(pending) => pending.keys(),
            Self::Distributed(pending) => pending.keys(),
            Self::FilePerKey(pending) => pending.keys(),
            Self::OffsetAllocator(pending) => pending.keys(),
        }
    }

    pub(crate) fn partition_accepted(
        self,
        accepted_keys: &HashSet<String>,
    ) -> (PendingStorageEviction, PendingStorageEviction) {
        match self {
            Self::Bucket(pending) => {
                let (accepted, unaccepted) = pending.partition_accepted(accepted_keys);
                (Self::Bucket(accepted), Self::Bucket(unaccepted))
            }
            Self::Distributed(pending) => {
                let (accepted, unaccepted) = pending.partition_accepted(accepted_keys);
                (Self::Distributed(accepted), Self::Distributed(unaccepted))
            }
            Self::FilePerKey(pending) => {
                let (accepted, unaccepted) = pending.partition_accepted(accepted_keys);
                (Self::FilePerKey(accepted), Self::FilePerKey(unaccepted))
            }
            Self::OffsetAllocator(pending) => {
                let (accepted, unaccepted) = pending.partition_accepted(accepted_keys);
                (
                    Self::OffsetAllocator(accepted),
                    Self::OffsetAllocator(unaccepted),
                )
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum AttachedLocalStorage {
    Bucket(Arc<BucketStorageBackend>),
    Distributed(Arc<DistributedStorageBackend>),
    FilePerKey(Arc<LocalStorageBackend>),
    OffsetAllocator(Arc<OffsetAllocatorStorageBackend>),
}

impl AttachedLocalStorage {
    pub(crate) fn storage_id(&self) -> StoreResult<Uuid> {
        match self {
            Self::Bucket(storage) => storage.storage_id(),
            Self::Distributed(storage) => storage.storage_id(),
            Self::FilePerKey(storage) => storage.storage_id(),
            Self::OffsetAllocator(storage) => storage.storage_id(),
        }
    }

    pub(crate) fn space_usage(&self) -> (u64, u64) {
        match self {
            Self::Bucket(storage) => storage.space_usage(),
            Self::Distributed(storage) => storage.space_usage(),
            Self::FilePerKey(storage) => storage.space_usage(),
            Self::OffsetAllocator(storage) => storage.space_usage(),
        }
    }

    pub(crate) fn prepare_write(
        &self,
        key: &str,
        required: u64,
    ) -> StoreResult<PendingStorageEviction> {
        match self {
            Self::Bucket(storage) => storage
                .prepare_write(key, required)
                .map(PendingStorageEviction::Bucket),
            Self::Distributed(storage) => storage
                .prepare_write(key, required)
                .map(PendingStorageEviction::Distributed),
            Self::FilePerKey(storage) => storage
                .prepare_write(key, required)
                .map(PendingStorageEviction::FilePerKey),
            Self::OffsetAllocator(storage) => storage
                .prepare_write(key, required)
                .map(PendingStorageEviction::OffsetAllocator),
        }
    }

    pub(crate) fn commit_write(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingStorageEviction,
        generation_id: Uuid,
    ) -> StoreResult<()> {
        match (self, pending) {
            (Self::Bucket(storage), PendingStorageEviction::Bucket(pending)) => {
                storage.commit_write_with_generation(key, data, pending, generation_id)
            }
            (Self::Distributed(storage), PendingStorageEviction::Distributed(pending)) => {
                storage.commit_write_with_generation(key, data, pending, generation_id)
            }
            (Self::FilePerKey(storage), PendingStorageEviction::FilePerKey(pending)) => {
                storage.commit_write_with_generation(key, data, pending, generation_id)
            }
            (Self::OffsetAllocator(storage), PendingStorageEviction::OffsetAllocator(pending)) => {
                storage.commit_write_with_generation(key, data, pending, generation_id)
            }
            _ => Err(StoreError::Internal(
                "local storage pending eviction backend mismatch".to_string(),
            )),
        }
    }

    pub(crate) fn rollback_eviction(&self, pending: PendingStorageEviction) {
        match (self, pending) {
            (Self::Bucket(storage), PendingStorageEviction::Bucket(pending)) => {
                storage.rollback_eviction(pending)
            }
            (Self::Distributed(storage), PendingStorageEviction::Distributed(pending)) => {
                storage.rollback_eviction(pending)
            }
            (Self::FilePerKey(storage), PendingStorageEviction::FilePerKey(pending)) => {
                storage.rollback_eviction(pending)
            }
            (Self::OffsetAllocator(storage), PendingStorageEviction::OffsetAllocator(pending)) => {
                storage.rollback_eviction(pending)
            }
            _ => tracing::error!("local storage pending eviction backend mismatch"),
        }
    }

    pub(crate) fn prepare_watermark_eviction(
        &self,
        high: f64,
        low: f64,
    ) -> StoreResult<PendingStorageEviction> {
        match self {
            Self::Bucket(storage) => storage
                .prepare_watermark_eviction(high, low)
                .map(PendingStorageEviction::Bucket),
            Self::Distributed(storage) => storage
                .prepare_watermark_eviction(high, low)
                .map(PendingStorageEviction::Distributed),
            Self::FilePerKey(storage) => storage
                .prepare_watermark_eviction(high, low)
                .map(PendingStorageEviction::FilePerKey),
            Self::OffsetAllocator(storage) => storage
                .prepare_watermark_eviction(high, low)
                .map(PendingStorageEviction::OffsetAllocator),
        }
    }

    pub(crate) fn commit_eviction(&self, pending: PendingStorageEviction) -> StoreResult<()> {
        match (self, pending) {
            (Self::Bucket(storage), PendingStorageEviction::Bucket(pending)) => {
                storage.commit_eviction(pending)
            }
            (Self::Distributed(storage), PendingStorageEviction::Distributed(pending)) => {
                storage.commit_eviction(pending)
            }
            (Self::FilePerKey(storage), PendingStorageEviction::FilePerKey(pending)) => {
                storage.commit_eviction(pending)
            }
            (Self::OffsetAllocator(storage), PendingStorageEviction::OffsetAllocator(pending)) => {
                storage.commit_eviction(pending)
            }
            _ => Err(StoreError::Internal(
                "local storage pending eviction backend mismatch".to_string(),
            )),
        }
    }

    pub(crate) fn read_object(&self, key: &str) -> StoreResult<Vec<u8>> {
        match self {
            Self::Bucket(storage) => storage.read_object(key),
            Self::Distributed(storage) => storage.read_object(key),
            Self::FilePerKey(storage) => storage.read_object(key),
            Self::OffsetAllocator(storage) => storage.read_object(key),
        }
    }

    pub(crate) fn delete_object(&self, key: &str) -> StoreResult<()> {
        match self {
            Self::Bucket(storage) => storage.delete_object(key),
            Self::Distributed(storage) => storage.delete_object(key),
            Self::FilePerKey(storage) => storage.delete_object(key),
            Self::OffsetAllocator(storage) => storage.delete_object(key),
        }
    }

    pub(crate) fn delete_object_if_generation(
        &self,
        key: &str,
        generation_id: Uuid,
    ) -> StoreResult<bool> {
        match self {
            Self::Bucket(storage) => storage.delete_object_if_generation(key, generation_id),
            Self::Distributed(storage) => storage.delete_object_if_generation(key, generation_id),
            Self::FilePerKey(storage) => storage.delete_object_if_generation(key, generation_id),
            Self::OffsetAllocator(storage) => {
                storage.delete_object_if_generation(key, generation_id)
            }
        }
    }

    pub(crate) fn remove_all(&self) -> StoreResult<usize> {
        match self {
            Self::Bucket(storage) => storage.remove_all(),
            Self::Distributed(storage) => storage.remove_all(),
            Self::FilePerKey(storage) => storage.remove_all(),
            Self::OffsetAllocator(storage) => storage.remove_all(),
        }
    }

    pub(crate) fn scan_meta(&self) -> StoreResult<Vec<(String, u64)>> {
        match self {
            Self::Bucket(storage) => storage.scan_meta(),
            Self::Distributed(storage) => storage.scan_meta(),
            Self::FilePerKey(storage) => storage.scan_meta(),
            Self::OffsetAllocator(storage) => storage.scan_meta(),
        }
    }

    pub(crate) fn scan_records(&self) -> StoreResult<Vec<LocalStorageRecordMetadata>> {
        match self {
            Self::Bucket(storage) => storage.scan_records(),
            Self::Distributed(storage) => storage.scan_records(),
            Self::FilePerKey(storage) => storage.scan_records(),
            Self::OffsetAllocator(storage) => storage.scan_records(),
        }
    }
}

/// Local disk storage backend for FilePerKey offload/promotion data.
///
/// Each key-value pair is stored as a single file under a hash-partitioned
/// 2-level directory tree. Optional FIFO eviction enforces a space quota.
///
/// C++ equivalent: `StorageBackendAdaptor` (FilePerKey) + `StorageBackend`
/// (file lifecycle + eviction) in `storage_backend.cpp`.
pub struct LocalStorageBackend {
    backend_id: Uuid,
    storage_id: RwLock<Option<Uuid>>,
    config: LocalStorageConfig,
    lifecycle: FilePerKeyLifecycle,
    init_lock: Mutex<()>,
    storage_lock: Mutex<Option<std::fs::File>>,
    namespace_identity: Mutex<Option<NamespaceIdentity>>,
    mutation_lock: Mutex<()>,
    reservation_registry: Arc<FilePerKeyReservationRegistry>,
    orphaned_records: Mutex<HashMap<String, FileRecord>>,
    next_generation: AtomicU64,
    next_reservation: AtomicU64,

    /// FIFO write queue with both the logical key and relative file path.
    /// Entries are lazily cleaned — stale entries (already deleted) are
    /// skipped during eviction and compacted periodically.
    write_queue: RwLock<VecDeque<FileRecord>>,

    /// Authoritative in-process record index. Queue entries are valid only
    /// when their generation matches the corresponding active record.
    active_records: RwLock<HashMap<String, FileRecord>>,

    /// Total quota in bytes (constant after init).
    total_space: RwLock<u64>,

    /// Currently used bytes on disk.
    used_space: RwLock<u64>,

    /// Whether `init()` has been called successfully.
    initialized: AtomicBool,

    available_space_probe: Box<AvailableSpaceProbe>,
}

impl LocalStorageBackend {
    /// Create a persistent backend. Does NOT touch the filesystem.
    ///
    /// Persistence is the safe default: initialization preserves canonical
    /// records and `Drop` never clears them. Use
    /// [`new_ephemeral`](Self::new_ephemeral) only for an explicitly
    /// disposable cache directory.
    /// Call [`init`](Self::init) before using any other method.
    pub fn new(config: LocalStorageConfig) -> Self {
        Self::new_with_lifecycle(config, FilePerKeyLifecycle::Persistent)
    }

    /// Create an explicitly disposable FilePerKey cache backend.
    ///
    /// Ephemeral storage has a distinct ownership marker, is cleared on
    /// initialization and `Drop`, and refuses to open a persistent directory.
    pub fn new_ephemeral(config: LocalStorageConfig) -> Self {
        Self::new_with_lifecycle(config, FilePerKeyLifecycle::Ephemeral)
    }

    /// Create a persistent FilePerKey backend.
    ///
    /// This is an explicit alias for [`new`](Self::new). Initialization scans
    /// and preserves records owned by the canonical persistent marker, and
    /// `Drop` does not clear them.
    pub fn new_persistent(config: LocalStorageConfig) -> Self {
        Self::new(config)
    }

    fn new_with_lifecycle(config: LocalStorageConfig, lifecycle: FilePerKeyLifecycle) -> Self {
        Self {
            backend_id: Uuid::new_v4(),
            storage_id: RwLock::new(None),
            config,
            lifecycle,
            init_lock: Mutex::new(()),
            storage_lock: Mutex::new(None),
            namespace_identity: Mutex::new(None),
            mutation_lock: Mutex::new(()),
            reservation_registry: Arc::new(FilePerKeyReservationRegistry::default()),
            orphaned_records: Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(1),
            next_reservation: AtomicU64::new(1),
            write_queue: RwLock::new(VecDeque::new()),
            active_records: RwLock::new(HashMap::new()),
            total_space: RwLock::new(0),
            used_space: RwLock::new(0),
            initialized: AtomicBool::new(false),
            available_space_probe: Box::new(|path| fs2::available_space(path)),
        }
    }

    #[cfg(test)]
    fn new_with_available_space_probe(
        config: LocalStorageConfig,
        available_space_probe: Box<AvailableSpaceProbe>,
    ) -> Self {
        Self {
            backend_id: Uuid::new_v4(),
            storage_id: RwLock::new(None),
            config,
            lifecycle: FilePerKeyLifecycle::Ephemeral,
            init_lock: Mutex::new(()),
            storage_lock: Mutex::new(None),
            namespace_identity: Mutex::new(None),
            mutation_lock: Mutex::new(()),
            reservation_registry: Arc::new(FilePerKeyReservationRegistry::default()),
            orphaned_records: Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(1),
            next_reservation: AtomicU64::new(1),
            write_queue: RwLock::new(VecDeque::new()),
            active_records: RwLock::new(HashMap::new()),
            total_space: RwLock::new(0),
            used_space: RwLock::new(0),
            initialized: AtomicBool::new(false),
            available_space_probe,
        }
    }

    // ------------------------------------------------------------------
    // Path helpers
    // ------------------------------------------------------------------

    /// Compute the canonical on-disk path from the full SHA-256 digest of the
    /// storage key.
    ///
    /// ```text
    /// digest = lowercase_hex(SHA-256(key))
    /// full   = <root>/<fsdir>/<digest[0..2]>/<digest[2..4]>/<digest>
    /// ```
    ///
    /// The digest is stable across Rust releases and avoids collisions caused
    /// by filename sanitization. The original key remains authenticated in the
    /// protobuf record envelope.
    pub fn key_path(&self, key: &str) -> PathBuf {
        let digest = Sha256::digest(key.as_bytes());
        let digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.config
            .root_dir
            .join(&self.config.fsdir)
            .join(&digest[0..2])
            .join(&digest[2..4])
            .join(&digest)
    }

    /// Return the data directory path (`<root>/<fsdir>`).
    fn data_dir(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.fsdir)
    }

    fn format_marker_path(&self) -> PathBuf {
        self.data_dir().join(FORMAT_MARKER_FILE)
    }

    fn storage_id_path(&self) -> PathBuf {
        self.data_dir().join(STORAGE_ID_FILE)
    }

    fn temp_dir(&self) -> PathBuf {
        self.data_dir().join(TEMP_DIRECTORY)
    }

    fn expected_format_marker(&self) -> &'static str {
        match self.lifecycle {
            FilePerKeyLifecycle::Ephemeral => FILE_PER_KEY_EPHEMERAL_FORMAT_MARKER,
            FilePerKeyLifecycle::Persistent => FILE_PER_KEY_PERSISTENT_FORMAT_MARKER,
        }
    }

    fn legacy_format_marker(&self) -> &'static str {
        match self.lifecycle {
            FilePerKeyLifecycle::Ephemeral => FILE_PER_KEY_EPHEMERAL_FORMAT_MARKER_V1,
            FilePerKeyLifecycle::Persistent => FILE_PER_KEY_PERSISTENT_FORMAT_MARKER_V1,
        }
    }

    fn ensure_record_parent(&self, path: &Path) -> StoreResult<()> {
        let data_dir = self.data_dir();
        let parent = path.parent().ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "FilePerKey record path has no parent: {}",
                path.display()
            ))
        })?;
        let relative = parent.strip_prefix(&data_dir).map_err(|_| {
            StoreError::InvalidParams(format!(
                "FilePerKey record path escapes data directory: {}",
                path.display()
            ))
        })?;
        let mut current = data_dir;
        for component in relative.components() {
            let std::path::Component::Normal(component) = component else {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey record path has unsafe component: {}",
                    path.display()
                )));
            };
            let next = current.join(component);
            match std::fs::symlink_metadata(&next) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(StoreError::InvalidParams(format!(
                        "FilePerKey refuses symbolic-link directory: {}",
                        next.display()
                    )));
                }
                Ok(metadata) if !metadata.is_dir() => {
                    return Err(StoreError::InvalidParams(format!(
                        "FilePerKey record parent is not a directory: {}",
                        next.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&next)?;
                    sync_directory(&current)?;
                }
                Err(error) => return Err(error.into()),
            }
            current = next;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Initialization
    // ------------------------------------------------------------------

    /// Initialize the backend: create directories, scan existing files,
    /// rebuild the FIFO queue, and compute space accounting.
    ///
    /// Must be called once before any other operation. Idempotent — subsequent
    /// calls are no-ops.
    ///
    /// C++ equivalent: `StorageBackend::Init(quota_bytes)` +
    /// `StorageBackendAdaptor::Init()`.
    pub fn init(&self) -> StoreResult<()> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _init_guard = self.init_lock.lock();
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.acquire_storage_lock()?;

        let data_dir = self.data_dir();
        self.claim_or_validate_storage_path(true)?;
        let storage_id =
            self.load_or_create_storage_id(self.lifecycle == FilePerKeyLifecycle::Ephemeral)?;
        *self.storage_id.write() = Some(storage_id);
        *self.namespace_identity.lock() = Some(self.capture_namespace_identity()?);
        if self.lifecycle == FilePerKeyLifecycle::Ephemeral {
            self.clean_storage_path_owned()?;
        }
        *self.namespace_identity.lock() = Some(self.capture_namespace_identity()?);
        std::fs::create_dir_all(&data_dir)?;

        // Determine quota.
        let total = if self.config.quota_bytes > 0 {
            self.config.quota_bytes
        } else {
            // Auto-detect: 90% of filesystem capacity.
            let available = fs2::available_space(&data_dir).unwrap_or(0);
            let total = fs2::total_space(&data_dir).unwrap_or(0);
            let capacity = std::cmp::max(available, total);
            (capacity as f64 * 0.9) as u64
        };
        *self.total_space.write() = total;

        // Scan existing files, sorted by creation time (oldest first).
        let mut entries = self.scan_files(&data_dir)?;

        entries.sort_by_key(|entry| entry.created);
        let mut recovered_keys = HashSet::with_capacity(entries.len());
        for entry in &entries {
            if !recovered_keys.insert(entry.storage_key.as_str()) {
                return Err(StoreError::InvalidParams(format!(
                    "duplicate FilePerKey record for storage key {:?}",
                    entry.storage_key
                )));
            }
        }
        let recovered_size = entries.iter().try_fold(0_u64, |used, entry| {
            used.checked_add(entry.size).ok_or_else(|| {
                StoreError::InvalidParams("FilePerKey recovered size overflow".to_string())
            })
        })?;
        if self.lifecycle == FilePerKeyLifecycle::Persistent && recovered_size > total {
            return Err(StoreError::InvalidParams(format!(
                "persistent FilePerKey data uses {recovered_size} bytes, exceeding configured \
                 quota {total}; increase quota or migrate/remove data explicitly"
            )));
        }

        let mut used = 0u64;
        let mut queue = self.write_queue.write();
        let mut active = self.active_records.write();
        queue.clear();
        active.clear();

        for entry in &entries {
            if self.lifecycle == FilePerKeyLifecycle::Ephemeral
                && used.saturating_add(entry.size) > total
                && self.config.enable_eviction
            {
                // Over quota — delete excess files from disk.
                let abs_path = data_dir.join(&entry.relative_path);
                let _ = std::fs::remove_file(&abs_path);
            } else {
                used += entry.size;
                let record = FileRecord {
                    storage_key: entry.storage_key.clone(),
                    relative_path: entry.relative_path.clone(),
                    size: entry.size,
                    generation: self.allocate_generation()?,
                    durable_generation_id: entry.durable_generation_id.unwrap_or_else(|| {
                        mooncake_store_core::legacy_local_disk_generation_id(
                            storage_id,
                            &entry.storage_key,
                        )
                    }),
                };
                active.insert(entry.relative_path.clone(), record.clone());
                if self.config.enable_eviction {
                    queue.push_back(record);
                }
            }
        }

        *self.used_space.write() = used;
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Recursively scan all files under `dir` and collect (relative_path, size, creation_time).
    fn scan_files(&self, dir: &Path) -> StoreResult<Vec<ScannedFile>> {
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let data_dir = self.data_dir();
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey refuses symbolic links: {}",
                    path.display()
                )));
            }
            if file_type.is_dir() {
                if entry.file_name() == TEMP_DIRECTORY {
                    continue;
                }
                files.extend(self.scan_files(&path)?);
            } else if file_type.is_file() {
                if path == self.format_marker_path() || path == self.storage_id_path() {
                    continue;
                }
                let contents = std::fs::read(&path)?;
                let (storage_key, value, durable_generation_id) =
                    decode_file_per_key_record_with_generation(&contents)?;
                let expected_path = self.key_path(storage_key);
                if path != expected_path {
                    return Err(StoreError::InvalidParams(format!(
                        "FilePerKey record path does not match its canonical key digest: {}",
                        path.display()
                    )));
                }
                let metadata = entry.metadata()?;
                let rel_path = path
                    .strip_prefix(&data_dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();
                files.push(ScannedFile {
                    storage_key: storage_key.to_string(),
                    relative_path: rel_path,
                    size: metadata.len(),
                    value_size: value.len() as u64,
                    durable_generation_id,
                    created: metadata.created().unwrap_or(UNIX_EPOCH),
                });
            } else {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey storage contains a non-regular entry: {}",
                    path.display()
                )));
            }
        }
        Ok(files)
    }

    // ------------------------------------------------------------------
    // Core I/O
    // ------------------------------------------------------------------

    /// Write a key-value pair to disk. Creates parent directories as needed.
    /// Returns a list of keys evicted to make space (empty if no eviction).
    ///
    /// C++ equivalent: `StorageBackendAdaptor::BatchOffload` (single-key path).
    pub fn write_object(&self, key: &str, data: &[u8]) -> StoreResult<Vec<String>> {
        self.ensure_init()?;
        let pending = self.prepare_write(key, data.len() as u64)?;
        let evicted = pending.keys();
        self.commit_write(key, data, pending)?;
        Ok(evicted)
    }

    pub(crate) fn prepare_write(
        &self,
        key: &str,
        value_size: u64,
    ) -> StoreResult<PendingLocalEviction> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.cleanup_orphaned_records()?;
        let required = file_per_key_v2_record_size(key.len() as u64, value_size)?;
        let token = self.allocate_reservation()?;
        let write_target = self.reserve_write_target(key, token)?;
        let previous_size = write_target
            .previous
            .as_ref()
            .map_or(0, |record| record.size);
        let write_growth = required.saturating_sub(previous_size);
        let mut pending = PendingLocalEviction {
            backend_id: self.backend_id,
            token,
            expected_record_size: Some(required),
            records: Vec::new(),
            write_target: Some(write_target),
            registry: Some(self.reservation_registry.clone()),
        };
        if self.config.enable_eviction {
            if let Err(error) = self.prepare_space_eviction(required, write_growth, &mut pending) {
                self.rollback_eviction_owned(pending);
                return Err(error);
            }
        }
        self.reservation_registry
            .write_growth
            .lock()
            .insert(token, write_growth);
        Ok(pending)
    }

    pub(crate) fn commit_write(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingLocalEviction,
    ) -> StoreResult<()> {
        self.commit_write_with_generation(key, data, pending, Uuid::new_v4())
    }

    pub(crate) fn commit_write_with_generation(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingLocalEviction,
        durable_generation_id: Uuid,
    ) -> StoreResult<()> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        let result = (|| -> StoreResult<()> {
            self.validate_existing_storage_path()?;
            let encoded =
                encode_file_per_key_record_with_generation(key, data, durable_generation_id)?;
            self.validate_pending_write(key, encoded.len() as u64, &pending)?;
            self.preflight_eviction_records(&pending)?;

            let path = self.key_path(key);
            self.ensure_record_parent(&path)?;
            let generation = self.allocate_generation()?;

            self.commit_eviction_records(&pending)?;
            let target = pending.write_target.as_ref().ok_or_else(|| {
                StoreError::Internal(
                    "FilePerKey write commit is missing its target reservation".to_string(),
                )
            })?;
            let previous_size = target.previous.as_ref().map_or(0, |record| record.size);
            let new_record = FileRecord {
                storage_key: key.to_string(),
                relative_path: target.relative_path.clone(),
                size: encoded.len() as u64,
                generation,
                durable_generation_id,
            };
            let new_used = (*self.used_space.read())
                .checked_sub(previous_size)
                .and_then(|used| used.checked_add(encoded.len() as u64))
                .ok_or_else(|| {
                    StoreError::Internal(format!(
                        "FilePerKey usage invariant failed while replacing {key:?}"
                    ))
                })?;
            let write_failure = match write_file_atomically(&path, &encoded, &self.temp_dir()) {
                Ok(()) => None,
                Err(failure) if failure.installed => Some(failure.error),
                Err(failure) => return Err(failure.error),
            };

            // Rename is the visibility/linearization point. Even if a later
            // directory fsync reports a durability error, the new generation
            // is already observable and must replace the old in-memory state.
            self.install_active_record(new_record);
            *self.used_space.write() = new_used;
            match write_failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })();

        if result.is_err() {
            self.abandon_pending_victims(&pending);
        }
        result
    }

    pub(crate) fn rollback_eviction(&self, pending: PendingLocalEviction) {
        let _mutation_guard = self.mutation_lock.lock();
        self.rollback_eviction_owned(pending);
    }

    pub(crate) fn prepare_watermark_eviction(
        &self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<PendingLocalEviction> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.cleanup_orphaned_records()?;
        validate_watermark_ratios(high_watermark_ratio, low_watermark_ratio)?;
        if !self.config.enable_eviction {
            return Ok(PendingLocalEviction::default());
        }

        let total = *self.total_space.read();
        let used = *self.used_space.read();
        if total == 0 || used <= (total as f64 * high_watermark_ratio) as u64 {
            return Ok(PendingLocalEviction::default());
        }

        let target = (total as f64 * low_watermark_ratio) as u64;
        let mut pending = PendingLocalEviction {
            backend_id: self.backend_id,
            token: self.allocate_reservation()?,
            expected_record_size: None,
            records: Vec::new(),
            write_target: None,
            registry: Some(self.reservation_registry.clone()),
        };
        if let Err(error) = self.prepare_eviction_bytes(used.saturating_sub(target), &mut pending) {
            self.rollback_eviction_owned(pending);
            return Err(error);
        }
        Ok(pending)
    }

    pub(crate) fn commit_eviction(&self, pending: PendingLocalEviction) -> StoreResult<()> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        let result = (|| -> StoreResult<()> {
            self.validate_existing_storage_path()?;
            self.preflight_eviction_records(&pending)?;
            self.commit_eviction_records(&pending)
        })();
        if result.is_err() {
            self.abandon_pending_victims(&pending);
        }
        result
    }

    fn commit_eviction_records(&self, pending: &PendingLocalEviction) -> StoreResult<()> {
        let total_victim_bytes = pending.records.iter().try_fold(0_u64, |total, record| {
            total.checked_add(record.size).ok_or_else(|| {
                StoreError::Internal("FilePerKey eviction byte total overflow".to_string())
            })
        })?;
        if total_victim_bytes > *self.used_space.read() {
            return Err(StoreError::Internal(format!(
                "FilePerKey usage invariant failed before eviction: victims={total_victim_bytes}, \
                 used={}",
                *self.used_space.read()
            )));
        }
        let mut changed_directories = HashSet::new();
        let mut first_error = None;
        for record in &pending.records {
            let path = self.data_dir().join(&record.relative_path);
            let removed = match std::fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        changed_directories.insert(parent.to_path_buf());
                    }
                    true
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // The reservation prevents an in-process delete or
                    // overwrite. A missing path therefore means an external
                    // unlink; reconcile the exact reserved generation once.
                    true
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    false
                }
            };
            if self
                .active_records
                .read()
                .get(&record.relative_path)
                .is_some_and(|current| current.generation == record.generation)
            {
                self.remove_active_record(&record.relative_path, record.generation)?;
                if removed {
                    let mut used = self.used_space.write();
                    *used = used.checked_sub(record.size).ok_or_else(|| {
                        StoreError::Internal(format!(
                            "FilePerKey usage underflow while evicting {:?}",
                            record.storage_key
                        ))
                    })?;
                } else {
                    self.orphaned_records
                        .lock()
                        .insert(record.relative_path.clone(), record.clone());
                }
            }
        }
        for directory in changed_directories {
            if let Err(error) = sync_directory(&directory) {
                first_error.get_or_insert(match error {
                    StoreError::Io(error) => error,
                    other => std::io::Error::other(other.to_string()),
                });
            }
        }
        if let Some(error) = first_error {
            return Err(error.into());
        }
        Ok(())
    }

    /// Read the value for a key from disk.
    ///
    /// C++ equivalent: `StorageBackendAdaptor::BatchLoad` (single-key path).
    pub fn read_object(&self, key: &str) -> StoreResult<Vec<u8>> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;

        let path = self.key_path(key);
        let relative_path = self.relative_record_path(&path)?;
        let record = self.active_records.read().get(&relative_path).cloned();
        let Some(record) = record else {
            if path.exists() {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey found an untracked record path: {}",
                    path.display()
                )));
            }
            return Err(StoreError::KeyNotFound(key.to_string()));
        };
        if !self.validate_record_on_disk(&record)? {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey active record disappeared outside the backend: {}",
                path.display()
            )));
        }
        let contents = std::fs::read(&path)?;
        let (stored_key, value) = decode_file_per_key_record(&contents)?;
        if stored_key != key {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey record key mismatch: requested={key:?}, stored={stored_key:?}"
            )));
        }
        Ok(value.to_vec())
    }

    /// Delete a key's file from disk.
    ///
    /// C++ equivalent: `StorageBackend::RemoveFile`.
    pub fn delete_object(&self, key: &str) -> StoreResult<()> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.cleanup_orphaned_records()?;

        let path = self.key_path(key);
        let relative_path = self.relative_record_path(&path)?;
        self.ensure_path_not_reserved(&relative_path)?;
        let record = self.active_records.read().get(&relative_path).cloned();
        let Some(record) = record else {
            if path.exists() {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey refuses to delete an untracked record path: {}",
                    path.display()
                )));
            }
            return Ok(());
        };
        if self.validate_record_on_disk(&record)? {
            std::fs::remove_file(&path)?;
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
        }
        self.remove_active_record(&relative_path, record.generation)?;
        let mut used = self.used_space.write();
        *used = used.checked_sub(record.size).ok_or_else(|| {
            StoreError::Internal(format!("FilePerKey usage underflow while deleting {key:?}"))
        })?;

        Ok(())
    }

    pub(crate) fn delete_object_if_generation(
        &self,
        key: &str,
        durable_generation_id: Uuid,
    ) -> StoreResult<bool> {
        if durable_generation_id.is_nil() {
            return Err(StoreError::InvalidParams(
                "FilePerKey generation must not be nil".to_string(),
            ));
        }
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.cleanup_orphaned_records()?;

        let path = self.key_path(key);
        let relative_path = self.relative_record_path(&path)?;
        self.ensure_path_not_reserved(&relative_path)?;
        let Some(record) = self.active_records.read().get(&relative_path).cloned() else {
            if path.exists() {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey refuses to delete an untracked record path: {}",
                    path.display()
                )));
            }
            return Ok(false);
        };
        if record.durable_generation_id != durable_generation_id {
            return Ok(false);
        }
        if self.validate_record_on_disk(&record)? {
            std::fs::remove_file(&path)?;
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
        }
        self.remove_active_record(&relative_path, record.generation)?;
        let mut used = self.used_space.write();
        *used = used.checked_sub(record.size).ok_or_else(|| {
            StoreError::Internal(format!("FilePerKey usage underflow while deleting {key:?}"))
        })?;
        Ok(true)
    }

    /// Check whether a key exists on disk.
    ///
    /// C++ equivalent: `StorageBackendAdaptor::IsExist`.
    pub fn exists(&self, key: &str) -> bool {
        let _mutation_guard = self.mutation_lock.lock();
        if self.validate_existing_storage_path().is_err() {
            return false;
        }
        let path = self.key_path(key);
        let Ok(relative_path) = self.relative_record_path(&path) else {
            return false;
        };
        self.active_records
            .read()
            .get(&relative_path)
            .is_some_and(|record| self.validate_record_on_disk(record).unwrap_or(false))
    }

    /// Return `(used_bytes, total_quota)`.
    pub fn space_usage(&self) -> (u64, u64) {
        (*self.used_space.read(), *self.total_space.read())
    }

    // ------------------------------------------------------------------
    // Bulk operations
    // ------------------------------------------------------------------

    /// Scan all keys and return `(key, size_bytes)` pairs.
    ///
    /// C++ equivalent: `StorageBackendAdaptor::ScanMeta`.
    pub fn scan_meta(&self) -> StoreResult<Vec<(String, u64)>> {
        Ok(self
            .scan_records()?
            .into_iter()
            .map(|record| (record.storage_key, record.value_size))
            .collect())
    }

    pub(crate) fn scan_records(&self) -> StoreResult<Vec<LocalStorageRecordMetadata>> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.ensure_no_pending_reservations()?;
        self.cleanup_orphaned_records()?;

        let data_dir = self.data_dir();
        let mut results = Vec::new();
        let file_entries = self.scan_files(&data_dir)?;
        let active = self.active_records.read();
        if file_entries.len() != active.len() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey scan/index mismatch: disk_records={}, active_records={}",
                file_entries.len(),
                active.len()
            )));
        }

        for entry in &file_entries {
            let Some(record) = active.get(&entry.relative_path) else {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey scan found untracked record {:?}",
                    entry.storage_key
                )));
            };
            if record.storage_key != entry.storage_key
                || record.size != entry.size
                || entry
                    .durable_generation_id
                    .is_some_and(|generation_id| generation_id != record.durable_generation_id)
            {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey scan found changed record {:?}",
                    entry.storage_key
                )));
            }
            results.push(LocalStorageRecordMetadata {
                storage_key: entry.storage_key.clone(),
                value_size: entry.value_size,
                generation_id: record.durable_generation_id,
            });
        }

        Ok(results)
    }

    /// Remove all files under the data directory.
    /// Returns the count of removed files.
    ///
    /// C++ equivalent: `StorageBackend::RemoveAll`.
    pub fn remove_all(&self) -> StoreResult<usize> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.ensure_no_pending_reservations()?;

        let data_dir = self.data_dir();
        let count = self.count_files(&data_dir)?;
        self.clean_storage_path_owned()?;

        Ok(count)
    }

    /// Return the durable identity of this local-storage namespace.
    ///
    /// Persistent backends keep this UUID across process restarts. Ephemeral
    /// backends generate a new UUID whenever they are initialized after their
    /// data has been cleared.
    pub fn storage_id(&self) -> StoreResult<Uuid> {
        self.ensure_init()?;
        self.storage_id.read().as_ref().copied().ok_or_else(|| {
            StoreError::Internal("FilePerKey storage identity was not initialized".to_string())
        })
    }

    /// Wipe every file and subdirectory under the data directory, keeping the
    /// directory itself. This is used by the P2P SSD/offload path, where local
    /// disk data is an ephemeral cache rather than durable FileStorage state.
    ///
    /// C++ equivalent: `StorageBackendInterface::CleanStoragePath()`, invoked
    /// by `StorageTier` on startup and destruction.
    pub fn clean_storage_path(&self) -> StoreResult<usize> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.ensure_no_pending_reservations()?;
        self.clean_storage_path_owned()
    }

    fn clean_storage_path_owned(&self) -> StoreResult<usize> {
        let data_dir = self.data_dir();
        self.validate_clean_storage_path(&data_dir)?;
        self.validate_existing_storage_path()?;
        self.ensure_no_pending_reservations()?;

        let mut removed = 0usize;
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            if entry.file_name() == FORMAT_MARKER_FILE || entry.file_name() == STORAGE_ID_FILE {
                continue;
            }
            std::fs::remove_dir_all(entry.path()).or_else(|err| {
                if err.kind() == std::io::ErrorKind::NotADirectory {
                    std::fs::remove_file(entry.path())
                } else {
                    Err(err)
                }
            })?;
            removed += 1;
        }
        sync_directory(&data_dir)?;

        self.write_queue.write().clear();
        self.active_records.write().clear();
        self.orphaned_records.lock().clear();
        *self.used_space.write() = 0;

        Ok(removed)
    }

    /// Remove keys whose logical storage keys match the given regex pattern.
    /// Returns the count of removed files.
    ///
    /// C++ equivalent: `StorageBackend::RemoveByRegex`.
    pub fn remove_by_regex(&self, pattern: &str) -> StoreResult<usize> {
        self.ensure_init()?;
        let _mutation_guard = self.mutation_lock.lock();
        self.ensure_storage_owner()?;
        self.validate_existing_storage_path()?;
        self.ensure_no_pending_reservations()?;
        self.cleanup_orphaned_records()?;

        let re = regex::Regex::new(pattern)
            .map_err(|e| StoreError::Internal(format!("invalid regex: {e}")))?;

        let data_dir = self.data_dir();
        let mut removed = 0usize;
        let mut changed_directories = HashSet::new();
        let file_entries = self.scan_files(&data_dir)?;

        for entry in &file_entries {
            if re.is_match(&entry.storage_key) {
                let abs_path = data_dir.join(&entry.relative_path);
                std::fs::remove_file(&abs_path)?;
                if let Some(parent) = abs_path.parent() {
                    changed_directories.insert(parent.to_path_buf());
                }
                let record = self
                    .active_records
                    .read()
                    .get(&entry.relative_path)
                    .cloned()
                    .ok_or_else(|| {
                        StoreError::InvalidParams(format!(
                            "FilePerKey regex scan found untracked record {:?}",
                            entry.storage_key
                        ))
                    })?;
                self.remove_active_record(&entry.relative_path, record.generation)?;
                let mut used = self.used_space.write();
                *used = used.checked_sub(record.size).ok_or_else(|| {
                    StoreError::Internal(format!(
                        "FilePerKey usage underflow while deleting {:?}",
                        entry.storage_key
                    ))
                })?;
                removed += 1;
            }
        }
        for directory in changed_directories {
            sync_directory(&directory)?;
        }

        Ok(removed)
    }

    // ------------------------------------------------------------------
    // Eviction (FIFO)
    // ------------------------------------------------------------------

    /// Ensure at least `required` bytes are free. Evicts oldest files (FIFO)
    /// if the quota would be exceeded. Returns the list of evicted keys.
    ///
    /// C++ equivalent: `StorageBackend::EnsureDiskSpace`.
    fn prepare_space_eviction(
        &self,
        required: u64,
        write_growth: u64,
        pending: &mut PendingLocalEviction,
    ) -> StoreResult<()> {
        let total = *self.total_space.read();
        let used = *self.used_space.read();
        let other_reserved_growth = self
            .reservation_registry
            .write_growth
            .lock()
            .values()
            .copied()
            .fold(0_u64, u64::saturating_add);
        let quota_deficit = used
            .saturating_add(other_reserved_growth)
            .saturating_add(write_growth)
            .saturating_sub(total);
        let disk_deficit = match (self.available_space_probe)(&self.data_dir()) {
            Ok(available) => required
                .saturating_add(MIN_FREE_SPACE_BYTES)
                .saturating_sub(available),
            Err(error) => {
                tracing::warn!(
                    "failed to query available disk space for {:?}: {error}",
                    self.data_dir()
                );
                0
            }
        };
        self.prepare_eviction_bytes(quota_deficit.max(disk_deficit), pending)
    }

    fn prepare_eviction_bytes(
        &self,
        bytes_to_free: u64,
        pending: &mut PendingLocalEviction,
    ) -> StoreResult<()> {
        if bytes_to_free == 0 {
            return Ok(());
        }

        let mut selected_bytes = 0u64;
        while selected_bytes < bytes_to_free {
            match self.reserve_one_eviction(pending.token) {
                Some(record) => {
                    selected_bytes = selected_bytes.saturating_add(record.size);
                    pending.records.push(record);
                }
                None => {
                    return Err(StoreError::Internal(
                        "disk full: cannot satisfy quota after eviction".to_string(),
                    ));
                }
            }
        }

        // Compact if too many stale entries.
        let stale_ratio = {
            let queue = self.write_queue.read();
            let active = self.active_records.read();
            if queue.is_empty() {
                0.0
            } else {
                let live_queue_entries = queue
                    .iter()
                    .filter(|record| {
                        active
                            .get(&record.relative_path)
                            .is_some_and(|current| current.generation == record.generation)
                    })
                    .count();
                1.0 - (live_queue_entries as f64 / queue.len() as f64)
            }
        };
        if stale_ratio > 0.5 {
            self.compact_write_queue();
        }

        Ok(())
    }

    /// Reserve the oldest valid, currently-unreserved FIFO record.
    fn reserve_one_eviction(&self, token: u64) -> Option<FileRecord> {
        let queue = self.write_queue.read();
        let active = self.active_records.read();
        let mut reserved = self.reservation_registry.paths.lock();
        let record = queue
            .iter()
            .find(|record| {
                active
                    .get(&record.relative_path)
                    .is_some_and(|current| current.generation == record.generation)
                    && !reserved.contains_key(&record.relative_path)
            })?
            .clone();
        reserved.insert(record.relative_path.clone(), token);
        Some(record)
    }

    fn install_active_record(&self, record: FileRecord) {
        self.active_records
            .write()
            .insert(record.relative_path.clone(), record.clone());
        if self.config.enable_eviction {
            self.write_queue.write().push_back(record);
        }
    }

    fn remove_active_record(&self, file_path: &str, generation: u64) -> StoreResult<FileRecord> {
        let mut active = self.active_records.write();
        let Some(current) = active.get(file_path) else {
            return Err(StoreError::Internal(format!(
                "FilePerKey active record disappeared: {file_path}"
            )));
        };
        if current.generation != generation {
            return Err(StoreError::Internal(format!(
                "FilePerKey record generation changed for {file_path}: expected {generation}, \
                 found {}",
                current.generation
            )));
        }
        Ok(active
            .remove(file_path)
            .expect("record was checked while holding active_records write lock"))
    }

    /// Compact stale entries from the write queue.
    fn compact_write_queue(&self) {
        let mut queue = self.write_queue.write();
        let active = self.active_records.read();
        queue.retain(|record| {
            active
                .get(&record.relative_path)
                .is_some_and(|current| current.generation == record.generation)
        });
    }

    fn allocate_generation(&self) -> StoreResult<u64> {
        self.next_generation
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| StoreError::Internal("FilePerKey record generation exhausted".to_string()))
    }

    fn allocate_reservation(&self) -> StoreResult<u64> {
        self.next_reservation
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| StoreError::Internal("FilePerKey reservation token exhausted".to_string()))
    }

    fn relative_record_path(&self, path: &Path) -> StoreResult<String> {
        path.strip_prefix(self.data_dir())
            .map_err(|_| {
                StoreError::InvalidParams(format!(
                    "FilePerKey record path escapes data directory: {}",
                    path.display()
                ))
            })
            .map(|relative| relative.to_string_lossy().to_string())
    }

    fn reserve_write_target(&self, key: &str, token: u64) -> StoreResult<WriteTargetReservation> {
        let path = self.key_path(key);
        let relative_path = self.relative_record_path(&path)?;
        {
            let reserved = self.reservation_registry.paths.lock();
            if let Some(owner) = reserved.get(&relative_path) {
                return Err(StoreError::Internal(format!(
                    "FilePerKey record mutation is already in progress for {key:?} \
                     (reservation {owner})"
                )));
            }
        }

        let previous = self.active_records.read().get(&relative_path).cloned();
        match &previous {
            Some(record) => {
                if !self.validate_record_on_disk(record)? {
                    return Err(StoreError::InvalidParams(format!(
                        "FilePerKey active record disappeared outside the backend: {}",
                        path.display()
                    )));
                }
            }
            None => match std::fs::symlink_metadata(&path) {
                Ok(_) => {
                    return Err(StoreError::InvalidParams(format!(
                        "FilePerKey refuses to overwrite an untracked record path: {}",
                        path.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            },
        }

        self.reservation_registry
            .paths
            .lock()
            .insert(relative_path.clone(), token);
        Ok(WriteTargetReservation {
            relative_path,
            previous,
        })
    }

    fn validate_pending_write(
        &self,
        key: &str,
        actual_record_size: u64,
        pending: &PendingLocalEviction,
    ) -> StoreResult<()> {
        if pending.backend_id != self.backend_id {
            return Err(StoreError::Internal(
                "FilePerKey write reservation belongs to another backend instance".to_string(),
            ));
        }
        if !pending.has_reservations() {
            return Err(StoreError::Internal(
                "FilePerKey write commit has no reservation token".to_string(),
            ));
        }
        let target = pending.write_target.as_ref().ok_or_else(|| {
            StoreError::Internal("FilePerKey write commit has no target reservation".to_string())
        })?;
        if pending.expected_record_size != Some(actual_record_size) {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey commit size differs from prepare: expected {:?}, got \
                 {actual_record_size}",
                pending.expected_record_size
            )));
        }
        let expected_path = self.relative_record_path(&self.key_path(key))?;
        if target.relative_path != expected_path {
            return Err(StoreError::Internal(format!(
                "FilePerKey write reservation target mismatch: expected {expected_path}, got {}",
                target.relative_path
            )));
        }
        if self
            .reservation_registry
            .paths
            .lock()
            .get(&target.relative_path)
            .copied()
            != Some(pending.token)
        {
            return Err(StoreError::Internal(format!(
                "FilePerKey write reservation token is stale for {key:?}"
            )));
        }

        let current = self
            .active_records
            .read()
            .get(&target.relative_path)
            .cloned();
        if current != target.previous {
            return Err(StoreError::Internal(format!(
                "FilePerKey write target generation changed for {key:?}"
            )));
        }
        match &target.previous {
            Some(record) if !self.validate_record_on_disk(record)? => {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey write target disappeared outside the backend: {}",
                    self.data_dir().join(&target.relative_path).display()
                )));
            }
            None if self.data_dir().join(&target.relative_path).exists() => {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey write target appeared outside the backend: {}",
                    self.data_dir().join(&target.relative_path).display()
                )));
            }
            _ => {}
        }
        Ok(())
    }

    fn preflight_eviction_records(&self, pending: &PendingLocalEviction) -> StoreResult<()> {
        if pending.records.is_empty() {
            return Ok(());
        }
        if pending.backend_id != self.backend_id {
            return Err(StoreError::Internal(
                "FilePerKey eviction reservation belongs to another backend instance".to_string(),
            ));
        }
        if !pending.has_reservations() {
            return Err(StoreError::Internal(
                "FilePerKey eviction commit has no reservation token".to_string(),
            ));
        }
        let active = self.active_records.read();
        let reserved = self.reservation_registry.paths.lock();
        for record in &pending.records {
            if reserved.get(&record.relative_path).copied() != Some(pending.token) {
                return Err(StoreError::Internal(format!(
                    "FilePerKey eviction reservation token is stale for {:?}",
                    record.storage_key
                )));
            }
            let Some(current) = active.get(&record.relative_path) else {
                return Err(StoreError::Internal(format!(
                    "FilePerKey eviction record disappeared from the active index: {:?}",
                    record.storage_key
                )));
            };
            if current != record {
                return Err(StoreError::Internal(format!(
                    "FilePerKey eviction generation changed for {:?}",
                    record.storage_key
                )));
            }
            // A missing file is an externally completed deletion and is
            // reconciled by commit. Any present file must still be exactly the
            // generation selected by prepare.
            let _ = self.validate_record_on_disk(record)?;
        }
        Ok(())
    }

    fn validate_record_on_disk(&self, record: &FileRecord) -> StoreResult<bool> {
        let path = self.data_dir().join(&record.relative_path);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey record is not a regular file: {}",
                path.display()
            )));
        }
        if metadata.len() != record.size {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey record size changed outside the backend: {} (expected {}, found {})",
                path.display(),
                record.size,
                metadata.len()
            )));
        }
        let contents = std::fs::read(&path)?;
        let (storage_key, _, durable_generation_id) =
            decode_file_per_key_record_with_generation(&contents)?;
        if storage_key != record.storage_key {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey record key changed outside the backend: {}",
                path.display()
            )));
        }
        if self.key_path(storage_key) != path {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey record no longer occupies its canonical path: {}",
                path.display()
            )));
        }
        let durable_generation_id = durable_generation_id.unwrap_or_else(|| {
            let storage_id = self
                .storage_id
                .read()
                .as_ref()
                .copied()
                .expect("initialized FilePerKey has a storage identity");
            mooncake_store_core::legacy_local_disk_generation_id(storage_id, storage_key)
        });
        if durable_generation_id != record.durable_generation_id {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey record generation changed outside the backend: {}",
                path.display()
            )));
        }
        Ok(true)
    }

    fn rollback_eviction_owned(&self, pending: PendingLocalEviction) {
        drop(pending);
    }

    /// Once the master has accepted an eviction notification, victims are
    /// logically irreversible even if local unlink/fsync later fails. Remove
    /// any still-active generations and retain present files as unpublishable
    /// orphans for retry instead of returning them to the FIFO.
    fn abandon_pending_victims(&self, pending: &PendingLocalEviction) {
        for record in &pending.records {
            let is_active = self
                .active_records
                .read()
                .get(&record.relative_path)
                .is_some_and(|current| current.generation == record.generation);
            if !is_active {
                continue;
            }
            if let Err(error) = self.remove_active_record(&record.relative_path, record.generation)
            {
                tracing::error!(
                    storage_key = %record.storage_key,
                    %error,
                    "failed to abandon accepted FilePerKey eviction"
                );
                continue;
            }
            let path = self.data_dir().join(&record.relative_path);
            match std::fs::symlink_metadata(&path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let mut used = self.used_space.write();
                    match used.checked_sub(record.size) {
                        Some(remaining) => *used = remaining,
                        None => tracing::error!(
                            storage_key = %record.storage_key,
                            used = *used,
                            record_size = record.size,
                            "FilePerKey usage underflow while abandoning missing victim"
                        ),
                    }
                }
                _ => {
                    self.orphaned_records
                        .lock()
                        .insert(record.relative_path.clone(), record.clone());
                }
            }
        }
    }

    fn cleanup_orphaned_records(&self) -> StoreResult<()> {
        let records = self
            .orphaned_records
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut changed_directories = HashSet::new();
        for record in records {
            let path = self.data_dir().join(&record.relative_path);
            if self.validate_record_on_disk(&record)? {
                std::fs::remove_file(&path)?;
                if let Some(parent) = path.parent() {
                    changed_directories.insert(parent.to_path_buf());
                }
            }
            let removed = self
                .orphaned_records
                .lock()
                .get(&record.relative_path)
                .is_some_and(|current| current.generation == record.generation);
            if removed {
                self.orphaned_records.lock().remove(&record.relative_path);
                let mut used = self.used_space.write();
                *used = used.checked_sub(record.size).ok_or_else(|| {
                    StoreError::Internal(format!(
                        "FilePerKey usage underflow while cleaning orphan {:?}",
                        record.storage_key
                    ))
                })?;
            }
        }
        for directory in changed_directories {
            sync_directory(&directory)?;
        }
        Ok(())
    }

    fn ensure_path_not_reserved(&self, relative_path: &str) -> StoreResult<()> {
        if let Some(token) = self
            .reservation_registry
            .paths
            .lock()
            .get(relative_path)
            .copied()
        {
            return Err(StoreError::Internal(format!(
                "FilePerKey record mutation is in progress (reservation {token})"
            )));
        }
        Ok(())
    }

    fn ensure_no_pending_reservations(&self) -> StoreResult<()> {
        if !self.reservation_registry.paths.lock().is_empty() {
            return Err(StoreError::Internal(
                "FilePerKey bulk operation cannot run while record mutations are pending"
                    .to_string(),
            ));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn ensure_init(&self) -> StoreResult<()> {
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(StoreError::Internal(
                "LocalStorageBackend not initialized — call init() first".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_storage_owner(&self) -> StoreResult<()> {
        if self.storage_lock.lock().is_none() {
            return Err(StoreError::Internal(
                "FilePerKey backend does not own the storage directory lock".to_string(),
            ));
        }
        Ok(())
    }

    fn acquire_storage_lock(&self) -> StoreResult<()> {
        if self.storage_lock.lock().is_some() {
            return Ok(());
        }
        self.validate_clean_storage_path(&self.data_dir())?;
        std::fs::create_dir_all(&self.config.root_dir)?;
        let lock_path = self
            .config
            .root_dir
            .join(format!(".{}.mooncake-storage.lock", self.config.fsdir));
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey storage directory is already open by another backend ({}): {error}",
                self.data_dir().display()
            ))
        })?;
        *self.storage_lock.lock() = Some(lock_file);
        Ok(())
    }

    fn count_files(&self, dir: &Path) -> StoreResult<usize> {
        if !dir.exists() {
            return Ok(0);
        }
        let mut count = 0usize;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if entry.file_name() == TEMP_DIRECTORY {
                    continue;
                }
                count += self.count_files(&path)?;
            } else if path.is_file() {
                if path == self.format_marker_path() || path == self.storage_id_path() {
                    continue;
                }
                count += 1;
            }
        }
        Ok(count)
    }

    /// Validate an already-initialized storage directory without creating or
    /// repairing anything. Runtime mutations must fail closed when an
    /// administrator removes/replaces the directory or ownership marker;
    /// otherwise the in-memory FIFO/accounting state would describe files
    /// that no longer exist.
    fn capture_namespace_identity(&self) -> StoreResult<NamespaceIdentity> {
        let data_metadata = std::fs::symlink_metadata(self.data_dir())?;
        let marker_metadata = std::fs::symlink_metadata(self.format_marker_path())?;
        let storage_id_metadata = std::fs::symlink_metadata(self.storage_id_path())?;
        Ok(NamespaceIdentity {
            data_directory: filesystem_identity(&data_metadata),
            format_marker: filesystem_identity(&marker_metadata),
            storage_id: filesystem_identity(&storage_id_metadata),
        })
    }

    fn validate_existing_storage_path(&self) -> StoreResult<()> {
        let data_dir = self.data_dir();
        self.validate_clean_storage_path(&data_dir)?;
        let canonical_root = std::fs::canonicalize(&self.config.root_dir).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey root directory disappeared after initialization ({}): {error}",
                self.config.root_dir.display()
            ))
        })?;
        let data_metadata = std::fs::symlink_metadata(&data_dir).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey storage directory disappeared after initialization ({}): {error}",
                data_dir.display()
            ))
        })?;
        if data_metadata.file_type().is_symlink() || !data_metadata.is_dir() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage directory was replaced after initialization: {}",
                data_dir.display()
            )));
        }
        let canonical_data_dir = std::fs::canonicalize(&data_dir)?;
        if canonical_data_dir.parent() != Some(canonical_root.as_path()) {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage directory no longer belongs to root_dir: {}",
                data_dir.display()
            )));
        }

        let marker_path = self.format_marker_path();
        let marker_metadata = std::fs::symlink_metadata(&marker_path).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey ownership marker disappeared after initialization ({}): {error}",
                marker_path.display()
            ))
        })?;
        if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey ownership marker was replaced after initialization: {}",
                marker_path.display()
            )));
        }
        let marker = std::fs::read_to_string(&marker_path)?;
        if marker != self.expected_format_marker() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey ownership marker changed after initialization: {}",
                marker_path.display()
            )));
        }
        let storage_id_path = self.storage_id_path();
        let storage_id_metadata = std::fs::symlink_metadata(&storage_id_path).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey storage identity disappeared after initialization ({}): {error}",
                storage_id_path.display()
            ))
        })?;
        if storage_id_metadata.file_type().is_symlink() || !storage_id_metadata.is_file() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage identity was replaced after initialization: {}",
                storage_id_path.display()
            )));
        }
        let persisted_storage_id = self.read_storage_id()?;
        let expected_storage_id = self.storage_id.read().as_ref().copied().ok_or_else(|| {
            StoreError::Internal("FilePerKey storage identity was not initialized".to_string())
        })?;
        if persisted_storage_id != expected_storage_id {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage identity changed after initialization: expected \
                 {expected_storage_id}, found {persisted_storage_id}"
            )));
        }
        let current_identity = NamespaceIdentity {
            data_directory: filesystem_identity(&data_metadata),
            format_marker: filesystem_identity(&marker_metadata),
            storage_id: filesystem_identity(&storage_id_metadata),
        };
        let expected_identity = *self.namespace_identity.lock();
        match expected_identity {
            Some(expected) if expected == current_identity => {}
            Some(expected) => {
                return Err(StoreError::InvalidParams(format!(
                    "FilePerKey storage namespace identity changed after initialization: \
                     expected {expected:?}, found {current_identity:?}"
                )));
            }
            None => {
                return Err(StoreError::Internal(
                    "FilePerKey storage namespace identity was not initialized".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn claim_or_validate_storage_path(&self, cleanup_stale_temp: bool) -> StoreResult<()> {
        let data_dir = self.data_dir();
        self.validate_clean_storage_path(&data_dir)?;
        std::fs::create_dir_all(&self.config.root_dir)?;
        let canonical_root = std::fs::canonicalize(&self.config.root_dir)?;
        if !data_dir.exists() {
            std::fs::create_dir(&data_dir)?;
            sync_directory(&canonical_root)?;
            return self.write_format_marker();
        }
        let data_metadata = std::fs::symlink_metadata(&data_dir)?;
        if data_metadata.file_type().is_symlink() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage path must not be a symbolic link: {}",
                data_dir.display()
            )));
        }
        if !data_metadata.is_dir() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage path is not a directory: {}",
                data_dir.display()
            )));
        }
        let canonical_data_dir = std::fs::canonicalize(&data_dir)?;
        if canonical_data_dir.parent() != Some(canonical_root.as_path()) {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage path is not a direct child of root_dir: {}",
                data_dir.display()
            )));
        }

        let marker_path = self.format_marker_path();
        if marker_path.exists() {
            let marker = std::fs::read_to_string(&marker_path)?;
            if marker == self.expected_format_marker() {
                if cleanup_stale_temp {
                    self.cleanup_temp_namespace()?;
                }
                return Ok(());
            }
            if marker == self.legacy_format_marker() {
                if cleanup_stale_temp {
                    self.cleanup_temp_namespace()?;
                }
                // v2 remains able to read v1 field-1/field-2 records. Publish
                // the upgraded namespace marker before capturing its inode.
                self.write_format_marker()?;
                return Ok(());
            }
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage path has an unsupported or foreign format marker: {}",
                marker_path.display()
            )));
        }

        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            if entry.file_name() == TEMP_DIRECTORY {
                let file_type = entry.file_type()?;
                if cleanup_stale_temp && file_type.is_dir() && !file_type.is_symlink() {
                    std::fs::remove_dir_all(entry.path())?;
                    sync_directory(&data_dir)?;
                    continue;
                }
            }
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey refuses to clear non-empty unowned storage path {}; \
                 remove or migrate its contents explicitly",
                data_dir.display()
            )));
        }
        self.write_format_marker()
    }

    fn cleanup_temp_namespace(&self) -> StoreResult<()> {
        let temp_dir = self.temp_dir();
        match std::fs::symlink_metadata(&temp_dir) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                Err(StoreError::InvalidParams(format!(
                    "FilePerKey temp namespace is not a regular directory: {}",
                    temp_dir.display()
                )))
            }
            Ok(_) => {
                std::fs::remove_dir_all(&temp_dir)?;
                sync_directory(&self.data_dir())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn write_format_marker(&self) -> StoreResult<()> {
        let marker_path = self.format_marker_path();
        write_file_atomically(
            &marker_path,
            self.expected_format_marker().as_bytes(),
            &self.temp_dir(),
        )
        .map_err(|failure| failure.error)
    }

    fn load_or_create_storage_id(&self, regenerate: bool) -> StoreResult<Uuid> {
        let path = self.storage_id_path();
        if !regenerate && path.exists() {
            return self.read_storage_id();
        }
        let storage_id = Uuid::new_v4();
        let contents = format!("{storage_id}\n");
        write_file_atomically(&path, contents.as_bytes(), &self.temp_dir())
            .map_err(|failure| failure.error)?;
        Ok(storage_id)
    }

    fn read_storage_id(&self) -> StoreResult<Uuid> {
        let path = self.storage_id_path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey storage identity is missing ({}): {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StoreError::InvalidParams(format!(
                "FilePerKey storage identity must be a regular file: {}",
                path.display()
            )));
        }
        let value = std::fs::read_to_string(&path)?;
        let storage_id = Uuid::parse_str(value.trim()).map_err(|error| {
            StoreError::InvalidParams(format!(
                "FilePerKey storage identity is invalid ({}): {error}",
                path.display()
            ))
        })?;
        if storage_id.is_nil() {
            return Err(StoreError::InvalidParams(
                "FilePerKey storage identity must not be nil".to_string(),
            ));
        }
        Ok(storage_id)
    }

    fn validate_clean_storage_path(&self, data_dir: &Path) -> StoreResult<()> {
        let mut components = Path::new(&self.config.fsdir).components();
        let valid_fsdir = matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(_)), None)
        );
        if !valid_fsdir {
            return Err(StoreError::InvalidParams(
                "LocalStorageBackend fsdir must be exactly one normal path component".to_string(),
            ));
        }
        if !self.config.root_dir.is_absolute()
            || !data_dir.is_absolute()
            || data_dir.parent().is_none()
        {
            return Err(StoreError::InvalidParams(format!(
                "LocalStorageBackend refuses to clean unsafe storage path: {}",
                data_dir.display()
            )));
        }
        Ok(())
    }
}

fn validate_watermark_ratios(high: f64, low: f64) -> StoreResult<()> {
    if !high.is_finite()
        || !low.is_finite()
        || !(0.0..=1.0).contains(&high)
        || high == 0.0
        || !(0.0..=1.0).contains(&low)
        || low == 0.0
        || low >= high
    {
        return Err(StoreError::InvalidParams(
            "disk eviction watermarks must satisfy 0 < low < high <= 1".to_string(),
        ));
    }
    Ok(())
}

impl Drop for LocalStorageBackend {
    fn drop(&mut self) {
        if self.lifecycle == FilePerKeyLifecycle::Persistent
            || !self.initialized.load(Ordering::SeqCst)
            || self.storage_lock.get_mut().is_none()
        {
            return;
        }
        if let Err(err) = self.clean_storage_path_owned() {
            tracing::warn!("LocalStorageBackend clean_storage_path on drop failed: {err}");
        }
    }
}

// Safety: all interior mutability is behind parking_lot::RwLock.
unsafe impl Send for LocalStorageBackend {}
unsafe impl Sync for LocalStorageBackend {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn backend_with_available_space_sequence(
        quota: u64,
        values: Vec<u64>,
    ) -> (LocalStorageBackend, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = LocalStorageConfig {
            root_dir: tmp.path().to_path_buf(),
            fsdir: "test_data".to_string(),
            enable_eviction: true,
            quota_bytes: quota,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = LocalStorageBackend::new_with_available_space_probe(
            config,
            Box::new(move |_| {
                let index = calls.fetch_add(1, Ordering::SeqCst);
                Ok(*values
                    .get(index)
                    .unwrap_or_else(|| values.last().expect("space sequence is empty")))
            }),
        );
        backend.init().unwrap();
        (backend, tmp)
    }

    #[test]
    fn actual_disk_space_shortage_triggers_fifo_eviction() {
        let enough_space = MIN_FREE_SPACE_BYTES + 1024;
        let (backend, _tmp) = backend_with_available_space_sequence(
            500,
            vec![enough_space, MIN_FREE_SPACE_BYTES + 49],
        );

        backend.write_object("old", &[0u8; 100]).unwrap();
        let evicted = backend.write_object("new", &[0u8; 50]).unwrap();

        assert_eq!(evicted, vec!["old".to_string()]);
        assert!(!backend.exists("old"));
        assert!(backend.exists("new"));
    }

    #[test]
    fn tenant_storage_key_round_trips_without_path_delimiters() {
        let storage_key = local_storage_key("租户/a", "模型/key:1");
        assert!(!storage_key.contains('\0'));
        assert_eq!(
            parse_local_storage_key(&storage_key),
            ("租户/a", "模型/key:1")
        );

        let default_key = local_storage_key("", "path/to/key");
        assert_eq!(
            parse_local_storage_key(&default_key),
            ("default", "path/to/key")
        );
    }

    #[test]
    fn scan_files_returns_named_file_entries() {
        let (backend, _tmp) = backend_with_available_space_sequence(100, vec![u64::MAX]);
        backend.write_object("tenant/key", &[1, 2, 3]).unwrap();

        let entries = backend.scan_files(&backend.data_dir()).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].storage_key, "tenant/key");
        assert_eq!(entries[0].size, 35);
        assert_eq!(entries[0].value_size, 3);
        assert_eq!(
            Path::new(&entries[0].relative_path)
                .file_name()
                .unwrap()
                .to_string_lossy(),
            backend
                .key_path("tenant/key")
                .file_name()
                .unwrap()
                .to_string_lossy()
        );
    }

    #[test]
    fn watermark_eviction_can_be_rolled_back_without_deleting_files() {
        let (backend, _tmp) = backend_with_available_space_sequence(180, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[0u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[0u8; 20]).unwrap();

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(pending.keys(), vec!["tenant/key-a"]);
        assert!(backend.exists("tenant/key-a"));
        assert_eq!(backend.space_usage(), (148, 180));

        backend.rollback_eviction(pending);
        let pending_again = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(pending_again.keys(), vec!["tenant/key-a"]);
    }

    #[test]
    fn watermark_eviction_commit_deletes_fifo_victims() {
        let (backend, _tmp) = backend_with_available_space_sequence(180, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[0u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[0u8; 20]).unwrap();

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        backend.commit_eviction(pending).unwrap();

        assert!(!backend.exists("tenant/key-a"));
        assert!(backend.exists("tenant/key-b"));
        assert_eq!(backend.space_usage(), (54, 180));
    }

    #[test]
    fn protobuf_kv_record_matches_cpp_struct_pb_wire_shape() {
        let encoded = encode_file_per_key_record("key", b"value").unwrap();
        assert_eq!(encoded, b"\x0a\x03key\x12\x05value");
        let (key, value) = decode_file_per_key_record(&encoded).unwrap();
        assert_eq!(key, "key");
        assert_eq!(value, b"value");

        let empty = encode_file_per_key_record("empty", b"").unwrap();
        assert_eq!(empty, b"\x0a\x05empty");
        let (key, value) = decode_file_per_key_record(&empty).unwrap();
        assert_eq!(key, "empty");
        assert!(value.is_empty());
    }

    #[test]
    fn persistent_backend_preserves_exact_slashed_key_across_restart() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = LocalStorageConfig {
            root_dir: tmp.path().to_path_buf(),
            fsdir: "persistent".to_string(),
            enable_eviction: false,
            quota_bytes: 1024,
        };
        {
            let backend = LocalStorageBackend::new_persistent(config.clone());
            backend.init().unwrap();
            backend.write_object("tenant/key", b"value").unwrap();
        }
        let backend = LocalStorageBackend::new_persistent(config);
        backend.init().unwrap();
        assert_eq!(backend.read_object("tenant/key").unwrap(), b"value");
        assert_eq!(
            backend.scan_meta().unwrap(),
            [("tenant/key".to_string(), 5)]
        );
    }

    #[test]
    fn pending_eviction_reservation_blocks_overwrite_but_allows_read() {
        let (backend, _tmp) = backend_with_available_space_sequence(180, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[7_u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[8_u8; 20]).unwrap();

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(pending.keys(), ["tenant/key-a"]);
        assert_eq!(backend.read_object("tenant/key-a").unwrap(), vec![7_u8; 60]);
        assert!(backend.scan_meta().is_err());
        assert!(
            backend
                .write_object("tenant/key-a", b"replacement")
                .is_err()
        );
        assert!(backend.delete_object("tenant/key-a").is_err());

        backend.rollback_eviction(pending);
        backend
            .write_object("tenant/key-a", b"replacement")
            .unwrap();
        assert_eq!(backend.read_object("tenant/key-a").unwrap(), b"replacement");
    }

    #[test]
    fn pending_writes_reserve_same_key_and_future_quota() {
        let (backend, _tmp) = backend_with_available_space_sequence(126, vec![u64::MAX]);

        let first = backend.prepare_write("a", 40).unwrap();
        assert!(backend.prepare_write("a", 40).is_err());
        let second = backend.prepare_write("b", 40).unwrap();
        assert!(
            backend.prepare_write("c", 40).is_err(),
            "the third prepare must account for both in-flight write reservations"
        );

        backend.commit_write("a", &[1_u8; 40], first).unwrap();
        backend.commit_write("b", &[2_u8; 40], second).unwrap();
        assert_eq!(backend.space_usage(), (126, 126));
        assert_eq!(backend.read_object("a").unwrap(), vec![1_u8; 40]);
        assert_eq!(backend.read_object("b").unwrap(), vec![2_u8; 40]);
    }

    #[test]
    fn external_unlink_of_reserved_victim_is_reconciled_once() {
        let (backend, _tmp) = backend_with_available_space_sequence(180, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[0_u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[0_u8; 20]).unwrap();

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        std::fs::remove_file(backend.key_path("tenant/key-a")).unwrap();
        backend.commit_eviction(pending).unwrap();

        assert_eq!(backend.space_usage(), (54, 180));
        assert!(!backend.exists("tenant/key-a"));
        assert!(backend.exists("tenant/key-b"));
    }

    #[test]
    fn runtime_directory_loss_fails_closed_without_recreating_state() {
        let (backend, _tmp) = backend_with_available_space_sequence(140, vec![u64::MAX]);
        backend.write_object("tenant/key", b"value").unwrap();
        let data_dir = backend.data_dir();
        std::fs::remove_dir_all(&data_dir).unwrap();

        assert!(backend.write_object("tenant/other", b"value").is_err());
        assert!(!data_dir.exists());
        assert_eq!(backend.space_usage().0, 37);
    }

    #[test]
    fn recreated_directory_with_matching_marker_still_fails_namespace_epoch() {
        let (backend, _tmp) = backend_with_available_space_sequence(140, vec![u64::MAX]);
        backend.write_object("tenant/key", b"value").unwrap();
        let data_dir = backend.data_dir();
        let marker = backend.expected_format_marker().as_bytes().to_vec();
        let storage_id = std::fs::read(backend.storage_id_path()).unwrap();
        std::fs::remove_dir_all(&data_dir).unwrap();
        std::fs::create_dir(&data_dir).unwrap();
        std::fs::write(backend.format_marker_path(), marker).unwrap();
        std::fs::write(backend.storage_id_path(), storage_id).unwrap();

        let error = backend.write_object("tenant/other", b"value").unwrap_err();
        assert!(error.to_string().contains("namespace identity changed"));
        assert!(!backend.key_path("tenant/other").exists());
    }

    #[test]
    fn dropping_pending_lease_releases_target_and_fifo_victim() {
        let (backend, _tmp) = backend_with_available_space_sequence(180, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[0_u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[0_u8; 20]).unwrap();

        let pending_write = backend.prepare_write("tenant/new", 1).unwrap();
        assert!(backend.prepare_write("tenant/new", 1).is_err());
        drop(pending_write);
        let retry = backend.prepare_write("tenant/new", 1).unwrap();
        drop(retry);

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(pending.keys(), ["tenant/key-a"]);
        drop(pending);
        let retry = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(retry.keys(), ["tenant/key-a"]);
    }

    #[test]
    fn commit_rejects_data_size_that_differs_from_prepare() {
        let (backend, _tmp) = backend_with_available_space_sequence(140, vec![u64::MAX]);
        let pending = backend.prepare_write("tenant/key", 1).unwrap();
        assert!(
            backend
                .commit_write("tenant/key", b"two-bytes", pending)
                .is_err()
        );
        assert!(!backend.exists("tenant/key"));

        backend.write_object("tenant/key", b"two-bytes").unwrap();
        assert_eq!(backend.read_object("tenant/key").unwrap(), b"two-bytes");
    }

    #[test]
    fn rename_visibility_updates_generation_before_durability_error() {
        let (backend, _tmp) = backend_with_available_space_sequence(140, vec![u64::MAX]);
        let pending = backend.prepare_write("tenant/key", 5).unwrap();
        std::fs::create_dir(backend.temp_dir()).unwrap();
        std::fs::write(backend.temp_dir().join("block-remove-dir"), b"x").unwrap();

        assert!(
            backend
                .commit_write("tenant/key", b"value", pending)
                .is_err(),
            "the deliberately non-empty temp namespace must surface a durability error"
        );
        assert_eq!(backend.read_object("tenant/key").unwrap(), b"value");
        assert_eq!(backend.space_usage(), (37, 140));
    }
}
