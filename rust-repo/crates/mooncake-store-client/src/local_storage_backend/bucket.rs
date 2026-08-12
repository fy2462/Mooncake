use super::LocalStorageRecordMetadata;
use super::config::{BucketEvictionPolicy, BucketStorageConfig};
use fs2::FileExt;
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const BUCKET_FORMAT_VERSION: u32 = 1;
const BUCKET_FORMAT_MARKER: &str = "backend=bucket\nformat=json\nversion=1\n";
const FORMAT_MARKER_FILE: &str = ".mooncake-storage-format";
const STORAGE_ID_FILE: &str = ".mooncake-storage-id";
const BUCKET_DIRECTORY: &str = "buckets";
const TEMP_DIRECTORY: &str = ".mooncake-tmp";
const LOCK_FILE: &str = ".mooncake-storage.lock";
const MIN_FREE_SPACE_BYTES: u64 = 256 * 1024 * 1024;
const ACCEPTED_EVICTION_JOURNAL: &str = ".mooncake-accepted-evictions.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DurableBucketEntry {
    storage_key: String,
    value: Vec<u8>,
    generation_id: String,
}

impl DurableBucketEntry {
    fn generation(&self) -> StoreResult<Uuid> {
        let generation = Uuid::parse_str(&self.generation_id).map_err(|error| {
            StoreError::InvalidParams(format!(
                "bucket entry {:?} has invalid generation: {error}",
                self.storage_key
            ))
        })?;
        if generation.is_nil() {
            return Err(StoreError::InvalidParams(format!(
                "bucket entry {:?} has nil generation",
                self.storage_key
            )));
        }
        Ok(generation)
    }

    fn logical_size(&self) -> StoreResult<u64> {
        let key_size = u64::try_from(self.storage_key.len())
            .map_err(|_| StoreError::Internal("bucket key size overflow".to_string()))?;
        let value_size = u64::try_from(self.value.len())
            .map_err(|_| StoreError::Internal("bucket value size overflow".to_string()))?;
        key_size
            .checked_add(value_size)
            .ok_or_else(|| StoreError::Internal("bucket entry size overflow".to_string()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DurableBucket {
    version: u32,
    bucket_id: u64,
    created_seq: u64,
    last_access_seq: u64,
    entries: Vec<DurableBucketEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AcceptedEvictionRecord {
    storage_key: String,
    generation_id: String,
}

impl DurableBucket {
    fn logical_size(&self) -> StoreResult<u64> {
        self.entries.iter().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.logical_size()?)
                .ok_or_else(|| StoreError::Internal("bucket size overflow".to_string()))
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BucketRecord {
    bucket_id: u64,
    value_size: u64,
    logical_size: u64,
    generation_id: Uuid,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BucketScanPage {
    pub(crate) records: Vec<LocalStorageRecordMetadata>,
    pub(crate) bucket_count: usize,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug)]
struct BucketMeta {
    logical_size: u64,
    key_count: usize,
    created_seq: u64,
    last_access_seq: u64,
}

#[derive(Debug, Default)]
struct BucketState {
    initialized: bool,
    storage_id: Option<Uuid>,
    capacity_bytes: u64,
    _namespace_lock: Option<std::fs::File>,
    next_bucket_id: u64,
    next_sequence: u64,
    total_logical_size: u64,
    buckets: BTreeMap<u64, BucketMeta>,
    records: HashMap<String, BucketRecord>,
    accepted_tombstones: HashMap<String, Uuid>,
}

#[derive(Debug, Default)]
struct BucketReservationRegistry {
    names: Mutex<HashMap<String, u64>>,
    write_growth: Mutex<HashMap<u64, u64>>,
}

impl BucketReservationRegistry {
    fn release(
        &self,
        token: u64,
        names: impl IntoIterator<Item = String>,
        release_write_growth: bool,
    ) {
        if token == 0 {
            return;
        }
        let mut reserved = self.names.lock();
        for name in names {
            if reserved.get(&name).copied() == Some(token) {
                reserved.remove(&name);
            }
        }
        if release_write_growth {
            self.write_growth.lock().remove(&token);
        }
    }
}

#[derive(Clone, Debug)]
struct BucketRecordSnapshot {
    storage_key: String,
    record: BucketRecord,
}

#[derive(Debug)]
struct BucketWriteReservation {
    storage_key: String,
    target_bucket_id: u64,
    previous: Option<BucketRecord>,
    required_value_size: u64,
}

#[derive(Debug)]
pub(crate) struct PendingBucketEviction {
    backend_id: Uuid,
    token: u64,
    records: Vec<BucketRecordSnapshot>,
    write_target: Option<BucketWriteReservation>,
    registry: Option<Arc<BucketReservationRegistry>>,
}

impl Default for PendingBucketEviction {
    fn default() -> Self {
        Self {
            backend_id: Uuid::nil(),
            token: 0,
            records: Vec::new(),
            write_target: None,
            registry: None,
        }
    }
}

impl PendingBucketEviction {
    pub(crate) fn keys(&self) -> Vec<String> {
        self.records
            .iter()
            .map(|snapshot| snapshot.storage_key.clone())
            .collect()
    }

    pub(crate) fn partition_accepted(mut self, accepted_keys: &HashSet<String>) -> (Self, Self) {
        let records = std::mem::take(&mut self.records);
        let (accepted, unaccepted) = records
            .into_iter()
            .partition(|snapshot| accepted_keys.contains(&snapshot.storage_key));
        let accepted_pending = Self {
            backend_id: self.backend_id,
            token: self.token,
            records: accepted,
            write_target: None,
            registry: self.registry.clone(),
        };
        self.records = unaccepted;
        (accepted_pending, self)
    }

    fn reservation_names(&self) -> Vec<String> {
        let mut names = self
            .records
            .iter()
            .map(|snapshot| snapshot.storage_key.clone())
            .collect::<Vec<_>>();
        if let Some(target) = &self.write_target {
            names.push(target.storage_key.clone());
            names.push(bucket_reservation_name(target.target_bucket_id));
        }
        names
    }
}

impl Drop for PendingBucketEviction {
    fn drop(&mut self) {
        if let Some(registry) = &self.registry {
            registry.release(
                self.token,
                self.reservation_names(),
                self.write_target.is_some(),
            );
        }
    }
}

/// Persistent, generation-aware client Bucket backend.
///
/// The C++ backend packs multiple keys into bounded bucket files and evicts at
/// bucket granularity. Rust keeps the same placement and selection semantics,
/// while its accepted-set commit may rewrite a partially accepted victim
/// bucket so Master-authoritative per-key eviction remains correct.
pub struct BucketStorageBackend {
    backend_id: Uuid,
    config: BucketStorageConfig,
    state: Mutex<BucketState>,
    ungrouped_offloading_objects: Mutex<HashMap<String, u64>>,
    init_lock: Mutex<()>,
    reservations: Arc<BucketReservationRegistry>,
    next_token: AtomicU64,
    available_space_probe: Box<dyn Fn(&Path) -> std::io::Result<u64> + Send + Sync>,
    remove_bucket_file: Box<dyn Fn(&Path) -> std::io::Result<()> + Send + Sync>,
}

impl BucketStorageBackend {
    pub fn new(config: BucketStorageConfig) -> Self {
        Self::new_with_probe(config, |path| fs2::available_space(path))
    }

    fn new_with_probe(
        config: BucketStorageConfig,
        available_space_probe: impl Fn(&Path) -> std::io::Result<u64> + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_probes(config, available_space_probe, |path| {
            std::fs::remove_file(path)
        })
    }

    fn new_with_probes(
        config: BucketStorageConfig,
        available_space_probe: impl Fn(&Path) -> std::io::Result<u64> + Send + Sync + 'static,
        remove_bucket_file: impl Fn(&Path) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            backend_id: Uuid::new_v4(),
            config,
            state: Mutex::new(BucketState::default()),
            ungrouped_offloading_objects: Mutex::new(HashMap::new()),
            init_lock: Mutex::new(()),
            reservations: Arc::new(BucketReservationRegistry::default()),
            next_token: AtomicU64::new(1),
            available_space_probe: Box::new(available_space_probe),
            remove_bucket_file: Box::new(remove_bucket_file),
        }
    }

    #[cfg(test)]
    fn new_with_available_space_probe(
        config: BucketStorageConfig,
        available_space_probe: impl Fn(&Path) -> std::io::Result<u64> + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_probe(config, available_space_probe)
    }

    #[cfg(test)]
    pub(crate) fn new_with_bucket_remove(
        config: BucketStorageConfig,
        remove_bucket_file: impl Fn(&Path) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_probes(
            config,
            |path| fs2::available_space(path),
            remove_bucket_file,
        )
    }

    pub(crate) fn is_enable_offloading(
        &self,
        total_keys_limit: usize,
        total_size_limit: u64,
    ) -> bool {
        let state = self.state.lock();
        if self.config.eviction_policy != BucketEvictionPolicy::None && self.config.quota_bytes > 0
        {
            return true;
        }

        state
            .records
            .len()
            .checked_add(self.config.bucket_keys_limit)
            .is_some_and(|projected| projected <= total_keys_limit)
            && state
                .total_logical_size
                .checked_add(self.config.bucket_size_limit)
                .is_some_and(|projected| projected <= total_size_limit)
    }

    pub(crate) fn allocate_offloading_buckets(
        &self,
        offloading_objects: &HashMap<String, u64>,
    ) -> StoreResult<Vec<Vec<String>>> {
        self.ensure_initialized()?;
        if offloading_objects.is_empty() {
            return Ok(Vec::new());
        }

        let mut ungrouped = self.ungrouped_offloading_objects.lock();
        let existing = self
            .state
            .lock()
            .records
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let objects = offloading_objects.iter().collect::<Vec<_>>();
        let mut next = 0usize;
        let mut buckets = Vec::new();

        while next < objects.len() {
            let mut bucket_keys = Vec::new();
            let mut bucket_objects = HashMap::new();
            let mut bucket_data_size = 0u64;

            for (key, size) in ungrouped.drain() {
                bucket_data_size = bucket_data_size.checked_add(size).ok_or_else(|| {
                    StoreError::Internal("offload bucket data size overflow".to_string())
                })?;
                bucket_keys.push(key.clone());
                bucket_objects.insert(key, size);
            }

            let mut candidate_slots = bucket_keys.len();
            while candidate_slots < self.config.bucket_keys_limit {
                let Some((key, size)) = objects.get(next).copied() else {
                    ungrouped.extend(bucket_objects);
                    return Ok(buckets);
                };
                if *size > self.config.bucket_size_limit || existing.contains(key) {
                    next += 1;
                    candidate_slots += 1;
                    continue;
                }
                let projected = bucket_data_size.checked_add(*size).ok_or_else(|| {
                    StoreError::Internal("offload bucket data size overflow".to_string())
                })?;
                if projected > self.config.bucket_size_limit {
                    break;
                }

                bucket_data_size = projected;
                bucket_keys.push(key.clone());
                bucket_objects.insert(key.clone(), *size);
                next += 1;
                candidate_slots += 1;
                if bucket_data_size == self.config.bucket_size_limit {
                    break;
                }
            }

            buckets.push(bucket_keys);
        }

        Ok(buckets)
    }

    #[cfg(test)]
    fn ungrouped_offloading_objects_size(&self) -> usize {
        self.ungrouped_offloading_objects.lock().len()
    }

    fn backend_dir(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.fsdir)
    }

    fn bucket_dir(&self) -> PathBuf {
        self.backend_dir().join(BUCKET_DIRECTORY)
    }

    fn bucket_path(&self, bucket_id: u64) -> PathBuf {
        self.bucket_dir().join(format!("{bucket_id}.bucket.json"))
    }

    fn temp_dir(&self) -> PathBuf {
        self.backend_dir().join(TEMP_DIRECTORY)
    }

    fn accepted_eviction_journal_path(&self) -> PathBuf {
        self.backend_dir().join(ACCEPTED_EVICTION_JOURNAL)
    }

    fn ensure_initialized(&self) -> StoreResult<()> {
        if self.state.lock().initialized {
            return Ok(());
        }
        let _guard = self.init_lock.lock();
        if self.state.lock().initialized {
            return Ok(());
        }
        self.config.validate().map_err(StoreError::InvalidParams)?;
        std::fs::create_dir_all(self.bucket_dir())?;
        std::fs::create_dir_all(self.temp_dir())?;
        let namespace_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.backend_dir().join(LOCK_FILE))?;
        namespace_lock.try_lock_exclusive().map_err(|error| {
            StoreError::InvalidParams(format!(
                "bucket namespace is already owned by another process: {error}"
            ))
        })?;
        self.load_or_create_marker()?;
        let storage_id = self.load_or_create_storage_id()?;
        let recovery_tombstones = self.read_accepted_eviction_journal()?.unwrap_or_default();
        if recovery_tombstones.len() > self.config.total_keys_limit {
            return Err(StoreError::InvalidParams(
                "accepted eviction journal exceeds configured key limit".to_string(),
            ));
        }
        let mut recovery_tombstone_map = HashMap::with_capacity(recovery_tombstones.len());
        for record in &recovery_tombstones {
            if record.storage_key.is_empty()
                || record.storage_key.len() as u64 > self.config.bucket_size_limit
            {
                return Err(StoreError::InvalidParams(
                    "accepted eviction journal contains an invalid key".to_string(),
                ));
            }
            let generation_id = Uuid::parse_str(&record.generation_id).map_err(|error| {
                StoreError::InvalidParams(format!(
                    "invalid accepted eviction generation for {:?}: {error}",
                    record.storage_key
                ))
            })?;
            if generation_id.is_nil()
                || recovery_tombstone_map
                    .insert(record.storage_key.clone(), generation_id)
                    .is_some()
            {
                return Err(StoreError::InvalidParams(
                    "accepted eviction journal contains an invalid or duplicate key".to_string(),
                ));
            }
        }
        let mut recovered_tombstone_count = 0usize;
        let mut recovered_tombstone_size = 0u64;

        let mut recovered = BucketState {
            initialized: false,
            storage_id: Some(storage_id),
            capacity_bytes: if self.config.quota_bytes == 0 {
                fs2::total_space(self.backend_dir())?.saturating_mul(9) / 10
            } else {
                self.config.quota_bytes
            },
            _namespace_lock: Some(namespace_lock),
            next_bucket_id: 1,
            next_sequence: 1,
            ..BucketState::default()
        };
        let mut paths = std::fs::read_dir(self.bucket_dir())?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        paths.sort();
        for path in paths {
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(StoreError::InvalidParams(format!(
                    "bucket namespace contains non-regular entry: {}",
                    path.display()
                )));
            }
            let bucket = self.read_bucket_path(&path)?;
            let expected = self.bucket_path(bucket.bucket_id);
            if path != expected {
                return Err(StoreError::InvalidParams(format!(
                    "bucket id/path mismatch: id={}, path={}",
                    bucket.bucket_id,
                    path.display()
                )));
            }
            self.index_recovered_bucket(&mut recovered, &bucket)?;
            for entry in &bucket.entries {
                let generation_id = entry.generation()?;
                if recovery_tombstone_map.get(&entry.storage_key) == Some(&generation_id) {
                    recovered_tombstone_count += 1;
                    recovered_tombstone_size = recovered_tombstone_size
                        .checked_add(entry.logical_size()?)
                        .ok_or_else(|| {
                            StoreError::Internal(
                                "accepted eviction recovery size overflow".to_string(),
                            )
                        })?;
                }
            }
            if recovered
                .records
                .len()
                .saturating_sub(recovered_tombstone_count)
                > self.config.total_keys_limit
            {
                return Err(StoreError::InvalidParams(
                    "recovered bucket key count exceeds configured limit".to_string(),
                ));
            }
            if recovered.capacity_bytes > 0
                && recovered
                    .total_logical_size
                    .saturating_sub(recovered_tombstone_size)
                    > recovered.capacity_bytes
            {
                return Err(StoreError::InvalidParams(
                    "recovered bucket data exceeds configured quota".to_string(),
                ));
            }
        }
        *self.state.lock() = recovered;
        self.finish_accepted_eviction_journal(recovery_tombstones)?;
        let mut state = self.state.lock();
        if state.records.len() > self.config.total_keys_limit {
            return Err(StoreError::InvalidParams(
                "recovered bucket key count exceeds configured limit".to_string(),
            ));
        }
        if state.capacity_bytes > 0 && state.total_logical_size > state.capacity_bytes {
            return Err(StoreError::InvalidParams(
                "recovered bucket data exceeds configured quota".to_string(),
            ));
        }
        state.initialized = true;
        Ok(())
    }

    fn finish_accepted_eviction_journal(
        &self,
        records: Vec<AcceptedEvictionRecord>,
    ) -> StoreResult<()> {
        let path = self.accepted_eviction_journal_path();
        if records.is_empty() && !path.exists() {
            return Ok(());
        }
        let mut state = self.state.lock();
        let mut snapshots = Vec::new();
        for record in records {
            let generation_id = Uuid::parse_str(&record.generation_id).map_err(|error| {
                StoreError::InvalidParams(format!(
                    "invalid accepted eviction generation for {:?}: {error}",
                    record.storage_key
                ))
            })?;
            match state.records.get(&record.storage_key).cloned() {
                Some(current) if current.generation_id == generation_id => {
                    state
                        .accepted_tombstones
                        .insert(record.storage_key.clone(), generation_id);
                    snapshots.push(BucketRecordSnapshot {
                        storage_key: record.storage_key,
                        record: current,
                    });
                }
                Some(current) => {
                    return Err(StoreError::InvalidParams(format!(
                        "accepted eviction journal generation mismatch for {:?}: journal={}, current={}",
                        record.storage_key, generation_id, current.generation_id
                    )));
                }
                None => {}
            }
        }
        self.apply_record_snapshots_locked(&mut state, &snapshots, false)?;
        std::fs::remove_file(&path)?;
        sync_directory(&self.backend_dir())?;
        state.accepted_tombstones.clear();
        Ok(())
    }

    fn read_accepted_eviction_journal(&self) -> StoreResult<Option<Vec<AcceptedEvictionRecord>>> {
        let path = self.accepted_eviction_journal_path();
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.file_type().is_file() {
            return Err(StoreError::InvalidParams(format!(
                "accepted eviction journal is not a regular file: {}",
                path.display()
            )));
        }
        let mut file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let per_record_limit = self
            .config
            .bucket_size_limit
            .checked_mul(6)
            .and_then(|limit| limit.checked_add(128))
            .ok_or_else(|| StoreError::Internal("journal size limit overflow".to_string()))?;
        let byte_limit = per_record_limit
            .checked_mul(u64::try_from(self.config.total_keys_limit).map_err(|_| {
                StoreError::Internal("journal key limit conversion overflow".to_string())
            })?)
            .and_then(|limit| limit.checked_add(2))
            .ok_or_else(|| StoreError::Internal("journal size limit overflow".to_string()))?;
        if metadata.len() > byte_limit {
            return Err(StoreError::InvalidParams(format!(
                "accepted eviction journal exceeds byte limit {byte_limit}"
            )));
        }
        let capacity = usize::try_from(metadata.len()).map_err(|_| {
            StoreError::InvalidParams("accepted eviction journal is too large".to_string())
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut limited = std::io::Read::take(&mut file, byte_limit.saturating_add(1));
        std::io::Read::read_to_end(&mut limited, &mut bytes)?;
        if bytes.len() as u64 > byte_limit {
            return Err(StoreError::InvalidParams(format!(
                "accepted eviction journal exceeds byte limit {byte_limit}"
            )));
        }
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            StoreError::InvalidParams(format!(
                "invalid accepted eviction journal {}: {error}",
                path.display()
            ))
        })
    }

    fn load_or_create_marker(&self) -> StoreResult<()> {
        let path = self.backend_dir().join(FORMAT_MARKER_FILE);
        match std::fs::read_to_string(&path) {
            Ok(marker) if marker == BUCKET_FORMAT_MARKER => Ok(()),
            Ok(_) => Err(StoreError::InvalidParams(format!(
                "unsupported bucket storage format marker: {}",
                path.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                write_new_synced_file(&path, BUCKET_FORMAT_MARKER.as_bytes())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn load_or_create_storage_id(&self) -> StoreResult<Uuid> {
        let path = self.backend_dir().join(STORAGE_ID_FILE);
        match std::fs::read_to_string(&path) {
            Ok(value) => {
                let id = Uuid::parse_str(value.trim()).map_err(|error| {
                    StoreError::InvalidParams(format!(
                        "invalid bucket storage id at {}: {error}",
                        path.display()
                    ))
                })?;
                if id.is_nil() {
                    return Err(StoreError::InvalidParams(
                        "bucket storage id must not be nil".to_string(),
                    ));
                }
                Ok(id)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let id = Uuid::new_v4();
                write_new_synced_file(&path, id.to_string().as_bytes())?;
                Ok(id)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn index_recovered_bucket(
        &self,
        state: &mut BucketState,
        bucket: &DurableBucket,
    ) -> StoreResult<()> {
        if bucket.version != BUCKET_FORMAT_VERSION || bucket.bucket_id == 0 {
            return Err(StoreError::InvalidParams(format!(
                "unsupported bucket header: version={}, id={}",
                bucket.version, bucket.bucket_id
            )));
        }
        if bucket.entries.is_empty() || bucket.entries.len() > self.config.bucket_keys_limit {
            return Err(StoreError::InvalidParams(format!(
                "bucket {} has invalid key count {}",
                bucket.bucket_id,
                bucket.entries.len()
            )));
        }
        let logical_size = bucket.logical_size()?;
        if logical_size > self.config.bucket_size_limit {
            return Err(StoreError::InvalidParams(format!(
                "bucket {} exceeds configured size limit",
                bucket.bucket_id
            )));
        }
        for entry in &bucket.entries {
            if entry.storage_key.is_empty() {
                return Err(StoreError::InvalidParams(format!(
                    "bucket {} contains an empty key",
                    bucket.bucket_id
                )));
            }
            let generation_id = entry.generation()?;
            let record = BucketRecord {
                bucket_id: bucket.bucket_id,
                value_size: entry.value.len() as u64,
                logical_size: entry.logical_size()?,
                generation_id,
            };
            if state
                .records
                .insert(entry.storage_key.clone(), record)
                .is_some()
            {
                return Err(StoreError::InvalidParams(format!(
                    "duplicate bucket key {:?}",
                    entry.storage_key
                )));
            }
        }
        state.total_logical_size = state
            .total_logical_size
            .checked_add(logical_size)
            .ok_or_else(|| StoreError::Internal("bucket usage overflow".to_string()))?;
        state.next_bucket_id = state.next_bucket_id.max(bucket.bucket_id.saturating_add(1));
        state.next_sequence = state.next_sequence.max(
            bucket
                .created_seq
                .max(bucket.last_access_seq)
                .saturating_add(1),
        );
        state.buckets.insert(
            bucket.bucket_id,
            BucketMeta {
                logical_size,
                key_count: bucket.entries.len(),
                created_seq: bucket.created_seq,
                last_access_seq: bucket.last_access_seq,
            },
        );
        Ok(())
    }

    fn read_bucket(&self, bucket_id: u64) -> StoreResult<DurableBucket> {
        self.read_bucket_path(&self.bucket_path(bucket_id))
    }

    fn read_bucket_path(&self, path: &Path) -> StoreResult<DurableBucket> {
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            StoreError::InvalidParams(format!("invalid bucket file {}: {error}", path.display()))
        })
    }

    fn write_bucket(&self, bucket: &DurableBucket) -> StoreResult<()> {
        let bytes = serde_json::to_vec(bucket)
            .map_err(|error| StoreError::Internal(format!("encode bucket: {error}")))?;
        write_file_atomically(
            &self.bucket_path(bucket.bucket_id),
            &bytes,
            &self.temp_dir(),
        )
    }

    pub fn storage_id(&self) -> StoreResult<Uuid> {
        self.ensure_initialized()?;
        self.state
            .lock()
            .storage_id
            .ok_or_else(|| StoreError::Internal("bucket storage id missing".to_string()))
    }

    pub fn space_usage(&self) -> (u64, u64) {
        if let Err(error) = self.ensure_initialized() {
            tracing::error!(%error, "failed to initialize bucket storage for usage");
            return (0, self.config.quota_bytes);
        }
        let state = self.state.lock();
        (state.total_logical_size, state.capacity_bytes)
    }

    pub(crate) fn prepare_write(
        &self,
        storage_key: &str,
        required_value_size: u64,
    ) -> StoreResult<PendingBucketEviction> {
        self.ensure_initialized()?;
        if storage_key.is_empty() {
            return Err(StoreError::InvalidParams(
                "bucket storage key must not be empty".to_string(),
            ));
        }
        let required_logical_size = (storage_key.len() as u64)
            .checked_add(required_value_size)
            .ok_or_else(|| StoreError::Internal("bucket write size overflow".to_string()))?;
        if required_logical_size > self.config.bucket_size_limit {
            return Err(StoreError::InvalidParams(format!(
                "object {:?} exceeds bucket size limit {}",
                storage_key, self.config.bucket_size_limit
            )));
        }

        let token = self.next_token.fetch_add(1, Ordering::Relaxed).max(1);
        let mut state = self.state.lock();
        if !state.accepted_tombstones.is_empty() {
            return Err(StoreError::Internal(
                "bucket backend has an unfinished accepted eviction journal".to_string(),
            ));
        }
        let mut reserved = self.reservations.names.lock();
        if reserved.contains_key(storage_key) {
            return Err(StoreError::ObjectExists(storage_key.to_string()));
        }
        let previous = state.records.get(storage_key).cloned();
        let target_bucket_id = if let Some(previous) = &previous {
            previous.bucket_id
        } else {
            let existing = state
                .buckets
                .iter()
                .rev()
                .find(|(bucket_id, meta)| {
                    !reserved.contains_key(&bucket_reservation_name(**bucket_id))
                        && meta.key_count < self.config.bucket_keys_limit
                        && meta
                            .logical_size
                            .checked_add(required_logical_size)
                            .is_some_and(|size| size <= self.config.bucket_size_limit)
                })
                .map(|(bucket_id, _)| *bucket_id);
            if let Some(bucket_id) = existing {
                bucket_id
            } else {
                let bucket_id = state.next_bucket_id;
                state.next_bucket_id = state.next_bucket_id.saturating_add(1);
                bucket_id
            }
        };
        if let Some(previous) = &previous {
            let target_bucket = state.buckets.get(&target_bucket_id).ok_or_else(|| {
                StoreError::Internal(format!(
                    "bucket index for {storage_key:?} points to missing bucket {target_bucket_id}"
                ))
            })?;
            let projected_bucket_size = target_bucket
                .logical_size
                .checked_sub(previous.logical_size)
                .and_then(|size| size.checked_add(required_logical_size))
                .ok_or_else(|| {
                    StoreError::Internal(format!(
                        "bucket replacement size invariant failed for {storage_key:?}"
                    ))
                })?;
            if projected_bucket_size > self.config.bucket_size_limit {
                return Err(StoreError::InvalidParams(format!(
                    "replacement for {storage_key:?} would exceed bucket size limit; \
                     remove the stale LocalDisk record before retrying"
                )));
            }
        }
        let replaced_size = previous.as_ref().map_or(0, |record| record.logical_size);
        let projected_keys = state.records.len() + usize::from(previous.is_none());
        if projected_keys > self.config.total_keys_limit {
            return Err(StoreError::InvalidParams(
                "bucket total key limit exceeded".to_string(),
            ));
        }
        let projected_usage = state
            .total_logical_size
            .saturating_sub(replaced_size)
            .checked_add(required_logical_size)
            .and_then(|usage| {
                self.reservations
                    .write_growth
                    .lock()
                    .values()
                    .try_fold(usage, |total, growth| total.checked_add(*growth))
            })
            .ok_or_else(|| StoreError::Internal("bucket projected usage overflow".to_string()))?;
        let backend_dir = self.backend_dir();
        let available = (self.available_space_probe)(&backend_dir)?;
        let disk_deficit = required_logical_size
            .saturating_add(MIN_FREE_SPACE_BYTES)
            .saturating_sub(available);
        let quota_exceeded = state.capacity_bytes > 0 && projected_usage > state.capacity_bytes;

        let mut victim_buckets = Vec::new();
        let mut reclaimed = 0u64;
        if quota_exceeded || disk_deficit > 0 {
            if self.config.eviction_policy == BucketEvictionPolicy::None {
                return Err(StoreError::InvalidParams(
                    "bucket capacity/disk watermark exceeded and eviction is disabled".to_string(),
                ));
            }
            let mut candidates = state
                .buckets
                .iter()
                .filter(|(bucket_id, _)| {
                    **bucket_id != target_bucket_id
                        && !state.records.iter().any(|(key, record)| {
                            record.bucket_id == **bucket_id && reserved.contains_key(key)
                        })
                })
                .map(|(bucket_id, meta)| (*bucket_id, meta.clone()))
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(bucket_id, meta)| match self.config.eviction_policy {
                BucketEvictionPolicy::Fifo => (meta.created_seq, *bucket_id),
                BucketEvictionPolicy::Lru => (meta.last_access_seq, *bucket_id),
                BucketEvictionPolicy::None => (u64::MAX, *bucket_id),
            });
            for (bucket_id, meta) in candidates {
                victim_buckets.push(bucket_id);
                reclaimed = reclaimed.saturating_add(meta.logical_size);
                let quota_satisfied = state.capacity_bytes == 0
                    || projected_usage.saturating_sub(reclaimed) <= state.capacity_bytes;
                if quota_satisfied && reclaimed >= disk_deficit {
                    break;
                }
            }
            let quota_unsatisfied = state.capacity_bytes > 0
                && projected_usage.saturating_sub(reclaimed) > state.capacity_bytes;
            if quota_unsatisfied || reclaimed < disk_deficit {
                return Err(StoreError::InvalidParams(
                    "bucket capacity/disk watermark cannot be satisfied by available victims"
                        .to_string(),
                ));
            }
        }

        let mut records = state
            .records
            .iter()
            .filter(|(_, record)| victim_buckets.contains(&record.bucket_id))
            .map(|(key, record)| BucketRecordSnapshot {
                storage_key: key.clone(),
                record: record.clone(),
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.storage_key.cmp(&right.storage_key));
        for snapshot in &records {
            reserved.insert(snapshot.storage_key.clone(), token);
        }
        reserved.insert(storage_key.to_string(), token);
        reserved.insert(bucket_reservation_name(target_bucket_id), token);
        self.reservations
            .write_growth
            .lock()
            .insert(token, required_logical_size.saturating_sub(replaced_size));
        drop(reserved);
        drop(state);

        Ok(PendingBucketEviction {
            backend_id: self.backend_id,
            token,
            records,
            write_target: Some(BucketWriteReservation {
                storage_key: storage_key.to_string(),
                target_bucket_id,
                previous,
                required_value_size,
            }),
            registry: Some(Arc::clone(&self.reservations)),
        })
    }

    pub(crate) fn commit_write_with_generation(
        &self,
        storage_key: &str,
        data: &[u8],
        pending: PendingBucketEviction,
        generation_id: Uuid,
    ) -> StoreResult<()> {
        self.ensure_initialized()?;
        if generation_id.is_nil() {
            return Err(StoreError::InvalidParams(
                "bucket generation must not be nil".to_string(),
            ));
        }
        if pending.backend_id != self.backend_id {
            return Err(StoreError::Internal(
                "bucket pending eviction belongs to another backend".to_string(),
            ));
        }
        let target = pending.write_target.as_ref().ok_or_else(|| {
            StoreError::Internal("bucket write is missing target reservation".to_string())
        })?;
        if target.storage_key != storage_key || target.required_value_size != data.len() as u64 {
            return Err(StoreError::Internal(
                "bucket write target changed after reservation".to_string(),
            ));
        }
        self.commit_record_snapshots(&pending.records)?;

        let mut state = self.state.lock();
        if state.records.get(storage_key) != target.previous.as_ref() {
            return Err(StoreError::Internal(format!(
                "bucket write target {storage_key:?} changed after reservation"
            )));
        }
        let mut bucket = if state.buckets.contains_key(&target.target_bucket_id) {
            self.read_bucket(target.target_bucket_id)?
        } else {
            let seq = state.next_sequence;
            state.next_sequence = state.next_sequence.saturating_add(1);
            DurableBucket {
                version: BUCKET_FORMAT_VERSION,
                bucket_id: target.target_bucket_id,
                created_seq: seq,
                last_access_seq: seq,
                entries: Vec::new(),
            }
        };
        bucket
            .entries
            .retain(|entry| entry.storage_key != storage_key);
        bucket.entries.push(DurableBucketEntry {
            storage_key: storage_key.to_string(),
            value: data.to_vec(),
            generation_id: generation_id.to_string(),
        });
        bucket
            .entries
            .sort_by(|left, right| left.storage_key.cmp(&right.storage_key));
        bucket.last_access_seq = state.next_sequence;
        state.next_sequence = state.next_sequence.saturating_add(1);
        let bucket_size = bucket.logical_size()?;
        if bucket.entries.len() > self.config.bucket_keys_limit
            || bucket_size > self.config.bucket_size_limit
        {
            return Err(StoreError::Internal(
                "bucket limits changed after reservation".to_string(),
            ));
        }
        self.write_bucket(&bucket)?;

        let previous_size = target
            .previous
            .as_ref()
            .map_or(0, |record| record.logical_size);
        let logical_size = (storage_key.len() as u64)
            .checked_add(data.len() as u64)
            .ok_or_else(|| StoreError::Internal("bucket record size overflow".to_string()))?;
        state.total_logical_size = state
            .total_logical_size
            .saturating_sub(previous_size)
            .checked_add(logical_size)
            .ok_or_else(|| StoreError::Internal("bucket usage overflow".to_string()))?;
        state.records.insert(
            storage_key.to_string(),
            BucketRecord {
                bucket_id: bucket.bucket_id,
                value_size: data.len() as u64,
                logical_size,
                generation_id,
            },
        );
        state.buckets.insert(
            bucket.bucket_id,
            BucketMeta {
                logical_size: bucket_size,
                key_count: bucket.entries.len(),
                created_seq: bucket.created_seq,
                last_access_seq: bucket.last_access_seq,
            },
        );
        Ok(())
    }

    pub(crate) fn rollback_eviction(&self, _pending: PendingBucketEviction) {}

    pub(crate) fn prepare_watermark_eviction(
        &self,
        high: f64,
        low: f64,
    ) -> StoreResult<PendingBucketEviction> {
        self.ensure_initialized()?;
        if !(0.0..=1.0).contains(&low) || !(0.0..=1.0).contains(&high) || low > high {
            return Err(StoreError::InvalidParams(
                "bucket watermark ratios must satisfy 0 <= low <= high <= 1".to_string(),
            ));
        }
        let state = self.state.lock();
        if !state.accepted_tombstones.is_empty() {
            return Err(StoreError::Internal(
                "bucket backend has an unfinished accepted eviction journal".to_string(),
            ));
        }
        if state.capacity_bytes == 0 || self.config.eviction_policy == BucketEvictionPolicy::None {
            return Ok(PendingBucketEviction::default());
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed).max(1);
        if state.total_logical_size as f64 / state.capacity_bytes as f64 <= high {
            return Ok(PendingBucketEviction::default());
        }
        let target = (state.capacity_bytes as f64 * low) as u64;
        let reserved = self.reservations.names.lock();
        let mut candidates = state
            .buckets
            .iter()
            .filter(|(bucket_id, _)| {
                !state.records.iter().any(|(key, record)| {
                    record.bucket_id == **bucket_id && reserved.contains_key(key)
                })
            })
            .map(|(bucket_id, meta)| (*bucket_id, meta.clone()))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(bucket_id, meta)| match self.config.eviction_policy {
            BucketEvictionPolicy::Fifo => (meta.created_seq, *bucket_id),
            BucketEvictionPolicy::Lru => (meta.last_access_seq, *bucket_id),
            BucketEvictionPolicy::None => (u64::MAX, *bucket_id),
        });
        let mut selected = HashSet::new();
        let mut remaining = state.total_logical_size;
        for (bucket_id, meta) in candidates {
            if remaining <= target {
                break;
            }
            selected.insert(bucket_id);
            remaining = remaining.saturating_sub(meta.logical_size);
        }
        drop(reserved);
        let mut records = state
            .records
            .iter()
            .filter(|(_, record)| selected.contains(&record.bucket_id))
            .map(|(key, record)| BucketRecordSnapshot {
                storage_key: key.clone(),
                record: record.clone(),
            })
            .collect::<Vec<_>>();
        // C++ notifies victims beginning with the oldest bucket; preserve the
        // FIFO/LRU selection order instead of re-sorting by storage key.
        let created_seq = |bucket_id: u64| {
            state
                .buckets
                .get(&bucket_id)
                .map(|meta| meta.created_seq)
                .unwrap_or(u64::MAX)
        };
        records.sort_by(|left, right| {
            created_seq(left.record.bucket_id)
                .cmp(&created_seq(right.record.bucket_id))
                .then_with(|| left.storage_key.cmp(&right.storage_key))
        });
        drop(state);
        let mut reserved = self.reservations.names.lock();
        for snapshot in &records {
            reserved.insert(snapshot.storage_key.clone(), token);
        }
        drop(reserved);
        Ok(PendingBucketEviction {
            backend_id: self.backend_id,
            token,
            records,
            write_target: None,
            registry: Some(Arc::clone(&self.reservations)),
        })
    }

    pub(crate) fn commit_eviction(&self, pending: PendingBucketEviction) -> StoreResult<()> {
        if pending.backend_id.is_nil() && pending.records.is_empty() {
            return Ok(());
        }
        if pending.backend_id != self.backend_id {
            return Err(StoreError::Internal(
                "bucket pending eviction belongs to another backend".to_string(),
            ));
        }
        self.commit_record_snapshots(&pending.records)
    }

    fn commit_record_snapshots(&self, snapshots: &[BucketRecordSnapshot]) -> StoreResult<()> {
        if snapshots.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock();
        if !state.accepted_tombstones.is_empty() {
            return Err(StoreError::Internal(
                "bucket backend has an unfinished accepted eviction journal".to_string(),
            ));
        }
        for snapshot in snapshots {
            if state.records.get(&snapshot.storage_key) != Some(&snapshot.record) {
                return Err(StoreError::Internal(format!(
                    "bucket victim {:?} changed after reservation",
                    snapshot.storage_key
                )));
            }
        }
        let journal = snapshots
            .iter()
            .map(|snapshot| AcceptedEvictionRecord {
                storage_key: snapshot.storage_key.clone(),
                generation_id: snapshot.record.generation_id.to_string(),
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&journal).map_err(|error| {
            StoreError::Internal(format!("encode accepted eviction journal: {error}"))
        })?;
        write_file_atomically(
            &self.accepted_eviction_journal_path(),
            &bytes,
            &self.temp_dir(),
        )?;
        state.accepted_tombstones = snapshots
            .iter()
            .map(|snapshot| (snapshot.storage_key.clone(), snapshot.record.generation_id))
            .collect();
        let finalize_deferred = self.apply_record_snapshots_locked(&mut state, snapshots, true)?;
        if !finalize_deferred {
            std::fs::remove_file(self.accepted_eviction_journal_path())?;
            sync_directory(&self.backend_dir())?;
            state.accepted_tombstones.clear();
        }
        Ok(())
    }

    fn apply_record_snapshots_locked(
        &self,
        state: &mut BucketState,
        snapshots: &[BucketRecordSnapshot],
        allow_deferred_unlink: bool,
    ) -> StoreResult<bool> {
        if snapshots.is_empty() {
            return Ok(false);
        }
        let mut finalize_deferred = false;
        let mut by_bucket: BTreeMap<u64, Vec<&BucketRecordSnapshot>> = BTreeMap::new();
        for snapshot in snapshots {
            by_bucket
                .entry(snapshot.record.bucket_id)
                .or_default()
                .push(snapshot);
        }
        for (bucket_id, snapshots) in by_bucket {
            for snapshot in &snapshots {
                if state.records.get(&snapshot.storage_key) != Some(&snapshot.record) {
                    return Err(StoreError::Internal(format!(
                        "bucket victim {:?} changed after reservation",
                        snapshot.storage_key
                    )));
                }
            }
            let mut bucket = self.read_bucket(bucket_id)?;
            let accepted = snapshots
                .iter()
                .map(|snapshot| snapshot.storage_key.as_str())
                .collect::<HashSet<_>>();
            bucket
                .entries
                .retain(|entry| !accepted.contains(entry.storage_key.as_str()));
            if bucket.entries.is_empty() {
                match (self.remove_bucket_file)(&self.bucket_path(bucket_id)) {
                    Ok(()) => sync_directory(&self.bucket_dir())?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) if allow_deferred_unlink => {
                        tracing::warn!(
                            bucket_id,
                            %error,
                            "deferring accepted bucket eviction finalization to restart"
                        );
                        finalize_deferred = true;
                    }
                    Err(error) => return Err(error.into()),
                }
                state.buckets.remove(&bucket_id);
            } else {
                bucket.last_access_seq = state.next_sequence;
                state.next_sequence = state.next_sequence.saturating_add(1);
                self.write_bucket(&bucket)?;
                state.buckets.insert(
                    bucket_id,
                    BucketMeta {
                        logical_size: bucket.logical_size()?,
                        key_count: bucket.entries.len(),
                        created_seq: bucket.created_seq,
                        last_access_seq: bucket.last_access_seq,
                    },
                );
            }
            for snapshot in snapshots {
                state.total_logical_size = state
                    .total_logical_size
                    .saturating_sub(snapshot.record.logical_size);
                state.records.remove(&snapshot.storage_key);
            }
        }
        Ok(finalize_deferred)
    }

    pub(crate) fn read_object(&self, storage_key: &str) -> StoreResult<Vec<u8>> {
        self.ensure_initialized()?;
        let mut state = self.state.lock();
        if state.accepted_tombstones.contains_key(storage_key) {
            return Err(StoreError::KeyNotFound(storage_key.to_string()));
        }
        let record = state
            .records
            .get(storage_key)
            .cloned()
            .ok_or_else(|| StoreError::KeyNotFound(storage_key.to_string()))?;
        let mut bucket = self.read_bucket(record.bucket_id)?;
        let entry = bucket
            .entries
            .iter()
            .find(|entry| entry.storage_key == storage_key)
            .ok_or_else(|| {
                StoreError::Internal(format!(
                    "bucket index points to missing key {storage_key:?}"
                ))
            })?;
        if entry.generation()? != record.generation_id
            || entry.value.len() as u64 != record.value_size
        {
            return Err(StoreError::Internal(format!(
                "bucket record changed outside backend for {storage_key:?}"
            )));
        }
        let value = entry.value.clone();
        bucket.last_access_seq = state.next_sequence;
        state.next_sequence = state.next_sequence.saturating_add(1);
        if self.config.eviction_policy == BucketEvictionPolicy::Lru {
            self.write_bucket(&bucket)?;
        }
        if let Some(meta) = state.buckets.get_mut(&record.bucket_id) {
            meta.last_access_seq = bucket.last_access_seq;
        }
        Ok(value)
    }

    pub(crate) fn delete_object(&self, storage_key: &str) -> StoreResult<()> {
        self.ensure_initialized()?;
        let record = self.state.lock().records.get(storage_key).cloned();
        match record {
            Some(record) => self.commit_record_snapshots(&[BucketRecordSnapshot {
                storage_key: storage_key.to_string(),
                record,
            }]),
            None => Ok(()),
        }
    }

    pub(crate) fn delete_object_if_generation(
        &self,
        storage_key: &str,
        generation_id: Uuid,
    ) -> StoreResult<bool> {
        self.ensure_initialized()?;
        let record = self.state.lock().records.get(storage_key).cloned();
        let Some(record) = record else {
            return Ok(false);
        };
        if record.generation_id != generation_id {
            return Ok(false);
        }
        self.commit_record_snapshots(&[BucketRecordSnapshot {
            storage_key: storage_key.to_string(),
            record,
        }])?;
        Ok(true)
    }

    pub(crate) fn remove_all(&self) -> StoreResult<usize> {
        self.ensure_initialized()?;
        let records = self
            .state
            .lock()
            .records
            .iter()
            .map(|(key, record)| BucketRecordSnapshot {
                storage_key: key.clone(),
                record: record.clone(),
            })
            .collect::<Vec<_>>();
        let count = records.len();
        self.commit_record_snapshots(&records)?;
        Ok(count)
    }

    pub(crate) fn scan_meta(&self) -> StoreResult<Vec<(String, u64)>> {
        self.ensure_initialized()?;
        let state = self.state.lock();
        let mut records = state
            .records
            .iter()
            .filter(|(key, _)| !state.accepted_tombstones.contains_key(key.as_str()))
            .map(|(key, record)| (key.clone(), record.value_size))
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(records)
    }

    pub(crate) fn scan_records(&self) -> StoreResult<Vec<LocalStorageRecordMetadata>> {
        self.ensure_initialized()?;
        let state = self.state.lock();
        let mut records = state
            .records
            .iter()
            .filter(|(key, _)| !state.accepted_tombstones.contains_key(key.as_str()))
            .map(|(key, record)| LocalStorageRecordMetadata {
                storage_key: key.clone(),
                value_size: record.value_size,
                generation_id: record.generation_id,
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.storage_key.cmp(&right.storage_key));
        Ok(records)
    }

    pub(crate) fn scan_bucket_page(
        &self,
        cursor: Option<&str>,
        key_limit: usize,
    ) -> StoreResult<BucketScanPage> {
        self.ensure_initialized()?;
        if key_limit == 0 {
            return Err(StoreError::InvalidParams(
                "bucket scan key limit must be positive".to_string(),
            ));
        }
        let start_bucket_id = match cursor {
            None => 0,
            Some(cursor) => cursor
                .strip_prefix("v1:")
                .ok_or_else(|| StoreError::InvalidParams("invalid bucket scan cursor".to_string()))?
                .parse::<u64>()
                .map_err(|_| StoreError::InvalidParams("invalid bucket scan cursor".to_string()))?,
        };
        let state = self.state.lock();
        let mut records = Vec::new();
        let mut bucket_count = 0usize;
        let mut next_cursor = None;
        for (bucket_id, bucket) in state.buckets.range(start_bucket_id..) {
            let mut bucket_records = state
                .records
                .iter()
                .filter(|(key, record)| {
                    record.bucket_id == *bucket_id
                        && !state.accepted_tombstones.contains_key(key.as_str())
                })
                .map(|(key, record)| LocalStorageRecordMetadata {
                    storage_key: key.clone(),
                    value_size: record.value_size,
                    generation_id: record.generation_id,
                })
                .collect::<Vec<_>>();
            bucket_records.sort_by(|left, right| left.storage_key.cmp(&right.storage_key));
            if bucket_records.len() > key_limit {
                return Err(StoreError::InvalidParams(format!(
                    "bucket key count {} exceeds scan limit {key_limit}",
                    bucket_records.len()
                )));
            }
            if records.len().saturating_add(bucket_records.len()) > key_limit {
                next_cursor = Some(format!("v1:{bucket_id}"));
                break;
            }
            records.extend(bucket_records);
            bucket_count += usize::from(bucket.key_count > 0);
        }
        Ok(BucketScanPage {
            records,
            bucket_count,
            next_cursor,
        })
    }
}

fn bucket_reservation_name(bucket_id: u64) -> String {
    format!("@bucket:{bucket_id}")
}

fn write_new_synced_file(path: &Path, contents: &[u8]) -> StoreResult<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    sync_directory(
        path.parent()
            .ok_or_else(|| StoreError::InvalidParams("path has no parent".to_string()))?,
    )
}

fn write_file_atomically(path: &Path, contents: &[u8], temp_dir: &Path) -> StoreResult<()> {
    std::fs::create_dir_all(temp_dir)?;
    let temporary = temp_dir.join(Uuid::new_v4().to_string());
    let result = (|| -> StoreResult<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        sync_directory(
            path.parent()
                .ok_or_else(|| StoreError::InvalidParams("path has no parent".to_string()))?,
        )
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn sync_directory(path: &Path) -> StoreResult<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use tempfile::TempDir;

    fn config(root: &TempDir) -> BucketStorageConfig {
        BucketStorageConfig {
            root_dir: root.path().to_path_buf(),
            fsdir: "bucket-test".to_string(),
            bucket_size_limit: 1024,
            bucket_keys_limit: 4,
            eviction_policy: BucketEvictionPolicy::Fifo,
            quota_bytes: 4096,
            total_keys_limit: 100,
        }
    }

    fn write(backend: &BucketStorageBackend, key: &str, value: &[u8], generation: Uuid) {
        let pending = backend.prepare_write(key, value.len() as u64).unwrap();
        assert!(pending.keys().is_empty());
        backend
            .commit_write_with_generation(key, value, pending, generation)
            .unwrap();
    }

    fn one_byte_tasks(count: usize) -> HashMap<String, u64> {
        (0..count).map(|i| (format!("test{i}"), 1)).collect()
    }

    #[test]
    fn cpp_parity_storage_backend_test_cpp_storagebackendtest_bucketscan_2da9f8ef() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 16 * 1024;
        config.quota_bytes = 256 * 1024;
        let backend = BucketStorageBackend::new(config);
        let mut expected = BTreeMap::new();
        for index in 0..100 {
            let key = format!("test-key-{index:03}");
            let value = vec![b'A' + (index % 26) as u8; 20 + index];
            write(&backend, &key, &value, Uuid::new_v4());
            expected.insert(key, value.len() as u64);
        }

        let first = backend.scan_bucket_page(None, 10).unwrap();
        assert_eq!(first.records.len(), 10);
        assert_eq!(first.bucket_count, 1);
        let first_cursor = first.next_cursor.clone().unwrap();
        for record in &first.records {
            assert_eq!(record.value_size, expected[&record.storage_key]);
        }

        let four = backend.scan_bucket_page(None, 45).unwrap();
        assert_eq!(four.records.len(), 40);
        assert_eq!(four.bucket_count, 4);
        let fifth_cursor = four.next_cursor.clone().unwrap();
        let next_four = backend.scan_bucket_page(Some(&fifth_cursor), 45).unwrap();
        assert_eq!(next_four.records.len(), 40);
        assert_eq!(next_four.bucket_count, 4);
        assert!(four.records.iter().all(|left| {
            next_four
                .records
                .iter()
                .all(|right| left.storage_key != right.storage_key)
        }));
        let ninth_cursor = next_four.next_cursor.clone().unwrap();
        let final_two = backend.scan_bucket_page(Some(&ninth_cursor), 45).unwrap();
        assert_eq!(final_two.records.len(), 20);
        assert_eq!(final_two.bucket_count, 2);
        assert!(final_two.next_cursor.is_none());

        let mut cursor = Some(first_cursor);
        let mut seen = first.records;
        while let Some(current) = cursor {
            let page = backend.scan_bucket_page(Some(&current), 10).unwrap();
            seen.extend(page.records);
            cursor = page.next_cursor;
        }
        let mut actual = BTreeMap::new();
        for record in seen {
            assert!(
                actual
                    .insert(record.storage_key, record.value_size)
                    .is_none(),
                "bucket scan returned a duplicate key"
            );
        }
        assert_eq!(actual, expected);

        let beyond = backend
            .scan_bucket_page(Some("v1:18446744073709551615"), 45)
            .unwrap();
        assert!(beyond.records.is_empty());
        assert_eq!(beyond.bucket_count, 0);
        assert!(beyond.next_cursor.is_none());
        assert!(matches!(
            backend.scan_bucket_page(None, 8),
            Err(StoreError::InvalidParams(message)) if message.contains("bucket key count")
        ));
    }

    #[test]
    fn cpp_parity_storage_backend_test_cpp_storagebackendtest_bucketpendingwriterejectsreentrantduplicate_cc440547()
     {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = 20 * 1024;
        let backend = BucketStorageBackend::new(config);
        let outer_value = vec![b'A'; 3 * 1024];
        let nested_value = vec![b'B'; 1024];

        let outer_pending = backend
            .prepare_write("shared_key", outer_value.len() as u64)
            .unwrap();
        assert!(matches!(
            backend.prepare_write("shared_key", nested_value.len() as u64),
            Err(StoreError::ObjectExists(key)) if key == "shared_key"
        ));

        backend
            .commit_write_with_generation("shared_key", &outer_value, outer_pending, Uuid::new_v4())
            .unwrap();
        assert_eq!(backend.read_object("shared_key").unwrap(), outer_value);
    }

    #[test]
    fn cpp_parity_file_storage_is_enable_offloading_preflights_full_bucket() {
        let default_root = TempDir::new().unwrap();
        let mut default_config = config(&default_root);
        default_config.quota_bytes = 0;
        let default_backend = BucketStorageBackend::new(default_config);
        for i in 0..100 {
            write(&default_backend, &format!("test-{i}"), b"x", Uuid::new_v4());
        }
        assert!(default_backend.is_enable_offloading(10_000_000, 2 * 1024 * 1024 * 1024 * 1024));

        let key_limited_root = TempDir::new().unwrap();
        let mut key_limited_config = config(&key_limited_root);
        key_limited_config.quota_bytes = 0;
        key_limited_config.bucket_keys_limit = 10;
        let key_limited_backend = BucketStorageBackend::new(key_limited_config);
        key_limited_backend.storage_id().unwrap();
        assert!(!key_limited_backend.is_enable_offloading(9, 2 * 1024 * 1024 * 1024 * 1024));

        let size_limited_root = TempDir::new().unwrap();
        let mut size_limited_config = config(&size_limited_root);
        size_limited_config.quota_bytes = 0;
        size_limited_config.bucket_size_limit = 969;
        let size_limited_backend = BucketStorageBackend::new(size_limited_config);
        size_limited_backend.storage_id().unwrap();
        assert!(!size_limited_backend.is_enable_offloading(10_000_000, 100));

        let explicit_quota_root = TempDir::new().unwrap();
        let mut explicit_quota_config = config(&explicit_quota_root);
        explicit_quota_config.quota_bytes = 4096;
        let explicit_quota_backend = BucketStorageBackend::new(explicit_quota_config);
        assert!(explicit_quota_backend.is_enable_offloading(0, 0));
    }

    #[test]
    fn cpp_parity_file_storage_group_offloading_keys_by_bucket_key_limit() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 969;
        let backend = BucketStorageBackend::new(config);
        let tasks = one_byte_tasks(35);

        let first = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert_eq!(first.len(), 3);
        assert!(first.iter().all(|bucket| bucket.len() == 10));
        assert_eq!(backend.ungrouped_offloading_objects_size(), 5);

        let second = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert_eq!(second.len(), 4);
        assert!(second.iter().all(|bucket| bucket.len() == 10));
        assert_eq!(backend.ungrouped_offloading_objects_size(), 0);
    }

    #[test]
    fn cpp_parity_file_storage_group_offloading_keys_by_bucket_size_limit() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 969;
        config.bucket_size_limit = 10;
        let backend = BucketStorageBackend::new(config);
        let tasks = one_byte_tasks(35);

        let first = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert_eq!(first.len(), 3);
        assert!(first.iter().all(|bucket| bucket.len() == 10));
        assert_eq!(backend.ungrouped_offloading_objects_size(), 5);

        let second = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert_eq!(second.len(), 4);
        assert!(second.iter().all(|bucket| bucket.len() == 10));
        assert_eq!(backend.ungrouped_offloading_objects_size(), 0);
    }

    #[test]
    fn cpp_parity_file_storage_group_offloading_keys_by_bucket_combined_limits() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 9;
        config.bucket_size_limit = 496;
        let backend = BucketStorageBackend::new(config);
        let tasks = (0..500)
            .map(|i| (format!("test{i}"), i as u64))
            .collect::<HashMap<_, _>>();

        let buckets = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert!(!buckets.is_empty());
        for bucket in buckets {
            assert!(bucket.len() <= 9);
            assert!(bucket.iter().map(|key| tasks[key]).sum::<u64>() <= 496);
        }
    }

    #[test]
    fn cpp_parity_file_storage_group_offloading_keys_by_bucket_retains_residue() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 500;
        config.bucket_size_limit = 256 * 1024 * 1024;
        let backend = BucketStorageBackend::new(config);

        assert!(
            backend
                .allocate_offloading_buckets(&one_byte_tasks(1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(backend.ungrouped_offloading_objects_size(), 1);
        assert!(
            backend
                .allocate_offloading_buckets(&HashMap::new())
                .unwrap()
                .is_empty()
        );
        assert_eq!(backend.ungrouped_offloading_objects_size(), 1);
        assert!(
            backend
                .allocate_offloading_buckets(&one_byte_tasks(7))
                .unwrap()
                .is_empty()
        );
        assert_eq!(backend.ungrouped_offloading_objects_size(), 7);
    }

    #[test]
    fn cpp_parity_offload_bucket_grouping_oversized_skip_consumes_key_slot() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 2;
        config.bucket_size_limit = 64;
        let backend = BucketStorageBackend::new(config);
        let mut tasks = one_byte_tasks(3);
        *tasks.values_mut().next().unwrap() = 65;

        let buckets = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 1);
        assert_eq!(backend.ungrouped_offloading_objects_size(), 1);
    }

    #[test]
    fn cpp_parity_offload_bucket_grouping_existing_skip_consumes_key_slot() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 2;
        config.bucket_size_limit = 64;
        let backend = BucketStorageBackend::new(config);
        let tasks = one_byte_tasks(3);
        let first_key = tasks.keys().next().unwrap().clone();
        write(&backend, &first_key, b"x", Uuid::new_v4());

        let buckets = backend.allocate_offloading_buckets(&tasks).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 1);
        assert_eq!(backend.ungrouped_offloading_objects_size(), 1);
    }

    #[test]
    fn bucket_packs_keys_and_recovers_generation_inventory() {
        let root = TempDir::new().unwrap();
        let config = config(&root);
        let first_generation = Uuid::new_v4();
        let second_generation = Uuid::new_v4();
        {
            let backend = BucketStorageBackend::new(config.clone());
            write(&backend, "tenant-a", b"one", first_generation);
            write(&backend, "tenant-b", b"two", second_generation);
            assert_eq!(backend.read_object("tenant-a").unwrap(), b"one");
            assert_eq!(backend.space_usage(), (22, 4096));
            assert_eq!(std::fs::read_dir(backend.bucket_dir()).unwrap().count(), 1);
        }

        let recovered = BucketStorageBackend::new(config);
        assert_eq!(
            recovered.scan_records().unwrap(),
            vec![
                LocalStorageRecordMetadata {
                    storage_key: "tenant-a".to_string(),
                    value_size: 3,
                    generation_id: first_generation,
                },
                LocalStorageRecordMetadata {
                    storage_key: "tenant-b".to_string(),
                    value_size: 3,
                    generation_id: second_generation,
                },
            ]
        );
        assert!(
            !recovered
                .delete_object_if_generation("tenant-a", Uuid::new_v4())
                .unwrap()
        );
        assert!(
            recovered
                .delete_object_if_generation("tenant-a", first_generation)
                .unwrap()
        );
    }

    // BucketStorageBackend_ConcurrentReadsNoBlocking: four synchronized readers
    // each complete ten five-key loads spanning multiple buckets; all 40 loads
    // succeed and every byte is exact.
    #[test]
    fn cpp_parity_bucket_concurrent_reads_all_complete() {
        let root = TempDir::new().unwrap();
        let config = config(&root);
        let backend = Arc::new(BucketStorageBackend::new(config));
        let keys: Vec<String> = (0..12).map(|i| format!("key-{i}")).collect();
        for (i, key) in keys.iter().enumerate() {
            write(
                &backend,
                key,
                &vec![b'a' + (i % 26) as u8; 16],
                Uuid::new_v4(),
            );
        }

        let barrier = Arc::new(Barrier::new(4));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let backend = Arc::clone(&backend);
            let keys = keys.clone();
            let barrier = Arc::clone(&barrier);
            readers.push(std::thread::spawn(move || {
                barrier.wait();
                let mut loads = 0;
                for _ in 0..10 {
                    for (i, key) in keys.iter().enumerate() {
                        assert_eq!(
                            backend.read_object(key).unwrap(),
                            vec![b'a' + (i % 26) as u8; 16]
                        );
                    }
                    loads += 1;
                }
                loads
            }));
        }
        let mut total = 0;
        for reader in readers {
            total += reader.join().unwrap();
        }
        assert_eq!(total, 40);
    }

    // BucketWatermarkEvictionUsesHandlerAndKeepsNewest: three one-key buckets
    // watermark-evict the two oldest in FIFO order, and the newest remains
    // byte-exact after commit.
    #[test]
    fn cpp_parity_bucket_watermark_eviction_uses_handler_and_keeps_newest() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_size_limit = 64;
        config.bucket_keys_limit = 1;
        config.quota_bytes = 90;
        let backend = BucketStorageBackend::new(config);
        write(&backend, "oldest", b"0000000000000000", Uuid::new_v4());
        write(&backend, "middle", b"1111111111111111", Uuid::new_v4());
        write(&backend, "newest", b"2222222222222222", Uuid::new_v4());

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(
            pending.keys(),
            vec!["oldest".to_string(), "middle".to_string()]
        );
        backend.commit_eviction(pending).unwrap();

        assert!(matches!(
            backend.read_object("oldest"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert!(matches!(
            backend.read_object("middle"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert_eq!(backend.read_object("newest").unwrap(), b"2222222222222222");
    }

    #[test]
    fn cpp_parity_storage_backend_test_cpp_storagebackendtest_bucketwatermarkevictiondoesnotoverevictforshareddisk_8022116a()
     {
        const AVAILABLE_BYTES: u64 = 1024 * 1024 * 1024;
        const HIGH_WATERMARK_BYTES: u64 = 14 * 1024;
        const LOW_WATERMARK_BYTES: u64 = 12 * 1024;
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = AVAILABLE_BYTES * 2;
        let backend =
            BucketStorageBackend::new_with_available_space_probe(config, |_| Ok(AVAILABLE_BYTES));
        for index in 0..3 {
            write(
                &backend,
                &format!("shared_disk_bucket_key_{index}"),
                &vec![b'A' + index as u8; 6 * 1024],
                Uuid::new_v4(),
            );
        }

        let pending = backend
            .prepare_watermark_eviction(
                HIGH_WATERMARK_BYTES as f64 / (AVAILABLE_BYTES * 2) as f64,
                LOW_WATERMARK_BYTES as f64 / (AVAILABLE_BYTES * 2) as f64,
            )
            .unwrap();
        let victims = pending.keys();
        assert!(!victims.is_empty());
        assert!(victims.len() < 3);
        assert_eq!(
            victims,
            ["shared_disk_bucket_key_0", "shared_disk_bucket_key_1"]
        );
        backend.commit_eviction(pending).unwrap();

        assert!(matches!(
            backend.read_object("shared_disk_bucket_key_0"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert_eq!(
            backend.read_object("shared_disk_bucket_key_2").unwrap(),
            vec![b'C'; 6 * 1024]
        );
    }

    #[test]
    fn cpp_parity_storage_backend_test_cpp_storagebackendtest_bucketrollbackpreservescapacityagainstconcurrentwrite_3dc22122()
     {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = 10 * 1024;
        let backend = BucketStorageBackend::new_with_available_space_probe(config, |_| {
            Ok(1024 * 1024 * 1024)
        });
        let old_value = vec![b'A'; 6 * 1024];
        let incoming_value = vec![b'B'; 6 * 1024];
        let concurrent_value = vec![b'C'; 3 * 1024];
        write(&backend, "old_key", &old_value, Uuid::new_v4());

        let incoming_pending = backend
            .prepare_write("incoming_key", incoming_value.len() as u64)
            .unwrap();
        assert_eq!(incoming_pending.keys(), ["old_key"]);
        assert!(matches!(
            backend.prepare_write("concurrent_key", concurrent_value.len() as u64),
            Err(StoreError::InvalidParams(message))
                if message.contains("capacity") || message.contains("victim")
        ));

        backend.rollback_eviction(incoming_pending);
        assert_eq!(backend.read_object("old_key").unwrap(), old_value);
        assert!(matches!(
            backend.read_object("incoming_key"),
            Err(StoreError::KeyNotFound(_))
        ));

        let concurrent_pending = backend
            .prepare_write("concurrent_key", concurrent_value.len() as u64)
            .unwrap();
        assert!(concurrent_pending.keys().is_empty());
        backend
            .commit_write_with_generation(
                "concurrent_key",
                &concurrent_value,
                concurrent_pending,
                Uuid::new_v4(),
            )
            .unwrap();
        assert_eq!(
            backend.read_object("concurrent_key").unwrap(),
            concurrent_value
        );
    }

    #[test]
    fn cpp_parity_storage_backend_test_cpp_storagebackendtest_bucketpendingevictionrejectsconcurrentduplicatewrite_a77e9955()
     {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = 10 * 1024;
        let backend = BucketStorageBackend::new_with_available_space_probe(config, |_| {
            Ok(1024 * 1024 * 1024)
        });
        let old_value = vec![b'A'; 6 * 1024];
        let incoming_value = vec![b'B'; 6 * 1024];
        let replacement_value = vec![b'C'; 3 * 1024];
        write(&backend, "old_key", &old_value, Uuid::new_v4());

        let incoming_pending = backend
            .prepare_write("incoming_key", incoming_value.len() as u64)
            .unwrap();
        assert_eq!(incoming_pending.keys(), ["old_key"]);
        assert!(matches!(
            backend.prepare_write("old_key", replacement_value.len() as u64),
            Err(StoreError::ObjectExists(key)) if key == "old_key"
        ));

        backend.rollback_eviction(incoming_pending);
        assert_eq!(backend.read_object("old_key").unwrap(), old_value);
        assert!(matches!(
            backend.read_object("incoming_key"),
            Err(StoreError::KeyNotFound(key)) if key == "incoming_key"
        ));

        let replacement_pending = backend
            .prepare_write("old_key", replacement_value.len() as u64)
            .unwrap();
        assert!(replacement_pending.keys().is_empty());
        backend
            .commit_write_with_generation(
                "old_key",
                &replacement_value,
                replacement_pending,
                Uuid::new_v4(),
            )
            .unwrap();
        assert_eq!(backend.read_object("old_key").unwrap(), replacement_value);
    }

    #[test]
    fn victim_partition_does_not_release_write_target_growth() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 1;
        config.bucket_size_limit = 64;
        config.quota_bytes = 60;
        let backend = BucketStorageBackend::new_with_available_space_probe(config, |_| {
            Ok(1024 * 1024 * 1024)
        });
        write(&backend, "old", &[b'A'; 32], Uuid::new_v4());
        let pending = backend.prepare_write("target", 32).unwrap();
        assert_eq!(pending.keys(), ["old"]);
        let token = pending.token;
        let (accepted, target) = pending.partition_accepted(&HashSet::from(["old".to_string()]));

        drop(accepted);
        assert!(
            backend
                .reservations
                .write_growth
                .lock()
                .contains_key(&token)
        );
        drop(target);
        assert!(
            !backend
                .reservations
                .write_growth
                .lock()
                .contains_key(&token)
        );
    }

    // BucketWatermarkEvictionRestoresMetadataWhenNotificationFails: a failed
    // notification leaves the oldest key exact; retrying the same watermark
    // selection then removes it.
    #[test]
    fn cpp_parity_bucket_watermark_eviction_restores_metadata_when_notification_fails() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_size_limit = 64;
        config.bucket_keys_limit = 1;
        config.quota_bytes = 90;
        let backend = BucketStorageBackend::new(config);
        write(&backend, "oldest", b"0000000000000000", Uuid::new_v4());
        write(&backend, "middle", b"1111111111111111", Uuid::new_v4());
        write(&backend, "newest", b"2222222222222222", Uuid::new_v4());

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(
            pending.keys(),
            vec!["oldest".to_string(), "middle".to_string()]
        );
        backend.rollback_eviction(pending);

        assert_eq!(backend.read_object("oldest").unwrap(), b"0000000000000000");

        let retried = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(
            retried.keys(),
            vec!["oldest".to_string(), "middle".to_string()]
        );
        backend.commit_eviction(retried).unwrap();

        assert!(matches!(
            backend.read_object("oldest"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert_eq!(backend.read_object("newest").unwrap(), b"2222222222222222");
    }

    // MissingBucketDataFileCleanup: after externally removing the sole durable
    // bucket file, a fresh backend initializes successfully and reports the
    // former key absent from existence and inventory.
    #[test]
    fn cpp_parity_bucket_missing_data_file_cleanup() {
        let root = TempDir::new().unwrap();
        let config = config(&root);
        {
            let backend = BucketStorageBackend::new(config.clone());
            write(&backend, "missing-key", b"value", Uuid::new_v4());
            write(&backend, "other-key", b"other", Uuid::new_v4());
            assert!(backend.bucket_dir().exists());
            for entry in std::fs::read_dir(backend.bucket_dir()).unwrap() {
                let path = entry.unwrap().path();
                assert!(path.is_file());
                std::fs::remove_file(&path).unwrap();
            }
        }

        let restarted = BucketStorageBackend::new(config);
        assert!(matches!(
            restarted.read_object("missing-key"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert!(restarted.scan_records().unwrap().is_empty());
    }

    // BucketWatermarkEvictionNoopsWhenPolicyIsNone: under policy NONE with
    // usage above the high watermark, watermark eviction selects no victims,
    // commits without side effects, and the object remains exact.
    #[test]
    fn cpp_parity_bucket_watermark_eviction_noops_when_policy_none() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.eviction_policy = BucketEvictionPolicy::None;
        config.quota_bytes = 64;
        config.bucket_size_limit = 64;
        config.bucket_keys_limit = 2;
        let backend = BucketStorageBackend::new(config);
        write(&backend, "a", b"aaaaaaaa", Uuid::new_v4());
        write(&backend, "b", b"bbbbbbbb", Uuid::new_v4());

        let pending = backend
            .prepare_watermark_eviction(0.70, 0.40)
            .expect("watermark eviction under NONE must succeed");
        assert!(
            pending.keys().is_empty(),
            "policy NONE must never select watermark victims"
        );
        backend.commit_eviction(pending).unwrap();

        assert_eq!(backend.read_object("a").unwrap(), b"aaaaaaaa");
        assert_eq!(backend.read_object("b").unwrap(), b"bbbbbbbb");
    }

    #[test]
    fn bucket_replaces_stale_local_disk_generation_in_place() {
        let root = TempDir::new().unwrap();
        let backend = BucketStorageBackend::new(config(&root));
        let first_generation = Uuid::new_v4();
        let second_generation = Uuid::new_v4();
        write(&backend, "tenant-a", b"old", first_generation);

        let pending = backend.prepare_write("tenant-a", 5).unwrap();
        assert!(pending.keys().is_empty());
        backend
            .commit_write_with_generation("tenant-a", b"newer", pending, second_generation)
            .unwrap();

        assert_eq!(backend.read_object("tenant-a").unwrap(), b"newer");
        assert!(
            !backend
                .delete_object_if_generation("tenant-a", first_generation)
                .unwrap()
        );
        assert!(
            backend
                .delete_object_if_generation("tenant-a", second_generation)
                .unwrap()
        );
    }

    #[test]
    fn fifo_eviction_returns_entire_oldest_bucket_before_write() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_size_limit = 8;
        config.bucket_keys_limit = 1;
        config.quota_bytes = 8;
        let backend = BucketStorageBackend::new(config);
        write(&backend, "a", b"1111111", Uuid::new_v4());

        let pending = backend.prepare_write("b", 7).unwrap();
        assert_eq!(pending.keys(), vec!["a".to_string()]);
        backend
            .commit_write_with_generation("b", b"2222222", pending, Uuid::new_v4())
            .unwrap();

        assert!(matches!(
            backend.read_object("a"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert_eq!(backend.read_object("b").unwrap(), b"2222222");
    }

    #[test]
    fn lru_read_moves_bucket_behind_unread_peer() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_size_limit = 8;
        config.bucket_keys_limit = 1;
        config.quota_bytes = 16;
        config.eviction_policy = BucketEvictionPolicy::Lru;
        let backend = BucketStorageBackend::new(config);
        write(&backend, "a", b"1111111", Uuid::new_v4());
        write(&backend, "b", b"2222222", Uuid::new_v4());
        assert_eq!(backend.read_object("a").unwrap(), b"1111111");

        let pending = backend.prepare_write("c", 7).unwrap();
        assert_eq!(pending.keys(), vec!["b".to_string()]);
    }

    #[test]
    fn accepted_subset_rewrites_bucket_without_deleting_unaccepted_key() {
        let root = TempDir::new().unwrap();
        let backend = BucketStorageBackend::new(config(&root));
        write(&backend, "a", b"one", Uuid::new_v4());
        write(&backend, "b", b"two", Uuid::new_v4());
        let state = backend.state.lock();
        let records = ["a", "b"]
            .into_iter()
            .map(|key| BucketRecordSnapshot {
                storage_key: key.to_string(),
                record: state.records.get(key).unwrap().clone(),
            })
            .collect();
        drop(state);
        let pending = PendingBucketEviction {
            backend_id: backend.backend_id,
            token: 0,
            records,
            write_target: None,
            registry: None,
        };
        let (accepted, unaccepted) = pending.partition_accepted(&HashSet::from(["a".to_string()]));
        backend.commit_eviction(accepted).unwrap();
        backend.rollback_eviction(unaccepted);

        assert!(matches!(
            backend.read_object("a"),
            Err(StoreError::KeyNotFound(_))
        ));
        assert_eq!(backend.read_object("b").unwrap(), b"two");
    }

    #[test]
    fn restart_finishes_durable_accepted_eviction_journal_before_inventory() {
        let root = TempDir::new().unwrap();
        let config = config(&root);
        let generation = Uuid::new_v4();
        {
            let backend = BucketStorageBackend::new(config.clone());
            write(&backend, "accepted", b"value", generation);
            let journal = vec![AcceptedEvictionRecord {
                storage_key: "accepted".to_string(),
                generation_id: generation.to_string(),
            }];
            write_file_atomically(
                &backend.accepted_eviction_journal_path(),
                &serde_json::to_vec(&journal).unwrap(),
                &backend.temp_dir(),
            )
            .unwrap();
        }

        let recovered = BucketStorageBackend::new(config);
        assert!(recovered.scan_records().unwrap().is_empty());
        assert!(!recovered.accepted_eviction_journal_path().exists());
    }

    #[test]
    fn cpp_parity_bucket_batch_offload_continues_after_finalize_failure() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = 10 * 1024;
        let failed_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failure = Arc::clone(&failed_once);
        let backend = BucketStorageBackend::new_with_bucket_remove(config.clone(), move |path| {
            if !failure.swap(true, Ordering::SeqCst) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected bucket unlink failure",
                ));
            }
            std::fs::remove_file(path)
        });
        let old = vec![b'A'; 6 * 1024];
        let new = vec![b'B'; 6 * 1024];
        write(&backend, "old_key", &old, Uuid::new_v4());

        let pending = backend.prepare_write("new_key", new.len() as u64).unwrap();
        assert_eq!(pending.keys(), ["old_key"]);
        backend
            .commit_write_with_generation("new_key", &new, pending, Uuid::new_v4())
            .unwrap();

        assert!(failed_once.load(Ordering::SeqCst));
        assert!(matches!(
            backend.read_object("old_key"),
            Err(StoreError::KeyNotFound(key)) if key == "old_key"
        ));
        assert_eq!(backend.read_object("new_key").unwrap(), new);
        drop(backend);

        let restarted = BucketStorageBackend::new(config);
        let restarted_old = restarted.read_object("old_key");
        assert!(
            matches!(restarted_old, Err(StoreError::KeyNotFound(ref key)) if key == "old_key"),
            "unexpected restarted old-key result: {restarted_old:?}"
        );
        assert_eq!(restarted.read_object("new_key").unwrap(), new);
        assert_eq!(restarted.scan_records().unwrap().len(), 1);
    }

    #[test]
    fn cpp_parity_bucket_watermark_returns_victims_after_finalize_failure() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_keys_limit = 10;
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = 10 * 1024;
        let failed_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failure = Arc::clone(&failed_once);
        let backend = BucketStorageBackend::new_with_bucket_remove(config.clone(), move |path| {
            if !failure.swap(true, Ordering::SeqCst) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected watermark bucket unlink failure",
                ));
            }
            std::fs::remove_file(path)
        });
        write(&backend, "watermark_key", &[b'W'; 6 * 1024], Uuid::new_v4());

        let pending = backend.prepare_watermark_eviction(0.50, 0.25).unwrap();
        let returned_keys = pending.keys();
        assert_eq!(returned_keys, ["watermark_key"]);
        backend.commit_eviction(pending).unwrap();

        assert!(failed_once.load(Ordering::SeqCst));
        assert!(matches!(
            backend.read_object("watermark_key"),
            Err(StoreError::KeyNotFound(key)) if key == "watermark_key"
        ));
        drop(backend);

        let restarted = BucketStorageBackend::new(config);
        assert!(restarted.scan_records().unwrap().is_empty());
        assert!(matches!(
            restarted.read_object("watermark_key"),
            Err(StoreError::KeyNotFound(key)) if key == "watermark_key"
        ));
    }

    #[test]
    fn deferred_eviction_journal_accepts_json_escaped_storage_key() {
        let root = TempDir::new().unwrap();
        let mut config = config(&root);
        config.bucket_size_limit = 8 * 1024;
        config.quota_bytes = 10 * 1024;
        let failed_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failure = Arc::clone(&failed_once);
        let backend = BucketStorageBackend::new_with_bucket_remove(config.clone(), move |path| {
            if !failure.swap(true, Ordering::SeqCst) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected escaped-key unlink failure",
                ));
            }
            std::fs::remove_file(path)
        });
        let escaped_key = "\"\\\n\r\t".repeat(200);
        write(&backend, &escaped_key, &[b'E'; 5 * 1024], Uuid::new_v4());
        let pending = backend.prepare_watermark_eviction(0.50, 0.25).unwrap();
        assert_eq!(pending.keys(), [escaped_key.clone()]);
        backend.commit_eviction(pending).unwrap();
        assert!(failed_once.load(Ordering::SeqCst));
        drop(backend);

        let restarted = BucketStorageBackend::new(config);
        assert!(restarted.scan_records().unwrap().is_empty());
        assert!(matches!(
            restarted.read_object(&escaped_key),
            Err(StoreError::KeyNotFound(key)) if key == escaped_key
        ));
    }
}
