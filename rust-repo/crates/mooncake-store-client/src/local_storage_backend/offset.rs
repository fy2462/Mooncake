use super::{OffsetAllocatorConfig, OffsetEvictionPolicy, OffsetPersistMode};
use fs2::FileExt;
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const RECORD_HEADER_SIZE: usize = 24;
const RECORD_HEADER_PREFIX_SIZE: usize = 20;
const RECORD_FLAG_HAS_CRC: u32 = 1;
const RECORD_KNOWN_FLAGS: u32 = RECORD_FLAG_HAS_CRC;
const RECORD_VALUE_ALIGNMENT: u64 = 4096;
const MAX_KEY_LEN: usize = 1024 * 1024;
const OWNER_LOCK_FILE: &str = ".offset_allocator.lock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordHeader {
    key_len: u32,
    value_len: u32,
    write_seq: u64,
    flags: u32,
    crc32: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordFormatError(&'static str);

impl RecordHeader {
    fn encode_prefix(&self) -> [u8; RECORD_HEADER_PREFIX_SIZE] {
        let mut encoded = [0; RECORD_HEADER_PREFIX_SIZE];
        encoded[0..4].copy_from_slice(&self.key_len.to_le_bytes());
        encoded[4..8].copy_from_slice(&self.value_len.to_le_bytes());
        encoded[8..16].copy_from_slice(&self.write_seq.to_le_bytes());
        encoded[16..20].copy_from_slice(&self.flags.to_le_bytes());
        encoded
    }

    fn encode(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut encoded = [0; RECORD_HEADER_SIZE];
        encoded[..RECORD_HEADER_PREFIX_SIZE].copy_from_slice(&self.encode_prefix());
        encoded[RECORD_HEADER_PREFIX_SIZE..].copy_from_slice(&self.crc32.to_le_bytes());
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, RecordFormatError> {
        if encoded.len() < RECORD_HEADER_SIZE {
            return Err(RecordFormatError("truncated record header"));
        }
        let header = Self {
            key_len: u32::from_le_bytes(encoded[0..4].try_into().unwrap()),
            value_len: u32::from_le_bytes(encoded[4..8].try_into().unwrap()),
            write_seq: u64::from_le_bytes(encoded[8..16].try_into().unwrap()),
            flags: u32::from_le_bytes(encoded[16..20].try_into().unwrap()),
            crc32: u32::from_le_bytes(encoded[20..24].try_into().unwrap()),
        };
        if header.flags & !RECORD_KNOWN_FLAGS != 0 {
            return Err(RecordFormatError("record header has unknown flags"));
        }
        header.checked_record_size()?;
        Ok(header)
    }

    fn value_padding(key_len: u64) -> Result<u64, RecordFormatError> {
        if key_len > MAX_KEY_LEN as u64 {
            return Err(RecordFormatError(
                "offset allocator key length exceeds 1 MiB",
            ));
        }
        if key_len > u32::MAX.into() {
            return Err(RecordFormatError("record key length exceeds u32"));
        }
        let head = (RECORD_HEADER_SIZE as u64)
            .checked_add(key_len)
            .ok_or(RecordFormatError("record size overflow"))?;
        Ok((RECORD_VALUE_ALIGNMENT - head % RECORD_VALUE_ALIGNMENT) % RECORD_VALUE_ALIGNMENT)
    }

    fn value_offset(key_len: u64) -> Result<u64, RecordFormatError> {
        let padding = Self::value_padding(key_len)?;
        (RECORD_HEADER_SIZE as u64)
            .checked_add(key_len)
            .and_then(|head| head.checked_add(padding))
            .ok_or(RecordFormatError("record size overflow"))
    }

    fn record_size(key_len: u64, value_len: u64) -> Result<u64, RecordFormatError> {
        if value_len > u32::MAX.into() {
            return Err(RecordFormatError("record value length exceeds u32"));
        }
        let record_size = Self::value_offset(key_len)?
            .checked_add(value_len)
            .ok_or(RecordFormatError("record size overflow"))?;
        if record_size > u32::MAX.into() {
            return Err(RecordFormatError("record size exceeds u32"));
        }
        Ok(record_size)
    }

    fn checked_record_size(&self) -> Result<u64, RecordFormatError> {
        Self::record_size(self.key_len.into(), self.value_len.into())
    }

    fn validate_extent(&self, record_offset: u64, arena_len: u64) -> Result<(), RecordFormatError> {
        let record_end = record_offset
            .checked_add(self.checked_record_size()?)
            .ok_or(RecordFormatError("record extent overflow"))?;
        if record_end > arena_len {
            return Err(RecordFormatError("record extent is out of bounds"));
        }
        Ok(())
    }

    #[cfg(test)]
    fn verify_crc(&self, key: &[u8], value: &[u8]) -> Result<(), RecordFormatError> {
        if key.len() != self.key_len as usize || value.len() != self.value_len as usize {
            return Err(RecordFormatError("record lengths do not match header"));
        }
        if self.flags & RECORD_FLAG_HAS_CRC != 0
            && crc32c([self.encode_prefix().as_slice(), key, value]) != self.crc32
        {
            return Err(RecordFormatError("record CRC-32C mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct Crc32c(u32);

impl Crc32c {
    fn new() -> Self {
        Self(u32::MAX)
    }

    fn extend(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                self.0 = if self.0 & 1 != 0 {
                    0x82f6_3b78 ^ (self.0 >> 1)
                } else {
                    self.0 >> 1
                };
            }
        }
    }

    fn finish(self) -> u32 {
        self.0 ^ u32::MAX
    }
}

fn crc32c<I, B>(chunks: I) -> u32
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let mut crc = Crc32c::new();
    for chunk in chunks {
        crc.extend(chunk.as_ref());
    }
    crc.finish()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OffsetEntryFormat {
    #[default]
    LegacyRaw,
    V3,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OffsetEntry {
    /// Start of the raw value for legacy entries and of the complete record for v3.
    offset: u64,
    /// Raw value length for legacy entries and complete record extent length for v3.
    len: u64,
    #[serde(default)]
    fifo_seq: u64,
    #[serde(default)]
    format: OffsetEntryFormat,
    /// Offset of the value relative to `offset`; zero for legacy raw values.
    #[serde(default)]
    value_offset: u64,
    /// Value length stored in a v3 header; `len` is authoritative for legacy values.
    #[serde(default)]
    value_len: u64,
    /// Expected on-disk record sequence. Zero is reserved for legacy entries.
    #[serde(default)]
    write_seq: u64,
}

impl OffsetEntry {
    #[cfg(test)]
    fn legacy(offset: u64, len: u64, fifo_seq: u64) -> Self {
        Self {
            offset,
            len,
            fifo_seq,
            format: OffsetEntryFormat::LegacyRaw,
            value_offset: 0,
            value_len: 0,
            write_seq: 0,
        }
    }

    fn value_len(&self) -> u64 {
        match self.format {
            OffsetEntryFormat::LegacyRaw => self.len,
            OffsetEntryFormat::V3 => self.value_len,
            OffsetEntryFormat::Unknown => 0,
        }
    }

    fn absolute_value_offset(&self) -> Option<u64> {
        self.offset.checked_add(self.value_offset)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedIndex {
    #[serde(default)]
    entries: HashMap<String, OffsetEntry>,
    #[serde(default)]
    next_offset: u64,
    #[serde(default)]
    next_fifo_seq: u64,
}

const CHECKPOINT_FORMAT: &str = "mooncake-offset-allocator-checkpoint";
const CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointPayload {
    index: PersistedIndex,
    #[serde(default)]
    next_write_seq: u64,
    #[serde(default)]
    tombstones: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointEnvelope {
    format: String,
    version: u32,
    payload_crc32c: u32,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointBoundary {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableWriteBoundary {
    RecordWritten,
    DataSync,
    DataSynced,
    CheckpointPublish,
}

#[derive(Debug)]
struct OffsetState {
    index: PersistedIndex,
    free_extents: BTreeMap<u64, u64>,
    used_bytes: u64,
    quota_bytes: u64,
    next_write_seq: u64,
    tombstones: Vec<String>,
    dirty: bool,
    last_checkpoint_at: Option<Duration>,
    pinned_extents: HashMap<ExtentIdentity, usize>,
    deferred_free_extents: HashMap<ExtentIdentity, OffsetEntry>,
    pending_rebuild_arena_len: Option<u64>,
}

impl Default for OffsetState {
    fn default() -> Self {
        Self {
            index: PersistedIndex::default(),
            free_extents: BTreeMap::new(),
            used_bytes: 0,
            quota_bytes: 0,
            next_write_seq: 1,
            tombstones: Vec::new(),
            dirty: false,
            last_checkpoint_at: None,
            pinned_extents: HashMap::new(),
            deferred_free_extents: HashMap::new(),
            pending_rebuild_arena_len: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExtentIdentity {
    offset: u64,
    len: u64,
    write_seq: u64,
}

impl From<&OffsetEntry> for ExtentIdentity {
    fn from(entry: &OffsetEntry) -> Self {
        Self {
            offset: entry.offset,
            len: entry.len,
            write_seq: entry.write_seq,
        }
    }
}

struct ReadExtentLease<'a> {
    backend: &'a OffsetAllocatorStorageBackend,
    entry: OffsetEntry,
}

impl Drop for ReadExtentLease<'_> {
    fn drop(&mut self) {
        self.backend.release_extent(&self.entry);
    }
}

struct OffsetReadPlan<'a> {
    file: std::fs::File,
    lease: ReadExtentLease<'a>,
}

#[derive(Debug)]
struct LoadedCheckpoint {
    index: PersistedIndex,
    next_write_seq: u64,
    tombstones: Vec<String>,
}

#[derive(Debug, Default)]
pub struct PendingOffsetEviction {
    victims: Vec<(String, OffsetEntry)>,
}

impl PendingOffsetEviction {
    pub fn keys(&self) -> Vec<String> {
        self.victims.iter().map(|(key, _)| key.clone()).collect()
    }
}

fn record_format_store_error(error: RecordFormatError) -> StoreError {
    StoreError::InvalidParams(error.0.to_string())
}

fn record_header(
    key: &str,
    value: &[u8],
    write_seq: u64,
    enable_crc: bool,
) -> StoreResult<(RecordHeader, u64)> {
    if write_seq == 0 {
        return Err(StoreError::Internal(
            "offset allocator v3 write sequence must be nonzero".to_string(),
        ));
    }
    let key_len = u32::try_from(key.len()).map_err(|_| {
        StoreError::InvalidParams("offset allocator key length exceeds u32".to_string())
    })?;
    let value_len = u32::try_from(value.len()).map_err(|_| {
        StoreError::InvalidParams("offset allocator value length exceeds u32".to_string())
    })?;
    let mut header = RecordHeader {
        key_len,
        value_len,
        write_seq,
        flags: if enable_crc { RECORD_FLAG_HAS_CRC } else { 0 },
        crc32: 0,
    };
    header
        .checked_record_size()
        .map_err(record_format_store_error)?;
    if enable_crc {
        header.crc32 = crc32c([header.encode_prefix().as_slice(), key.as_bytes(), value]);
    }
    let padding = RecordHeader::value_padding(key_len.into()).map_err(record_format_store_error)?;
    Ok((header, padding))
}

fn write_zero_padding(writer: &mut impl Write, mut len: u64) -> StoreResult<()> {
    const ZEROS: [u8; 4096] = [0; 4096];
    while len > 0 {
        let chunk = len.min(ZEROS.len() as u64) as usize;
        writer.write_all(&ZEROS[..chunk])?;
        len -= chunk as u64;
    }
    Ok(())
}

/// Append/reuse offset allocator used by the client SSD offload path.
///
/// Metadata is persisted separately from the data file. FIFO eviction removes
/// logical entries first and makes their extents available for subsequent
/// writes, keeping the data file bounded by the configured quota.
pub struct OffsetAllocatorStorageBackend {
    config: OffsetAllocatorConfig,
    state: Mutex<OffsetState>,
    init_lock: Mutex<()>,
    owner_lock: Mutex<Option<std::fs::File>>,
    initialized: AtomicBool,
    clock: Arc<dyn Fn() -> Duration + Send + Sync>,
}

impl OffsetAllocatorStorageBackend {
    pub fn new(config: OffsetAllocatorConfig) -> Self {
        let started = Instant::now();
        Self {
            config,
            state: Mutex::new(OffsetState::default()),
            init_lock: Mutex::new(()),
            owner_lock: Mutex::new(None),
            initialized: AtomicBool::new(false),
            clock: Arc::new(move || started.elapsed()),
        }
    }

    fn data_dir(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.fsdir)
    }

    fn data_path(&self) -> PathBuf {
        self.data_dir().join("offset_allocator.data")
    }

    fn index_path(&self) -> PathBuf {
        self.data_dir().join("offset_allocator.index.json")
    }

    fn owner_lock_path(&self) -> PathBuf {
        self.data_dir().join(OWNER_LOCK_FILE)
    }

    pub fn init(&self) -> StoreResult<()> {
        let _init_guard = self.init_lock.lock();
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        self.config.validate().map_err(StoreError::InvalidParams)?;
        std::fs::create_dir_all(self.data_dir())?;

        // Take ownership before inspecting or mutating any persistent file.
        // The open descriptor holds the advisory lock for this backend's full
        // lifetime; a failed init releases it without cleaning another
        // backend's data.
        let owner_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.owner_lock_path())?;
        owner_file.try_lock_exclusive().map_err(|error| {
            StoreError::Internal(format!(
                "offset allocator data directory is already owned: {error}"
            ))
        })?;
        *self.owner_lock.lock() = Some(owner_file);

        let result = self.init_owned_directory();
        if result.is_err() {
            if let Some(file) = self.owner_lock.lock().take() {
                let _ = FileExt::unlock(&file);
            }
        }
        result
    }

    fn init_owned_directory(&self) -> StoreResult<()> {
        if self.config.persist_mode == OffsetPersistMode::Disabled {
            // Never leave an old arena available after publishing/removing its
            // checkpoint in non-persistent mode.
            remove_file_if_present(&self.data_path())?;
            remove_file_if_present(&self.index_path())?;
            remove_stale_checkpoint_tmp(&self.index_path())?;
            let quota_bytes = if self.config.quota_bytes > 0 {
                self.config.quota_bytes
            } else {
                (fs2::available_space(self.data_dir())? as f64 * 0.90) as u64
            };
            *self.state.lock() = OffsetState {
                quota_bytes,
                ..OffsetState::default()
            };
            self.initialized.store(true, Ordering::Release);
            return Ok(());
        }

        remove_stale_checkpoint_tmp(&self.index_path())?;
        let loaded = load_checkpoint(&self.index_path())?;
        let file_len = match std::fs::metadata(self.data_path()) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let mut recovered = recover_checkpoint(loaded, &self.data_path(), file_len)?;
        let upgraded = normalize_loaded_index(&mut recovered.index, file_len);
        let quota_bytes = if self.config.quota_bytes > 0 {
            self.config.quota_bytes
        } else {
            (fs2::available_space(self.data_dir())? as f64 * 0.90) as u64
        };
        let used_bytes = recovered
            .index
            .entries
            .values()
            .map(|entry| entry.len)
            .sum();
        let free_extents = rebuild_free_extents(&recovered.index, file_len);
        *self.state.lock() = OffsetState {
            index: recovered.index,
            free_extents,
            used_bytes,
            quota_bytes,
            next_write_seq: recovered.next_write_seq.max(1),
            tombstones: recovered.tombstones,
            dirty: false,
            last_checkpoint_at: None,
            pinned_extents: HashMap::new(),
            deferred_free_extents: HashMap::new(),
            pending_rebuild_arena_len: None,
        };
        if upgraded {
            let mut state = self.state.lock();
            state.dirty = true;
            self.checkpoint_dirty_state(&mut state, true)?;
        }
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    fn ensure_init(&self) -> StoreResult<()> {
        if self.initialized.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(StoreError::Internal(
                "offset allocator storage backend is not initialized".to_string(),
            ))
        }
    }

    pub fn write_object(&self, key: &str, data: &[u8]) -> StoreResult<Vec<String>> {
        let pending = self.prepare_write(key, data.len() as u64)?;
        let evicted = pending.keys();
        self.commit_write(key, data, pending)?;
        Ok(evicted)
    }

    pub fn prepare_write(&self, key: &str, required: u64) -> StoreResult<PendingOffsetEviction> {
        self.ensure_init()?;
        let required = RecordHeader::record_size(key.len() as u64, required)
            .map_err(record_format_store_error)?;
        let state = self.state.lock();
        if required > state.quota_bytes {
            return Err(StoreError::NoAvailableHandle);
        }
        let replaced = state.index.entries.get(key).cloned();
        let replaced_len = replaced.as_ref().map(|entry| entry.len).unwrap_or(0);
        let high = (state.quota_bytes as f64 * self.config.high_ratio) as u64;
        let low = (state.quota_bytes as f64 * self.config.low_ratio) as u64;
        let (keys_high, keys_low) = key_watermarks(&self.config);
        let over_bytes = state
            .used_bytes
            .saturating_sub(replaced_len)
            .saturating_add(required)
            > high;
        let over_keys = state.index.entries.len() > keys_high;
        let mut pending = PendingOffsetEviction::default();

        if self.config.eviction_policy == OffsetEvictionPolicy::Fifo && (over_bytes || over_keys) {
            let minimum_victims = if over_keys {
                self.config.fallback_evict_batch
            } else {
                0
            };
            let mut projected_used = state.used_bytes;
            let mut projected_keys = state.index.entries.len();
            while (projected_used
                .saturating_sub(replaced_len)
                .saturating_add(required)
                > low
                || projected_keys > keys_low
                || pending.victims.len() < minimum_victims)
                && pending.victims.len() < self.config.max_evict_per_offload
            {
                let victim = state
                    .index
                    .entries
                    .iter()
                    .filter(|(candidate, _)| candidate.as_str() != key)
                    .filter(|(candidate, _)| {
                        !pending
                            .victims
                            .iter()
                            .any(|(selected, _)| selected == *candidate)
                    })
                    .min_by_key(|(_, entry)| entry.fifo_seq)
                    .map(|(candidate, entry)| (candidate.clone(), entry.clone()));
                let Some((victim, entry)) = victim else {
                    break;
                };
                projected_used = projected_used.saturating_sub(entry.len);
                projected_keys = projected_keys.saturating_sub(1);
                pending.victims.push((victim, entry));
            }
        }
        Ok(pending)
    }

    pub fn rollback_eviction(&self, pending: PendingOffsetEviction) {
        drop(pending);
    }

    pub fn commit_write(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingOffsetEviction,
    ) -> StoreResult<()> {
        self.commit_write_with_hook(key, data, pending, |_| Ok(()))
    }

    fn commit_write_with_hook(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingOffsetEviction,
        mut boundary_hook: impl FnMut(DurableWriteBoundary) -> StoreResult<()>,
    ) -> StoreResult<()> {
        self.ensure_init()?;
        let required = RecordHeader::record_size(key.len() as u64, data.len() as u64)
            .map_err(record_format_store_error)?;
        let mut state = self.state.lock();
        let replaced = state.index.entries.get(key).cloned();
        let committed_victims = apply_pending_eviction(&mut state, pending);

        let mut candidate_free = state.free_extents.clone();
        for (_, entry) in &committed_victims {
            if !state
                .pinned_extents
                .contains_key(&ExtentIdentity::from(entry))
            {
                insert_free_extent(&mut candidate_free, entry.offset, entry.len);
            }
        }
        let mut allocation = allocate_extent(
            &mut candidate_free,
            state.index.next_offset,
            state.quota_bytes,
            required,
        );
        if allocation.is_none() && committed_victims.is_empty() {
            self.relaxed_checkpoint_before_allocation(&mut state);
            candidate_free = state.free_extents.clone();
            allocation = allocate_extent(
                &mut candidate_free,
                state.index.next_offset,
                state.quota_bytes,
                required,
            );
        }
        let Some((offset, candidate_next_offset)) = allocation else {
            self.finalize_evicted_entries(&mut state, committed_victims)?;
            return Err(StoreError::NoAvailableHandle);
        };

        let write_seq = state.next_write_seq;
        let next_write_seq = write_seq.checked_add(1).ok_or_else(|| {
            StoreError::Internal("offset allocator write sequence exhausted".to_string())
        })?;
        let (header, padding) = record_header(key, data, write_seq, self.config.enable_record_crc)?;
        debug_assert_eq!(header.checked_record_size().ok(), Some(required));
        let write_result = (|| -> StoreResult<()> {
            let mut data_file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(self.data_path())?;
            data_file.seek(SeekFrom::Start(offset))?;
            data_file.write_all(&header.encode())?;
            data_file.write_all(key.as_bytes())?;
            write_zero_padding(&mut data_file, padding)?;
            data_file.write_all(data)?;
            boundary_hook(DurableWriteBoundary::RecordWritten)?;
            boundary_hook(DurableWriteBoundary::DataSync)?;
            data_file.sync_data()?;
            boundary_hook(DurableWriteBoundary::DataSynced)?;
            Ok(())
        })();
        if let Err(error) = write_result {
            self.finalize_evicted_entries(&mut state, committed_victims)?;
            return Err(error);
        }

        state.free_extents = candidate_free;
        if let Some(arena_len) = state.pending_rebuild_arena_len.as_mut() {
            // A write may append while an older generation is still pinned.
            // Keep the eventual whole-map rebuild wide enough to cover every
            // byte written before the final lease is released.
            *arena_len = (*arena_len).max(candidate_next_offset);
        }
        defer_pinned_extents(&mut state, &committed_victims);
        state.index.next_offset = candidate_next_offset;
        state.next_write_seq = next_write_seq;

        if let Some(old) = replaced {
            state.index.entries.remove(key);
            state.used_bytes = state.used_bytes.saturating_sub(old.len);
            retire_extent(&mut state, old);
        }
        let fifo_seq = state.index.next_fifo_seq;
        state.index.next_fifo_seq = state.index.next_fifo_seq.saturating_add(1);
        state.index.entries.insert(
            key.to_string(),
            OffsetEntry {
                offset,
                len: required,
                fifo_seq,
                format: OffsetEntryFormat::V3,
                value_offset: RecordHeader::value_offset(key.len() as u64)
                    .map_err(record_format_store_error)?,
                value_len: data.len() as u64,
                write_seq,
            },
        );
        state.used_bytes = state.used_bytes.saturating_add(required);
        state.tombstones.retain(|tombstone| tombstone != key);
        self.after_mutation_with_hook(&mut state, &mut boundary_hook)?;
        Ok(())
    }

    pub fn read_object(&self, key: &str) -> StoreResult<Vec<u8>> {
        self.read_object_with_hook(key, || {})
    }

    fn read_object_with_hook(
        &self,
        key: &str,
        after_entry_lookup: impl FnOnce(),
    ) -> StoreResult<Vec<u8>> {
        self.ensure_init()?;
        let mut plan = self.prepare_read(key)?;
        after_entry_lookup();
        let value_offset = plan.lease.entry.absolute_value_offset().ok_or_else(|| {
            StoreError::Internal("offset allocator value offset overflow".to_string())
        })?;
        plan.file.seek(SeekFrom::Start(value_offset))?;
        let value_len = usize::try_from(plan.lease.entry.value_len()).map_err(|_| {
            StoreError::Internal("offset allocator value length exceeds usize".to_string())
        })?;
        let mut value = vec![0; value_len];
        plan.file.read_exact(&mut value)?;
        Ok(value)
    }

    fn prepare_read(&self, key: &str) -> StoreResult<OffsetReadPlan<'_>> {
        let mut state = self.state.lock();
        let entry = state
            .index
            .entries
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::KeyNotFound(key.to_string()))?;
        // Keep lookup, opening the arena, and acquiring the logical extent
        // lease atomic with respect to remove_all. If open fails, no pin has
        // been published and therefore no cleanup can be missed.
        let file = std::fs::File::open(self.data_path())?;
        *state
            .pinned_extents
            .entry(ExtentIdentity::from(&entry))
            .or_default() += 1;
        Ok(OffsetReadPlan {
            file,
            lease: ReadExtentLease {
                backend: self,
                entry,
            },
        })
    }

    fn release_extent(&self, entry: &OffsetEntry) {
        let identity = ExtentIdentity::from(entry);
        let mut state = self.state.lock();
        let Some(readers) = state.pinned_extents.get_mut(&identity) else {
            debug_assert!(false, "offset allocator extent lease was not pinned");
            return;
        };
        *readers -= 1;
        if *readers == 0 {
            state.pinned_extents.remove(&identity);
            if let Some(entry) = state.deferred_free_extents.remove(&identity) {
                insert_free_extent(&mut state.free_extents, entry.offset, entry.len);
            }
        }
        if state.pinned_extents.is_empty() && state.deferred_free_extents.is_empty() {
            if let Some(arena_len) = state.pending_rebuild_arena_len.take() {
                // Rebuild from the current live index, not from the index that
                // existed when the checkpoint completed. Mutations between
                // checkpoint and final unpin therefore cannot resurrect an
                // extent from the detached generation (ABA).
                state.free_extents = rebuild_free_extents(&state.index, arena_len);
            }
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        self.initialized.load(Ordering::Acquire)
            && self.state.lock().index.entries.contains_key(key)
    }

    pub fn space_usage(&self) -> (u64, u64) {
        let state = self.state.lock();
        (state.used_bytes, state.quota_bytes)
    }

    pub fn prepare_watermark_eviction(
        &self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<PendingOffsetEviction> {
        self.ensure_init()?;
        if !(0.0 < low_watermark_ratio
            && low_watermark_ratio < high_watermark_ratio
            && high_watermark_ratio <= 1.0)
        {
            return Err(StoreError::InvalidParams(
                "watermarks must satisfy 0 < low < high <= 1".to_string(),
            ));
        }
        let state = self.state.lock();
        let high = (state.quota_bytes as f64 * high_watermark_ratio) as u64;
        if state.used_bytes <= high {
            return Ok(PendingOffsetEviction::default());
        }
        let low = (state.quota_bytes as f64 * low_watermark_ratio) as u64;
        let mut pending = PendingOffsetEviction::default();
        let mut projected_used = state.used_bytes;
        while projected_used > low && pending.victims.len() < self.config.max_evict_per_offload {
            let victim = state
                .index
                .entries
                .iter()
                .filter(|(candidate, _)| {
                    !pending
                        .victims
                        .iter()
                        .any(|(selected, _)| selected == *candidate)
                })
                .min_by_key(|(_, entry)| entry.fifo_seq)
                .map(|(key, entry)| (key.clone(), entry.clone()));
            let Some((victim, entry)) = victim else {
                break;
            };
            projected_used = projected_used.saturating_sub(entry.len);
            pending.victims.push((victim, entry));
        }
        Ok(pending)
    }

    pub fn commit_eviction(&self, pending: PendingOffsetEviction) -> StoreResult<()> {
        self.ensure_init()?;
        let mut state = self.state.lock();
        let committed_victims = apply_pending_eviction(&mut state, pending);
        self.finalize_evicted_entries(&mut state, committed_victims)
    }

    pub fn delete_object(&self, key: &str) -> StoreResult<()> {
        self.ensure_init()?;
        let mut state = self.state.lock();
        if let Some(entry) = state.index.entries.remove(key) {
            state.used_bytes = state.used_bytes.saturating_sub(entry.len);
            retire_extent(&mut state, entry);
            record_tombstone(&mut state, key);
            self.after_mutation(&mut state)?;
        } else if state.dirty && self.config.persist_mode == OffsetPersistMode::Strict {
            // A prior strict delete may have committed its in-memory removal
            // before its checkpoint failed. Retrying the same public call must
            // retry that durability barrier even though the key is now absent.
            self.checkpoint_dirty_state(&mut state, true)?;
        }
        Ok(())
    }

    pub fn remove_all(&self) -> StoreResult<usize> {
        self.ensure_init()?;
        let mut state = self.state.lock();
        let count = state.index.entries.len();
        let removed_keys: Vec<_> = state.index.entries.keys().cloned().collect();
        let arena_len = match std::fs::metadata(self.data_path()) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        state.index = PersistedIndex::default();
        state.free_extents.clear();
        state.deferred_free_extents.clear();
        // remove_all starts a new logical generation. Any earlier deferred
        // rebuild described the generation being detached and is stale now.
        state.pending_rebuild_arena_len = None;
        state.used_bytes = 0;
        for key in removed_keys {
            record_tombstone(&mut state, &key);
        }
        match self.config.persist_mode {
            OffsetPersistMode::Relaxed => {
                // Until the next relaxed checkpoint, the previous checkpoint
                // is authoritative. Preserve its arena bytes and append beyond
                // them so an abrupt restart can recover that generation.
                state.index.next_offset = arena_len;
                self.after_mutation(&mut state)?;
            }
            OffsetPersistMode::Strict => {
                // Publish the empty generation before unlinking the old arena.
                // A failed checkpoint leaves both the dirty in-memory state and
                // old arena available for retry. If unlink fails, restart still
                // observes the already-durable empty checkpoint.
                state.dirty = true;
                self.checkpoint_dirty_state(&mut state, false)?;
                remove_file_if_present(&self.data_path())?;
                state.pending_rebuild_arena_len = None;
                state.free_extents.clear();
            }
            OffsetPersistMode::Disabled => {
                remove_file_if_present(&self.data_path())?;
                state.pending_rebuild_arena_len = None;
                self.after_mutation(&mut state)?;
            }
        }
        Ok(count)
    }

    fn persist_state(&self, state: &OffsetState) -> StoreResult<()> {
        write_checkpoint_state(
            &self.index_path(),
            &state.index,
            state.next_write_seq,
            &state.tombstones,
        )
    }

    fn after_mutation(&self, state: &mut OffsetState) -> StoreResult<()> {
        self.after_mutation_with_hook(state, &mut |_| Ok(()))
    }

    fn after_mutation_with_hook(
        &self,
        state: &mut OffsetState,
        boundary_hook: &mut impl FnMut(DurableWriteBoundary) -> StoreResult<()>,
    ) -> StoreResult<()> {
        match self.config.persist_mode {
            OffsetPersistMode::Disabled => {
                state.dirty = false;
                state.tombstones.clear();
                Ok(())
            }
            OffsetPersistMode::Strict => {
                state.dirty = true;
                self.checkpoint_dirty_state_with_hook(state, true, boundary_hook)
            }
            OffsetPersistMode::Relaxed => {
                state.dirty = true;
                let now = (self.clock)();
                let interval = Duration::from_secs(self.config.persist_interval_seconds as u64);
                let due = state
                    .last_checkpoint_at
                    .is_none_or(|last| now.saturating_sub(last) >= interval);
                if due {
                    // Relaxed mode preserves the dirty state and availability
                    // when a periodic durability barrier fails.
                    let _ = self.checkpoint_dirty_state_with_hook(state, true, boundary_hook);
                }
                Ok(())
            }
        }
    }

    fn checkpoint_dirty_state(&self, state: &mut OffsetState, sync_arena: bool) -> StoreResult<()> {
        self.checkpoint_dirty_state_with_hook(state, sync_arena, &mut |_| Ok(()))
    }

    fn checkpoint_dirty_state_with_hook(
        &self,
        state: &mut OffsetState,
        sync_arena: bool,
        boundary_hook: &mut impl FnMut(DurableWriteBoundary) -> StoreResult<()>,
    ) -> StoreResult<()> {
        if !state.dirty || self.config.persist_mode == OffsetPersistMode::Disabled {
            return Ok(());
        }
        if sync_arena {
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.data_path())
            {
                Ok(file) => file.sync_data()?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        // At this boundary the candidate metadata is dirty and all referenced
        // arena bytes have been synced, but the checkpoint has not yet been
        // published. Fault injection here therefore models a real failed
        // publication without losing retry state.
        boundary_hook(DurableWriteBoundary::CheckpointPublish)?;
        self.persist_state(state)?;
        state.dirty = false;
        state.tombstones.clear();
        state.last_checkpoint_at = Some((self.clock)());
        let arena_len = std::fs::metadata(self.data_path())
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if state.pinned_extents.is_empty() && state.deferred_free_extents.is_empty() {
            state.free_extents = rebuild_free_extents(&state.index, arena_len);
            state.pending_rebuild_arena_len = None;
        } else {
            // A checkpoint is a generation-reclamation barrier, but pinned
            // readers may still own extents from the prior generation. Delay
            // the whole-map rebuild until the final old lease is released.
            state.pending_rebuild_arena_len = Some(arena_len);
        }
        Ok(())
    }

    fn relaxed_checkpoint_before_allocation(&self, state: &mut OffsetState) {
        if self.config.persist_mode != OffsetPersistMode::Relaxed || !state.dirty {
            return;
        }
        let now = (self.clock)();
        let interval = Duration::from_secs(self.config.persist_interval_seconds as u64);
        let due = state
            .last_checkpoint_at
            .is_none_or(|last| now.saturating_sub(last) >= interval);
        if due {
            // Best effort matches relaxed post-mutation scheduling. A success
            // rebuilds free extents before allocation, allowing a full old
            // generation cleared by remove_all to make forward progress.
            // This preallocation barrier must not consume the periodic due
            // time: the write about to be allocated still needs its own
            // post-mutation checkpoint before it can be considered durable.
            let scheduled_checkpoint_at = state.last_checkpoint_at;
            if self.checkpoint_dirty_state(state, true).is_ok() {
                state.last_checkpoint_at = scheduled_checkpoint_at;
            }
        }
    }

    #[cfg(test)]
    fn simulate_abrupt_exit(self) {
        if let Some(file) = self.owner_lock.lock().take() {
            let _ = FileExt::unlock(&file);
        }
        std::mem::forget(self);
    }

    fn finalize_evicted_entries(
        &self,
        state: &mut OffsetState,
        entries: Vec<(String, OffsetEntry)>,
    ) -> StoreResult<()> {
        if entries.is_empty() {
            if state.dirty && self.config.persist_mode == OffsetPersistMode::Strict {
                self.checkpoint_dirty_state(state, true)?;
            }
            return Ok(());
        }
        for (key, entry) in entries {
            record_tombstone(state, &key);
            retire_extent(state, entry);
        }
        self.after_mutation(state)
    }
}

impl Drop for OffsetAllocatorStorageBackend {
    fn drop(&mut self) {
        if self.initialized.load(Ordering::Acquire)
            && self.config.persist_mode != OffsetPersistMode::Disabled
        {
            let mut state = self.state.lock();
            let _ = self.checkpoint_dirty_state(&mut state, true);
        }
        if let Some(file) = self.owner_lock.lock().take() {
            let _ = FileExt::unlock(&file);
        }
    }
}

fn remove_file_if_present(path: &Path) -> StoreResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn checkpoint_tmp_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

fn remove_stale_checkpoint_tmp(path: &Path) -> StoreResult<()> {
    match std::fs::remove_file(checkpoint_tmp_path(path)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn canonical_json_bytes(value: &serde_json::Value) -> StoreResult<Vec<u8>> {
    fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(canonicalize).collect())
            }
            serde_json::Value::Object(values) => {
                let sorted = values
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect();
                serde_json::Value::Object(sorted)
            }
            other => other.clone(),
        }
    }

    Ok(serde_json::to_vec(&canonicalize(value))?)
}

#[cfg(test)]
fn checkpoint_envelope(index: &PersistedIndex) -> StoreResult<CheckpointEnvelope> {
    checkpoint_envelope_state(index, 1, &[])
}

fn checkpoint_envelope_state(
    index: &PersistedIndex,
    next_write_seq: u64,
    tombstones: &[String],
) -> StoreResult<CheckpointEnvelope> {
    let payload = serde_json::to_value(CheckpointPayload {
        index: PersistedIndex {
            entries: index.entries.clone(),
            next_offset: index.next_offset,
            next_fifo_seq: index.next_fifo_seq,
        },
        next_write_seq: next_write_seq.max(1),
        tombstones: tombstones.to_vec(),
    })?;
    Ok(CheckpointEnvelope {
        format: CHECKPOINT_FORMAT.to_string(),
        version: CHECKPOINT_VERSION,
        payload_crc32c: crc32c([canonical_json_bytes(&payload)?]),
        payload,
    })
}

fn empty_loaded_checkpoint() -> LoadedCheckpoint {
    LoadedCheckpoint {
        index: PersistedIndex::default(),
        next_write_seq: 1,
        tombstones: Vec::new(),
    }
}

fn load_checkpoint(path: &Path) -> StoreResult<LoadedCheckpoint> {
    let mut bytes = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut file) => file.read_to_end(&mut bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(empty_loaded_checkpoint());
        }
        Err(error) => return Err(error.into()),
    };

    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Ok(empty_loaded_checkpoint());
    };
    if value.get("format").and_then(serde_json::Value::as_str) != Some(CHECKPOINT_FORMAT) {
        return Ok(LoadedCheckpoint {
            index: serde_json::from_value(value).unwrap_or_default(),
            next_write_seq: 1,
            tombstones: Vec::new(),
        });
    }

    let Ok(envelope) = serde_json::from_value::<CheckpointEnvelope>(value) else {
        return Ok(empty_loaded_checkpoint());
    };
    if envelope.version != CHECKPOINT_VERSION {
        return Ok(empty_loaded_checkpoint());
    }
    let Ok(payload_bytes) = canonical_json_bytes(&envelope.payload) else {
        return Ok(empty_loaded_checkpoint());
    };
    if crc32c([payload_bytes]) != envelope.payload_crc32c {
        return Ok(empty_loaded_checkpoint());
    }
    let Ok(payload) = serde_json::from_value::<CheckpointPayload>(envelope.payload) else {
        return Ok(empty_loaded_checkpoint());
    };
    Ok(LoadedCheckpoint {
        index: payload.index,
        next_write_seq: payload.next_write_seq.max(1),
        tombstones: payload.tombstones,
    })
}

#[cfg(test)]
fn load_persisted_index(path: &Path) -> StoreResult<PersistedIndex> {
    load_checkpoint(path).map(|loaded| loaded.index)
}

#[cfg(test)]
fn write_checkpoint(path: &Path, index: &PersistedIndex) -> StoreResult<()> {
    write_checkpoint_with_hook(path, index, |_| Ok(()))
}

#[cfg(test)]
fn write_checkpoint_with_hook(
    path: &Path,
    index: &PersistedIndex,
    mut boundary_hook: impl FnMut(CheckpointBoundary) -> std::io::Result<()>,
) -> StoreResult<()> {
    write_checkpoint_state_with_hook(path, index, 1, &[], &mut boundary_hook)
}

fn write_checkpoint_state(
    path: &Path,
    index: &PersistedIndex,
    next_write_seq: u64,
    tombstones: &[String],
) -> StoreResult<()> {
    write_checkpoint_state_with_hook(path, index, next_write_seq, tombstones, &mut |_| Ok(()))
}

fn write_checkpoint_state_with_hook(
    path: &Path,
    index: &PersistedIndex,
    next_write_seq: u64,
    tombstones: &[String],
    boundary_hook: &mut impl FnMut(CheckpointBoundary) -> std::io::Result<()>,
) -> StoreResult<()> {
    let temporary = checkpoint_tmp_path(path);
    let bytes = serde_json::to_vec(&checkpoint_envelope_state(
        index,
        next_write_seq,
        tombstones,
    )?)?;
    let result = (|| -> StoreResult<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)?;
        boundary_hook(CheckpointBoundary::Write)?;
        file.write_all(&bytes)?;
        boundary_hook(CheckpointBoundary::FileSync)?;
        file.sync_all()?;
        drop(file);
        boundary_hook(CheckpointBoundary::Rename)?;
        std::fs::rename(&temporary, path)?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let directory = std::fs::File::open(parent)?;
        boundary_hook(CheckpointBoundary::DirectorySync)?;
        directory.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn recover_checkpoint(
    mut loaded: LoadedCheckpoint,
    arena_path: &Path,
    arena_len: u64,
) -> StoreResult<LoadedCheckpoint> {
    if loaded.index.entries.is_empty() {
        loaded.index.next_offset = 0;
        return Ok(loaded);
    }

    let mut arena = match std::fs::File::open(arena_path) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let mut candidates: Vec<_> = std::mem::take(&mut loaded.index.entries)
        .into_iter()
        .collect();
    candidates.sort_by(|(left_key, left), (right_key, right)| {
        left.offset
            .cmp(&right.offset)
            .then_with(|| left_key.cmp(right_key))
    });

    let mut survivors = HashMap::new();
    let mut occupied: Vec<(u64, u64)> = Vec::new();
    for (key, entry) in candidates {
        let Some(end) = entry.offset.checked_add(entry.len) else {
            continue;
        };
        if end > arena_len {
            continue;
        }
        if occupied
            .iter()
            .any(|(start, occupied_end)| entry.offset < *occupied_end && *start < end)
        {
            continue;
        }

        let valid = match entry.format {
            OffsetEntryFormat::LegacyRaw => {
                entry.value_offset == 0
                    && entry.value_len == 0
                    && entry.write_seq == 0
                    && (entry.len == 0 || arena.is_some())
            }
            OffsetEntryFormat::V3 => {
                let Some(file) = arena.as_mut() else {
                    continue;
                };
                validate_v3_record(file, arena_len, &key, &entry, loaded.next_write_seq)?
            }
            OffsetEntryFormat::Unknown => false,
        };
        if valid {
            occupied.push((entry.offset, end));
            survivors.insert(key, entry);
        }
    }
    for tombstone in &loaded.tombstones {
        survivors.remove(tombstone);
    }
    loaded.index.entries = survivors;
    Ok(loaded)
}

fn read_exact_record_part(
    file: &mut std::fs::File,
    offset: u64,
    bytes: &mut [u8],
) -> StoreResult<bool> {
    file.seek(SeekFrom::Start(offset))?;
    match file.read_exact(bytes) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_v3_record(
    file: &mut std::fs::File,
    arena_len: u64,
    checkpoint_key: &str,
    entry: &OffsetEntry,
    next_write_seq: u64,
) -> StoreResult<bool> {
    if entry.write_seq == 0
        || entry.write_seq >= next_write_seq
        || entry.value_len > u32::MAX.into()
    {
        return Ok(false);
    }
    let mut encoded_header = [0; RECORD_HEADER_SIZE];
    if !read_exact_record_part(file, entry.offset, &mut encoded_header)? {
        return Ok(false);
    }
    let Ok(header) = RecordHeader::decode(&encoded_header) else {
        return Ok(false);
    };
    let Ok(record_size) = header.checked_record_size() else {
        return Ok(false);
    };
    let Ok(value_offset) = RecordHeader::value_offset(header.key_len.into()) else {
        return Ok(false);
    };
    if header.write_seq != entry.write_seq
        || header.key_len as usize != checkpoint_key.len()
        || u64::from(header.value_len) != entry.value_len
        || record_size != entry.len
        || value_offset != entry.value_offset
        || header.validate_extent(entry.offset, arena_len).is_err()
    {
        return Ok(false);
    }

    let mut key = vec![0; header.key_len as usize];
    let Some(key_offset) = entry.offset.checked_add(RECORD_HEADER_SIZE as u64) else {
        return Ok(false);
    };
    if !read_exact_record_part(file, key_offset, &mut key)? || key != checkpoint_key.as_bytes() {
        return Ok(false);
    }

    let key_end = RECORD_HEADER_SIZE as u64 + u64::from(header.key_len);
    let padding_len = value_offset - key_end;
    let mut padding = vec![0; padding_len as usize];
    let Some(padding_offset) = entry.offset.checked_add(key_end) else {
        return Ok(false);
    };
    if !read_exact_record_part(file, padding_offset, &mut padding)?
        || padding.iter().any(|byte| *byte != 0)
    {
        return Ok(false);
    }

    let Some(value_absolute) = entry.offset.checked_add(value_offset) else {
        return Ok(false);
    };
    if header.flags & RECORD_FLAG_HAS_CRC == 0 {
        return Ok(true);
    }
    file.seek(SeekFrom::Start(value_absolute))?;
    let mut checksum = Crc32c::new();
    checksum.extend(&header.encode_prefix());
    checksum.extend(&key);
    let mut remaining = u64::from(header.value_len);
    let mut chunk = vec![0; 1024 * 1024];
    while remaining > 0 {
        let read_len = remaining.min(chunk.len() as u64) as usize;
        match file.read_exact(&mut chunk[..read_len]) {
            Ok(()) => checksum.extend(&chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        remaining -= read_len as u64;
    }
    Ok(checksum.finish() == header.crc32)
}

fn key_watermarks(config: &OffsetAllocatorConfig) -> (usize, usize) {
    let high = (config.total_keys_limit as f64 * config.keys_high_ratio) as usize;
    let mut low = (config.total_keys_limit as f64 * config.keys_low_ratio) as usize;
    if high > 0 && low >= high {
        low = 1.max(high - 1);
    }
    (high, low)
}

fn normalize_loaded_index(index: &mut PersistedIndex, file_len: u64) -> bool {
    let occupied_end = index
        .entries
        .values()
        .map(|entry| entry.offset.saturating_add(entry.len))
        .max()
        .unwrap_or(0);
    // The durable high-water mark is advisory. Rebuild it from the arena and
    // validated survivors so a corrupt oversized checkpoint value cannot
    // permanently make the allocator report that its quota is exhausted.
    let safe_next_offset = occupied_end.max(file_len);
    let mut changed = safe_next_offset != index.next_offset;
    index.next_offset = safe_next_offset;

    let mut keys_by_fifo: Vec<_> = index.entries.keys().cloned().collect();
    keys_by_fifo.sort_by(|left, right| {
        let left_entry = &index.entries[left];
        let right_entry = &index.entries[right];
        left_entry
            .fifo_seq
            .cmp(&right_entry.fifo_seq)
            .then_with(|| left_entry.offset.cmp(&right_entry.offset))
            .then_with(|| left.cmp(right))
    });
    for (fifo_seq, key) in keys_by_fifo.into_iter().enumerate() {
        let fifo_seq = fifo_seq as u64;
        let entry = index.entries.get_mut(&key).unwrap();
        if entry.fifo_seq != fifo_seq {
            entry.fifo_seq = fifo_seq;
            changed = true;
        }
    }
    let repaired_next_fifo_seq = index.entries.len() as u64;
    if index.next_fifo_seq != repaired_next_fifo_seq {
        index.next_fifo_seq = repaired_next_fifo_seq;
        changed = true;
    }

    changed
}

fn rebuild_free_extents(index: &PersistedIndex, file_len: u64) -> BTreeMap<u64, u64> {
    let mut entries: Vec<_> = index.entries.values().collect();
    entries.sort_by_key(|entry| entry.offset);
    let mut free = BTreeMap::new();
    let mut cursor = 0;
    for entry in entries {
        if cursor < entry.offset {
            free.insert(cursor, entry.offset - cursor);
        }
        cursor = cursor.max(entry.offset.saturating_add(entry.len));
    }
    if cursor < file_len {
        free.insert(cursor, file_len - cursor);
    }
    free
}

fn allocate_extent(
    free_extents: &mut BTreeMap<u64, u64>,
    next_offset: u64,
    quota_bytes: u64,
    required: u64,
) -> Option<(u64, u64)> {
    let reusable = free_extents
        .iter()
        .find(|(_, len)| **len >= required)
        .map(|(offset, len)| (*offset, *len));
    if let Some((offset, len)) = reusable {
        free_extents.remove(&offset);
        if len > required {
            free_extents.insert(offset + required, len - required);
        }
        return Some((offset, next_offset));
    }
    if next_offset.saturating_add(required) > quota_bytes {
        return None;
    }
    Some((next_offset, next_offset.saturating_add(required)))
}

fn apply_pending_eviction(
    state: &mut OffsetState,
    pending: PendingOffsetEviction,
) -> Vec<(String, OffsetEntry)> {
    let mut removed = Vec::with_capacity(pending.victims.len());
    for (key, expected) in pending.victims {
        if state.index.entries.get(&key) != Some(&expected) {
            continue;
        }
        let entry = state.index.entries.remove(&key).unwrap();
        state.used_bytes = state.used_bytes.saturating_sub(entry.len);
        removed.push((key, entry));
    }
    removed
}

fn defer_pinned_extents(state: &mut OffsetState, entries: &[(String, OffsetEntry)]) {
    for (_, entry) in entries {
        let identity = ExtentIdentity::from(entry);
        if state.pinned_extents.contains_key(&identity) {
            state.deferred_free_extents.insert(identity, entry.clone());
        }
    }
}

fn record_tombstone(state: &mut OffsetState, key: &str) {
    if !state.tombstones.iter().any(|existing| existing == key) {
        state.tombstones.push(key.to_string());
    }
}

fn retire_extent(state: &mut OffsetState, entry: OffsetEntry) {
    let identity = ExtentIdentity::from(&entry);
    if state.pinned_extents.contains_key(&identity) {
        state.deferred_free_extents.insert(identity, entry);
    } else {
        insert_free_extent(&mut state.free_extents, entry.offset, entry.len);
    }
}

fn insert_free_extent(extents: &mut BTreeMap<u64, u64>, offset: u64, len: u64) {
    if len == 0 {
        return;
    }
    let mut start = offset;
    let mut length = len;
    if let Some((&previous_offset, &previous_len)) = extents.range(..offset).next_back() {
        if previous_offset.saturating_add(previous_len) == offset {
            start = previous_offset;
            length = length.saturating_add(previous_len);
            extents.remove(&previous_offset);
        }
    }
    if let Some((&next_offset, &next_len)) = extents.range(start..).next() {
        if start.saturating_add(length) == next_offset {
            length = length.saturating_add(next_len);
            extents.remove(&next_offset);
        }
    }
    extents.insert(start, length);
}

#[cfg(test)]
mod checkpoint_tests {
    use super::{
        CHECKPOINT_VERSION, CheckpointBoundary, OffsetEntry, PersistedIndex, checkpoint_envelope,
        checkpoint_tmp_path, load_checkpoint, load_persisted_index, remove_stale_checkpoint_tmp,
        write_checkpoint, write_checkpoint_state, write_checkpoint_with_hook,
    };
    use std::collections::HashMap;
    use std::io::Error;

    fn index(key: &str, offset: u64) -> PersistedIndex {
        PersistedIndex {
            entries: HashMap::from([(key.to_string(), OffsetEntry::legacy(offset, 4, offset))]),
            next_offset: offset + 4,
            next_fifo_seq: offset + 1,
        }
    }

    #[test]
    fn checkpoint_round_trip_verifies_payload_crc() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 8);

        write_checkpoint(&path, &expected).unwrap();

        assert_eq!(
            load_persisted_index(&path).unwrap().entries,
            expected.entries
        );
    }

    #[test]
    fn checkpoint_state_round_trips_sequence_and_future_tombstone_payload() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 8);
        let tombstones = vec!["evicted".to_string()];

        write_checkpoint_state(&path, &expected, 42, &tombstones).unwrap();
        let loaded = load_checkpoint(&path).unwrap();

        assert_eq!(loaded.index.entries, expected.entries);
        assert_eq!(loaded.next_write_seq, 42);
        assert_eq!(loaded.tombstones, tombstones);
    }

    #[test]
    fn unsupported_corrupt_and_malformed_checkpoints_fall_back_without_rewriting() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 8);

        let mut unsupported =
            serde_json::to_value(checkpoint_envelope(&expected).unwrap()).unwrap();
        unsupported["version"] = serde_json::json!(CHECKPOINT_VERSION + 1);
        let unsupported_bytes = serde_json::to_vec(&unsupported).unwrap();
        std::fs::write(&path, &unsupported_bytes).unwrap();
        assert!(load_persisted_index(&path).unwrap().entries.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), unsupported_bytes);

        let mut corrupt = serde_json::to_value(checkpoint_envelope(&expected).unwrap()).unwrap();
        corrupt["payload"]["index"]["next_offset"] = serde_json::json!(99);
        let corrupt_bytes = serde_json::to_vec(&corrupt).unwrap();
        std::fs::write(&path, &corrupt_bytes).unwrap();
        assert!(load_persisted_index(&path).unwrap().entries.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), corrupt_bytes);

        let malformed = b"{not-json".to_vec();
        std::fs::write(&path, &malformed).unwrap();
        assert!(load_persisted_index(&path).unwrap().entries.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), malformed);
    }

    #[test]
    fn legacy_raw_value_index_loads_without_touching_arena_or_index() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let arena = temp.path().join("offset_allocator.data");
        let legacy = br#"{"entries":{"a":{"offset":0,"len":4,"fifo_seq":3}},"next_offset":4,"next_fifo_seq":4}"#;
        std::fs::write(&path, legacy).unwrap();
        std::fs::write(&arena, b"aaaa").unwrap();

        let loaded = load_persisted_index(&path).unwrap();

        assert_eq!(loaded.entries["a"].offset, 0);
        assert_eq!(loaded.entries["a"].len, 4);
        assert_eq!(std::fs::read(&path).unwrap(), legacy);
        assert_eq!(std::fs::read(&arena).unwrap(), b"aaaa");
    }

    #[test]
    fn stale_checkpoint_tmp_is_removed_without_touching_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 0);
        write_checkpoint(&path, &expected).unwrap();
        std::fs::write(checkpoint_tmp_path(&path), b"partial").unwrap();

        remove_stale_checkpoint_tmp(&path).unwrap();

        assert!(!checkpoint_tmp_path(&path).exists());
        assert_eq!(
            load_persisted_index(&path).unwrap().entries,
            expected.entries
        );
    }

    #[test]
    fn checkpoint_boundaries_are_ordered_and_failures_leave_a_usable_checkpoint() {
        for failure in [
            CheckpointBoundary::Write,
            CheckpointBoundary::FileSync,
            CheckpointBoundary::Rename,
            CheckpointBoundary::DirectorySync,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("offset_allocator.index.json");
            let previous = index("previous", 0);
            let replacement = index("replacement", 8);
            write_checkpoint(&path, &previous).unwrap();
            let mut observed = Vec::new();

            let result = write_checkpoint_with_hook(&path, &replacement, |boundary| {
                observed.push(boundary);
                if boundary == failure {
                    Err(Error::other("injected checkpoint failure"))
                } else {
                    Ok(())
                }
            });

            assert!(result.is_err(), "{failure:?}");
            let recovered = load_persisted_index(&path).unwrap();
            if failure == CheckpointBoundary::DirectorySync {
                assert_eq!(recovered.entries, replacement.entries);
            } else {
                assert_eq!(recovered.entries, previous.entries);
            }
            let expected_prefix = [
                CheckpointBoundary::Write,
                CheckpointBoundary::FileSync,
                CheckpointBoundary::Rename,
                CheckpointBoundary::DirectorySync,
            ];
            let failure_index = expected_prefix
                .iter()
                .position(|boundary| *boundary == failure)
                .unwrap();
            assert_eq!(observed, expected_prefix[..=failure_index]);
            assert!(!checkpoint_tmp_path(&path).exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn transient_open_error_is_returned_without_truncating_arena() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let arena = temp.path().join("offset_allocator.data");
        symlink("offset_allocator.index.json", &path).unwrap();
        std::fs::write(&arena, b"recoverable-arena").unwrap();

        assert!(load_persisted_index(&path).is_err());
        assert_eq!(std::fs::read(&arena).unwrap(), b"recoverable-arena");
        assert!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_permission_error_is_returned_without_rewriting_files() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let arena = temp.path().join("offset_allocator.data");
        let checkpoint = b"recoverable-checkpoint";
        std::fs::write(&path, checkpoint).unwrap();
        std::fs::write(&arena, b"recoverable-arena").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0)).unwrap();

        let result = load_persisted_index(&path);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), checkpoint);
        assert_eq!(std::fs::read(&arena).unwrap(), b"recoverable-arena");
    }
}

#[cfg(test)]
mod record_primitive_tests {
    use super::{
        RECORD_FLAG_HAS_CRC, RECORD_HEADER_PREFIX_SIZE, RECORD_HEADER_SIZE, RecordHeader, crc32c,
    };

    #[test]
    fn crc32c_matches_castagnoli_check_vector_and_streaming() {
        assert_eq!(crc32c([b"123", b"456", b"789"]), 0xe306_9283);
        assert_eq!(crc32c([b"".as_slice()]), 0);
    }

    #[test]
    fn record_header_uses_explicit_little_endian_v3_layout() {
        let header = RecordHeader {
            key_len: 0x000f_0304,
            value_len: 0x0012_1314,
            write_seq: 0x2122_2324_2526_2728,
            flags: RECORD_FLAG_HAS_CRC,
            crc32: 0x3132_3334,
        };

        let encoded = header.encode();
        assert_eq!(RECORD_HEADER_SIZE, 24);
        assert_eq!(RECORD_HEADER_PREFIX_SIZE, 20);
        assert_eq!(
            encoded,
            [
                0x04, 0x03, 0x0f, 0x00, 0x14, 0x13, 0x12, 0x00, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23,
                0x22, 0x21, 0x01, 0x00, 0x00, 0x00, 0x34, 0x33, 0x32, 0x31,
            ]
        );
        assert_eq!(RecordHeader::decode(&encoded).unwrap(), header);
    }

    #[test]
    fn record_layout_aligns_values_to_four_kibibytes() {
        assert_eq!(RecordHeader::value_padding(4072).unwrap(), 0);
        assert_eq!(RecordHeader::value_padding(4073).unwrap(), 4095);
        assert_eq!(RecordHeader::value_offset(0).unwrap(), 4096);
        assert_eq!(RecordHeader::record_size(5, 1000).unwrap(), 5096);
        assert_eq!(RecordHeader::value_offset(5).unwrap() % 4096, 0);
    }

    #[test]
    fn record_header_rejects_truncation_unknown_flags_and_size_overflow() {
        assert!(RecordHeader::decode(&[0; RECORD_HEADER_SIZE - 1]).is_err());

        let mut unknown_flags = [0u8; RECORD_HEADER_SIZE];
        unknown_flags[16..20].copy_from_slice(&2u32.to_le_bytes());
        assert!(RecordHeader::decode(&unknown_flags).is_err());

        assert!(RecordHeader::value_offset(u64::MAX).is_err());
        assert!(RecordHeader::record_size(0, u64::MAX).is_err());
    }

    #[test]
    fn record_header_checks_extent_bounds_without_truncating_lengths() {
        let header = RecordHeader {
            key_len: 5,
            value_len: 1000,
            write_seq: 7,
            flags: 0,
            crc32: 0,
        };

        assert_eq!(header.checked_record_size().unwrap(), 5096);
        assert!(header.validate_extent(100, 5196).is_ok());
        assert!(header.validate_extent(100, 5195).is_err());
        assert!(header.validate_extent(u64::MAX - 10, u64::MAX).is_err());
    }

    #[test]
    fn record_header_caps_total_record_size_at_u32_max() {
        let max_value_len = u32::MAX - 4096;
        let max_header = RecordHeader {
            key_len: 0,
            value_len: max_value_len,
            write_seq: 7,
            flags: 0,
            crc32: 0,
        };
        let oversized_header = RecordHeader {
            value_len: max_value_len + 1,
            ..max_header
        };

        assert_eq!(
            RecordHeader::record_size(0, u64::from(max_value_len)).unwrap(),
            u64::from(u32::MAX)
        );
        assert_eq!(
            max_header.checked_record_size().unwrap(),
            u64::from(u32::MAX)
        );
        assert_eq!(
            RecordHeader::decode(&max_header.encode()).unwrap(),
            max_header
        );
        assert!(max_header.validate_extent(0, u64::from(u32::MAX)).is_ok());

        assert!(RecordHeader::record_size(0, u64::from(max_value_len) + 1).is_err());
        assert!(oversized_header.checked_record_size().is_err());
        assert!(RecordHeader::decode(&oversized_header.encode()).is_err());
        assert!(
            oversized_header
                .validate_extent(0, u64::from(u32::MAX) + 1)
                .is_err()
        );
    }

    #[test]
    fn record_crc_covers_prefix_key_and_value_but_not_crc_field_or_padding() {
        let mut header = RecordHeader {
            key_len: 3,
            value_len: 5,
            write_seq: 9,
            flags: RECORD_FLAG_HAS_CRC,
            crc32: 0,
        };
        let prefix = header.encode_prefix();
        let expected = crc32c([prefix.as_slice(), b"key", b"value"]);

        header.crc32 = expected;
        assert!(header.verify_crc(b"key", b"value").is_ok());
        assert!(header.verify_crc(b"key", b"Value").is_err());
    }
}

#[cfg(test)]
mod durable_recovery_tests {
    use super::super::OffsetPersistMode;
    use super::{
        DurableWriteBoundary, OffsetAllocatorConfig, OffsetAllocatorStorageBackend,
        OffsetEvictionPolicy, RECORD_FLAG_HAS_CRC, RECORD_HEADER_SIZE, RecordHeader,
        canonical_json_bytes, crc32c,
    };
    use mooncake_store_core::StoreError;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Barrier};

    fn config(root_dir: PathBuf, mode: OffsetPersistMode, crc: bool) -> OffsetAllocatorConfig {
        OffsetAllocatorConfig {
            root_dir,
            fsdir: "offset".to_string(),
            eviction_policy: OffsetEvictionPolicy::Fifo,
            quota_bytes: 64 * 1024,
            total_keys_limit: 64,
            high_ratio: 0.95,
            low_ratio: 0.80,
            keys_high_ratio: 0.95,
            keys_low_ratio: 0.80,
            max_evict_per_offload: 64,
            fallback_evict_batch: 2,
            persist_mode: mode,
            persist_interval_seconds: 60,
            enable_record_crc: crc,
        }
    }

    fn arena(root: &Path) -> PathBuf {
        root.join("offset/offset_allocator.data")
    }

    fn checkpoint(root: &Path) -> PathBuf {
        root.join("offset/offset_allocator.index.json")
    }

    fn checkpoint_json(root: &Path) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(checkpoint(root)).unwrap()).unwrap()
    }

    fn checkpoint_entry(root: &Path, key: &str) -> serde_json::Value {
        checkpoint_json(root)["payload"]["index"]["entries"][key].clone()
    }

    fn rewrite_checkpoint_payload(root: &Path, mutate: impl FnOnce(&mut serde_json::Value)) {
        let mut envelope = checkpoint_json(root);
        mutate(&mut envelope["payload"]);
        envelope["payload_crc32c"] = serde_json::json!(crc32c([canonical_json_bytes(
            &envelope["payload"]
        )
        .unwrap()]));
        std::fs::write(checkpoint(root), serde_json::to_vec(&envelope).unwrap()).unwrap();
    }

    fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_data().unwrap();
    }

    fn record_offset(root: &Path, key: &str) -> u64 {
        checkpoint_entry(root, key)["offset"].as_u64().unwrap()
    }

    fn value_offset(root: &Path, key: &str) -> u64 {
        let entry = checkpoint_entry(root, key);
        entry["offset"].as_u64().unwrap() + entry["value_offset"].as_u64().unwrap()
    }

    fn restart(root: &Path, mode: OffsetPersistMode, crc: bool) -> OffsetAllocatorStorageBackend {
        let backend = OffsetAllocatorStorageBackend::new(config(root.to_path_buf(), mode, crc));
        backend.init().unwrap();
        backend
    }

    #[test]
    fn strict_v3_record_and_nonzero_sequence_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("alpha", b"value-a").unwrap();
        drop(backend);

        let durable = checkpoint_json(temp.path());
        assert!(durable["payload"]["next_write_seq"].as_u64().unwrap() > 1);
        let entry = &durable["payload"]["index"]["entries"]["alpha"];
        assert_eq!(entry["format"], "v3");
        assert!(entry["write_seq"].as_u64().unwrap() > 0);

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert_eq!(restarted.read_object("alpha").unwrap(), b"value-a");
    }

    #[test]
    fn active_reader_keeps_deleted_extent_until_value_copy_finishes() {
        let temp = tempfile::tempdir().unwrap();
        let mut constrained = config(temp.path().to_path_buf(), OffsetPersistMode::Strict, true);
        constrained.quota_bytes = 4_100;
        let backend = Arc::new(OffsetAllocatorStorageBackend::new(constrained));
        backend.init().unwrap();
        backend.write_object("a", b"aaaa").unwrap();
        let original_offset = record_offset(temp.path(), "a");

        let looked_up = Arc::new(Barrier::new(3));
        let continue_first = Arc::new(Barrier::new(2));
        let continue_second = Arc::new(Barrier::new(2));
        let first_reader = {
            let backend = Arc::clone(&backend);
            let looked_up = Arc::clone(&looked_up);
            let continue_read = Arc::clone(&continue_first);
            std::thread::spawn(move || {
                backend.read_object_with_hook("a", || {
                    looked_up.wait();
                    continue_read.wait();
                })
            })
        };
        let second_reader = {
            let backend = Arc::clone(&backend);
            let looked_up = Arc::clone(&looked_up);
            let continue_read = Arc::clone(&continue_second);
            std::thread::spawn(move || {
                backend.read_object_with_hook("a", || {
                    looked_up.wait();
                    continue_read.wait();
                })
            })
        };

        looked_up.wait();
        backend.delete_object("a").unwrap();
        assert!(matches!(
            backend.write_object("b", b"bbbb"),
            Err(StoreError::NoAvailableHandle)
        ));
        continue_first.wait();
        assert_eq!(first_reader.join().unwrap().unwrap(), b"aaaa");
        assert!(matches!(
            backend.write_object("b", b"bbbb"),
            Err(StoreError::NoAvailableHandle)
        ));
        continue_second.wait();
        assert_eq!(second_reader.join().unwrap().unwrap(), b"aaaa");
        backend.write_object("b", b"bbbb").unwrap();
        assert_eq!(record_offset(temp.path(), "b"), original_offset);
    }

    #[test]
    fn active_reader_keeps_evicted_extent_out_of_the_reuse_pool() {
        let temp = tempfile::tempdir().unwrap();
        let mut constrained = config(temp.path().to_path_buf(), OffsetPersistMode::Strict, true);
        constrained.quota_bytes = 4_100;
        let backend = Arc::new(OffsetAllocatorStorageBackend::new(constrained));
        backend.init().unwrap();
        backend.write_object("a", b"aaaa").unwrap();
        let original_offset = record_offset(temp.path(), "a");
        let looked_up = Arc::new(Barrier::new(2));
        let continue_read = Arc::new(Barrier::new(2));
        let reader = {
            let backend = Arc::clone(&backend);
            let looked_up = Arc::clone(&looked_up);
            let continue_read = Arc::clone(&continue_read);
            std::thread::spawn(move || {
                backend.read_object_with_hook("a", || {
                    looked_up.wait();
                    continue_read.wait();
                })
            })
        };

        looked_up.wait();
        assert!(matches!(
            backend.write_object("b", b"bbbb"),
            Err(StoreError::NoAvailableHandle)
        ));
        assert!(!backend.exists("a"));
        continue_read.wait();
        assert_eq!(reader.join().unwrap().unwrap(), b"aaaa");
        backend.write_object("b", b"bbbb").unwrap();
        assert_eq!(record_offset(temp.path(), "b"), original_offset);
    }

    #[test]
    fn active_reader_keeps_open_file_alive_across_remove_all() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Arc::new(OffsetAllocatorStorageBackend::new(config(
            temp.path().to_path_buf(),
            OffsetPersistMode::Strict,
            true,
        )));
        backend.init().unwrap();
        backend.write_object("a", b"old-value").unwrap();

        let read_started = Arc::new(Barrier::new(2));
        let continue_read = Arc::new(Barrier::new(2));
        let reader = {
            let backend = Arc::clone(&backend);
            let read_started = Arc::clone(&read_started);
            let continue_read = Arc::clone(&continue_read);
            std::thread::spawn(move || {
                backend.read_object_with_hook("a", || {
                    read_started.wait();
                    continue_read.wait();
                })
            })
        };

        read_started.wait();
        assert_eq!(backend.remove_all().unwrap(), 1);
        assert!(matches!(
            backend.read_object("a"),
            Err(StoreError::KeyNotFound(key)) if key == "a"
        ));
        continue_read.wait();
        assert_eq!(reader.join().unwrap().unwrap(), b"old-value");
    }

    #[test]
    fn remove_all_detaches_deferred_extents_from_the_new_arena() {
        let temp = tempfile::tempdir().unwrap();
        let backend = OffsetAllocatorStorageBackend::new(config(
            temp.path().to_path_buf(),
            OffsetPersistMode::Strict,
            true,
        ));
        backend.init().unwrap();
        backend.write_object("a", b"old-a").unwrap();
        backend.write_object("b", b"old-b").unwrap();

        let first_a = backend.prepare_read("a").unwrap();
        let second_a = backend.prepare_read("a").unwrap();
        let old_b = backend.prepare_read("b").unwrap();
        backend.delete_object("a").unwrap();
        backend.write_object("b", b"replacement-b").unwrap();
        {
            let state = backend.state.lock();
            assert_eq!(state.pinned_extents.values().sum::<usize>(), 3);
            assert_eq!(state.deferred_free_extents.len(), 2);
        }

        assert_eq!(backend.remove_all().unwrap(), 1);
        backend.write_object("new", b"new-arena-value").unwrap();
        assert_eq!(record_offset(temp.path(), "new"), 0);

        drop(first_a);
        assert_eq!(
            backend.state.lock().pinned_extents.values().sum::<usize>(),
            2
        );
        drop(second_a);
        drop(old_b);
        {
            let state = backend.state.lock();
            assert!(state.pinned_extents.is_empty());
            assert!(state.deferred_free_extents.is_empty());
            assert!(state.free_extents.is_empty());
        }

        backend.write_object("later", b"later-value").unwrap();
        assert_ne!(record_offset(temp.path(), "later"), 0);
        assert_eq!(backend.read_object("new").unwrap(), b"new-arena-value");
    }

    #[test]
    fn failed_read_open_does_not_leak_an_extent_pin() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("a", b"value").unwrap();
        std::fs::remove_file(arena(temp.path())).unwrap();

        assert!(matches!(backend.read_object("a"), Err(StoreError::Io(_))));
        assert!(backend.state.lock().pinned_extents.is_empty());
    }

    #[test]
    fn active_reader_observes_old_value_during_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Arc::new(OffsetAllocatorStorageBackend::new(config(
            temp.path().to_path_buf(),
            OffsetPersistMode::Strict,
            true,
        )));
        backend.init().unwrap();
        backend.write_object("a", b"old-value").unwrap();

        let read_started = Arc::new(Barrier::new(2));
        let continue_read = Arc::new(Barrier::new(2));
        let reader = {
            let backend = Arc::clone(&backend);
            let read_started = Arc::clone(&read_started);
            let continue_read = Arc::clone(&continue_read);
            std::thread::spawn(move || {
                backend.read_object_with_hook("a", || {
                    read_started.wait();
                    continue_read.wait();
                })
            })
        };

        read_started.wait();
        backend.write_object("a", b"new-value").unwrap();
        assert_eq!(backend.read_object("a").unwrap(), b"new-value");
        continue_read.wait();
        assert_eq!(reader.join().unwrap().unwrap(), b"old-value");
    }

    #[test]
    fn write_key_limit_accepts_one_mib_and_rejects_the_next_byte() {
        const ONE_MIB: usize = 1024 * 1024;
        let temp = tempfile::tempdir().unwrap();
        let mut roomy = config(temp.path().to_path_buf(), OffsetPersistMode::Strict, true);
        roomy.quota_bytes = 4 * 1024 * 1024;
        let backend = OffsetAllocatorStorageBackend::new(roomy);
        backend.init().unwrap();
        let maximum = "m".repeat(ONE_MIB);
        backend.write_object(&maximum, b"value").unwrap();
        let arena_len = std::fs::metadata(arena(temp.path())).unwrap().len();

        let oversized = "x".repeat(ONE_MIB + 1);
        assert!(matches!(
            backend.write_object(&oversized, b"value"),
            Err(StoreError::InvalidParams(_))
        ));
        assert_eq!(
            std::fs::metadata(arena(temp.path())).unwrap().len(),
            arena_len
        );
    }

    #[test]
    fn recovery_rebuilds_next_offset_instead_of_trusting_checkpoint_high_water() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("a", b"aaaa").unwrap();
        let arena_len = std::fs::metadata(arena(temp.path())).unwrap().len();
        drop(backend);
        rewrite_checkpoint_payload(temp.path(), |payload| {
            payload["index"]["next_offset"] = serde_json::json!(u64::MAX - 1);
        });

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert_eq!(restarted.read_object("a").unwrap(), b"aaaa");
        restarted.write_object("b", b"bbbb").unwrap();
        assert_eq!(record_offset(temp.path(), "b"), arena_len);
    }

    #[test]
    fn recovery_rejects_extent_overflow_without_dropping_other_records() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("bad", b"bad-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        drop(backend);
        rewrite_checkpoint_payload(temp.path(), |payload| {
            payload["index"]["entries"]["bad"]["offset"] = serde_json::json!(u64::MAX - 10);
            payload["index"]["entries"]["bad"]["len"] = serde_json::json!(100);
        });

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("bad"));
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
    }

    #[test]
    fn recovery_rejects_header_key_over_one_mib_before_allocating_it() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("bad", b"bad-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        let bad = record_offset(temp.path(), "bad");
        drop(backend);
        write_at(
            &arena(temp.path()),
            bad,
            &((1024 * 1024 + 1) as u32).to_le_bytes(),
        );

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("bad"));
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
    }

    #[test]
    fn data_sync_failure_does_not_publish_record_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        let pending = backend.prepare_write("unsynced", 5).unwrap();

        let result = backend.commit_write_with_hook("unsynced", b"value", pending, |boundary| {
            if boundary == DurableWriteBoundary::DataSync {
                Err(StoreError::Internal(
                    "injected sync_data failure".to_string(),
                ))
            } else {
                Ok(())
            }
        });

        assert!(result.is_err());
        assert!(!backend.exists("unsynced"));
        assert!(!checkpoint(temp.path()).exists());
        drop(backend);
        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("unsynced"));
    }

    #[test]
    fn record_data_is_synced_before_checkpoint_publication() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        let pending = backend.prepare_write("ordered", 5).unwrap();
        let mut observed = Vec::new();

        let result = backend.commit_write_with_hook("ordered", b"value", pending, |boundary| {
            observed.push(boundary);
            if boundary == DurableWriteBoundary::CheckpointPublish {
                Err(StoreError::Internal(
                    "injected before checkpoint publication".to_string(),
                ))
            } else {
                Ok(())
            }
        });

        assert!(result.is_err());
        assert_eq!(
            observed,
            [
                DurableWriteBoundary::RecordWritten,
                DurableWriteBoundary::DataSync,
                DurableWriteBoundary::DataSynced,
                DurableWriteBoundary::CheckpointPublish,
            ]
        );
        assert!(arena(temp.path()).exists());
        assert!(!checkpoint(temp.path()).exists());
        assert!(backend.state.lock().dirty);
        backend.simulate_abrupt_exit();

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("ordered"));
    }

    #[test]
    fn abrupt_relaxed_restart_recovers_only_the_older_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Relaxed, true);
        backend.write_object("before", b"durable").unwrap();
        let older_checkpoint = std::fs::read(checkpoint(temp.path())).unwrap();
        backend.write_object("after", b"not-checkpointed").unwrap();
        std::fs::write(checkpoint(temp.path()), older_checkpoint).unwrap();
        backend.simulate_abrupt_exit();

        let restarted = restart(temp.path(), OffsetPersistMode::Relaxed, true);
        assert_eq!(restarted.read_object("before").unwrap(), b"durable");
        assert!(!restarted.exists("after"));
    }

    #[test]
    fn truncated_header_drops_only_the_affected_record() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("good", b"good-value").unwrap();
        backend.write_object("torn", b"torn-value").unwrap();
        let torn = record_offset(temp.path(), "torn");
        drop(backend);
        std::fs::OpenOptions::new()
            .write(true)
            .open(arena(temp.path()))
            .unwrap()
            .set_len(torn + RECORD_HEADER_SIZE as u64 - 1)
            .unwrap();

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
        assert!(!restarted.exists("torn"));
    }

    #[test]
    fn truncated_value_drops_only_the_affected_record() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("good", b"good-value").unwrap();
        backend.write_object("torn", b"torn-value").unwrap();
        let entry = checkpoint_entry(temp.path(), "torn");
        let end = entry["offset"].as_u64().unwrap() + entry["len"].as_u64().unwrap();
        drop(backend);
        std::fs::OpenOptions::new()
            .write(true)
            .open(arena(temp.path()))
            .unwrap()
            .set_len(end - 1)
            .unwrap();

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
        assert!(!restarted.exists("torn"));
    }

    #[test]
    fn unknown_record_flags_drop_only_the_affected_record() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("bad", b"bad-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        let bad = record_offset(temp.path(), "bad");
        drop(backend);
        write_at(&arena(temp.path()), bad + 16, &2u32.to_le_bytes());

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("bad"));
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
    }

    #[test]
    fn key_length_and_sequence_mismatches_are_isolated() {
        for (case, mutate) in [("key", 0u8), ("length", 1u8), ("sequence", 2u8)] {
            let temp = tempfile::tempdir().unwrap();
            let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
            backend.write_object("bad", b"bad-value").unwrap();
            backend.write_object("good", b"good-value").unwrap();
            let bad = record_offset(temp.path(), "bad");
            let entry = checkpoint_entry(temp.path(), "bad");
            drop(backend);
            match mutate {
                0 => write_at(&arena(temp.path()), bad + RECORD_HEADER_SIZE as u64, b"B"),
                1 => {
                    let changed = entry["value_len"].as_u64().unwrap() as u32 + 1;
                    write_at(&arena(temp.path()), bad + 4, &changed.to_le_bytes());
                }
                2 => {
                    let changed = entry["write_seq"].as_u64().unwrap() + 1;
                    write_at(&arena(temp.path()), bad + 8, &changed.to_le_bytes());
                }
                _ => unreachable!(),
            }

            let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
            assert!(!restarted.exists("bad"), "{case}");
            assert_eq!(
                restarted.read_object("good").unwrap(),
                b"good-value",
                "{case}"
            );
        }
    }

    #[test]
    fn crc_corruption_drops_one_key_and_rebuilds_allocator_state() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("bad", b"bad-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        let bad_record = checkpoint_entry(temp.path(), "bad");
        let bad_record_offset = bad_record["offset"].as_u64().unwrap();
        let bad_value = value_offset(temp.path(), "bad");
        let arena_len = std::fs::metadata(arena(temp.path())).unwrap().len();
        drop(backend);
        write_at(&arena(temp.path()), bad_value, b"B");

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("bad"));
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
        let used_after_recovery = restarted.space_usage().0;
        restarted.write_object("replacement", b"new-value").unwrap();
        assert!(restarted.space_usage().0 > used_after_recovery);
        assert_eq!(
            checkpoint_entry(temp.path(), "replacement")["offset"],
            bad_record_offset
        );
        assert_eq!(
            std::fs::metadata(arena(temp.path())).unwrap().len(),
            arena_len
        );
    }

    #[test]
    fn recovery_preserves_fifo_order_among_surviving_records() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("a", b"aaaa").unwrap();
        backend.write_object("b", b"bbbb").unwrap();
        backend.write_object("c", b"cccc").unwrap();
        let a_value = value_offset(temp.path(), "a");
        drop(backend);
        write_at(&arena(temp.path()), a_value, b"A");

        let mut constrained = config(temp.path().to_path_buf(), OffsetPersistMode::Strict, true);
        constrained.quota_bytes = 8_200;
        let restarted = OffsetAllocatorStorageBackend::new(constrained);
        restarted.init().unwrap();
        assert!(!restarted.exists("a"));
        assert_eq!(restarted.prepare_write("d", 4).unwrap().keys(), ["b", "c"]);
    }

    #[test]
    fn crc_disabled_round_trip_still_rejects_post_checkpoint_sequence() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, false);
        backend.write_object("stale", b"old-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        let stale = record_offset(temp.path(), "stale");
        let stale_entry = checkpoint_entry(temp.path(), "stale");
        let flags = std::fs::read(arena(temp.path())).unwrap()
            [stale as usize + 16..stale as usize + 20]
            .try_into()
            .map(u32::from_le_bytes)
            .unwrap();
        assert_eq!(flags & RECORD_FLAG_HAS_CRC, 0);
        drop(backend);

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, false);
        assert_eq!(restarted.read_object("stale").unwrap(), b"old-value");
        drop(restarted);

        let post_checkpoint_seq = stale_entry["write_seq"].as_u64().unwrap() + 10;
        write_at(
            &arena(temp.path()),
            stale + 8,
            &post_checkpoint_seq.to_le_bytes(),
        );
        let rejected = restart(temp.path(), OffsetPersistMode::Strict, false);
        assert!(!rejected.exists("stale"));
        assert_eq!(rejected.read_object("good").unwrap(), b"good-value");
    }

    #[test]
    fn checkpoint_entry_mismatch_is_not_partially_published() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("bad", b"bad-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        drop(backend);
        rewrite_checkpoint_payload(temp.path(), |payload| {
            payload["index"]["entries"]["bad"]["write_seq"] = serde_json::json!(u64::MAX);
        });

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("bad"));
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
    }

    #[test]
    fn unknown_entry_format_drops_only_that_checkpoint_entry() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("bad", b"bad-value").unwrap();
        backend.write_object("good", b"good-value").unwrap();
        drop(backend);
        rewrite_checkpoint_payload(temp.path(), |payload| {
            payload["index"]["entries"]["bad"]["format"] = serde_json::json!("future_v4");
        });

        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert!(!restarted.exists("bad"));
        assert_eq!(restarted.read_object("good").unwrap(), b"good-value");
    }

    #[test]
    fn mixed_legacy_raw_and_v3_records_survive_checkpoint_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("offset");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(arena(temp.path()), b"legacy-value").unwrap();
        std::fs::write(
            checkpoint(temp.path()),
            r#"{"entries":{"legacy":{"offset":0,"len":12,"fifo_seq":4}},"next_offset":12,"next_fifo_seq":5}"#,
        )
        .unwrap();

        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert_eq!(backend.read_object("legacy").unwrap(), b"legacy-value");
        backend.write_object("v3", b"new-value").unwrap();
        drop(backend);

        let durable = checkpoint_json(temp.path());
        assert_eq!(
            durable["payload"]["index"]["entries"]["legacy"]["format"],
            "legacy_raw"
        );
        assert_eq!(durable["payload"]["index"]["entries"]["v3"]["format"], "v3");
        let restarted = restart(temp.path(), OffsetPersistMode::Strict, true);
        assert_eq!(restarted.read_object("legacy").unwrap(), b"legacy-value");
        assert_eq!(restarted.read_object("v3").unwrap(), b"new-value");
    }

    #[test]
    fn encoded_record_layout_contains_zero_padding() {
        let temp = tempfile::tempdir().unwrap();
        let backend = restart(temp.path(), OffsetPersistMode::Strict, true);
        backend.write_object("key", b"value").unwrap();
        let entry = checkpoint_entry(temp.path(), "key");
        let bytes = std::fs::read(arena(temp.path())).unwrap();
        let start = entry["offset"].as_u64().unwrap() as usize;
        let value = start + entry["value_offset"].as_u64().unwrap() as usize;
        assert_eq!(
            &bytes[start + RECORD_HEADER_SIZE..start + RECORD_HEADER_SIZE + 3],
            b"key"
        );
        assert!(
            bytes[start + RECORD_HEADER_SIZE + 3..value]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert_eq!(&bytes[value..value + 5], b"value");

        let header = RecordHeader::decode(&bytes[start..start + RECORD_HEADER_SIZE]).unwrap();
        assert_eq!(header.key_len, 3);
        assert_eq!(header.value_len, 5);
    }
}

#[cfg(test)]
mod persistence_mode_tests {
    use super::super::OffsetPersistMode;
    use super::{
        DurableWriteBoundary, OffsetAllocatorConfig, OffsetAllocatorStorageBackend,
        OffsetEvictionPolicy, PendingOffsetEviction, RecordHeader, canonical_json_bytes,
        checkpoint_tmp_path, crc32c, load_checkpoint,
    };
    use mooncake_store_core::StoreError;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    fn config(root: &Path, mode: OffsetPersistMode) -> OffsetAllocatorConfig {
        OffsetAllocatorConfig {
            root_dir: root.to_path_buf(),
            fsdir: "offset".to_string(),
            eviction_policy: OffsetEvictionPolicy::Fifo,
            quota_bytes: 12_300,
            total_keys_limit: 16,
            high_ratio: 0.90,
            low_ratio: 0.80,
            keys_high_ratio: 0.90,
            keys_low_ratio: 0.80,
            max_evict_per_offload: 16,
            fallback_evict_batch: 2,
            persist_mode: mode,
            persist_interval_seconds: 60,
            enable_record_crc: true,
        }
    }

    fn backend(root: &Path, mode: OffsetPersistMode) -> OffsetAllocatorStorageBackend {
        let backend = OffsetAllocatorStorageBackend::new(config(root, mode));
        backend.init().unwrap();
        backend
    }

    fn checkpoint(root: &Path) -> PathBuf {
        root.join("offset/offset_allocator.index.json")
    }

    fn arena(root: &Path) -> PathBuf {
        root.join("offset/offset_allocator.data")
    }

    fn add_tombstone_to_checkpoint(root: &Path, key: &str) {
        let path = checkpoint(root);
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        envelope["payload"]["tombstones"] = serde_json::json!([key]);
        envelope["payload_crc32c"] = serde_json::json!(crc32c([canonical_json_bytes(
            &envelope["payload"]
        )
        .unwrap()]));
        std::fs::write(path, serde_json::to_vec(&envelope).unwrap()).unwrap();
    }

    fn backend_with_clock(
        root: &Path,
        mode: OffsetPersistMode,
        now_seconds: Arc<AtomicU64>,
    ) -> OffsetAllocatorStorageBackend {
        let mut roomy = config(root, mode);
        roomy.quota_bytes = 64 * 1024;
        let mut backend = OffsetAllocatorStorageBackend::new(roomy);
        backend.clock = Arc::new(move || Duration::from_secs(now_seconds.load(Ordering::SeqCst)));
        backend.init().unwrap();
        backend
    }

    #[test]
    fn disabled_init_discards_stale_checkpoint_and_arena_then_never_persists() {
        let temp = tempfile::tempdir().unwrap();
        {
            let strict = backend(temp.path(), OffsetPersistMode::Strict);
            strict.write_object("stale", b"old").unwrap();
        }
        std::fs::write(checkpoint_tmp_path(&checkpoint(temp.path())), b"partial").unwrap();

        let disabled = backend(temp.path(), OffsetPersistMode::Disabled);

        assert!(!disabled.exists("stale"));
        assert!(!checkpoint(temp.path()).exists());
        assert!(!checkpoint_tmp_path(&checkpoint(temp.path())).exists());
        assert!(!arena(temp.path()).exists());
        disabled.write_object("session-only", b"value").unwrap();
        assert!(!checkpoint(temp.path()).exists());
    }

    #[test]
    fn disabled_mutations_do_not_accumulate_persistence_bookkeeping() {
        let temp = tempfile::tempdir().unwrap();
        let disabled = backend(temp.path(), OffsetPersistMode::Disabled);
        disabled.write_object("delete-me", b"value").unwrap();
        disabled.delete_object("delete-me").unwrap();

        let state = disabled.state.lock();
        assert!(!state.dirty);
        assert!(state.tombstones.is_empty());
    }

    #[test]
    fn strict_checkpoint_failure_returns_error_and_next_mutation_retries_dirty_state() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        let pending = strict.prepare_write("first", 5).unwrap();

        let result = strict.commit_write_with_hook("first", b"value", pending, |boundary| {
            if boundary == DurableWriteBoundary::CheckpointPublish {
                Err(StoreError::Internal(
                    "injected checkpoint failure".to_string(),
                ))
            } else {
                Ok(())
            }
        });

        assert!(result.is_err());
        assert!(strict.exists("first"));
        assert!(strict.state.lock().dirty);
        strict.write_object("retry", b"value").unwrap();
        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(durable.index.entries.contains_key("first"));
        assert!(durable.index.entries.contains_key("retry"));
    }

    #[test]
    fn strict_empty_eviction_retry_flushes_prior_failed_tombstone() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        strict.write_object("victim", b"value").unwrap();
        std::fs::create_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        let pending = strict.prepare_watermark_eviction(0.01, 0.001).unwrap();

        assert!(strict.commit_eviction(pending).is_err());
        assert!(strict.state.lock().dirty);

        std::fs::remove_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        strict
            .commit_eviction(PendingOffsetEviction::default())
            .unwrap();
        assert!(!strict.state.lock().dirty);
        assert!(
            load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .tombstones
                .contains(&"victim".to_string())
        );
    }

    #[test]
    fn relaxed_due_checkpoint_reclaims_full_removed_generation_before_allocating() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let record_len = RecordHeader::record_size(1, 1).unwrap();
        let mut exact = config(temp.path(), OffsetPersistMode::Relaxed);
        exact.eviction_policy = OffsetEvictionPolicy::None;
        exact.quota_bytes = record_len * 2;
        let mut relaxed = OffsetAllocatorStorageBackend::new(exact);
        let test_now = Arc::clone(&now);
        relaxed.clock = Arc::new(move || Duration::from_secs(test_now.load(Ordering::SeqCst)));
        relaxed.init().unwrap();
        relaxed.write_object("a", b"1").unwrap();
        relaxed.write_object("b", b"2").unwrap();
        assert_eq!(
            std::fs::metadata(arena(temp.path())).unwrap().len(),
            record_len * 2
        );
        relaxed.remove_all().unwrap();

        now.store(160, Ordering::SeqCst);
        relaxed.write_object("c", b"3").unwrap();

        assert_eq!(relaxed.read_object("c").unwrap(), b"3");
        assert!(relaxed.state.lock().index.entries["c"].offset < record_len * 2);
        relaxed.simulate_abrupt_exit();

        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert_eq!(restarted.read_object("c").unwrap(), b"3");
        assert!(!restarted.exists("a"));
        assert!(!restarted.exists("b"));
    }

    #[test]
    fn relaxed_generation_rebuild_waits_for_last_read_pin() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let record_len = RecordHeader::record_size(1, 1).unwrap();
        let mut exact = config(temp.path(), OffsetPersistMode::Relaxed);
        exact.eviction_policy = OffsetEvictionPolicy::None;
        exact.quota_bytes = record_len;
        let mut relaxed = OffsetAllocatorStorageBackend::new(exact);
        let test_now = Arc::clone(&now);
        relaxed.clock = Arc::new(move || Duration::from_secs(test_now.load(Ordering::SeqCst)));
        relaxed.init().unwrap();
        relaxed.write_object("a", b"1").unwrap();
        let held_read = relaxed.prepare_read("a").unwrap();

        relaxed.remove_all().unwrap();
        now.store(160, Ordering::SeqCst);
        assert!(matches!(
            relaxed.write_object("b", b"2"),
            Err(StoreError::NoAvailableHandle)
        ));

        drop(held_read);
        relaxed.write_object("b", b"2").unwrap();
        relaxed.simulate_abrupt_exit();

        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert!(!restarted.exists("a"));
        assert_eq!(restarted.read_object("b").unwrap(), b"2");
    }

    #[test]
    fn relaxed_pending_rebuild_tracks_writes_multiple_pins_and_deferred_extents() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let record_len = RecordHeader::record_size(1, 1).unwrap();
        let mut exact = config(temp.path(), OffsetPersistMode::Relaxed);
        exact.eviction_policy = OffsetEvictionPolicy::None;
        exact.quota_bytes = record_len * 3;
        let mut relaxed = OffsetAllocatorStorageBackend::new(exact);
        let test_now = Arc::clone(&now);
        relaxed.clock = Arc::new(move || Duration::from_secs(test_now.load(Ordering::SeqCst)));
        relaxed.init().unwrap();
        relaxed.write_object("a", b"1").unwrap();
        relaxed.write_object("b", b"2").unwrap();
        let held_a = relaxed.prepare_read("a").unwrap();
        let held_b = relaxed.prepare_read("b").unwrap();

        relaxed.remove_all().unwrap();
        now.store(160, Ordering::SeqCst);
        // This mutation lands between the generation checkpoint and the last
        // old-generation unpin, so the pending rebuild length must expand.
        relaxed.write_object("c", b"3").unwrap();
        let held_c = relaxed.prepare_read("c").unwrap();
        relaxed.delete_object("c").unwrap();
        assert_eq!(relaxed.state.lock().deferred_free_extents.len(), 1);

        drop(held_a);
        drop(held_b);
        assert!(relaxed.state.lock().pending_rebuild_arena_len.is_some());
        drop(held_c);
        assert!(relaxed.state.lock().pending_rebuild_arena_len.is_none());
        assert!(relaxed.state.lock().deferred_free_extents.is_empty());

        now.store(220, Ordering::SeqCst);
        relaxed.write_object("d", b"4").unwrap();
        assert_eq!(relaxed.read_object("d").unwrap(), b"4");
        relaxed.simulate_abrupt_exit();

        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert!(!restarted.exists("a"));
        assert!(!restarted.exists("b"));
        assert!(!restarted.exists("c"));
        assert_eq!(restarted.read_object("d").unwrap(), b"4");
    }

    #[test]
    fn init_is_serialized_for_one_backend_instance() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Arc::new(OffsetAllocatorStorageBackend::new(config(
            temp.path(),
            OffsetPersistMode::Strict,
        )));
        let start = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let backend = Arc::clone(&backend);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    backend.init()
                })
            })
            .collect();

        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        backend.write_object("safe", b"value").unwrap();
        assert_eq!(backend.read_object("safe").unwrap(), b"value");
    }

    #[test]
    fn data_directory_has_one_live_backend_owner_and_failed_init_preserves_data() {
        let temp = tempfile::tempdir().unwrap();
        let first = backend(temp.path(), OffsetPersistMode::Strict);
        first.write_object("preserved", b"value").unwrap();
        let second =
            OffsetAllocatorStorageBackend::new(config(temp.path(), OffsetPersistMode::Strict));

        assert!(second.init().is_err());
        assert_eq!(first.read_object("preserved").unwrap(), b"value");
        drop(first);

        second.init().unwrap();
        assert_eq!(second.read_object("preserved").unwrap(), b"value");
    }

    #[test]
    fn strict_real_checkpoint_failure_keeps_dirty_state_for_retry() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        std::fs::create_dir_all(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();

        assert!(strict.write_object("first", b"value").is_err());
        assert!(strict.exists("first"));
        assert!(strict.state.lock().dirty);

        std::fs::remove_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        strict.write_object("retry", b"value").unwrap();
        assert!(!strict.state.lock().dirty);
        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(durable.index.entries.contains_key("first"));
        assert!(durable.index.entries.contains_key("retry"));
    }

    #[test]
    fn strict_delete_retry_of_already_removed_key_flushes_dirty_tombstone() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        strict.write_object("delete-me", b"value").unwrap();
        std::fs::create_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();

        assert!(strict.delete_object("delete-me").is_err());
        assert!(!strict.exists("delete-me"));
        assert!(strict.state.lock().dirty);

        std::fs::remove_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        strict.delete_object("delete-me").unwrap();
        assert!(!strict.state.lock().dirty);
        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(!durable.index.entries.contains_key("delete-me"));
        assert!(durable.tombstones.contains(&"delete-me".to_string()));
    }

    #[test]
    fn relaxed_first_mutation_checkpoints_but_second_waits_for_interval() {
        let temp = tempfile::tempdir().unwrap();
        let relaxed = backend(temp.path(), OffsetPersistMode::Relaxed);

        relaxed.write_object("first", b"one").unwrap();
        let first_checkpoint = std::fs::read(checkpoint(temp.path())).unwrap();
        relaxed.write_object("second", b"two").unwrap();

        assert_eq!(
            std::fs::read(checkpoint(temp.path())).unwrap(),
            first_checkpoint
        );
        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(durable.index.entries.contains_key("first"));
        assert!(!durable.index.entries.contains_key("second"));
        relaxed.simulate_abrupt_exit();
    }

    #[test]
    fn failed_relaxed_write_without_eviction_does_not_mark_metadata_dirty() {
        let temp = tempfile::tempdir().unwrap();
        let relaxed = backend(temp.path(), OffsetPersistMode::Relaxed);
        std::fs::create_dir(arena(temp.path())).unwrap();

        assert!(relaxed.write_object("never-published", b"value").is_err());
        assert!(!relaxed.exists("never-published"));
        assert!(!relaxed.state.lock().dirty);
        assert!(!checkpoint(temp.path()).exists());
    }

    #[test]
    fn relaxed_interval_uses_injected_monotonic_clock_without_sleeping() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let relaxed = backend_with_clock(temp.path(), OffsetPersistMode::Relaxed, Arc::clone(&now));
        relaxed.write_object("first", b"one").unwrap();
        relaxed.write_object("second", b"two").unwrap();

        now.store(159, Ordering::SeqCst);
        relaxed.write_object("third", b"three").unwrap();
        let before_due = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(!before_due.index.entries.contains_key("second"));
        assert!(!before_due.index.entries.contains_key("third"));

        now.store(160, Ordering::SeqCst);
        relaxed.write_object("fourth", b"four").unwrap();
        let due = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(due.index.entries.contains_key("second"));
        assert!(due.index.entries.contains_key("third"));
        assert!(due.index.entries.contains_key("fourth"));
        relaxed.simulate_abrupt_exit();
    }

    #[test]
    fn relaxed_periodic_failure_is_best_effort_and_retries_when_due_again() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let relaxed = backend_with_clock(temp.path(), OffsetPersistMode::Relaxed, Arc::clone(&now));
        relaxed.write_object("first", b"one").unwrap();
        std::fs::create_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        now.store(160, Ordering::SeqCst);

        relaxed.write_object("during-failure", b"two").unwrap();
        assert!(relaxed.exists("during-failure"));
        assert!(relaxed.state.lock().dirty);

        std::fs::remove_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        relaxed.write_object("retry", b"three").unwrap();
        assert!(!relaxed.state.lock().dirty);
        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(durable.index.entries.contains_key("during-failure"));
        assert!(durable.index.entries.contains_key("retry"));
        relaxed.simulate_abrupt_exit();
    }

    #[test]
    fn relaxed_drop_best_effort_checkpoints_dirty_state() {
        let temp = tempfile::tempdir().unwrap();
        let relaxed = backend(temp.path(), OffsetPersistMode::Relaxed);
        relaxed.write_object("first", b"one").unwrap();
        relaxed.write_object("drop-me", b"two").unwrap();
        assert!(
            !load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .index
                .entries
                .contains_key("drop-me")
        );

        drop(relaxed);

        assert!(
            load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .index
                .entries
                .contains_key("drop-me")
        );
    }

    #[test]
    fn relaxed_drop_waits_for_last_arc_without_deadlocking() {
        let temp = tempfile::tempdir().unwrap();
        let relaxed = Arc::new(backend(temp.path(), OffsetPersistMode::Relaxed));
        relaxed.write_object("first", b"one").unwrap();
        let first_checkpoint = std::fs::read(checkpoint(temp.path())).unwrap();
        relaxed.write_object("last-arc", b"two").unwrap();
        let survivor = Arc::clone(&relaxed);

        drop(relaxed);
        assert_eq!(
            std::fs::read(checkpoint(temp.path())).unwrap(),
            first_checkpoint
        );
        assert!(survivor.exists("last-arc"));

        drop(survivor);
        assert!(
            load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .index
                .entries
                .contains_key("last-arc")
        );
    }

    #[test]
    fn rollback_does_not_create_tombstone_but_finalized_eviction_does() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        strict.write_object("a", b"aaaa").unwrap();
        strict.write_object("b", b"bbbb").unwrap();

        let rolled_back = strict.prepare_write("c", 4).unwrap();
        assert_eq!(rolled_back.keys(), ["a"]);
        strict.rollback_eviction(rolled_back);
        assert!(strict.state.lock().tombstones.is_empty());

        let finalized = strict.prepare_watermark_eviction(0.60, 0.20).unwrap();
        assert!(!finalized.keys().is_empty());
        let victims = finalized.keys();
        strict.commit_eviction(finalized).unwrap();

        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        for victim in victims {
            assert!(durable.tombstones.contains(&victim));
            assert!(!durable.index.entries.contains_key(&victim));
        }
    }

    #[test]
    fn recovery_applies_finalized_tombstone_after_record_scan() {
        let temp = tempfile::tempdir().unwrap();
        {
            let strict = backend(temp.path(), OffsetPersistMode::Strict);
            strict.write_object("resurrected", b"old-value").unwrap();
        }
        add_tombstone_to_checkpoint(temp.path(), "resurrected");

        let restarted = backend(temp.path(), OffsetPersistMode::Strict);

        assert!(!restarted.exists("resurrected"));
    }

    #[test]
    fn abrupt_relaxed_eviction_has_documented_resurrection_window() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let relaxed = backend_with_clock(temp.path(), OffsetPersistMode::Relaxed, Arc::clone(&now));
        relaxed.write_object("checkpointed", b"old-value").unwrap();
        let pending = relaxed.prepare_watermark_eviction(0.01, 0.001).unwrap();
        assert_eq!(pending.keys(), ["checkpointed"]);
        relaxed.commit_eviction(pending).unwrap();
        assert!(!relaxed.exists("checkpointed"));
        relaxed.simulate_abrupt_exit();

        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert_eq!(restarted.read_object("checkpointed").unwrap(), b"old-value");
    }

    #[test]
    fn rewrite_clears_pending_tombstone_before_next_relaxed_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let relaxed = backend_with_clock(temp.path(), OffsetPersistMode::Relaxed, Arc::clone(&now));
        relaxed.write_object("a", b"aaaa").unwrap();
        relaxed.write_object("b", b"bbbb").unwrap();
        let pending = relaxed.prepare_watermark_eviction(0.10, 0.05).unwrap();
        assert!(pending.keys().contains(&"a".to_string()));
        relaxed.commit_eviction(pending).unwrap();
        assert!(relaxed.state.lock().tombstones.contains(&"a".to_string()));

        relaxed.write_object("a", b"rewritten").unwrap();
        assert!(!relaxed.state.lock().tombstones.contains(&"a".to_string()));
        now.store(160, Ordering::SeqCst);
        relaxed.write_object("trigger", b"checkpoint").unwrap();

        let durable = load_checkpoint(&checkpoint(temp.path())).unwrap();
        assert!(durable.index.entries.contains_key("a"));
        assert!(!durable.tombstones.contains(&"a".to_string()));
        relaxed.simulate_abrupt_exit();
        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert_eq!(restarted.read_object("a").unwrap(), b"rewritten");
    }

    #[test]
    fn strict_remove_all_publishes_empty_checkpoint_and_restarts_empty() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        strict.write_object("a", b"value").unwrap();

        assert_eq!(strict.remove_all().unwrap(), 1);
        assert!(
            load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .index
                .entries
                .is_empty()
        );
        drop(strict);

        let restarted = backend(temp.path(), OffsetPersistMode::Strict);
        assert!(!restarted.exists("a"));
    }

    #[test]
    fn strict_remove_all_checkpoint_failure_is_retryable() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        strict.write_object("a", b"value").unwrap();
        std::fs::create_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();

        assert!(strict.remove_all().is_err());
        assert!(!strict.exists("a"));
        assert!(strict.state.lock().dirty);

        std::fs::remove_dir(checkpoint_tmp_path(&checkpoint(temp.path()))).unwrap();
        assert_eq!(strict.remove_all().unwrap(), 0);
        assert!(!strict.state.lock().dirty);
        assert!(
            load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .index
                .entries
                .is_empty()
        );
    }

    #[test]
    fn strict_remove_all_unlink_failure_leaves_durable_empty_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let strict = backend(temp.path(), OffsetPersistMode::Strict);
        strict
            .write_object("must-not-revive", b"old-value")
            .unwrap();
        let stale_arena = std::fs::read(arena(temp.path())).unwrap();
        std::fs::remove_file(arena(temp.path())).unwrap();
        std::fs::create_dir(arena(temp.path())).unwrap();
        std::fs::write(arena(temp.path()).join("blocker"), b"x").unwrap();

        assert!(strict.remove_all().is_err());
        assert!(
            load_checkpoint(&checkpoint(temp.path()))
                .unwrap()
                .index
                .entries
                .is_empty()
        );
        drop(strict);
        std::fs::remove_dir_all(arena(temp.path())).unwrap();
        std::fs::write(arena(temp.path()), stale_arena).unwrap();

        let restarted = backend(temp.path(), OffsetPersistMode::Strict);
        assert!(!restarted.exists("must-not-revive"));
    }

    #[test]
    fn abrupt_relaxed_remove_all_recovers_last_checkpoint_until_interval() {
        let temp = tempfile::tempdir().unwrap();
        let relaxed = backend(temp.path(), OffsetPersistMode::Relaxed);
        relaxed.write_object("checkpointed", b"old-value").unwrap();
        let prior_checkpoint = std::fs::read(checkpoint(temp.path())).unwrap();

        assert_eq!(relaxed.remove_all().unwrap(), 1);
        assert!(!relaxed.exists("checkpointed"));
        assert_eq!(
            std::fs::read(checkpoint(temp.path())).unwrap(),
            prior_checkpoint
        );
        assert!(arena(temp.path()).exists());
        relaxed.simulate_abrupt_exit();

        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert_eq!(restarted.read_object("checkpointed").unwrap(), b"old-value");
    }

    #[test]
    fn relaxed_remove_all_appends_until_due_checkpoint_replaces_generation() {
        let temp = tempfile::tempdir().unwrap();
        let now = Arc::new(AtomicU64::new(100));
        let relaxed = backend_with_clock(temp.path(), OffsetPersistMode::Relaxed, Arc::clone(&now));
        relaxed.write_object("old", b"old-value").unwrap();
        let old_arena_len = std::fs::metadata(arena(temp.path())).unwrap().len();
        relaxed.remove_all().unwrap();
        relaxed.write_object("new", b"new-value").unwrap();
        assert!(relaxed.state.lock().index.entries["new"].offset >= old_arena_len);

        now.store(160, Ordering::SeqCst);
        relaxed.write_object("trigger", b"checkpoint").unwrap();
        relaxed.simulate_abrupt_exit();

        let restarted = backend(temp.path(), OffsetPersistMode::Relaxed);
        assert!(!restarted.exists("old"));
        assert_eq!(restarted.read_object("new").unwrap(), b"new-value");
        assert_eq!(restarted.read_object("trigger").unwrap(), b"checkpoint");
    }
}
