use super::*;
use std::io::Write;

const LOCAL_OPLOG_V2_MAGIC: &[u8; 8] = b"MCOPLG02";
const LOCAL_OPLOG_V2_FRAME_HEADER_LEN: usize = 24;
const LOCAL_OPLOG_V1_FRAME_HEADER_LEN: usize = 8;

pub struct LocalFsOpLogStore {
    /// Root directory for segment files.
    /// 分段文件的根目录。
    dir: PathBuf,
    /// Maximum entries per segment file.
    max_entries_per_segment: usize,
    /// Entries accumulated in the current (not yet flushed) segment.
    buffer: Vec<OpLogRecord>,
    /// Monotonically increasing sequence counter.
    last_seq: u64,
    /// Starting sequence of the current segment.
    current_segment_seq: u64,
    /// A failed persistence attempt may have reached an indeterminate point.
    /// Refuse later appends in the same process instead of committing a record
    /// whose caller already observed an error.
    poisoned: Option<String>,
}

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), HaError> {
    if snapshot_id.is_empty()
        || snapshot_id.contains('/')
        || snapshot_id.contains("..")
        || snapshot_id.as_bytes().contains(&0)
    {
        return Err(HaError::InvalidBackend(format!(
            "invalid snapshot id: {snapshot_id}"
        )));
    }
    Ok(())
}

impl LocalFsOpLogStore {
    /// Create a local filesystem oplog store.
    /// 创建本地文件 oplog store。
    /// - Creates the directory if it doesn't exist. / 创建目录（如不存在）
    /// - Recovers last_seq from existing segment files. / 从已有分段文件恢复 last_seq
    pub fn new(dir: &Path, max_entries_per_segment: usize) -> Result<Self, HaError> {
        fs::create_dir_all(dir)
            .map_err(|e| HaError::InvalidBackend(format!("oplog dir create: {e}")))?;

        let dir_buf = dir.to_path_buf();

        let mut store = Self {
            dir: dir_buf,
            max_entries_per_segment: max_entries_per_segment.max(1),
            buffer: Vec::new(),
            last_seq: 0,
            current_segment_seq: 0,
            poisoned: None,
        };
        // Recover latest sequence from existing segment files
        store.recover()?;
        Ok(store)
    }

    /// Recover last_seq from existing segment files: scan for the highest-
    /// numbered segment file and parse its last record's seq.
    /// 从磁盘恢复 last_seq：扫描编号最大的分段文件，解析其中最后一条记录的 seq。
    fn recover(&mut self) -> Result<(), HaError> {
        let persisted_latest = self.read_persisted_latest()?;
        let mut segments: Vec<u64> = self.list_segment_files()?;
        segments.sort();
        let mut previous_seq: Option<u64> = None;
        let mut saw_v2_segment = false;
        for start_seq in segments {
            let (entries, is_v2) = self.read_segment_with_format(start_seq)?;
            saw_v2_segment |= is_v2;
            if let Some(previous_seq) = previous_seq {
                if previous_seq.checked_add(1) != Some(entries[0].seq) {
                    return Err(HaError::InvalidBackend(format!(
                        "oplog segment sequence gap: previous={previous_seq}, next={}",
                        entries[0].seq
                    )));
                }
            }
            previous_seq = entries.last().map(|entry| entry.seq);
            self.current_segment_seq = start_seq;
        }
        match (persisted_latest, previous_seq) {
            (Some(persisted_latest), Some(segment_max)) if segment_max > persisted_latest => {
                return Err(HaError::InvalidBackend(format!(
                    "oplog segment exceeds committed latest pointer: segment_max={segment_max}, latest={persisted_latest}"
                )));
            }
            (Some(persisted_latest), _) => self.last_seq = persisted_latest,
            (None, _) if saw_v2_segment => {
                return Err(HaError::InvalidBackend(
                    "version 2 oplog segments exist without a committed latest pointer".into(),
                ));
            }
            (None, Some(segment_max)) => self.last_seq = segment_max,
            (None, None) => {}
        }
        Ok(())
    }

    /// List segment files in the oplog directory, returning their start_seq.
    /// 列出 oplog 目录中的分段文件，返回其 start_seq。
    fn list_segment_files(&self) -> Result<Vec<u64>, HaError> {
        let mut segments = Vec::new();
        let dir_entries = fs::read_dir(&self.dir)
            .map_err(|e| HaError::InvalidBackend(format!("oplog read dir: {e}")))?;
        for entry in dir_entries {
            let entry =
                entry.map_err(|e| HaError::InvalidBackend(format!("oplog dir entry: {e}")))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str
                .strip_prefix("oplog_")
                .and_then(|s| s.strip_suffix(".bin"))
            {
                let seq = rest.parse::<u64>().map_err(|error| {
                    HaError::InvalidBackend(format!(
                        "oplog segment filename has invalid sequence {name_str:?}: {error}"
                    ))
                })?;
                segments.push(seq);
            }
        }
        Ok(segments)
    }

    /// Build the path for a segment file given its start sequence number.
    /// 根据起始序列号构建分段文件的路径。
    fn segment_path(&self, start_seq: u64) -> PathBuf {
        self.dir.join(format!("oplog_{:020}.bin", start_seq))
    }

    fn latest_path(&self) -> PathBuf {
        self.dir.join("latest")
    }

    fn snapshots_dir(&self) -> PathBuf {
        self.dir.join("snapshots")
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.snapshots_dir().join(snapshot_id)
    }

    fn ensure_not_poisoned(&self) -> Result<(), HaError> {
        if let Some(reason) = &self.poisoned {
            return Err(HaError::InvalidBackend(format!(
                "local oplog is poisoned after a persistence failure: {reason}"
            )));
        }
        Ok(())
    }

    fn read_persisted_latest(&self) -> Result<Option<u64>, HaError> {
        match fs::read_to_string(self.latest_path()) {
            Ok(value) => value.trim().parse::<u64>().map(Some).map_err(|error| {
                HaError::InvalidBackend(format!("oplog latest sequence is malformed: {error}"))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(HaError::InvalidBackend(format!(
                "oplog read latest: {error}"
            ))),
        }
    }

    fn read_segment(&self, start_seq: u64) -> Result<Vec<OpLogRecord>, HaError> {
        self.read_segment_with_format(start_seq)
            .map(|(entries, _)| entries)
    }

    fn read_segment_with_format(
        &self,
        start_seq: u64,
    ) -> Result<(Vec<OpLogRecord>, bool), HaError> {
        let path = self.segment_path(start_seq);
        let data = fs::read(&path)
            .map_err(|error| HaError::InvalidBackend(format!("oplog read segment: {error}")))?;
        let is_v2 = data.starts_with(LOCAL_OPLOG_V2_MAGIC);
        let entries = Self::parse_entries(&data)?;
        let Some(first) = entries.first() else {
            return Err(HaError::InvalidBackend(format!(
                "oplog segment is empty: {}",
                path.display()
            )));
        };
        if first.seq != start_seq {
            return Err(HaError::InvalidBackend(format!(
                "oplog segment filename/content mismatch: file_start={start_seq}, first_seq={}",
                first.seq
            )));
        }
        for pair in entries.windows(2) {
            if pair[0].seq.checked_add(1) != Some(pair[1].seq) {
                return Err(HaError::InvalidBackend(format!(
                    "oplog segment contains a sequence gap: previous={}, next={}",
                    pair[0].seq, pair[1].seq
                )));
            }
        }
        Ok((entries, is_v2))
    }

    /// Flush entries to a segment file atomically and durably.
    /// 将 buffer 原子、持久地写入分段文件。
    ///
    /// Writes to .tmp, renames to final, then fsyncs the parent directory for durability.
    /// 写入 .tmp 文件后 rename 到最终文件名，并 fsync 父目录保证持久性。
    fn flush_inner(dir: &Path, entries: &[OpLogRecord]) -> Result<(), HaError> {
        if entries.is_empty() {
            return Ok(());
        }
        for pair in entries.windows(2) {
            if pair[0].seq.checked_add(1) != Some(pair[1].seq) {
                return Err(HaError::InvalidBackend(format!(
                    "refusing to flush non-contiguous oplog entries: previous={}, next={}",
                    pair[0].seq, pair[1].seq
                )));
            }
        }
        let start_seq = entries[0].seq;
        let tmp_path = dir.join(format!("oplog_{:020}.tmp", start_seq));
        let final_path = dir.join(format!("oplog_{:020}.bin", start_seq));

        // V2 keeps the full u64 sequence/view and detects torn or corrupt
        // payloads. Legacy v1 files remain readable during upgrade.
        let mut data = Vec::with_capacity(entries.len() * 128);
        data.extend_from_slice(LOCAL_OPLOG_V2_MAGIC);
        for entry in entries {
            let payload = entry.payload.as_bytes();
            if payload.len() > MAX_PAYLOAD_SIZE {
                return Err(HaError::InvalidBackend(format!(
                    "oplog record payload exceeds maximum size at seq={}",
                    entry.seq
                )));
            }
            let payload_len = u32::try_from(payload.len()).map_err(|_| {
                HaError::InvalidBackend(format!(
                    "oplog record payload length does not fit u32 at seq={}",
                    entry.seq
                ))
            })?;
            data.extend_from_slice(&entry.seq.to_le_bytes());
            data.extend_from_slice(&entry.producer_view_version.to_le_bytes());
            data.extend_from_slice(&payload_len.to_le_bytes());
            data.extend_from_slice(&xxh32(payload, 0).to_le_bytes());
            data.extend_from_slice(payload);
        }

        let mut file = fs::File::create(&tmp_path)
            .map_err(|e| HaError::InvalidBackend(format!("oplog create segment: {e}")))?;
        file.write_all(&data)
            .map_err(|e| HaError::InvalidBackend(format!("oplog write segment: {e}")))?;
        file.sync_all()
            .map_err(|e| HaError::InvalidBackend(format!("oplog sync segment: {e}")))?;
        drop(file);
        fs::rename(&tmp_path, &final_path)
            .map_err(|e| HaError::InvalidBackend(format!("oplog rename segment: {e}")))?;
        fs::File::open(dir)
            .and_then(|file| file.sync_all())
            .map_err(|e| HaError::InvalidBackend(format!("oplog sync directory: {e}")))?;
        Ok(())
    }

    /// Write the current buffer to a segment file atomically.
    pub(super) fn flush(&mut self) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        let result = self.flush_unpoisoned();
        if let Err(error) = &result {
            self.poisoned = Some(error.to_string());
        }
        result
    }

    fn flush_unpoisoned(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        Self::flush_inner(&self.dir, &self.buffer)?;
        self.current_segment_seq = self.buffer.first().map(|e| e.seq).unwrap_or(0);
        self.write_latest(self.last_seq)?;
        self.buffer.clear();
        Ok(())
    }

    fn write_latest(&self, sequence_id: u64) -> Result<(), HaError> {
        let tmp_path = self.dir.join("latest.tmp");
        let mut file = fs::File::create(&tmp_path)
            .map_err(|e| HaError::InvalidBackend(format!("oplog create latest: {e}")))?;
        file.write_all(sequence_id.to_string().as_bytes())
            .map_err(|e| HaError::InvalidBackend(format!("oplog write latest: {e}")))?;
        file.sync_all()
            .map_err(|e| HaError::InvalidBackend(format!("oplog sync latest: {e}")))?;
        drop(file);
        fs::rename(&tmp_path, self.latest_path())
            .map_err(|e| HaError::InvalidBackend(format!("oplog rename latest: {e}")))?;
        fs::File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|e| HaError::InvalidBackend(format!("oplog sync directory: {e}")))?;
        Ok(())
    }

    /// Parse the current v2 format or the legacy v1 format. Any incomplete or
    /// corrupt frame is rejected so recovery never silently accepts a prefix.
    fn parse_entries(data: &[u8]) -> Result<Vec<OpLogRecord>, HaError> {
        if data.starts_with(LOCAL_OPLOG_V2_MAGIC) {
            return Self::parse_v2_entries(&data[LOCAL_OPLOG_V2_MAGIC.len()..]);
        }
        Self::parse_v1_entries(data)
    }

    fn parse_v2_entries(data: &[u8]) -> Result<Vec<OpLogRecord>, HaError> {
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let header_end = offset
                .checked_add(LOCAL_OPLOG_V2_FRAME_HEADER_LEN)
                .ok_or_else(|| HaError::InvalidBackend("oplog v2 frame offset overflow".into()))?;
            if header_end > data.len() {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v2 frame header is truncated at offset={offset}"
                )));
            }
            let seq = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            let producer_view_version =
                u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());
            let payload_len =
                u32::from_le_bytes(data[offset + 16..offset + 20].try_into().unwrap()) as usize;
            let expected_checksum =
                u32::from_le_bytes(data[offset + 20..header_end].try_into().unwrap());
            if payload_len > MAX_PAYLOAD_SIZE {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v2 payload exceeds maximum size at seq={seq}"
                )));
            }
            let payload_end = header_end.checked_add(payload_len).ok_or_else(|| {
                HaError::InvalidBackend(format!("oplog v2 payload offset overflows at seq={seq}"))
            })?;
            if payload_end > data.len() {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v2 payload is truncated at seq={seq}"
                )));
            }
            let payload_bytes = &data[header_end..payload_end];
            if xxh32(payload_bytes, 0) != expected_checksum {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v2 payload checksum mismatch at seq={seq}"
                )));
            }
            let payload = String::from_utf8(payload_bytes.to_vec()).map_err(|error| {
                HaError::InvalidBackend(format!(
                    "oplog record payload is invalid UTF-8 at seq={seq}: {error}"
                ))
            })?;
            entries.push(OpLogRecord {
                seq,
                producer_view_version,
                payload,
            });
            offset = payload_end;
        }
        Ok(entries)
    }

    fn parse_v1_entries(data: &[u8]) -> Result<Vec<OpLogRecord>, HaError> {
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let header_end = offset
                .checked_add(LOCAL_OPLOG_V1_FRAME_HEADER_LEN)
                .ok_or_else(|| HaError::InvalidBackend("oplog v1 frame offset overflow".into()))?;
            if header_end > data.len() {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v1 frame header is truncated at offset={offset}"
                )));
            }
            let seq = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as u64;
            let payload_len =
                u32::from_le_bytes(data[offset + 4..header_end].try_into().unwrap()) as usize;
            if payload_len > MAX_PAYLOAD_SIZE {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v1 payload exceeds maximum size at seq={seq}"
                )));
            }
            let payload_end = header_end.checked_add(payload_len).ok_or_else(|| {
                HaError::InvalidBackend(format!("oplog v1 payload offset overflows at seq={seq}"))
            })?;
            if payload_end > data.len() {
                return Err(HaError::InvalidBackend(format!(
                    "oplog v1 payload is truncated at seq={seq}"
                )));
            }
            let payload =
                String::from_utf8(data[header_end..payload_end].to_vec()).map_err(|error| {
                    HaError::InvalidBackend(format!(
                        "oplog record payload is invalid UTF-8 at seq={seq}: {error}"
                    ))
                })?;
            entries.push(OpLogRecord {
                seq,
                producer_view_version: 0,
                payload,
            });
            offset = payload_end;
        }
        Ok(entries)
    }
}

impl OpLogStore for LocalFsOpLogStore {
    /// Append an entry and synchronously flush a full segment. A successful
    /// threshold flush means the segment and latest pointer are durable.
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.ensure_not_poisoned()?;
        self.last_seq = self.last_seq.checked_add(1).ok_or_else(|| {
            HaError::InvalidBackend("oplog sequence exhausted at u64::MAX".into())
        })?;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        if self.buffer.len() >= self.max_entries_per_segment {
            self.flush()?;
        }
        Ok(self.last_seq)
    }

    /// Read entries starting from since_seq.
    /// Reads both on-disk segment files and in-memory (unflushed) buffer.
    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        self.ensure_not_poisoned()?;
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let segments = self.list_segment_files()?;
        let mut all_entries = Vec::new();

        let mut sorted_segs = segments;
        sorted_segs.sort();
        for start_seq in sorted_segs {
            for entry in self.read_segment(start_seq)? {
                if entry.seq >= since_seq {
                    all_entries.push(entry);
                }
            }
        }

        for entry in &self.buffer {
            if entry.seq >= since_seq && !all_entries.iter().any(|item| item.seq == entry.seq) {
                all_entries.push(entry.clone());
            }
        }

        all_entries.sort_by_key(|entry| entry.seq);
        all_entries.truncate(max_count);
        Ok(all_entries)
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.ensure_not_poisoned()?;
        let mut max_seq = self.last_seq;
        for start_seq in self.list_segment_files()? {
            if let Some(entry) = self.read_segment(start_seq)?.last() {
                max_seq = max_seq.max(entry.seq);
            }
        }
        for entry in &self.buffer {
            max_seq = max_seq.max(entry.seq);
        }
        Ok(max_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        if sequence_id < self.last_seq || !self.buffer.is_empty() {
            return Err(HaError::InvalidBackend(
                "oplog latest sequence cannot move backwards or bypass buffered entries".into(),
            ));
        }
        if let Err(error) = self.write_latest(sequence_id) {
            self.poisoned = Some(error.to_string());
            return Err(error);
        }
        self.last_seq = sequence_id;
        Ok(())
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        validate_snapshot_id(snapshot_id)?;
        fs::create_dir_all(self.snapshots_dir())
            .map_err(|e| HaError::InvalidBackend(format!("oplog create snapshots dir: {e}")))?;
        let tmp_path = self.snapshots_dir().join(format!("{snapshot_id}.tmp"));
        let mut file = fs::File::create(&tmp_path)
            .map_err(|e| HaError::InvalidBackend(format!("oplog create snapshot seq: {e}")))?;
        file.write_all(sequence_id.to_string().as_bytes())
            .map_err(|e| HaError::InvalidBackend(format!("oplog write snapshot seq: {e}")))?;
        file.sync_all()
            .map_err(|e| HaError::InvalidBackend(format!("oplog sync snapshot seq: {e}")))?;
        drop(file);
        fs::rename(&tmp_path, self.snapshot_path(snapshot_id))
            .map_err(|e| HaError::InvalidBackend(format!("oplog rename snapshot seq: {e}")))?;
        fs::File::open(self.snapshots_dir())
            .and_then(|file| file.sync_all())
            .map_err(|e| HaError::InvalidBackend(format!("oplog sync snapshots dir: {e}")))?;
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.ensure_not_poisoned()?;
        validate_snapshot_id(snapshot_id)?;
        let value = fs::read_to_string(self.snapshot_path(snapshot_id))
            .map_err(|e| HaError::InvalidBackend(format!("oplog read snapshot seq: {e}")))?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|e| HaError::InvalidBackend(format!("oplog parse snapshot seq: {e}")))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        let result: Result<(), HaError> = (|| {
            self.buffer.retain(|entry| entry.seq >= before_sequence_id);
            for start_seq in self.list_segment_files()? {
                let path = self.segment_path(start_seq);
                let entries = self.read_segment(start_seq)?;
                let retained = entries
                    .iter()
                    .filter(|entry| entry.seq >= before_sequence_id)
                    .cloned()
                    .collect::<Vec<_>>();
                if retained.is_empty() {
                    fs::remove_file(&path).map_err(|e| {
                        HaError::InvalidBackend(format!("oplog cleanup segment: {e}"))
                    })?;
                } else if retained.len() != entries.len() {
                    Self::flush_inner(&self.dir, &retained)?;
                    fs::remove_file(&path).map_err(|e| {
                        HaError::InvalidBackend(format!("oplog rewrite segment: {e}"))
                    })?;
                }
            }
            fs::File::open(&self.dir)
                .and_then(|file| file.sync_all())
                .map_err(|e| HaError::InvalidBackend(format!("oplog sync directory: {e}")))?;
            self.write_latest(self.last_seq)?;
            Ok(())
        })();
        if let Err(error) = &result {
            self.poisoned = Some(error.to_string());
        }
        result
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        self.flush()
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records
            .last()
            .map(|r| r.seq.saturating_add(1))
            .unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}
