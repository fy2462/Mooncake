use super::*;

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
    /// Channel to send full buffers to the background flush thread.
    pub(super) flush_tx: mpsc::Sender<Vec<OpLogRecord>>,
    /// Background flush thread handle — joined on drop.
    _flush_handle: Option<std::thread::JoinHandle<()>>,
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
    /// - Spawns a background flush thread via mpsc channel. / 启动后台 flush 线程（通过 mpsc channel 接收待刷盘的 buffer）
    /// - Recovers last_seq from existing segment files. / 从已有分段文件恢复 last_seq
    pub fn new(dir: &Path, max_entries_per_segment: usize) -> Result<Self, HaError> {
        fs::create_dir_all(dir)
            .map_err(|e| HaError::InvalidBackend(format!("oplog dir create: {e}")))?;

        let dir_buf = dir.to_path_buf();
        let (flush_tx, flush_rx) = mpsc::channel::<Vec<OpLogRecord>>();

        // Background thread: asynchronously writes full buffers to disk.
        // 后台线程：异步将满 buffer 写入磁盘，不阻塞 append 调用方。
        let flush_dir = dir_buf.clone();
        let flush_handle = std::thread::spawn(move || {
            for entries in flush_rx {
                if let Err(e) = Self::flush_inner(&flush_dir, &entries) {
                    warn!("oplog background flush failed: {e}");
                }
            }
        });

        let mut store = Self {
            dir: dir_buf,
            max_entries_per_segment: max_entries_per_segment.max(1000),
            buffer: Vec::new(),
            last_seq: 0,
            current_segment_seq: 0,
            flush_tx,
            _flush_handle: Some(flush_handle),
        };
        // Recover latest sequence from existing segment files
        store.recover()?;
        Ok(store)
    }

    /// Recover last_seq from existing segment files: scan for the highest-
    /// numbered segment file and parse its last record's seq.
    /// 从磁盘恢复 last_seq：扫描编号最大的分段文件，解析其中最后一条记录的 seq。
    fn recover(&mut self) -> Result<(), HaError> {
        let persisted_latest = fs::read_to_string(self.latest_path())
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok());
        let mut segments: Vec<u64> = self.list_segment_files()?;
        segments.sort();
        if let Some(&highest_start) = segments.last() {
            let path = self.segment_path(highest_start);
            let data = fs::read(&path)
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            let entries = Self::parse_entries(&data);
            if let Some(last) = entries.last() {
                self.last_seq = last.seq;
                self.current_segment_seq = highest_start;
            }
        }
        if let Some(persisted_latest) = persisted_latest {
            self.last_seq = self.last_seq.max(persisted_latest);
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
                if let Ok(seq) = rest.parse::<u64>() {
                    segments.push(seq);
                }
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

    /// Flush entries to a segment file atomically (synchronous — used by both
    /// the background thread and explicit flush() calls).
    /// 将 buffer 原子写入分段文件（同步操作，由后台线程和显式 flush() 调用）。
    ///
    /// Writes to .tmp, renames to final, then fsyncs the parent directory for durability.
    /// 写入 .tmp 文件后 rename 到最终文件名，并 fsync 父目录保证持久性。
    fn flush_inner(dir: &Path, entries: &[OpLogRecord]) -> Result<(), HaError> {
        if entries.is_empty() {
            return Ok(());
        }
        let start_seq = entries.first().unwrap().seq;
        let tmp_path = dir.join(format!("oplog_{:020}.tmp", start_seq));
        let final_path = dir.join(format!("oplog_{:020}.bin", start_seq));

        // Serialize entries in binary frame format
        // 以二进制帧格式序列化条目
        let mut data = Vec::with_capacity(entries.len() * 128);
        for entry in entries {
            let payload = entry.payload.as_bytes();
            data.extend_from_slice(&(entry.seq as u32).to_le_bytes());
            data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            data.extend_from_slice(payload);
        }

        fs::write(&tmp_path, &data)
            .map_err(|e| HaError::InvalidBackend(format!("oplog write segment: {e}")))?;
        fs::rename(&tmp_path, &final_path)
            .map_err(|e| HaError::InvalidBackend(format!("oplog rename segment: {e}")))?;
        // fsync parent directory for durability
        if let Ok(f) = fs::File::open(dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    /// Write the current buffer to a segment file atomically.
    pub(super) fn flush(&mut self) -> Result<(), HaError> {
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
        fs::write(self.latest_path(), sequence_id.to_string())
            .map_err(|e| HaError::InvalidBackend(format!("oplog write latest: {e}")))?;
        if let Ok(f) = fs::File::open(&self.dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    /// Parse entries from binary frame format: [4B seq LE][4B payload_len LE][payload].
    /// 解析二进制帧格式的数据：每帧 [4B seq LE][4B payload_len LE][payload]。
    /// 遇到不完整帧时停止解析，保证容错性。
    ///
    /// Stops parsing on incomplete frames for fault tolerance.
    fn parse_entries(data: &[u8]) -> Vec<OpLogRecord> {
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset + 8 <= data.len() {
            let seq = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as u64;
            let payload_len = u32::from_le_bytes([
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]) as usize;
            offset += 8;
            if offset + payload_len > data.len() {
                break;
            }
            let payload = String::from_utf8_lossy(&data[offset..offset + payload_len]).into_owned();
            entries.push(OpLogRecord {
                seq,
                producer_view_version: 0,
                payload,
            });
            offset += payload_len;
        }
        entries
    }
}

impl OpLogStore for LocalFsOpLogStore {
    /// Append entry: buffer in memory, flush via background thread when buffer is full.
    /// 追加条目：先写入内存 buffer，buffer 满时通过 channel 发送给后台线程异步刷盘。
    ///
    /// If the flush channel is closed (background thread exited abnormally),
    /// falls back to a synchronous flush to prevent data loss.
    /// 若 channel 已关闭（后台线程异常退出），回退到同步 flush 保证数据不丢失。
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        // When buffer is full: send to background thread for async flush
        // buffer 满时触发异步刷盘：swap 空 buffer 后将旧数据发给后台线程
        if self.buffer.len() >= self.max_entries_per_segment {
            let to_flush = std::mem::take(&mut self.buffer);
            match self.flush_tx.send(to_flush) {
                Ok(()) => {} // Background thread handles the flush / 后台线程接管刷盘
                Err(mpsc::SendError(entries)) => {
                    // Channel closed — fallback to synchronous flush to prevent data loss
                    // Channel 关闭，回退到同步 flush 兜底（防止数据丢失）
                    warn!("oplog flush channel closed, falling back to sync flush");
                    self.buffer = entries;
                    self.flush()?;
                }
            }
        }
        Ok(self.last_seq)
    }

    /// Read entries starting from since_seq.
    /// Reads both on-disk segment files and in-memory (unflushed) buffer.
    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        let segments = self.list_segment_files()?;
        let mut all_entries = Vec::new();

        // Read from on-disk segment files
        let mut sorted_segs = segments;
        sorted_segs.sort();
        for start_seq in sorted_segs {
            if start_seq > since_seq + 100_000 {
                break; // optimization: don't read far-ahead segments
            }
            let data = fs::read(self.segment_path(start_seq))
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            for entry in Self::parse_entries(&data) {
                if entry.seq >= since_seq {
                    all_entries.push(entry);
                    if all_entries.len() >= max_count {
                        return Ok(all_entries);
                    }
                }
            }
        }

        // Also include buffered (unflushed) entries
        for entry in &self.buffer {
            if entry.seq >= since_seq {
                all_entries.push(entry.clone());
                if all_entries.len() >= max_count {
                    break;
                }
            }
        }

        all_entries.truncate(max_count);
        Ok(all_entries)
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        let mut max_seq = self.last_seq;
        for start_seq in self.list_segment_files()? {
            let data = fs::read(self.segment_path(start_seq))
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            if let Some(entry) = Self::parse_entries(&data).last() {
                max_seq = max_seq.max(entry.seq);
            }
        }
        for entry in &self.buffer {
            max_seq = max_seq.max(entry.seq);
        }
        Ok(max_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.last_seq = sequence_id;
        self.write_latest(sequence_id)
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        fs::create_dir_all(self.snapshots_dir())
            .map_err(|e| HaError::InvalidBackend(format!("oplog create snapshots dir: {e}")))?;
        fs::write(self.snapshot_path(snapshot_id), sequence_id.to_string())
            .map_err(|e| HaError::InvalidBackend(format!("oplog write snapshot seq: {e}")))?;
        if let Ok(f) = fs::File::open(self.snapshots_dir()) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        validate_snapshot_id(snapshot_id)?;
        let value = fs::read_to_string(self.snapshot_path(snapshot_id))
            .map_err(|e| HaError::InvalidBackend(format!("oplog read snapshot seq: {e}")))?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|e| HaError::InvalidBackend(format!("oplog parse snapshot seq: {e}")))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.buffer.retain(|entry| entry.seq >= before_sequence_id);
        for start_seq in self.list_segment_files()? {
            let path = self.segment_path(start_seq);
            let data = fs::read(&path)
                .map_err(|e| HaError::InvalidBackend(format!("oplog read segment: {e}")))?;
            let entries = Self::parse_entries(&data);
            let retained = entries
                .iter()
                .filter(|entry| entry.seq >= before_sequence_id)
                .cloned()
                .collect::<Vec<_>>();
            if retained.is_empty() {
                fs::remove_file(&path)
                    .map_err(|e| HaError::InvalidBackend(format!("oplog cleanup segment: {e}")))?;
            } else if retained.len() != entries.len() {
                fs::remove_file(&path)
                    .map_err(|e| HaError::InvalidBackend(format!("oplog rewrite segment: {e}")))?;
                Self::flush_inner(&self.dir, &retained)?;
            }
        }
        if let Ok(f) = fs::File::open(&self.dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        self.flush()
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}
