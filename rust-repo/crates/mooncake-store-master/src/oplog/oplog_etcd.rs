use super::oplog_wire::*;
use super::*;
use etcd_client::{Compare, CompareOp, Txn, TxnOp};
use std::cell::RefCell;
use std::future::Future;

struct CurrentThreadRuntime {
    runtime: tokio::runtime::Runtime,
    #[cfg(test)]
    marker: u64,
}

thread_local! {
    static CURRENT_THREAD_RUNTIME: RefCell<Option<CurrentThreadRuntime>> = const { RefCell::new(None) };
}

#[cfg(test)]
static NEXT_CURRENT_THREAD_RUNTIME_MARKER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

pub struct EtcdOpLogStore {
    client: etcd_client::Client,
    key_prefix: String,
    last_seq: u64,
    /// Entries accumulated for batch write.
    buffer: Vec<OpLogRecord>,
    /// Etcd election ownership used to fence every mutating transaction.
    /// Reader-only stores leave this unset and reject writes.
    writer_fence: Option<EtcdWriterFence>,
    /// Prevent a failed append from being committed by a later caller.
    poisoned: Option<String>,
}

#[derive(Clone)]
struct EtcdWriterFence {
    election_key: String,
    producer_view_version: u64,
    producer_revision: i64,
}

impl EtcdOpLogStore {
    fn inclusive_range_start(since_seq: u64) -> u64 {
        since_seq
    }

    fn ensure_not_poisoned(&self) -> Result<(), HaError> {
        if let Some(reason) = &self.poisoned {
            return Err(HaError::InvalidBackend(format!(
                "etcd oplog is poisoned after a persistence failure: {reason}"
            )));
        }
        Ok(())
    }

    async fn fetch_latest_sequence(&self) -> Result<u64, HaError> {
        let c = self.client.clone();
        let latest_key = self.latest_key();
        let response = c
            .kv_client()
            .get(latest_key.as_bytes(), None)
            .await
            .map_err(|e| HaError::InvalidBackend(format!("etcd get oplog latest: {e}")))?;
        parse_latest_sequence_value(response.kvs().first().map(|kv| kv.value()))
    }

    /// Create an etcd-backed oplog store, recovering last_seq from the /latest key.
    /// 创建 etcd oplog store，通过读取 `/latest` key 恢复 last_seq。
    pub async fn new(client: etcd_client::Client, key_prefix: &str) -> Result<Self, HaError> {
        Self::new_inner(client, key_prefix, None).await
    }

    pub async fn new_leader(
        client: etcd_client::Client,
        key_prefix: &str,
        election_key: impl Into<String>,
        producer_view_version: u64,
    ) -> Result<Self, HaError> {
        let election_key = election_key.into();
        if election_key.trim().is_empty() {
            return Err(HaError::InvalidBackend(
                "etcd oplog writer election key is empty".into(),
            ));
        }
        if producer_view_version == 0 {
            return Err(HaError::InvalidBackend(
                "etcd oplog writer producer view is zero".into(),
            ));
        }
        let producer_revision = i64::try_from(producer_view_version).map_err(|_| {
            HaError::InvalidBackend(format!(
                "producer view {producer_view_version} exceeds etcd revision range"
            ))
        })?;
        Self::new_inner(
            client,
            key_prefix,
            Some(EtcdWriterFence {
                election_key,
                producer_view_version,
                producer_revision,
            }),
        )
        .await
    }

    async fn new_inner(
        client: etcd_client::Client,
        key_prefix: &str,
        writer_fence: Option<EtcdWriterFence>,
    ) -> Result<Self, HaError> {
        let prefix = key_prefix.trim_end_matches('/').to_string();
        let mut store = Self {
            client,
            key_prefix: prefix,
            last_seq: 0,
            buffer: Vec::new(),
            writer_fence,
            poisoned: None,
        };
        store.recover().await?;
        Ok(store)
    }

    fn reader_clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            key_prefix: self.key_prefix.clone(),
            last_seq: self.last_seq,
            buffer: Vec::new(),
            writer_fence: None,
            poisoned: self.poisoned.clone(),
        }
    }

    fn writer_fence(&self) -> Result<&EtcdWriterFence, HaError> {
        self.writer_fence.as_ref().ok_or_else(|| {
            HaError::InvalidBackend("reader-only etcd oplog store rejects mutation".into())
        })
    }

    fn writer_compare(&self) -> Result<Compare, HaError> {
        let fence = self.writer_fence()?;
        Ok(Compare::mod_revision(
            fence.election_key.as_bytes().to_vec(),
            CompareOp::Equal,
            fence.producer_revision,
        ))
    }

    /// Recover `last_seq` from the `/latest` key.
    async fn recover(&mut self) -> Result<(), HaError> {
        let c = self.client.clone();
        self.last_seq = self.fetch_latest_sequence().await?;
        let range_start = self.entry_key(0);
        let range_end = self.latest_key();
        let response = c
            .kv_client()
            .get(
                range_start.as_bytes(),
                Some(
                    etcd_client::GetOptions::new()
                        .with_range(range_end.as_bytes())
                        .with_sort(
                            etcd_client::SortTarget::Key,
                            etcd_client::SortOrder::Descend,
                        )
                        .with_limit(1),
                ),
            )
            .await
            .map_err(|e| HaError::InvalidBackend(format!("etcd get max oplog: {e}")))?;
        if let Some(kv) = response.kvs().first() {
            let segment_max = parse_etcd_entry_key_sequence(kv.key())?;
            if segment_max > self.last_seq {
                return Err(HaError::InvalidBackend(format!(
                    "etcd oplog entry exceeds committed latest pointer: entry_max={segment_max}, latest={}",
                    self.last_seq
                )));
            }
        }
        Ok(())
    }

    /// Build the etcd key for a given sequence number.
    fn entry_key(&self, seq: u64) -> String {
        Self::format_entry_key(&self.key_prefix, seq)
    }

    pub(super) fn format_entry_key(key_prefix: &str, seq: u64) -> String {
        format!("{}/{:020}", key_prefix.trim_end_matches('/'), seq)
    }

    /// Build the etcd key for the latest sequence pointer.
    fn latest_key(&self) -> String {
        format!("{}/latest", self.key_prefix)
    }

    fn snapshot_key(&self, snapshot_id: &str) -> String {
        format!("{}/snapshot/{}", self.key_prefix, snapshot_id)
    }

    /// Atomically commit buffered entries and the `/latest` pointer in one
    /// etcd transaction, so readers cannot observe an uncommitted prefix.
    async fn flush(&mut self) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        let result = self.flush_unpoisoned().await;
        if let Err(error) = &result {
            self.poisoned = Some(error.to_string());
        }
        result
    }

    async fn flush_unpoisoned(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let fence = self.writer_fence()?;
        let (expected_previous_seq, max_seq) = validate_buffer_sequence(&self.buffer)?;
        if max_seq != self.last_seq {
            return Err(HaError::InvalidBackend(format!(
                "etcd oplog buffered sequence diverges from local latest: buffered={max_seq}, local={}",
                self.last_seq
            )));
        }
        validate_buffer_producer_view(&self.buffer, fence.producer_view_version)?;
        let latest_key = self.latest_key();
        let mut compares = Vec::with_capacity(self.buffer.len() + 2);
        if expected_previous_seq == 0 {
            compares.push(Compare::version(
                latest_key.as_bytes().to_vec(),
                CompareOp::Equal,
                0,
            ));
        } else {
            compares.push(Compare::value(
                latest_key.as_bytes().to_vec(),
                CompareOp::Equal,
                expected_previous_seq.to_string().into_bytes(),
            ));
        }
        for entry in &self.buffer {
            compares.push(Compare::version(
                self.entry_key(entry.seq).into_bytes(),
                CompareOp::Equal,
                0,
            ));
        }
        compares.push(self.writer_compare()?);
        let mut operations = Vec::with_capacity(self.buffer.len() + 1);
        for entry in &self.buffer {
            let key = self.entry_key(entry.seq);
            let value = serialize_etcd_oplog_value(entry)?;
            operations.push(TxnOp::put(key.into_bytes(), value.into_bytes(), None));
        }
        operations.push(TxnOp::put(
            latest_key.into_bytes(),
            max_seq.to_string().into_bytes(),
            None,
        ));
        let mut client = self.client.clone();
        let response = client
            .txn(Txn::new().when(compares).and_then(operations))
            .await
            .map_err(|error| {
                metrics::OPLOG_ETCD_WRITE_FAILURES.inc();
                metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(started.elapsed().as_micros() as f64);
                HaError::InvalidBackend(format!("etcd commit oplog: {error}"))
            })?;
        if !response.succeeded() {
            metrics::OPLOG_ETCD_WRITE_FAILURES.inc();
            metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(started.elapsed().as_micros() as f64);
            return Err(HaError::InvalidBackend(format!(
                "etcd oplog writer fenced or sequence CAS failed: expected_previous_seq={expected_previous_seq}"
            )));
        }

        self.buffer.clear();
        metrics::OPLOG_BATCH_COMMITS.inc();
        metrics::OPLOG_SYNC_BATCH_COMMITS.inc();
        metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(started.elapsed().as_micros() as f64);
        Ok(())
    }
}

fn validate_buffer_sequence(buffer: &[OpLogRecord]) -> Result<(u64, u64), HaError> {
    let first_sequence = buffer
        .first()
        .map(|entry| entry.seq)
        .ok_or_else(|| HaError::InvalidBackend("empty etcd oplog buffer".into()))?;
    let expected_previous_sequence = first_sequence.checked_sub(1).ok_or_else(|| {
        HaError::InvalidBackend("etcd oplog sequence zero cannot be appended".into())
    })?;
    for pair in buffer.windows(2) {
        if pair[0].seq.checked_add(1) != Some(pair[1].seq) {
            return Err(HaError::InvalidBackend(format!(
                "etcd oplog append batch has a sequence gap: previous={}, next={}",
                pair[0].seq, pair[1].seq
            )));
        }
    }
    Ok((
        expected_previous_sequence,
        buffer.last().expect("non-empty buffer").seq,
    ))
}

pub(super) fn assign_buffered_sequences(
    last_seq: u64,
    entries: &[OpLogRecord],
) -> Result<Vec<OpLogRecord>, HaError> {
    let mut next_seq = last_seq;
    entries
        .iter()
        .cloned()
        .map(|entry| {
            next_seq = next_seq.checked_add(1).ok_or_else(|| {
                HaError::InvalidBackend("oplog sequence exhausted at u64::MAX".into())
            })?;
            Ok(OpLogRecord {
                seq: next_seq,
                ..entry
            })
        })
        .collect()
}

fn validate_buffer_producer_view(
    buffer: &[OpLogRecord],
    expected_producer_view_version: u64,
) -> Result<(), HaError> {
    if buffer.is_empty() {
        return Err(HaError::InvalidBackend("empty etcd oplog buffer".into()));
    }
    if buffer
        .iter()
        .any(|entry| entry.producer_view_version != expected_producer_view_version)
    {
        return Err(HaError::InvalidBackend(format!(
            "etcd oplog batch producer view does not match writer fence: expected={expected_producer_view_version}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        EtcdOpLogStore, block_on_runtime, current_thread_runtime_marker_for_test,
        decode_etcd_range_entry, parse_latest_sequence_value, parse_snapshot_sequence_value,
        run_notifier_thread, serialize_etcd_oplog_value, validate_buffer_producer_view,
        validate_buffer_sequence,
    };
    use crate::ha::OpLogRecord;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn inclusive_read_range_starts_at_requested_sequence() {
        assert_eq!(EtcdOpLogStore::inclusive_range_start(0), 0);
        assert_eq!(EtcdOpLogStore::inclusive_range_start(7), 7);
        assert_eq!(
            EtcdOpLogStore::format_entry_key(
                "/oplog/cluster",
                EtcdOpLogStore::inclusive_range_start(7),
            ),
            "/oplog/cluster/00000000000000000007"
        );
    }

    #[test]
    fn corrupt_range_entry_returns_contextual_error() {
        let key = b"/oplog/cluster/00000000000000000024";
        let value = serde_json::to_vec(&OpLogRecord {
            seq: 24,
            producer_view_version: 1,
            payload: serde_json::json!({
                "op": "put_end",
                "key": "k1",
                "tenant_id": 42,
                "user_key": "k1",
                "replicas": [],
            })
            .to_string(),
        })
        .unwrap();

        let error = decode_etcd_range_entry(key, &value).unwrap_err();

        assert!(error.to_string().contains("00000000000000000024"));
        assert!(error.to_string().contains("tenant"));
    }

    #[test]
    fn range_entry_rejects_key_value_sequence_mismatch() {
        let key = b"/oplog/cluster/00000000000000000024";
        let value = serialize_etcd_oplog_value(&OpLogRecord {
            seq: 25,
            producer_view_version: 1,
            payload: serde_json::json!({
                "op": "remove",
                "key": "default\0k1",
            })
            .to_string(),
        })
        .unwrap();

        let error = decode_etcd_range_entry(key, value.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("key/value sequence mismatch"));
    }

    #[test]
    fn latest_sequence_parser_is_fail_closed() {
        assert_eq!(parse_latest_sequence_value(None).unwrap(), 0);
        assert_eq!(parse_latest_sequence_value(Some(b"42")).unwrap(), 42);
        assert!(parse_latest_sequence_value(Some(&[0xff])).is_err());
        assert!(parse_latest_sequence_value(Some(b"not-a-sequence")).is_err());
    }

    #[test]
    fn snapshot_sequence_parser_distinguishes_missing_and_corrupt_values() {
        assert_eq!(
            parse_snapshot_sequence_value("snapshot-a", Some(b"42")).unwrap(),
            42
        );
        assert!(parse_snapshot_sequence_value("snapshot-a", None).is_err());
        assert!(parse_snapshot_sequence_value("snapshot-a", Some(&[0xff])).is_err());
        assert!(parse_snapshot_sequence_value("snapshot-a", Some(b"not-a-sequence")).is_err());
    }

    #[test]
    fn leader_writer_requires_one_nonzero_producer_view_per_batch() {
        let record = |seq, producer_view_version| OpLogRecord {
            seq,
            producer_view_version,
            payload: r#"{"op":"remove","key":"default\0k"}"#.to_string(),
        };

        assert!(validate_buffer_producer_view(&[record(1, 7), record(2, 7)], 7).is_ok());
        assert!(validate_buffer_producer_view(&[record(1, 0)], 7).is_err());
        assert!(validate_buffer_producer_view(&[record(1, 7), record(2, 8)], 7).is_err());
        assert!(validate_buffer_producer_view(&[], 7).is_err());
    }

    #[test]
    fn leader_writer_requires_nonzero_contiguous_sequences() {
        let record = |seq| OpLogRecord {
            seq,
            producer_view_version: 7,
            payload: r#"{"op":"remove","key":"default\0k"}"#.to_string(),
        };

        assert_eq!(
            validate_buffer_sequence(&[record(41), record(42), record(43)]).unwrap(),
            (40, 43)
        );
        assert!(validate_buffer_sequence(&[]).is_err());
        assert!(validate_buffer_sequence(&[record(0)]).is_err());
        assert!(validate_buffer_sequence(&[record(41), record(43)]).is_err());
    }

    #[test]
    fn plain_thread_reuses_current_thread_runtime() {
        block_on_runtime(async {});
        let first_marker = current_thread_runtime_marker_for_test();

        block_on_runtime(async {});
        let second_marker = current_thread_runtime_marker_for_test();

        assert_eq!(first_marker, second_marker);
    }

    #[test]
    fn notifier_thread_unwind_marks_health_unhealthy() {
        let healthy = Arc::new(AtomicBool::new(true));
        let thread_health = healthy.clone();

        let result = std::thread::spawn(move || {
            run_notifier_thread(thread_health, || panic!("injected notifier panic"));
        })
        .join();

        assert!(result.is_err());
        assert!(!healthy.load(Ordering::Acquire));
    }
}

impl OpLogStore for EtcdOpLogStore {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.ensure_not_poisoned()?;
        self.writer_fence()?;
        let mut assigned = assign_buffered_sequences(self.last_seq, std::slice::from_ref(entry))?;
        let entry = assigned.pop().expect("one assigned entry");
        self.last_seq = entry.seq;
        self.buffer.push(entry);
        Ok(self.last_seq)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        block_on_runtime(self.read_since_async(since_seq, max_count))
    }

    fn latest_sequence(&self) -> u64 {
        match block_on_runtime(self.fetch_latest_sequence()) {
            Ok(sequence) => sequence,
            Err(error) => {
                warn!("failed to refresh etcd oplog latest sequence: {error}");
                self.last_seq
            }
        }
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.ensure_not_poisoned()?;
        let mut max_seq = self.last_seq;
        let mut committed_latest = block_on_runtime(self.fetch_latest_sequence())?;
        max_seq = max_seq.max(committed_latest);

        let c = self.client.clone();
        let range_start = self.entry_key(0);
        let range_end = self.latest_key();
        let response = block_on_runtime(async move {
            c.kv_client()
                .get(
                    range_start.as_bytes(),
                    Some(
                        etcd_client::GetOptions::new()
                            .with_range(range_end.as_bytes())
                            .with_sort(
                                etcd_client::SortTarget::Key,
                                etcd_client::SortOrder::Descend,
                            )
                            .with_limit(1),
                    ),
                )
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd get max oplog: {e}")))
        })?;
        if let Some(kv) = response.kvs().first() {
            let backend_max = parse_etcd_entry_key_sequence(kv.key())?;
            if backend_max > committed_latest {
                // A commit may have landed between the two reads. Refresh the
                // commit pointer once before classifying the entry as orphaned.
                committed_latest = block_on_runtime(self.fetch_latest_sequence())?;
                if backend_max > committed_latest {
                    return Err(HaError::InvalidBackend(format!(
                        "etcd oplog entry exceeds committed latest pointer: entry_max={backend_max}, latest={committed_latest}"
                    )));
                }
            }
            max_seq = max_seq.max(committed_latest);
            max_seq = max_seq.max(backend_max);
        }
        Ok(max_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        self.writer_fence()?;
        if sequence_id < self.last_seq || !self.buffer.is_empty() {
            return Err(HaError::InvalidBackend(
                "oplog latest sequence cannot move backwards or bypass buffered entries".into(),
            ));
        }
        if sequence_id == self.last_seq {
            return Ok(());
        }
        let mut c = self.client.clone();
        let latest_key = self.latest_key();
        let latest_compare = if self.last_seq == 0 {
            Compare::version(latest_key.as_bytes().to_vec(), CompareOp::Equal, 0)
        } else {
            Compare::value(
                latest_key.as_bytes().to_vec(),
                CompareOp::Equal,
                self.last_seq.to_string().into_bytes(),
            )
        };
        let writer_compare = self.writer_compare()?;
        if let Err(error) =
            block_on_runtime(async move {
                let response =
                    c.txn(Txn::new().when([latest_compare, writer_compare]).and_then([
                        TxnOp::put(
                            latest_key.into_bytes(),
                            sequence_id.to_string().into_bytes(),
                            None,
                        ),
                    ]))
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("etcd put oplog latest: {e}")))?;
                if !response.succeeded() {
                    return Err(HaError::InvalidBackend(
                        "etcd oplog writer fenced or latest sequence CAS failed".into(),
                    ));
                }
                Ok(())
            })
        {
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
        self.writer_fence()?;
        if sequence_id > self.last_seq {
            return Err(HaError::InvalidBackend(format!(
                "snapshot sequence {sequence_id} exceeds committed oplog sequence {}",
                self.last_seq
            )));
        }
        let mut c = self.client.clone();
        let key = self.snapshot_key(snapshot_id);
        let writer_compare = self.writer_compare()?;
        if let Err(error) = block_on_runtime(async move {
            let response = c
                .txn(Txn::new().when([writer_compare]).and_then([TxnOp::put(
                    key.into_bytes(),
                    sequence_id.to_string().into_bytes(),
                    None,
                )]))
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd put snapshot seq: {e}")))?;
            if !response.succeeded() {
                return Err(HaError::InvalidBackend(
                    "etcd oplog writer fenced before snapshot sequence publication".into(),
                ));
            }
            Ok(())
        }) {
            self.poisoned = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.ensure_not_poisoned()?;
        validate_snapshot_id(snapshot_id)?;
        let c = self.client.clone();
        let key = self.snapshot_key(snapshot_id);
        let response = block_on_runtime(async move {
            c.kv_client()
                .get(key.as_bytes(), None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd get snapshot seq: {e}")))
        })?;
        parse_snapshot_sequence_value(snapshot_id, response.kvs().first().map(|kv| kv.value()))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.ensure_not_poisoned()?;
        self.writer_fence()?;
        let mut c = self.client.clone();
        let range_start = self.entry_key(0);
        let range_end = self.entry_key(before_sequence_id);
        let writer_compare = self.writer_compare()?;
        if let Err(error) = block_on_runtime(async move {
            let response = c
                .txn(Txn::new().when([writer_compare]).and_then([TxnOp::delete(
                    range_start.into_bytes(),
                    Some(etcd_client::DeleteOptions::new().with_range(range_end.into_bytes())),
                )]))
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd cleanup oplog: {e}")))?;
            if !response.succeeded() {
                return Err(HaError::InvalidBackend(
                    "etcd oplog writer fenced before cleanup".into(),
                ));
            }
            Ok(())
        }) {
            self.poisoned = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        block_on_runtime(self.flush())
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

    fn create_change_notifier(&self) -> Option<Box<dyn OpLogChangeNotifier>> {
        Some(Box::new(EtcdOpLogChangeNotifier::new(self.reader_clone())))
    }
}

struct EtcdOpLogChangeNotifier {
    store: EtcdOpLogStore,
    shutdown_tx: Option<tokio::sync::watch::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    healthy: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl EtcdOpLogChangeNotifier {
    fn new(store: EtcdOpLogStore) -> Self {
        Self {
            store,
            shutdown_tx: None,
            thread: None,
            healthy: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

fn run_notifier_thread<F>(healthy: std::sync::Arc<std::sync::atomic::AtomicBool>, watch: F)
where
    F: FnOnce(),
{
    struct HealthGuard(std::sync::Arc<std::sync::atomic::AtomicBool>);

    impl Drop for HealthGuard {
        fn drop(&mut self) {
            self.0.store(false, std::sync::atomic::Ordering::Release);
        }
    }

    let _health_guard = HealthGuard(healthy);
    watch();
}

impl OpLogChangeNotifier for EtcdOpLogChangeNotifier {
    fn start(
        &mut self,
        start_seq_id: u64,
        mut on_entry: OpLogEntryCallback,
        mut on_error: OpLogErrorCallback,
    ) -> Result<(), HaError> {
        if self.thread.is_some() {
            return Ok(());
        }
        let store = self.store.reader_clone();
        let healthy = self.healthy.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        self.shutdown_tx = Some(shutdown_tx);
        self.thread = Some(std::thread::spawn(move || {
            run_notifier_thread(healthy.clone(), move || {
                let health_for_watch = healthy.clone();
                let _ = block_on_runtime(store.watch_entries_from_until_with_health(
                    start_seq_id,
                    ETCD_WATCH_SYNC_BATCH_SIZE,
                    shutdown_rx,
                    Some(health_for_watch),
                    move |entry| on_entry(entry),
                    move |err| on_error(err),
                ));
            });
        }));
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.healthy
            .store(false, std::sync::atomic::Ordering::Release);
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(std::sync::atomic::Ordering::Acquire)
    }
}

fn block_on_runtime<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        _ => CURRENT_THREAD_RUNTIME.with(|runtime| {
            let mut runtime = runtime.borrow_mut();
            let runtime = runtime.get_or_insert_with(|| CurrentThreadRuntime {
                runtime: tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create current-thread tokio runtime"),
                #[cfg(test)]
                marker: NEXT_CURRENT_THREAD_RUNTIME_MARKER
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            });
            runtime.runtime.block_on(future)
        }),
    }
}

#[cfg(test)]
fn current_thread_runtime_marker_for_test() -> u64 {
    CURRENT_THREAD_RUNTIME.with(|runtime| {
        runtime
            .borrow()
            .as_ref()
            .expect("current-thread runtime was not initialized")
            .marker
    })
}

fn set_etcd_watch_health(
    health: &Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    healthy: bool,
) {
    if let Some(health) = health {
        health.store(healthy, std::sync::atomic::Ordering::Release);
    }
}

async fn sleep_reconnect_delay(
    reconnect_count: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<()>,
) -> bool {
    let delay_ms = (ETCD_WATCH_RECONNECT_DELAY_MS * reconnect_count.max(1) as u64)
        .min(ETCD_WATCH_MAX_RECONNECT_DELAY_MS);
    tokio::select! {
        changed = shutdown_rx.changed() => {
            let _ = changed;
            false
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(delay_ms)) => true,
    }
}

fn decode_etcd_range_entry(key: &[u8], value: &[u8]) -> Result<OpLogRecord, HaError> {
    let key_sequence = parse_etcd_entry_key_sequence(key)?;
    let key = String::from_utf8_lossy(key);
    let value = std::str::from_utf8(value).map_err(|error| {
        HaError::InvalidBackend(format!(
            "etcd oplog value for key {key:?} is not UTF-8: {error}"
        ))
    })?;
    let record = deserialize_etcd_oplog_value(value).map_err(|error| {
        HaError::InvalidBackend(format!("corrupt etcd oplog record at key {key:?}: {error}"))
    })?;
    if record.seq != key_sequence {
        return Err(HaError::InvalidBackend(format!(
            "etcd oplog key/value sequence mismatch at key {key:?}: record_seq={}",
            record.seq
        )));
    }
    Ok(record)
}

fn parse_etcd_entry_key_sequence(key: &[u8]) -> Result<u64, HaError> {
    let key = std::str::from_utf8(key).map_err(|error| {
        HaError::InvalidBackend(format!("etcd oplog key is not UTF-8: {error}"))
    })?;
    key.rsplit('/')
        .next()
        .ok_or_else(|| HaError::InvalidBackend(format!("etcd oplog key has no sequence: {key:?}")))?
        .parse::<u64>()
        .map_err(|error| {
            HaError::InvalidBackend(format!(
                "etcd oplog key has invalid sequence {key:?}: {error}"
            ))
        })
}

fn parse_latest_sequence_value(value: Option<&[u8]>) -> Result<u64, HaError> {
    let Some(value) = value else {
        return Ok(0);
    };
    let value = std::str::from_utf8(value).map_err(|error| {
        HaError::InvalidBackend(format!("etcd oplog latest is not UTF-8: {error}"))
    })?;
    value.parse::<u64>().map_err(|error| {
        HaError::InvalidBackend(format!(
            "etcd oplog latest has invalid sequence {value:?}: {error}"
        ))
    })
}

fn parse_snapshot_sequence_value(snapshot_id: &str, value: Option<&[u8]>) -> Result<u64, HaError> {
    let value = value
        .ok_or_else(|| HaError::InvalidBackend(format!("snapshot not found: {snapshot_id}")))?;
    let value = std::str::from_utf8(value).map_err(|error| {
        HaError::InvalidBackend(format!(
            "etcd snapshot {snapshot_id:?} sequence is not UTF-8: {error}"
        ))
    })?;
    value.parse::<u64>().map_err(|error| {
        HaError::InvalidBackend(format!(
            "etcd snapshot {snapshot_id:?} has invalid sequence {value:?}: {error}"
        ))
    })
}

// =============================================================================
// EtcdOpLogStore — async extension methods
// =============================================================================

/// Async extension for etcd-backed stores that need full range queries.
impl EtcdOpLogStore {
    /// Read entries from etcd starting from `since_seq` (async).
    /// 从 etcd 异步读取条目（起始序列号 since_seq）。
    pub async fn read_since_async(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        self.read_since_with_revision_async(since_seq, max_count)
            .await
            .map(|(entries, _)| entries)
    }

    pub async fn read_since_with_revision_async(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<(Vec<OpLogRecord>, i64), HaError> {
        self.ensure_not_poisoned()?;
        let mut entries = Vec::new();
        let c = self.client.clone();

        // OpLogStore::read_since is inclusive, matching the in-memory and
        // local-file backends.
        let range_end = self.latest_key();
        let range_start = self.entry_key(Self::inclusive_range_start(since_seq));

        let revision = match c
            .kv_client()
            .get(
                range_start.as_bytes(),
                Some(etcd_client::GetOptions::new().with_range(range_end.as_bytes())),
            )
            .await
        {
            Ok(resp) => {
                let revision = resp.header().map(|h| h.revision()).unwrap_or_default();
                for kv in resp.kvs().iter().take(max_count) {
                    entries.push(decode_etcd_range_entry(kv.key(), kv.value())?);
                    if entries.len() >= max_count {
                        break;
                    }
                }
                revision
            }
            Err(e) => {
                return Err(HaError::InvalidBackend(format!(
                    "etcd range query for oplog failed: {e}"
                )));
            }
        };

        // Supplement with buffered (not yet flushed) entries
        for entry in &self.buffer {
            if entry.seq >= since_seq
                && entries.len() < max_count
                && !entries.iter().any(|e| e.seq == entry.seq)
            {
                entries.push(entry.clone());
            }
        }

        entries.sort_by_key(|e| e.seq);
        for pair in entries.windows(2) {
            if pair[0].seq.checked_add(1) != Some(pair[1].seq) {
                return Err(HaError::InvalidBackend(format!(
                    "etcd oplog contains a sequence gap: previous={}, next={}",
                    pair[0].seq, pair[1].seq
                )));
            }
        }
        entries.truncate(max_count);
        Ok((entries, revision))
    }

    /// Flush buffered entries to etcd (async). Call periodically or before shutdown.
    pub async fn flush_async(&mut self) -> Result<(), HaError> {
        self.flush().await
    }

    pub async fn watch_entries_from<F, E>(
        &self,
        start_seq_id: u64,
        max_historical_batch: usize,
        mut on_entry: F,
        mut on_error: E,
    ) -> Result<(), HaError>
    where
        F: FnMut(OpLogRecord) + Send,
        E: FnMut(HaError) + Send,
    {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        self.watch_entries_from_until(
            start_seq_id,
            max_historical_batch,
            shutdown_rx,
            &mut on_entry,
            &mut on_error,
        )
        .await
    }

    pub async fn watch_entries_from_until<F, E>(
        &self,
        start_seq_id: u64,
        max_historical_batch: usize,
        shutdown_rx: tokio::sync::watch::Receiver<()>,
        on_entry: F,
        on_error: E,
    ) -> Result<(), HaError>
    where
        F: FnMut(OpLogRecord) + Send,
        E: FnMut(HaError) + Send,
    {
        self.watch_entries_from_until_with_health(
            start_seq_id,
            max_historical_batch,
            shutdown_rx,
            None,
            on_entry,
            on_error,
        )
        .await
    }

    async fn watch_entries_from_until_with_health<F, E>(
        &self,
        start_seq_id: u64,
        max_historical_batch: usize,
        mut shutdown_rx: tokio::sync::watch::Receiver<()>,
        health: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
        mut on_entry: F,
        mut on_error: E,
    ) -> Result<(), HaError>
    where
        F: FnMut(OpLogRecord) + Send,
        E: FnMut(HaError) + Send,
    {
        let batch_size = max_historical_batch.max(1);
        let mut next_read_seq = start_seq_id;
        let mut last_seq = start_seq_id.saturating_sub(1);
        let mut next_watch_revision = 0;
        let mut consecutive_errors = 0usize;
        let mut reconnect_count = 0usize;

        loop {
            if shutdown_rx.has_changed().unwrap_or(true) {
                set_etcd_watch_health(&health, false);
                return Ok(());
            }

            match self
                .deliver_historical_entries(
                    next_read_seq,
                    batch_size,
                    &mut shutdown_rx,
                    &mut on_entry,
                )
                .await
            {
                Ok((delivered_last_seq, read_revision)) => {
                    if delivered_last_seq > last_seq {
                        last_seq = delivered_last_seq;
                        next_read_seq = last_seq;
                    }
                    if read_revision > 0 {
                        next_watch_revision = read_revision.saturating_add(1);
                    }
                }
                Err(e) => {
                    set_etcd_watch_health(&health, false);
                    on_error(e.clone());
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    if consecutive_errors >= ETCD_WATCH_MAX_CONSECUTIVE_ERRORS {
                        return Err(e);
                    }
                    reconnect_count = reconnect_count.saturating_add(1);
                    if !sleep_reconnect_delay(reconnect_count, &mut shutdown_rx).await {
                        set_etcd_watch_health(&health, false);
                        return Ok(());
                    }
                    continue;
                }
            }

            let watch_prefix = format!("{}/", self.key_prefix.trim_end_matches('/'));
            let mut watch_client = self.client.clone().watch_client();
            let options = etcd_client::WatchOptions::new()
                .with_prefix()
                .with_start_revision(next_watch_revision.max(1));
            let watch_result = watch_client
                .watch(watch_prefix.as_bytes(), Some(options))
                .await;
            let (_watcher, mut stream) = match watch_result {
                Ok(watch) => watch,
                Err(e) => {
                    let err = HaError::InvalidBackend(format!("etcd watch oplog: {e}"));
                    metrics::OPLOG_WATCH_DISCONNECTIONS.inc();
                    set_etcd_watch_health(&health, false);
                    on_error(err.clone());
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    if consecutive_errors >= ETCD_WATCH_MAX_CONSECUTIVE_ERRORS {
                        return Err(err);
                    }
                    reconnect_count = reconnect_count.saturating_add(1);
                    if !sleep_reconnect_delay(reconnect_count, &mut shutdown_rx).await {
                        set_etcd_watch_health(&health, false);
                        return Ok(());
                    }
                    continue;
                }
            };

            set_etcd_watch_health(&health, true);
            consecutive_errors = 0;
            reconnect_count = 0;

            loop {
                let message = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        let _ = changed;
                        set_etcd_watch_health(&health, false);
                        return Ok(());
                    }
                    message = stream.message() => message,
                };
                match message {
                    Ok(Some(response)) => {
                        if let Some(header) = response.header() {
                            next_watch_revision =
                                next_watch_revision.max(header.revision().saturating_add(1));
                        }
                        if response.canceled() {
                            let err = HaError::InvalidBackend(format!(
                                "etcd watch canceled: {}",
                                response.cancel_reason()
                            ));
                            metrics::OPLOG_WATCH_DISCONNECTIONS.inc();
                            set_etcd_watch_health(&health, false);
                            on_error(err);
                            break;
                        }
                        for event in response.events() {
                            let Some(kv) = event.kv() else {
                                continue;
                            };
                            next_watch_revision =
                                next_watch_revision.max(kv.mod_revision().saturating_add(1));
                            let key = String::from_utf8_lossy(kv.key());
                            if key.ends_with("/latest") || key.contains("/snapshot/") {
                                continue;
                            }
                            if event.event_type() == etcd_client::EventType::Delete {
                                continue;
                            }
                            match decode_etcd_range_entry(kv.key(), kv.value()) {
                                Ok(entry) => {
                                    if entry.seq > last_seq {
                                        last_seq = entry.seq;
                                        next_read_seq = entry.seq;
                                        on_entry(entry);
                                    }
                                    consecutive_errors = 0;
                                }
                                Err(e) => {
                                    on_error(e);
                                    consecutive_errors = consecutive_errors.saturating_add(1);
                                }
                            }
                        }
                        if consecutive_errors >= ETCD_WATCH_MAX_CONSECUTIVE_ERRORS {
                            set_etcd_watch_health(&health, false);
                            break;
                        }
                    }
                    Ok(None) => {
                        let err = HaError::InvalidBackend("etcd watch stream closed".to_string());
                        metrics::OPLOG_WATCH_DISCONNECTIONS.inc();
                        set_etcd_watch_health(&health, false);
                        on_error(err);
                        break;
                    }
                    Err(e) => {
                        let err = HaError::InvalidBackend(format!("etcd watch stream: {e}"));
                        metrics::OPLOG_WATCH_DISCONNECTIONS.inc();
                        set_etcd_watch_health(&health, false);
                        on_error(err);
                        break;
                    }
                }
            }

            reconnect_count = reconnect_count.saturating_add(1);
            if !sleep_reconnect_delay(reconnect_count, &mut shutdown_rx).await {
                set_etcd_watch_health(&health, false);
                return Ok(());
            }
        }
    }

    async fn deliver_historical_entries<F>(
        &self,
        start_seq_id: u64,
        batch_size: usize,
        shutdown_rx: &mut tokio::sync::watch::Receiver<()>,
        on_entry: &mut F,
    ) -> Result<(u64, i64), HaError>
    where
        F: FnMut(OpLogRecord) + Send,
    {
        let mut read_seq = start_seq_id;
        let mut last_seq = start_seq_id.saturating_sub(1);
        let mut last_revision = 0;
        loop {
            if shutdown_rx.has_changed().unwrap_or(true) {
                return Ok((last_seq, last_revision));
            }
            let (historical, read_revision) = self
                .read_since_with_revision_async(read_seq, batch_size)
                .await?;
            if read_revision > 0 {
                last_revision = read_revision;
            }
            let delivered = historical.len();
            for entry in historical {
                if entry.seq > last_seq {
                    last_seq = entry.seq;
                    read_seq = entry.seq;
                    on_entry(entry);
                }
            }
            if delivered < batch_size {
                break;
            }
        }
        Ok((last_seq, last_revision))
    }
}
