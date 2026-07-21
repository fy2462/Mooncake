use super::oplog_wire::*;
use super::*;
use std::future::Future;

pub struct EtcdOpLogStore {
    client: etcd_client::Client,
    key_prefix: String,
    last_seq: u64,
    /// Entries accumulated for batch write.
    buffer: Vec<OpLogRecord>,
}

impl EtcdOpLogStore {
    /// Create an etcd-backed oplog store, recovering last_seq from the /latest key.
    /// 创建 etcd oplog store，通过读取 `/latest` key 恢复 last_seq。
    pub async fn new(client: etcd_client::Client, key_prefix: &str) -> Result<Self, HaError> {
        let prefix = key_prefix.trim_end_matches('/').to_string();
        let mut store = Self {
            client,
            key_prefix: prefix,
            last_seq: 0,
            buffer: Vec::new(),
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
        }
    }

    /// Recover `last_seq` from the `/latest` key.
    async fn recover(&mut self) -> Result<(), HaError> {
        let latest_key = format!("{}/latest", self.key_prefix);
        let c = self.client.clone();
        match c.kv_client().get(latest_key.as_bytes(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        self.last_seq = val.parse::<u64>().unwrap_or(0);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read oplog latest key: {}", e);
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

    /// Flush buffered entries to etcd: put each record, then update /latest.
    /// 将 buffer 批量写入 etcd：逐个 put 每条记录，最后更新 `/latest` 指针。
    async fn flush(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let c = self.client.clone();
        for entry in &self.buffer {
            let key = self.entry_key(entry.seq);
            let value = serialize_etcd_oplog_value(entry)?;
            if let Err(e) = c
                .kv_client()
                .put(key.as_bytes(), value.as_bytes(), None)
                .await
            {
                metrics::OPLOG_ETCD_WRITE_FAILURES.inc();
                metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(started.elapsed().as_micros() as f64);
                return Err(HaError::InvalidBackend(format!("etcd put oplog: {e}")));
            }
        }
        // Update latest pointer
        let max_seq = self.buffer.last().unwrap().seq;
        let latest_val = max_seq.to_string();
        if let Err(e) = c
            .kv_client()
            .put(self.latest_key().as_bytes(), latest_val.as_bytes(), None)
            .await
        {
            metrics::OPLOG_ETCD_WRITE_FAILURES.inc();
            metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(started.elapsed().as_micros() as f64);
            return Err(HaError::InvalidBackend(format!(
                "etcd put oplog latest: {e}"
            )));
        }

        self.buffer.clear();
        metrics::OPLOG_BATCH_COMMITS.inc();
        metrics::OPLOG_SYNC_BATCH_COMMITS.inc();
        metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(started.elapsed().as_micros() as f64);
        Ok(())
    }
}

impl OpLogStore for EtcdOpLogStore {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        block_on_runtime(self.flush())?;
        Ok(self.last_seq)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        block_on_runtime(self.read_since_async(since_seq, max_count))
    }

    fn latest_sequence(&self) -> u64 {
        let c = self.client.clone();
        let latest_key = self.latest_key();
        block_on_runtime(async move {
            match c.kv_client().get(latest_key.as_bytes(), None).await {
                Ok(resp) => resp
                    .kvs()
                    .first()
                    .and_then(|kv| String::from_utf8(kv.value().to_vec()).ok())
                    .and_then(|v| v.parse::<u64>().ok()),
                Err(_) => None,
            }
        })
        .unwrap_or(self.last_seq)
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        let mut max_seq = self.last_seq;
        let c = self.client.clone();
        let range_start = self.entry_key(0);
        let range_end = self.entry_key(u64::MAX);
        let backend_max = block_on_runtime(async move {
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
        })?
        .kvs()
        .first()
        .and_then(|kv| String::from_utf8(kv.key().to_vec()).ok())
        .and_then(|key| {
            key.rsplit('/')
                .next()
                .and_then(|seq| seq.parse::<u64>().ok())
        });
        if let Some(backend_max) = backend_max {
            max_seq = max_seq.max(backend_max);
        }
        Ok(max_seq)
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.last_seq = sequence_id;
        let c = self.client.clone();
        let latest_key = self.latest_key();
        block_on_runtime(async move {
            c.kv_client()
                .put(
                    latest_key.as_bytes(),
                    sequence_id.to_string().as_bytes(),
                    None,
                )
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd put oplog latest: {e}")))
        })?;
        Ok(())
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        validate_snapshot_id(snapshot_id)?;
        let c = self.client.clone();
        let key = self.snapshot_key(snapshot_id);
        block_on_runtime(async move {
            c.kv_client()
                .put(key.as_bytes(), sequence_id.to_string().as_bytes(), None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd put snapshot seq: {e}")))
        })?;
        Ok(())
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        validate_snapshot_id(snapshot_id)?;
        let c = self.client.clone();
        let key = self.snapshot_key(snapshot_id);
        let value = block_on_runtime(async move {
            c.kv_client()
                .get(key.as_bytes(), None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd get snapshot seq: {e}")))
        })?
        .kvs()
        .first()
        .and_then(|kv| String::from_utf8(kv.value().to_vec()).ok())
        .ok_or_else(|| HaError::InvalidBackend(format!("snapshot not found: {snapshot_id}")))?;
        value
            .parse::<u64>()
            .map_err(|e| HaError::InvalidBackend(format!("etcd parse snapshot seq: {e}")))
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        let c = self.client.clone();
        let range_start = self.entry_key(0);
        let range_end = self.entry_key(before_sequence_id);
        block_on_runtime(async move {
            c.kv_client()
                .delete(
                    range_start.as_bytes(),
                    Some(etcd_client::DeleteOptions::new().with_range(range_end.as_bytes())),
                )
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd cleanup oplog: {e}")))
        })?;
        Ok(())
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        block_on_runtime(self.flush())
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
            let health_for_watch = healthy.clone();
            let _ = block_on_runtime(store.watch_entries_from_until_with_health(
                start_seq_id,
                ETCD_WATCH_SYNC_BATCH_SIZE,
                shutdown_rx,
                Some(health_for_watch),
                move |entry| on_entry(entry),
                move |err| on_error(err),
            ));
            healthy.store(false, std::sync::atomic::Ordering::Release);
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
        _ => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create temporary tokio runtime")
            .block_on(future),
    }
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
        let mut entries = Vec::new();
        let c = self.client.clone();

        // C++ ReadOpLogSinceWithRevision returns entries strictly after start_sequence_id.
        let range_end = self.entry_key(u64::MAX);
        let range_start = self.entry_key(since_seq.saturating_add(1));

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
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        if let Ok(entry) = deserialize_etcd_oplog_value(&val) {
                            entries.push(entry);
                            if entries.len() >= max_count {
                                break;
                            }
                        }
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
            if entry.seq > since_seq
                && entries.len() < max_count
                && !entries.iter().any(|e| e.seq == entry.seq)
            {
                entries.push(entry.clone());
            }
        }

        entries.sort_by_key(|e| e.seq);
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
                            let value = match String::from_utf8(kv.value().to_vec()) {
                                Ok(value) => value,
                                Err(e) => {
                                    let err = HaError::InvalidBackend(format!(
                                        "etcd watch oplog utf8: {e}"
                                    ));
                                    on_error(err);
                                    consecutive_errors = consecutive_errors.saturating_add(1);
                                    continue;
                                }
                            };
                            match deserialize_etcd_oplog_value(&value) {
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
