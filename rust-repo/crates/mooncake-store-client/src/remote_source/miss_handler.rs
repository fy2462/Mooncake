use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Mutex, oneshot, Semaphore};

use super::config::RemoteSourceConfig;
use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};
use crate::LocalHotCache;

enum InflightEntry {
    Pending(Vec<oneshot::Sender<RemoteSourceResult<Vec<u8>>>>),
}

#[derive(Debug, Default)]
pub struct MissHandlerStats {
    pub total_misses: u64,
    pub coalesced_misses: u64,
    pub successful_fetches: u64,
    pub failed_fetches: u64,
    pub cache_hits: u64,
    pub bytes_transferred: u64,
    pub prefetch_keys_requested: u64,
    pub prefetch_keys_succeeded: u64,
    pub fetch_latency_us_sum: u64,
    pub fetch_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct MissHandlerSnapshot {
    pub total_misses: u64,
    pub coalesced_misses: u64,
    pub successful_fetches: u64,
    pub failed_fetches: u64,
    pub cache_hits: u64,
    pub bytes_transferred: u64,
    pub prefetch_keys_requested: u64,
    pub prefetch_keys_succeeded: u64,
    pub avg_fetch_latency_us: u64,
    pub miss_rate: f64,
}

pub struct MissHandler<S: RemoteSource> {
    source: Arc<S>,
    config: RemoteSourceConfig,
    inflight: Mutex<HashMap<String, InflightEntry>>,
    admission: Semaphore,
    hot_cache: Option<Arc<LocalHotCache>>,
    // All stats use AtomicU64 for lock-free updates
    total_misses: AtomicU64,
    coalesced_misses: AtomicU64,
    successful_fetches: AtomicU64,
    failed_fetches: AtomicU64,
    cache_hits: AtomicU64,
    bytes_transferred: AtomicU64,
    prefetch_keys_requested: AtomicU64,
    prefetch_keys_succeeded: AtomicU64,
    fetch_latency_us_sum: AtomicU64,
    fetch_count: AtomicU64,
}

impl<S: RemoteSource + 'static> MissHandler<S> {
    pub fn new(source: S, config: RemoteSourceConfig) -> Self {
        let max_concurrent = config.max_concurrent_fetches;
        Self {
            source: Arc::new(source),
            config,
            inflight: Mutex::new(HashMap::new()),
            admission: Semaphore::new(max_concurrent),
            hot_cache: None,
            total_misses: AtomicU64::new(0),
            coalesced_misses: AtomicU64::new(0),
            successful_fetches: AtomicU64::new(0),
            failed_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            prefetch_keys_requested: AtomicU64::new(0),
            prefetch_keys_succeeded: AtomicU64::new(0),
            fetch_latency_us_sum: AtomicU64::new(0),
            fetch_count: AtomicU64::new(0),
        }
    }

    pub fn with_hot_cache(mut self, cache: Arc<LocalHotCache>) -> Self {
        self.hot_cache = Some(cache);
        self
    }

    pub fn is_enabled(&self) -> bool { self.config.enabled }

    pub fn config(&self) -> &RemoteSourceConfig { &self.config }

    pub async fn handle_miss(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        if let Some(ref cache) = self.hot_cache {
            if let Some(data) = cache.get(key) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(data);
            }
        }

        if !self.config.enabled {
            return Err(RemoteSourceError::NotFound(key.to_string()));
        }

        {
            let mut inflight = self.inflight.lock().await;
            self.total_misses.fetch_add(1, Ordering::Relaxed);

            if !inflight.contains_key(key) {
                inflight.insert(key.to_string(), InflightEntry::Pending(Vec::new()));
            } else {
                self.coalesced_misses.fetch_add(1, Ordering::Relaxed);
                let (tx, rx) = oneshot::channel();
                if let Some(InflightEntry::Pending(waiters)) = inflight.get_mut(key) {
                    waiters.push(tx);
                }
                drop(inflight);
                let data = rx.await.unwrap_or(Err(RemoteSourceError::Internal(
                    "fetcher dropped".to_string(),
                )))?;
                if let Some(ref cache) = self.hot_cache {
                    cache.put(key, &data);
                }
                return Ok(data);
            }
        }

        let _permit = self.admission.acquire().await.map_err(|_| {
            RemoteSourceError::Internal("admission semaphore closed".to_string())
        })?;

        let start = Instant::now();
        let result = self.source.get(key).await;
        let elapsed_us = start.elapsed().as_micros() as u64;

        self.fetch_latency_us_sum.fetch_add(elapsed_us, Ordering::Relaxed);
        self.fetch_count.fetch_add(1, Ordering::Relaxed);

        if let Ok(ref data) = result {
            if let Some(ref cache) = self.hot_cache {
                cache.put(key, data);
            }
            self.bytes_transferred.fetch_add(data.len() as u64, Ordering::Relaxed);
            self.successful_fetches.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_fetches.fetch_add(1, Ordering::Relaxed);
        }

        let waiters = {
            let mut inflight = self.inflight.lock().await;
            match inflight.remove(key) {
                Some(InflightEntry::Pending(waiters)) => waiters,
                _ => Vec::new(),
            }
        };

        tracing::debug!(
            key = %key, elapsed_us = elapsed_us, waiters = waiters.len(),
            success = result.is_ok(), "miss_handler fetch completed"
        );

        let result_clone = result.clone();
        for waiter in waiters {
            let _ = waiter.send(result_clone.clone());
        }

        result
    }

    pub async fn batch_fetch(&self, keys: &[String]) {
        let n = keys.len() as u64;
        self.prefetch_keys_requested.fetch_add(n, Ordering::Relaxed);

        let results = self.source.prefetch_keys(keys).await;

        let mut succeeded = 0u64;
        if let Some(ref cache) = self.hot_cache {
            for (key, result) in keys.iter().zip(results.iter()) {
                if let Ok(data) = result {
                    cache.put(key, data);
                    succeeded += 1;
                }
            }
        }
        if succeeded > 0 {
            self.prefetch_keys_succeeded.fetch_add(succeeded, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> MissHandlerSnapshot {
        let total = self.total_misses.load(Ordering::Relaxed);
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let fetch_n = self.fetch_count.load(Ordering::Relaxed);
        let latency_sum = self.fetch_latency_us_sum.load(Ordering::Relaxed);

        MissHandlerSnapshot {
            total_misses: total,
            coalesced_misses: self.coalesced_misses.load(Ordering::Relaxed),
            successful_fetches: self.successful_fetches.load(Ordering::Relaxed),
            failed_fetches: self.failed_fetches.load(Ordering::Relaxed),
            cache_hits: hits,
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            prefetch_keys_requested: self.prefetch_keys_requested.load(Ordering::Relaxed),
            prefetch_keys_succeeded: self.prefetch_keys_succeeded.load(Ordering::Relaxed),
            avg_fetch_latency_us: if fetch_n > 0 { latency_sum / fetch_n } else { 0 },
            miss_rate: if total > 0 { total as f64 / (total + hits) as f64 } else { 0.0 },
        }
    }

    pub fn stats(&self) -> MissHandlerStats {
        MissHandlerStats {
            total_misses: self.total_misses.load(Ordering::Relaxed),
            coalesced_misses: self.coalesced_misses.load(Ordering::Relaxed),
            successful_fetches: self.successful_fetches.load(Ordering::Relaxed),
            failed_fetches: self.failed_fetches.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            prefetch_keys_requested: self.prefetch_keys_requested.load(Ordering::Relaxed),
            prefetch_keys_succeeded: self.prefetch_keys_succeeded.load(Ordering::Relaxed),
            fetch_latency_us_sum: self.fetch_latency_us_sum.load(Ordering::Relaxed),
            fetch_count: self.fetch_count.load(Ordering::Relaxed),
        }
    }

    pub fn source(&self) -> &Arc<S> { &self.source }

    pub fn hot_cache(&self) -> Option<&Arc<LocalHotCache>> { self.hot_cache.as_ref() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    struct MemSource {
        data: StdMutex<HashMap<String, Vec<u8>>>,
        delay: bool,
    }

    impl MemSource {
        fn new() -> Self { Self { data: StdMutex::new(HashMap::new()), delay: false } }
        fn with_delay(mut self) -> Self { self.delay = true; self }
        fn put(&self, k: &str, v: &[u8]) { self.data.lock().unwrap().insert(k.into(), v.to_vec()); }
    }

    #[async_trait::async_trait]
    impl RemoteSource for MemSource {
        async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
            if self.delay {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            self.data.lock().unwrap().get(key).cloned()
                .ok_or_else(|| RemoteSourceError::NotFound(key.to_string()))
        }
    }

    #[tokio::test]
    async fn enabled_handler_fetches() {
        let s = MemSource::new();
        s.put("hello", b"world");
        let h = MissHandler::new(s, RemoteSourceConfig { enabled: true, ..Default::default() });
        assert_eq!(h.handle_miss("hello").await.unwrap(), b"world");
    }

    #[tokio::test]
    async fn disabled_returns_not_found() {
        let s = MemSource::new();
        s.put("hello", b"world");
        let h = MissHandler::new(s, RemoteSourceConfig { enabled: false, ..Default::default() });
        assert!(matches!(h.handle_miss("hello").await, Err(RemoteSourceError::NotFound(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedup_same_key_returns_correct_data() {
        let s = MemSource::new().with_delay();
        s.put("k", b"v");
        let h = Arc::new(MissHandler::new(s, RemoteSourceConfig { enabled: true, ..Default::default() }));
        let h1 = h.clone();
        let h2 = h.clone();
        // tokio::spawn on multi-thread runtime → true parallelism
        let t1 = tokio::spawn(async move { h1.handle_miss("k").await });
        let t2 = tokio::spawn(async move { h2.handle_miss("k").await });
        let (r1, r2) = tokio::join!(t1, t2);
        assert_eq!(r1.unwrap().unwrap(), b"v");
        assert_eq!(r2.unwrap().unwrap(), b"v");
        let snap = h.snapshot();
        assert_eq!(snap.total_misses, 2);
        assert_eq!(snap.successful_fetches, 1);
    }

    #[tokio::test]
    async fn batch_fetch_stats() {
        let s = MemSource::new();
        s.put("a", b"1"); s.put("b", b"2");
        let cache = Arc::new(LocalHotCache::default());
        let h = MissHandler::new(s, RemoteSourceConfig { enabled: true, ..Default::default() })
            .with_hot_cache(cache);
        let keys: Vec<String> = ["a", "b", "missing"].iter().map(|s| s.to_string()).collect();
        h.batch_fetch(&keys).await;
        let snap = h.snapshot();
        assert_eq!(snap.prefetch_keys_requested, 3);
        assert_eq!(snap.prefetch_keys_succeeded, 2);
    }
}
