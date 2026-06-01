use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use mooncake_store_client::{
    LocalHotCache, MissHandler, RemoteSource, RemoteSourceConfig, RemoteSourceError,
    RemoteSourceResult,
};

struct MemSource {
    data: StdMutex<HashMap<String, Vec<u8>>>,
    delay: bool,
}

impl MemSource {
    fn new() -> Self {
        Self {
            data: StdMutex::new(HashMap::new()),
            delay: false,
        }
    }
    fn with_delay(mut self) -> Self {
        self.delay = true;
        self
    }
    fn put(&self, k: &str, v: &[u8]) {
        self.data.lock().unwrap().insert(k.into(), v.to_vec());
    }
}

#[async_trait::async_trait]
impl RemoteSource for MemSource {
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        if self.delay {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        self.data
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| RemoteSourceError::NotFound(key.to_string()))
    }
}

#[tokio::test]
async fn enabled_handler_fetches() {
    let s = MemSource::new();
    s.put("hello", b"world");
    let h = MissHandler::new(
        s,
        RemoteSourceConfig {
            enabled: true,
            ..Default::default()
        },
    );
    assert_eq!(h.handle_miss("hello").await.unwrap(), b"world");
}

#[tokio::test]
async fn disabled_returns_not_found() {
    let s = MemSource::new();
    s.put("hello", b"world");
    let h = MissHandler::new(
        s,
        RemoteSourceConfig {
            enabled: false,
            ..Default::default()
        },
    );
    assert!(matches!(
        h.handle_miss("hello").await,
        Err(RemoteSourceError::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_same_key_returns_correct_data() {
    let s = MemSource::new().with_delay();
    s.put("k", b"v");
    let h = Arc::new(MissHandler::new(
        s,
        RemoteSourceConfig {
            enabled: true,
            ..Default::default()
        },
    ));
    let h1 = h.clone();
    let h2 = h.clone();
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
    s.put("a", b"1");
    s.put("b", b"2");
    let cache = Arc::new(LocalHotCache::default());
    let h = MissHandler::new(
        s,
        RemoteSourceConfig {
            enabled: true,
            ..Default::default()
        },
    )
    .with_hot_cache(cache);
    let keys: Vec<String> = ["a", "b", "missing"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    h.batch_fetch(&keys).await;
    let snap = h.snapshot();
    assert_eq!(snap.prefetch_keys_requested, 3);
    assert_eq!(snap.prefetch_keys_succeeded, 2);
}
