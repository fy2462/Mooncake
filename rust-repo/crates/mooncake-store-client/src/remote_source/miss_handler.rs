//! # Miss Handler — 缓存未命中处理
//!
//! 核心组件：处理缓存未命中 → 远程获取 → 热缓存填充的完整流程。
//! (Core component: handles the complete flow of cache miss → remote fetch → hot cache population.)
//!
//! ## 设计亮点 (Design Highlights)
//!
//! ### 1. 请求合并 / 去重 (Request Coalescing / Dedup)
//! 当多个并发的 `handle_miss` 调用请求同一个 key 时，只有第一个调用真正发起远程获取；
//! 后续调用者通过 `oneshot` channel 等待结果。这避免了"惊群效应"(thundering herd)。
//!
//! ### 2. 准入控制 (Admission Control)
//! 通过 `tokio::sync::Semaphore` 限制最大并发远程获取数 (`max_concurrent_fetches`)，
//! 防止瞬间大量未命中导致的资源耗尽。
//!
//! ### 3. 热缓存集成 (Hot Cache Integration)
//! 每次远程获取成功后自动写入 `LocalHotCache`，后续访问直接命中缓存。
//!
//! ### 4. 无锁统计 (Lock-Free Statistics)
//! 所有统计计数器 (`total_misses`, `successful_fetches`, etc.) 均使用 `AtomicU64`，
//! 避免统计收集影响关键路径性能。
//!
//! ## 数据流 (Data Flow)
//!
//! ```text
//! handle_miss(key)
//!   ├─ hot_cache.get(key) → hit? → return cached data
//!   ├─ enabled? → no → return NotFound
//!   ├─ inflight check → coalesced? → wait for sender
//!   ├─ acquire semaphore permit
//!   ├─ source.get(key)                     ← 实际远程获取
//!   ├─ hot_cache.put(key, data)            ← 填充热缓存
//!   └─ notify waiters via oneshot channels  ← 唤醒等待者
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{oneshot, Mutex, Semaphore};

use super::config::RemoteSourceConfig;
use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};
use crate::LocalHotCache;

/// 飞行中请求条目：持有等待者的 oneshot 发送端。
/// (Inflight request entry: holds oneshot senders for waiting callers.)
enum InflightEntry {
    Pending(Vec<oneshot::Sender<RemoteSourceResult<Vec<u8>>>>),
}

/// MissHandler 的累积统计信息。
/// (Cumulative statistics for the MissHandler.)
///
/// 所有字段使用 `u64` 表示无符号计数值。
#[derive(Debug, Default)]
pub struct MissHandlerStats {
    /// 总未命中次数 (total cache misses)
    pub total_misses: u64,
    /// 被合并的未命中次数（等待其他请求的结果） (coalesced: waited for another in-flight request)
    pub coalesced_misses: u64,
    /// 成功从远程获取的次数 (successful remote fetches)
    pub successful_fetches: u64,
    /// 远程获取失败的次数 (failed remote fetches)
    pub failed_fetches: u64,
    /// 热缓存命中次数 (hot cache hits)
    pub cache_hits: u64,
    /// 传输的总字节数 (total bytes transferred from remote source)
    pub bytes_transferred: u64,
    /// 请求预取的 key 数量 (total keys requested via batch_fetch)
    pub prefetch_keys_requested: u64,
    /// 预取成功的 key 数量 (keys successfully prefetched)
    pub prefetch_keys_succeeded: u64,
    /// 远程获取延迟总和（微秒） (sum of fetch latencies in microseconds)
    pub fetch_latency_us_sum: u64,
    /// 远程获取次数 (number of fetch operations)
    pub fetch_count: u64,
}

/// MissHandler 的快照统计，包含派生指标。
/// (Snapshot statistics for MissHandler, including derived metrics.)
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
    /// 平均远程获取延迟（微秒） (average fetch latency in microseconds)
    pub avg_fetch_latency_us: u64,
    /// 未命中率 = total_misses / (total_misses + cache_hits)
    /// (miss rate: total_misses divided by total requests)
    pub miss_rate: f64,
}

/// 缓存未命中处理器：协调远程获取 + 热缓存 + 请求合并。
/// (Cache-miss handler: coordinates remote fetching, hot caching, and request coalescing.)
///
/// ## 泛型参数 (Generic Parameter)
/// - `S`: 实现 [`RemoteSource`] 的远程源类型
///
/// ## 字段 (Fields)
/// - `source`: 远程数据源（Arc 共享）
/// - `config`: 远程源配置
/// - `inflight`: 正在进行的请求映射表 (key → 等待者列表)
/// - `admission`: 并发控制信号量
/// - `hot_cache`: 可选的热缓存引用
/// - 统计计数器: 全部使用 `AtomicU64` 实现无锁更新
pub struct MissHandler<S: RemoteSource> {
    /// 远程数据源 (remote data source)
    source: Arc<S>,
    /// 配置 (configuration)
    config: RemoteSourceConfig,
    /// 飞行中请求：key → 等待者列表 (inflight requests: key → waiter list)
    inflight: Mutex<HashMap<String, InflightEntry>>,
    /// 并发准入信号量 (concurrency admission semaphore)
    admission: Semaphore,
    /// 可选的热缓存，用于缓存远程获取结果 (optional hot cache for fetched data)
    hot_cache: Option<Arc<LocalHotCache>>,
    // All stats use AtomicU64 for lock-free updates
    // 所有统计计数器使用 AtomicU64，避免锁竞争影响关键路径
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
    /// 创建新的 MissHandler。
    /// (Create a new MissHandler with the given source and config.)
    ///
    /// 信号量初始许可数设为 `config.max_concurrent_fetches`。
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

    /// 附加热缓存：远程获取成功后自动写入缓存。
    /// (Attach a hot cache: fetched data is automatically cached on success.)
    pub fn with_hot_cache(mut self, cache: Arc<LocalHotCache>) -> Self {
        self.hot_cache = Some(cache);
        self
    }

    /// 返回远程源是否启用。
    /// (Returns whether the remote source is enabled.)
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// 返回远程源配置的不可变引用。
    /// (Returns a reference to the remote source config.)
    pub fn config(&self) -> &RemoteSourceConfig {
        &self.config
    }

    /// 处理缓存未命中：查询热缓存 → 远程获取 → 填充热缓存。
    /// (Handle a cache miss: check hot cache → fetch from remote → populate hot cache.)
    ///
    /// ## 流程 (Flow)
    /// 1. **查热缓存**: 命中则直接返回，未命中继续
    /// 2. **检查启用状态**: 未启用则返回 `NotFound`
    /// 3. **检查飞行中请求 (inflight dedup)**:
    ///    - 若无飞行中请求 → 注册为此 key 的获取者 (fetcher)
    ///    - 若已有飞行中请求 → 注册为等待者 (waiter)，通过 oneshot channel 接收结果
    /// 4. **获取信号量许可** (准入控制)
    /// 5. **执行远程获取** `source.get(key)`
    /// 6. **成功时写入热缓存**
    /// 7. **通知所有等待者** 结果
    ///
    /// ## 并发行为 (Concurrency)
    /// 多个并发的 `handle_miss("same_key")` 调用只会触发一次远程获取；
    /// 其余调用等待并复用获取结果（请求合并/去重）。
    pub async fn handle_miss(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        // Step 1: 查热缓存 (check hot cache first)
        if let Some(ref cache) = self.hot_cache {
            if let Some(data) = cache.get(key) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(data);
            }
        }

        // Step 2: 远程源未启用 (remote source disabled)
        if !self.config.enabled {
            return Err(RemoteSourceError::NotFound(key.to_string()));
        }

        // Step 3: 检查/注册飞行中请求 (check/register inflight request)
        {
            let mut inflight = self.inflight.lock().await;
            self.total_misses.fetch_add(1, Ordering::Relaxed);

            if !inflight.contains_key(key) {
                // 此调用者是 key 的"获取者" (this caller is the "fetcher" for this key)
                inflight.insert(key.to_string(), InflightEntry::Pending(Vec::new()));
            } else {
                // 此调用者是"等待者" (this caller is a "waiter")
                self.coalesced_misses.fetch_add(1, Ordering::Relaxed);
                let (tx, rx) = oneshot::channel();
                if let Some(InflightEntry::Pending(waiters)) = inflight.get_mut(key) {
                    waiters.push(tx);
                }
                drop(inflight);
                // 等待获取者完成 (wait for the fetcher to complete)
                let data = rx.await.unwrap_or(Err(RemoteSourceError::Internal(
                    "fetcher dropped".to_string(),
                )))?;
                // 也写入热缓存（后续可直接命中）
                // Also cache for future hits
                if let Some(ref cache) = self.hot_cache {
                    cache.put(key, &data);
                }
                return Ok(data);
            }
        }

        // Step 4: 获取信号量许可 (acquire admission semaphore)
        let _permit =
            self.admission.acquire().await.map_err(|_| {
                RemoteSourceError::Internal("admission semaphore closed".to_string())
            })?;

        // Step 5: 执行远程获取 (execute remote fetch)
        let start = Instant::now();
        let result = self.source.get(key).await;
        let elapsed_us = start.elapsed().as_micros() as u64;

        self.fetch_latency_us_sum
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.fetch_count.fetch_add(1, Ordering::Relaxed);

        // Step 6: 成功时写入热缓存 + 更新统计 (on success: cache + update stats)
        if let Ok(ref data) = result {
            if let Some(ref cache) = self.hot_cache {
                cache.put(key, data);
            }
            self.bytes_transferred
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            self.successful_fetches.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_fetches.fetch_add(1, Ordering::Relaxed);
        }

        // Step 7: 取出等待者列表 + 通知 (retrieve waiters + notify)
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

        // 将结果发送给所有等待者 (send result to all waiters)
        let result_clone = result.clone();
        for waiter in waiters {
            let _ = waiter.send(result_clone.clone());
        }

        result
    }

    /// 批量预取：从远程源获取多个 key 并写入热缓存。
    /// (Batch prefetch: fetch multiple keys from remote source and cache them.)
    ///
    /// 与 `handle_miss` 不同，此方法不进行请求合并/去重，
    /// 直接调用 `source.prefetch_keys`。适用于主动预热 (warmup) 场景。
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
            self.prefetch_keys_succeeded
                .fetch_add(succeeded, Ordering::Relaxed);
        }
    }

    /// 生成当前统计信息的快照（包含派生指标）。
    /// (Create a snapshot of current statistics, including derived metrics.)
    ///
    /// 派生指标 (derived metrics):
    /// - `avg_fetch_latency_us`: `fetch_latency_us_sum / fetch_count`
    /// - `miss_rate`: `total_misses / (total_misses + cache_hits)`
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
            avg_fetch_latency_us: if fetch_n > 0 {
                latency_sum / fetch_n
            } else {
                0
            },
            miss_rate: if total > 0 {
                total as f64 / (total + hits) as f64
            } else {
                0.0
            },
        }
    }

    /// 返回原始统计信息（无派生指标）。
    /// (Returns raw statistics without derived metrics.)
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

    /// 返回远程源的引用。
    /// (Returns a reference to the remote source.)
    pub fn source(&self) -> &Arc<S> {
        &self.source
    }

    /// 返回热缓存的引用（如果已配置）。
    /// (Returns a reference to the hot cache, if configured.)
    pub fn hot_cache(&self) -> Option<&Arc<LocalHotCache>> {
        self.hot_cache.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// 内存中的 RemoteSource 实现，用于单元测试。
    /// (In-memory RemoteSource implementation for unit testing.)
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
        // tokio::spawn on multi-thread runtime → true parallelism
        // 在 multi-thread runtime 上 spawn 可实现真正的并行
        let t1 = tokio::spawn(async move { h1.handle_miss("k").await });
        let t2 = tokio::spawn(async move { h2.handle_miss("k").await });
        let (r1, r2) = tokio::join!(t1, t2);
        assert_eq!(r1.unwrap().unwrap(), b"v");
        assert_eq!(r2.unwrap().unwrap(), b"v");
        let snap = h.snapshot();
        // 两次未命中，但只触发一次远程获取 (2 misses, but only 1 actual fetch)
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
}
