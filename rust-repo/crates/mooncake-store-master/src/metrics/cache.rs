use lazy_static::lazy_static;
use prometheus::{IntCounter, IntGauge};

// =============================================================================
// Cache hit counters — 缓存命中计数器
// =============================================================================

lazy_static! {
    pub static ref MEM_CACHE_HITS: IntCounter = IntCounter::new(
        "mooncake_store_mem_cache_hits_total",
        "total memory cache hits"
    )
    .unwrap();
    pub static ref FILE_CACHE_HITS: IntCounter = IntCounter::new(
        "mooncake_store_file_cache_hits_total",
        "total file cache hits"
    )
    .unwrap();
    pub static ref MEM_CACHE_HIT_BYTES: IntCounter = IntCounter::new(
        "mooncake_store_mem_cache_hit_bytes_total",
        "total bytes served from memory cache hits"
    )
    .unwrap();
    pub static ref FILE_CACHE_HIT_BYTES: IntCounter = IntCounter::new(
        "mooncake_store_file_cache_hit_bytes_total",
        "total bytes served from file cache hits"
    )
    .unwrap();
    pub static ref MEM_CACHE_TOTAL: IntGauge = IntGauge::new(
        "mooncake_store_mem_cache_objects",
        "current objects with completed memory cache replicas"
    )
    .unwrap();
    pub static ref FILE_CACHE_TOTAL: IntGauge = IntGauge::new(
        "mooncake_store_file_cache_objects",
        "current objects with completed file cache replicas"
    )
    .unwrap();
    pub static ref VALID_GETS: IntCounter = IntCounter::new(
        "mooncake_store_valid_gets_total",
        "total valid get requests"
    )
    .unwrap();
    pub static ref TOTAL_GETS: IntCounter = IntCounter::new(
        "mooncake_store_total_gets_total",
        "total scalar and batch replica-list key lookups"
    )
    .unwrap();
}
