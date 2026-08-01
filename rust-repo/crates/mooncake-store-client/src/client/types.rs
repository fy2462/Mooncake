// ---------------------------------------------------------------------------
// BufferHandle — owned get result (key + data + size triple)
// 拥有所有权的读取结果句柄，包含 key、data 和 size 三元组
// ---------------------------------------------------------------------------

/// C++-compatible non-blocking client health disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ClientHealthStatus {
    Healthy = 0,
    NotInitialized = 1,
    MasterUnreachable = 2,
}

/// Owned buffer returned by [`get_buffer`](MooncakeClient::get_buffer).
///
/// Unlike the low-level `get()` which returns a raw `Vec<u8>`, this struct
/// bundles the key and byte-length together so the caller does not have to
/// track them separately.
///
/// 与返回裸 Vec<u8> 的低级 get() 不同，BufferHandle 将 key 和字节长度绑定在一起，
/// 调用者无需单独追踪这些信息。对应 C++ 的 `BufferHandle` 结构体。
pub struct BufferHandle {
    /// The raw payload bytes. / 原始负载字节。
    pub data: Vec<u8>,
    /// The key this data was fetched for. / 此数据对应的 key。
    pub key: String,
    /// Byte length of the payload (== data.len()). / 负载的字节长度。
    pub size: usize,
}

/// Cached response for a replica-list query.
///
/// This mirrors the C++ `CachedQueryResultResponse`: callers can batch-query
/// placement metadata once, then pass the cache into ranged reads to avoid
/// repeated master RPCs while the lease is still valid.
#[derive(Debug, Clone)]
pub struct CachedQueryResultResponse {
    pub success: bool,
    pub replicas: Vec<mooncake_store_core::ReplicaDescriptor>,
    pub lease_valid_until: std::time::Instant,
    pub error_status: i32,
    pub error_message: String,
}

impl CachedQueryResultResponse {
    pub fn success(
        replicas: Vec<mooncake_store_core::ReplicaDescriptor>,
        lease_ttl_ms: u64,
    ) -> Self {
        Self::success_from(std::time::Instant::now(), replicas, lease_ttl_ms)
    }

    pub(crate) fn success_from(
        query_started_at: std::time::Instant,
        replicas: Vec<mooncake_store_core::ReplicaDescriptor>,
        lease_ttl_ms: u64,
    ) -> Self {
        Self {
            success: true,
            replicas,
            lease_valid_until: query_started_at + std::time::Duration::from_millis(lease_ttl_ms),
            error_status: 0,
            error_message: String::new(),
        }
    }

    pub fn failure(error_status: i32, error_message: impl Into<String>) -> Self {
        Self {
            success: false,
            replicas: Vec::new(),
            lease_valid_until: std::time::Instant::now(),
            error_status,
            error_message: error_message.into(),
        }
    }

    pub fn is_lease_expired(&self) -> bool {
        std::time::Instant::now() >= self.lease_valid_until
    }
}
