// ---------------------------------------------------------------------------
// BufferHandle — owned get result (key + data + size triple)
// 拥有所有权的读取结果句柄，包含 key、data 和 size 三元组
// ---------------------------------------------------------------------------

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
