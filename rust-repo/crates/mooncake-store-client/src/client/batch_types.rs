use mooncake_store_core::{ReplicaDescriptor, ReplicateConfig};

#[derive(Debug, Clone)]
pub struct BatchPutStartResult {
    pub key: String,
    pub replicas: Vec<ReplicaDescriptor>,
    pub status: i32,
    pub tenant_id: String,
}

/// Entry for a single key in a batch upsert end call.
/// batch_upsert_end 中单个 key 的条目。
///
/// C++ equivalent: each element of the `keys` vector in
/// `MasterClient::BatchUpsertEnd(keys)`.
pub struct BatchUpsertEntry<'a> {
    /// The object key. / 对象 key。
    pub key: &'a str,
    /// Byte length of the data. / 数据的字节长度。
    pub slice_length: u64,
    /// Replication configuration for this key. / 此 key 的副本配置。
    pub config: ReplicateConfig,
    /// Replica type (MEMORY=0, DISK=1, etc.). / 副本类型（MEMORY=0, DISK=1 等）。
    pub replica_type: i32,
    /// Tenant ID for multi-tenancy. / 多租户的租户 ID。
    pub tenant_id: &'a str,
}
