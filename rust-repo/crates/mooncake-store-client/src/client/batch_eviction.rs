use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // EvictDiskReplica — evict a single key's disk replica
    // 驱逐单个 key 的磁盘副本
    //
    // C++ equivalent: MasterClient::EvictDiskReplica(key, replica_type)
    // -----------------------------------------------------------------------

    /// Notify the master that a disk replica for a single key was evicted
    /// locally. The master removes the corresponding replica from its metadata.
    ///
    /// 通知 master 单个 key 的磁盘副本已在本地被驱逐。
    /// Master 从元数据中移除对应的副本。
    pub async fn evict_disk_replica(
        &mut self,
        key: &str,
        replica_type: i32,
        tenant_id: &str,
    ) -> StoreResult<()> {
        let request = proto::EvictDiskReplicaRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type,
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .evict_disk_replica(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // BatchEvictDiskReplica — batch evict disk replicas
    // 批量驱逐磁盘副本
    //
    // C++ equivalent: MasterClient::BatchEvictDiskReplica(keys, replica_type)
    // -----------------------------------------------------------------------

    /// Notify the master that disk replicas for multiple keys were evicted
    /// locally. All keys share the same replica_type filter.
    ///
    /// 通知 master 多个 key 的磁盘副本已在本地被驱逐。
    /// 所有 key 共享同一个 replica_type 过滤器。
    pub async fn batch_evict_disk_replica(
        &mut self,
        keys: &[String],
        replica_type: i32,
        tenant_id: &str,
    ) -> StoreResult<Vec<i32>> {
        let request = proto::BatchEvictDiskReplicaRequest {
            client_id: Some(self.client_id_proto()),
            keys: keys.to_vec(),
            replica_type,
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .batch_evict_disk_replica(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.statuses.len() != keys.len() {
            return Err(mooncake_store_core::StoreError::Internal(format!(
                "BatchEvictDiskReplica returned {} statuses for {} keys",
                response.statuses.len(),
                keys.len()
            )));
        }
        Ok(response.statuses)
    }
}
