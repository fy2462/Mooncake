use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use uuid::Uuid;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Internal helpers / 内部辅助函数
    // -----------------------------------------------------------------------

    /// Convert Rust UUID to protobuf UUID (high/low u64 pair).
    /// 将 Rust UUID 转换为 protobuf UUID（high/low u64 对）。
    pub(crate) fn client_id_proto(&self) -> proto::Uuid {
        let (h, l) = self.client_id.as_u64_pair();
        proto::Uuid { high: h, low: l }
    }

    /// Query the master for the list of replicas hosting a given key.
    /// Returns an empty vector if the key is not found.
    ///
    /// 向 master 查询持有给定 key 的副本列表。
    /// 如果 key 未找到则返回空向量。
    /// C++ equivalent: Client::GetReplicaList()
    pub(crate) async fn fetch_replicas(
        &mut self,
        key: &str,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let tenant_id = self.tenant_id.clone();
        self.fetch_replicas_for_tenant(key, &tenant_id).await
    }

    pub(crate) async fn fetch_replicas_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        Ok(self
            .fetch_query_response_for_tenant(key, tenant_id)
            .await?
            .replicas)
    }

    pub(crate) async fn fetch_query_response_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
    ) -> StoreResult<super::CachedQueryResultResponse> {
        let query_started_at = std::time::Instant::now();
        let request = proto::GetReplicaListRequest {
            key: key.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .get_replica_list(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);
        Ok(super::CachedQueryResultResponse::success_from(
            query_started_at,
            replicas,
            response.lease_ttl_ms,
        ))
    }

    /// Query the master for replica lists for multiple keys in one RPC.
    /// Results preserve input order and each key carries its own error.
    ///
    /// 批量向 master 查询多个 key 的副本列表。
    /// 返回顺序与输入一致，每个 key 独立携带错误。
    /// C++ equivalent: Client::BatchQuery() → BatchGetReplicaList()
    pub(crate) async fn fetch_batch_replicas(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<StoreResult<Vec<ReplicaDescriptor>>>> {
        let responses = self.fetch_batch_query_responses(keys).await?;
        Ok(responses
            .into_iter()
            .zip(keys.iter())
            .map(|(result, key)| {
                if result.success {
                    Ok(result.replicas)
                } else {
                    match result.error_status {
                        -1 => Err(StoreError::KeyNotFound(key.clone())),
                        -5 => Err(StoreError::ReplicaNotReady),
                        _ => Err(StoreError::Internal(result.error_message)),
                    }
                }
            })
            .collect())
    }

    pub(crate) async fn fetch_batch_query_responses(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<super::CachedQueryResultResponse>> {
        let query_started_at = std::time::Instant::now();
        let request = proto::BatchGetReplicaListRequest {
            keys: keys.to_vec(),
            tenant_id: self.tenant_id.clone(),
        };
        let response = self
            .master
            .batch_get_replica_list(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.results.len() != keys.len() {
            return Err(StoreError::Internal(format!(
                "BatchGetReplicaList response size mismatch: expected {}, got {}",
                keys.len(),
                response.results.len()
            )));
        }

        let results = response
            .results
            .into_iter()
            .map(|result| match result.status {
                0 => match result.response {
                    Some(response) => super::CachedQueryResultResponse::success_from(
                        query_started_at,
                        self.replicas_from_proto(&response.replicas),
                        response.lease_ttl_ms,
                    ),
                    None => super::CachedQueryResultResponse::failure(
                        -2,
                        "missing replica response for successful query",
                    ),
                },
                status => super::CachedQueryResultResponse::failure(status, result.error_message),
            })
            .collect();
        Ok(results)
    }

    /// Convert protobuf replica descriptors into domain [`ReplicaDescriptor`]s.
    ///
    /// Maps proto enums:
    /// - `status`: 1=Allocating, 2=Written, 3=Complete, 4=Failed
    /// - `replica_type`: 1=Disk, 2=LocalDisk, 3=NoFSsd, default=Memory
    ///
    /// 将 protobuf 副本描述符转换为领域 ReplicaDescriptor。
    /// 映射 proto 枚举：status (1=Allocating, 2=Written, 3=Complete, 4=Failed)
    /// 和 replica_type (1=Disk, 2=LocalDisk, 3=NoFSsd, 默认=Memory)。
    pub(crate) fn replicas_from_proto(
        &self,
        replicas: &[proto::ReplicaDescriptor],
    ) -> Vec<ReplicaDescriptor> {
        replicas
            .iter()
            .filter_map(|r| {
                let sid = r.segment_id.as_ref()?;
                Some(ReplicaDescriptor {
                    refcnt: 0,
                    segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                    segment_name: match mooncake_store_core::ReplicaType::from_replica_wire(
                        r.replica_type,
                    ) {
                        mooncake_store_core::ReplicaType::Disk if !r.file_path.is_empty() => {
                            r.file_path.clone()
                        }
                        mooncake_store_core::ReplicaType::Memory
                            if !r.transport_endpoint.is_empty() =>
                        {
                            r.transport_endpoint.clone()
                        }
                        _ => r.segment_name.clone(),
                    },
                    offset: r.offset,
                    size: r.size,
                    base_addr: r.base_addr,
                    protocol: r.protocol.clone(),
                    status: mooncake_store_core::ReplicaStatus::from_replica_wire(r.status),
                    replica_type: mooncake_store_core::ReplicaType::from_replica_wire(
                        r.replica_type,
                    ),
                    holder_client_id: r
                        .holder_client_id
                        .as_ref()
                        .map(|id| Uuid::from_u64_pair(id.high, id.low)),
                    local_disk_storage_id: r
                        .local_disk_storage_id
                        .as_ref()
                        .map(|id| Uuid::from_u64_pair(id.high, id.low)),
                    local_disk_generation_id: r
                        .local_disk_generation_id
                        .as_ref()
                        .map(|id| Uuid::from_u64_pair(id.high, id.low)),
                    handle_valid: true,
                })
            })
            .collect()
    }

    /// Select the best replica from a list, matching C++ `SelectBestReplica`.
    ///
    /// # Priority order (优先级顺序 — C++ `real_client.cpp:286-325`)
    ///
    /// | Priority | Replica Type | Locality  | Behavior                        |
    /// |----------|-------------|-----------|----------------------------------|
    /// | 1        | MEMORY      | Local     | Return immediately (最优)       |
    /// | 2        | MEMORY      | Remote    | First seen, or lowest score when enabled |
    /// | 3        | NOF_SSD     | Local     | Return immediately (次优)       |
    /// | 4        | NOF_SSD     | Remote    | First seen (任意远程 NOF_SSD)    |
    /// | 5        | LOCAL_DISK  | —         | Last one wins (覆盖 DISK)       |
    /// | 6        | DISK        | —         | Only if no LOCAL_DISK found      |
    ///
    /// # Algorithm (算法)
    ///
    /// **Pass 1** — scan for MEMORY and NOF_SSD:
    /// - If a local MEMORY replica is found, return it immediately (short-circuit).
    /// - If a local NOF_SSD replica is found, return it immediately.
    /// - Otherwise, remember the first remote MEMORY and first remote NOF_SSD.
    /// - When scoring is enabled, choose the lowest-scoring remote MEMORY;
    ///   equal scores retain master return order.
    ///
    /// **第一遍** —— 扫描 MEMORY 和 NOF_SSD：
    /// - 找到本地 MEMORY 副本则立即返回（短路）。
    /// - 找到本地 NOF_SSD 副本则立即返回。
    /// - 否则记住第一个远程 MEMORY 和第一个远程 NOF_SSD。
    /// - 启用评分时，选择分数最低的远程 MEMORY；同分保持 master 返回顺序。
    ///
    /// **Pass 2** — if no MEMORY/NOF_SSD found, scan for disk-based replicas:
    /// - LOCAL_DISK always overwrites any previous disk pick.
    /// - DISK is only chosen if no LOCAL_DISK was found.
    ///
    /// **第二遍** —— 如果没有找到 MEMORY/NOF_SSD，扫描基于磁盘的副本：
    /// - LOCAL_DISK 总是覆盖之前的磁盘选择。
    /// - DISK 仅在未找到任何 LOCAL_DISK 时被选择。
    ///
    /// Only replicas with `status == Complete` are considered.
    /// 仅考虑 status == Complete 的副本。
    ///
    /// 从副本列表中选择最优副本，完全匹配 C++ `SelectBestReplica` 逻辑。
    pub(crate) fn select_best_replica<'a>(
        &self,
        replicas: &'a [ReplicaDescriptor],
    ) -> Option<&'a ReplicaDescriptor> {
        let endpoints = self.local_endpoints.read();
        super::replica_selection::select_best_replica(
            replicas,
            &endpoints,
            &self.replica_selection_policy,
        )
    }
}
