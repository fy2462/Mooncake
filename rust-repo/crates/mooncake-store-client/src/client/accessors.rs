use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::collections::HashMap;
use uuid::Uuid;

fn expected_query_tenant(enable_tenant_scope: bool, configured_tenant: &str) -> &str {
    if !enable_tenant_scope || configured_tenant.is_empty() {
        "default"
    } else {
        configured_tenant
    }
}

fn validate_regex_identity(
    expected_tenant: &str,
    scoped_key: &str,
    tenant_id: &str,
    user_key: &str,
) -> StoreResult<()> {
    if tenant_id != expected_tenant || user_key.is_empty() {
        return Err(StoreError::Internal(format!(
            "QueryByRegex returned invalid identity: tenant={tenant_id:?}, user_key={user_key:?}"
        )));
    }
    let expected_scoped_key = format!("{tenant_id}\0{user_key}");
    if scoped_key != expected_scoped_key {
        return Err(StoreError::Internal(format!(
            "QueryByRegex returned inconsistent scoped identity: key={scoped_key:?}, expected={expected_scoped_key:?}"
        )));
    }
    Ok(())
}

impl MooncakeClient {
    /// Capacity of the TE-registered scratch allocation configured at Client
    /// creation. Buffer-pool adapters use this only to match the C++ default
    /// capacity policy; they never suballocate from the scratch lease.
    pub fn local_buffer_capacity(&self) -> usize {
        self.local_buffer.len()
    }

    /// Configure remote MEMORY selection while building a client value.
    pub fn with_replica_selection_policy(mut self, policy: super::ReplicaSelectionPolicy) -> Self {
        self.replica_selection_policy = policy;
        self
    }

    /// Replace this client's remote MEMORY selection policy.
    pub fn set_replica_selection_policy(&mut self, policy: super::ReplicaSelectionPolicy) {
        self.replica_selection_policy = policy;
    }

    /// Query the master for the list of replicas hosting a given key,
    /// without fetching the data. Returns an empty vector if the key is
    /// not found.
    ///
    /// C++ equivalent: `Client::Query` / `GetReplicaList`
    ///
    /// 查询 master 获取某个 key 的副本列表，但不获取数据。
    /// 如果 key 未找到则返回空向量。
    pub async fn get_replica_list(&mut self, key: &str) -> StoreResult<Vec<ReplicaDescriptor>> {
        self.fetch_replicas(key).await
    }

    /// Query one object's placement metadata while preserving its lease TTL
    /// and per-key status.
    ///
    /// C++ equivalent: `Client::Query`.
    pub async fn query(&mut self, key: &str) -> StoreResult<super::CachedQueryResultResponse> {
        let mut results = self.fetch_batch_query_responses(&[key.to_string()]).await?;
        if results.len() != 1 {
            return Err(StoreError::Internal(format!(
                "single-key Query returned {} results",
                results.len()
            )));
        }
        Ok(results.remove(0))
    }

    /// Query replica lists for multiple keys using one BatchGetReplicaList RPC.
    pub async fn batch_get_replica_list(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<Vec<ReplicaDescriptor>>> {
        self.batch_get_replica_list_results(keys)
            .await?
            .into_iter()
            .collect::<StoreResult<Vec<_>>>()
    }

    /// Query replica lists in one RPC while retaining each key's result.
    pub async fn batch_get_replica_list_results(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<StoreResult<Vec<ReplicaDescriptor>>>> {
        self.fetch_batch_replicas(keys).await
    }

    /// Batch-query replica placement metadata and preserve lease TTL.
    ///
    /// C++ equivalent: `RealClient::batch_get_query_results`.
    pub async fn batch_get_query_results(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<super::CachedQueryResultResponse>> {
        self.fetch_batch_query_responses(keys).await
    }

    /// C++-named alias for batch placement queries with per-key status and
    /// lease TTL.
    pub async fn batch_query(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<super::CachedQueryResultResponse>> {
        self.batch_get_query_results(keys).await
    }

    /// Query routable objects whose user keys match a regular expression.
    ///
    /// This deliberately uses `GetReplicaListByRegex`, not the diagnostic
    /// `QueryByRegex` RPC: C++ `Client::QueryByRegex` returns only Complete,
    /// routable replicas and refreshes their leases.
    pub async fn query_by_regex(
        &mut self,
        pattern: &str,
    ) -> StoreResult<HashMap<String, Vec<ReplicaDescriptor>>> {
        let tenant_id = self.tenant_id.clone();
        let expected_tenant = expected_query_tenant(self.enable_tenant_scope, &tenant_id);
        let response = self
            .master
            .get_replica_list_by_regex(self.rpc_request(proto::GetReplicaListByRegexRequest {
                key_regex: pattern.to_string(),
                tenant_id: tenant_id.clone(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let mut matches = HashMap::with_capacity(response.entries.len());
        for entry in response.entries {
            validate_regex_identity(
                expected_tenant,
                &entry.key,
                &entry.tenant_id,
                &entry.user_key,
            )?;
            let user_key = entry.user_key;
            let replicas = self.replicas_from_proto(&entry.replicas);
            if matches.insert(user_key.clone(), replicas).is_some() {
                return Err(StoreError::Internal(format!(
                    "QueryByRegex returned duplicate key {user_key:?}"
                )));
            }
        }
        Ok(matches)
    }

    /// Returns `true` if the client has been torn down. / 如果客户端已关闭则返回 true。
    pub fn is_closed(&self) -> bool {
        self.shutdown_state.is_closed()
    }

    /// Return the C++ public health code without performing network I/O.
    pub fn health_status(&self) -> super::ClientHealthStatus {
        if self.shutdown_state.is_closed() {
            super::ClientHealthStatus::NotInitialized
        } else if self.health_state.is_healthy() {
            super::ClientHealthStatus::Healthy
        } else {
            super::ClientHealthStatus::MasterUnreachable
        }
    }

    /// Resolve the health of an optional client slot used by language bindings.
    pub fn health_status_for(client: Option<&Self>) -> super::ClientHealthStatus {
        client.map_or(
            super::ClientHealthStatus::NotInitialized,
            Self::health_status,
        )
    }

    /// Return this client's UUID. / 返回当前客户端 UUID。
    pub fn client_id(&self) -> Uuid {
        self.client_id
    }

    /// Return the default tenant used by convenience APIs.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Bound client health/metrics HTTP port, if the optional server started.
    pub fn client_http_port(&self) -> Option<u16> {
        self.client_http_server_state.port()
    }

    /// Serialize this client's persistent Prometheus registry.
    ///
    /// Matches C++ `Client::SerializeMetrics`; when
    /// `MC_STORE_CLIENT_METRIC=0`, metrics are intentionally unavailable.
    pub fn serialize_metrics(&self) -> StoreResult<String> {
        let metrics = self
            .metrics
            .as_ref()
            .ok_or_else(|| StoreError::InvalidParams("client metrics are disabled".to_string()))?;
        let body = metrics.render_prometheus(
            self.health_state.is_healthy(),
            self.shutdown_state.is_closed(),
        )?;
        String::from_utf8(body)
            .map_err(|error| StoreError::Internal(format!("invalid metrics UTF-8: {error}")))
    }

    /// Return the human-readable metrics summary exposed by C++ clients.
    pub fn summary_metrics(&self) -> StoreResult<String> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.summary())
            .ok_or_else(|| StoreError::InvalidParams("client metrics are disabled".to_string()))
    }

    /// Tear down the client: set the shutdown flag, unregister the local buffer
    /// and all user-registered buffers from the TransferEngine.
    ///
    /// After this call the client should not be used for further operations.
    ///
    /// 关闭客户端：设置关闭标志，从 TransferEngine 中取消注册本地缓冲区和
    /// 所有用户注册的缓冲区。此调用后客户端不应再用于任何操作。
    /// C++ 等价：`Client::TearDownAll()`。
    pub async fn tear_down_all(&mut self) -> StoreResult<()> {
        self.shutdown_state.close();

        // Stop offload RPC server if running.
        self.offload_server_state.stop();
        self.client_http_server_state.stop();
        self.metrics_reporter_state.stop();

        // Stop publishing every UUID before releasing any backing allocation.
        let mounted_segments = self
            .mounted_segment_ids
            .read()
            .iter()
            .map(|(id, name)| (*id, name.clone()))
            .collect::<Vec<_>>();
        let mut failed_segment_ids = std::collections::HashSet::new();
        let mut failed_segment_names = std::collections::HashSet::new();
        for (segment_id, segment_name) in &mounted_segments {
            let result = self
                .master
                .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                    segment_id: Some(Self::uuid_to_proto_uuid(*segment_id)),
                    client_id: Some(self.client_id_proto()),
                }))
                .await;
            if let Err(error) = result {
                failed_segment_ids.insert(*segment_id);
                failed_segment_names.insert(segment_name.clone());
                tracing::warn!(
                    %error,
                    %segment_id,
                    %segment_name,
                    "failed to unmount Store segment during teardown"
                );
            }
        }
        let segment_names = self
            .owned_store_segments
            .iter()
            .map(|segment| segment.segment_name.clone())
            .collect::<std::collections::HashSet<_>>();
        let mut retained_segment_names = failed_segment_names.clone();
        self.mounted_segment_ids.write().clear();
        self.mounted_external_segments.write().clear();

        if let Some(registration) = self.cxl_segment_registration.take() {
            if failed_segment_names.contains(&self.local_hostname) {
                tracing::error!(
                    "leaking native CXL registration because Master unmount was not proven"
                );
                std::mem::forget(registration);
            } else if let Ok(engine) = self.engine.required_arc()
                && let Err(error) =
                    crate::memory_ffi::unregister_cxl_segment(&engine, &registration)
            {
                tracing::error!(%error, "failed to unregister native CXL segment");
            }
        }

        for mut segment in std::mem::take(&mut self.owned_store_segments) {
            if failed_segment_ids.contains(&segment.segment_id) {
                tracing::error!(
                    segment_id = %segment.segment_id,
                    "leaking Store segment because Master unmount was not proven"
                );
                std::mem::forget(segment);
                continue;
            }
            let te_unregistered: StoreResult<()> = match self.engine.required_arc() {
                Ok(engine) => crate::memory_ffi::unregister_local_memory(&engine, &segment.buffer),
                Err(error) => Err(StoreError::from(error)),
            };
            if let Err(error) = te_unregistered {
                retained_segment_names.insert(segment.segment_name.clone());
                tracing::error!(
                    %error,
                    segment_id = %segment.segment_id,
                    "leaking Store segment because Transfer Engine unregister failed"
                );
                std::mem::forget(segment);
            } else if !segment.buffer.release() {
                tracing::error!("leaking Store segment because CUDA host unregister failed");
            }
        }

        // unregister local buffer / 取消注册本地缓冲区
        self.local_buffer.wait_until_available().await;
        if let Some(mut registration) = self.local_buffer.take_registration() {
            if let Err(error) = self.engine.unregister_owned_memory(&mut registration) {
                retained_segment_names.insert(self.local_hostname.clone());
                tracing::error!(
                    %error,
                    "typed staging-buffer teardown failed; RAII will retain or leak its owner safely"
                );
            }
        }

        // unregister all user-registered buffers / 取消注册所有用户注册的缓冲区
        let registrations = std::mem::take(&mut *self.registered_buffers.write());
        for mut registration in registrations.into_values() {
            if let Err(error) = self.engine.unregister_owned_memory(&mut registration) {
                retained_segment_names.insert(self.local_hostname.clone());
                tracing::error!(
                    %error,
                    registration = ?registration,
                    "typed registered-memory teardown failed; RAII will retain or leak its owner safely"
                );
            }
        }

        // Native Transfer Engine memory registrations retain metadata owned by
        // their local segment. Remove the segment only after every backing
        // registration has been unregistered; reversing this order can leave
        // unregisterLocalMemory dereferencing already-removed metadata.
        for segment_name in segment_names {
            if retained_segment_names.contains(&segment_name) {
                tracing::error!(
                    %segment_name,
                    "retaining local Transfer Engine segment because teardown was not proven"
                );
                continue;
            }
            if let Err(error) = self.engine.remove_local_segment(&segment_name) {
                tracing::warn!(
                    %error,
                    %segment_name,
                    "failed to remove local Transfer Engine segment"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{expected_query_tenant, validate_regex_identity};

    #[test]
    fn query_tenant_matches_master_normalization() {
        assert_eq!(expected_query_tenant(false, "tenant-a"), "default");
        assert_eq!(expected_query_tenant(true, ""), "default");
        assert_eq!(expected_query_tenant(true, "tenant-a"), "tenant-a");
    }

    #[test]
    fn regex_identity_requires_matching_tenant_and_scoped_key() {
        validate_regex_identity("tenant-a", "tenant-a\0key", "tenant-a", "key").unwrap();
        assert!(validate_regex_identity("tenant-a", "tenant-b\0key", "tenant-b", "key").is_err());
        assert!(validate_regex_identity("tenant-a", "tenant-a\0other", "tenant-a", "key").is_err());
    }
}
