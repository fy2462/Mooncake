use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::ReplicaDescriptor;
use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use uuid::Uuid;

impl MooncakeClient {
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

    /// Query replica lists for multiple keys using one BatchGetReplicaList RPC.
    pub async fn batch_get_replica_list(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<Vec<ReplicaDescriptor>>> {
        self.fetch_batch_replicas(keys)
            .await?
            .into_iter()
            .collect::<StoreResult<Vec<_>>>()
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

    /// Returns `true` if the client has been torn down. / 如果客户端已关闭则返回 true。
    pub fn is_closed(&self) -> bool {
        self.shutdown_state.is_closed()
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

        // Stop publishing/using the Store segment before releasing its CUDA
        // registration and backing allocation.
        if !self.segment_name.is_empty() {
            let segment_id = {
                self.mounted_segment_ids
                    .read()
                    .get(&self.segment_name)
                    .copied()
            };
            if let Some(segment_id) = segment_id {
                let result = self
                    .master
                    .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                        segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
                        client_id: Some(self.client_id_proto()),
                    }))
                    .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "failed to unmount Store segment during teardown");
                }
            }
            if let Err(error) = self.engine.remove_local_segment(&self.segment_name) {
                tracing::warn!(%error, "failed to remove local Transfer Engine segment");
            }
        }

        if let Some(mut segment_buffer) = self.segment_buffer.take() {
            let te_unregistered = unsafe {
                self.engine
                    .unregister_local_memory(segment_buffer.as_ptr() as *mut c_void)
            };
            if let Err(error) = te_unregistered {
                tracing::error!(
                    %error,
                    "leaking Store segment because Transfer Engine unregister failed"
                );
                std::mem::forget(segment_buffer);
            } else if !segment_buffer.release() {
                tracing::error!("leaking Store segment because CUDA host unregister failed");
            }
        }

        // unregister local buffer / 取消注册本地缓冲区
        unsafe {
            let _ = self
                .engine
                .unregister_local_memory(self.local_buffer.as_ptr() as *mut c_void);
        }

        // unregister all user-registered buffers / 取消注册所有用户注册的缓冲区
        let ptrs: Vec<usize> = self.registered_buffers.read().keys().copied().collect();
        for ptr in &ptrs {
            unsafe {
                let _ = self.engine.unregister_local_memory(*ptr as *mut c_void);
            }
        }
        self.registered_buffers.write().clear();
        Ok(())
    }
}
