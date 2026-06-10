use super::MooncakeClient;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::ReplicaDescriptor;
use std::ffi::c_void;
use uuid::Uuid;

impl MooncakeClient {
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
        *self.tear_down.read()
    }

    /// Return this client's UUID. / 返回当前客户端 UUID。
    pub fn client_id(&self) -> Uuid {
        self.client_id
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
        *self.tear_down.write() = true;

        // Stop offload RPC server if running.
        if let Some(handle) = self.offload_server_handle.write().take() {
            handle.abort();
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
