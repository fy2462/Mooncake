use super::{EngramClient, EngramStore};
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicateConfig, StoreError};

impl<C: EngramClient> EngramStore<C> {
    /// 从存储中移除所有嵌入表数据。
    /// (Remove all embedding table data from the store.)
    ///
    /// 逐个删除每个 head 对应的 key，忽略 `KeyNotFound` 错误。
    /// 返回成功删除的 key 数量。
    pub async fn remove_from_store(&mut self) -> StoreResult<usize> {
        let mut removed = 0usize;
        let mut first_error = None;

        for key in &self.embed_keys {
            match self.store.remove(key).await {
                Ok(()) => removed += 1,
                Err(StoreError::KeyNotFound(_)) => {} // 允许 key 不存在 (key might not exist yet)
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(removed),
        }
    }

    /// 将嵌入数据批量写入存储。
    /// (Populate the store with embedding data from buffers.)
    ///
    /// ## 参数 (Parameters)
    /// - `embedding_buffers`: 每个 head 一个 buffer，大小需等于 `vocab_size[head] * dim * sizeof(f32)`
    ///
    /// ## 流程 (Flow)
    /// 1. 验证 buffer 大小
    /// 2. 检查 key 是否已存在 → 若存在则返回 `ObjectExists` 错误
    /// 3. 注册所有 buffer 到 RDMA（失败时回滚已注册的）
    /// 4. 调用 `batch_put_from` 批量写入
    /// 5. 注销所有 buffer
    /// 6. 若写入失败 → 回滚：删除已写入的 key
    ///
    /// ## 事务语义 (Transactional Semantics)
    /// 尽力保证原子性：写入失败时会清理已写入数据，但非严格 ACID。
    pub async fn populate(&mut self, embedding_buffers: &[&[u8]]) -> StoreResult<()> {
        if embedding_buffers.len() != self.embed_keys.len() {
            return Err(StoreError::InvalidParams(format!(
                "embedding_buffers length must equal number of heads ({})",
                self.embed_keys.len()
            )));
        }

        // 验证每个 buffer 大小 (validate buffer sizes)
        for (head_id, buffer) in embedding_buffers.iter().enumerate() {
            let expected = self.table_vocab_sizes[head_id] as usize
                * self.embedding_dim
                * std::mem::size_of::<f32>();
            if buffer.len() != expected {
                return Err(StoreError::InvalidParams(format!(
                    "buffer size mismatch for head {head_id}: expected {expected}, got {}",
                    buffer.len()
                )));
            }
        }

        // 检查 key 是否已存在 (check for pre-existing keys)
        let exists_results = self.store.batch_is_exist(&self.embed_keys).await?;
        if exists_results.len() != self.embed_keys.len() {
            return Err(StoreError::Internal(
                "batch_is_exist returned unexpected result length".to_string(),
            ));
        }
        for (head_id, exists) in exists_results.iter().enumerate() {
            if *exists {
                return Err(StoreError::ObjectExists(self.embed_keys[head_id].clone()));
            }
        }

        // 注册所有 buffer 到 RDMA (register all buffers for RDMA transfer)
        for (index, buffer) in embedding_buffers.iter().enumerate() {
            if let Err(err) = self.store.register_buffer(buffer, &self.buffer_location) {
                // 回滚已注册的 buffer (rollback already-registered buffers)
                for registered in &embedding_buffers[..index] {
                    let _ = self.store.unregister_buffer(registered);
                }
                return Err(StoreError::Internal(format!(
                    "failed to register embedding buffer {index}: {err}"
                )));
            }
        }

        // 批量写入 (batch write)
        let put_results = self
            .store
            .batch_put_from(
                &self.embed_keys,
                embedding_buffers,
                Some(ReplicateConfig::default()),
            )
            .await;
        let unregister_errors = embedding_buffers
            .iter()
            .filter_map(|buf| self.store.unregister_buffer(buf).err())
            .collect::<Vec<_>>();

        let put_results = put_results?;
        let put_succeeded = put_results.len() == self.embed_keys.len()
            && put_results.iter().all(|result| *result == 0);

        // 失败时回滚 (rollback on failure)
        if !put_succeeded || !unregister_errors.is_empty() {
            for key in &self.embed_keys {
                let _ = self.store.remove(key).await;
            }
            if let Some(err) = unregister_errors.into_iter().next() {
                return Err(StoreError::Internal(format!(
                    "populate rolled back because buffer cleanup failed: {err}"
                )));
            }
            return Err(StoreError::Internal(format!(
                "populate failed with statuses: {:?}",
                put_results
            )));
        }

        Ok(())
    }
}
