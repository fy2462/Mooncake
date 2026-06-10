use super::storage::local_storage_key;
use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::sync::Arc;
use uuid::Uuid;

impl MooncakeClient {
    /// Execute a complete offload cycle:
    /// 1. Heartbeat to get objects-to-offload from master.
    /// 2. Read object data from memory.
    /// 3. Write data to local disk.
    /// 4. Notify master of success.
    ///
    /// Requires a [`LocalStorageBackend`] to be attached via
    /// [`with_local_storage_backend`](Self::with_local_storage_backend).
    ///
    /// Returns the number of objects successfully offloaded.
    ///
    /// 执行完整的 offload 循环：
    /// 1. 心跳获取待 offload 对象。
    /// 2. 从内存读取对象数据。
    /// 3. 将数据写入本地磁盘。
    /// 4. 通知 master 成功。
    ///
    /// 需要先通过 with_local_storage_backend 挂载本地存储后端。
    ///
    /// 返回成功 offload 的对象数量。
    pub async fn offload_objects(&mut self, enable_offloading: bool) -> StoreResult<usize> {
        let tasks = self
            .offload_object_heartbeat_tasks(enable_offloading)
            .await?;
        if tasks.is_empty() {
            return Ok(0);
        }

        if self.offload_rpc_address().is_empty() {
            let _ = self.start_offload_server().await?;
        }
        let transport_endpoint = self.offload_rpc_address();

        let storage = self.local_storage.as_ref().ok_or_else(|| {
            StoreError::Internal("no local storage backend configured".to_string())
        })?;
        let storage = Arc::clone(storage);

        let mut offloaded = 0usize;
        let mut success_tasks = Vec::with_capacity(tasks.len());
        let mut metadatas = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let key = task.key.as_str();
            let tenant_id = task.tenant_id.as_str();
            if task.size < 0 {
                tracing::warn!(target: "storage_debug", %tenant_id, %key, size = task.size, "offload: invalid negative task size");
                continue;
            }
            // Read object data from memory.
            let data = match self.get_for_tenant(key, tenant_id).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: failed to get object from memory, skipping");
                    continue;
                }
            };

            // Write to local disk (blocking I/O).
            let key_owned = local_storage_key(tenant_id, key);
            let s = Arc::clone(&storage);
            let write_result =
                tokio::task::spawn_blocking(move || s.write_object(&key_owned, &data))
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))??;

            // Log any evicted keys.
            for evicted_key in &write_result {
                tracing::info!(target: "storage_debug", %evicted_key, "offload: evicted old file");
            }

            offloaded += 1;
            success_tasks.push(task.clone());
            metadatas.push(proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: task.size,
                transport_endpoint: transport_endpoint.clone(),
            });
        }

        if !success_tasks.is_empty() {
            self.notify_offload_success_tasks(success_tasks, metadatas)
                .await?;
        }

        Ok(offloaded)
    }

    /// Execute a complete promotion cycle:
    /// 1. Heartbeat to get objects-to-promote from master.
    /// 2. Read data from local disk.
    /// 3. Allocate a memory replica via `promotion_alloc_start`.
    /// 4. Write data to the allocated replica via `write_to_replica`.
    /// 5. Notify master of success or failure.
    ///
    /// Requires a [`LocalStorageBackend`] to be attached via
    /// [`with_local_storage_backend`](Self::with_local_storage_backend).
    ///
    /// Returns the number of objects successfully promoted.
    ///
    /// 执行完整的 promotion 循环：
    /// 1. 心跳获取待 promotion 对象。
    /// 2. 从本地磁盘读取数据。
    /// 3. 通过 promotion_alloc_start 分配内存副本。
    /// 4. 通过 write_to_replica 将数据写入分配的副本。
    /// 5. 通知 master 成功或失败。
    ///
    /// 需要先通过 with_local_storage_backend 挂载本地存储后端。
    ///
    /// 返回成功 promotion 的对象数量。
    pub async fn promote_objects(&mut self) -> StoreResult<usize> {
        let tasks = self.promotion_object_heartbeat_tasks().await?;
        if tasks.is_empty() {
            return Ok(0);
        }

        let storage = self.local_storage.as_ref().ok_or_else(|| {
            StoreError::Internal("no local storage backend configured".to_string())
        })?;
        let storage = Arc::clone(storage);

        let mut promoted = 0usize;

        for task in &tasks {
            let key = task.key.as_str();
            let tenant_id = task.tenant_id.as_str();
            if task.size < 0 {
                tracing::warn!(target: "storage_debug", %tenant_id, %key, size = task.size, "promotion: invalid negative task size");
                let _ = self
                    .notify_promotion_failure_for_tenant(key, tenant_id)
                    .await;
                continue;
            }
            // Read from local disk (blocking I/O).
            let key_owned = local_storage_key(tenant_id, key);
            let data = {
                let s = Arc::clone(&storage);
                tokio::task::spawn_blocking(move || s.read_object(&key_owned))
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))??
            };

            // Allocate a memory replica.
            let replica = match self
                .promotion_alloc_start_for_tenant(key, tenant_id, task.size as u64, vec![])
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "promotion: alloc failed");
                    let _ = self
                        .notify_promotion_failure_for_tenant(key, tenant_id)
                        .await;
                    continue;
                }
            };

            // Write data to the allocated memory replica.
            match self.write_to_replica(&replica, &data).await {
                Ok(()) => {
                    self.notify_promotion_success_for_tenant(key, tenant_id)
                        .await?;
                    promoted += 1;
                }
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "promotion: write_to_replica failed");
                    let _ = self
                        .notify_promotion_failure_for_tenant(key, tenant_id)
                        .await;
                }
            }
        }

        Ok(promoted)
    }

    // -----------------------------------------------------------------------
    // Dynamic segment mount/unmount
    // 动态 segment 挂载/卸载
    //
    // C++ equivalent: RealClient::mountSegment / unmountSegment /
    // allocateAndMountSegment / unmountAndFreeSegment
    // -----------------------------------------------------------------------

    /// Mount a memory segment with the given name, size, and base address.
    /// The memory must already be allocated and registered with the
    /// TransferEngine before calling this (for externally-mapped segments).
    /// After mounting, the segment is registered as a local endpoint for
    /// locality-aware replica selection.
    ///
    /// 挂载指定名称、大小和基地址的内存 segment。
    /// 调用前内存必须已分配并已向 TransferEngine 注册（用于外部映射的 segment）。
    /// 挂载后，该 segment 被注册为本地端点，用于本地性感知副本选择。
    ///
    /// For internally-allocated segments (where this node allocates memory and
    /// opens the segment on the TE), this is handled automatically in
    /// [`create`](Self::create) when `global_segment_size > 0`.
    ///
    /// 对于内部分配的 segment（本节点分配内存并在 TE 上打开 segment），
    /// 在 create() 中 global_segment_size > 0 时自动处理。
    ///
    /// C++ equivalent: `Client::MountSegment()`
    pub async fn mount_segment(
        &mut self,
        segment_name: &str,
        size: u64,
        base_addr: u64,
    ) -> StoreResult<()> {
        let response = self
            .master
            .mount_segment(proto::MountSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment_name: segment_name.to_string(),
                size,
                base_addr,
                te_endpoint: self.local_hostname.clone(),
                protocol: self.protocol.clone(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let segment_id = response.segment_id.as_ref().ok_or_else(|| {
            StoreError::Internal("MountSegment response missing segment_id".to_string())
        })?;
        self.mounted_segment_ids.write().insert(
            segment_name.to_string(),
            Uuid::from_u64_pair(segment_id.high, segment_id.low),
        );
        // Register as a local endpoint for subsequent locality checks
        self.register_local_endpoint(segment_name);
        Ok(())
    }

    /// Unmount a previously mounted segment from the master.
    ///
    /// 从 master 卸载之前挂载的 segment。
    ///
    /// # Arguments
    /// - `segment_name` — the name of the segment to unmount.
    ///   要卸载的 segment 名称。
    /// - `grace_period_ms` — if > 0, schedules a graceful unmount where the
    ///   master waits for the grace period before actually removing the
    ///   segment. If 0, unmounts immediately.
    ///   如果 > 0，安排优雅卸载——master 在优雅期等待后再实际删除 segment。
    ///   如果为 0，立即卸载。
    ///
    /// C++ equivalent: `Client::UnmountSegment()`
    pub async fn unmount_segment(
        &mut self,
        segment_name: &str,
        grace_period_ms: u64,
    ) -> StoreResult<()> {
        let segment_id = self
            .mounted_segment_ids
            .read()
            .get(segment_name)
            .copied()
            .ok_or_else(|| StoreError::SegmentNotFound(segment_name.to_string()))?;
        let segment_id_proto = Self::uuid_to_proto_uuid(segment_id);

        if grace_period_ms > 0 {
            self.master
                .graceful_unmount_segment(proto::GracefulUnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                    grace_period_ms,
                })
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        } else {
            self.master
                .unmount_segment(proto::UnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                })
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        };
        self.mounted_segment_ids.write().remove(segment_name);
        self.unregister_local_endpoint(segment_name);
        Ok(())
    }
}
