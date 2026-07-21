use super::storage::OffloadTaskItem;
use super::MooncakeClient;
use crate::local_storage_backend::{local_storage_key, parse_local_storage_key};
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::collections::HashMap;
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
            if let Err(error) = self.start_offload_server().await {
                self.notify_offload_failure_tasks(tasks).await?;
                return Err(error);
            }
        }
        let transport_endpoint = self.offload_rpc_address();

        let Some(storage) = self.local_storage.as_ref() else {
            self.notify_offload_failure_tasks(tasks).await?;
            return Err(StoreError::Internal(
                "no local storage backend configured".to_string(),
            ));
        };
        let storage = storage.clone();

        let mut offloaded = 0usize;
        let mut notify_tasks = Vec::with_capacity(tasks.len());
        let mut metadatas = Vec::with_capacity(tasks.len());
        let mut committed_storage_keys = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let key = task.key.as_str();
            let tenant_id = task.tenant_id.as_str();
            if task.size < 0 {
                tracing::warn!(target: "storage_debug", %tenant_id, %key, size = task.size, "offload: invalid negative task size");
                notify_tasks.push(task.clone());
                metadatas.push(failed_offload_metadata());
                continue;
            }
            // Read object data from memory.
            let data = match self.get_for_tenant(key, tenant_id).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: failed to get object from memory, skipping");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
            };

            // Reserve FIFO victims first. Master metadata is updated before
            // those files are deleted, so readers never get routed to an
            // already-removed LOCAL_DISK replica.
            let key_owned = local_storage_key(tenant_id, key);
            let key_for_prepare = key_owned.clone();
            let s = storage.clone();
            let pending = match tokio::task::spawn_blocking(move || {
                s.prepare_write(&key_for_prepare, data.len() as u64)
                    .map(|pending| (pending, data))
            })
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: failed to reserve local storage");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: local storage write task failed");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
            };
            let (pending_eviction, data) = pending;
            let evicted_keys = pending_eviction.keys();
            if let Err(error) = self.notify_evicted_disk_replicas(&evicted_keys).await {
                storage.rollback_eviction(pending_eviction);
                tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "offload: failed to publish local eviction");
                notify_tasks.push(task.clone());
                metadatas.push(failed_offload_metadata());
                continue;
            }

            let key_for_write = key_owned.clone();
            let s = storage.clone();
            let write_result = tokio::task::spawn_blocking(move || {
                s.commit_write(&key_for_write, &data, pending_eviction)
            })
            .await;
            match write_result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "offload: failed to write object to local storage");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
                Err(error) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "offload: local storage write task failed");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
            }

            for evicted_key in &evicted_keys {
                tracing::info!(target: "storage_debug", %evicted_key, "offload: evicted old file");
            }

            offloaded += 1;
            committed_storage_keys.push(key_owned);
            notify_tasks.push(task.clone());
            metadatas.push(proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: task.size,
                transport_endpoint: transport_endpoint.clone(),
            });
        }

        if !notify_tasks.is_empty() {
            if let Err(error) = self
                .notify_offload_success_tasks(notify_tasks, metadatas)
                .await
            {
                let storage = storage.clone();
                tokio::task::spawn_blocking(move || {
                    for storage_key in committed_storage_keys {
                        if let Err(cleanup_error) = storage.delete_object(&storage_key) {
                            tracing::warn!(target: "storage_debug", %storage_key, %cleanup_error, "offload: failed to roll back unpublished local object");
                        }
                    }
                })
                .await
                .map_err(|join_error| StoreError::Internal(join_error.to_string()))?;
                return Err(error);
            }
        }

        Ok(offloaded)
    }

    async fn notify_offload_failure_tasks(
        &mut self,
        tasks: Vec<OffloadTaskItem>,
    ) -> StoreResult<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let metadatas = tasks.iter().map(|_| failed_offload_metadata()).collect();
        self.notify_offload_success_tasks(tasks, metadatas).await
    }

    async fn notify_evicted_disk_replicas(&mut self, storage_keys: &[String]) -> StoreResult<()> {
        let mut keys_by_tenant: HashMap<String, Vec<String>> = HashMap::new();
        for storage_key in storage_keys {
            let (tenant_id, key) = parse_local_storage_key(storage_key);
            keys_by_tenant
                .entry(tenant_id.to_string())
                .or_default()
                .push(key.to_string());
        }

        for (tenant_id, keys) in keys_by_tenant {
            self.batch_evict_disk_replica(
                &keys,
                proto::replica_descriptor::ReplicaType::LocalDisk as i32,
                &tenant_id,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn run_disk_watermark_eviction(
        &mut self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<usize> {
        let Some(storage) = self.local_storage.as_ref().cloned() else {
            return Ok(0);
        };
        let prepare_storage = storage.clone();
        let pending = tokio::task::spawn_blocking(move || {
            prepare_storage.prepare_watermark_eviction(high_watermark_ratio, low_watermark_ratio)
        })
        .await
        .map_err(|error| StoreError::Internal(error.to_string()))??;
        let evicted_keys = pending.keys();
        if let Err(error) = self.notify_evicted_disk_replicas(&evicted_keys).await {
            storage.rollback_eviction(pending);
            return Err(error);
        }
        let count = evicted_keys.len();
        tokio::task::spawn_blocking(move || storage.commit_eviction(pending))
            .await
            .map_err(|error| StoreError::Internal(error.to_string()))??;
        Ok(count)
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
        let storage = storage.clone();

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
                let s = storage.clone();
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
            .mount_segment(self.rpc_request(proto::MountSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment_name: segment_name.to_string(),
                size,
                base_addr,
                te_endpoint: self.local_hostname.clone(),
                protocol: self.protocol.clone(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
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
                .graceful_unmount_segment(self.rpc_request(proto::GracefulUnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                    grace_period_ms,
                }))
                .await
                .map_err(Self::rpc_status_to_error)?;
        } else {
            self.master
                .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                }))
                .await
                .map_err(Self::rpc_status_to_error)?;
        };
        self.mounted_segment_ids.write().remove(segment_name);
        self.unregister_local_endpoint(segment_name);
        Ok(())
    }
}

fn failed_offload_metadata() -> proto::StorageObjectMetadata {
    proto::StorageObjectMetadata {
        bucket_id: -1,
        offset: 0,
        key_size: 0,
        data_size: -1,
        transport_endpoint: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_offload_metadata_uses_negative_size_sentinel() {
        let metadata = failed_offload_metadata();
        assert_eq!(metadata.data_size, -1);
        assert_eq!(metadata.bucket_id, -1);
    }
}
