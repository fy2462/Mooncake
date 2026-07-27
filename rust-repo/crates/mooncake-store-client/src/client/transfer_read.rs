use super::MooncakeClient;
use super::metrics::TransferOperationKind;
use super::transfer_local::WritableBufferRegion;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use transfer_engine_ffi::{RegisteredSubmitOutcome, RegisteredTransferRequest};

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // read_from_replica — generic read with local_memcpy fast path
    // 从副本读取 —— 带 local_memcpy 快速路径的通用读取
    //
    // Mirror of write_to_replica (with Opcode::Read instead of Write).
    // write_to_replica 的镜像（Opcode::Read 代替 Write）。
    //
    // C++ equivalent: Client::ReadFromReplica() in real_client.cpp
    // -----------------------------------------------------------------------

    /// Read data from a specific replica. Automatically chooses local_memcpy
    /// fast path when the replica is local and segment_buffer is available.
    ///
    /// 从指定副本读取数据。当副本为本地且 segment_buffer 可用时自动选择
    /// local_memcpy 快速路径。
    pub(crate) async fn read_from_replica(
        &self,
        key: &str,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        self.read_from_replica_for_tenant(key, &self.tenant_id, replica)
            .await
    }

    pub(crate) async fn read_from_replica_for_tenant(
        &self,
        key: &str,
        tenant_id: &str,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            offset = replica.offset,
            base_addr = replica.base_addr,
            replica_type = ?replica.replica_type,
            replica_size = replica.size,
            is_local = self.is_local_replica(replica),
            has_seg_buf = self.local_owned_segment(replica).is_some(),
            "read_from_replica: ENTER"
        );

        if replica.replica_type == mooncake_store_core::ReplicaType::Disk {
            let storage = self.global_disk.as_ref().ok_or_else(|| {
                StoreError::InvalidParams(
                    "Master returned a global DISK replica but the client has no shared storage backend"
                        .to_string(),
                )
            })?;
            let expected_size = usize::try_from(replica.size).map_err(|_| {
                StoreError::InvalidParams(format!(
                    "global DISK object size cannot fit usize: {}",
                    replica.size
                ))
            })?;
            let result = storage
                .read(replica.segment_name.clone(), expected_size)
                .await;
            if let Ok(data) = &result
                && let Some(metrics) = &self.metrics
            {
                metrics.observe_transfer_bytes(TransferOperationKind::Read, data.len() as u64);
            }
            return result;
        }

        // LOCAL_DISK on remote node: use P2P offload RPC.
        // C++ equivalent: branch in real_client.cpp that calls
        // `batch_get_into_offload_object_internal`.
        if replica.replica_type == mooncake_store_core::ReplicaType::LocalDisk
            && !self.is_local_replica(replica)
        {
            let result = self
                .read_from_remote_local_disk(key, tenant_id, replica)
                .await;
            if let Ok(data) = &result
                && let Some(metrics) = &self.metrics
            {
                metrics.observe_transfer_bytes(TransferOperationKind::Read, data.len() as u64);
            }
            return result;
        }

        // Fast path: local segment — direct memcpy, no TE overhead.
        // 快速路径：本地 segment —— 直接 memcpy，无 TE 开销。
        if self.is_local_replica(replica) && self.local_owned_segment(replica).is_some() {
            tracing::info!(target: "te_debug", "read_from_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_read(replica);
            tracing::info!(
                target: "te_debug",
                ok = result.is_ok(),
                data_len = result.as_ref().map(|v| v.len()).unwrap_or(0),
                "read_from_replica: EXIT (local_memcpy)"
            );
            if let Ok(data) = &result
                && let Some(metrics) = &self.metrics
            {
                metrics.observe_transfer_bytes(TransferOperationKind::Read, data.len() as u64);
                metrics.observe_read_strategy("local_memcpy");
            }
            return result;
        }

        // Remote path: validate object size fits in local_buffer.
        // 远程路径：验证对象大小适合 local_buffer。
        if replica.size > self.local_buffer.len() as u64 {
            tracing::error!(
                target: "te_debug",
                replica_size = replica.size,
                buf_len = self.local_buffer.len(),
                "read_from_replica: object size exceeds local buffer"
            );
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds local buffer size {}",
                replica.size,
                self.local_buffer.len()
            )));
        }

        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            local_buf_ptr = ?self.local_buffer.base_ptr(),
            local_buf_len = self.local_buffer.len(),
            "read_from_replica: opening segment"
        );
        self.local_buffer.wait_until_available().await;
        let staging_lease = self.local_buffer.lease()?;
        let read_len = usize::try_from(replica.size).map_err(|_| {
            StoreError::InvalidParams(format!("replica size cannot fit usize: {}", replica.size))
        })?;
        let target_offset = Self::checked_replica_target_offset(replica, 0)?;
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: segment opened");

        // Build transfer: Read from remote segment into local_buffer.
        // 构建传输：从远端 segment 读入 local_buffer。
        let request = self.close_segment_on_prepare_error(
            segment_id,
            (|| -> StoreResult<_> {
                Ok(RegisteredTransferRequest::read(
                    self.local_buffer
                        .writable_region(&staging_lease, 0, read_len)?,
                    segment_id,
                    target_offset,
                    read_len,
                )?)
            })(),
        )?;
        tracing::info!(
            target: "te_debug",
            tgt_id = segment_id.0,
            tgt_off = target_offset,
            len = read_len,
            "read_from_replica: submitting transfer"
        );

        let outcome = match self.engine.submit_transfer(vec![request]) {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(error.into());
            }
        };
        let batch = match outcome {
            RegisteredSubmitOutcome::Submitted(batch) => batch,
            RegisteredSubmitOutcome::NativeRejected { error, batch } => {
                let _completion = self
                    .release_failed_submission_owned(
                        batch,
                        segment_id,
                        std::time::Duration::from_secs(10),
                        staging_lease,
                    )
                    .await?;
                return Err(error.into());
            }
        };
        tracing::info!(target: "te_debug", "read_from_replica: transfer submitted, polling...");

        let completion = self
            .wait_for_transfer_batch_owned(
                batch,
                segment_id,
                std::time::Duration::from_secs(10),
                staging_lease,
            )
            .await?;
        let transferred = completion.statuses[0].transferred_bytes;
        let transferred = usize::try_from(transferred).map_err(|_| {
            StoreError::Internal("transferred byte count cannot fit usize".to_string())
        })?;
        if transferred != read_len {
            return Err(StoreError::Internal(format!(
                "transfer reported {transferred} bytes for a {read_len}-byte request"
            )));
        }

        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: segment closed");

        // Extract the read data from local_buffer. / 从 local_buffer 提取读取的数据。
        let result = self
            .local_buffer
            .copy_to_vec(&completion.payload, 0, transferred)?;
        drop(completion);
        tracing::info!(
            target: "te_debug",
            result_len = result.len(),
            "read_from_replica: EXIT (success)"
        );
        if let Some(metrics) = &self.metrics {
            metrics.observe_transfer_bytes(TransferOperationKind::Read, result.len() as u64);
            metrics.observe_read_strategy("transfer_engine");
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // zero_copy_read — read directly into a caller-provided buffer
    // 零拷贝读取 —— 直接读入调用者提供的缓冲区
    //
    // Mirror of zero_copy_write (with Opcode::Read instead of Write).
    // zero_copy_write 的镜像（Opcode::Read 代替 Write）。
    //
    // The caller's buffer is the DESTINATION of the RDMA read, so no
    // intermediate copy from local_buffer is needed.
    //
    // 调用者的缓冲区是 RDMA 读的目标，因此不需要从 local_buffer 进行中间拷贝。
    // -----------------------------------------------------------------------

    /// Zero-copy read from a replica into a caller-provided buffer.
    /// The buffer must be pre-registered with the TE via
    /// [`register_owned_buffer`](Self::register_owned_buffer).
    ///
    /// 从副本零拷贝读取到调用者提供的缓冲区。
    /// 缓冲区必须通过 register_owned_buffer 预先向 TE 注册。
    ///
    /// # Returns
    /// The number of bytes actually transferred. / 实际传输的字节数。
    ///
    /// `buffer` is already a bounds-checked writable registration capability
    /// whose lease is transferred to the completion reaper.
    pub(crate) async fn zero_copy_read(
        &mut self,
        replica: &ReplicaDescriptor,
        buffer: WritableBufferRegion,
    ) -> StoreResult<usize> {
        let size = buffer.len();
        let destination = buffer.foreign_region();
        let device_destination =
            crate::data_plane_ffi::is_device_memory(self.accelerator.as_ref(), destination)?;
        if device_destination && size > self.local_buffer.len() {
            return Err(StoreError::InvalidParams(format!(
                "device read size {size} exceeds staging buffer size {}",
                self.local_buffer.len()
            )));
        }
        let staging_lease = if device_destination {
            self.local_buffer.wait_until_available().await;
            Some(self.local_buffer.lease()?)
        } else {
            None
        };
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            size,
            "zero_copy_read: ENTER"
        );

        let segment_id: transfer_engine_ffi::SegmentId =
            self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_read: segment opened");

        // Build transfer: source = caller's buffer (RDMA destination).
        // 构建传输：源 = 调用者缓冲区（RDMA 目标）。
        let request = self.close_segment_on_prepare_error(
            segment_id,
            (|| -> StoreResult<_> {
                let local_region = match staging_lease.as_ref() {
                    Some(lease) => self.local_buffer.writable_region(lease, 0, size)?,
                    None => buffer.registered_region(),
                };
                Ok(RegisteredTransferRequest::read(
                    local_region,
                    segment_id,
                    Self::checked_replica_target_offset(replica, 0)?,
                    size,
                )?)
            })(),
        )?;
        let outcome = match self.engine.submit_transfer(vec![request]) {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(error.into());
            }
        };
        let batch = match outcome {
            RegisteredSubmitOutcome::Submitted(batch) => batch,
            RegisteredSubmitOutcome::NativeRejected { error, batch } => {
                let _completion = self
                    .release_failed_submission_owned(
                        batch,
                        segment_id,
                        std::time::Duration::from_secs(10),
                        (buffer, staging_lease),
                    )
                    .await?;
                return Err(error.into());
            }
        };
        tracing::info!(target: "te_debug", "zero_copy_read: transfer submitted, polling...");

        let completion = self
            .wait_for_transfer_batch_owned(
                batch,
                segment_id,
                std::time::Duration::from_secs(10),
                (buffer, staging_lease),
            )
            .await?;
        let transferred = completion.statuses[0].transferred_bytes;
        let transferred = usize::try_from(transferred).map_err(|_| {
            StoreError::Internal("transferred byte count cannot fit usize".to_string())
        })?;
        if transferred != size {
            return Err(StoreError::Internal(format!(
                "transfer reported {transferred} bytes for a {size}-byte destination"
            )));
        }

        let (buffer, staging_lease) = completion.payload;
        if device_destination {
            let lease = staging_lease.as_ref().ok_or_else(|| {
                StoreError::Internal("device read lost its staging lease".to_string())
            })?;
            let staged = self.local_buffer.copy_to_vec(lease, 0, transferred)?;
            crate::data_plane_ffi::scatter_host_to_device(
                self.accelerator.as_ref(),
                destination,
                &staged,
            )?;
        }

        drop(staging_lease);
        drop(buffer);
        tracing::info!(target: "te_debug", transferred, "zero_copy_read: EXIT (success)");
        if let Some(metrics) = &self.metrics {
            metrics.observe_transfer_bytes(TransferOperationKind::Read, transferred as u64);
        }
        Ok(transferred)
    }
}
