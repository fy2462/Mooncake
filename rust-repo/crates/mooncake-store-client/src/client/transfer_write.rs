use super::MooncakeClient;
use super::metrics::TransferOperationKind;
use super::transfer_local::ReadableBufferRegion;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use transfer_engine_ffi::{RegisteredSubmitOutcome, RegisteredTransferRequest};

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // write_to_replica — generic write with local_memcpy fast path
    // 向副本写入 —— 带 local_memcpy 快速路径的通用写入
    //
    // Flow (流程):
    //   1. Check is_local_replica && segment_buffer → local_memcpy (fast path)
    //      → done, no TE resources needed.
    //      检查本地副本 + segment_buffer → local_memcpy（快速路径），无需 TE 资源。
    //
    //   2. (Remote path) Copy data into local_buffer → open_segment →
    //      submit owner-bearing transfer → poll through native
    //      quiescence/free → close_segment.
    //      (远程路径) 拷贝数据到 local_buffer → open_segment →
    //      提交 owner-bearing transfer → 轮询到 native quiescence/free →
    //      close_segment。
    //
    // Resource cleanup: on failure or timeout, batch_id is freed and segment
    // is closed before returning the error. This prevents resource leaks in
    // the TransferEngine.
    //
    // 资源清理：在失败或超时时，先释放 batch_id 并关闭 segment 再返回错误。
    // 这防止了 TransferEngine 中的资源泄漏。
    //
    // C++ equivalent: Client::WriteToReplica() in real_client.cpp
    // -----------------------------------------------------------------------

    /// Write data to a specific replica. Automatically chooses local_memcpy
    /// fast path when the replica is local and segment_buffer is available.
    ///
    /// 向指定副本写入数据。当副本为本地且 segment_buffer 可用时自动选择
    /// local_memcpy 快速路径。
    pub(crate) async fn write_to_replica(
        &self,
        replica: &ReplicaDescriptor,
        data: &[u8],
    ) -> StoreResult<()> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            offset = replica.offset,
            base_addr = replica.base_addr,
            data_len = data.len(),
            is_local = self.is_local_replica(replica),
            has_seg_buf = self.local_owned_segment(replica).is_some(),
            "write_to_replica: ENTER"
        );

        if replica.protocol == "cxl" {
            let registration = self.cxl_segment_registration.as_ref().ok_or_else(|| {
                StoreError::Internal("CXL replica requires a live CXL mapping".to_string())
            })?;
            let target_offset = Self::checked_replica_target_offset(replica, 0)?;
            registration.copy_from_host(target_offset, data)?;
            if let Some(metrics) = &self.metrics {
                metrics.observe_transfer_bytes(TransferOperationKind::Write, data.len() as u64);
            }
            return Ok(());
        }

        // Fast path: local segment — direct memcpy, no TE overhead.
        // 快速路径：本地 segment —— 直接 memcpy，无 TE 开销。
        if self.is_local_replica(replica) && self.local_owned_segment(replica).is_some() {
            tracing::info!(target: "te_debug", "write_to_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_write(replica, data);
            tracing::info!(target: "te_debug", ok = result.is_ok(), "write_to_replica: EXIT (local_memcpy)");
            if result.is_ok()
                && let Some(metrics) = &self.metrics
            {
                metrics.observe_transfer_bytes(TransferOperationKind::Write, data.len() as u64);
            }
            return result;
        }

        // Remote path: validate data fits in local_buffer, then transfer via TE.
        // 远程路径：验证数据适合 local_buffer，然后通过 TE 传输。
        if data.len() > self.local_buffer.len() {
            tracing::error!(
                target: "te_debug",
                data_len = data.len(),
                buf_len = self.local_buffer.len(),
                "write_to_replica: data size exceeds local buffer"
            );
            return Err(StoreError::InvalidParams(format!(
                "data size {} exceeds local buffer size {}",
                data.len(),
                self.local_buffer.len()
            )));
        }

        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            local_buf_ptr = ?self.local_buffer.base_ptr(),
            local_buf_len = self.local_buffer.len(),
            "write_to_replica: opening segment"
        );

        let staging_lease = self.local_buffer.lease()?;
        self.local_buffer.copy_from_slice(&staging_lease, 0, data)?;
        let target_offset = Self::checked_replica_target_offset(replica, 0)?;

        // Step 1: open the segment on the TE. / 第 1 步：在 TE 上打开 segment。
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: segment opened");

        // Step 2: data already resides in the exclusively leased staging
        // buffer, which can be transferred to a cancellation reaper.
        tracing::info!(target: "te_debug", data_len = data.len(), "write_to_replica: data copied to local_buffer");

        // Step 3: build and submit an owner-bearing transfer request.
        let request = self.close_segment_on_prepare_error(
            segment_id,
            (|| -> StoreResult<_> {
                Ok(RegisteredTransferRequest::write(
                    self.local_buffer
                        .readable_region(&staging_lease, 0, data.len())?,
                    segment_id,
                    target_offset,
                    data.len(),
                )?)
            })(),
        )?;
        tracing::info!(
            target: "te_debug",
            tgt_id = segment_id.0,
            tgt_off = target_offset,
            len = data.len(),
            "write_to_replica: submitting transfer"
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
        tracing::info!(target: "te_debug", "write_to_replica: transfer submitted, polling...");

        // Step 5: hand every raw-pointer owner to the reaper synchronously,
        // then await completion. Cancellation only drops the receiver.
        let completion = self
            .wait_for_transfer_batch_owned(
                batch,
                segment_id,
                std::time::Duration::from_secs(10),
                staging_lease,
            )
            .await?;
        if !super::transfer::transfer_statuses_match_lengths(
            &completion.statuses,
            [data.len() as u64],
        ) {
            return Err(StoreError::Internal(format!(
                "write transfer did not complete exactly {} bytes",
                data.len()
            )));
        }
        tracing::info!(
            target: "te_debug",
            transferred = completion.statuses[0].transferred_bytes,
            "write_to_replica: transfer COMPLETED"
        );

        // Step 6: the reaper already released the batch and segment.
        drop(completion);
        tracing::info!(target: "te_debug", "write_to_replica: EXIT (success)");
        if let Some(metrics) = &self.metrics {
            metrics.observe_transfer_bytes(TransferOperationKind::Write, data.len() as u64);
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // zero_copy_write — write directly from a caller-provided buffer
    // 零拷贝写入 —— 直接从调用者提供的缓冲区写入
    //
    // Unlike write_to_replica, this does NOT copy data into local_buffer first.
    // Instead, the caller's buffer (which must be pre-registered with the TE)
    // is used directly as the source of the RDMA transfer. This eliminates one
    // memcpy.
    //
    // 与 write_to_replica 不同，此方法不先将数据拷贝到 local_buffer。
    // 而是直接使用调用者的缓冲区（必须已向 TE 预先注册）作为 RDMA 传输的源。
    // 这消除了额外的 memcpy。
    //
    // Resource lifecycle (资源生命周期):
    //   open_segment → typed registered submit →
    //   poll through native quiescence/free → close_segment
    //
    // C++ equivalent: zero-copy path inside Client::WriteToReplica() when
    // the caller passes an externally-registered buffer.
    // -----------------------------------------------------------------------

    /// Zero-copy write to a replica using a caller-provided buffer.
    /// The buffer must be pre-registered with the TE via
    /// [`register_owned_buffer`](Self::register_owned_buffer).
    ///
    /// 使用调用者提供的缓冲区进行零拷贝写入。
    /// 缓冲区必须通过 register_owned_buffer 预先向 TE 注册。
    ///
    /// `buffer` is already a bounds-checked readable registration capability
    /// whose lease is transferred to the completion reaper.
    pub(crate) async fn zero_copy_write(
        &mut self,
        replica: &ReplicaDescriptor,
        buffer: ReadableBufferRegion,
    ) -> StoreResult<()> {
        let size = buffer.len();
        let source = buffer.foreign_region();
        let device_source =
            crate::data_plane_ffi::is_device_memory(self.accelerator.as_ref(), source)?;
        let staging_lease = if device_source {
            let lease = self.local_buffer.lease()?;
            let mut staged = vec![0_u8; size];
            crate::data_plane_ffi::gather_device_to_host(
                self.accelerator.as_ref(),
                source,
                &mut staged,
            )?;
            self.local_buffer.copy_from_slice(&lease, 0, &staged)?;
            Some(lease)
        } else {
            None
        };
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            size,
            "zero_copy_write: ENTER"
        );

        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_write: segment opened");

        // Build transfer: source = caller's buffer directly (no intermediate copy).
        // 构建传输：源 = 直接使用调用者缓冲区（无中间拷贝）。
        let request = self.close_segment_on_prepare_error(
            segment_id,
            (|| -> StoreResult<_> {
                let local_region = match staging_lease.as_ref() {
                    Some(lease) => self.local_buffer.readable_region(lease, 0, size)?,
                    None => buffer.registered_region(),
                };
                Ok(RegisteredTransferRequest::write(
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
        tracing::info!(target: "te_debug", "zero_copy_write: transfer submitted, polling...");

        let completion = self
            .wait_for_transfer_batch_owned(
                batch,
                segment_id,
                std::time::Duration::from_secs(10),
                (buffer, staging_lease),
            )
            .await?;
        if !super::transfer::transfer_statuses_match_lengths(&completion.statuses, [size as u64]) {
            return Err(StoreError::Internal(format!(
                "zero-copy write did not complete exactly {size} bytes"
            )));
        }
        drop(completion);
        tracing::info!(target: "te_debug", "zero_copy_write: EXIT (success)");
        if let Some(metrics) = &self.metrics {
            metrics.observe_transfer_bytes(TransferOperationKind::Write, size as u64);
        }
        Ok(())
    }
}
