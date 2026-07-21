use super::MooncakeClient;
use super::transfer_local::RegisteredBufferRegion;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest};

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
    //      allocate_batch_id → submit transfer → poll status (10s timeout) →
    //      free_batch_id → close_segment.
    //      (远程路径) 拷贝数据到 local_buffer → open_segment →
    //      allocate_batch_id → 提交传输 → 轮询状态 (10s 超时) →
    //      free_batch_id → close_segment。
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
            has_seg_buf = self.segment_buffer.is_some(),
            "write_to_replica: ENTER"
        );

        // Fast path: local segment — direct memcpy, no TE overhead.
        // 快速路径：本地 segment —— 直接 memcpy，无 TE 开销。
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            tracing::info!(target: "te_debug", "write_to_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_write(replica, data);
            tracing::info!(target: "te_debug", ok = result.is_ok(), "write_to_replica: EXIT (local_memcpy)");
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
            local_buf_ptr = ?self.local_buffer.as_ptr(),
            local_buf_len = self.local_buffer.len(),
            "write_to_replica: opening segment"
        );
        // Step 1: open the segment on the TE. / 第 1 步：在 TE 上打开 segment。
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: segment opened");

        // Step 2: allocate a batch_id for grouping transfer requests.
        // 第 2 步：分配 batch_id 用于分组传输请求。
        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "write_to_replica: batch_id allocated");

        // Step 3: copy data into the registered local_buffer (TE source).
        // 第 3 步：将数据拷贝到已注册的 local_buffer（TE 源）。
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.local_buffer.as_ptr() as *mut u8,
                data.len(),
            );
        }
        tracing::info!(target: "te_debug", data_len = data.len(), "write_to_replica: data copied to local_buffer");

        // Step 4: build and submit the transfer request.
        // 第 4 步：构建并提交传输请求。
        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset,
            length: data.len() as u64,
        };
        tracing::info!(
            target: "te_debug",
            src = ?request.source,
            tgt_id = request.target_id.0,
            tgt_off = request.target_offset,
            len = request.length,
            "write_to_replica: submitting transfer"
        );

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "write_to_replica: transfer submitted, polling...");

        // Step 5: poll transfer status with 10s timeout.
        // 第 5 步：以 10s 超时轮询传输状态。
        // 10s is generous for RDMA (us-scale) but covers TCP retransmissions
        // and slow NVMe-oF targets. 10s 对 RDMA（微秒级）很充裕，但覆盖了 TCP
        // 重传和慢速 NVMe-oF 目标。
        let statuses = match self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            Ok(statuses) => statuses,
            Err(e) => {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(e);
            }
        };
        tracing::info!(
            target: "te_debug",
            transferred = statuses[0].transferred_bytes,
            "write_to_replica: transfer COMPLETED"
        );

        // Step 6: cleanup resources. / 第 6 步：清理资源。
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "write_to_replica: freeing batch_id");
        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: closing segment");
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", "write_to_replica: EXIT (success)");
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
    //   open_segment → allocate_batch_id → submit → poll(10s) →
    //   free_batch_id → close_segment
    //
    // C++ equivalent: zero-copy path inside Client::WriteToReplica() when
    // the caller passes an externally-registered buffer.
    // -----------------------------------------------------------------------

    /// Zero-copy write to a replica using a caller-provided buffer.
    /// The buffer must be pre-registered with the TE via [`register_buffer`].
    ///
    /// 使用调用者提供的缓冲区进行零拷贝写入。
    /// 缓冲区必须通过 register_buffer 预先向 TE 注册。
    ///
    /// # Safety
    /// `buffer` must point to at least `size` bytes of valid memory that has
    /// been registered with the TE. / buffer 必须指向至少 size 字节的已向 TE
    /// 注册的有效内存。
    pub(crate) async fn zero_copy_write(
        &self,
        replica: &ReplicaDescriptor,
        buffer: RegisteredBufferRegion,
    ) -> StoreResult<()> {
        let size = buffer.len();
        let buffer = buffer.as_mut_ptr();
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            buf = ?buffer,
            size,
            "zero_copy_write: ENTER"
        );

        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_write: segment opened");

        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "zero_copy_write: batch_id allocated");

        // Build transfer: source = caller's buffer directly (no intermediate copy).
        // 构建传输：源 = 直接使用调用者缓冲区（无中间拷贝）。
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "zero_copy_write: transfer submitted, polling...");

        if let Err(e) = self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e);
        }

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", "zero_copy_write: EXIT (success)");
        Ok(())
    }
}
