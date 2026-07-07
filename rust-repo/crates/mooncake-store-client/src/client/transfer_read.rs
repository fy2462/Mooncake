use super::MooncakeClient;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest};

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
            has_seg_buf = self.segment_buffer.is_some(),
            "read_from_replica: ENTER"
        );

        // LOCAL_DISK on remote node: use P2P offload RPC.
        // C++ equivalent: branch in real_client.cpp that calls
        // `batch_get_into_offload_object_internal`.
        if replica.replica_type == mooncake_store_core::ReplicaType::LocalDisk
            && !self.is_local_replica(replica)
        {
            return self
                .read_from_remote_local_disk(key, tenant_id, replica)
                .await;
        }

        // Fast path: local segment — direct memcpy, no TE overhead.
        // 快速路径：本地 segment —— 直接 memcpy，无 TE 开销。
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            tracing::info!(target: "te_debug", "read_from_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_read(replica);
            tracing::info!(
                target: "te_debug",
                ok = result.is_ok(),
                data_len = result.as_ref().map(|v| v.len()).unwrap_or(0),
                "read_from_replica: EXIT (local_memcpy)"
            );
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
            local_buf_ptr = ?self.local_buffer.as_ptr(),
            local_buf_len = self.local_buffer.len(),
            "read_from_replica: opening segment"
        );
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: segment opened");

        let read_len = replica.size as usize;
        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, read_len, "read_from_replica: batch_id allocated");

        // Build transfer: Read from remote segment into local_buffer.
        // 构建传输：从远端 segment 读入 local_buffer。
        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void, // destination / 目标
            target_id: segment_id,
            target_offset, // source offset in remote segment / 远端 segment 中的源偏移
            length: read_len as u64,
        };
        tracing::info!(
            target: "te_debug",
            src = ?request.source,
            tgt_id = request.target_id.0,
            tgt_off = request.target_offset,
            len = request.length,
            "read_from_replica: submitting transfer"
        );

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "read_from_replica: transfer submitted, polling...");

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
        let transferred = statuses[0].transferred_bytes;

        tracing::info!(target: "te_debug", batch_id = batch_id.0, "read_from_replica: freeing batch_id");
        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: closing segment");
        self.engine.close_segment(segment_id)?;

        // Extract the read data from local_buffer. / 从 local_buffer 提取读取的数据。
        let result: Vec<u8> = self.local_buffer[..(transferred as usize)].to_vec();
        tracing::info!(
            target: "te_debug",
            result_len = result.len(),
            "read_from_replica: EXIT (success)"
        );
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
    /// The buffer must be pre-registered with the TE via [`register_buffer`].
    ///
    /// 从副本零拷贝读取到调用者提供的缓冲区。
    /// 缓冲区必须通过 register_buffer 预先向 TE 注册。
    ///
    /// # Returns
    /// The number of bytes actually transferred. / 实际传输的字节数。
    ///
    /// # Safety
    /// `buffer` must point to at least `size` bytes of valid memory that has
    /// been registered with the TE. / buffer 必须指向至少 size 字节的已向 TE
    /// 注册的有效内存。
    pub(crate) async unsafe fn zero_copy_read(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            buf = ?buffer,
            size,
            "zero_copy_read: ENTER"
        );

        let segment_id: transfer_engine_ffi::SegmentId =
            self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_read: segment opened");

        let batch_id: transfer_engine_ffi::BatchId = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "zero_copy_read: batch_id allocated");

        // Build transfer: source = caller's buffer (RDMA destination).
        // 构建传输：源 = 调用者缓冲区（RDMA 目标）。
        let request: TransferRequest = TransferRequest {
            opcode: Opcode::Read,
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
        tracing::info!(target: "te_debug", "zero_copy_read: transfer submitted, polling...");

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
        let transferred = statuses[0].transferred_bytes;

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", transferred, "zero_copy_read: EXIT (success)");
        Ok(transferred as usize)
    }
}
