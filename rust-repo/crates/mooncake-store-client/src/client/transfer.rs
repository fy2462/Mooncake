use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // LOCAL_MEMCPY — bypass TE for same-node transfers (matches C++ strategy)
    // -----------------------------------------------------------------------

    fn is_local_replica(&self, replica: &ReplicaDescriptor) -> bool {
        self.local_endpoints.read().contains(&replica.segment_name)
    }

    fn local_memcpy_write(&self, replica: &ReplicaDescriptor, data: &[u8]) -> StoreResult<()> {
        let seg = self
            .segment_buffer
            .as_ref()
            .ok_or_else(|| StoreError::Internal("no local segment buffer".into()))?;
        let offset = replica.offset as usize;
        let len = data.len();
        if offset + len > seg.len() {
            return Err(StoreError::InvalidParams(format!(
                "local write out of bounds: offset={} len={} segment_size={}",
                offset,
                len,
                seg.len()
            )));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), seg.as_ptr().add(offset) as *mut u8, len);
        }
        Ok(())
    }

    fn local_memcpy_read(&self, replica: &ReplicaDescriptor) -> StoreResult<Vec<u8>> {
        let seg = self
            .segment_buffer
            .as_ref()
            .ok_or_else(|| StoreError::Internal("no local segment buffer".into()))?;
        let offset = replica.offset as usize;
        let len = replica.size as usize;
        if offset + len > seg.len() {
            return Err(StoreError::InvalidParams(format!(
                "local read out of bounds: offset={} len={} segment_size={}",
                offset,
                len,
                seg.len()
            )));
        }
        let mut result = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(seg.as_ptr().add(offset), result.as_mut_ptr(), len);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Buffer registration (zero-copy path)
    // -----------------------------------------------------------------------

    pub unsafe fn register_buffer(
        &self,
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> StoreResult<()> {
        unsafe {
            self.engine
                .register_local_memory(buffer, size, location, true)?;
        }
        self.registered_buffers
            .write()
            .insert(buffer as usize, (size, location.to_string()));
        Ok(())
    }

    pub unsafe fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()> {
        unsafe {
            self.engine.unregister_local_memory(buffer)?;
        }
        self.registered_buffers.write().remove(&(buffer as usize));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    pub(crate) fn client_id_proto(&self) -> proto::Uuid {
        let (h, l) = self.client_id.as_u64_pair();
        proto::Uuid { high: h, low: l }
    }

    pub(crate) async fn fetch_replicas(
        &mut self,
        key: &str,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let request = proto::GetReplicaListRequest {
            key: key.to_string(),
        };
        let response = self
            .master
            .get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);
        Ok(replicas)
    }

    pub(crate) fn replicas_from_proto(
        &self,
        replicas: &[proto::ReplicaDescriptor],
    ) -> Vec<ReplicaDescriptor> {
        replicas
            .iter()
            .filter_map(|r| {
                let sid = r.segment_id.as_ref()?;
                Some(ReplicaDescriptor {
                    refcnt: 0,
                    segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                    segment_name: r.segment_name.clone(),
                    offset: r.offset,
                    size: r.size,
                    base_addr: r.base_addr,
                    status: match r.status {
                        1 => mooncake_store_core::ReplicaStatus::Allocating,
                        2 => mooncake_store_core::ReplicaStatus::Written,
                        3 => mooncake_store_core::ReplicaStatus::Complete,
                        4 => mooncake_store_core::ReplicaStatus::Failed,
                        _ => mooncake_store_core::ReplicaStatus::Undefined,
                    },
                    replica_type: match r.replica_type {
                        1 => mooncake_store_core::ReplicaType::Disk,
                        2 => mooncake_store_core::ReplicaType::LocalDisk,
                        3 => mooncake_store_core::ReplicaType::NoFSsd,
                        _ => mooncake_store_core::ReplicaType::Memory,
                    },
                    holder_client_id: r
                        .holder_client_id
                        .as_ref()
                        .map(|id| Uuid::from_u64_pair(id.high, id.low)),
                    handle_valid: true,
                })
            })
            .collect()
    }

    /// 从副本列表中选择最优副本，完全匹配 C++ `SelectBestReplica` 逻辑。
    ///
    /// 优先级（C++ `real_client.cpp:286-325`）：
    /// 1. 本地 MEMORY（segment_name 匹配本地端点）→ 立即返回
    /// 2. 任意远程 MEMORY
    /// 3. 本地 NOF_SSD（segment_name 匹配本地端点）→ 立即返回
    /// 4. 任意远程 NOF_SSD
    /// 5. LOCAL_DISK（如果有多个，最后一个生效；覆盖 DISK）
    /// 6. DISK（仅在没有任何 LOCAL_DISK 时）
    pub(crate) fn select_best_replica<'a>(
        &self,
        replicas: &'a [ReplicaDescriptor],
    ) -> Option<&'a ReplicaDescriptor> {
        let endpoints = self.local_endpoints.read();
        let mut first_memory: Option<&ReplicaDescriptor> = None;
        let mut first_nof: Option<&ReplicaDescriptor> = None;

        // 第一遍：优先本地 MEMORY/NOF，否则记录首次出现的远程副本
        for r in replicas {
            if r.status != mooncake_store_core::ReplicaStatus::Complete {
                continue;
            }
            match r.replica_type {
                mooncake_store_core::ReplicaType::Memory => {
                    if endpoints.contains(&r.segment_name) {
                        return Some(r); // 本地 MEMORY —— 最优
                    }
                    if first_memory.is_none() {
                        first_memory = Some(r);
                    }
                }
                mooncake_store_core::ReplicaType::NoFSsd => {
                    if endpoints.contains(&r.segment_name) {
                        return Some(r); // 本地 NOF_SSD —— 次优
                    }
                    if first_nof.is_none() {
                        first_nof = Some(r);
                    }
                }
                _ => {}
            }
        }

        if let Some(r) = first_memory {
            return Some(r);
        }
        if let Some(r) = first_nof {
            return Some(r);
        }

        // 第二遍：LOCAL_DISK 优先，DISK 作为最后备选
        let mut best: Option<&ReplicaDescriptor> = None;
        for r in replicas {
            if r.status != mooncake_store_core::ReplicaStatus::Complete {
                continue;
            }
            match r.replica_type {
                mooncake_store_core::ReplicaType::LocalDisk => {
                    best = Some(r); // LOCAL_DISK 始终覆盖 DISK
                }
                mooncake_store_core::ReplicaType::Disk
                    if best.is_none() => {
                        best = Some(r);
                    }
                _ => {}
            }
        }
        best
    }

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

        // Fast path: local segment — direct memcpy, no TE overhead
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            tracing::info!(target: "te_debug", "write_to_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_write(replica, data);
            tracing::info!(target: "te_debug", ok = result.is_ok(), "write_to_replica: EXIT (local_memcpy)");
            return result;
        }

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
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: segment opened");

        let batch_id = self.engine.allocate_batch_id(1)?;
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "write_to_replica: batch_id allocated");

        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.local_buffer.as_ptr() as *mut u8,
                data.len(),
            );
        }
        tracing::info!(target: "te_debug", data_len = data.len(), "write_to_replica: data copied to local_buffer");

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

        self.engine.submit_transfer(batch_id, &[request])?;
        tracing::info!(target: "te_debug", "write_to_replica: transfer submitted, polling...");

        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);
        let mut poll_count: u64 = 0;
        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            poll_count += 1;
            if status.status == TransferStatusEnum::Completed {
                tracing::info!(
                    target: "te_debug",
                    poll_count,
                    transferred = status.transferred_bytes,
                    elapsed_ms = start.elapsed().as_millis(),
                    "write_to_replica: transfer COMPLETED"
                );
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                tracing::error!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    batch_id = batch_id.0,
                    seg_id = segment_id.0,
                    "write_to_replica: transfer FAILED (leaking batch_id and segment!)"
                );
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(StoreError::OperationFailed(-1));
            }
            if start.elapsed() > timeout {
                tracing::error!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    batch_id = batch_id.0,
                    "write_to_replica: transfer TIMEOUT"
                );
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        tracing::info!(target: "te_debug", batch_id = batch_id.0, "write_to_replica: freeing batch_id");
        self.engine.free_batch_id(batch_id)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: closing segment");
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", "write_to_replica: EXIT (success)");
        Ok(())
    }

    pub(crate) async unsafe fn zero_copy_write(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<()> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            buf = ?buffer,
            size,
            "zero_copy_write: ENTER"
        );

        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_write: segment opened");

        let batch_id = self.engine.allocate_batch_id(1)?;
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "zero_copy_write: batch_id allocated");

        let request = TransferRequest {
            opcode: Opcode::Write,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;
        tracing::info!(target: "te_debug", "zero_copy_write: transfer submitted, polling...");

        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);
        let mut poll_count: u64 = 0;
        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            poll_count += 1;
            if status.status.is_terminal() {
                if status.status != TransferStatusEnum::Completed {
                    tracing::error!(
                        target: "te_debug",
                        poll_count,
                        elapsed_ms = start.elapsed().as_millis(),
                        batch_id = batch_id.0,
                        seg_id = segment_id.0,
                        "zero_copy_write: transfer FAILED (cleaning up)"
                    );
                    let _ = self.engine.free_batch_id(batch_id);
                    let _ = self.engine.close_segment(segment_id);
                    return Err(StoreError::OperationFailed(-1));
                }
                tracing::info!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    "zero_copy_write: transfer COMPLETED"
                );
                break;
            }
            if start.elapsed() > timeout {
                tracing::error!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    batch_id = batch_id.0,
                    seg_id = segment_id.0,
                    "zero_copy_write: transfer TIMEOUT (cleaning up)"
                );
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", "zero_copy_write: EXIT (success)");
        Ok(())
    }

    pub(crate) async fn read_from_replica(
        &self,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            offset = replica.offset,
            base_addr = replica.base_addr,
            replica_size = replica.size,
            is_local = self.is_local_replica(replica),
            has_seg_buf = self.segment_buffer.is_some(),
            "read_from_replica: ENTER"
        );

        // Fast path: local segment — direct memcpy, no TE overhead
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
        let batch_id = self.engine.allocate_batch_id(1)?;
        tracing::info!(target: "te_debug", batch_id = batch_id.0, read_len, "read_from_replica: batch_id allocated");

        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset,
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

        self.engine.submit_transfer(batch_id, &[request])?;
        tracing::info!(target: "te_debug", "read_from_replica: transfer submitted, polling...");

        let mut transferred: u64;
        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);
        let mut poll_count: u64 = 0;
        loop {
            let status: transfer_engine_ffi::TransferStatus =
                self.engine.get_transfer_status(batch_id, 0)?;
            poll_count += 1;
            transferred = status.transferred_bytes;
            if status.status == TransferStatusEnum::Completed {
                tracing::info!(
                    target: "te_debug",
                    poll_count,
                    transferred,
                    elapsed_ms = start.elapsed().as_millis(),
                    "read_from_replica: transfer COMPLETED"
                );
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                tracing::error!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    batch_id = batch_id.0,
                    seg_id = segment_id.0,
                    "read_from_replica: transfer FAILED (leaking batch_id and segment!)"
                );
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(StoreError::OperationFailed(-1));
            }
            if start.elapsed() > timeout {
                tracing::error!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    batch_id = batch_id.0,
                    "read_from_replica: transfer TIMEOUT"
                );
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        tracing::info!(target: "te_debug", batch_id = batch_id.0, "read_from_replica: freeing batch_id");
        self.engine.free_batch_id(batch_id)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: closing segment");
        self.engine.close_segment(segment_id)?;

        let result: Vec<u8> = self.local_buffer[..(transferred as usize)].to_vec();
        tracing::info!(
            target: "te_debug",
            result_len = result.len(),
            "read_from_replica: EXIT (success)"
        );
        Ok(result)
    }

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

        let batch_id: transfer_engine_ffi::BatchId = self.engine.allocate_batch_id(1)?;
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "zero_copy_read: batch_id allocated");

        let request: TransferRequest = TransferRequest {
            opcode: Opcode::Read,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;
        tracing::info!(target: "te_debug", "zero_copy_read: transfer submitted, polling...");

        let mut transferred: u64;
        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);
        let mut poll_count: u64 = 0;
        loop {
            let status: transfer_engine_ffi::TransferStatus =
                self.engine.get_transfer_status(batch_id, 0)?;
            poll_count += 1;
            transferred = status.transferred_bytes;
            if status.status.is_terminal() {
                if status.status != TransferStatusEnum::Completed {
                    tracing::error!(
                        target: "te_debug",
                        poll_count,
                        elapsed_ms = start.elapsed().as_millis(),
                        batch_id = batch_id.0,
                        seg_id = segment_id.0,
                        "zero_copy_read: transfer FAILED (cleaning up)"
                    );
                    let _ = self.engine.free_batch_id(batch_id);
                    let _ = self.engine.close_segment(segment_id);
                    return Err(StoreError::OperationFailed(-1));
                }
                tracing::info!(
                    target: "te_debug",
                    poll_count,
                    transferred,
                    elapsed_ms = start.elapsed().as_millis(),
                    "zero_copy_read: transfer COMPLETED"
                );
                break;
            }
            if start.elapsed() > timeout {
                tracing::error!(
                    target: "te_debug",
                    poll_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    batch_id = batch_id.0,
                    seg_id = segment_id.0,
                    "zero_copy_read: transfer TIMEOUT (cleaning up)"
                );
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", transferred, "zero_copy_read: EXIT (success)");
        Ok(transferred as usize)
    }
}
