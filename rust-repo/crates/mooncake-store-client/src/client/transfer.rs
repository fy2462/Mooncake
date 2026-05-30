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
        // Fast path: local segment — direct memcpy, no TE overhead
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            return self.local_memcpy_write(replica, data);
        }

        if data.len() > self.local_buffer.len() {
            return Err(StoreError::InvalidParams(format!(
                "data size {} exceeds local buffer size {}",
                data.len(),
                self.local_buffer.len()
            )));
        }

        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        let batch_id = self.engine.allocate_batch_id(1)?;

        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.local_buffer.as_ptr() as *mut u8,
                data.len(),
            );
        }

        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset,
            length: data.len() as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);
        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            if status.status == TransferStatusEnum::Completed {
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                return Err(StoreError::OperationFailed(-1));
            }
            if start.elapsed() > timeout {
                self.engine.free_batch_id(batch_id)?;
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        Ok(())
    }

    pub(crate) async unsafe fn zero_copy_write(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<()> {
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        let batch_id = self.engine.allocate_batch_id(1)?;

        let request = TransferRequest {
            opcode: Opcode::Write,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            if status.status.is_terminal() {
                if status.status != TransferStatusEnum::Completed {
                    return Err(StoreError::OperationFailed(-1));
                }
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        Ok(())
    }

    pub(crate) async fn read_from_replica(
        &self,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        // Fast path: local segment — direct memcpy, no TE overhead
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            return self.local_memcpy_read(replica);
        }

        if replica.size > self.local_buffer.len() as u64 {
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds local buffer size {}",
                replica.size,
                self.local_buffer.len()
            )));
        }
        let segment_id = self.engine.open_segment(&replica.segment_name)?;

        let read_len = replica.size as usize;
        let batch_id = self.engine.allocate_batch_id(1)?;

        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset,
            length: read_len as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let mut transferred: u64;
        let start = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(10);
        loop {
            let status: transfer_engine_ffi::TransferStatus =
                self.engine.get_transfer_status(batch_id, 0)?;
            transferred = status.transferred_bytes;
            if status.status == TransferStatusEnum::Completed {
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                return Err(StoreError::OperationFailed(-1));
            }
            if start.elapsed() > timeout {
                self.engine.free_batch_id(batch_id)?;
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;

        let result: Vec<u8> = self.local_buffer[..(transferred as usize)].to_vec();
        Ok(result)
    }

    pub(crate) async unsafe fn zero_copy_read(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        let segment_id: transfer_engine_ffi::SegmentId =
            self.engine.open_segment(&replica.segment_name)?;
        let batch_id: transfer_engine_ffi::BatchId = self.engine.allocate_batch_id(1)?;

        let request: TransferRequest = TransferRequest {
            opcode: Opcode::Read,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let mut transferred: u64;
        loop {
            let status: transfer_engine_ffi::TransferStatus =
                self.engine.get_transfer_status(batch_id, 0)?;
            transferred = status.transferred_bytes;
            if status.status.is_terminal() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        Ok(transferred as usize)
    }
}
