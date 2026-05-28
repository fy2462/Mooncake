use mooncake_store_core::{ReplicaDescriptor, StoreError};
use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
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
        unsafe { self.engine.unregister_local_memory(buffer)?; }
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

    pub(crate) async fn fetch_replicas(&mut self, key: &str) -> StoreResult<Vec<ReplicaDescriptor>> {
        let request = proto::GetReplicaListRequest { key: key.to_string() };
        let response = self
            .master
            .get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(self.replicas_from_proto(&response.replicas))
    }

    pub(crate) fn replicas_from_proto(&self, replicas: &[proto::ReplicaDescriptor]) -> Vec<ReplicaDescriptor> {
        replicas.iter().filter_map(|r| {
            let sid = r.segment_id.as_ref()?;
            Some(ReplicaDescriptor {
                refcnt: 0,
                segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                segment_name: r.segment_name.clone(),
                offset: r.offset,
                size: r.size,
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
        }).collect()
    }

    pub(crate) fn select_best_replica<'a>(&self, replicas: &'a [ReplicaDescriptor]) -> Option<&'a ReplicaDescriptor> {
        // C++ SelectBestReplica 优先级：local MEMORY > any MEMORY > N_OF_SSD > LOCAL_DISK > DISK
        // Priority: local MEMORY > any MEMORY > N_OF_SSD > LOCAL_DISK > DISK
        replicas.iter()
            .filter(|r| r.status == mooncake_store_core::ReplicaStatus::Complete)
            .max_by_key(|r| match r.replica_type {
                mooncake_store_core::ReplicaType::Memory => 3,
                mooncake_store_core::ReplicaType::NoFSsd => 2,
                mooncake_store_core::ReplicaType::LocalDisk => 1,
                mooncake_store_core::ReplicaType::Disk => 0,
                _ => -1,
            })
    }

    pub(crate) async fn write_to_replica(
        &self,
        replica: &ReplicaDescriptor,
        data: &[u8],
    ) -> StoreResult<()> {
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

        let request = TransferRequest {
            opcode: Opcode::Write,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset: replica.offset,
            length: data.len() as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            if status.status == TransferStatusEnum::Completed {
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                return Err(StoreError::OperationFailed(-1));
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
            target_offset: replica.offset,
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

        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset: replica.offset,
            length: read_len as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let mut transferred: u64;
        loop {
            let status: transfer_engine_ffi::TransferStatus = self.engine.get_transfer_status(batch_id, 0)?;
            transferred = status.transferred_bytes;
            if status.status == TransferStatusEnum::Completed {
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                return Err(StoreError::OperationFailed(-1));
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
        let segment_id: transfer_engine_ffi::SegmentId = self.engine.open_segment(&replica.segment_name)?;
        let batch_id: transfer_engine_ffi::BatchId = self.engine.allocate_batch_id(1)?;

        let request: TransferRequest = TransferRequest {
            opcode: Opcode::Read,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let mut transferred: u64;
        loop {
            let status: transfer_engine_ffi::TransferStatus = self.engine.get_transfer_status(batch_id, 0)?;
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
