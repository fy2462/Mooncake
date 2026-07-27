use super::MooncakeClient;
use crate::data_plane_ffi::ForeignMemoryRegion;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{
    ReadableRegisteredMemoryRegion, RegisteredMemoryAccess, RegisteredMemoryId, StableMemoryOwner,
    WritableRegisteredMemoryRegion,
};

pub use transfer_engine_ffi::RegisteredMemoryId as BufferRegistrationId;

fn ranges_overlap(
    first_base: usize,
    first_end: usize,
    second_base: usize,
    second_end: usize,
) -> bool {
    first_base < second_end && second_base < first_end
}

fn validate_target_address(buffer: *mut c_void) -> StoreResult<usize> {
    let target = buffer as usize;
    if target == 0 {
        return Err(StoreError::InvalidParams(
            "buffer address must not be null".to_string(),
        ));
    }
    Ok(target)
}

fn unregistered_buffer_error() -> StoreError {
    StoreError::InvalidParams("buffer is not within externally registered memory".to_string())
}

#[derive(Clone, Debug)]
pub(crate) struct ReadableBufferRegion {
    len: usize,
    registration: ReadableRegisteredMemoryRegion,
}

impl ReadableBufferRegion {
    pub(crate) fn as_mut_ptr(&self) -> *mut c_void {
        self.registration.as_ptr() as *mut c_void
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn foreign_region(&self) -> ForeignMemoryRegion {
        ForeignMemoryRegion::from_caller_owned_raw(self.as_mut_ptr(), self.len)
    }

    pub(crate) fn registered_region(&self) -> ReadableRegisteredMemoryRegion {
        self.registration.clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WritableBufferRegion {
    len: usize,
    registration: WritableRegisteredMemoryRegion,
}

impl WritableBufferRegion {
    pub(crate) fn as_mut_ptr(&self) -> *mut c_void {
        self.registration.as_mut_ptr()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn foreign_region(&self) -> ForeignMemoryRegion {
        ForeignMemoryRegion::from_caller_owned_raw(self.as_mut_ptr(), self.len)
    }

    pub(crate) fn registered_region(&self) -> WritableRegisteredMemoryRegion {
        self.registration.clone()
    }
}

impl MooncakeClient {
    pub(crate) fn resolve_readable_buffer_region(
        &self,
        buffer: *mut c_void,
        requested_size: usize,
    ) -> StoreResult<ReadableBufferRegion> {
        let target = validate_target_address(buffer)?;
        for registration in self.registered_buffers.read().values() {
            let id = registration.id()?;
            if target >= id.base_address() {
                let offset = target - id.base_address();
                if let Ok(region) = registration.readable_region(offset, requested_size) {
                    return Ok(ReadableBufferRegion {
                        len: requested_size,
                        registration: region,
                    });
                }
            }
        }
        Err(unregistered_buffer_error())
    }

    pub(crate) fn resolve_writable_buffer_region(
        &self,
        buffer: *mut c_void,
        requested_size: usize,
    ) -> StoreResult<WritableBufferRegion> {
        let target = validate_target_address(buffer)?;
        for registration in self.registered_buffers.read().values() {
            let id = registration.id()?;
            if target >= id.base_address() {
                let offset = target - id.base_address();
                if let Ok(region) = registration.writable_region(offset, requested_size) {
                    return Ok(WritableBufferRegion {
                        len: requested_size,
                        registration: region,
                    });
                }
            }
        }
        Err(unregistered_buffer_error())
    }

    // -----------------------------------------------------------------------
    // LOCAL_MEMCPY — bypass TE for same-node transfers (matches C++ strategy)
    // 本地内存拷贝 —— 同节点传输绕过 TE（与 C++ 策略一致）
    //
    // When the target replica is hosted on a locally-mounted segment, we can
    // directly memcpy into/from the segment_buffer without involving the
    // TransferEngine at all. This avoids RDMA/TCP stack overhead entirely.
    //
    // 当目标副本位于本地挂载的 segment 上时，可以直接从 segment_buffer
    // 进行 memcpy，完全不需要 TransferEngine。这避免了 RDMA/TCP 栈的全部开销。
    // C++ equivalent: real_client.cpp local memcpy paths inside
    // ReadFromReplica / WriteToReplica.
    // -----------------------------------------------------------------------

    /// Check whether a replica's segment is locally mounted on this node.
    /// 检查副本的 segment 是否在本地节点上挂载。
    pub(super) fn is_local_replica(&self, replica: &ReplicaDescriptor) -> bool {
        self.local_endpoints.read().contains(&replica.segment_name)
    }

    pub(super) fn local_owned_segment(
        &self,
        replica: &ReplicaDescriptor,
    ) -> Option<&super::OwnedStoreSegment> {
        self.owned_store_segments.iter().find(|segment| {
            segment.segment_name == replica.segment_name && segment.base_addr() == replica.base_addr
        })
    }

    /// Direct memory copy into the local segment buffer (same-node write).
    /// Only call this when `is_local_replica(replica)` returns `true` and
    /// `segment_buffer` is `Some`.
    ///
    /// 直接内存拷贝到本地 segment 缓冲区（同节点写）。
    /// 仅在 is_local_replica(replica) 为 true 且 segment_buffer 为 Some 时调用。
    pub(super) fn local_memcpy_write(
        &self,
        replica: &ReplicaDescriptor,
        data: &[u8],
    ) -> StoreResult<()> {
        let segment = self.local_owned_segment(replica).ok_or_else(|| {
            StoreError::Internal(format!(
                "no owned local segment for name={:?} base_addr={}",
                replica.segment_name, replica.base_addr
            ))
        })?;
        let seg = &segment.buffer;
        let offset = usize::try_from(replica.offset).map_err(|_| {
            StoreError::InvalidParams(format!(
                "local write offset cannot fit usize: {}",
                replica.offset
            ))
        })?;
        seg.copy_from_slice(offset, data)
    }

    /// Direct memory copy from the local segment buffer (same-node read).
    /// Only call this when `is_local_replica(replica)` returns `true` and
    /// `segment_buffer` is `Some`.
    ///
    /// 直接从本地 segment 缓冲区内存拷贝（同节点读）。
    /// 仅在 is_local_replica(replica) 为 true 且 segment_buffer 为 Some 时调用。
    pub(super) fn local_memcpy_read(&self, replica: &ReplicaDescriptor) -> StoreResult<Vec<u8>> {
        let segment = self.local_owned_segment(replica).ok_or_else(|| {
            StoreError::Internal(format!(
                "no owned local segment for name={:?} base_addr={}",
                replica.segment_name, replica.base_addr
            ))
        })?;
        let seg = &segment.buffer;
        let offset = usize::try_from(replica.offset).map_err(|_| {
            StoreError::InvalidParams(format!(
                "local read offset cannot fit usize: {}",
                replica.offset
            ))
        })?;
        let len = usize::try_from(replica.size).map_err(|_| {
            StoreError::InvalidParams(format!(
                "local read size cannot fit usize: {}",
                replica.size
            ))
        })?;
        seg.copy_to_vec(offset, len)
    }

    // -----------------------------------------------------------------------
    // Buffer registration (zero-copy path)
    // 缓冲区注册（零拷贝路径）
    //
    // Externally-managed buffers must be registered with the TransferEngine
    // before they can be used as source/destination in zero-copy transfers.
    //
    // 外部管理的缓冲区必须先向 TransferEngine 注册，然后才能用作零拷贝传输的源/目标。
    // -----------------------------------------------------------------------

    /// Register an allocation-owning stable-memory capability.
    ///
    /// The Transfer Engine FFI owns `owner` until native unregistration
    /// succeeds. This is the preferred safe boundary for language bindings and
    /// other adapters that can retain the real allocation owner.
    pub fn register_owned_buffer<O>(
        &self,
        owner: O,
        location: &str,
    ) -> StoreResult<BufferRegistrationId>
    where
        O: StableMemoryOwner,
    {
        let base = owner.base_address().as_ptr() as usize;
        let size = owner.length();
        let end = base.checked_add(size).ok_or_else(|| {
            StoreError::InvalidParams("registered buffer range overflows usize".to_string())
        })?;
        let local_base = self.local_buffer.base_ptr() as usize;
        let local_end = local_base
            .checked_add(self.local_buffer.len())
            .ok_or_else(|| StoreError::Internal("local buffer range overflow".to_string()))?;
        if ranges_overlap(base, end, local_base, local_end) {
            return Err(StoreError::InvalidParams(
                "external registration overlaps the Store local buffer".to_string(),
            ));
        }
        for segment in &self.owned_store_segments {
            let segment_base = segment.buffer.as_ptr() as usize;
            let segment_end = segment_base
                .checked_add(segment.buffer.len())
                .ok_or_else(|| StoreError::Internal("segment buffer range overflow".to_string()))?;
            if ranges_overlap(base, end, segment_base, segment_end) {
                return Err(StoreError::InvalidParams(
                    "external registration overlaps the Store segment buffer".to_string(),
                ));
            }
        }

        let mut registrations = self.registered_buffers.write();
        let registration = self.engine.register_owned_memory(
            owner,
            location,
            true,
            RegisteredMemoryAccess::ReadWrite,
        );
        let registration = registration?;
        let registration_id = registration.id()?;
        registrations.insert(base, registration);
        Ok(registration_id)
    }

    /// Unregister one exact registration generation from the TransferEngine.
    ///
    /// This operation is rejected while a Store transfer holds a region lease.
    /// A stale identity is also rejected without touching a newer registration
    /// at the same address.
    pub fn unregister_buffer_handle(
        &self,
        registration_id: BufferRegistrationId,
    ) -> StoreResult<()> {
        self.unregister_buffer_generation(registration_id)
    }

    fn unregister_buffer_generation(&self, registration_id: RegisteredMemoryId) -> StoreResult<()> {
        let base = registration_id.base_address();
        let mut registrations = self.registered_buffers.write();
        {
            let registration = registrations.get_mut(&base).ok_or_else(|| {
                StoreError::InvalidParams(format!(
                    "buffer {base:#x} is not an exact registered base address"
                ))
            })?;
            let current_id = registration.id()?;
            if current_id != registration_id {
                return Err(StoreError::InvalidParams(format!(
                    "stale buffer registration identity: expected generation {}, current generation {} at {base:#x}",
                    registration_id.generation(),
                    current_id.generation()
                )));
            }
            self.engine.unregister_owned_memory(registration)?;
        }
        registrations.remove(&base);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_ranges_use_half_open_overlap_semantics() {
        assert!(ranges_overlap(100, 200, 150, 250));
        assert!(ranges_overlap(100, 200, 50, 101));
        assert!(!ranges_overlap(100, 200, 200, 300));
        assert!(!ranges_overlap(100, 200, 0, 100));
    }
}
