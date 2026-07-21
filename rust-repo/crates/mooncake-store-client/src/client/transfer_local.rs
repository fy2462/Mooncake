use super::MooncakeClient;
use crate::data_plane_ffi::ForeignMemoryRegion;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;

/// A Store-validated region of memory registered with the Transfer Engine.
///
/// The fields are private so data-plane operations cannot be invoked with an
/// arbitrary raw pointer. The unsafe registration API establishes the memory
/// lifetime invariant; region resolution enforces bounds for each operation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RegisteredBufferRegion {
    address: usize,
    len: usize,
}

impl RegisteredBufferRegion {
    pub(crate) fn as_mut_ptr(self) -> *mut c_void {
        self.address as *mut c_void
    }

    pub(crate) fn len(self) -> usize {
        self.len
    }

    pub(crate) fn foreign_region(self) -> ForeignMemoryRegion {
        ForeignMemoryRegion::from_caller_owned_raw(self.as_mut_ptr(), self.len)
    }
}

impl MooncakeClient {
    pub(crate) fn resolve_writable_buffer_region(
        &self,
        buffer: *mut c_void,
        requested_size: usize,
    ) -> StoreResult<RegisteredBufferRegion> {
        let target = buffer as usize;
        for (&base, &(size, _)) in self.registered_buffers.read().iter() {
            if target >= base {
                let offset = target - base;
                if offset < size && offset.saturating_add(requested_size) <= size {
                    return Ok(RegisteredBufferRegion {
                        address: target,
                        len: requested_size,
                    });
                }
            }
        }

        let base = self.local_buffer.as_ptr() as usize;
        if target >= base {
            let offset = target - base;
            if offset < self.local_buffer.len()
                && offset.saturating_add(requested_size) <= self.local_buffer.len()
            {
                return Ok(RegisteredBufferRegion {
                    address: target,
                    len: requested_size,
                });
            }
        }

        Err(StoreError::InvalidParams(
            "buffer is not within Store-managed writable memory".to_string(),
        ))
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
        let seg = self
            .segment_buffer
            .as_ref()
            .ok_or_else(|| StoreError::Internal("no local segment buffer".into()))?;
        let offset = replica.offset as usize;
        let len = data.len();
        // Bounds check / 越界检查
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

    /// Direct memory copy from the local segment buffer (same-node read).
    /// Only call this when `is_local_replica(replica)` returns `true` and
    /// `segment_buffer` is `Some`.
    ///
    /// 直接从本地 segment 缓冲区内存拷贝（同节点读）。
    /// 仅在 is_local_replica(replica) 为 true 且 segment_buffer 为 Some 时调用。
    pub(super) fn local_memcpy_read(&self, replica: &ReplicaDescriptor) -> StoreResult<Vec<u8>> {
        let seg = self
            .segment_buffer
            .as_ref()
            .ok_or_else(|| StoreError::Internal("no local segment buffer".into()))?;
        let offset = replica.offset as usize;
        let len = replica.size as usize;
        // Bounds check / 越界检查
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
    // 缓冲区注册（零拷贝路径）
    //
    // Externally-managed buffers must be registered with the TransferEngine
    // before they can be used as source/destination in zero-copy transfers.
    //
    // 外部管理的缓冲区必须先向 TransferEngine 注册，然后才能用作零拷贝传输的源/目标。
    // -----------------------------------------------------------------------

    /// Register an externally-managed buffer with the TransferEngine.
    ///
    /// After registration, the TE can DMA directly into/from this buffer,
    /// enabling true zero-copy I/O.
    ///
    /// 向 TransferEngine 注册外部管理的缓冲区。
    /// 注册后，TE 可以直接对此缓冲区进行 DMA 操作，实现真正的零拷贝 I/O。
    ///
    /// # Safety
    /// `buffer` must point to valid memory of at least `size` bytes and must
    /// remain alive until [`unregister_buffer`](Self::unregister_buffer) is called.
    ///
    /// buffer 必须指向至少 size 字节的有效内存，并且在调用 unregister_buffer
    /// 之前必须保持存活。
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

    /// Unregister a previously-registered buffer from the TransferEngine.
    /// 从 TransferEngine 取消注册之前注册的缓冲区。
    ///
    /// # Safety
    /// `buffer` must have been previously registered via `register_buffer`.
    /// buffer 必须之前已通过 register_buffer 注册。
    pub unsafe fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()> {
        unsafe {
            self.engine.unregister_local_memory(buffer)?;
        }
        self.registered_buffers.write().remove(&(buffer as usize));
        Ok(())
    }
}
