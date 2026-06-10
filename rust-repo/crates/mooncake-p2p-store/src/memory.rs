use crate::error::P2pStoreError;
use crate::store::MAX_CHUNK_SIZE;
use parking_lot::Mutex;
use std::ffi::c_void;
use std::sync::Arc;
use transfer_engine_ffi::TransferEngine;

#[derive(Debug, Clone)]
struct BufferHandle {
    addr: usize,
    length: u64,
    ref_count: usize,
}

#[derive(Debug)]
pub(crate) struct RegisteredMemory {
    engine: Arc<TransferEngine>,
    buffers: Mutex<Vec<BufferHandle>>,
}

impl RegisteredMemory {
    pub fn new(engine: Arc<TransferEngine>) -> Self {
        Self {
            engine,
            buffers: Mutex::new(Vec::new()),
        }
    }

    pub fn add(
        &self,
        addr: usize,
        length: u64,
        max_shard_size: u64,
        location: &str,
    ) -> Result<(), P2pStoreError> {
        validate_shard_size(max_shard_size)?;
        {
            let mut buffers = self.buffers.lock();
            for entry in buffers.iter_mut() {
                if entry.addr == addr && entry.length == length {
                    entry.ref_count += 1;
                    return Ok(());
                }
                let entry_end = entry.addr.saturating_add(entry.length as usize);
                let request_end = addr.saturating_add(length as usize);
                if addr < entry_end && request_end > entry.addr {
                    return Err(P2pStoreError::AddressOverlapped);
                }
            }
            buffers.push(BufferHandle {
                addr,
                length,
                ref_count: 1,
            });
        }

        let mut registered = Vec::new();
        let mut offset = 0;
        while offset < length {
            let chunk_size = MAX_CHUNK_SIZE.min(length - offset);
            let base = addr + offset as usize;
            let result = unsafe {
                self.engine.register_local_memory(
                    base as *mut c_void,
                    chunk_size as usize,
                    location,
                    true,
                )
            };
            if result.is_err() {
                self.rollback_registered(&registered);
                self.remove_handle_without_unregister(addr, length);
                return Err(P2pStoreError::TransferEngine);
            }
            registered.push(base);
            offset += MAX_CHUNK_SIZE;
        }

        Ok(())
    }

    pub fn remove(
        &self,
        addr: usize,
        length: u64,
        max_shard_size: u64,
    ) -> Result<(), P2pStoreError> {
        validate_shard_size(max_shard_size)?;
        let should_unregister = {
            let mut buffers = self.buffers.lock();
            let Some(index) = buffers
                .iter()
                .position(|entry| entry.addr == addr && entry.length == length)
            else {
                return Err(P2pStoreError::InvalidArgument);
            };
            if buffers[index].ref_count > 1 {
                buffers[index].ref_count -= 1;
                false
            } else {
                buffers.remove(index);
                true
            }
        };
        if !should_unregister {
            return Ok(());
        }

        let mut first_error = None;
        let mut offset = 0;
        while offset < length {
            let base = addr + offset as usize;
            let result = unsafe { self.engine.unregister_local_memory(base as *mut c_void) };
            if result.is_err() && first_error.is_none() {
                first_error = Some(P2pStoreError::TransferEngine);
            }
            offset += MAX_CHUNK_SIZE;
        }
        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn rollback_registered(&self, addrs: &[usize]) {
        for &addr in addrs {
            let _ = unsafe { self.engine.unregister_local_memory(addr as *mut c_void) };
        }
    }

    fn remove_handle_without_unregister(&self, addr: usize, length: u64) {
        let mut buffers = self.buffers.lock();
        if let Some(index) = buffers
            .iter()
            .position(|entry| entry.addr == addr && entry.length == length)
        {
            buffers.remove(index);
        }
    }
}

fn validate_shard_size(max_shard_size: u64) -> Result<(), P2pStoreError> {
    if max_shard_size == 0 || MAX_CHUNK_SIZE % max_shard_size != 0 {
        return Err(P2pStoreError::InvalidArgument);
    }
    Ok(())
}
