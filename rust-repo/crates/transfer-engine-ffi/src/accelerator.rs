//! Minimal accelerator-memory primitives exposed by the native data plane.

use crate::ffi;
use crate::{TransferEngineError, TransferEngineResult};
use std::ffi::c_void;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PointerMemoryType {
    Host,
    Device,
}

impl PointerMemoryType {
    fn from_raw(value: i32) -> TransferEngineResult<Self> {
        match value {
            value if value == ffi::MEMORY_POINTER_HOST as i32 => Ok(Self::Host),
            value if value == ffi::MEMORY_POINTER_DEVICE as i32 => Ok(Self::Device),
            value => Err(TransferEngineError::OperationFailed(value)),
        }
    }
}

/// Classify an address without constructing a Rust reference to its memory.
///
/// # Safety
/// `pointer` must identify an address recognized by the active host or
/// accelerator runtime and remain allocated for the duration of the call.
pub unsafe fn classify_pointer(pointer: *const c_void) -> TransferEngineResult<PointerMemoryType> {
    PointerMemoryType::from_raw(unsafe { ffi::classifyMemoryPointer(pointer) })
}

/// Copy memory that may reside on an accelerator into a host slice.
///
/// # Safety
/// `source` must be readable for `destination.len()` bytes.
pub unsafe fn copy_to_host(
    destination: &mut [u8],
    source: *const c_void,
) -> TransferEngineResult<()> {
    let result = unsafe {
        ffi::copyMemoryToHost(destination.as_mut_ptr().cast(), source, destination.len())
    };
    if result == 0 {
        Ok(())
    } else {
        Err(TransferEngineError::OperationFailed(result))
    }
}

/// Copy a host slice into memory that may reside on an accelerator.
///
/// # Safety
/// `destination` must be writable for `source.len()` bytes.
pub unsafe fn copy_from_host(destination: *mut c_void, source: &[u8]) -> TransferEngineResult<()> {
    let result =
        unsafe { ffi::copyMemoryFromHost(destination, source.as_ptr().cast(), source.len()) };
    if result == 0 {
        Ok(())
    } else {
        Err(TransferEngineError::OperationFailed(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_memory_type_rejects_unknown_native_values() {
        assert_eq!(
            PointerMemoryType::from_raw(ffi::MEMORY_POINTER_HOST as i32).unwrap(),
            PointerMemoryType::Host
        );
        assert_eq!(
            PointerMemoryType::from_raw(ffi::MEMORY_POINTER_DEVICE as i32).unwrap(),
            PointerMemoryType::Device
        );
        assert!(matches!(
            PointerMemoryType::from_raw(-1),
            Err(TransferEngineError::OperationFailed(-1))
        ));
    }
}
