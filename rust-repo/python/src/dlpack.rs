//! DLPack-backed ownership capability for Python accelerator allocations.
//!
//! A Python object reference alone does not prove that a raw device address
//! remains allocated. DLPack transfers a managed allocation lease with an
//! explicit deleter, shape, dtype, device and byte offset. This module consumes
//! that lease and keeps it inside `transfer_engine_ffi::RegisteredMemory`.

use crate::to_py_err;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::ffi::c_void;
use std::fmt;
use std::ptr::NonNull;
use transfer_engine_ffi::{PointerMemoryType, StableMemoryOwner};

const DLPACK_MAJOR_VERSION: u32 = 1;
const DLPACK_READ_ONLY: u64 = 1;
const DLTENSOR_NAME: &[u8] = b"dltensor\0";
const USED_DLTENSOR_NAME: &[u8] = b"used_dltensor\0";
const VERSIONED_DLTENSOR_NAME: &[u8] = b"dltensor_versioned\0";
const USED_VERSIONED_DLTENSOR_NAME: &[u8] = b"used_dltensor_versioned\0";

const DL_CPU: i32 = 1;
const DL_CUDA: i32 = 2;
const DL_CUDA_HOST: i32 = 3;
const DL_ROCM: i32 = 10;
const DL_ROCM_HOST: i32 = 11;
const DL_CUDA_MANAGED: i32 = 13;

#[repr(C)]
#[derive(Clone, Copy)]
struct DLPackVersion {
    major: u32,
    minor: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DLDevice {
    device_type: i32,
    device_id: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DLDataType {
    code: u8,
    bits: u8,
    lanes: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DLTensor {
    data: *mut c_void,
    device: DLDevice,
    ndim: i32,
    dtype: DLDataType,
    shape: *mut i64,
    strides: *mut i64,
    byte_offset: u64,
}

#[repr(C)]
struct DLManagedTensor {
    dl_tensor: DLTensor,
    manager_ctx: *mut c_void,
    deleter: Option<unsafe extern "C" fn(*mut DLManagedTensor)>,
}

#[repr(C)]
struct DLManagedTensorVersioned {
    version: DLPackVersion,
    manager_ctx: *mut c_void,
    deleter: Option<unsafe extern "C" fn(*mut DLManagedTensorVersioned)>,
    flags: u64,
    dl_tensor: DLTensor,
}

enum ManagedTensorLease {
    Legacy {
        address: usize,
        deleter: Option<unsafe extern "C" fn(*mut DLManagedTensor)>,
    },
    Versioned {
        address: usize,
        deleter: Option<unsafe extern "C" fn(*mut DLManagedTensorVersioned)>,
    },
}

enum ManagedTensorDescriptor {
    Legacy {
        address: usize,
        deleter: Option<unsafe extern "C" fn(*mut DLManagedTensor)>,
    },
    Versioned {
        address: usize,
        deleter: Option<unsafe extern "C" fn(*mut DLManagedTensorVersioned)>,
    },
}

impl ManagedTensorDescriptor {
    fn into_lease(self) -> ManagedTensorLease {
        match self {
            Self::Legacy { address, deleter } => ManagedTensorLease::Legacy { address, deleter },
            Self::Versioned { address, deleter } => {
                ManagedTensorLease::Versioned { address, deleter }
            }
        }
    }
}

impl fmt::Debug for ManagedTensorLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Legacy { address, .. } => formatter
                .debug_struct("LegacyDLPackLease")
                .field("managed_tensor", address)
                .finish(),
            Self::Versioned { address, .. } => formatter
                .debug_struct("VersionedDLPackLease")
                .field("managed_tensor", address)
                .finish(),
        }
    }
}

impl Drop for ManagedTensorLease {
    fn drop(&mut self) {
        unsafe {
            match self {
                Self::Legacy { address, deleter } => {
                    if let Some(deleter) = deleter {
                        deleter(*address as *mut DLManagedTensor);
                    }
                }
                Self::Versioned { address, deleter } => {
                    if let Some(deleter) = deleter {
                        deleter(*address as *mut DLManagedTensorVersioned);
                    }
                }
            }
        }
    }
}

/// Allocation owner produced by a consumed DLPack capsule.
///
/// The managed tensor deleter runs only after native unregistration and all
/// typed in-flight region leases have quiesced.
pub(crate) struct DLPackMemoryOwner {
    base: usize,
    len: usize,
    location: String,
    lease: ManagedTensorLease,
    // The capsule is renamed to `used_dltensor[_versioned]`; retaining it
    // prevents unrelated Python mutation of the capsule object while its
    // managed pointer is owned here. Its destructor no longer calls deleter.
    _capsule: Py<PyAny>,
    // Retain the exact Python identity used by the binding registry. DLPack's
    // managed lease is the allocation proof, while this reference also
    // prevents Python object-address ABA during registration lookup.
    _python_owner: Py<PyAny>,
}

impl fmt::Debug for DLPackMemoryOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DLPackMemoryOwner")
            .field("base", &self.base)
            .field("len", &self.len)
            .field("location", &self.location)
            .field("lease", &self.lease)
            .finish()
    }
}

unsafe impl StableMemoryOwner for DLPackMemoryOwner {
    // The consumed DLManagedTensor is the allocation-lifetime proof. Its
    // producer-provided deleter is retained until this owner drops.
    fn base_address(&self) -> NonNull<c_void> {
        NonNull::new(self.base as *mut c_void).expect("validated DLPack device address")
    }

    fn length(&self) -> usize {
        self.len
    }
}

struct CapsuleView {
    tensor: DLTensor,
    read_only: bool,
    version_minor: Option<u32>,
    descriptor: ManagedTensorDescriptor,
    consumed_name: &'static [u8],
}

impl DLPackMemoryOwner {
    pub(crate) fn from_python(
        owner: &Bound<'_, PyAny>,
        expected_base: Option<usize>,
        requested_len: Option<usize>,
        requested_location: Option<&str>,
    ) -> PyResult<Self> {
        if !native_dlpack_interop_available() {
            return Err(to_py_err(
                "accelerator DLPack registration is unavailable: the current Transfer Engine C ABI cannot verify the native device ordinal or establish DMA-safe producer synchronization",
            ));
        }
        let capsule = export_capsule(owner)?;
        let view = inspect_capsule(&capsule)?;
        let (base, capacity, location) = validate_tensor(
            &view.tensor,
            view.read_only,
            view.version_minor,
            expected_base,
            requested_len,
            requested_location,
        )?;

        let memory_type = unsafe { transfer_engine_ffi::classify_pointer(base as *const c_void) }
            .map_err(to_py_err)?;
        if memory_type != PointerMemoryType::Device {
            return Err(to_py_err(format!(
                "DLPack address {base:#x} is not classified as device memory by the native runtime"
            )));
        }
        let len = requested_len.unwrap_or(capacity);
        let rename_result = unsafe {
            pyo3::ffi::PyCapsule_SetName(capsule.as_ptr(), view.consumed_name.as_ptr().cast())
        };
        if rename_result != 0 {
            return Err(PyErr::fetch(capsule.py()));
        }

        Ok(Self {
            base,
            len,
            location,
            lease: view.descriptor.into_lease(),
            _capsule: capsule.unbind(),
            _python_owner: owner.clone().unbind(),
        })
    }

    pub(crate) fn base(&self) -> usize {
        self.base
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn location(&self) -> &str {
        &self.location
    }
}

fn native_dlpack_interop_available() -> bool {
    // Classic Store parity intentionally uses only the existing TE C ABI.
    // Re-enable accelerator DLPack registration only behind an independently
    // reviewed native capability for exact device identity and synchronization.
    false
}

fn export_capsule<'py>(owner: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let kwargs = PyDict::new(owner.py());
    kwargs.set_item("max_version", (DLPACK_MAJOR_VERSION, 0_u32))?;
    match owner.call_method("__dlpack__", (), Some(&kwargs)) {
        Ok(capsule) => Ok(capsule),
        Err(error)
            if error.is_instance_of::<PyTypeError>(owner.py())
                && is_unsupported_max_version_error(&error) =>
        {
            owner.call_method0("__dlpack__")
        }
        Err(error) => Err(error),
    }
}

fn is_unsupported_max_version_error(error: &PyErr) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("max_version")
        && (message.contains("unexpected keyword")
            || message.contains("invalid keyword")
            || message.contains("takes no keyword")
            || message.contains("keyword arguments"))
}

fn inspect_capsule(capsule: &Bound<'_, PyAny>) -> PyResult<CapsuleView> {
    let is_versioned = unsafe {
        pyo3::ffi::PyCapsule_IsValid(capsule.as_ptr(), VERSIONED_DLTENSOR_NAME.as_ptr().cast())
    } != 0;
    if is_versioned {
        let pointer = unsafe {
            pyo3::ffi::PyCapsule_GetPointer(
                capsule.as_ptr(),
                VERSIONED_DLTENSOR_NAME.as_ptr().cast(),
            )
        };
        let pointer = NonNull::new(pointer.cast::<DLManagedTensorVersioned>())
            .ok_or_else(|| PyErr::fetch(capsule.py()))?;
        let version = unsafe { pointer.as_ref().version };
        if version.major != DLPACK_MAJOR_VERSION {
            return Err(to_py_err(format!(
                "unsupported DLPack ABI major version {}; expected {}",
                version.major, DLPACK_MAJOR_VERSION
            )));
        }
        let managed = unsafe { pointer.as_ref() };
        return Ok(CapsuleView {
            tensor: managed.dl_tensor,
            read_only: managed.flags & DLPACK_READ_ONLY != 0,
            version_minor: Some(version.minor),
            descriptor: ManagedTensorDescriptor::Versioned {
                address: pointer.as_ptr() as usize,
                deleter: managed.deleter,
            },
            consumed_name: USED_VERSIONED_DLTENSOR_NAME,
        });
    }

    let is_legacy =
        unsafe { pyo3::ffi::PyCapsule_IsValid(capsule.as_ptr(), DLTENSOR_NAME.as_ptr().cast()) }
            != 0;
    if !is_legacy {
        return Err(to_py_err(
            "__dlpack__ must return a fresh 'dltensor' or 'dltensor_versioned' PyCapsule",
        ));
    }
    let pointer =
        unsafe { pyo3::ffi::PyCapsule_GetPointer(capsule.as_ptr(), DLTENSOR_NAME.as_ptr().cast()) };
    let pointer = NonNull::new(pointer.cast::<DLManagedTensor>())
        .ok_or_else(|| PyErr::fetch(capsule.py()))?;
    let managed = unsafe { pointer.as_ref() };
    Ok(CapsuleView {
        tensor: managed.dl_tensor,
        read_only: false,
        version_minor: None,
        descriptor: ManagedTensorDescriptor::Legacy {
            address: pointer.as_ptr() as usize,
            deleter: managed.deleter,
        },
        consumed_name: USED_DLTENSOR_NAME,
    })
}

fn validate_tensor(
    tensor: &DLTensor,
    read_only: bool,
    version_minor: Option<u32>,
    expected_base: Option<usize>,
    requested_len: Option<usize>,
    requested_location: Option<&str>,
) -> PyResult<(usize, usize, String)> {
    if read_only {
        return Err(to_py_err(
            "DLPack tensor is read-only but Store registration requires read/write access",
        ));
    }
    if tensor.ndim < 0 || tensor.ndim > 64 {
        return Err(to_py_err(format!(
            "DLPack ndim {} is outside the supported range 0..=64",
            tensor.ndim
        )));
    }
    if tensor.dtype.bits == 0 || tensor.dtype.lanes == 0 {
        return Err(to_py_err("DLPack dtype bits and lanes must be non-zero"));
    }
    let element_bits = usize::from(tensor.dtype.bits)
        .checked_mul(usize::from(tensor.dtype.lanes))
        .ok_or_else(|| to_py_err("DLPack dtype size overflows usize"))?;
    if element_bits % 8 != 0 {
        return Err(to_py_err(
            "sub-byte DLPack dtypes are not supported for Store byte registration",
        ));
    }
    let element_bytes = element_bits / 8;

    let ndim = usize::try_from(tensor.ndim).expect("non-negative ndim fits usize");
    if ndim != 0 && tensor.shape.is_null() {
        return Err(to_py_err("DLPack shape is null for a non-scalar tensor"));
    }
    let shape = if ndim == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(tensor.shape, ndim) }
    };
    let mut elements = 1_usize;
    for &dimension in shape {
        if dimension < 0 {
            return Err(to_py_err("DLPack shape contains a negative dimension"));
        }
        elements = elements
            .checked_mul(dimension as usize)
            .ok_or_else(|| to_py_err("DLPack element count overflows usize"))?;
    }

    if ndim != 0 && tensor.strides.is_null() && version_minor.is_some_and(|minor| minor >= 2) {
        return Err(to_py_err(
            "DLPack 1.2+ tensor must provide explicit strides",
        ));
    }
    if !tensor.strides.is_null() {
        let strides = unsafe { std::slice::from_raw_parts(tensor.strides, ndim) };
        let mut expected_stride = 1_usize;
        for (&dimension, &stride) in shape.iter().zip(strides.iter()).rev() {
            if stride < 0 {
                return Err(to_py_err(
                    "negative-stride DLPack tensors are not supported",
                ));
            }
            if dimension > 1 && stride as usize != expected_stride {
                return Err(to_py_err(
                    "DLPack tensor must be C-contiguous for Store registration",
                ));
            }
            expected_stride = expected_stride
                .checked_mul(dimension as usize)
                .ok_or_else(|| to_py_err("DLPack stride extent overflows usize"))?;
        }
    }

    let capacity = elements
        .checked_mul(element_bytes)
        .ok_or_else(|| to_py_err("DLPack byte capacity overflows usize"))?;
    if capacity == 0 {
        return Err(to_py_err("zero-length DLPack tensors cannot be registered"));
    }
    let data = tensor.data as usize;
    if data == 0 {
        return Err(to_py_err("DLPack data pointer is null"));
    }
    let byte_offset = usize::try_from(tensor.byte_offset)
        .map_err(|_| to_py_err("DLPack byte offset cannot fit usize"))?;
    let base = data
        .checked_add(byte_offset)
        .ok_or_else(|| to_py_err("DLPack base address overflows usize"))?;
    if let Some(expected) = expected_base
        && expected != base
    {
        return Err(to_py_err(format!(
            "raw address {expected:#x} does not match DLPack view base {base:#x}"
        )));
    }
    if let Some(len) = requested_len {
        if len == 0 {
            return Err(to_py_err("registered DLPack size must be non-zero"));
        }
        if len > capacity {
            return Err(to_py_err(format!(
                "registered size {len} exceeds DLPack contiguous capacity {capacity}"
            )));
        }
    }

    let location = canonical_device_location(tensor.device)?;
    if let Some(requested) = requested_location
        && requested != location
    {
        return Err(to_py_err(format!(
            "requested location {requested:?} does not match DLPack device {location:?}"
        )));
    }
    Ok((base, capacity, location))
}

fn canonical_device_location(device: DLDevice) -> PyResult<String> {
    if device.device_id < 0 {
        return Err(to_py_err("DLPack device id must be non-negative"));
    }
    let prefix = match device.device_type {
        DL_CUDA => "cuda",
        DL_ROCM => "hip",
        DL_CUDA_MANAGED => {
            return Err(to_py_err(
                "CUDA managed memory does not have a stable device-local registration location",
            ));
        }
        DL_CPU | DL_CUDA_HOST | DL_ROCM_HOST => {
            return Err(to_py_err(
                "DLPack owner describes host memory; use the Python buffer protocol instead",
            ));
        }
        device_type => {
            return Err(to_py_err(format!(
                "DLPack device type {device_type} is not supported by the Store device owner"
            )));
        }
    };
    Ok(format!("{prefix}:{}", device.device_id))
}
