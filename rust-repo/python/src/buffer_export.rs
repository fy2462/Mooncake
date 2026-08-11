//! Buffer-protocol export that tolerates PEP 3118 exporters returning a NULL
//! `strides` pointer for C-contiguous memory.
//!
//! pyo3 0.28 rejects NULL strides in `PyUntypedBuffer::get` with
//! `BufferError: strides is null`, but `ctypes` arrays (and other exporters)
//! legally omit strides for contiguous layouts. This wrapper falls back to a
//! raw `PyObject_GetBuffer` + `PyBuffer_Release` view when that happens.

use pyo3::buffer::PyUntypedBuffer;
use pyo3::exceptions::PyBufferError;
use pyo3::ffi;
use pyo3::prelude::*;
use std::marker::PhantomPinned;
use std::pin::Pin;

#[derive(Debug)]
pub(crate) enum ExportedBufferView {
    Py(PyUntypedBuffer),
    Manual(ManualPyBuffer),
}

pub(crate) struct ManualPyBuffer {
    // A Py_buffer exporter may make fields point back into the Py_buffer
    // itself. Keep it at the address where PyObject_GetBuffer initialized it.
    view: Pin<Box<RawBuffer>>,
}

#[repr(transparent)]
struct RawBuffer(ffi::Py_buffer, PhantomPinned);

impl std::fmt::Debug for ManualPyBuffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManualPyBuffer")
            .field("buf", &self.view.0.buf)
            .field("len", &self.view.0.len)
            .field("readonly", &self.view.0.readonly)
            .finish()
    }
}

impl Drop for ManualPyBuffer {
    fn drop(&mut self) {
        let _ = Python::try_attach(|_| {
            // SAFETY: `view` was initialized by a successful PyObject_GetBuffer
            // and is released exactly once here while the GIL is held.
            unsafe { ffi::PyBuffer_Release(&mut Pin::get_unchecked_mut(self.view.as_mut()).0) }
        });
        // With no attached interpreter, the Py_buffer has no Rust destructor,
        // so its exporter reference is intentionally leaked instead of being
        // released without the GIL (matching PyUntypedBuffer's behavior).
    }
}

// SAFETY: the view keeps its exporter alive via `view.obj`, and release is
// performed with an attached interpreter; this matches pyo3's own
// `unsafe impl Send for PyUntypedBuffer`.
unsafe impl Send for ManualPyBuffer {}
// SAFETY: the exporter is pinned by `view.obj` for the view lifetime, and the
// raw pointers are only dereferenced while the export is alive.
unsafe impl Sync for ManualPyBuffer {}

impl ExportedBufferView {
    pub(crate) fn get(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        match PyUntypedBuffer::get(obj) {
            Ok(buffer) => Ok(Self::Py(buffer)),
            Err(_) => {
                let mut raw = Box::<RawBuffer>::new_uninit();
                // SAFETY: PyObject_GetBuffer initializes `raw` on success.
                let rc = unsafe {
                    ffi::PyObject_GetBuffer(
                        obj.as_ptr(),
                        raw.as_mut_ptr().cast::<ffi::Py_buffer>(),
                        ffi::PyBUF_FULL_RO,
                    )
                };
                if rc != 0 {
                    return Err(PyErr::fetch(obj.py()));
                }
                // SAFETY: initialized on success (rc == 0).
                let mut view = Pin::from(unsafe { raw.assume_init() });
                if view.0.shape.is_null() {
                    // SAFETY: released once; we own the initialized view.
                    unsafe { ffi::PyBuffer_Release(&mut Pin::get_unchecked_mut(view.as_mut()).0) };
                    return Err(PyBufferError::new_err("shape is null"));
                }
                Ok(Self::Manual(ManualPyBuffer { view }))
            }
        }
    }

    pub(crate) fn buf_ptr(&self) -> *mut std::ffi::c_void {
        match self {
            Self::Py(buffer) => buffer.buf_ptr(),
            Self::Manual(buffer) => buffer.view.0.buf,
        }
    }

    pub(crate) fn len_bytes(&self) -> usize {
        match self {
            Self::Py(buffer) => buffer.len_bytes(),
            Self::Manual(buffer) => buffer.view.0.len.max(0) as usize,
        }
    }

    pub(crate) fn readonly(&self) -> bool {
        match self {
            Self::Py(buffer) => buffer.readonly(),
            Self::Manual(buffer) => buffer.view.0.readonly == 1,
        }
    }

    pub(crate) fn is_c_contiguous(&self) -> bool {
        match self {
            Self::Py(buffer) => buffer.is_c_contiguous(),
            Self::Manual(buffer) => {
                // SAFETY: `view` is a live Py_buffer attached to the
                // interpreter; PyBuffer_IsContiguous handles NULL strides.
                unsafe { ffi::PyBuffer_IsContiguous(&buffer.view.0, b'C' as std::ffi::c_char) != 0 }
            }
        }
    }
}
