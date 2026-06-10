use crate::to_py_err;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyByteArray;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Default)]
struct PoolState {
    free: BTreeMap<usize, Vec<Vec<u8>>>,
    borrowed_bytes: usize,
    closed: bool,
}

#[pyclass(name = "BufferPool", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct BufferPoolPy {
    max_bytes: usize,
    min_size_class: usize,
    alignment: usize,
    state: Arc<Mutex<PoolState>>,
}

#[pymethods]
impl BufferPoolPy {
    #[new]
    #[pyo3(signature = (max_bytes, min_size_class = 4096, alignment = 4096))]
    fn new(max_bytes: usize, min_size_class: usize, alignment: usize) -> PyResult<Self> {
        if max_bytes == 0 {
            return Err(to_py_err("max_bytes must be > 0"));
        }
        if min_size_class == 0 || alignment == 0 {
            return Err(to_py_err("min_size_class and alignment must be > 0"));
        }
        Ok(Self {
            max_bytes,
            min_size_class,
            alignment,
            state: Arc::new(Mutex::new(PoolState::default())),
        })
    }

    #[pyo3(signature = (size, block = true, timeout = None))]
    fn acquire<'py>(
        &self,
        py: Python<'py>,
        size: usize,
        block: bool,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyByteArray>> {
        let _ = (block, timeout);
        let class = self.size_class(size)?;
        let mut state = self.state.lock();
        if state.closed {
            return Err(to_py_err("buffer pool is closed"));
        }
        if state.borrowed_bytes + class > self.max_bytes {
            return Err(to_py_err("buffer pool capacity exceeded"));
        }
        let mut data = state
            .free
            .get_mut(&class)
            .and_then(|buffers| buffers.pop())
            .unwrap_or_else(|| vec![0; class]);
        data.resize(class, 0);
        state.borrowed_bytes += class;
        Ok(PyByteArray::new(py, &data))
    }

    #[pyo3(signature = (size))]
    fn buffer<'py>(&self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyByteArray>> {
        self.acquire(py, size, true, None)
    }

    fn release(&self, buffer: &Bound<'_, PyAny>) -> PyResult<()> {
        let len = buffer.len()?;
        let class = self.size_class(len)?;
        let mut state = self.state.lock();
        if state.closed {
            return Ok(());
        }
        if state.borrowed_bytes >= class {
            state.borrowed_bytes -= class;
        } else {
            state.borrowed_bytes = 0;
        }
        state.free.entry(class).or_default().push(vec![0; class]);
        Ok(())
    }

    #[pyo3(signature = (size, count))]
    fn prewarm(&self, size: usize, count: usize) -> PyResult<()> {
        let class = self.size_class(size)?;
        let mut state = self.state.lock();
        if state.closed {
            return Err(to_py_err("buffer pool is closed"));
        }
        for _ in 0..count {
            state.free.entry(class).or_default().push(vec![0; class]);
        }
        Ok(())
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.free.clear();
        state.borrowed_bytes = 0;
        state.closed = true;
    }

    #[getter]
    fn borrowed_bytes(&self) -> usize {
        self.state.lock().borrowed_bytes
    }

    #[getter]
    fn capacity_bytes(&self) -> usize {
        self.max_bytes
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_value: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) {
        let _ = (exc_type, exc_value, traceback);
        self.close();
    }
}

impl BufferPoolPy {
    fn size_class(&self, size: usize) -> PyResult<usize> {
        if size == 0 {
            return Err(to_py_err("size must be > 0"));
        }
        let base = size.max(self.min_size_class);
        Ok(base.div_ceil(self.alignment) * self.alignment)
    }
}

#[pyclass(name = "RegisteredBufferPool", skip_from_py_object)]
pub(crate) struct RegisteredBufferPoolPy {
    pool: BufferPoolPy,
}

#[pymethods]
impl RegisteredBufferPoolPy {
    #[new]
    #[pyo3(signature = (max_bytes, min_size_class = 4096, alignment = 4096))]
    fn new(max_bytes: usize, min_size_class: usize, alignment: usize) -> PyResult<Self> {
        Ok(Self {
            pool: BufferPoolPy::new(max_bytes, min_size_class, alignment)?,
        })
    }

    #[pyo3(signature = (size, block = true, timeout = None))]
    fn acquire<'py>(
        &self,
        py: Python<'py>,
        size: usize,
        block: bool,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyByteArray>> {
        self.pool.acquire(py, size, block, timeout)
    }

    #[pyo3(signature = (size))]
    fn buffer<'py>(&self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyByteArray>> {
        self.pool.buffer(py, size)
    }

    fn release(&self, buffer: &Bound<'_, PyAny>) -> PyResult<()> {
        self.pool.release(buffer)
    }

    #[pyo3(signature = (size, count))]
    fn prewarm(&self, size: usize, count: usize) -> PyResult<()> {
        self.pool.prewarm(size, count)
    }

    fn close(&self) {
        self.pool.close();
    }

    #[getter]
    fn borrowed_bytes(&self) -> usize {
        self.pool.borrowed_bytes()
    }

    #[getter]
    fn capacity_bytes(&self) -> usize {
        self.pool.capacity_bytes()
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_value: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) {
        let _ = (exc_type, exc_value, traceback);
        self.close();
    }
}
