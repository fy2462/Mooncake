use crate::client::{get_buffer_ptr, replicas_to_py, take_client, PythonMooncakeClient};
use crate::to_py_err;
use mooncake_store_client::CachedQueryResultResponse;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::ffi::c_void;

fn cached_query_to_py(result: CachedQueryResultResponse) -> PyResult<Py<PyAny>> {
    let py = unsafe { Python::assume_attached() };
    let lease_expired = result.is_lease_expired();
    let dict = PyDict::new(py);
    dict.set_item("success", result.success)?;
    dict.set_item("replicas", replicas_to_py(result.replicas))?;
    dict.set_item("lease_expired", lease_expired)?;
    dict.set_item("error_status", result.error_status)?;
    dict.set_item("error_message", result.error_message)?;
    Ok(dict.into_any().unbind())
}

pub(crate) fn build_nof_te_endpoint(
    nqn: String,
    nsid: u64,
    traddr: String,
    trsvcid: u64,
    trtype: Option<String>,
) -> String {
    mooncake_store_client::MooncakeClient::build_nof_te_endpoint_with_trtype(
        &nqn,
        nsid,
        &traddr,
        trsvcid,
        trtype.as_deref(),
    )
}

pub(crate) fn register_nof_ssd<'py>(
    slf: &Bound<'py, PythonMooncakeClient>,
    py: Python<'py>,
    nqn: String,
    nsid: u64,
    traddr: String,
    trsvcid: u64,
    base: u64,
    size: u64,
    trtype: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let inner = slf.borrow().inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut client = take_client(&inner)?;
        let segment = client
            .register_nof_ssd_with_trtype(
                &nqn,
                nsid,
                &traddr,
                trsvcid,
                base,
                size,
                trtype.as_deref(),
            )
            .await;
        *inner.lock() = Some(client);
        let segment = segment.map_err(to_py_err)?;
        Ok((
            segment.id.to_string(),
            segment.name,
            segment.base,
            segment.size,
            segment.te_endpoint,
            segment.client_id.to_string(),
        ))
    })
}

pub(crate) fn unregister_nof_ssd_by_endpoint<'py>(
    slf: &Bound<'py, PythonMooncakeClient>,
    py: Python<'py>,
    nqn: String,
    nsid: u64,
    traddr: String,
    trsvcid: u64,
    trtype: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let inner = slf.borrow().inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut client = take_client(&inner)?;
        let result = client
            .unregister_nof_ssd_by_endpoint_with_trtype(
                &nqn,
                nsid,
                &traddr,
                trsvcid,
                trtype.as_deref(),
            )
            .await;
        *inner.lock() = Some(client);
        result.map_err(to_py_err)
    })
}

pub(crate) fn batch_get_query_results(
    slf: &Bound<'_, PythonMooncakeClient>,
    keys: Vec<String>,
) -> PyResult<Vec<Py<PyAny>>> {
    let inner = slf.borrow().inner.clone();
    let results = tokio::runtime::Handle::current().block_on(async {
        let mut client = take_client(&inner)?;
        let result = client.batch_get_query_results(&keys).await;
        *inner.lock() = Some(client);
        result.map_err(to_py_err)
    })?;
    results.into_iter().map(cached_query_to_py).collect()
}

pub(crate) fn get_segments_detail<'py>(
    slf: &Bound<'py, PythonMooncakeClient>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let inner = slf.borrow().inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut client = take_client(&inner)?;
        let details = client.get_segments_detail().await.map_err(to_py_err)?;
        *inner.lock() = Some(client);
        let out: Vec<HashMap<String, String>> = details
            .into_iter()
            .map(|detail| {
                HashMap::from([
                    ("segment_name".to_string(), detail.segment_name),
                    ("segment_id".to_string(), detail.segment_id.to_string()),
                    ("client_id".to_string(), detail.client_id.to_string()),
                    ("base_address".to_string(), detail.base_address.to_string()),
                    ("size_bytes".to_string(), detail.size_bytes.to_string()),
                    ("te_endpoint".to_string(), detail.te_endpoint),
                    ("protocol".to_string(), detail.protocol),
                    ("status".to_string(), detail.status.to_string()),
                    (
                        "allocator_used_bytes".to_string(),
                        detail.allocator_used_bytes.to_string(),
                    ),
                    (
                        "allocator_capacity_bytes".to_string(),
                        detail.allocator_capacity_bytes.to_string(),
                    ),
                    ("nof".to_string(), detail.nof.to_string()),
                ])
            })
            .collect();
        Ok(out)
    })
}

pub(crate) fn get_into_ranges_cached(
    slf: &Bound<'_, PythonMooncakeClient>,
    buffers: Vec<Bound<'_, PyAny>>,
    all_keys: Vec<Vec<String>>,
    all_dst_offsets: Vec<Vec<Vec<usize>>>,
    all_src_offsets: Vec<Vec<Vec<usize>>>,
    all_sizes: Vec<Vec<Vec<usize>>>,
) -> PyResult<Vec<Vec<Vec<i64>>>> {
    let ptrs: Vec<*mut c_void> = buffers
        .iter()
        .map(|buffer| get_buffer_ptr(buffer).map(|(ptr, _)| ptr))
        .collect::<PyResult<_>>()?;
    let mut unique_keys = Vec::<String>::new();
    for keys in &all_keys {
        for key in keys {
            if !unique_keys.contains(key) {
                unique_keys.push(key.clone());
            }
        }
    }
    let inner = slf.borrow().inner.clone();
    tokio::runtime::Handle::current().block_on(async {
        let mut client = take_client(&inner)?;
        let query_results = client
            .batch_get_query_results(&unique_keys)
            .await
            .map_err(to_py_err)?;
        let cache: HashMap<String, CachedQueryResultResponse> = unique_keys
            .into_iter()
            .zip(query_results.into_iter())
            .collect();
        let result = unsafe {
            client
                .get_into_ranges_with_query_cache(
                    &ptrs,
                    &all_keys,
                    &all_dst_offsets,
                    &all_src_offsets,
                    &all_sizes,
                    &cache,
                )
                .await
        };
        *inner.lock() = Some(client);
        result.map_err(to_py_err)
    })
}
