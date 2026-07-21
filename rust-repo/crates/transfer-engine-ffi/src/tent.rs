use crate::ffi;
use crate::{
    BatchId, Opcode, TransferEngineError, TransferEngineResult, TransferRequest, TransferStatus,
    TransferStatusEnum,
};
use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::ptr::NonNull;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum TentIntent {
    #[default]
    Unspecified = ffi::TENT_INTENT_UNSPEC as i32,
    ForegroundGet = ffi::TENT_INTENT_FOREGROUND_GET as i32,
    BackgroundPrefetch = ffi::TENT_INTENT_BACKGROUND_PREFETCH as i32,
    Migration = ffi::TENT_INTENT_MIGRATION as i32,
    Checkpoint = ffi::TENT_INTENT_CHECKPOINT as i32,
    WeightLoading = ffi::TENT_INTENT_WEIGHT_LOADING as i32,
    StagingInternal = ffi::TENT_INTENT_STAGING_INTERNAL as i32,
}

impl TryFrom<i32> for TentIntent {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Unspecified as i32 => Ok(Self::Unspecified),
            value if value == Self::ForegroundGet as i32 => Ok(Self::ForegroundGet),
            value if value == Self::BackgroundPrefetch as i32 => Ok(Self::BackgroundPrefetch),
            value if value == Self::Migration as i32 => Ok(Self::Migration),
            value if value == Self::Checkpoint as i32 => Ok(Self::Checkpoint),
            value if value == Self::WeightLoading as i32 => Ok(Self::WeightLoading),
            value if value == Self::StagingInternal as i32 => Ok(Self::StagingInternal),
            _ => Err(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum TentTransport {
    #[default]
    Unspecified = ffi::TRANSPORT_UNSPEC as i32,
    Rdma = ffi::TRANSPORT_RDMA as i32,
    Mnnvl = ffi::TRANSPORT_MNNVL as i32,
    Shm = ffi::TRANSPORT_SHM as i32,
    Nvlink = ffi::TRANSPORT_NVLINK as i32,
    Gds = ffi::TRANSPORT_GDS as i32,
    IoUring = ffi::TRANSPORT_IOURING as i32,
    Tcp = ffi::TRANSPORT_TCP as i32,
    AscendDirect = ffi::TRANSPORT_ASCEND_DIRECT as i32,
    SunriseLink = ffi::TRANSPORT_SUNRISE_LINK as i32,
}

impl TryFrom<i32> for TentTransport {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::Rdma),
            2 => Ok(Self::Mnnvl),
            3 => Ok(Self::Shm),
            4 => Ok(Self::Nvlink),
            5 => Ok(Self::Gds),
            6 => Ok(Self::IoUring),
            7 => Ok(Self::Tcp),
            8 => Ok(Self::AscendDirect),
            9 => Ok(Self::SunriseLink),
            _ => Err(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum TentPriority {
    #[default]
    High = 0,
    Medium = 1,
    Low = 2,
}

impl TryFrom<i32> for TentPriority {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::High),
            1 => Ok(Self::Medium),
            2 => Ok(Self::Low),
            _ => Err(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TentRequestOptions {
    pub priority: TentPriority,
    pub transport: TentTransport,
    pub policy_name: Option<String>,
    pub deadline_ns: u64,
    pub intent: TentIntent,
}

impl Default for TentRequestOptions {
    fn default() -> Self {
        Self {
            priority: TentPriority::High,
            transport: TentTransport::Unspecified,
            policy_name: None,
            deadline_ns: 0,
            intent: TentIntent::Unspecified,
        }
    }
}

impl TentRequestOptions {
    pub fn is_legacy_compatible(&self) -> bool {
        self.policy_name.is_none()
            && self.deadline_ns == 0
            && self.intent == TentIntent::Unspecified
    }
}

#[derive(Debug, Clone)]
pub struct TentTransferRequest {
    pub opcode: Opcode,
    pub source: *mut c_void,
    pub target_id: u64,
    pub target_offset: u64,
    pub length: u64,
    pub options: TentRequestOptions,
}

impl TentTransferRequest {
    pub fn new(request: TransferRequest, options: TentRequestOptions) -> Self {
        Self {
            opcode: request.opcode,
            source: request.source,
            target_id: request.target_id.0 as u64,
            target_offset: request.target_offset,
            length: request.length,
            options,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_raw_parts(
        opcode: Opcode,
        source: *mut c_void,
        target_id: u64,
        target_offset: u64,
        length: u64,
        options: TentRequestOptions,
    ) -> Self {
        Self {
            opcode,
            source,
            target_id,
            target_offset,
            length,
            options,
        }
    }
}

// SAFETY: requests are descriptors. The caller must keep source memory valid
// and registered until the submitted transfer reaches a terminal state.
unsafe impl Send for TentTransferRequest {}
unsafe impl Sync for TentTransferRequest {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TentMetricsStatus {
    pub tent_available: bool,
    pub metrics_enabled: bool,
    pub metrics_initialized: bool,
    pub http_port: Option<u16>,
}

impl TentMetricsStatus {
    pub fn is_log_only(&self) -> bool {
        self.metrics_enabled && self.http_port.is_none()
    }
}

pub(crate) struct RawTentRequests {
    #[allow(dead_code)]
    policy_names: Vec<Option<CString>>,
    pub(crate) requests: Vec<ffi::tent_request_v2_t>,
}

pub(crate) fn build_raw_requests(
    requests: &[TentTransferRequest],
) -> TransferEngineResult<RawTentRequests> {
    let policy_names = requests
        .iter()
        .map(|request| {
            request
                .options
                .policy_name
                .as_deref()
                .map(CString::new)
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let raw_requests = requests
        .iter()
        .zip(&policy_names)
        .map(|(request, policy_name)| ffi::tent_request_v2_t {
            struct_size: size_of::<ffi::tent_request_v2_t>() as u32,
            version: ffi::TENT_REQUEST_V2_VERSION,
            opcode: request.opcode as i32,
            source: request.source,
            target_id: request.target_id,
            target_offset: request.target_offset,
            length: request.length,
            priority: request.options.priority as i32,
            transport_hint: request.options.transport as i32,
            policy_name: policy_name
                .as_ref()
                .map_or(std::ptr::null(), |name| name.as_ptr()),
            deadline_ns: request.options.deadline_ns,
            intent_type: request.options.intent as i32,
        })
        .collect();
    Ok(RawTentRequests {
        policy_names,
        requests: raw_requests,
    })
}

fn build_legacy_requests(requests: &[TentTransferRequest]) -> Vec<ffi::tent_request_t> {
    requests
        .iter()
        .map(|request| ffi::tent_request_t {
            opcode: request.opcode as i32,
            source: request.source,
            target_id: request.target_id,
            target_offset: request.target_offset,
            length: request.length,
            priority: request.options.priority as i32,
            transport_hint: request.options.transport as i32,
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmissionAbi {
    Legacy,
    V2,
}

fn submission_abi(requests: &[TentTransferRequest]) -> SubmissionAbi {
    if requests
        .iter()
        .all(|request| request.options.is_legacy_compatible())
    {
        SubmissionAbi::Legacy
    } else {
        SubmissionAbi::V2
    }
}

fn check_native_result(rc: i32) -> TransferEngineResult<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(TransferEngineError::OperationFailed(rc))
    }
}

/// Owning safe wrapper for the repository's existing TENT C API.
#[derive(Debug)]
pub struct TentEngine {
    handle: NonNull<c_void>,
}

// SAFETY: TENT owns synchronization for its opaque engine handle. Rust never
// dereferences the handle and all access goes through the C API.
unsafe impl Send for TentEngine {}
unsafe impl Sync for TentEngine {}

impl TentEngine {
    pub fn create(
        config_path: Option<&str>,
        overrides: &[(&str, &str)],
    ) -> TransferEngineResult<Self> {
        // Validate every borrowed string before mutating TENT's thread-local
        // configuration or entering any native function.
        let config_path = config_path.map(CString::new).transpose()?;
        let overrides = overrides
            .iter()
            .map(|(key, value)| Ok((CString::new(*key)?, CString::new(*value)?)))
            .collect::<TransferEngineResult<Vec<_>>>()?;
        if let Some(path) = &config_path {
            unsafe { ffi::tent_load_config_from_file(path.as_ptr()) };
        }
        for (key, value) in &overrides {
            unsafe { ffi::tent_set_config(key.as_ptr(), value.as_ptr()) };
        }
        let handle = NonNull::new(unsafe { ffi::tent_create_engine() })
            .ok_or(TransferEngineError::NullHandle)?;
        Ok(Self { handle })
    }

    pub fn available(&self) -> bool {
        unsafe { ffi::tent_available(self.handle.as_ptr()) != 0 }
    }

    pub fn open_segment(&self, segment_name: &str) -> TransferEngineResult<u64> {
        let name = CString::new(segment_name)?;
        let mut handle = 0;
        let rc =
            unsafe { ffi::tent_open_segment(self.handle.as_ptr(), &mut handle, name.as_ptr()) };
        if rc != 0 {
            return Err(TransferEngineError::OperationFailed(rc));
        }
        Ok(handle)
    }

    pub fn close_segment(&self, segment_id: u64) -> TransferEngineResult<()> {
        let rc = unsafe { ffi::tent_close_segment(self.handle.as_ptr(), segment_id) };
        check_native_result(rc)
    }

    /// Register memory owned by a foreign runtime such as CPython/PyTorch.
    /// Raw-address unsafety is contained in this FFI crate; the foreign caller
    /// is responsible for keeping the allocation alive until unregistration.
    pub fn register_foreign_memory(&self, address: usize, size: usize) -> TransferEngineResult<()> {
        if address == 0 || size == 0 {
            return Err(TransferEngineError::NullPointer);
        }
        let rc = unsafe {
            ffi::tent_register_memory(self.handle.as_ptr(), address as *mut c_void, size)
        };
        check_native_result(rc)
    }

    pub fn unregister_foreign_memory(
        &self,
        address: usize,
        size: usize,
    ) -> TransferEngineResult<()> {
        if address == 0 {
            return Err(TransferEngineError::NullPointer);
        }
        let rc = unsafe {
            ffi::tent_unregister_memory(self.handle.as_ptr(), address as *mut c_void, size)
        };
        check_native_result(rc)
    }

    pub fn allocate_batch(&self, batch_size: usize) -> TransferEngineResult<BatchId> {
        let id = unsafe { ffi::tent_allocate_batch(self.handle.as_ptr(), batch_size) };
        if id == u64::MAX {
            return Err(TransferEngineError::OperationFailed(-1));
        }
        Ok(BatchId(id))
    }

    /// Submit through the legacy TENT ABI when the request uses only fields
    /// supported by `tent_request_t`; otherwise use `tent_submit_v2`.
    pub fn submit_transfer(
        &self,
        batch_id: BatchId,
        requests: &[TentTransferRequest],
    ) -> TransferEngineResult<()> {
        let rc = if submission_abi(requests) == SubmissionAbi::Legacy {
            let mut raw = build_legacy_requests(requests);
            unsafe {
                ffi::tent_submit(
                    self.handle.as_ptr(),
                    batch_id.0,
                    raw.as_mut_ptr(),
                    raw.len(),
                )
            }
        } else {
            let raw = build_raw_requests(requests)?;
            unsafe {
                ffi::tent_submit_v2(
                    self.handle.as_ptr(),
                    batch_id.0,
                    raw.requests.as_ptr(),
                    raw.requests.len(),
                )
            }
        };
        check_native_result(rc)
    }

    pub fn cancel_transfer(&self, batch_id: BatchId, task_id: usize) -> TransferEngineResult<()> {
        let rc = unsafe { ffi::tent_cancel_task(self.handle.as_ptr(), batch_id.0, task_id) };
        check_native_result(rc)
    }

    pub fn transfer_status(
        &self,
        batch_id: BatchId,
        task_id: usize,
    ) -> TransferEngineResult<TransferStatus> {
        let mut raw = ffi::tent_status_t {
            status: 0,
            transferred_bytes: 0,
        };
        let rc =
            unsafe { ffi::tent_task_status(self.handle.as_ptr(), batch_id.0, task_id, &mut raw) };
        if rc != 0 {
            return Err(TransferEngineError::OperationFailed(rc));
        }
        Ok(TransferStatus {
            status: TransferStatusEnum::from_i32(raw.status),
            transferred_bytes: raw.transferred_bytes,
        })
    }

    pub fn free_batch(&self, batch_id: BatchId) -> TransferEngineResult<()> {
        let rc = unsafe { ffi::tent_free_batch(self.handle.as_ptr(), batch_id.0) };
        check_native_result(rc)
    }

    pub fn metrics_status(&self) -> TransferEngineResult<TentMetricsStatus> {
        let mut raw = ffi::tent_metrics_status_v1_t {
            struct_size: size_of::<ffi::tent_metrics_status_v1_t>() as u32,
            metrics_enabled: 0,
            metrics_initialized: 0,
            http_port: 0,
        };
        let rc = unsafe { ffi::tent_metrics_status(self.handle.as_ptr(), &mut raw) };
        if rc != 0 {
            return Err(TransferEngineError::OperationFailed(rc));
        }
        Ok(TentMetricsStatus {
            tent_available: self.available(),
            metrics_enabled: raw.metrics_enabled != 0,
            metrics_initialized: raw.metrics_initialized != 0,
            http_port: (raw.http_port != 0).then_some(raw.http_port),
        })
    }
}

impl Drop for TentEngine {
    fn drop(&mut self) {
        unsafe { ffi::tent_destroy_engine(self.handle.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SubmissionAbi, TentIntent, TentMetricsStatus, TentPriority, TentRequestOptions,
        TentTransferRequest, build_raw_requests, check_native_result, submission_abi,
    };
    use crate::{Opcode, SegmentId, TransferRequest};
    use std::ffi::c_void;
    use std::mem::{align_of, size_of};

    fn request() -> TransferRequest {
        TransferRequest {
            opcode: Opcode::Read,
            source: 0x1000usize as *mut c_void,
            target_id: SegmentId(7),
            target_offset: 64,
            length: 4096,
        }
    }

    #[test]
    fn legacy_request_abi_layout_stays_unchanged() {
        assert_eq!(size_of::<crate::ffi::transfer_request_t>(), 40);
        assert_eq!(align_of::<crate::ffi::transfer_request_t>(), 8);
        assert_eq!(size_of::<crate::ffi::tent_request_t>(), 48);
        assert_eq!(align_of::<crate::ffi::tent_request_t>(), 8);
        assert_eq!(size_of::<crate::ffi::tent_request_v2_t>(), 80);
        assert_eq!(align_of::<crate::ffi::tent_request_v2_t>(), 8);
    }

    #[test]
    fn intent_values_round_trip_through_the_v2_abi() {
        for intent in [
            TentIntent::Unspecified,
            TentIntent::ForegroundGet,
            TentIntent::BackgroundPrefetch,
            TentIntent::Migration,
            TentIntent::Checkpoint,
            TentIntent::WeightLoading,
            TentIntent::StagingInternal,
        ] {
            assert_eq!(TentIntent::try_from(intent as i32).unwrap(), intent);
        }
        assert!(TentIntent::try_from(7).is_err());
    }

    #[test]
    fn default_options_preserve_legacy_submission_semantics() {
        let options = TentRequestOptions::default();
        assert!(options.is_legacy_compatible());
        assert_eq!(options.priority, super::TentPriority::High);
        assert_eq!(options.deadline_ns, 0);
        assert_eq!(options.intent, TentIntent::Unspecified);
        assert!(options.policy_name.is_none());
    }

    #[test]
    fn priority_and_transport_use_legacy_abi_but_v2_fields_do_not() {
        let priority_request = TentTransferRequest::new(
            request(),
            TentRequestOptions {
                priority: TentPriority::Low,
                ..Default::default()
            },
        );
        assert_eq!(submission_abi(&[priority_request]), SubmissionAbi::Legacy);

        let deadline_request = TentTransferRequest::new(
            request(),
            TentRequestOptions {
                deadline_ns: 1,
                ..Default::default()
            },
        );
        assert_eq!(submission_abi(&[deadline_request]), SubmissionAbi::V2);
    }

    #[test]
    fn raw_requests_keep_policy_names_alive_for_the_native_call() {
        let requests = vec![TentTransferRequest::new(
            request(),
            TentRequestOptions {
                policy_name: Some("latency-sensitive".to_string()),
                deadline_ns: 123,
                intent: TentIntent::ForegroundGet,
                ..Default::default()
            },
        )];

        let raw = build_raw_requests(&requests).unwrap();
        assert_eq!(raw.requests[0].deadline_ns, 123);
        assert_eq!(raw.requests[0].intent_type, 1);
        let policy = unsafe { std::ffi::CStr::from_ptr(raw.requests[0].policy_name) };
        assert_eq!(policy.to_str().unwrap(), "latency-sensitive");
    }

    #[test]
    fn policy_name_with_interior_nul_is_rejected_before_ffi() {
        let requests = vec![TentTransferRequest::new(
            request(),
            TentRequestOptions {
                policy_name: Some("invalid\0policy".to_string()),
                ..Default::default()
            },
        )];
        assert!(build_raw_requests(&requests).is_err());
    }

    #[test]
    fn zero_metrics_port_means_http_is_unavailable() {
        let status = TentMetricsStatus {
            tent_available: true,
            metrics_enabled: true,
            metrics_initialized: true,
            http_port: None,
        };
        assert!(status.is_log_only());
    }

    #[test]
    fn cancellation_result_codes_map_without_losing_failures() {
        assert!(check_native_result(0).is_ok());
        assert!(matches!(
            check_native_result(-1),
            Err(crate::TransferEngineError::OperationFailed(-1))
        ));
    }
}
