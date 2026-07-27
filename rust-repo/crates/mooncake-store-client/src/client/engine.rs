use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use transfer_engine_ffi::{
    RegisteredMemory, RegisteredMemoryAccess, RegisteredSubmitOutcome, RegisteredTransferRequest,
    SegmentId, StableMemoryOwner, SubmittedRegisteredBatch, TransferEngine, TransferEngineError,
    TransferEngineResult,
};

use super::reaper::{TransferCompletion, TransferReaper};

/// Optional data plane owned by a Store client. `rpc_only` clients keep this
/// disabled so metadata/control-plane APIs do not initialize native TE state.
pub(crate) struct ClientTransferEngine {
    inner: Option<Arc<TransferEngine>>,
    reaper: Option<Arc<TransferReaper>>,
}

impl ClientTransferEngine {
    pub(crate) fn enabled(engine: Arc<TransferEngine>) -> Self {
        Self {
            inner: Some(engine),
            reaper: Some(Arc::new(TransferReaper::new())),
        }
    }

    pub(crate) fn disabled() -> Self {
        Self {
            inner: None,
            reaper: None,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    fn require(&self) -> TransferEngineResult<&TransferEngine> {
        self.inner
            .as_deref()
            .ok_or(TransferEngineError::NativeOperationFailed {
                operation: "rpc_only_data_plane",
                code: -1,
            })
    }

    pub(crate) fn required_arc(&self) -> TransferEngineResult<Arc<TransferEngine>> {
        self.inner
            .as_ref()
            .cloned()
            .ok_or(TransferEngineError::NativeOperationFailed {
                operation: "rpc_only_data_plane",
                code: -1,
            })
    }

    pub(crate) fn open_segment(&self, segment_name: &str) -> TransferEngineResult<SegmentId> {
        self.require()?.open_segment(segment_name)
    }

    pub(crate) fn get_local_ip_and_port(&self) -> TransferEngineResult<String> {
        self.require()?.get_local_ip_and_port()
    }

    pub(crate) fn close_segment(&self, segment_id: SegmentId) -> TransferEngineResult<()> {
        self.require()?.close_segment(segment_id)
    }

    pub(crate) fn remove_local_segment(&self, segment_name: &str) -> TransferEngineResult<()> {
        self.require()?.remove_local_segment(segment_name)
    }

    pub(crate) fn submit_transfer(
        &self,
        requests: Vec<RegisteredTransferRequest>,
    ) -> TransferEngineResult<RegisteredSubmitOutcome> {
        self.required_arc()?.submit_registered_transfer(requests)
    }

    fn require_reaper(&self) -> TransferEngineResult<&TransferReaper> {
        self.reaper
            .as_deref()
            .ok_or(TransferEngineError::NativeOperationFailed {
                operation: "rpc_only_transfer_reaper",
                code: -1,
            })
    }

    pub(crate) fn reap_submitted_batch<P>(
        &self,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: Duration,
        payload: P,
    ) -> TransferEngineResult<oneshot::Receiver<TransferEngineResult<TransferCompletion<P>>>>
    where
        P: Send + 'static,
    {
        let engine = match (self.required_arc(), self.require_reaper()) {
            (Ok(engine), Ok(reaper)) => (engine, reaper),
            (Err(error), _) | (_, Err(error)) => {
                // Submission has already succeeded at this call boundary.
                // If the adapter invariant is broken, leaking is safer than
                // releasing memory that native code may still access.
                std::mem::forget(batch);
                std::mem::forget(payload);
                return Err(error);
            }
        };
        Ok(engine
            .1
            .submit_batch(engine.0, batch, segment_id, warn_after, payload))
    }

    pub(crate) fn reap_failed_batch<P>(
        &self,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: Duration,
        payload: P,
    ) -> TransferEngineResult<oneshot::Receiver<TransferEngineResult<TransferCompletion<P>>>>
    where
        P: Send + 'static,
    {
        let engine = match (self.required_arc(), self.require_reaper()) {
            (Ok(engine), Ok(reaper)) => (engine, reaper),
            (Err(error), _) | (_, Err(error)) => {
                // A failed submission may still have admitted native slices.
                // Preserve payload lifetime even if the adapter invariant is
                // unexpectedly unavailable.
                std::mem::forget(batch);
                std::mem::forget(payload);
                return Err(error);
            }
        };
        Ok(engine
            .1
            .submit_failed_batch(engine.0, batch, segment_id, warn_after, payload))
    }

    pub(crate) fn register_owned_memory<O>(
        &self,
        owner: O,
        location: &str,
        remote_accessible: bool,
        access: RegisteredMemoryAccess,
    ) -> TransferEngineResult<RegisteredMemory>
    where
        O: StableMemoryOwner,
    {
        self.required_arc()?
            .register_owned_memory(owner, location, remote_accessible, access)
    }

    pub(crate) fn unregister_owned_memory(
        &self,
        registration: &mut RegisteredMemory,
    ) -> TransferEngineResult<()> {
        self.require()?.unregister_owned_memory(registration)
    }
}
