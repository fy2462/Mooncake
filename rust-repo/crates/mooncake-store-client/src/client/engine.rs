use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use tokio::sync::oneshot;
use transfer_engine_ffi::{
    RegisteredMemory, RegisteredMemoryAccess, RegisteredSubmitOutcome, RegisteredTransferRequest,
    SegmentId, StableMemoryOwner, SubmittedRegisteredBatch, TransferEngine, TransferEngineError,
    TransferEngineResult,
};

use super::endpoint::{PortReservation, ResolvedClientEndpoint};
use super::reaper::{TransferCompletion, TransferReaper};

/// Caller-owned proof that a classic Transfer Engine was initialized for one
/// exact Store identity. Keeping the identity beside the native handle makes
/// it impossible for client setup to publish a different endpoint/protocol.
pub struct InitializedTransferEngine {
    pub(crate) inner: Arc<TransferEngine>,
    metadata_conn_string: String,
    local_hostname: String,
    protocol: String,
    _port_reservation: Option<PortReservation>,
    binding: Arc<ExclusiveEngineUse>,
}

#[derive(Default)]
pub(crate) struct ExclusiveEngineUse {
    state: AtomicU8,
}

const ENGINE_BINDING_AVAILABLE: u8 = 0;
const ENGINE_BINDING_ACTIVE: u8 = 1;
const ENGINE_BINDING_POISONED: u8 = 2;

impl ExclusiveEngineUse {
    pub(crate) fn acquire(
        self: &Arc<Self>,
    ) -> mooncake_store_core::error::StoreResult<ExclusiveEngineUseLease> {
        self.state
            .compare_exchange(
                ENGINE_BINDING_AVAILABLE,
                ENGINE_BINDING_ACTIVE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                mooncake_store_core::StoreError::InvalidParams(
                    "external Transfer Engine is already bound to another Store client".to_string(),
                )
            })?;
        Ok(ExclusiveEngineUseLease {
            binding: Arc::clone(self),
            reusable: false,
        })
    }
}

pub(crate) struct ExclusiveEngineUseLease {
    binding: Arc<ExclusiveEngineUse>,
    reusable: bool,
}

impl ExclusiveEngineUseLease {
    pub(crate) fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Drop for ExclusiveEngineUseLease {
    fn drop(&mut self) {
        self.binding.state.store(
            if self.reusable {
                ENGINE_BINDING_AVAILABLE
            } else {
                ENGINE_BINDING_POISONED
            },
            Ordering::Release,
        );
    }
}

pub(crate) struct InitializedTransferEngineLease {
    pub(crate) owner: Arc<InitializedTransferEngine>,
    binding: ExclusiveEngineUseLease,
}

impl InitializedTransferEngineLease {
    pub(crate) fn mark_reusable(&mut self) {
        self.binding.mark_reusable();
    }
}

impl InitializedTransferEngine {
    pub fn create(
        metadata_conn_string: &str,
        local_hostname: &str,
        protocol: &str,
        topology_matrix: Option<&str>,
    ) -> mooncake_store_core::error::StoreResult<Self> {
        let endpoint = ResolvedClientEndpoint::from_explicit(local_hostname)?;
        let engine = TransferEngine::create(
            metadata_conn_string,
            &endpoint.server_name,
            &endpoint.host,
            u64::from(endpoint.port),
            false,
        )?;
        engine.install_transport(protocol, topology_matrix)?;
        Ok(Self {
            inner: Arc::new(engine),
            metadata_conn_string: metadata_conn_string.to_string(),
            local_hostname: endpoint.server_name,
            protocol: protocol.to_string(),
            _port_reservation: endpoint.reservation,
            binding: Arc::new(ExclusiveEngineUse::default()),
        })
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
    ) -> mooncake_store_core::error::StoreResult<InitializedTransferEngineLease> {
        let binding = self.binding.acquire()?;
        Ok(InitializedTransferEngineLease {
            owner: Arc::clone(self),
            binding,
        })
    }

    pub(crate) fn validate_identity(
        &self,
        metadata_conn_string: &str,
        local_hostname: &str,
        protocol: &str,
    ) -> mooncake_store_core::error::StoreResult<()> {
        use mooncake_store_core::StoreError;

        if self.metadata_conn_string != metadata_conn_string {
            return Err(StoreError::InvalidParams(
                "external Transfer Engine metadata connection does not match client setup"
                    .to_string(),
            ));
        }
        if self.local_hostname != local_hostname {
            return Err(StoreError::InvalidParams(format!(
                "external Transfer Engine local endpoint {:?} does not match client endpoint {:?}",
                self.local_hostname, local_hostname
            )));
        }
        if self.protocol != protocol {
            return Err(StoreError::InvalidParams(format!(
                "external Transfer Engine protocol {:?} does not match client protocol {:?}",
                self.protocol, protocol
            )));
        }
        Ok(())
    }
}

/// Optional data plane owned by a Store client. `rpc_only` clients keep this
/// disabled so metadata/control-plane APIs do not initialize native TE state.
pub(crate) struct ClientTransferEngine {
    inner: Option<Arc<TransferEngine>>,
    _external_lease: Option<InitializedTransferEngineLease>,
    reaper: Option<Arc<TransferReaper>>,
}

impl ClientTransferEngine {
    pub(crate) fn enabled(engine: Arc<TransferEngine>) -> Self {
        Self {
            inner: Some(engine),
            _external_lease: None,
            reaper: Some(Arc::new(TransferReaper::new())),
        }
    }

    pub(crate) fn enabled_external(engine: InitializedTransferEngineLease) -> Self {
        Self {
            inner: Some(Arc::clone(&engine.owner.inner)),
            _external_lease: Some(engine),
            reaper: Some(Arc::new(TransferReaper::new())),
        }
    }

    pub(crate) fn disabled() -> Self {
        Self {
            inner: None,
            _external_lease: None,
            reaper: None,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) fn mark_external_engine_reusable(&mut self) {
        if let Some(lease) = self._external_lease.as_mut() {
            lease.mark_reusable();
        }
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
