use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::time::{Duration, Instant};
use transfer_engine_ffi::{RegisteredSubmitOutcome, RegisteredTransferRequest};

use super::MooncakeClient;

/// Remote offload allocation whose release is ordered after local native
/// quiescence by moving this guard into the transfer-reaper payload.
struct RemoteOffloadBatchGuard {
    peer_addr: String,
    batch_id: u64,
    runtime: tokio::runtime::Handle,
}

impl RemoteOffloadBatchGuard {
    fn new(peer_addr: &str, batch_id: u64) -> Self {
        Self {
            peer_addr: peer_addr.to_string(),
            batch_id,
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl Drop for RemoteOffloadBatchGuard {
    fn drop(&mut self) {
        let peer_addr = self.peer_addr.clone();
        let batch_id = self.batch_id;
        self.runtime.spawn(async move {
            crate::offload::client::release_offload_buffer(&peer_addr, batch_id).await;
        });
    }
}

impl MooncakeClient {
    /// Read data from a remote LOCAL_DISK replica via P2P offload RPC.
    ///
    /// C++ equivalent: `RealClient::batch_get_into_offload_object_internal`
    pub(crate) async fn read_from_remote_local_disk(
        &self,
        key: &str,
        tenant_id: &str,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        let peer_addr = &replica.segment_name;
        let size = i64::try_from(replica.size).map_err(|_| {
            StoreError::InvalidParams(format!(
                "remote LocalDisk replica size {} exceeds the supported RPC range",
                replica.size
            ))
        })?;
        let read_len = usize::try_from(replica.size).map_err(|_| {
            StoreError::InvalidParams(format!(
                "remote LocalDisk replica size {} exceeds the local address space",
                replica.size
            ))
        })?;
        if read_len > self.local_buffer.len() {
            return Err(StoreError::InvalidParams(format!(
                "remote LocalDisk replica size {} exceeds local buffer size {}",
                read_len,
                self.local_buffer.len()
            )));
        }

        tracing::info!(
            target: "te_debug",
            peer_addr,
            replica_size = replica.size,
            "read_from_remote_local_disk: dispatching P2P offload read"
        );

        self.local_buffer.wait_until_available().await;
        let staging_lease = self.local_buffer.lease()?;
        let started_at = Instant::now();
        let result = crate::offload::client::batch_get_offload_objects(
            peer_addr,
            &[key.to_string()],
            &[size],
            &[tenant_id.to_string()],
        )
        .await
        .map_err(|e| StoreError::Internal(format!("P2P offload read: {e}")))?;
        let remote_batch = RemoteOffloadBatchGuard::new(peer_addr, result.batch_id);

        let target_offset = match result.pointers.as_slice() {
            [pointer] => *pointer,
            pointers => {
                return Err(StoreError::Internal(format!(
                    "P2P offload read returned {} pointers for one object",
                    pointers.len()
                )));
            }
        };
        let seg_id = match self.engine.open_segment(&result.transfer_engine_addr) {
            Ok(seg_id) => seg_id,
            Err(e) => {
                return Err(StoreError::Internal(format!("open peer segment: {e}")));
            }
        };
        let request = self.close_segment_on_prepare_error(
            seg_id,
            (|| -> StoreResult<_> {
                Ok(RegisteredTransferRequest::read(
                    self.local_buffer
                        .writable_region(&staging_lease, 0, read_len)?,
                    seg_id,
                    target_offset,
                    read_len,
                )?)
            })(),
        )?;
        let outcome = match self.engine.submit_transfer(vec![request]) {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = self.engine.close_segment(seg_id);
                return Err(StoreError::Internal(format!(
                    "prepare offload transfer: {error}"
                )));
            }
        };
        let batch = match outcome {
            RegisteredSubmitOutcome::Submitted(batch) => batch,
            RegisteredSubmitOutcome::NativeRejected { error, batch } => {
                let _completion = self
                    .release_failed_submission_owned(
                        batch,
                        seg_id,
                        std::time::Duration::from_secs(10),
                        (staging_lease, remote_batch),
                    )
                    .await?;
                return Err(StoreError::Internal(format!(
                    "submit offload transfer: {error}"
                )));
            }
        };

        let completion = self
            .wait_for_transfer_batch_owned(
                batch,
                seg_id,
                std::time::Duration::from_secs(10),
                (staging_lease, remote_batch),
            )
            .await
            .map_err(|e| StoreError::Internal(format!("poll offload transfer: {e}")))?;
        if !super::transfer::transfer_statuses_match_lengths(
            &completion.statuses,
            [read_len as u64],
        ) {
            return Err(StoreError::Internal(format!(
                "offload read did not complete exactly {read_len} bytes"
            )));
        }
        let elapsed = started_at.elapsed();
        let data = self
            .local_buffer
            .copy_to_vec(&completion.payload.0, 0, read_len)?;
        drop(completion);
        if elapsed >= Duration::from_millis(result.gc_ttl_ms) {
            return Err(StoreError::OperationFailed(-706));
        }
        Ok(data)
    }
}
