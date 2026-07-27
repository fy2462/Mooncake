// ============================================================================
// TransferEngine integration: replica selection, data transfer, buffer
// registration. This module implements the data-plane logic shared by
// read and write paths.
//
// TransferEngine 集成：副本选择、数据传输、缓冲区注册。
// 此模块实现了读写路径共享的数据面逻辑。
//
// Key patterns (关键模式):
//   - local_memcpy: fast path for same-node transfers (bypasses TE)
//   - write_to_replica / read_from_replica: TE transfer with per-replica
//     resource lifecycle (open_segment → typed registered submit →
//     poll through native quiescence/free → close_segment)
//   - zero_copy_read / zero_copy_write: same TE pattern but with
//     caller-provided (pre-registered) buffers instead of local_buffer
//
// C++ equivalent: real_client.cpp (SelectBestReplica, WriteToReplica,
// ReadFromReplica, local memcpy paths)
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use transfer_engine_ffi::{SegmentId, SubmittedRegisteredBatch, TransferStatusEnum};

use super::MooncakeClient;
use super::reaper::TransferCompletion;

pub(super) fn transfer_statuses_match_lengths(
    statuses: &[transfer_engine_ffi::TransferStatus],
    lengths: impl IntoIterator<Item = u64>,
) -> bool {
    let mut lengths = lengths.into_iter();
    for status in statuses {
        let Some(expected) = lengths.next() else {
            return false;
        };
        if status.status != TransferStatusEnum::Completed || status.transferred_bytes != expected {
            return false;
        }
    }
    lengths.next().is_none()
}

impl MooncakeClient {
    pub(crate) fn checked_replica_target_offset(
        replica: &ReplicaDescriptor,
        relative_offset: usize,
    ) -> StoreResult<u64> {
        let relative_offset = u64::try_from(relative_offset).map_err(|_| {
            StoreError::InvalidParams(
                "local transfer offset exceeds the remote address space".to_string(),
            )
        })?;
        replica
            .base_addr
            .checked_add(replica.offset)
            .and_then(|offset| offset.checked_add(relative_offset))
            .ok_or_else(|| {
                StoreError::InvalidParams(
                    "replica transfer target offset overflows u64".to_string(),
                )
            })
    }

    /// Close a segment when typed request construction fails before a native
    /// batch exists. Once submission returns a batch, ownership moves to the
    /// reaper instead.
    pub(crate) fn close_segment_on_prepare_error<T>(
        &self,
        segment_id: SegmentId,
        result: StoreResult<T>,
    ) -> StoreResult<T> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if let Err(close_error) = self.engine.close_segment(segment_id) {
                    tracing::error!(
                        %close_error,
                        segment_id = segment_id.0,
                        "failed to close segment after typed request preparation error"
                    );
                }
                Err(error)
            }
        }
    }

    /// Move every resource reachable by a submitted native request into the
    /// reaper before the first cancellation point. Dropping the returned
    /// future only drops the oneshot receiver; the worker still owns the
    /// batch, segment and payload until native quiescence.
    pub(crate) async fn wait_for_transfer_batch_owned<P>(
        &self,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: std::time::Duration,
        payload: P,
    ) -> StoreResult<TransferCompletion<P>>
    where
        P: Send + 'static,
    {
        let completion = self
            .engine
            .reap_submitted_batch(batch, segment_id, warn_after, payload)?;
        let completion = completion.await.map_err(|_| {
            StoreError::Internal(
                "transfer reaper terminated without returning native resources".to_string(),
            )
        })??;
        if completion
            .statuses
            .iter()
            .all(|status| status.status == TransferStatusEnum::Completed)
        {
            return Ok(completion);
        }
        Err(StoreError::OperationFailed(-1))
    }

    pub(crate) async fn wait_for_transfer_batch_terminal_owned<P>(
        &self,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: std::time::Duration,
        payload: P,
    ) -> StoreResult<TransferCompletion<P>>
    where
        P: Send + 'static,
    {
        let completion = self
            .engine
            .reap_submitted_batch(batch, segment_id, warn_after, payload)?;
        completion
            .await
            .map_err(|_| {
                StoreError::Internal(
                    "transfer reaper terminated without returning native resources".to_string(),
                )
            })?
            .map_err(Into::into)
    }

    /// Failed submission can still leave accepted native slices. Ownership is
    /// transferred synchronously, then the caller may await quiescence without
    /// making the raw payload cancellation-sensitive.
    pub(crate) async fn release_failed_submission_owned<P>(
        &self,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: std::time::Duration,
        payload: P,
    ) -> StoreResult<TransferCompletion<P>>
    where
        P: Send + 'static,
    {
        let completion = self
            .engine
            .reap_failed_batch(batch, segment_id, warn_after, payload)?;
        completion
            .await
            .map_err(|_| {
                StoreError::Internal(
                    "transfer reaper terminated while releasing a failed submission".to_string(),
                )
            })?
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::MooncakeClient;
    use super::transfer_statuses_match_lengths;
    use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
    use transfer_engine_ffi::{TransferStatus, TransferStatusEnum};
    use uuid::Uuid;

    #[test]
    fn exact_transfer_length_is_required() {
        let completed = [TransferStatus {
            status: TransferStatusEnum::Completed,
            transferred_bytes: 7,
        }];

        assert!(transfer_statuses_match_lengths(&completed, [7]));
        assert!(!transfer_statuses_match_lengths(&completed, [6]));
        assert!(!transfer_statuses_match_lengths(&completed, [7, 0]));
    }

    #[test]
    fn replica_target_offset_fails_closed_on_address_overflow() {
        let replica = ReplicaDescriptor {
            replica_type: ReplicaType::Memory,
            status: ReplicaStatus::Complete,
            segment_name: "remote".to_string(),
            segment_id: Uuid::new_v4(),
            base_addr: u64::MAX - 3,
            offset: 2,
            size: 1,
            holder_client_id: None,
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            protocol: "tcp".to_string(),
        };
        assert!(MooncakeClient::checked_replica_target_offset(&replica, 1).is_ok());
        assert!(MooncakeClient::checked_replica_target_offset(&replica, 2).is_err());
    }
}
