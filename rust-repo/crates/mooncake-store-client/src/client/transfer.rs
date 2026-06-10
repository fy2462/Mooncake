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
//     resource lifecycle (open_segment → allocate_batch_id → submit →
//     poll 10s → free_batch_id → close_segment)
//   - zero_copy_read / zero_copy_write: same TE pattern but with
//     caller-provided (pre-registered) buffers instead of local_buffer
//
// C++ equivalent: real_client.cpp (SelectBestReplica, WriteToReplica,
// ReadFromReplica, local memcpy paths)
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use transfer_engine_ffi::{BatchId, TransferStatusEnum};

use super::MooncakeClient;

impl MooncakeClient {
    pub(crate) async fn wait_for_transfer_batch(
        &self,
        batch_id: BatchId,
        task_count: usize,
        timeout: tokio::time::Duration,
    ) -> StoreResult<Vec<transfer_engine_ffi::TransferStatus>> {
        let statuses = self
            .wait_for_transfer_batch_terminal(batch_id, task_count, timeout)
            .await?;
        if statuses
            .iter()
            .all(|status| status.status == TransferStatusEnum::Completed)
        {
            return Ok(statuses);
        }
        Err(StoreError::OperationFailed(-1))
    }

    pub(crate) async fn wait_for_transfer_batch_terminal(
        &self,
        batch_id: BatchId,
        task_count: usize,
        timeout: tokio::time::Duration,
    ) -> StoreResult<Vec<transfer_engine_ffi::TransferStatus>> {
        let start = tokio::time::Instant::now();
        let mut statuses = vec![
            transfer_engine_ffi::TransferStatus {
                status: TransferStatusEnum::Waiting,
                transferred_bytes: 0,
            };
            task_count
        ];
        let mut terminal = vec![false; task_count];

        loop {
            let mut all_terminal = true;
            for task_id in 0..task_count {
                if terminal[task_id] {
                    continue;
                }
                let status = self.engine.get_transfer_status(batch_id, task_id)?;
                statuses[task_id] = status.clone();
                if status.status.is_terminal() {
                    terminal[task_id] = true;
                } else {
                    all_terminal = false;
                }
            }
            if all_terminal {
                return Ok(statuses);
            }
            if start.elapsed() > timeout {
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }
    }
}
