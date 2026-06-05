use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest};

use super::MooncakeClient;

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
        let size = replica.size as i64;

        tracing::info!(
            target: "te_debug",
            peer_addr,
            replica_size = replica.size,
            "read_from_remote_local_disk: dispatching P2P offload read"
        );

        let result = crate::offload::client::batch_get_offload_objects(
            peer_addr,
            &[key.to_string()],
            &[size],
            &[tenant_id.to_string()],
        )
        .await
        .map_err(|e| StoreError::Internal(format!("P2P offload read: {e}")))?;

        let seg_id = self
            .engine
            .open_segment(&result.transfer_engine_addr)
            .map_err(|e| StoreError::Internal(format!("open peer segment: {e}")))?;
        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(seg_id);
                return Err(StoreError::Internal(format!("allocate batch: {e}")));
            }
        };

        let read_len = replica.size as usize;
        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: seg_id,
            target_offset: result.pointers[0],
            length: read_len as u64,
        };

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(seg_id);
            return Err(StoreError::Internal(format!(
                "submit offload transfer: {e}"
            )));
        }

        if let Err(e) = self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(seg_id);
            return Err(StoreError::Internal(format!("poll offload transfer: {e}")));
        }

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(seg_id);
            return Err(StoreError::Internal(format!("free offload batch: {e}")));
        }
        let _ = self.engine.close_segment(seg_id);

        let release_addr = peer_addr.to_string();
        tokio::spawn(async move {
            crate::offload::client::release_offload_buffer(&release_addr, result.batch_id).await;
        });

        Ok(self.local_buffer[..read_len].to_vec())
    }
}
