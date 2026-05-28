use mooncake_store_core::{ReplicateConfig, StoreError};
use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Put
    // -----------------------------------------------------------------------

    pub async fn put(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        if key.is_empty() || value.is_empty() {
            return Err(StoreError::InvalidParams(
                "key is empty or value has zero length".to_string(),
            ));
        }
        let cfg = config.unwrap_or_default();

        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: value.len() as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self
            .master
            .put_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();

        let replicas = self.replicas_from_proto(&response.replicas);
        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        for replica in &replicas {
            if let Err(e) = self.write_to_replica(replica, value).await {
                // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                let revoke_req = proto::PutRevokeRequest {
                    client_id: Some(self.client_id_proto()),
                    key: key.to_string(),
                    replica_type: 0,
                };
                let _ = self.master.put_revoke(revoke_req).await;
                return Err(e);
            }
        }

        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master
            .put_end(end_request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    pub async unsafe fn put_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();

        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: size as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self.master.put_start(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        for replica in &replicas {
            if let Err(e) = self.zero_copy_write(replica, buffer, size).await {
                // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                let revoke_req = proto::PutRevokeRequest {
                    client_id: Some(self.client_id_proto()),
                    key: key.to_string(),
                    replica_type: 0,
                };
                let _ = self.master.put_revoke(revoke_req).await;
                return Err(e);
            }
        }

        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master.put_end(end_request).await.map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Put parts (split data across multiple writes)
    // -----------------------------------------------------------------------

    pub async fn put_parts(
        &mut self,
        key: &str,
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();
        let total_len: usize = values.iter().map(|v| v.len()).sum();

        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: total_len as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self.master.put_start(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        for replica in &replicas {
            let segment_id = self.engine.open_segment(&replica.segment_name)?;
            let batch_id = self.engine.allocate_batch_id(values.len())?;

            let requests: Vec<TransferRequest> = values
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    let src_offset = values[..i].iter().map(|v| v.len()).sum::<usize>();
                    let tgt_offset = replica.offset + src_offset as u64;
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            self.local_buffer[src_offset..].as_ptr() as *mut u8,
                            data.len(),
                        );
                    }
                    TransferRequest {
                        opcode: Opcode::Write,
                        source: unsafe { self.local_buffer.as_ptr().add(src_offset) as *mut c_void },
                        target_id: segment_id,
                        target_offset: tgt_offset,
                        length: data.len() as u64,
                    }
                })
                .collect();

            self.engine.submit_transfer(batch_id, &requests)?;

            for i in 0..values.len() {
                loop {
                    let status = self.engine.get_transfer_status(batch_id, i)?;
                    if status.status == TransferStatusEnum::Completed {
                        break;
                    }
                    if status.status == TransferStatusEnum::Failed {
                        // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                        let revoke_req = proto::PutRevokeRequest {
                            client_id: Some(self.client_id_proto()),
                            key: key.to_string(),
                            replica_type: 0,
                        };
                        let _ = self.master.put_revoke(revoke_req).await;
                        return Err(StoreError::OperationFailed(-1));
                    }
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                }
            }

            self.engine.free_batch_id(batch_id)?;
            self.engine.close_segment(segment_id)?;
        }

        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master.put_end(end_request).await.map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Batch Put
    // -----------------------------------------------------------------------

    pub async fn batch_put(
        &mut self,
        keys: &[String],
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        if keys.len() != values.len() {
            return Err(StoreError::InvalidParams(
                "keys and values length mismatch".to_string(),
            ));
        }
        let mut statuses = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.put(key, values[i], config.clone()).await {
                Ok(()) => statuses.push(0),
                Err(_) => statuses.push(-1),
            }
        }
        Ok(statuses)
    }

    pub async unsafe fn batch_put_from(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        let mut statuses = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.put_from(key, buffers[i], sizes[i], config.clone()).await {
                Ok(()) => statuses.push(0),
                Err(_) => statuses.push(-1),
            }
        }
        Ok(statuses)
    }
}
