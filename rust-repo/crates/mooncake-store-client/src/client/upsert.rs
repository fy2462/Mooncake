use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, ReplicateConfig, StoreError};
use std::ffi::c_void;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Upsert
    // -----------------------------------------------------------------------

    pub async fn upsert(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let cfg = config.unwrap_or_default();
        let request = proto::UpsertRequest {
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
            .upsert(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

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

        let end_request = proto::BatchUpsertEndRequest {
            entries: vec![proto::UpsertEntry {
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
            }],
        };
        self.master
            .batch_upsert_end(end_request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(replicas)
    }

    pub async unsafe fn upsert_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let cfg = config.unwrap_or_default();
        let request = proto::UpsertRequest {
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

        let response = self
            .master
            .upsert(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
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

        let end_request = proto::BatchUpsertEndRequest {
            entries: vec![proto::UpsertEntry {
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
            }],
        };
        self.master
            .batch_upsert_end(end_request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(replicas)
    }

    pub async unsafe fn batch_upsert_from(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<Vec<ReplicaDescriptor>>> {
        let mut results = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            results.push(
                self.upsert_from(key, buffers[i], sizes[i], config.clone())
                    .await?,
            );
        }
        Ok(results)
    }

    pub async fn upsert_parts(
        &mut self,
        key: &str,
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let total_len: usize = values.iter().map(|v| v.len()).sum();
        let mut concatenated = Vec::with_capacity(total_len);
        for v in values {
            concatenated.extend_from_slice(v);
        }
        self.upsert(key, &concatenated, config).await
    }
}
