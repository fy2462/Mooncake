use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::collections::HashMap;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Storage management
    // -----------------------------------------------------------------------

    pub async fn mount_local_disk_segment(&mut self, enable_offloading: bool) -> StoreResult<()> {
        self.master
            .mount_local_disk_segment(proto::MountLocalDiskSegmentRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn offload_object_heartbeat(
        &mut self,
        enable_offloading: bool,
    ) -> StoreResult<HashMap<String, i64>> {
        let response = self
            .master
            .offload_object_heartbeat(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.objects)
    }

    pub async fn report_ssd_capacity(&mut self, bytes: i64) -> StoreResult<()> {
        self.master
            .report_ssd_capacity(proto::ReportSsdCapacityRequest {
                client_id: Some(self.client_id_proto()),
                ssd_total_capacity_bytes: bytes,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn notify_offload_success(
        &mut self,
        keys: Vec<String>,
        metadatas: Vec<proto::StorageObjectMetadata>,
    ) -> StoreResult<()> {
        self.master
            .notify_offload_success(proto::NotifyOffloadSuccessRequest {
                client_id: Some(self.client_id_proto()),
                keys,
                metadatas,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn promotion_object_heartbeat(&mut self) -> StoreResult<HashMap<String, i64>> {
        let response = self
            .master
            .promotion_object_heartbeat(proto::PromotionObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.objects)
    }

    pub async fn promotion_alloc_start(
        &mut self,
        key: &str,
        size: u64,
        preferred_segments: Vec<String>,
    ) -> StoreResult<ReplicaDescriptor> {
        let response = self
            .master
            .promotion_alloc_start(proto::PromotionAllocStartRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                size,
                preferred_segments,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let descriptor = response
            .memory_descriptor
            .as_ref()
            .ok_or(StoreError::OperationFailed(-1))?;
        Ok(self
            .replicas_from_proto(std::slice::from_ref(descriptor))
            .remove(0))
    }

    pub async fn notify_promotion_success(&mut self, key: &str) -> StoreResult<()> {
        self.master
            .notify_promotion_success(proto::NotifyPromotionSuccessRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn notify_promotion_failure(&mut self, key: &str) -> StoreResult<()> {
        self.master
            .notify_promotion_failure(proto::NotifyPromotionFailureRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }
}
