// ============================================================================
// Write operations: put, put_from, put_parts, batch_put
// 写操作：put、put_from、put_parts、batch_put
//
// C++ equivalent: real_client.cpp Put() / PutFrom() / PutParts() / BatchPut()
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig, StoreError};
use std::collections::HashMap;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest};

use super::{
    MooncakeClient,
    finalize::{ReplicaFinalizeDecision, ReplicaTransferSummary, determine_finalize_decision},
};
use crate::proto;

pub(super) const BATCH_STATUS_OBJECT_ALREADY_EXISTS: i32 = -7;

impl MooncakeClient {
    pub(super) async fn put_end_for_type(
        &mut self,
        key: &str,
        replica_type: ReplicaType,
        tenant_id: &str,
    ) -> StoreResult<()> {
        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: replica_type.into(),
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .put_end(self.rpc_request(end_request))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    pub(super) async fn put_revoke_for_type(
        &mut self,
        key: &str,
        replica_type: ReplicaType,
        tenant_id: &str,
    ) -> StoreResult<()> {
        let revoke_req = proto::PutRevokeRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: replica_type.into(),
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .put_revoke(self.rpc_request(revoke_req))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    pub(super) async fn finalize_put_for_key(
        &mut self,
        key: &str,
        decision: ReplicaFinalizeDecision,
        tenant_id: &str,
    ) -> StoreResult<()> {
        if let Some(replica_type) = decision.end_type {
            self.put_end_for_type(key, replica_type, tenant_id).await?;
        }
        if let Some(replica_type) = decision.revoke_type {
            self.put_revoke_for_type(key, replica_type, tenant_id)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn finalize_batch_put_groups(
        &mut self,
        keys: &[String],
        decisions: &[(usize, ReplicaFinalizeDecision)],
        statuses: &mut [i32],
        tenant_id: &str,
    ) {
        let mut end_groups: HashMap<ReplicaType, Vec<(usize, String)>> = HashMap::new();
        let mut revoke_groups: HashMap<ReplicaType, Vec<(usize, String)>> = HashMap::new();
        for (idx, decision) in decisions {
            if let Some(replica_type) = decision.end_type {
                end_groups
                    .entry(replica_type)
                    .or_default()
                    .push((*idx, keys[*idx].clone()));
            }
            if let Some(replica_type) = decision.revoke_type {
                revoke_groups
                    .entry(replica_type)
                    .or_default()
                    .push((*idx, keys[*idx].clone()));
            }
        }

        for (replica_type, group) in end_groups {
            let group_keys = group.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
            match self
                .batch_put_end(&group_keys, replica_type.into(), tenant_id)
                .await
            {
                Ok(end_statuses) if end_statuses.len() == group.len() => {
                    for ((idx, _), status) in group.into_iter().zip(end_statuses) {
                        if status != 0 {
                            statuses[idx] = status;
                        }
                    }
                }
                _ => {
                    for (idx, _) in group {
                        statuses[idx] = -1;
                    }
                }
            }
        }

        for (replica_type, group) in revoke_groups {
            let group_keys = group.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
            match self
                .batch_put_revoke(&group_keys, replica_type.into(), tenant_id)
                .await
            {
                Ok(revoke_statuses) if revoke_statuses.len() == group.len() => {
                    for ((idx, _), status) in group.into_iter().zip(revoke_statuses) {
                        if status != 0 {
                            statuses[idx] = status;
                        }
                    }
                }
                _ => {
                    for (idx, _) in group {
                        statuses[idx] = -1;
                    }
                }
            }
        }
    }

    pub(super) fn put_start_error_from_status(key: &str, status: tonic::Status) -> StoreError {
        if status.code() == tonic::Code::AlreadyExists {
            StoreError::ObjectExists(key.to_string())
        } else {
            Self::rpc_status_to_error(status)
        }
    }

    pub(super) async unsafe fn write_parts_from_to_replica(
        &self,
        replica: &ReplicaDescriptor,
        buffers: &[*mut c_void],
        sizes: &[usize],
    ) -> StoreResult<()> {
        if buffers.len() != sizes.len() {
            return Err(StoreError::InvalidParams(
                "buffers and sizes length mismatch".to_string(),
            ));
        }
        if buffers.is_empty() {
            return Err(StoreError::InvalidParams(
                "object must contain at least one buffer".to_string(),
            ));
        }

        let total_len = sizes.iter().try_fold(0usize, |acc, &size| {
            acc.checked_add(size)
                .ok_or_else(|| StoreError::InvalidParams("object size overflow".to_string()))
        })?;
        if total_len as u64 > replica.size {
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds replica size {}",
                total_len, replica.size
            )));
        }

        // 从这里开始同时持有远端 Segment 和 native batch 资源；后续每个错误分支都要
        // 按 batch → Segment 的逆序清理，但 source buffer 必须继续活到任务终态。
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        let batch_id = match self.engine.allocate_batch_id(buffers.len()) {
            Ok(id) => id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };

        let mut offset = 0u64;
        let mut requests = Vec::with_capacity(buffers.len());
        for (&buffer, &size) in buffers.iter().zip(sizes.iter()) {
            requests.push(TransferRequest {
                opcode: Opcode::Write,
                source: buffer,
                target_id: segment_id,
                target_offset: replica.base_addr + replica.offset + offset,
                length: size as u64,
            });
            offset += size as u64;
        }

        if let Err(e) = self.engine.submit_transfer(batch_id, &requests) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }

        // wait_for_transfer_batch 是 payload 生命周期的栅栏：只有越过它，调用者才可
        // 认为 RNIC/transport 不再访问 requests 中的裸指针。
        if let Err(e) = self
            .wait_for_transfer_batch(
                batch_id,
                buffers.len(),
                tokio::time::Duration::from_secs(10),
            )
            .await
        {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e);
        }

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        self.engine.close_segment(segment_id)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Put — three-phase write lifecycle (三阶段写生命周期):
    //   put_start  →  write_to_replica (per replica)  →  put_end
    //
    // If any replica write fails, PutRevoke is called to roll back the
    // allocation on the master. This matches the C++ behaviour.
    //
    // 如果任何副本写入失败，调用 PutRevoke 在 master 上回滚分配。
    // 这与 C++ 行为一致。
    // -----------------------------------------------------------------------

    /// Store a key-value pair into the Mooncake distributed store.
    ///
    /// # Three-phase lifecycle (三阶段生命周期)
    ///
    /// 1. **put_start** — ask the master to allocate replicas (Memory + NoF_SSD +
    ///    Disk, depending on `config`). The master returns a list of
    ///    `ReplicaDescriptor` with segment, offset, and base_addr.
    ///
    ///    请求 master 分配副本（Memory + NoF_SSD + Disk，取决于 config）。
    ///    master 返回包含 segment、offset 和 base_addr 的 ReplicaDescriptor 列表。
    ///    C++ 等价：`Client::PutStart()`。
    ///
    /// 2. **write_to_replica** — for each allocated replica, write the data via
    ///    local_memcpy (same-node fast path) or TransferEngine (RDMA/TCP).
    ///
    ///    对每个已分配的副本，通过 local_memcpy（同节点快速路径）或
    ///    TransferEngine（RDMA/TCP）写入数据。
    ///    C++ 等价：`Client::WriteToReplica()`。
    ///
    /// 3. **put_end** — notify the master that all replicas have been written,
    ///    committing the put. The replicas transition from Allocating to Written
    ///    status.
    ///
    ///    通知 master 所有副本已写入，提交本次 put。
    ///    副本状态从 Allocating 转换为 Written。
    ///    C++ 等价：`Client::PutEnd()`。
    ///
    /// # Error handling (错误处理)
    ///
    /// If any replica write fails, **PutRevoke** is sent to the master to release
    /// the allocated resources. Without this, the master would leak replica
    /// allocations.
    ///
    /// 如果任何副本写入失败，向 master 发送 PutRevoke 释放已分配的资源。
    /// 否则 master 会泄漏副本分配。C++ 等价：`Client::PutRevoke()`。
    ///
    /// # Arguments
    /// - `key` — unique key (must be non-empty). / 唯一键（必须非空）。
    /// - `value` — the payload bytes (must be non-empty). / 负载字节（必须非空）。
    /// - `config` — replication configuration (replica count, pinning, preferred
    ///   segments, etc.). Defaults are used if `None`.
    ///   复制配置（副本数、锁定、首选 segment 等）。如果为 None 则使用默认值。
    pub async fn put(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        tracing::info!(target: "te_debug", %key, value_len = value.len(), "put: ENTER");

        // Guard: empty key or value is invalid. / 守卫：空 key 或 value 无效。
        if key.is_empty() || value.is_empty() {
            tracing::error!(target: "te_debug", %key, "put: empty key or value");
            return Err(StoreError::InvalidParams(
                "key is empty or value has zero length".to_string(),
            ));
        }
        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();

        // Phase 1: put_start — allocate replicas on master.
        // 阶段 1：put_start —— 在 master 上分配副本。
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: value.len() as u64,
            tenant_id: tenant_id.clone(),
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
                group_ids: cfg.group_ids.clone(),
            }),
        };

        tracing::info!(target: "te_debug", %key, "put: calling put_start");
        let response = match self.master.put_start(self.rpc_request(request)).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                tracing::error!(target: "te_debug", %key, error = %status, "put: put_start FAILED");
                let err = Self::put_start_error_from_status(key, status);
                if matches!(err, StoreError::ObjectExists(_)) {
                    return Ok(());
                }
                return Err(err);
            }
        };

        let replicas = self.replicas_from_proto(&response.replicas);
        tracing::info!(target: "te_debug", %key, replica_count = replicas.len(), "put: replicas allocated");
        if replicas.is_empty() {
            tracing::error!(target: "te_debug", %key, "put: no replicas allocated");
            return Err(StoreError::NoAvailableHandle);
        }

        let mut transfer_summary = ReplicaTransferSummary::from_replicas(&replicas);
        let mut first_error = None;

        // Phase 2: write_to_replica — write data to each allocated Memory/NoF replica.
        // 阶段 2：write_to_replica —— 向每个已分配 Memory/NoF 副本写入数据。
        for (i, replica) in replicas.iter().enumerate() {
            if !matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) {
                continue;
            }
            tracing::info!(
                target: "te_debug", %key, replica_idx = i, total = replicas.len(),
                seg_name = %replica.segment_name,
                "put: writing to replica"
            );
            match self.write_to_replica(replica, value).await {
                Ok(()) => transfer_summary.record_success(replica.replica_type),
                Err(e) => {
                    tracing::error!(target: "te_debug", %key, replica_idx = i, error = %e, "put: write_to_replica FAILED");
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // Phase 3: C++-style finalize. Reliable modes end/revoke ALL; flexible
        // 1+1 Memory/NoF mode can keep the successful side and revoke the failed side.
        // 阶段 3：按 C++ 策略提交或撤销副本。
        tracing::info!(target: "te_debug", %key, "put: calling put_end");
        let decision = determine_finalize_decision(&cfg, &transfer_summary);
        self.finalize_put_for_key(key, decision, &tenant_id).await?;
        if !decision.success {
            return Err(first_error.unwrap_or(StoreError::NoAvailableHandle));
        }

        tracing::info!(target: "te_debug", %key, "put: EXIT (success)");
        Ok(())
    }
}
