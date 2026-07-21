// ============================================================================
// Upsert operations — "update or insert" semantics.
// Upsert 操作 —— "更新或插入"语义。
//
// Key difference from put (与 put 的关键区别):
//   - put: creates a NEW key-value pair. If the key already exists, behavior
//     depends on master implementation (typically rejected or overwritten
//     only if explicitly configured).
//     put 创建一个新键值对。如果 key 已存在，行为取决于 master 实现
//     （通常拒绝或仅在明确配置时覆盖）。
//
//   - upsert: UPDATE or INSERT. If the key exists, its value is updated
//     in-place. If it doesn't exist, a new entry is created. The master
//     allocates replicas that may reuse existing allocations.
//     upsert 更新或插入。如果 key 存在，则原地更新其值。如果不存在，
//     则创建新条目。Master 分配的副本可能重用已有分配。
//
// Another notable difference:
//   - put uses PutStart → PutEnd gRPC calls.
//     put 使用 PutStart → PutEnd gRPC 调用。
//   - upsert uses UpsertStart (via UpsertRequest) → BatchUpsertEnd gRPC calls.
//     upsert 使用 UpsertStart（通过 UpsertRequest）→ BatchUpsertEnd gRPC 调用。
//
// upsert also returns the list of allocated replicas, which put does not.
// upsert 还会返回已分配副本的列表，而 put 不返回。
//
// C++ equivalent: real_client.cpp Upsert() / UpsertFrom() / BatchUpsertFrom() /
// UpsertParts()
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig, StoreError};
use std::collections::HashMap;
use std::ffi::c_void;

use super::{
    finalize::{determine_finalize_decision, ReplicaFinalizeDecision, ReplicaTransferSummary},
    MooncakeClient,
};
use crate::client::batch_types::BatchUpsertEntry;
use crate::proto;

impl MooncakeClient {
    async unsafe fn batch_upsert_from_internal(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<(Vec<i32>, Vec<Vec<ReplicaDescriptor>>)> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Err(StoreError::InvalidParams(
                "keys, buffers, and sizes length mismatch".to_string(),
            ));
        }
        if keys.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();
        let slice_lengths: Vec<u64> = sizes.iter().map(|&size| size as u64).collect();
        let start_results = match self
            .batch_upsert_start_results(keys, &slice_lengths, &cfg, &tenant_id)
            .await
        {
            Ok(results) => results,
            Err(_) => return Ok((vec![-1; keys.len()], vec![Vec::new(); keys.len()])),
        };
        if start_results.len() != keys.len() {
            return Ok((vec![-1; keys.len()], vec![Vec::new(); keys.len()]));
        }

        let mut statuses = vec![-1i32; keys.len()];
        let mut descriptors = vec![Vec::new(); keys.len()];
        let mut decisions = Vec::new();

        for (idx, (_key, start_result)) in keys.iter().zip(start_results.iter()).enumerate() {
            if start_result.status != 0 || start_result.replicas.is_empty() {
                statuses[idx] = start_result.status;
                continue;
            }

            let mut transfer_summary =
                ReplicaTransferSummary::from_replicas(&start_result.replicas);
            for replica in &start_result.replicas {
                if !matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                ) {
                    continue;
                }
                if unsafe {
                    self.zero_copy_write(replica, buffers[idx], sizes[idx])
                        .await
                }
                .is_err()
                {
                    transfer_summary.record_failure(replica.replica_type);
                } else {
                    transfer_summary.record_success(replica.replica_type);
                }
            }

            let decision = determine_finalize_decision(&cfg, &transfer_summary);
            if decision.success {
                statuses[idx] = 0;
                descriptors[idx] = start_result.replicas.clone();
            }
            decisions.push((idx, decision));
        }

        self.finalize_batch_upsert_groups(
            keys,
            &cfg,
            &decisions,
            &mut statuses,
            &mut descriptors,
            &tenant_id,
        )
        .await;

        Ok((statuses, descriptors))
    }

    async fn finalize_batch_upsert_groups(
        &mut self,
        keys: &[String],
        cfg: &ReplicateConfig,
        decisions: &[(usize, ReplicaFinalizeDecision)],
        statuses: &mut [i32],
        descriptors: &mut [Vec<ReplicaDescriptor>],
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
            let entries: Vec<BatchUpsertEntry<'_>> = group_keys
                .iter()
                .map(|key| BatchUpsertEntry {
                    key,
                    slice_length: 0,
                    config: cfg.clone(),
                    replica_type: replica_type.into(),
                    tenant_id,
                })
                .collect();
            match self.batch_upsert_end(&entries).await {
                Ok(end_statuses) if end_statuses.len() == group.len() => {
                    for ((idx, _), status) in group.into_iter().zip(end_statuses) {
                        if status != 0 {
                            statuses[idx] = status;
                            descriptors[idx].clear();
                        }
                    }
                }
                _ => {
                    for (idx, _) in group {
                        statuses[idx] = -1;
                        descriptors[idx].clear();
                    }
                }
            }
        }

        for (replica_type, group) in revoke_groups {
            let group_keys = group.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
            let entries: Vec<BatchUpsertEntry<'_>> = group_keys
                .iter()
                .map(|key| BatchUpsertEntry {
                    key,
                    slice_length: 0,
                    config: cfg.clone(),
                    replica_type: replica_type.into(),
                    tenant_id,
                })
                .collect();
            match self.batch_upsert_revoke(&entries).await {
                Ok(revoke_statuses) if revoke_statuses.len() == group.len() => {
                    for ((idx, _), status) in group.into_iter().zip(revoke_statuses) {
                        if status != 0 {
                            statuses[idx] = status;
                            descriptors[idx].clear();
                        }
                    }
                }
                _ => {
                    for (idx, _) in group {
                        statuses[idx] = -1;
                        descriptors[idx].clear();
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Upsert — single key update-or-insert
    // Upsert —— 单 key 更新或插入
    //
    // Lifecycle (生命周期):
    //   UpsertRequest (master allocates/updates replicas)
    //   → write_to_replica (per replica)
    //   → BatchUpsertEnd (commit)
    //
    // On failure: PutRevoke to roll back. / 失败时：PutRevoke 回滚。
    // -----------------------------------------------------------------------

    /// Upsert a key-value pair: update if exists, insert if not.
    ///
    /// 更新或插入键值对：如果存在则更新，如果不存在则插入。
    ///
    /// # Returns (返回值)
    /// The list of [`ReplicaDescriptor`]s allocated for this key.
    /// Unlike [`put`](Self::put), upsert returns the replicas to the caller.
    /// 为此 key 分配的 ReplicaDescriptor 列表。
    /// 与 put 不同，upsert 将副本返回给调用者。
    ///
    /// # Error handling (错误处理)
    /// If any replica write fails, PutRevoke is called and the error is
    /// propagated immediately (same pattern as put).
    ///
    /// 如果任何副本写入失败，调用 PutRevoke 并立即传播错误（与 put 相同的模式）。
    pub async fn upsert(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();

        // Phase 1: request master to upsert (allocates or updates replicas).
        // 阶段 1：请求 master 进行 upsert（分配或更新副本）。
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
                group_ids: cfg.group_ids.clone(),
            }),
            tenant_id: tenant_id.clone(),
        };

        let response = self
            .master
            .upsert(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        // Phase 2: write data to each allocated replica.
        // 阶段 2：向每个已分配副本写入数据。
        for replica in &replicas {
            if let Err(e) = self.write_to_replica(replica, value).await {
                // On failure: revoke all allocations. / 失败时：撤销所有分配。
                // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                let revoke_req = proto::PutRevokeRequest {
                    client_id: Some(self.client_id_proto()),
                    key: key.to_string(),
                    replica_type: 0,
                    tenant_id: tenant_id.clone(),
                };
                let _ = self.master.put_revoke(self.rpc_request(revoke_req)).await;
                return Err(e);
            }
        }

        // Phase 3: commit via BatchUpsertEnd.
        // 阶段 3：通过 BatchUpsertEnd 提交。
        // Note: Upsert uses BatchUpsertEnd (not PutEnd) because the protocol
        // supports batching multiple upsert entries in a single commit.
        // 注意：Upsert 使用 BatchUpsertEnd（而非 PutEnd），因为协议支持在
        // 单次提交中批量处理多个 upsert 条目。
        let end_request = proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                replica_type: 0, // MEMORY
                tenant_id: tenant_id.clone(),
            }],
        };
        self.master
            .batch_upsert_end(self.rpc_request(end_request))
            .await
            .map_err(Self::rpc_status_to_error)?;

        Ok(replicas)
    }

    /// Zero-copy upsert from a caller-provided buffer.
    ///
    /// Same semantics as [`upsert`](Self::upsert) but uses
    /// [`zero_copy_write`](Self::zero_copy_write) to transfer data directly
    /// from the caller's pre-registered buffer, avoiding a copy into
    /// `local_buffer`.
    ///
    /// 从调用者提供的缓冲区进行零拷贝 upsert。
    /// 语义与 upsert 相同，但使用 zero_copy_write 直接从调用者预先注册的缓冲区
    /// 传输数据，避免拷贝到 local_buffer。
    ///
    /// # Safety
    /// `buffer` must be pre-registered with the TE via
    /// [`register_buffer`](Self::register_buffer).
    ///
    /// buffer 必须通过 register_buffer 预先向 TE 注册。
    /// C++ equivalent: `Client::UpsertFrom(key, buffer, size, config)`
    pub async unsafe fn upsert_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();

        // Phase 1: upsert start. / 阶段 1：upsert 开始。
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
                group_ids: cfg.group_ids.clone(),
            }),
            tenant_id: tenant_id.clone(),
        };

        let response = self
            .master
            .upsert(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        // Phase 2: zero-copy write to each replica. / 阶段 2：零拷贝写入每个副本。
        for replica in &replicas {
            if let Err(e) = self.zero_copy_write(replica, buffer, size).await {
                // On failure: revoke. / 失败时：撤销。
                // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                let revoke_req = proto::PutRevokeRequest {
                    client_id: Some(self.client_id_proto()),
                    key: key.to_string(),
                    replica_type: 0,
                    tenant_id: tenant_id.clone(),
                };
                let _ = self.master.put_revoke(self.rpc_request(revoke_req)).await;
                return Err(e);
            }
        }

        // Phase 3: commit. / 阶段 3：提交。
        let end_request = proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                replica_type: 0, // MEMORY
                tenant_id: tenant_id.clone(),
            }],
        };
        self.master
            .batch_upsert_end(self.rpc_request(end_request))
            .await
            .map_err(Self::rpc_status_to_error)?;

        Ok(replicas)
    }

    /// Batch upsert from pre-registered buffers.
    ///
    /// Each key is upserted independently via
    /// [`upsert_from`](Self::upsert_from). Note that unlike
    /// [`batch_put`](Self::batch_put), this method propagates errors rather
    /// than using per-key error tolerance — a failure on any key aborts the
    /// entire batch. This is because upsert returns the allocated replicas
    /// for each key, and partial results could leave the caller with an
    /// inconsistent view.
    ///
    /// 从预注册缓冲区批量 upsert。
    /// 每个 key 通过 upsert_from 独立 upsert。注意与 batch_put 不同，
    /// 此方法传播错误而非按 key 容错 —— 任何 key 的失败都会中止整个批次。
    /// 这是因为 upsert 返回每个 key 的已分配副本，部分结果会导致调用者
    /// 视图不一致。
    ///
    /// # Safety
    /// All `buffers[i]` must be pre-registered with the TE.
    ///
    /// 所有 buffers[i] 必须预先向 TE 注册。
    pub async unsafe fn batch_upsert_from(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<Vec<ReplicaDescriptor>>> {
        let (_statuses, descriptors) = unsafe {
            self.batch_upsert_from_internal(keys, buffers, sizes, config)
                .await?
        };
        Ok(descriptors)
    }

    /// C++-style batch upsert from pre-registered buffers.
    ///
    /// Returns one status per key (`0` on success, negative on failure), matching
    /// `RealClient::batch_upsert_from`.
    pub async unsafe fn batch_upsert_from_statuses(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        let (statuses, _descriptors) = unsafe {
            self.batch_upsert_from_internal(keys, buffers, sizes, config)
                .await?
        };
        Ok(statuses)
    }

    /// Upsert as multiple data slices. Convenience wrapper that concatenates
    /// the slices and delegates to [`upsert`](Self::upsert).
    ///
    /// 以多个数据切片进行 upsert。便捷封装，将切片拼接后委托给 upsert。
    ///
    /// C++ equivalent: `Client::UpsertParts(key, values, config)`
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
