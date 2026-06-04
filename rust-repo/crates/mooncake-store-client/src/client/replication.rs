// ============================================================================
// Replica Replication — copy & move operations for replica redistribution.
// 副本复制与迁移 —— 用于副本再分配的 copy/move 操作。
//
// These methods implement the client-side lifecycle for Copy and Move
// operations, matching the C++ Client::Copy / Client::Move / ExecuteReplicaTransfer
// pattern in client_service.cpp.
//
// 这些方法实现了 Copy 和 Move 操作的客户端生命周期，匹配 C++ 中
// Client::Copy / Client::Move / ExecuteReplicaTransfer 模式。
//
// ## Flow (流程)
//
// Copy:  CopyStart → read source → write each target → CopyEnd (or CopyRevoke)
// Move:  MoveStart → read source → write target    → MoveEnd (or MoveRevoke)
//
// C++ equivalents:
//   client_service.cpp:2955  — ExecuteReplicaTransfer
//   client_service.cpp:3012  — Client::Copy
//   client_service.cpp:3056  — Client::Move
//   master_client.cpp        — MasterClient::CopyStart/CopyEnd/etc.
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // =======================================================================
    // RPC-level methods — raw gRPC calls to master
    // RPC 层方法 —— 向 master 发送的原始 gRPC 调用
    //
    // C++ equivalents: MasterClient::CopyStart / CopyEnd / CopyRevoke /
    // MoveStart / MoveEnd / MoveRevoke in master_client.cpp
    // =======================================================================

    // -----------------------------------------------------------------------
    // CopyStart — begin a copy replication task
    // 发起副本复制任务，master 校验 source 存在且 Complete 后在目标 segment 上分配副本
    //
    // C++ equivalent: MasterClient::CopyStart(key, source, targets)
    // -----------------------------------------------------------------------

    /// Start a copy operation: allocate target replicas on the given segments.
    /// The master validates that the source replica exists and is Complete,
    /// then allocates new replicas on each target segment.
    ///
    /// 开始复制操作：在给定 segment 上分配目标副本。
    /// Master 校验源副本存在且 Complete 后，在每个目标 segment 上分配新副本。
    pub async fn copy_start(
        &mut self,
        key: &str,
        source: &str,
        targets: &[String],
        tenant_id: &str,
    ) -> StoreResult<(ReplicaDescriptor, Vec<ReplicaDescriptor>)> {
        let request = proto::CopyStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            source: source.to_string(),
            targets: targets.to_vec(),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .copy_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let source = self
            .replicas_from_proto(std::slice::from_ref(
                response
                    .source
                    .as_ref()
                    .ok_or(StoreError::OperationFailed(-1))?,
            ))
            .pop()
            .ok_or(StoreError::OperationFailed(-1))?;
        let targets = self.replicas_from_proto(&response.targets);
        Ok((source, targets))
    }

    // -----------------------------------------------------------------------
    // CopyEnd — commit a copy replication task
    // 提交复制任务，将目标副本标记为 Complete
    //
    // C++ equivalent: MasterClient::CopyEnd(key)
    // -----------------------------------------------------------------------

    /// Complete a copy operation: mark target replicas as Complete.
    ///
    /// 完成复制操作：将目标副本标记为 Complete。
    pub async fn copy_end(&mut self, key: &str, tenant_id: &str) -> StoreResult<()> {
        let request = proto::CopyEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .copy_end(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // CopyRevoke — roll back a copy task
    // 回滚复制任务，释放已分配的 target 副本
    //
    // C++ equivalent: MasterClient::CopyRevoke(key)
    // -----------------------------------------------------------------------

    /// Revoke a copy operation: remove allocated target replicas and release
    /// the source refcnt.
    ///
    /// 撤销复制操作：移除已分配的 target 副本并释放 source refcnt。
    pub async fn copy_revoke(&mut self, key: &str, tenant_id: &str) -> StoreResult<()> {
        let request = proto::CopyRevokeRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .copy_revoke(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MoveStart — begin a move replication task
    // 发起副本迁移任务，在目标 segment 上分配/复用副本
    //
    // C++ equivalent: MasterClient::MoveStart(key, source, target)
    // -----------------------------------------------------------------------

    /// Start a move operation: allocate or reuse a replica on the target
    /// segment. The source and target must differ.
    ///
    /// 开始移动操作：在目标 segment 上分配或复用副本。源和目标必须不同。
    pub async fn move_start(
        &mut self,
        key: &str,
        source: &str,
        target: &str,
        tenant_id: &str,
    ) -> StoreResult<(ReplicaDescriptor, Option<ReplicaDescriptor>)> {
        let request = proto::MoveStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            source: source.to_string(),
            target: target.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .move_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let source = self
            .replicas_from_proto(std::slice::from_ref(
                response
                    .source
                    .as_ref()
                    .ok_or(StoreError::OperationFailed(-1))?,
            ))
            .pop()
            .ok_or(StoreError::OperationFailed(-1))?;
        let target = response
            .target
            .as_ref()
            .map(|t| {
                self.replicas_from_proto(std::slice::from_ref(t))
                    .pop()
                    .ok_or(StoreError::OperationFailed(-1))
            })
            .transpose()?;
        Ok((source, target))
    }

    // -----------------------------------------------------------------------
    // MoveEnd — commit a move replication task
    // 提交迁移任务，标记目标 Complete，延迟释放源副本
    //
    // C++ equivalent: MasterClient::MoveEnd(key)
    // -----------------------------------------------------------------------

    /// Complete a move operation: mark the target as Complete and schedule
    /// delayed release of the source replica to prevent RDMA in-flight from
    /// accessing reclaimed memory.
    ///
    /// 完成移动操作：将目标标记为 Complete，并安排源副本的延迟释放，
    /// 防止 RDMA 在途访问回收后的内存。
    pub async fn move_end(&mut self, key: &str, tenant_id: &str) -> StoreResult<()> {
        let request = proto::MoveEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .move_end(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MoveRevoke — roll back a move task
    // 回滚迁移任务，释放已分配的 target 副本
    //
    // C++ equivalent: MasterClient::MoveRevoke(key)
    // -----------------------------------------------------------------------

    /// Revoke a move operation: remove the allocated target replica and release
    /// the source refcnt.
    ///
    /// 撤销移动操作：移除已分配的 target 副本并释放 source refcnt。
    pub async fn move_revoke(&mut self, key: &str, tenant_id: &str) -> StoreResult<()> {
        let request = proto::MoveRevokeRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        self.master
            .move_revoke(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    // =======================================================================
    // High-level methods — full lifecycle with data transfer
    // 高层方法 —— 含数据传输的完整生命周期
    //
    // C++ equivalents: Client::Copy / Client::Move / ExecuteReplicaTransfer
    // in client_service.cpp
    // =======================================================================

    // -----------------------------------------------------------------------
    // copy — full replica copy lifecycle
    // 完整副本复制生命周期
    //
    // Flow: CopyStart → read source → write each target → CopyEnd (or CopyRevoke on failure)
    //
    // C++ equivalent: Client::Copy in client_service.cpp:3012
    // -----------------------------------------------------------------------

    /// Copy a key's data from a source segment to one or more target segments.
    ///
    /// # Lifecycle (生命周期)
    ///
    /// 1. **CopyStart** — master allocates target replicas and pins the source.
    ///    Master 分配目标副本并固定源副本。
    ///
    /// 2. **Read source** — read data from the source replica into local_buffer.
    ///    从源副本读取数据到 local_buffer。
    ///
    /// 3. **Write targets** — for each target, write data via RDMA/TCP.
    ///    对每个目标通过 RDMA/TCP 写入数据。
    ///
    /// 4. **CopyEnd** — commit, or **CopyRevoke** on any failure.
    ///    提交；任何步骤失败时回滚。
    ///
    /// # Arguments
    /// - `key` — the object key to copy. / 要复制的对象 key。
    /// - `source` — source segment name. / 源 segment 名称。
    /// - `targets` — target segment names. / 目标 segment 名称列表。
    pub async fn copy(&mut self, key: &str, source: &str, targets: &[String]) -> StoreResult<()> {
        // Phase 1: CopyStart — allocate targets + pin source.
        // 阶段 1：CopyStart —— 分配目标 + 固定源。
        let (source_replica, target_replicas) = self.copy_start(key, source, targets, "").await?;

        if target_replicas.is_empty() {
            // Targets already exist — just finalize.
            // 目标已存在 —— 直接完成。
            return self.copy_end(key, "").await;
        }

        // Phase 2: read source data, then write to each target.
        // 阶段 2：读取源数据，然后写入每个目标。
        let key_owned = key.to_string();
        match self
            .do_execute_replica_transfer(&key_owned, "copy", &source_replica, &target_replicas)
            .await
        {
            Ok(()) => {
                self.copy_end(&key_owned, "").await?;
                Ok(())
            }
            Err(e) => {
                self.copy_revoke(&key_owned, "").await.ok();
                Err(e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // move_object — full replica move lifecycle
    // 完整副本移动生命周期
    //
    // Flow: MoveStart → read source → write target → MoveEnd (or MoveRevoke on failure)
    //
    // C++ equivalent: Client::Move in client_service.cpp:3056
    // -----------------------------------------------------------------------

    /// Move a key's data from a source segment to a target segment.
    /// After a successful move, the source replica is released (with a delay
    /// to prevent RDMA in-flight conflicts).
    ///
    /// 将 key 的数据从源 segment 移动到目标 segment。
    /// 移动成功后，源副本将被释放（延迟释放以防止 RDMA 在途冲突）。
    ///
    /// # Arguments
    /// - `key` — the object key to move. / 要移动的对象 key。
    /// - `source` — source segment name. / 源 segment 名称。
    /// - `target` — target segment name. / 目标 segment 名称。
    pub async fn move_object(&mut self, key: &str, source: &str, target: &str) -> StoreResult<()> {
        // Phase 1: MoveStart — allocate/reuse target + pin source.
        // 阶段 1：MoveStart —— 分配/复用目标 + 固定源。
        let (source_replica, target_opt) = self.move_start(key, source, target, "").await?;

        let Some(target_replica) = target_opt else {
            // Target already exists — just finalize.
            // 目标已存在 —— 直接完成。
            return self.move_end(key, "").await;
        };

        // Phase 2: read source data, then write to target.
        // 阶段 2：读取源数据，然后写入目标。
        let key_owned = key.to_string();
        match self
            .do_execute_replica_transfer(&key_owned, "move", &source_replica, &[target_replica])
            .await
        {
            Ok(()) => {
                self.move_end(&key_owned, "").await?;
                Ok(())
            }
            Err(e) => {
                self.move_revoke(&key_owned, "").await.ok();
                Err(e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // execute_replica_transfer — shared transfer helper
    // 共享的传输辅助函数
    //
    // C++ equivalent: Client::ExecuteReplicaTransfer in client_service.cpp:2955
    // -----------------------------------------------------------------------

    /// Execute the data transfer phase of a copy/move operation.
    ///
    /// # Flow (流程)
    ///
    /// 1. Validate the source is a MEMORY replica. / 校验源为 MEMORY 副本。
    /// 2. Read source data via `read_from_replica`. / 通过 read_from_replica 读取源数据。
    /// 3. Write data to each target via `write_to_replica`. / 通过 write_to_replica 写入每个目标。
    /// 4. Call `end_fn` to commit (CopyEnd / MoveEnd). / 调用 end_fn 提交。
    /// 5. On any failure, call `revoke_fn` to roll back. / 任何失败时调用 revoke_fn 回滚。
    /// Execute the data transfer phase of a copy/move operation: read source,
    /// write all targets. The caller handles end/revoke via the returned Result.
    async fn do_execute_replica_transfer(
        &mut self,
        key: &str,
        action_name: &str,
        source: &ReplicaDescriptor,
        targets: &[ReplicaDescriptor],
    ) -> StoreResult<()> {
        if source.replica_type != mooncake_store_core::ReplicaType::Memory {
            tracing::error!(
                target: "te_debug", %key,
                replica_type = ?source.replica_type,
                "{action_name}: source replica is not MEMORY type",
            );
            return Err(StoreError::InvalidParams(format!(
                "{action_name}: source replica is not MEMORY type"
            )));
        }
        let data = self.read_from_replica(key, source).await.map_err(|e| {
            tracing::error!(target: "te_debug", %key, %e, "{action_name}: failed to read source replica");
            e
        })?;
        for (i, target) in targets.iter().enumerate() {
            self.write_to_replica(target, &data).await.map_err(|e| {
                tracing::error!(target: "te_debug", %key, target_index = i,
                    target_segment = %target.segment_name, %e,
                    "{action_name}: failed to write to target replica");
                e
            })?;
        }
        tracing::info!(target: "te_debug", %key, target_count = targets.len(),
            "{action_name}: transfer completed successfully");
        Ok(())
    }
}
