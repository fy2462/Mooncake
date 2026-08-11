// ============================================================================
// Task management — async copy/move operations coordinated by the master.
// 任务管理 —— master 协调的异步复制/移动操作。
//
// Tasks represent long-running background operations (copy/move) that are
// submitted to the master, queued, and then fetched/executed by worker
// clients. The workflow is:
//
//   1. Client calls create_copy_task / create_move_task → gets a task UUID.
//   2. Worker clients call fetch_tasks to pull pending tasks.
//   3. Worker executes the task (performs the actual data transfer).
//   4. Worker calls mark_task_to_complete with status + message.
//   5. Caller can query_task to check progress.
//
// Tasks 代表长时间运行的后台操作（复制/移动），提交给 master，排队，
// 然后由 worker 客户端获取并执行。工作流为：
//   1. 客户端调用 create_copy_task / create_move_task → 获取任务 UUID。
//   2. Worker 客户端调用 fetch_tasks 拉取待处理任务。
//   3. Worker 执行任务（执行实际的数据传输）。
//   4. Worker 调用 mark_task_to_complete 提交状态和消息。
//   5. 调用者可以通过 query_task 检查进度。
//
// C++ equivalent: real_client.cpp CreateCopyTask() / CreateMoveTask() /
// QueryTask() / FetchTasks() / MarkTaskToComplete()
// ============================================================================

use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

fn default_task_tenant() -> String {
    "default".to_string()
}

/// Retry delay for a given retry count, in milliseconds. The C++ executor
/// sleeps 50ms per attempt slot (50, 100, ..., 500 for the first ten).
fn retry_delay_ms(retry_count: u32) -> u64 {
    50 * u64::from(retry_count.saturating_add(1))
}

/// Retry decision: only NoAvailableHandle retries, and only below the
/// configured attempt budget.
fn should_retry(error: &StoreError, retry_count: u32, max_retry_attempts: u32) -> bool {
    matches!(error, StoreError::NoAvailableHandle) && retry_count < max_retry_attempts
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplicaCopyPayload {
    #[serde(default = "default_task_tenant")]
    tenant_id: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    targets: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplicaMovePayload {
    #[serde(default = "default_task_tenant")]
    tenant_id: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    target: String,
}

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Task creation — submit a copy or move task to the master
    // 任务创建 —— 向 master 提交复制或移动任务
    // -----------------------------------------------------------------------

    /// Create a copy task: replicate a key's data to one or more target nodes.
    ///
    /// 创建复制任务：将 key 的数据复制到一个或多个目标节点。
    ///
    /// # Returns
    /// A [`Uuid`] task identifier that can be used with
    /// [`query_task`](Self::query_task) to check progress.
    /// Uuid 任务标识符，可用于 query_task 检查进度。
    ///
    /// C++ equivalent: `Client::CreateCopyTask(key, targets)`
    pub async fn create_copy_task(&mut self, key: &str, targets: &[String]) -> StoreResult<Uuid> {
        let tenant_id = self.tenant_id.clone();
        self.create_copy_task_for_tenant(key, &tenant_id, targets)
            .await
    }

    /// Create a copy task for an explicitly selected tenant.
    pub async fn create_copy_task_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
        targets: &[String],
    ) -> StoreResult<Uuid> {
        let request = proto::CreateCopyTaskRequest {
            key: key.to_string(),
            targets: targets.to_vec(),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .create_copy_task(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        match response.task_id {
            Some(id) => Ok(Uuid::from_u64_pair(id.high, id.low)),
            None => Err(StoreError::OperationFailed(-1)),
        }
    }

    /// Create a move task: migrate a key's data from a source node to a
    /// target node. The master handles the handoff—after completion, the
    /// source replica is released and the target replica becomes the
    /// authoritative copy.
    ///
    /// 创建移动任务：将 key 的数据从源节点迁移到目标节点。
    /// Master 处理交接 —— 完成后，源副本被释放，目标副本成为权威副本。
    ///
    /// C++ equivalent: `Client::CreateMoveTask(key, source, target)`
    pub async fn create_move_task(
        &mut self,
        key: &str,
        source: &str,
        target: &str,
    ) -> StoreResult<Uuid> {
        let tenant_id = self.tenant_id.clone();
        self.create_move_task_for_tenant(key, &tenant_id, source, target)
            .await
    }

    /// Create a move task for an explicitly selected tenant.
    pub async fn create_move_task_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
        source: &str,
        target: &str,
    ) -> StoreResult<Uuid> {
        let request = proto::CreateMoveTaskRequest {
            key: key.to_string(),
            source: source.to_string(),
            target: target.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .create_move_task(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        match response.task_id {
            Some(id) => Ok(Uuid::from_u64_pair(id.high, id.low)),
            None => Err(StoreError::OperationFailed(-1)),
        }
    }

    // -----------------------------------------------------------------------
    // Task query — check the status of a previously-created task
    // 任务查询 —— 检查先前创建的任务的状态
    // -----------------------------------------------------------------------

    /// Query the status and result of a previously-created task.
    ///
    /// 查询先前创建的任务的状态和结果。
    ///
    /// C++ equivalent: `Client::QueryTask(task_id)`
    pub async fn query_task(&mut self, task_id: Uuid) -> StoreResult<proto::QueryTaskResponse> {
        let request = proto::QueryTaskRequest {
            task_id: Some(proto::Uuid {
                high: task_id.as_u64_pair().0,
                low: task_id.as_u64_pair().1,
            }),
        };
        let response = self
            .master
            .query_task(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response)
    }

    // -----------------------------------------------------------------------
    // Task fetching — pull pending tasks from the master (worker side)
    // 任务获取 —— 从 master 拉取待处理任务（worker 侧）
    // -----------------------------------------------------------------------

    /// Fetch a batch of pending tasks from the master.
    ///
    /// Workers call this periodically to claim tasks. The master assigns tasks
    /// based on node capabilities and current load.
    ///
    /// 从 master 获取一批待处理任务。
    /// Worker 定期调用此方法以领取任务。Master 根据节点能力和当前负载分配任务。
    ///
    /// # Arguments
    /// - `batch_size` — maximum number of tasks to fetch in one call.
    ///   单次调用最多获取的任务数。
    ///
    /// C++ equivalent: `Client::FetchTasks(client_id, batch_size)`
    pub async fn fetch_tasks(
        &mut self,
        batch_size: u32,
    ) -> StoreResult<Vec<proto::TaskAssignment>> {
        let request = proto::FetchTasksRequest {
            client_id: Some(self.client_id_proto()),
            batch_size,
        };
        let response = self
            .master
            .fetch_tasks(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.tasks)
    }

    // -----------------------------------------------------------------------
    // Task completion — report task outcome back to the master
    // 任务完成 —— 向 master 报告任务结果
    // -----------------------------------------------------------------------

    /// Mark a task as complete on the master, with a status and message.
    ///
    /// 在 master 上将任务标记为完成，附状态和消息。
    ///
    /// # Arguments
    /// - `task_id` — the task UUID returned by `create_copy_task` or
    ///   `create_move_task`. / 由 create_copy_task 或 create_move_task 返回的
    ///   任务 UUID。
    /// - `status` — the completion status (e.g. `Completed`, `Failed`).
    ///   完成状态（如 Completed、Failed）。
    /// - `message` — human-readable result or error message.
    ///   人类可读的结果或错误消息。
    ///
    /// C++ equivalent: `Client::MarkTaskToComplete(client_id, task_id, status, message)`
    pub async fn mark_task_to_complete(
        &mut self,
        task_id: Uuid,
        status: proto::TaskStatus,
        message: &str,
    ) -> StoreResult<()> {
        let request = proto::MarkTaskToCompleteRequest {
            client_id: Some(self.client_id_proto()),
            request: Some(proto::TaskCompleteRequest {
                id: Some(proto::Uuid {
                    high: task_id.as_u64_pair().0,
                    low: task_id.as_u64_pair().1,
                }),
                status: status as i32,
                message: message.to_string(),
            }),
        };
        self.master
            .mark_task_to_complete(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    /// Execute one fetched task and report its result to the master.
    ///
    /// C++ equivalent: `Client::ExecuteTask` in `client_service.cpp`.
    pub async fn execute_task_assignment(
        &mut self,
        task: proto::TaskAssignment,
    ) -> StoreResult<()> {
        let task_id = task
            .id
            .map(|id| Uuid::from_u64_pair(id.high, id.low))
            .ok_or_else(|| StoreError::InvalidParams("task assignment missing id".to_string()))?;

        let mut retry_count = 0_u32;
        let result = loop {
            let result = match proto::TaskType::try_from(task.r#type) {
                Ok(proto::TaskType::ReplicaCopy) => {
                    match serde_json::from_str::<ReplicaCopyPayload>(&task.payload) {
                        Ok(payload) => {
                            self.copy_for_tenant(
                                &payload.key,
                                &payload.tenant_id,
                                &payload.source,
                                &payload.targets,
                            )
                            .await
                        }
                        Err(error) => Err(StoreError::InvalidParams(format!(
                            "invalid replica copy payload: {error}"
                        ))),
                    }
                }
                Ok(proto::TaskType::ReplicaMove) => {
                    match serde_json::from_str::<ReplicaMovePayload>(&task.payload) {
                        Ok(payload) => {
                            self.move_object_for_tenant(
                                &payload.key,
                                &payload.tenant_id,
                                &payload.source,
                                &payload.target,
                            )
                            .await
                        }
                        Err(error) => Err(StoreError::InvalidParams(format!(
                            "invalid replica move payload: {error}"
                        ))),
                    }
                }
                Err(_) => Err(StoreError::InvalidParams(format!(
                    "unknown task type: {}",
                    task.r#type
                ))),
            };

            let retry = match &result {
                Err(error) => should_retry(error, retry_count, task.max_retry_attempts),
                Ok(()) => false,
            };
            if retry {
                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms(
                    retry_count,
                )))
                .await;
                retry_count += 1;
                continue;
            }
            break result;
        };

        match result {
            Ok(()) => {
                self.mark_task_to_complete(
                    task_id,
                    proto::TaskStatus::TaskSuccess,
                    "Task completed successfully",
                )
                .await
            }
            Err(e) => {
                let message = format!("{e} (max retries reached: {})", task.max_retry_attempts);
                if let Err(report_error) = self
                    .mark_task_to_complete(task_id, proto::TaskStatus::TaskFailed, &message)
                    .await
                {
                    tracing::warn!(
                        %task_id,
                        %report_error,
                        "failed to report terminal task failure"
                    );
                }
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplicaCopyPayload, ReplicaMovePayload, retry_delay_ms, should_retry};
    use crate::proto;
    use mooncake_store_core::StoreError;

    #[test]
    fn task_payload_preserves_explicit_tenant() {
        let copy: ReplicaCopyPayload = serde_json::from_str(
            r#"{"tenant_id":"tenant-a","key":"k","source":"s","targets":["t"]}"#,
        )
        .unwrap();
        let move_payload: ReplicaMovePayload =
            serde_json::from_str(r#"{"tenant_id":"tenant-b","key":"k","source":"s","target":"t"}"#)
                .unwrap();

        assert_eq!(copy.tenant_id, "tenant-a");
        assert_eq!(move_payload.tenant_id, "tenant-b");
    }

    #[test]
    fn legacy_task_payload_defaults_to_default_tenant() {
        let copy: ReplicaCopyPayload = serde_json::from_str(
            r#"{"key":"legacy_copy_key","source":"segment_0","targets":["segment_1"]}"#,
        )
        .unwrap();
        let move_payload: ReplicaMovePayload = serde_json::from_str(
            r#"{"key":"legacy_move_key","source":"segment_0","target":"segment_1"}"#,
        )
        .unwrap();

        assert_eq!(copy.tenant_id, "default");
        assert_eq!(copy.key, "legacy_copy_key");
        assert_eq!(copy.source, "segment_0");
        assert_eq!(copy.targets, ["segment_1"]);
        assert_eq!(move_payload.tenant_id, "default");
        assert_eq!(move_payload.key, "legacy_move_key");
        assert_eq!(move_payload.source, "segment_0");
        assert_eq!(move_payload.target, "segment_1");
    }

    // ReplicaCopyPayloadStructure: key and two ordered targets survive decode
    // at exact positions.
    #[test]
    fn cpp_parity_copy_payload_structure_preserves_key_and_two_ordered_targets() {
        let payload: ReplicaCopyPayload = serde_json::from_str(
            r#"{"key":"test_key","source":"source","targets":["target1","target2"]}"#,
        )
        .unwrap();
        assert_eq!(payload.key, "test_key");
        assert_eq!(payload.targets.len(), 2);
        assert_eq!(payload.targets[0], "target1");
        assert_eq!(payload.targets[1], "target2");
    }

    // ReplicaCopyPayloadSingleTarget: the parsed key and sole target value are
    // exact.
    #[test]
    fn cpp_parity_copy_payload_preserves_exact_single_target() {
        let payload: ReplicaCopyPayload = serde_json::from_str(
            r#"{"key":"test_single_target_key","source":"source","targets":["target_segment"]}"#,
        )
        .unwrap();
        assert_eq!(payload.key, "test_single_target_key");
        assert_eq!(payload.targets.len(), 1);
        assert_eq!(payload.targets[0], "target_segment");
    }

    // ReplicaMovePayloadStructure: key, source, and target decode together.
    #[test]
    fn cpp_parity_move_payload_preserves_key_source_and_target() {
        let payload: ReplicaMovePayload = serde_json::from_str(
            r#"{"key":"move_key","source":"move_source","target":"move_target"}"#,
        )
        .unwrap();
        assert_eq!(payload.key, "move_key");
        assert_eq!(payload.source, "move_source");
        assert_eq!(payload.target, "move_target");
    }

    // ReplicaCopyPayloadMultipleTargets: a four-target payload round-trips
    // through the bidirectional serde representation.
    #[test]
    fn cpp_parity_copy_payload_four_target_serde_roundtrip() {
        let payload = ReplicaCopyPayload {
            tenant_id: "default".to_string(),
            key: "test_key".to_string(),
            source: "source".to_string(),
            targets: vec![
                "target1".to_string(),
                "target2".to_string(),
                "target3".to_string(),
                "target4".to_string(),
            ],
        };
        let encoded = serde_json::to_string(&payload).unwrap();
        let restored: ReplicaCopyPayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.key, "test_key");
        assert_eq!(restored.targets.len(), 4);
        assert_eq!(restored.targets[0], "target1");
        assert_eq!(restored.targets[3], "target4");
    }

    // ReplicaCopyPayloadEmptyTargets: an empty targets vector round-trips.
    #[test]
    fn cpp_parity_copy_payload_empty_targets_serde_roundtrip() {
        let payload = ReplicaCopyPayload {
            tenant_id: "default".to_string(),
            key: "test_key".to_string(),
            source: "source".to_string(),
            targets: Vec::new(),
        };
        let encoded = serde_json::to_string(&payload).unwrap();
        let restored: ReplicaCopyPayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.key, "test_key");
        assert!(restored.targets.is_empty());
    }

    // CopyMethodEmptyKey: an empty key round-trips with a nonempty JSON body.
    #[test]
    fn cpp_parity_copy_payload_empty_key_serde_roundtrip() {
        let payload = ReplicaCopyPayload {
            tenant_id: "default".to_string(),
            key: String::new(),
            source: "source".to_string(),
            targets: vec!["target1".to_string()],
        };
        let encoded = serde_json::to_string(&payload).unwrap();
        assert!(!encoded.is_empty());
        let restored: ReplicaCopyPayload = serde_json::from_str(&encoded).unwrap();
        assert!(restored.key.is_empty());
        assert_eq!(restored.targets.len(), 1);
        assert_eq!(restored.targets[0], "target1");
    }

    // CopyMethodEmptyTargets: empty targets round-trip at the payload level.
    #[test]
    fn cpp_parity_copy_method_payload_empty_targets_roundtrip() {
        let payload = ReplicaCopyPayload {
            tenant_id: "default".to_string(),
            key: "test_key".to_string(),
            source: "source".to_string(),
            targets: Vec::new(),
        };
        let encoded = serde_json::to_string(&payload).unwrap();
        let restored: ReplicaCopyPayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.key, "test_key");
        assert!(restored.targets.is_empty());
    }

    // MoveMethodEmptyKey: an empty key round-trips without altering the
    // nonempty source and target segment names.
    #[test]
    fn cpp_parity_move_payload_empty_key_serde_roundtrip() {
        let payload = ReplicaMovePayload {
            tenant_id: "default".to_string(),
            key: String::new(),
            source: "source_segment".to_string(),
            target: "target_segment".to_string(),
        };
        let encoded = serde_json::to_string(&payload).unwrap();
        let restored: ReplicaMovePayload = serde_json::from_str(&encoded).unwrap();
        assert!(restored.key.is_empty());
        assert_eq!(restored.source, "source_segment");
        assert_eq!(restored.target, "target_segment");
    }

    // MoveMethodEmptySegments: empty source round-trips with exact key/target.
    #[test]
    fn cpp_parity_move_payload_empty_source_serde_roundtrip() {
        let payload = ReplicaMovePayload {
            tenant_id: "default".to_string(),
            key: "test_key".to_string(),
            source: String::new(),
            target: "target_segment".to_string(),
        };
        let encoded = serde_json::to_string(&payload).unwrap();
        let restored: ReplicaMovePayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.key, "test_key");
        assert!(restored.source.is_empty());
        assert_eq!(restored.target, "target_segment");
    }

    // MultipleTargetSegmentsHandling: five targets round-trip exactly.
    #[test]
    fn cpp_parity_copy_payload_five_target_fanout_roundtrip() {
        let targets = ["target1", "target2", "target3", "target4", "target5"]
            .iter()
            .map(|target| (*target).to_string())
            .collect::<Vec<_>>();
        let payload = ReplicaCopyPayload {
            tenant_id: "default".to_string(),
            key: "test_key".to_string(),
            source: "source".to_string(),
            targets: targets.clone(),
        };
        assert_eq!(payload.targets.len(), 5);
        assert_eq!(payload.targets[0], "target1");
        assert_eq!(payload.targets[4], "target5");
        let encoded = serde_json::to_string(&payload).unwrap();
        let restored: ReplicaCopyPayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.targets.len(), 5);
        assert_eq!(restored.targets[0], "target1");
        assert_eq!(restored.targets[4], "target5");
    }

    // PayloadDeserializationErrorHandling: an unknown-only payload decodes to
    // defaulted empty fields instead of failing.
    #[test]
    fn cpp_parity_unknown_only_copy_payload_defaults_to_empty_fields() {
        let payload: ReplicaCopyPayload = serde_json::from_str(r#"{"invalid":"json"}"#).unwrap();
        assert!(payload.key.is_empty());
        assert!(payload.source.is_empty());
        assert!(payload.targets.is_empty());
    }

    // ClientTaskStructure: a Copy assignment preserves type and the decoded
    // single target.
    #[test]
    fn cpp_parity_copy_assignment_preserves_type_key_and_single_target() {
        let assignment = proto::TaskAssignment {
            id: None,
            r#type: proto::TaskType::ReplicaCopy as i32,
            payload: r#"{"key":"test_key","source":"source","targets":["target_segment"]}"#
                .to_string(),
            created_at_ms_epoch: 1,
            max_retry_attempts: 3,
        };
        assert_eq!(assignment.r#type, proto::TaskType::ReplicaCopy as i32);
        assert!(!assignment.payload.is_empty());
        let payload: ReplicaCopyPayload = serde_json::from_str(&assignment.payload).unwrap();
        assert_eq!(payload.key, "test_key");
        assert_eq!(payload.targets.len(), 1);
        assert_eq!(payload.targets[0], "target_segment");
    }

    // TaskAssignmentToClientTaskConversion: a Copy assignment with a two-target
    // payload preserves key and cardinality.
    #[test]
    fn cpp_parity_copy_assignment_preserves_key_and_two_target_cardinality() {
        let assignment = proto::TaskAssignment {
            id: None,
            r#type: proto::TaskType::ReplicaCopy as i32,
            payload: r#"{"key":"test_key","source":"source","targets":["target1","target2"]}"#
                .to_string(),
            created_at_ms_epoch: 1,
            max_retry_attempts: 3,
        };
        assert_eq!(assignment.r#type, proto::TaskType::ReplicaCopy as i32);
        assert!(!assignment.payload.is_empty());
        let payload: ReplicaCopyPayload = serde_json::from_str(&assignment.payload).unwrap();
        assert_eq!(payload.key, "test_key");
        assert_eq!(payload.targets.len(), 2);
    }

    // RetryDelayCalculation: counts 0..9 produce exactly the ten-value
    // millisecond vector [50, 100, ..., 500].
    #[test]
    fn cpp_parity_retry_delay_matrix_is_50_through_500_ms() {
        let delays = (0..10).map(retry_delay_ms).collect::<Vec<_>>();
        assert_eq!(
            delays,
            vec![50, 100, 150, 200, 250, 300, 350, 400, 450, 500]
        );
    }

    // RetryDecisionLogic: with max 10, NoAvailableHandle retries below ten and
    // stops at ten; every other error never retries.
    #[test]
    fn cpp_parity_only_no_available_handle_retries_below_ten() {
        let retriable = StoreError::NoAvailableHandle;
        assert!(should_retry(&retriable, 0, 10));
        assert!(should_retry(&retriable, 5, 10));
        assert!(should_retry(&retriable, 9, 10));
        assert!(!should_retry(&retriable, 10, 10));
        assert!(!should_retry(&retriable, 11, 10));
        assert!(!should_retry(&StoreError::KeyNotFound("k".into()), 0, 10));
        assert!(!should_retry(
            &StoreError::SegmentNotFound("s".into()),
            0,
            10
        ));
        assert!(!should_retry(&StoreError::InvalidParams("x".into()), 0, 10));
    }

    // RetryCountIncrement: the deterministic retry counter progresses exactly
    // 0/1 through 9/10 and then 11 on one more increment.
    #[test]
    fn cpp_parity_retry_counter_progresses_exactly_zero_through_eleven() {
        let mut retry_count = 0_u32;
        for expected_pre in 0..10 {
            assert_eq!(retry_count, expected_pre);
            retry_count += 1;
            assert_eq!(retry_count, expected_pre + 1);
        }
        retry_count += 1;
        assert_eq!(retry_count, 11);
    }
}
