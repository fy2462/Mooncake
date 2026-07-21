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
use serde::Deserialize;
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

#[derive(Debug, Deserialize)]
struct ReplicaCopyPayload {
    key: String,
    source: String,
    targets: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ReplicaMovePayload {
    key: String,
    source: String,
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
        let request = proto::CreateCopyTaskRequest {
            key: key.to_string(),
            targets: targets.to_vec(),
            tenant_id: self.tenant_id.clone(),
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
        let request = proto::CreateMoveTaskRequest {
            key: key.to_string(),
            source: source.to_string(),
            target: target.to_string(),
            tenant_id: self.tenant_id.clone(),
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

        let result = match proto::TaskType::try_from(task.r#type) {
            Ok(proto::TaskType::ReplicaCopy) => {
                let payload: ReplicaCopyPayload =
                    serde_json::from_str(&task.payload).map_err(|e| {
                        StoreError::InvalidParams(format!("invalid replica copy payload: {e}"))
                    })?;
                self.copy(&payload.key, &payload.source, &payload.targets)
                    .await
            }
            Ok(proto::TaskType::ReplicaMove) => {
                let payload: ReplicaMovePayload =
                    serde_json::from_str(&task.payload).map_err(|e| {
                        StoreError::InvalidParams(format!("invalid replica move payload: {e}"))
                    })?;
                self.move_object(&payload.key, &payload.source, &payload.target)
                    .await
            }
            Err(_) => Err(StoreError::InvalidParams(format!(
                "unknown task type: {}",
                task.r#type
            ))),
        };

        match result {
            Ok(()) => {
                self.mark_task_to_complete(task_id, proto::TaskStatus::TaskSuccess, "")
                    .await
            }
            Err(e) => {
                let message = e.to_string();
                self.mark_task_to_complete(task_id, proto::TaskStatus::TaskFailed, &message)
                    .await?;
                Err(e)
            }
        }
    }
}
