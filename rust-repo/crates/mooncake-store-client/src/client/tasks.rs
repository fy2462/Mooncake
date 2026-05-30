use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Task management
    // -----------------------------------------------------------------------

    pub async fn create_copy_task(&mut self, key: &str, targets: &[String]) -> StoreResult<Uuid> {
        let request = proto::CreateCopyTaskRequest {
            key: key.to_string(),
            targets: targets.to_vec(),
        };
        let response = self
            .master
            .create_copy_task(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        match response.task_id {
            Some(id) => Ok(Uuid::from_u64_pair(id.high, id.low)),
            None => Err(StoreError::OperationFailed(-1)),
        }
    }

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
        };
        let response = self
            .master
            .create_move_task(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        match response.task_id {
            Some(id) => Ok(Uuid::from_u64_pair(id.high, id.low)),
            None => Err(StoreError::OperationFailed(-1)),
        }
    }

    pub async fn query_task(&mut self, task_id: Uuid) -> StoreResult<proto::QueryTaskResponse> {
        let request = proto::QueryTaskRequest {
            task_id: Some(proto::Uuid {
                high: task_id.as_u64_pair().0,
                low: task_id.as_u64_pair().1,
            }),
        };
        let response = self
            .master
            .query_task(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response)
    }

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
            .fetch_tasks(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.tasks)
    }

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
            .mark_task_to_complete(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }
}
