//! Client-visible drain-job lifecycle: create and poll drain jobs.

use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    /// Create a drain job moving the listed source segments to target
    /// segments, then return the job id.
    pub async fn create_drain_job(
        &mut self,
        segments: &[String],
        target_segments: &[String],
        max_concurrency: u32,
    ) -> StoreResult<Uuid> {
        let request = proto::CreateDrainJobRequest {
            segments: segments.to_vec(),
            target_segments: target_segments.to_vec(),
            max_concurrency,
        };
        let response = self
            .master
            .create_drain_job(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let job_id = response.job_id.ok_or_else(|| {
            StoreError::InvalidParams("create_drain_job response missing job_id".to_string())
        })?;
        Ok(Uuid::from_u64_pair(job_id.high, job_id.low))
    }

    /// Query a drain job's current status and counters.
    pub async fn query_drain_job(
        &mut self,
        job_id: Uuid,
    ) -> StoreResult<proto::QueryDrainJobResponse> {
        let request = proto::QueryDrainJobRequest {
            job_id: Some(proto::Uuid {
                high: job_id.as_u64_pair().0,
                low: job_id.as_u64_pair().1,
            }),
        };
        self.master
            .query_drain_job(self.rpc_request(request))
            .await
            .map(|response| response.into_inner())
            .map_err(Self::rpc_status_to_error)
    }
}
