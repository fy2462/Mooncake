use super::*;

#[tonic::async_trait]
impl MasterService for MasterServiceImpl {
    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        MasterServiceImpl::ping_impl(self, request).await
    }

    async fn mount_segment(
        &self,
        request: Request<proto::MountSegmentRequest>,
    ) -> Result<Response<proto::MountSegmentResponse>, Status> {
        MasterServiceImpl::mount_segment_impl(self, request).await
    }

    async fn unmount_segment(
        &self,
        request: Request<proto::UnmountSegmentRequest>,
    ) -> Result<Response<proto::UnmountSegmentResponse>, Status> {
        MasterServiceImpl::unmount_segment_impl(self, request).await
    }

    async fn graceful_unmount_segment(
        &self,
        request: Request<proto::GracefulUnmountSegmentRequest>,
    ) -> Result<Response<proto::GracefulUnmountSegmentResponse>, Status> {
        MasterServiceImpl::graceful_unmount_segment_impl(self, request).await
    }

    async fn re_mount_segment(
        &self,
        request: Request<proto::ReMountSegmentRequest>,
    ) -> Result<Response<proto::ReMountSegmentResponse>, Status> {
        MasterServiceImpl::re_mount_segment_impl(self, request).await
    }

    async fn mount_local_disk_segment(
        &self,
        request: Request<proto::MountLocalDiskSegmentRequest>,
    ) -> Result<Response<proto::MountLocalDiskSegmentResponse>, Status> {
        MasterServiceImpl::mount_local_disk_segment_impl(self, request).await
    }

    async fn offload_object_heartbeat(
        &self,
        request: Request<proto::OffloadObjectHeartbeatRequest>,
    ) -> Result<Response<proto::OffloadObjectHeartbeatResponse>, Status> {
        MasterServiceImpl::offload_object_heartbeat_impl(self, request).await
    }

    async fn report_ssd_capacity(
        &self,
        request: Request<proto::ReportSsdCapacityRequest>,
    ) -> Result<Response<proto::ReportSsdCapacityResponse>, Status> {
        MasterServiceImpl::report_ssd_capacity_impl(self, request).await
    }

    async fn notify_offload_success(
        &self,
        request: Request<proto::NotifyOffloadSuccessRequest>,
    ) -> Result<Response<proto::NotifyOffloadSuccessResponse>, Status> {
        MasterServiceImpl::notify_offload_success_impl(self, request).await
    }

    async fn promotion_object_heartbeat(
        &self,
        request: Request<proto::PromotionObjectHeartbeatRequest>,
    ) -> Result<Response<proto::PromotionObjectHeartbeatResponse>, Status> {
        MasterServiceImpl::promotion_object_heartbeat_impl(self, request).await
    }

    async fn promotion_alloc_start(
        &self,
        request: Request<proto::PromotionAllocStartRequest>,
    ) -> Result<Response<proto::PromotionAllocStartResponse>, Status> {
        MasterServiceImpl::promotion_alloc_start_impl(self, request).await
    }

    async fn notify_promotion_success(
        &self,
        request: Request<proto::NotifyPromotionSuccessRequest>,
    ) -> Result<Response<proto::NotifyPromotionSuccessResponse>, Status> {
        MasterServiceImpl::notify_promotion_success_impl(self, request).await
    }

    async fn notify_promotion_failure(
        &self,
        request: Request<proto::NotifyPromotionFailureRequest>,
    ) -> Result<Response<proto::NotifyPromotionFailureResponse>, Status> {
        MasterServiceImpl::notify_promotion_failure_impl(self, request).await
    }

    async fn exist_key(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        MasterServiceImpl::exist_key_impl(self, request).await
    }

    async fn get_all_keys(
        &self,
        request: Request<proto::GetAllKeysRequest>,
    ) -> Result<Response<proto::GetAllKeysResponse>, Status> {
        MasterServiceImpl::get_all_keys_impl(self, request).await
    }

    async fn get_all_segments(
        &self,
        request: Request<proto::GetAllSegmentsRequest>,
    ) -> Result<Response<proto::GetAllSegmentsResponse>, Status> {
        MasterServiceImpl::get_all_segments_impl(self, request).await
    }

    async fn put_start(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        MasterServiceImpl::put_start_impl(self, request).await
    }

    async fn put_end(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        MasterServiceImpl::put_end_impl(self, request).await
    }

    async fn put_revoke(
        &self,
        request: Request<proto::PutRevokeRequest>,
    ) -> Result<Response<proto::PutRevokeResponse>, Status> {
        MasterServiceImpl::put_revoke_impl(self, request).await
    }

    async fn add_replica(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        MasterServiceImpl::add_replica_impl(self, request).await
    }

    async fn get_replica_list(
        &self,
        request: Request<proto::GetReplicaListRequest>,
    ) -> Result<Response<proto::GetReplicaListResponse>, Status> {
        MasterServiceImpl::get_replica_list_impl(self, request).await
    }

    async fn remove(
        &self,
        request: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        MasterServiceImpl::remove_impl(self, request).await
    }

    async fn remove_by_regex(
        &self,
        request: Request<proto::RemoveByRegexRequest>,
    ) -> Result<Response<proto::RemoveByRegexResponse>, Status> {
        MasterServiceImpl::remove_by_regex_impl(self, request).await
    }

    async fn remove_all(
        &self,
        request: Request<proto::RemoveAllRequest>,
    ) -> Result<Response<proto::RemoveAllResponse>, Status> {
        MasterServiceImpl::remove_all_impl(self, request).await
    }

    async fn batch_exist_key(
        &self,
        request: Request<proto::BatchExistKeyRequest>,
    ) -> Result<Response<proto::BatchExistKeyResponse>, Status> {
        MasterServiceImpl::batch_exist_key_impl(self, request).await
    }

    async fn batch_query_ip(
        &self,
        request: Request<proto::BatchQueryIpRequest>,
    ) -> Result<Response<proto::BatchQueryIpResponse>, Status> {
        MasterServiceImpl::batch_query_ip_impl(self, request).await
    }

    async fn batch_replica_clear(
        &self,
        request: Request<proto::BatchReplicaClearRequest>,
    ) -> Result<Response<proto::BatchReplicaClearResponse>, Status> {
        MasterServiceImpl::batch_replica_clear_impl(self, request).await
    }

    async fn query_by_regex(
        &self,
        request: Request<proto::QueryByRegexRequest>,
    ) -> Result<Response<proto::QueryByRegexResponse>, Status> {
        MasterServiceImpl::query_by_regex_impl(self, request).await
    }

    async fn query_segments(
        &self,
        request: Request<proto::QuerySegmentsRequest>,
    ) -> Result<Response<proto::QuerySegmentsResponse>, Status> {
        MasterServiceImpl::query_segments_impl(self, request).await
    }

    async fn query_ip(
        &self,
        request: Request<proto::QueryIpRequest>,
    ) -> Result<Response<proto::QueryIpResponse>, Status> {
        MasterServiceImpl::query_ip_impl(self, request).await
    }

    async fn get_storage_config(
        &self,
        request: Request<proto::GetStorageConfigRequest>,
    ) -> Result<Response<proto::GetStorageConfigResponse>, Status> {
        MasterServiceImpl::get_storage_config_impl(self, request).await
    }

    async fn upsert(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        MasterServiceImpl::upsert_impl(self, request).await
    }

    async fn copy_start(
        &self,
        request: Request<proto::CopyStartRequest>,
    ) -> Result<Response<proto::CopyStartResponse>, Status> {
        MasterServiceImpl::copy_start_impl(self, request).await
    }

    async fn copy_end(
        &self,
        request: Request<proto::CopyEndRequest>,
    ) -> Result<Response<proto::CopyEndResponse>, Status> {
        MasterServiceImpl::copy_end_impl(self, request).await
    }

    async fn copy_revoke(
        &self,
        request: Request<proto::CopyRevokeRequest>,
    ) -> Result<Response<proto::CopyRevokeResponse>, Status> {
        MasterServiceImpl::copy_revoke_impl(self, request).await
    }

    async fn move_start(
        &self,
        request: Request<proto::MoveStartRequest>,
    ) -> Result<Response<proto::MoveStartResponse>, Status> {
        MasterServiceImpl::move_start_impl(self, request).await
    }

    async fn move_end(
        &self,
        request: Request<proto::MoveEndRequest>,
    ) -> Result<Response<proto::MoveEndResponse>, Status> {
        MasterServiceImpl::move_end_impl(self, request).await
    }

    async fn move_revoke(
        &self,
        request: Request<proto::MoveRevokeRequest>,
    ) -> Result<Response<proto::MoveRevokeResponse>, Status> {
        MasterServiceImpl::move_revoke_impl(self, request).await
    }

    async fn create_copy_task(
        &self,
        request: Request<proto::CreateCopyTaskRequest>,
    ) -> Result<Response<proto::CreateCopyTaskResponse>, Status> {
        MasterServiceImpl::create_copy_task_impl(self, request).await
    }

    async fn create_move_task(
        &self,
        request: Request<proto::CreateMoveTaskRequest>,
    ) -> Result<Response<proto::CreateMoveTaskResponse>, Status> {
        MasterServiceImpl::create_move_task_impl(self, request).await
    }

    async fn query_task(
        &self,
        request: Request<proto::QueryTaskRequest>,
    ) -> Result<Response<proto::QueryTaskResponse>, Status> {
        MasterServiceImpl::query_task_impl(self, request).await
    }

    async fn fetch_tasks(
        &self,
        request: Request<proto::FetchTasksRequest>,
    ) -> Result<Response<proto::FetchTasksResponse>, Status> {
        MasterServiceImpl::fetch_tasks_impl(self, request).await
    }

    async fn mark_task_to_complete(
        &self,
        request: Request<proto::MarkTaskToCompleteRequest>,
    ) -> Result<Response<proto::MarkTaskToCompleteResponse>, Status> {
        MasterServiceImpl::mark_task_to_complete_impl(self, request).await
    }

    async fn batch_put_end(
        &self,
        request: Request<proto::BatchPutEndRequest>,
    ) -> Result<Response<proto::BatchPutEndResponse>, Status> {
        MasterServiceImpl::batch_put_end_impl(self, request).await
    }

    async fn batch_put_revoke(
        &self,
        request: Request<proto::BatchPutRevokeRequest>,
    ) -> Result<Response<proto::BatchPutRevokeResponse>, Status> {
        MasterServiceImpl::batch_put_revoke_impl(self, request).await
    }

    async fn batch_remove(
        &self,
        request: Request<proto::BatchRemoveRequest>,
    ) -> Result<Response<proto::BatchRemoveResponse>, Status> {
        MasterServiceImpl::batch_remove_impl(self, request).await
    }

    async fn batch_upsert_end(
        &self,
        request: Request<proto::BatchUpsertEndRequest>,
    ) -> Result<Response<proto::BatchUpsertEndResponse>, Status> {
        MasterServiceImpl::batch_upsert_end_impl(self, request).await
    }
}
