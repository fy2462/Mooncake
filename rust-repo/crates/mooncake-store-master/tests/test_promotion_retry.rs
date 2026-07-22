mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::Request;
use uuid::Uuid;

async fn mount_local_disk_and_notify_success(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    key: &str,
) {
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: 128,
                transport_endpoint: "holder".into(),
            }],
            tasks: vec![],
        }),
    )
    .await
    .unwrap();
}

async fn trigger_promotion_lookup(service: &MasterServiceImpl, key: &str) {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn promotion_heartbeat(service: &MasterServiceImpl, holder_id: Uuid) -> Vec<String> {
    MasterService::promotion_object_heartbeat(
        service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
    .into_iter()
    .map(|task| task.key)
    .collect()
}

#[test]
fn promotion_candidate_state_starts_empty() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        ..Default::default()
    });

    assert_eq!(service.promotion_candidate_count_for_test(), 0);
}

#[tokio::test]
async fn promotion_rejection_records_candidate_at_watermark() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        eviction_high_watermark_ratio: 0.0,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk_and_notify_success(&service, holder_id, "watermark-hot").await;

    trigger_promotion_lookup(&service, "watermark-hot").await;
    trigger_promotion_lookup(&service, "watermark-hot").await;

    assert_eq!(service.promotion_candidate_count_for_test(), 1);
}

#[tokio::test]
async fn promotion_rejection_records_candidate_at_queue_cap() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 0,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk_and_notify_success(&service, holder_id, "queue-hot").await;

    trigger_promotion_lookup(&service, "queue-hot").await;
    trigger_promotion_lookup(&service, "queue-hot").await;

    assert_eq!(service.promotion_candidate_count_for_test(), 1);
}

#[tokio::test]
async fn due_candidate_is_queued_after_queue_capacity_recovers() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk_and_notify_success(&service, holder_id, "queue-first").await;
    mount_local_disk_and_notify_success(&service, holder_id, "queue-retry").await;

    for key in ["queue-first", "queue-retry"] {
        trigger_promotion_lookup(&service, key).await;
        trigger_promotion_lookup(&service, key).await;
    }
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    let first = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(first, vec!["queue-first"]);
    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "queue-first".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    service.make_promotion_candidates_due_for_test();
    service.run_promotion_candidate_retry_for_test();

    assert_eq!(service.promotion_candidate_count_for_test(), 0);
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["queue-retry"]
    );
}

#[tokio::test]
async fn retry_candidates_expire_by_age_or_retry_budget() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 0,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk_and_notify_success(&service, holder_id, "expired").await;
    for _ in 0..2 {
        trigger_promotion_lookup(&service, "expired").await;
    }
    service.age_promotion_candidates_for_test(std::time::Duration::from_secs(61));
    service.run_promotion_candidate_retry_for_test();
    assert_eq!(service.promotion_candidate_count_for_test(), 0);

    mount_local_disk_and_notify_success(&service, holder_id, "exhausted").await;
    for _ in 0..2 {
        trigger_promotion_lookup(&service, "exhausted").await;
    }
    service.set_promotion_candidate_retry_count_for_test("exhausted", "", 7);
    service.make_promotion_candidates_due_for_test();
    service.run_promotion_candidate_retry_for_test();
    assert_eq!(service.promotion_candidate_count_for_test(), 0);
}
