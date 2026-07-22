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
