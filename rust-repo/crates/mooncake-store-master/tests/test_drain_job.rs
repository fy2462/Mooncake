mod common;
use common::proto_uuid;
use mooncake_store_master::{proto::master_service_server::MasterService, MasterServiceImpl};
use tonic::Request;

#[tokio::test]
async fn test_create_and_query_drain_job() {
    let service = MasterServiceImpl::default();

    // Mount two segments to drain from and one target
    let client_id = uuid::Uuid::new_v4();
    service
        .mount_segment(Request::new(
            mooncake_store_master::proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: "host1:12345".into(),
                size: 4096,
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
            },
        ))
        .await
        .unwrap();
    service
        .mount_segment(Request::new(
            mooncake_store_master::proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: "host2:12345".into(),
                size: 4096,
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
            },
        ))
        .await
        .unwrap();

    // Create put_start objects on host1 to have something to drain
    service
        .put_start(Request::new(
            mooncake_store_master::proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "drain_key".into(),
                slice_length: 256,
                tenant_id: String::new(),
                config: Some(mooncake_store_master::proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "host1:12345".into(),
                    ..Default::default()
                }),
            },
        ))
        .await
        .unwrap();
    // Complete the put so the replica is COMPLETE
    service
        .put_end(Request::new(mooncake_store_master::proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "drain_key".into(),
            replica_type: mooncake_store_master::proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: String::new(),
        }))
        .await
        .unwrap();

    // Create drain job
    let create_resp = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["host1:12345".into()],
                target_segments: vec!["host2:12345".into()],
                max_concurrency: 1,
            },
        ))
        .await
        .unwrap();
    let create_resp = create_resp.into_inner();
    let job_id = create_resp.job_id.unwrap();
    let job_uuid = uuid::Uuid::from_u64_pair(job_id.high, job_id.low);

    // Query the drain job
    let query_resp = service
        .query_drain_job(Request::new(
            mooncake_store_master::proto::QueryDrainJobRequest {
                job_id: Some(job_id.clone()),
            },
        ))
        .await
        .unwrap();
    let query_resp = query_resp.into_inner();

    assert_eq!(query_resp.id.unwrap().high, job_id.high);
    assert_eq!(query_resp.segments.len(), 1);
    assert_eq!(query_resp.segments[0], "host1:12345");

    // Verify source segment status is DRAINING
    let seg_status = service
        .query_segment_status(Request::new(
            mooncake_store_master::proto::QuerySegmentStatusRequest {
                segment_name: "host1:12345".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(seg_status.status, 2i32); // SegmentStatus::Draining

    // Cancel the drain job
    service
        .cancel_drain_job(Request::new(
            mooncake_store_master::proto::CancelDrainJobRequest {
                job_id: Some(proto_uuid(job_uuid)),
            },
        ))
        .await
        .unwrap();

    // After cancel, query sees CANCELED
    let query2 = service
        .query_drain_job(Request::new(
            mooncake_store_master::proto::QueryDrainJobRequest {
                job_id: Some(job_id),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(query2.status, 5i32); // JobStatus::Canceled

    // Segment restored to ACTIVE
    let seg_status2 = service
        .query_segment_status(Request::new(
            mooncake_store_master::proto::QuerySegmentStatusRequest {
                segment_name: "host1:12345".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(seg_status2.status, 1i32); // SegmentStatus::Active
}

#[tokio::test]
async fn test_drain_job_rejects_empty_segments() {
    let service = MasterServiceImpl::default();
    let err = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec![],
                target_segments: vec!["target:1".into()],
                max_concurrency: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_drain_job_schedules_one_unit_per_draining_source_replica() {
    let service = MasterServiceImpl::default();
    let client_id = uuid::Uuid::new_v4();
    for name in ["source-a:1", "source-b:1", "target-a:1"] {
        service
            .mount_segment(Request::new(
                mooncake_store_master::proto::MountSegmentRequest {
                    client_id: Some(proto_uuid(client_id)),
                    segment_name: name.into(),
                    size: 4096,
                    base_addr: 0x100000000,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                },
            ))
            .await
            .unwrap();
    }

    service
        .put_start(Request::new(
            mooncake_store_master::proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "multi-source-drain".into(),
                slice_length: 256,
                tenant_id: String::new(),
                config: Some(mooncake_store_master::proto::ReplicateConfig {
                    replica_num: 2,
                    preferred_segments: vec!["source-a:1".into(), "source-b:1".into()],
                    ..Default::default()
                }),
            },
        ))
        .await
        .unwrap();
    service
        .put_end(Request::new(mooncake_store_master::proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "multi-source-drain".into(),
            replica_type: mooncake_store_master::proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: String::new(),
        }))
        .await
        .unwrap();

    let create_resp = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["source-a:1".into(), "source-b:1".into()],
                target_segments: vec!["target-a:1".into()],
                max_concurrency: 2,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let job_id = create_resp.job_id.unwrap();

    let query_resp = service
        .query_drain_job(Request::new(
            mooncake_store_master::proto::QueryDrainJobRequest {
                job_id: Some(job_id),
            },
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(query_resp.active_units, 2);
}

#[tokio::test]
async fn test_drain_job_rejects_invalid_concurrency_duplicates_and_overlap() {
    let service = MasterServiceImpl::default();
    let client_id = uuid::Uuid::new_v4();
    for name in ["source-a:1", "target-a:1"] {
        service
            .mount_segment(Request::new(
                mooncake_store_master::proto::MountSegmentRequest {
                    client_id: Some(proto_uuid(client_id)),
                    segment_name: name.into(),
                    size: 4096,
                    base_addr: 0x100000000,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                },
            ))
            .await
            .unwrap();
    }

    let zero_concurrency = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["source-a:1".into()],
                target_segments: vec!["target-a:1".into()],
                max_concurrency: 0,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(zero_concurrency.code(), tonic::Code::InvalidArgument);

    let duplicate_source = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["source-a:1".into(), "source-a:1".into()],
                target_segments: vec!["target-a:1".into()],
                max_concurrency: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(duplicate_source.code(), tonic::Code::InvalidArgument);

    let overlap = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["source-a:1".into()],
                target_segments: vec!["source-a:1".into()],
                max_concurrency: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(overlap.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_drain_job_rejects_unknown_segment() {
    let service = MasterServiceImpl::default();
    let err = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["nonexistent:1".into()],
                target_segments: vec!["target:1".into()],
                max_concurrency: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_cancel_drain_job_not_found() {
    let service = MasterServiceImpl::default();
    let fake_id = uuid::Uuid::new_v4();
    let err = service
        .cancel_drain_job(Request::new(
            mooncake_store_master::proto::CancelDrainJobRequest {
                job_id: Some(proto_uuid(fake_id)),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_segment_status_returns_active_after_mount() {
    let service = MasterServiceImpl::default();
    let client_id = uuid::Uuid::new_v4();
    service
        .mount_segment(Request::new(
            mooncake_store_master::proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: "mynode:9999".into(),
                size: 4096,
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
            },
        ))
        .await
        .unwrap();

    let resp = service
        .query_segment_status(Request::new(
            mooncake_store_master::proto::QuerySegmentStatusRequest {
                segment_name: "mynode:9999".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, 1i32); // SegmentStatus::Active

    // Query by name should be not found for unknown
    let err = service
        .query_segment_status(Request::new(
            mooncake_store_master::proto::QuerySegmentStatusRequest {
                segment_name: "ghost:1".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}
