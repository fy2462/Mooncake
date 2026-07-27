mod common;
use common::proto_uuid;
use mooncake_store_master::{
    MasterRuntimeConfig, MasterServiceImpl, proto, proto::master_service_server::MasterService,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

async fn mount_memory_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) -> Uuid {
    static NEXT_BASE: AtomicU64 = AtomicU64::new(0x1_0000_0000);
    let response = MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 4096,
            base_addr: NEXT_BASE.fetch_add(0x1_0000, Ordering::Relaxed),
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let segment_id = response.segment_id.expect("mount must return segment id");
    Uuid::from_u64_pair(segment_id.high, segment_id.low)
}

async fn mount_nof_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) -> Uuid {
    let segment_id = Uuid::new_v4();
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(segment_id)),
                name: segment_name.into(),
                base: 0,
                size: 4096,
                te_endpoint: format!("nof://{segment_name}"),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap();
    segment_id
}

async fn put_complete_on_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    segment_name: &str,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: segment_name.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn create_drain_job(service: &MasterServiceImpl, source: &str, targets: &[&str]) -> Uuid {
    let response = MasterService::create_drain_job(
        service,
        Request::new(proto::CreateDrainJobRequest {
            segments: vec![source.into()],
            target_segments: targets.iter().map(|target| (*target).into()).collect(),
            max_concurrency: 1,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let job_id = response.job_id.unwrap();
    Uuid::from_u64_pair(job_id.high, job_id.low)
}

async fn query_drain_job(
    service: &MasterServiceImpl,
    job_id: Uuid,
) -> proto::QueryDrainJobResponse {
    MasterService::query_drain_job(
        service,
        Request::new(proto::QueryDrainJobRequest {
            job_id: Some(proto_uuid(job_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner()
}

async fn query_segment_status(service: &MasterServiceImpl, segment_name: &str) -> i32 {
    MasterService::query_segment_status(
        service,
        Request::new(proto::QuerySegmentStatusRequest {
            segment_name: segment_name.into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
}

async fn report_drain_task(service: &MasterServiceImpl, job_id: Uuid, status: proto::TaskStatus) {
    let task = service
        .drain_task_for_test(job_id)
        .expect("drain task must exist");
    MasterService::mark_task_to_complete(
        service,
        Request::new(proto::MarkTaskToCompleteRequest {
            client_id: Some(proto_uuid(
                task.info
                    .assigned_client
                    .expect("drain task must have an assigned client"),
            )),
            request: Some(proto::TaskCompleteRequest {
                id: Some(proto_uuid(task.info.id)),
                status: status as i32,
                message: "test completion".into(),
            }),
        }),
    )
    .await
    .unwrap();
}

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
                host_id: String::new(),
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
                host_id: String::new(),
            },
        ))
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
                    host_id: String::new(),
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
                    host_id: String::new(),
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

    let duplicate_target_job = service
        .create_drain_job(Request::new(
            mooncake_store_master::proto::CreateDrainJobRequest {
                segments: vec!["source-a:1".into()],
                target_segments: vec!["target-a:1".into(), "target-a:1".into()],
                max_concurrency: 1,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .job_id
        .unwrap();
    service
        .cancel_drain_job(Request::new(proto::CancelDrainJobRequest {
            job_id: Some(duplicate_target_job),
        }))
        .await
        .unwrap();

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
async fn test_cancel_drain_job_rejects_active_move_tasks() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "cancel-source:1").await;
    mount_memory_segment(&service, client_id, "cancel-target:1").await;
    put_complete_on_segment(&service, client_id, "cancel-key", "cancel-source:1").await;

    let job_id = create_drain_job(&service, "cancel-source:1", &["cancel-target:1"]).await;
    assert_eq!(query_drain_job(&service, job_id).await.active_units, 1);

    let error = MasterService::cancel_drain_job(
        &service,
        Request::new(proto::CancelDrainJobRequest {
            job_id: Some(proto_uuid(job_id)),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        query_segment_status(&service, "cancel-source:1").await,
        proto::SegmentStatus::Draining as i32
    );
}

#[tokio::test]
async fn test_concurrent_drain_creation_cannot_claim_the_same_source_twice() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "shared-source:1").await;
    mount_memory_segment(&service, client_id, "shared-target:1").await;
    let request = || {
        Request::new(proto::CreateDrainJobRequest {
            segments: vec!["shared-source:1".into()],
            target_segments: vec!["shared-target:1".into()],
            max_concurrency: 1,
        })
    };

    let (first, second) = tokio::join!(
        MasterService::create_drain_job(&service, request()),
        MasterService::create_drain_job(&service, request())
    );

    assert_ne!(first.is_ok(), second.is_ok());
    let error = first.err().or_else(|| second.err()).unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
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
                host_id: String::new(),
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

#[tokio::test]
async fn test_drain_without_eligible_target_remains_running() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "blocked-source:1").await;
    put_complete_on_segment(&service, client_id, "blocked-key", "blocked-source:1").await;

    let job_id = create_drain_job(&service, "blocked-source:1", &[]).await;
    let initial = query_drain_job(&service, job_id).await;
    assert_eq!(initial.status, proto::JobStatus::Running as i32);
    assert_eq!(initial.active_units, 0);
    assert_eq!(initial.blocked_units, 1);

    service.process_drain_jobs_once_for_test();
    let refreshed = query_drain_job(&service, job_id).await;
    assert_eq!(refreshed.status, proto::JobStatus::Running as i32);
    assert_eq!(
        query_segment_status(&service, "blocked-source:1").await,
        proto::SegmentStatus::Draining as i32
    );
}

#[tokio::test]
async fn test_drain_with_allocating_source_replica_cannot_report_success() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "allocating-source:1").await;
    mount_memory_segment(&service, client_id, "allocating-target:1").await;
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "allocating-key".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "allocating-source:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();

    let job_id = create_drain_job(&service, "allocating-source:1", &["allocating-target:1"]).await;
    service.process_drain_jobs_once_for_test();

    let job = query_drain_job(&service, job_id).await;
    assert_eq!(job.status, proto::JobStatus::Running as i32);
    assert_eq!(job.blocked_units, 1);
    assert_eq!(
        query_segment_status(&service, "allocating-source:1").await,
        proto::SegmentStatus::Draining as i32
    );
}

#[tokio::test]
async fn test_drain_terminal_failure_restores_source_to_allocation_pool() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "failed-source:1").await;
    mount_memory_segment(&service, client_id, "failed-target:1").await;
    put_complete_on_segment(&service, client_id, "failed-key", "failed-source:1").await;

    let job_id = create_drain_job(&service, "failed-source:1", &["failed-target:1"]).await;
    for attempt in 1..=3 {
        report_drain_task(&service, job_id, proto::TaskStatus::TaskFailed).await;
        service.process_drain_jobs_once_for_test();
        let job = query_drain_job(&service, job_id).await;
        assert_eq!(job.failed_units, attempt);
        if attempt < 3 {
            assert_eq!(job.status, proto::JobStatus::Running as i32);
            assert_eq!(job.active_units, 1);
        } else {
            assert_eq!(job.status, proto::JobStatus::Failed as i32);
            assert_eq!(job.active_units, 0);
            assert_eq!(job.message, "Drain job failed: unrecoverable units remain");
        }
    }

    assert_eq!(
        query_segment_status(&service, "failed-source:1").await,
        proto::SegmentStatus::Active as i32
    );
    let retry_job = create_drain_job(&service, "failed-source:1", &["failed-target:1"]).await;
    assert_ne!(retry_job, job_id);
    assert_eq!(
        query_drain_job(&service, retry_job).await.status,
        proto::JobStatus::Running as i32
    );
}

#[tokio::test]
async fn test_nof_drain_retry_stays_bound_to_exact_source_owner() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        enable_nof: true,
        ..Default::default()
    });
    let nof_owner = Uuid::new_v4();
    let target_owner = Uuid::new_v4();
    mount_nof_segment(&service, nof_owner, "nof-drain-source:1").await;
    mount_memory_segment(&service, target_owner, "nof-drain-target:1").await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(nof_owner)),
            key: "nof-drain-key".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 0,
                nof_replica_num: 1,
                preferred_nof_segments: vec!["nof-drain-source:1".into()],
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(nof_owner)),
            key: "nof-drain-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::NofSsd as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let job_id = create_drain_job(&service, "nof-drain-source:1", &["nof-drain-target:1"]).await;
    let first = service
        .drain_task_for_test(job_id)
        .expect("NoF drain must schedule its first task");
    assert_eq!(first.info.assigned_client, Some(nof_owner));

    report_drain_task(&service, job_id, proto::TaskStatus::TaskFailed).await;
    service.process_drain_jobs_once_for_test();

    let retried = service
        .drain_task_for_test(job_id)
        .expect("background Drain must reschedule the failed NoF unit");
    assert_eq!(retried.info.assigned_client, Some(nof_owner));
    let job = query_drain_job(&service, job_id).await;
    assert_eq!(job.status, proto::JobStatus::Running as i32);
    assert_eq!(job.failed_units, 1);
    assert_eq!(job.active_units, 1);

    for _ in 2..=3 {
        report_drain_task(&service, job_id, proto::TaskStatus::TaskFailed).await;
        service.process_drain_jobs_once_for_test();
    }
    let terminal = query_drain_job(&service, job_id).await;
    assert_eq!(terminal.status, proto::JobStatus::Failed as i32);
    assert_eq!(terminal.failed_units, 3);
    assert_eq!(terminal.active_units, 0);
    assert_eq!(
        query_segment_status(&service, "nof-drain-source:1").await,
        proto::SegmentStatus::Active as i32
    );
}

#[tokio::test]
async fn test_successful_drain_marks_source_unavailable() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        put_start_release_timeout: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "success-source:1").await;
    mount_memory_segment(&service, client_id, "success-target:1").await;
    put_complete_on_segment(&service, client_id, "success-key", "success-source:1").await;

    let job_id = create_drain_job(&service, "success-source:1", &["success-target:1"]).await;
    MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "success-key".into(),
            source: "success-source:1".into(),
            target: "success-target:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "success-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    report_drain_task(&service, job_id, proto::TaskStatus::TaskSuccess).await;
    service.process_drain_jobs_once_for_test();

    let job = query_drain_job(&service, job_id).await;
    assert_eq!(job.status, proto::JobStatus::Succeeded as i32);
    assert_eq!(job.succeeded_units, 1);
    assert_eq!(
        query_segment_status(&service, "success-source:1").await,
        proto::SegmentStatus::Unavailable as i32
    );
}

#[tokio::test]
async fn test_drain_move_keeps_exact_target_after_same_name_mount() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let source_owner = Uuid::new_v4();
    let other_owner = Uuid::new_v4();
    mount_memory_segment(&service, source_owner, "identity-source:1").await;
    let scheduled_target = mount_memory_segment(&service, source_owner, "identity-target:1").await;
    put_complete_on_segment(&service, source_owner, "identity-key", "identity-source:1").await;

    let _job_id = create_drain_job(&service, "identity-source:1", &["identity-target:1"]).await;
    let same_name_peer = mount_memory_segment(&service, other_owner, "identity-target:1").await;
    assert_ne!(scheduled_target, same_name_peer);

    let response = MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(source_owner)),
            key: "identity-key".into(),
            source: "identity-source:1".into(),
            target: "identity-target:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let target_id = response
        .target
        .and_then(|target| target.segment_id)
        .expect("MoveStart must return exact target identity");

    assert_eq!(
        Uuid::from_u64_pair(target_id.high, target_id.low),
        scheduled_target
    );
}

#[tokio::test]
async fn test_finished_task_reaper_waits_for_drain_to_consume_terminal_status() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        put_start_release_timeout: Duration::ZERO,
        max_total_finished_tasks: 0,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "reaper-source:1").await;
    mount_memory_segment(&service, client_id, "reaper-target:1").await;
    put_complete_on_segment(&service, client_id, "reaper-key", "reaper-source:1").await;

    let job_id = create_drain_job(&service, "reaper-source:1", &["reaper-target:1"]).await;
    let task_id = service
        .drain_task_for_test(job_id)
        .expect("drain task must be scheduled")
        .info
        .id;
    MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "reaper-key".into(),
            source: "reaper-source:1".into(),
            target: "reaper-target:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "reaper-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    report_drain_task(&service, job_id, proto::TaskStatus::TaskSuccess).await;

    service.reap_expired_background_tasks_for_test();
    MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(proto_uuid(task_id)),
        }),
    )
    .await
    .expect("active Drain must retain its terminal task for status consumption");

    service.process_drain_jobs_once_for_test();
    let job = query_drain_job(&service, job_id).await;
    assert_eq!(job.status, proto::JobStatus::Succeeded as i32);
    assert_eq!(job.succeeded_units, 1);

    service.reap_expired_background_tasks_for_test();
    let error = MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(proto_uuid(task_id)),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_new_allocations_exclude_draining_segments() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "excluded-source:1").await;
    mount_memory_segment(&service, client_id, "eligible-target:1").await;

    let job_id = create_drain_job(&service, "excluded-source:1", &["eligible-target:1"]).await;
    assert_eq!(
        query_drain_job(&service, job_id).await.status,
        proto::JobStatus::Running as i32
    );

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "new-during-drain".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "excluded-source:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(put.replicas.len(), 1);
    assert_eq!(put.replicas[0].segment_name, "eligible-target:1");
}
