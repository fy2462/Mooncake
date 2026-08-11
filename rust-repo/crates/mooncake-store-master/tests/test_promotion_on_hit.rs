mod common;

use common::proto_uuid;
use mooncake_store_master::metrics;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{Code, Request};
use uuid::Uuid;

/// Serializes every test in this binary: promotion metrics are process-global
/// Prometheus statics, and each test drives admission/completion paths, so
/// delta assertions require exclusive access across the awaited RPC calls.
static METRICS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static NEXT_SEGMENT_BASE: AtomicU64 = AtomicU64::new(0x1_0000_0000);

fn runtime_config(promotion_on_hit: bool, admission_threshold: u8) -> MasterRuntimeConfig {
    MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit,
        promotion_admission_threshold: admission_threshold,
        ..Default::default()
    }
}

async fn mount_local_disk(service: &MasterServiceImpl, holder_id: Uuid) {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: false,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: false,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
}

async fn mount_memory_segment(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    name: &str,
) -> mooncake_store_master::proto::Uuid {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            segment_name: name.into(),
            size: 4096,
            base_addr: NEXT_SEGMENT_BASE.fetch_add(0x1_0000, Ordering::Relaxed),
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .segment_id
    .unwrap()
}

async fn put_memory_object(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    segment_name: &str,
    key: &str,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: key.into(),
            slice_length: 1024,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: segment_name.into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

/// Seed a LOCAL_DISK-only object on `holder_id`'s storage namespace, mirroring
/// the C++ `InjectLocalDiskReplica` helper.
async fn seed_local_disk_object(service: &MasterServiceImpl, holder_id: Uuid, key: &str) {
    let segment_name = format!("promotion-on-hit-source-{key}");
    let segment_id = mount_memory_segment(service, holder_id, &segment_name).await;
    put_memory_object(service, holder_id, &segment_name, key).await;
    let heartbeat = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let task = heartbeat
        .tasks
        .into_iter()
        .find(|task| task.key == key)
        .expect("Master must issue the promotion fixture offload task");
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: 1024,
                transport_endpoint: "holder".into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    MasterService::unmount_segment(
        service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(segment_id),
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap();
}

async fn seed_local_disk_object_for_tenant(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    tenant_id: &str,
    key: &str,
) {
    let segment_name = format!("promotion-on-hit-tenant-{key}");
    let segment_id = mount_memory_segment(service, holder_id, &segment_name).await;
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: key.into(),
            slice_length: 1024,
            tenant_id: tenant_id.into(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: segment_name.clone(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();
    let heartbeat = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let task = heartbeat
        .tasks
        .into_iter()
        .find(|task| task.tenant_id == tenant_id && task.key == key)
        .expect("Master must issue the tenant promotion fixture offload task");
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: 1024,
                transport_endpoint: "holder".into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    MasterService::unmount_segment(
        service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(segment_id),
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap();
}

fn temp_policy_uri() -> String {
    tempfile::NamedTempFile::new()
        .unwrap()
        .path()
        .to_string_lossy()
        .into_owned()
}

/// Seed an object that keeps both a complete MEMORY replica and a LOCAL_DISK
/// replica (the C++ `PutObject` + `InjectLocalDiskReplica` shape).
async fn seed_dual_replica_object(service: &MasterServiceImpl, holder_id: Uuid, key: &str) {
    let segment_name = format!("promotion-on-hit-dual-{key}");
    mount_memory_segment(service, holder_id, &segment_name).await;
    put_memory_object(service, holder_id, &segment_name, key).await;
    let heartbeat = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let task = heartbeat
        .tasks
        .into_iter()
        .find(|task| task.key == key)
        .expect("Master must issue the promotion fixture offload task");
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: 1024,
                transport_endpoint: "holder".into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    // Keep the memory segment mounted so the object holds both replica kinds.
}

async fn trigger_lookup(service: &MasterServiceImpl, key: &str) {
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

// PromotionOnHitTest.DefaultOffNoPromotion: with promotion_on_hit=false,
// repeated reads never enqueue a promotion task.
#[tokio::test]
async fn cpp_parity_promotion_default_off_no_promotion() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(false, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k1").await;

    for _ in 0..5 {
        trigger_lookup(&service, "k1").await;
    }
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());
}

// PromotionOnHitTest.NoLocalDiskNoPromotion: a MEMORY-only object is never
// promoted regardless of read count.
#[tokio::test]
async fn cpp_parity_promotion_no_local_disk_no_promotion() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    mount_memory_segment(&service, holder_id, "mem-only").await;
    put_memory_object(&service, holder_id, "mem-only", "k_mem_only").await;

    for _ in 0..5 {
        trigger_lookup(&service, "k_mem_only").await;
    }
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());
}

// PromotionOnHitTest.MemoryReplicaPresentNoPromotion: a key with both a
// complete MEMORY and a LOCAL_DISK replica is not promoted.
#[tokio::test]
async fn cpp_parity_promotion_memory_replica_present_no_promotion() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_dual_replica_object(&service, holder_id, "k_dual").await;

    for _ in 0..5 {
        trigger_lookup(&service, "k_dual").await;
    }
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());
}

// PromotionOnHitTest.HeartbeatReturnsErrorForUnknownClient: an unregistered
// client heartbeat fails with SEGMENT_NOT_FOUND (tonic NotFound).
#[tokio::test]
async fn cpp_parity_promotion_heartbeat_unknown_client_errors() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let error = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
}

// PromotionOnHitTest.AllocStartUnknownKey: PromotionAllocStart for a missing
// key reports OBJECT_NOT_FOUND (tonic NotFound).
#[tokio::test]
async fn cpp_parity_promotion_alloc_start_unknown_key_not_found() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let error = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "nonexistent".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
}

// PromotionOnHitTest.NotifyUnknownKey: NotifyPromotionSuccess for a missing
// key reports OBJECT_NOT_FOUND (tonic NotFound).
#[tokio::test]
async fn cpp_parity_promotion_notify_success_unknown_key_not_found() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let error = MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "nonexistent".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
}

// PromotionOnHitTest.RacingReadersDedup: many concurrent readers of one
// LOCAL_DISK-only key collapse into exactly one heartbeat task.
#[tokio::test]
async fn cpp_parity_promotion_racing_readers_dedup() {
    let _guard = METRICS_LOCK.lock().await;
    let service = Arc::new(MasterServiceImpl::with_runtime_config(runtime_config(
        true, 1,
    )));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;

    let mut readers = Vec::new();
    for _ in 0..32 {
        let service = service.clone();
        readers.push(tokio::spawn(async move {
            for _ in 0..10 {
                trigger_lookup(&service, "k_cold").await;
            }
        }));
    }
    for reader in readers {
        reader.await.unwrap();
    }

    let pending = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0], "k_cold");
}

async fn seed_many_local_disk_objects(service: &MasterServiceImpl, holder_id: Uuid, keys: &[&str]) {
    for key in keys {
        seed_local_disk_object(service, holder_id, key).await;
    }
}

// PromotionOnHitTest.MaxPerHeartbeatKnobControlsBatchSize: with the knob at
// three, five queued keys drain as 3, then 2, then 0.
#[tokio::test]
async fn cpp_parity_promotion_max_per_heartbeat_knob_controls_batch_size() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_max_per_heartbeat: 3,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    let keys = ["k_0", "k_1", "k_2", "k_3", "k_4"];
    seed_many_local_disk_objects(&service, holder_id, &keys).await;
    for key in keys {
        trigger_lookup(&service, key).await;
    }

    assert_eq!(promotion_heartbeat(&service, holder_id).await.len(), 3);
    assert_eq!(promotion_heartbeat(&service, holder_id).await.len(), 2);
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());
}

// PromotionOnHitTest.MaxPerHeartbeatZeroClampsToOne: a zero knob still
// delivers the single queued task (never silently disables delivery).
#[tokio::test]
async fn cpp_parity_promotion_max_per_heartbeat_zero_clamps_to_one() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_max_per_heartbeat: 0,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k1").await;
    trigger_lookup(&service, "k1").await;

    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k1");
}

// PromotionOnHitTest.HeartbeatBoundedBatchPreservesLeftovers: the default
// one-task-per-heartbeat bound drains three queued keys one per call and then
// returns empty, with every key still readable afterwards.
#[tokio::test]
async fn cpp_parity_promotion_heartbeat_bounded_batch_preserves_leftovers() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    let keys = ["hb_k1", "hb_k2", "hb_k3"];
    seed_many_local_disk_objects(&service, holder_id, &keys).await;
    for key in keys {
        trigger_lookup(&service, key).await;
    }

    let tick1 = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(tick1.len(), 1);
    assert!(keys.contains(&tick1[0].as_str()));
    let tick2 = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(tick2.len(), 1);
    assert_ne!(tick2[0], tick1[0]);
    let tick3 = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(tick3.len(), 1);
    assert_ne!(tick3[0], tick1[0]);
    assert_ne!(tick3[0], tick2[0]);
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());

    for key in keys {
        trigger_lookup(&service, key).await;
    }
}

// PromotionOnHitTest.AdmissionThresholdZeroClampsToOne: a zero threshold is
// clamped so the very first read admits a promotion.
#[tokio::test]
async fn cpp_parity_promotion_admission_threshold_zero_clamps_to_one() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 0));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_zero_threshold").await;

    trigger_lookup(&service, "k_zero_threshold").await;
    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k_zero_threshold");
}

// PromotionOnHitTest.AdmissionThresholdAboveMaxClampsToMax: the admission
// gate is unreachable above the CountMinSketch saturating max (255), so a
// threshold of 255 admits exactly on the 255th read and never before.
#[tokio::test]
async fn cpp_parity_promotion_admission_threshold_above_max_clamps_to_max() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 255));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_max_threshold").await;

    for _ in 0..254 {
        trigger_lookup(&service, "k_max_threshold").await;
    }
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());

    trigger_lookup(&service, "k_max_threshold").await;
    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k_max_threshold");
}

// PromotionOnHitTest.BatchGetReplicaListPromotesLocalDiskOnlyObject: a single
// get plus a client batch get of two LOCAL_DISK-only keys admit exactly two
// tasks, raise admitted/in-flight by two, and both keys appear in one
// heartbeat.
#[tokio::test]
async fn cpp_parity_promotion_batch_get_admits_local_disk_only_object() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_max_per_heartbeat: 2,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    let single_key = "k_single_get_promote";
    let batch_key = "k_batch_get_promote";
    seed_local_disk_object(&service, holder_id, single_key).await;
    seed_local_disk_object(&service, holder_id, batch_key).await;

    let admitted_pre = metrics::PROMOTION_ADMITTED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    let single = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: single_key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(single.replicas.len(), 1);

    let batch = MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec![batch_key.into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch.results.len(), 1);
    assert_eq!(
        batch.results[0].response.as_ref().unwrap().replicas.len(),
        1
    );

    assert_eq!(metrics::PROMOTION_ADMITTED.get() - admitted_pre, 2);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 2);

    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 2);
    assert!(heartbeat.iter().any(|key| key == single_key));
    assert!(heartbeat.iter().any(|key| key == batch_key));
}

// PromotionOnHitTest.BatchGetReplicaListForAdminDoesNotPromoteLocalDiskOnly
// Object: the read-only admin batch query returns the LocalDisk replica but
// admits nothing.
#[tokio::test]
async fn cpp_parity_promotion_batch_get_admin_does_not_promote() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_max_per_heartbeat: 2,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    let key = "k_batch_admin_no_promote";
    seed_local_disk_object(&service, holder_id, key).await;

    let admitted_pre = metrics::PROMOTION_ADMITTED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    let results = service.batch_replica_lists_for_admin_for_test(&[key.to_string()], "");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].response.as_ref().unwrap().replicas.len(), 1);

    assert_eq!(metrics::PROMOTION_ADMITTED.get() - admitted_pre, 0);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());
}

// PromotionOnHitTest.BatchGetReplicaListForAdminDoesNotUpdateCacheHitMetrics:
// an admin batch read leaves the memory cache-hit counter untouched, while the
// client-facing batch read bumps it.
#[tokio::test]
async fn cpp_parity_promotion_batch_get_admin_does_not_update_cache_hit_metrics() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    mount_memory_segment(&service, holder_id, "metrics-segment").await;
    put_memory_object(&service, holder_id, "metrics-segment", "k_admin_no_metric").await;

    let before_admin = metrics::MEM_CACHE_HITS.get();
    let results =
        service.batch_replica_lists_for_admin_for_test(&["k_admin_no_metric".to_string()], "");
    assert_eq!(results.len(), 1);
    assert!(results[0].response.is_some());
    assert_eq!(metrics::MEM_CACHE_HITS.get(), before_admin);

    MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec!["k_admin_no_metric".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(metrics::MEM_CACHE_HITS.get() > before_admin);
}

// PromotionOnHitTest.MetricsFunnelTracksSuccessfulPromotion: admission raises
// admitted and in-flight by one; a full AllocStart + NotifyPromotionSuccess
// raises completed and completed_bytes by the object size and returns
// in-flight to baseline.
#[tokio::test]
async fn cpp_parity_promotion_metrics_funnel_tracks_successful_promotion() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_hot").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;

    let admitted_pre = metrics::PROMOTION_ADMITTED.get();
    let completed_pre = metrics::PROMOTION_COMPLETED.get();
    let bytes_pre = metrics::PROMOTION_COMPLETED_BYTES.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    trigger_lookup(&service, "k_hot").await;
    assert_eq!(metrics::PROMOTION_ADMITTED.get() - admitted_pre, 1);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_hot".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_hot".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(metrics::PROMOTION_COMPLETED.get() - completed_pre, 1);
    assert_eq!(metrics::PROMOTION_COMPLETED_BYTES.get() - bytes_pre, 1024);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);
}

// PromotionOnHitTest.MetricsRejectionCountersIncrementOnGateMiss: the
// frequency, cap, and watermark gates each raise their own rejection counter
// exactly once.
#[tokio::test]
async fn cpp_parity_promotion_metrics_rejection_counters_increment_on_gate_miss() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 2,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_a").await;
    seed_local_disk_object(&service, holder_id, "k_b").await;

    let freq_pre = metrics::PROMOTION_REJECTED_FREQUENCY.get();
    let cap_pre = metrics::PROMOTION_REJECTED_CAP.get();
    let watermark_pre = metrics::PROMOTION_REJECTED_WATERMARK.get();

    // First read: frequency 1 < threshold 2.
    trigger_lookup(&service, "k_a").await;
    assert_eq!(metrics::PROMOTION_REJECTED_FREQUENCY.get() - freq_pre, 1);

    // Second read: admitted, in-flight reaches the queue limit of one.
    trigger_lookup(&service, "k_a").await;
    assert_eq!(metrics::PROMOTION_REJECTED_FREQUENCY.get() - freq_pre, 1);

    // Third read on a distinct key: frequency 1 < threshold 2 rejected.
    trigger_lookup(&service, "k_b").await;
    assert_eq!(metrics::PROMOTION_REJECTED_FREQUENCY.get() - freq_pre, 2);

    // Fourth read on the same key: passes frequency, cap gate rejects.
    trigger_lookup(&service, "k_b").await;
    assert_eq!(metrics::PROMOTION_REJECTED_CAP.get() - cap_pre, 1);

    // Watermark gate: force DRAM at/above the high watermark.
    let service = Arc::new(MasterServiceImpl::with_runtime_config(
        MasterRuntimeConfig {
            enable_offload: true,
            promotion_on_hit: true,
            promotion_admission_threshold: 1,
            eviction_high_watermark_ratio: 0.0,
            ..Default::default()
        },
    ));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_wm").await;
    trigger_lookup(&service, "k_wm").await;
    assert_eq!(
        metrics::PROMOTION_REJECTED_WATERMARK.get() - watermark_pre,
        1
    );
}

// PromotionOnHitTest.MetricsRemoveMidPromotionCountsAsCancelled: removing a
// key with an in-flight promotion task raises cancelled by one and returns
// in-flight to baseline.
#[tokio::test]
async fn cpp_parity_promotion_metrics_remove_mid_promotion_counts_as_cancelled() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_drop").await;

    let admitted_pre = metrics::PROMOTION_ADMITTED.get();
    let cancelled_pre = metrics::PROMOTION_CANCELLED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    trigger_lookup(&service, "k_drop").await;
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "k_drop".into(),
            tenant_id: String::new(),
            force: true,
        }),
    )
    .await
    .unwrap();

    assert_eq!(metrics::PROMOTION_ADMITTED.get() - admitted_pre, 1);
    assert_eq!(metrics::PROMOTION_CANCELLED.get() - cancelled_pre, 1);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);
}

// PromotionOnHitTest.QueueLimitRejectsBeyondCap: with a global queue limit of
// one, only the first admitted key is returned by heartbeat and the second is
// cap-rejected.
#[tokio::test]
async fn cpp_parity_promotion_queue_limit_rejects_beyond_cap() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_first").await;
    seed_local_disk_object(&service, holder_id, "k_second").await;

    let cap_pre = metrics::PROMOTION_REJECTED_CAP.get();

    trigger_lookup(&service, "k_first").await;
    trigger_lookup(&service, "k_second").await;

    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k_first");
    assert_eq!(metrics::PROMOTION_REJECTED_CAP.get() - cap_pre, 1);
}

// PromotionOnHitTest.NotifySuccessDecrementsCounter: completing the first task
// at queue limit one frees the global slot so a second key is admitted and
// returned by heartbeat.
#[tokio::test]
async fn cpp_parity_promotion_notify_success_decrements_counter() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_first").await;
    seed_local_disk_object(&service, holder_id, "k_second").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;

    trigger_lookup(&service, "k_first").await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_first".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_first".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    trigger_lookup(&service, "k_second").await;
    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k_second");
}

// PromotionOnHitTest.AllocStartRejectsNonHolder: an intruder AllocStart is
// rejected with INVALID_PARAMS (tonic PermissionDenied), stages no buffer, and
// leaves the task available for the legitimate holder.
#[tokio::test]
async fn cpp_parity_promotion_alloc_start_rejects_non_holder() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;
    trigger_lookup(&service, "k_cold").await;

    let error = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

// PromotionOnHitTest.AllocStartRejectsSizeMismatch: half and four-times size
// requests are rejected without consuming the task, and the exact-size request
// then succeeds.
#[tokio::test]
async fn cpp_parity_promotion_alloc_start_rejects_size_mismatch() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;
    trigger_lookup(&service, "k_cold").await;

    for bad_size in [512, 4096] {
        let error = MasterService::promotion_alloc_start(
            &service,
            Request::new(proto::PromotionAllocStartRequest {
                client_id: Some(proto_uuid(holder_id)),
                key: "k_cold".into(),
                size: bad_size,
                preferred_segments: vec![],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

// PromotionOnHitTest.NotifyRejectsNonHolder: an intruder NotifyPromotionSuccess
// is rejected without committing the staged replica, and the holder can still
// complete it.
#[tokio::test]
async fn cpp_parity_promotion_notify_rejects_non_holder() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;
    trigger_lookup(&service, "k_cold").await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let error = MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "k_cold".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);

    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

// PromotionOnHitTest.NotifyFailureRejectsNonHolder: an intruder
// NotifyPromotionFailure is rejected and the holder's subsequent failure
// cleanup still succeeds.
#[tokio::test]
async fn cpp_parity_promotion_notify_failure_rejects_non_holder() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;
    trigger_lookup(&service, "k_cold").await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let error = MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "k_cold".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);

    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

// PromotionOnHitTest.NotifyFailureReleasesStateImmediately: a holder failure
// after AllocStart bumps failed by one, returns in-flight to baseline, frees
// the queue-limit-one slot for a second key, and repeated failure
// notifications are idempotent.
#[tokio::test]
async fn cpp_parity_promotion_notify_failure_releases_state_immediately() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_a").await;
    seed_local_disk_object(&service, holder_id, "k_b").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;

    let failed_pre = metrics::PROMOTION_FAILED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    trigger_lookup(&service, "k_a").await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_a".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_a".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::PROMOTION_FAILED.get() - failed_pre, 1);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);

    // Repeated failure notification on the same key is idempotent.
    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_a".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::PROMOTION_FAILED.get() - failed_pre, 1);

    trigger_lookup(&service, "k_b").await;
    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k_b");
}

// PromotionOnHitTest.QueueLimitRejectsCrossShard: the in-flight cap is global
// in Rust (the C++ per-shard tenant maps share one cluster-wide counter), so
// distinct keys behave identically to the same-shard case: only the first
// admitted key is returned and the second is cap-rejected.
#[tokio::test]
async fn cpp_parity_promotion_queue_limit_rejects_cross_shard() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_alpha").await;
    seed_local_disk_object(&service, holder_id, "k_beta").await;

    let cap_pre = metrics::PROMOTION_REJECTED_CAP.get();
    trigger_lookup(&service, "k_alpha").await;
    trigger_lookup(&service, "k_beta").await;

    let heartbeat = promotion_heartbeat(&service, holder_id).await;
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0], "k_alpha");
    assert_eq!(metrics::PROMOTION_REJECTED_CAP.get() - cap_pre, 1);
}

// PromotionOnHitTest.AdmissionFrequencyIsTenantScoped: the promotion sketch
// keys by tenant-scoped key, so the same user key read once in each of two
// tenants queues nothing, while its second read in tenant A queues exactly one
// task marked tenant A.
#[tokio::test]
async fn cpp_parity_promotion_admission_frequency_is_tenant_scoped() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 2,
        tenant_quota_connector_uri: temp_policy_uri(),
        ..Default::default()
    });
    service
        .upsert_tenant_quota_policy("tenant-a", 64 * 1024 * 1024)
        .expect("tenant policy");
    service
        .upsert_tenant_quota_policy("tenant-b", 64 * 1024 * 1024)
        .expect("tenant policy");
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object_for_tenant(&service, holder_id, "tenant-a", "shared_hot_key").await;
    seed_local_disk_object_for_tenant(&service, holder_id, "tenant-b", "shared_hot_key").await;

    let get = |tenant: &str| {
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "shared_hot_key".into(),
                tenant_id: tenant.into(),
            }),
        )
    };
    get("tenant-a").await.unwrap();
    get("tenant-b").await.unwrap();
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());

    get("tenant-a").await.unwrap();
    let pending = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(pending.tasks.len(), 1);
    assert_eq!(pending.tasks[0].tenant_id, "tenant-a");
    assert_eq!(pending.tasks[0].key, "shared_hot_key");
}

// PromotionOnHitTest.MultiSegmentAllocPicksAvailableSegment: a LocalDisk-only
// holder with no DRAM promotes onto a separately mounted available DRAM
// segment.
#[tokio::test]
async fn cpp_parity_promotion_multi_segment_alloc_picks_available_segment() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_hot").await;
    let dram_client_id = Uuid::new_v4();
    let available_segment_id =
        mount_memory_segment(&service, dram_client_id, "available-dram").await;
    trigger_lookup(&service, "k_hot").await;

    let response = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_hot".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let descriptor = response.memory_descriptor.expect("staged descriptor");
    assert_eq!(descriptor.segment_id, Some(available_segment_id));
}

// PromotionOnHitTest.MultiSegmentAllocRespectsPreferred: when two DRAM
// segments are available, AllocStart honors the supplied preferred segment.
#[tokio::test]
async fn cpp_parity_promotion_multi_segment_alloc_respects_preferred() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(runtime_config(true, 1));
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_hot").await;
    let dram_client_id = Uuid::new_v4();
    mount_memory_segment(&service, dram_client_id, "dram-a").await;
    let preferred_segment_id = mount_memory_segment(&service, dram_client_id, "dram-b").await;
    trigger_lookup(&service, "k_hot").await;

    let response = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_hot".into(),
            size: 1024,
            preferred_segments: vec!["dram-b".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let descriptor = response.memory_descriptor.expect("staged descriptor");
    assert_eq!(descriptor.segment_id, Some(preferred_segment_id));
}

async fn remove_key(service: &MasterServiceImpl, key: &str) {
    MasterService::remove(
        service,
        Request::new(proto::RemoveRequest {
            key: key.into(),
            tenant_id: String::new(),
            force: true,
        }),
    )
    .await
    .unwrap();
}

// PromotionOnHitTest.RemoveErasesPromotionTask: force Remove of an in-flight
// key frees the queue-limit-one slot so a second key is admitted immediately.
#[tokio::test]
async fn cpp_parity_promotion_remove_erases_promotion_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_first").await;
    seed_local_disk_object(&service, holder_id, "k_second").await;

    trigger_lookup(&service, "k_first").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_first"]
    );

    remove_key(&service, "k_first").await;
    trigger_lookup(&service, "k_second").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_second"]
    );
}

// PromotionOnHitTest.RemoveByRegexErasesPromotionTask: RemoveByRegex matching
// exactly one in-flight key frees the queue-limit-one slot for another key.
#[tokio::test]
async fn cpp_parity_promotion_remove_by_regex_erases_promotion_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "regex_k1").await;
    seed_local_disk_object(&service, holder_id, "other_k2").await;

    trigger_lookup(&service, "regex_k1").await;
    let response = MasterService::remove_by_regex(
        &service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: "^regex_".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.removed_count, 1);

    // The C++ RemoveByRegex path does not prune the holder's
    // promotion_objects queue, so the stale entry may surface first (the C++
    // test relies on map ordering); the contract is that the queue-limit-one
    // slot is freed so other_k2 can be admitted and eventually delivered.
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();
    trigger_lookup(&service, "other_k2").await;
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);
    let mut seen = Vec::new();
    for _ in 0..4 {
        let heartbeat = promotion_heartbeat(&service, holder_id).await;
        if heartbeat.is_empty() {
            break;
        }
        seen.extend(heartbeat);
    }
    assert!(seen.iter().any(|key| key == "other_k2"));
}

// PromotionOnHitTest.RemoveAllErasesPromotionTask: RemoveAll of an in-flight
// key increments cancelled once and frees the queue-limit-one slot for a newly
// seeded second key.
#[tokio::test]
async fn cpp_parity_promotion_remove_all_erases_promotion_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_first").await;

    let cancelled_pre = metrics::PROMOTION_CANCELLED.get();
    trigger_lookup(&service, "k_first").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_first"]
    );

    let response = MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.removed_count >= 1);
    assert_eq!(metrics::PROMOTION_CANCELLED.get() - cancelled_pre, 1);

    seed_local_disk_object(&service, holder_id, "k_second").await;
    trigger_lookup(&service, "k_second").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_second"]
    );
}

// PromotionOnHitTest.BatchRemoveErasesPromotionTask: normal BatchRemove of an
// in-flight key succeeds, increments cancelled once, and frees the
// queue-limit-one slot for a second key.
#[tokio::test]
async fn cpp_parity_promotion_batch_remove_erases_promotion_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_first").await;
    seed_local_disk_object(&service, holder_id, "k_second").await;

    let cancelled_pre = metrics::PROMOTION_CANCELLED.get();
    trigger_lookup(&service, "k_first").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_first"]
    );

    let response = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["k_first".into()],
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.statuses.len(), 1);
    assert_eq!(metrics::PROMOTION_CANCELLED.get() - cancelled_pre, 1);

    trigger_lookup(&service, "k_second").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_second"]
    );
}

// PromotionOnHitTest.StalePromotionReaper: after a queued task is drained and
// its deadline expires, the reaper unblocks dedup so a fresh read requeues the
// same key; the expired metric rises exactly once and in-flight returns to
// baseline. Rust drives the production reaper explicitly with a zero release
// timeout instead of sleeping.
#[tokio::test]
async fn cpp_parity_promotion_stale_promotion_reaper() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        put_start_release_timeout: std::time::Duration::ZERO,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;

    let expired_pre = metrics::PROMOTION_EXPIRED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    trigger_lookup(&service, "k_cold").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_cold"]
    );

    // Dedup gate blocks re-enqueue while the task is in flight.
    trigger_lookup(&service, "k_cold").await;
    assert!(promotion_heartbeat(&service, holder_id).await.is_empty());

    service.reap_expired_background_tasks_for_test();
    assert_eq!(metrics::PROMOTION_EXPIRED.get() - expired_pre, 1);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);

    trigger_lookup(&service, "k_cold").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_cold"]
    );
}

// PromotionOnHitTest.ReaperPopsStagedMemoryReplicaOnExpiry: an AllocStart
// staged replica that is never committed is popped by the reaper, returning
// in-flight to baseline; a later NotifyPromotionSuccess fails because no task
// remains.
#[tokio::test]
async fn cpp_parity_promotion_reaper_pops_staged_memory_replica_on_expiry() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        put_start_release_timeout: std::time::Duration::ZERO,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;

    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();
    trigger_lookup(&service, "k_cold").await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    service.reap_expired_background_tasks_for_test();
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);

    let error = MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
}

// PromotionOnHitTest.AllocStartRejectsReapedTask: after an unallocated queued
// task expires, AllocStart fails with REPLICA_IS_NOT_READY (tonic
// FailedPrecondition) because the task is gone.
#[tokio::test]
async fn cpp_parity_promotion_alloc_start_rejects_reaped_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        put_start_release_timeout: std::time::Duration::ZERO,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    mount_memory_segment(&service, holder_id, "dram-pool").await;
    trigger_lookup(&service, "k_cold").await;

    service.reap_expired_background_tasks_for_test();

    let error = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            size: 1024,
            preferred_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
}

// PromotionOnHitTest.RemoveDuringPromotion: force Remove of a queued key
// increments cancelled once, a later NotifyPromotionSuccess reports the
// missing object, and re-injecting plus a fresh read queues the same key again.
#[tokio::test]
async fn cpp_parity_promotion_remove_during_promotion() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        put_start_release_timeout: std::time::Duration::ZERO,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_cold").await;

    let cancelled_pre = metrics::PROMOTION_CANCELLED.get();
    trigger_lookup(&service, "k_cold").await;

    remove_key(&service, "k_cold").await;
    assert_eq!(metrics::PROMOTION_CANCELLED.get() - cancelled_pre, 1);

    let error = MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_cold".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);

    service.reap_expired_background_tasks_for_test();
    seed_local_disk_object(&service, holder_id, "k_cold").await;
    trigger_lookup(&service, "k_cold").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_cold"]
    );
}

// PromotionOnHitTest.ClientExpiryClearsPromotionTask: when the holder expires
// before the task deadline, its queued task is cancelled once and a kept-alive
// second holder can admit a key at the global queue limit one.
#[tokio::test]
async fn cpp_parity_promotion_client_expiry_clears_promotion_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_a = Uuid::new_v4();
    let holder_b = Uuid::new_v4();
    mount_local_disk(&service, holder_a).await;
    mount_local_disk(&service, holder_b).await;
    seed_local_disk_object(&service, holder_a, "k_first").await;

    let cancelled_pre = metrics::PROMOTION_CANCELLED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();
    trigger_lookup(&service, "k_first").await;
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);

    service.expire_client_for_test(holder_a);
    service.clear_invalid_handles_for_test();
    assert_eq!(metrics::PROMOTION_CANCELLED.get() - cancelled_pre, 1);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);

    seed_local_disk_object(&service, holder_b, "k_second").await;
    trigger_lookup(&service, "k_second").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_b).await,
        vec!["k_second"]
    );
}

fn watermark_reject_config() -> MasterRuntimeConfig {
    MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        eviction_high_watermark_ratio: 0.0,
        ..Default::default()
    }
}

// PromotionOnHitTest.RetryCandidate_WatermarkRejectionRecordsCandidate: a
// watermark-rejected read records exactly one candidate and a second read
// refreshes it without a duplicate.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_watermark_rejection_records_candidate() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(watermark_reject_config());
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_wm").await;

    let recorded_pre = metrics::PROMOTION_CANDIDATE_RECORDED.get();
    trigger_lookup(&service, "k_wm").await;
    assert_eq!(
        metrics::PROMOTION_CANDIDATE_RECORDED.get() - recorded_pre,
        1
    );
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    trigger_lookup(&service, "k_wm").await;
    assert_eq!(service.promotion_candidate_count_for_test(), 1);
}

// PromotionOnHitTest.RetryCandidate_CapRejectedThenQueuedOnRetry: a candidate
// recorded while the queue is full is admitted once the active slot is
// released and the retry scanner reaches it.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_cap_rejected_then_queued_on_retry() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_busy").await;
    seed_local_disk_object(&service, holder_id, "k_retry").await;

    let candidate_admitted_pre = metrics::PROMOTION_CANDIDATE_ADMITTED.get();
    let promotion_admitted_pre = metrics::PROMOTION_ADMITTED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();

    trigger_lookup(&service, "k_busy").await;
    assert_eq!(
        metrics::PROMOTION_ADMITTED.get() - promotion_admitted_pre,
        1
    );
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);

    trigger_lookup(&service, "k_retry").await;
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "k_busy".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);

    service.make_promotion_candidates_due_for_test();
    assert_eq!(service.run_promotion_candidate_retry_for_test(), ());
    assert_eq!(service.promotion_candidate_count_for_test(), 0);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);
    assert_eq!(
        metrics::PROMOTION_CANDIDATE_ADMITTED.get() - candidate_admitted_pre,
        1
    );
    assert_eq!(
        promotion_heartbeat(&service, holder_id).await,
        vec!["k_retry"]
    );
}

// PromotionOnHitTest.RetryCandidate_NoCandidatesOrNoShardBudgetNoops: retry
// with no candidates or with a zero shard budget is a no-op that leaves the
// recorded candidate present.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_no_candidates_or_no_shard_budget_noops() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(watermark_reject_config());
    service.run_promotion_candidate_retry_for_test();

    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_noop").await;
    trigger_lookup(&service, "k_noop").await;
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    service.make_promotion_candidates_due_for_test();
    service.run_promotion_candidate_retry_with_budget_for_test(0);
    assert_eq!(service.promotion_candidate_count_for_test(), 1);
}

// PromotionOnHitTest.RetryCandidate_ExhaustedAfterMaxRetries: a candidate is
// removed after the maximum retry scans and the expired-evaluated metric
// increases.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_exhausted_after_max_retries() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(watermark_reject_config());
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_exhaust").await;
    trigger_lookup(&service, "k_exhaust").await;
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    let expired_pre = metrics::PROMOTION_CANDIDATE_EXPIRED_EVALUATED.get();
    for _ in 0..=10 {
        service.make_promotion_candidates_due_for_test();
        service.run_promotion_candidate_retry_for_test();
    }

    assert_eq!(service.promotion_candidate_count_for_test(), 0);
    assert!(metrics::PROMOTION_CANDIDATE_EXPIRED_EVALUATED.get() > expired_pre);
}

// PromotionOnHitTest.RetryCandidate_ObjectRemovedMidRetry: removing the object
// while its watermark candidate is pending makes the retry erase the candidate
// without admitting it.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_object_removed_mid_retry() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(watermark_reject_config());
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_rm").await;
    trigger_lookup(&service, "k_rm").await;
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    let admitted_pre = metrics::PROMOTION_CANDIDATE_ADMITTED.get();
    remove_key(&service, "k_rm").await;
    service.make_promotion_candidates_due_for_test();
    service.run_promotion_candidate_retry_for_test();

    assert_eq!(service.promotion_candidate_count_for_test(), 0);
    assert_eq!(
        metrics::PROMOTION_CANDIDATE_ADMITTED.get() - admitted_pre,
        0
    );
}

// PromotionOnHitTest.RetryCandidate_MultipleKeysTracked: five distinct
// watermark-rejected keys create five independent candidates and the
// unevaluated-expired counter stays unchanged.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_multiple_keys_tracked() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(watermark_reject_config());
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    let keys = [
        "k_multi_0",
        "k_multi_1",
        "k_multi_2",
        "k_multi_3",
        "k_multi_4",
    ];
    let recorded_pre = metrics::PROMOTION_CANDIDATE_RECORDED.get();
    let unevaluated_pre = metrics::PROMOTION_CANDIDATE_EXPIRED_UNEVALUATED.get();
    for key in keys {
        seed_local_disk_object(&service, holder_id, key).await;
        trigger_lookup(&service, key).await;
    }

    assert_eq!(service.promotion_candidate_count_for_test(), 5);
    assert_eq!(
        metrics::PROMOTION_CANDIDATE_RECORDED.get() - recorded_pre,
        5
    );
    assert_eq!(
        metrics::PROMOTION_CANDIDATE_EXPIRED_UNEVALUATED.get() - unevaluated_pre,
        0
    );
}

// PromotionOnHitTest.RetryCandidate_ClearOnReload: reload cleanup clears the
// recorded candidate, resets the global candidate count to zero, and leaves
// promotion in-flight zero.
#[tokio::test]
async fn cpp_parity_promotion_retry_candidate_clear_on_reload() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(watermark_reject_config());
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "k_reload").await;
    trigger_lookup(&service, "k_reload").await;
    assert_eq!(service.promotion_candidate_count_for_test(), 1);

    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();
    service.clear_promotion_candidates_for_reload_for_test();
    assert_eq!(service.promotion_candidate_count_for_test(), 0);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);
}

// PromotionOnHitTest.BatchRemoveStaleHandleErasesPromotionTask: BatchRemove's
// stale-handle path erases the stale-held LocalDisk replica (invalidating the
// object), cancels the in-flight promotion task, reports OBJECT_NOT_FOUND for
// that key, and frees the queue-limit-one slot for an active second holder.
#[tokio::test]
async fn cpp_parity_promotion_batch_remove_stale_handle_erases_promotion_task() {
    let _guard = METRICS_LOCK.lock().await;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_a = Uuid::new_v4();
    let holder_b = Uuid::new_v4();
    mount_local_disk(&service, holder_a).await;
    mount_local_disk(&service, holder_b).await;
    seed_local_disk_object(&service, holder_a, "k_first").await;

    let cancelled_pre = metrics::PROMOTION_CANCELLED.get();
    let in_flight_pre = metrics::PROMOTION_IN_FLIGHT.get();
    trigger_lookup(&service, "k_first").await;
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 1);

    // Make the first holder stale so BatchRemove's stale-handle pre-cleanup
    // erases the LocalDisk replica it holds (C++: client absent from
    // ok_client_ after PrepareSegment without ReMount).
    service.expire_client_for_test(holder_a);
    let response = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["k_first".into()],
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.statuses.len(), 1);
    assert_eq!(response.statuses[0], -1); // BatchStatus::KeyNotFound
    assert_eq!(metrics::PROMOTION_CANCELLED.get() - cancelled_pre, 1);
    assert_eq!(metrics::PROMOTION_IN_FLIGHT.get() - in_flight_pre, 0);

    seed_local_disk_object(&service, holder_b, "k_second").await;
    trigger_lookup(&service, "k_second").await;
    assert_eq!(
        promotion_heartbeat(&service, holder_b).await,
        vec!["k_second"]
    );
}
