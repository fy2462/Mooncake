mod common;

use common::proto_uuid;
use mooncake_store_master::allocator::AllocationStrategy;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

static METRICS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn ssd_aware_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        lease_ttl: Duration::ZERO,
        allocation_strategy: AllocationStrategy::SsdFreeRatioFirst,
        ..Default::default()
    })
}

async fn mount_ssd_segment(service: &MasterServiceImpl, client_id: Uuid, segment_name: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 64 * 1024 * 1024,
            base_addr: 0x100000000 + (segment_name.len() as u64) * 0x10000000,
            te_endpoint: segment_name.into(),
            protocol: "tcp".into(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_local_disk(service: &MasterServiceImpl, client_id: Uuid) {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
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
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
}

async fn report_ssd_capacity(service: &MasterServiceImpl, client_id: Uuid, bytes: i64) {
    MasterService::report_ssd_capacity(
        service,
        Request::new(proto::ReportSsdCapacityRequest {
            client_id: Some(proto_uuid(client_id)),
            ssd_total_capacity_bytes: bytes,
        }),
    )
    .await
    .unwrap();
}

async fn put_memory_and_unsolicited_offload(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    segment_name: &str,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
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
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    // put_end admits an offload task for a LocalDisk-enabled holder; obtain the
    // Master-issued task (with its byte generation) and complete it directly
    // without waiting for a client heartbeat loop.
    let heartbeat = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(client_id)),
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
        .expect("put_end must admit an offload task");
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: size as i64,
                transport_endpoint: segment_name.into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
}

async fn probe(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
) -> proto::ReplicaDescriptor {
    let response = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 64,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    response.replicas.into_iter().next().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cpp_parity_remove_releases_local_disk_usage_for_service_ranking() {
    let _guard = METRICS_LOCK.lock().await;
    let service = ssd_aware_service();
    let holder1 = Uuid::new_v4();
    let holder2 = Uuid::new_v4();
    mount_ssd_segment(&service, holder1, "ssd-holder-1:1").await;
    mount_ssd_segment(&service, holder2, "ssd-holder-2:1").await;
    mount_local_disk(&service, holder1).await;
    mount_local_disk(&service, holder2).await;
    report_ssd_capacity(&service, holder1, 1000).await;
    report_ssd_capacity(&service, holder2, 1000).await;

    put_memory_and_unsolicited_offload(&service, holder1, "heavy", 800, "ssd-holder-1:1").await;
    put_memory_and_unsolicited_offload(&service, holder2, "light", 100, "ssd-holder-2:1").await;

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "heavy".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let allocated = probe(&service, holder1, "probe-after-remove").await;
    assert_eq!(
        allocated.replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
    assert_eq!(allocated.segment_name, "ssd-holder-1:1");
}

#[tokio::test(flavor = "multi_thread")]
async fn cpp_parity_batch_clear_all_releases_local_disk_usage_for_service_ranking() {
    let _guard = METRICS_LOCK.lock().await;
    let service = ssd_aware_service();
    let holder1 = Uuid::new_v4();
    let holder2 = Uuid::new_v4();
    mount_ssd_segment(&service, holder1, "clear-holder-1:1").await;
    mount_ssd_segment(&service, holder2, "clear-holder-2:1").await;
    mount_local_disk(&service, holder1).await;
    mount_local_disk(&service, holder2).await;
    report_ssd_capacity(&service, holder1, 1000).await;
    report_ssd_capacity(&service, holder2, 1000).await;

    put_memory_and_unsolicited_offload(&service, holder1, "heavy", 800, "clear-holder-1:1").await;
    put_memory_and_unsolicited_offload(&service, holder2, "light", 100, "clear-holder-2:1").await;

    let cleared = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["heavy".into()],
            client_id: Some(proto_uuid(holder1)),
            segment_name: String::new(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(cleared.cleared_keys, vec!["heavy".to_string()]);

    let allocated = probe(&service, holder1, "probe-after-clear").await;
    assert_eq!(
        allocated.replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
    assert_eq!(allocated.segment_name, "clear-holder-1:1");
}

#[tokio::test(flavor = "multi_thread")]
async fn cpp_parity_local_disk_eviction_updates_service_ssd_ranking() {
    let _guard = METRICS_LOCK.lock().await;
    let service = ssd_aware_service();
    let holder1 = Uuid::new_v4();
    let holder2 = Uuid::new_v4();
    mount_ssd_segment(&service, holder1, "evict-holder-1:1").await;
    mount_ssd_segment(&service, holder2, "evict-holder-2:1").await;
    mount_local_disk(&service, holder1).await;
    mount_local_disk(&service, holder2).await;
    report_ssd_capacity(&service, holder1, 1000).await;
    report_ssd_capacity(&service, holder2, 1000).await;

    put_memory_and_unsolicited_offload(&service, holder1, "heavy", 800, "evict-holder-1:1").await;
    put_memory_and_unsolicited_offload(&service, holder2, "light", 100, "evict-holder-2:1").await;

    // More free SSD ratio on holder2, so the first probe lands there.
    let first = probe(&service, holder1, "probe-before-evict").await;
    assert_eq!(
        first.replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
    assert_eq!(first.segment_name, "evict-holder-2:1");

    MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(holder1)),
            key: "heavy".into(),
            replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let second = probe(&service, holder1, "probe-after-evict").await;
    assert_eq!(
        second.replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
    assert_eq!(second.segment_name, "evict-holder-1:1");
}

#[tokio::test(flavor = "multi_thread")]
async fn cpp_parity_evict_local_disk_from_same_memory_object_updates_only_file_total() {
    let _guard = METRICS_LOCK.lock().await;
    let service = ssd_aware_service();
    let holder = Uuid::new_v4();
    mount_ssd_segment(&service, holder, "metrics-holder:1").await;
    mount_local_disk(&service, holder).await;
    report_ssd_capacity(&service, holder, 1000).await;

    let base_mem_total = mooncake_store_master::metrics::MEM_CACHE_TOTAL.get();
    let base_file_total = mooncake_store_master::metrics::FILE_CACHE_TOTAL.get();
    put_memory_and_unsolicited_offload(&service, holder, "key", 128, "metrics-holder:1").await;
    assert_eq!(
        mooncake_store_master::metrics::MEM_CACHE_TOTAL.get(),
        base_mem_total + 1
    );
    assert_eq!(
        mooncake_store_master::metrics::FILE_CACHE_TOTAL.get(),
        base_file_total + 1
    );

    MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(holder)),
            key: "key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        mooncake_store_master::metrics::MEM_CACHE_TOTAL.get(),
        base_mem_total + 1
    );
    assert_eq!(
        mooncake_store_master::metrics::FILE_CACHE_TOTAL.get(),
        base_file_total
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cpp_parity_service_ssd_free_ratio_prefers_fresher_holder() {
    let _guard = METRICS_LOCK.lock().await;
    let service = ssd_aware_service();
    let holder1 = Uuid::new_v4();
    let holder2 = Uuid::new_v4();
    mount_ssd_segment(&service, holder1, "free-holder-1:1").await;
    mount_ssd_segment(&service, holder2, "free-holder-2:1").await;
    mount_local_disk(&service, holder1).await;
    mount_local_disk(&service, holder2).await;
    report_ssd_capacity(&service, holder1, 1000).await;
    report_ssd_capacity(&service, holder2, 1000).await;

    put_memory_and_unsolicited_offload(&service, holder1, "heavy", 800, "free-holder-1:1").await;
    put_memory_and_unsolicited_offload(&service, holder2, "light", 100, "free-holder-2:1").await;

    let allocated = probe(&service, holder1, "free-ratio-probe").await;
    assert_eq!(
        allocated.replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
    assert_eq!(allocated.segment_name, "free-holder-2:1");
}
