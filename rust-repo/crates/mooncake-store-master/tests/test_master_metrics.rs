mod common;

use common::proto_uuid;
use mooncake_store_master::metrics;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Mutex;
use tonic::Request;
use uuid::Uuid;

static METRICS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn replicate_config(segment: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: segment.into(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
    }
}

fn multi_replicate_config(segments: &[&str]) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: segments.len() as u32,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: String::new(),
        prefer_alloc_in_same_node: false,
        preferred_segments: segments.iter().map(|segment| (*segment).into()).collect(),
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
    }
}

async fn mount_memory_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete(service: &MasterServiceImpl, client_id: Uuid, key: &str, segment: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(replicate_config(segment)),
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
}

async fn put_complete_with_config(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    config: proto::ReplicateConfig,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(config),
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
}

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

#[tokio::test]
async fn test_cache_hit_metrics_count_memory_and_local_disk_bytes() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: false,
        ..Default::default()
    });
    let memory_client = Uuid::new_v4();
    let disk_holder = Uuid::new_v4();
    mount_memory_segment(&service, memory_client, "metrics-memory:1").await;
    put_complete(
        &service,
        memory_client,
        "metrics-memory-key",
        "metrics-memory:1",
    )
    .await;
    mount_local_disk_and_notify_success(&service, disk_holder, "metrics-disk-key").await;

    let base_mem_hits = metrics::MEM_CACHE_HITS.get();
    let base_file_hits = metrics::FILE_CACHE_HITS.get();
    let base_mem_hit_bytes = metrics::MEM_CACHE_HIT_BYTES.get();
    let base_file_hit_bytes = metrics::FILE_CACHE_HIT_BYTES.get();
    let base_valid_gets = metrics::VALID_GETS.get();

    for key in ["metrics-memory-key", "metrics-disk-key"] {
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    assert_eq!(metrics::MEM_CACHE_HITS.get(), base_mem_hits + 1);
    assert_eq!(metrics::FILE_CACHE_HITS.get(), base_file_hits + 1);
    assert_eq!(metrics::MEM_CACHE_HIT_BYTES.get(), base_mem_hit_bytes + 128);
    assert_eq!(
        metrics::FILE_CACHE_HIT_BYTES.get(),
        base_file_hit_bytes + 128
    );
    assert_eq!(metrics::VALID_GETS.get(), base_valid_gets + 2);
}

#[tokio::test]
async fn test_cache_total_metrics_track_object_inventory() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: false,
        ..Default::default()
    });
    let memory_client = Uuid::new_v4();
    let disk_holder = Uuid::new_v4();
    mount_memory_segment(&service, memory_client, "inventory-memory:1").await;
    mount_memory_segment(&service, memory_client, "inventory-memory:2").await;

    let base_mem_total = metrics::MEM_CACHE_TOTAL.get();
    let base_file_total = metrics::FILE_CACHE_TOTAL.get();

    put_complete_with_config(
        &service,
        memory_client,
        "inventory-memory-key",
        multi_replicate_config(&["inventory-memory:1", "inventory-memory:2"]),
    )
    .await;
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total + 1);

    mount_local_disk_and_notify_success(&service, disk_holder, "inventory-disk-key").await;
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total + 1);

    MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(disk_holder)),
            key: "inventory-disk-key".into(),
            tenant_id: String::new(),
            replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "inventory-memory-key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total);
}
