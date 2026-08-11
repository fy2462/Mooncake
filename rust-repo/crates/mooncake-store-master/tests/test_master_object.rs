mod common;
use common::proto_uuid;
use mooncake_store_master::oplog::{InMemoryOpLog, OpLogManager};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Barrier;
use tonic::Request;
use uuid::Uuid;

async fn mount_segment_with_size(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    size: u64,
    base_addr: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size,
            base_addr,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_start_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    slice_length: u64,
    replica_num: u32,
    preferred_segment: Option<&str>,
) -> proto::PutStartResponse {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num,
                preferred_segment: preferred_segment.unwrap_or("").into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner()
}

async fn put_end_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    replica_type: i32,
) {
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn remove_object(service: &MasterServiceImpl, key: &str) -> Result<(), tonic::Code> {
    MasterService::remove(
        service,
        Request::new(proto::RemoveRequest {
            key: key.into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn remove_all_objects(service: &MasterServiceImpl, force: bool) -> i64 {
    MasterService::remove_all(
        service,
        Request::new(proto::RemoveAllRequest {
            force,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .removed_count
}

async fn get_replica_list_result(
    service: &MasterServiceImpl,
    key: &str,
) -> Result<proto::GetReplicaListResponse, tonic::Status> {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|response| response.into_inner())
}

#[tokio::test]
async fn random_replica_count_put_lifecycle_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig::default());
    let client_id = Uuid::new_v4();
    let segment_size = 16 * 1024 * 1024;
    for index in 0..5 {
        mount_segment_with_size(
            &service,
            client_id,
            &format!("segment_{index}"),
            segment_size,
            0x300000000 + index * segment_size,
        )
        .await;
    }

    let replica_num = 1 + (client_id.as_u128() % 5) as u32;
    let key = "test_key";
    let started = put_start_object(&service, client_id, key, 1024, replica_num, None).await;
    assert!(!started.replicas.is_empty());
    for replica in &started.replicas {
        assert_eq!(
            replica.status,
            proto::replica_descriptor::ReplicaStatus::Allocating as i32
        );
    }

    let get_error = get_replica_list_result(&service, key).await.unwrap_err();
    assert_eq!(get_error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        remove_object(&service, key).await,
        Err(tonic::Code::FailedPrecondition)
    );

    put_end_object(
        &service,
        client_id,
        key,
        proto::replica_descriptor::ReplicaType::Memory as i32,
    )
    .await;

    let list = get_replica_list_result(&service, key).await.unwrap();
    assert_eq!(list.replicas.len(), replica_num as usize);
    for replica in &list.replicas {
        assert_eq!(
            replica.status,
            proto::replica_descriptor::ReplicaStatus::Complete as i32
        );
    }
}

#[tokio::test]
async fn randomized_put_remove_absence_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig::default());
    let client_id = Uuid::new_v4();
    mount_segment_with_size(
        &service,
        client_id,
        "random:1",
        16 * 1024 * 1024,
        0x300000000,
    )
    .await;

    let mut state = client_id.as_u128();
    for _ in 0..10 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let key = format!("test_key{}", state % 1000);
        put_start_object(&service, client_id, &key, 1024, 1, None).await;
        put_end_object(
            &service,
            client_id,
            &key,
            proto::replica_descriptor::ReplicaType::Memory as i32,
        )
        .await;
        assert_eq!(remove_object(&service, &key).await, Ok(()));
        assert_eq!(
            get_replica_list_result(&service, &key)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
}

#[tokio::test]
async fn remove_all_ten_completed_objects_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment_with_size(
        &service,
        client_id,
        "remove-all:1",
        16 * 1024 * 1024,
        0x300000000,
    )
    .await;

    for index in 0..10 {
        let key = format!("test_key{index}");
        put_start_object(&service, client_id, &key, 1024, 1, None).await;
        put_end_object(
            &service,
            client_id,
            &key,
            proto::replica_descriptor::ReplicaType::Memory as i32,
        )
        .await;
    }
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(remove_all_objects(&service, false).await, 10);
    for index in 0..10 {
        assert_eq!(
            get_replica_list_result(&service, &format!("test_key{index}"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
}

#[tokio::test]
async fn concurrent_writes_and_remove_all_account_exactly_parity() {
    let service = Arc::new(MasterServiceImpl::with_runtime_config(
        MasterRuntimeConfig::default(),
    ));
    let client_id = Uuid::new_v4();
    mount_segment_with_size(
        &service,
        client_id,
        "concurrent-write:1",
        256 * 1024 * 1024,
        0x300000000,
    )
    .await;

    let success_writes = Arc::new(AtomicUsize::new(0));
    let remove_all_done = Arc::new(AtomicBool::new(false));
    let total_removed = Arc::new(AtomicUsize::new(0));

    let mut writers = Vec::new();
    for thread_index in 0..4 {
        let service = Arc::clone(&service);
        let success_writes = Arc::clone(&success_writes);
        writers.push(tokio::spawn(async move {
            for object_index in 0..100 {
                let key = format!("key_{thread_index}_{object_index}");
                if put_start_object(&service, client_id, &key, 1024, 1, None)
                    .await
                    .replicas
                    .is_empty()
                {
                    continue;
                }
                put_end_object(
                    &service,
                    client_id,
                    &key,
                    proto::replica_descriptor::ReplicaType::Memory as i32,
                )
                .await;
                success_writes.fetch_add(1, Ordering::SeqCst);
                // Keep the writers running past the delayed remover, matching
                // the C++ per-object sleeps that interleave with RemoveAll.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }

    let service_remover = Arc::clone(&service);
    let remover_done = Arc::clone(&remove_all_done);
    let remover_total = Arc::clone(&total_removed);
    let remover = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let removed = remove_all_objects(&service_remover, true).await;
        assert!(removed > 0);
        remover_done.store(true, Ordering::SeqCst);
        remover_total.fetch_add(removed as usize, Ordering::SeqCst);
    });

    for writer in writers {
        writer.await.unwrap();
    }
    remover.await.unwrap();

    assert!(success_writes.load(Ordering::SeqCst) > 0);
    assert!(remove_all_done.load(Ordering::SeqCst));
    let final_removed = remove_all_objects(&service, true).await;
    assert!(final_removed > 0);
    total_removed.fetch_add(final_removed as usize, Ordering::SeqCst);
    assert_eq!(total_removed.load(Ordering::SeqCst), 400);
}

#[tokio::test]
async fn concurrent_reads_and_remove_all_eventual_absence_parity() {
    const OBJECT_COUNT: usize = 1000;
    let service = Arc::new(MasterServiceImpl::with_runtime_config(
        MasterRuntimeConfig {
            lease_ttl: Duration::from_millis(200),
            ..Default::default()
        },
    ));
    let client_id = Uuid::new_v4();
    mount_segment_with_size(
        &service,
        client_id,
        "concurrent-read:1",
        256 * 1024 * 1024,
        0x300000000,
    )
    .await;

    for index in 0..OBJECT_COUNT {
        let key = format!("pre_key_{index}");
        put_start_object(&service, client_id, &key, 1024, 1, None).await;
        put_end_object(
            &service,
            client_id,
            &key,
            proto::replica_descriptor::ReplicaType::Memory as i32,
        )
        .await;
    }

    let success_reads = Arc::new(AtomicUsize::new(0));
    let remove_all_done = Arc::new(AtomicBool::new(false));

    let mut readers = Vec::new();
    for _ in 0..4 {
        let service = Arc::clone(&service);
        let success_reads = Arc::clone(&success_reads);
        readers.push(tokio::spawn(async move {
            for index in 0..OBJECT_COUNT {
                if get_replica_list_result(&service, &format!("pre_key_{index}"))
                    .await
                    .is_ok()
                {
                    success_reads.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    let service_remover = Arc::clone(&service);
    let remover_done = Arc::clone(&remove_all_done);
    let remover = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let removed = remove_all_objects(&service_remover, true).await;
        assert!(removed > 0);
        remover_done.store(true, Ordering::SeqCst);
    });

    for reader in readers {
        reader.await.unwrap();
    }
    remover.await.unwrap();

    assert!(remove_all_done.load(Ordering::SeqCst));
    let reads = success_reads.load(Ordering::SeqCst);
    assert!(reads > 0);
    assert_ne!(reads, OBJECT_COUNT);

    tokio::time::sleep(Duration::from_millis(210)).await;
    remove_all_objects(&service, true).await;
    for index in 0..OBJECT_COUNT {
        assert_eq!(
            get_replica_list_result(&service, &format!("pre_key_{index}"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
}

#[tokio::test]
async fn two_concurrent_remove_all_calls_partition_exact_count_parity() {
    const OBJECT_COUNT: usize = 1000;
    let service = Arc::new(MasterServiceImpl::with_runtime_config(
        MasterRuntimeConfig::default(),
    ));
    let client_id = Uuid::new_v4();
    mount_segment_with_size(
        &service,
        client_id,
        "concurrent-remove-all:1",
        16 * 1024 * 1024 * 100,
        0x300000000,
    )
    .await;

    for index in 0..OBJECT_COUNT {
        let key = format!("pre_key_{index}");
        put_start_object(&service, client_id, &key, 1024, 1, None).await;
        put_end_object(
            &service,
            client_id,
            &key,
            proto::replica_descriptor::ReplicaType::Memory as i32,
        )
        .await;
    }

    let remove_all_count = Arc::new(AtomicUsize::new(0));
    let mut removers = Vec::new();
    for _ in 0..2 {
        let service = Arc::clone(&service);
        let remove_all_count = Arc::clone(&remove_all_count);
        removers.push(tokio::spawn(async move {
            let removed = remove_all_objects(&service, true).await;
            remove_all_count.fetch_add(removed as usize, Ordering::SeqCst);
        }));
    }
    for remover in removers {
        remover.await.unwrap();
    }

    assert_eq!(remove_all_count.load(Ordering::SeqCst), OBJECT_COUNT);
    for index in 0..OBJECT_COUNT {
        assert_eq!(
            get_replica_list_result(&service, &format!("pre_key_{index}"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
}

async fn mount_batch_clear_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    index: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024 * 1024,
            base_addr: 0x100000000 + index * 0x200000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete_batch_clear_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    replica_num: usize,
    preferred_segment: &str,
) -> Vec<proto::ReplicaDescriptor> {
    let started = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: replica_num as u32,
                preferred_segment: preferred_segment.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), replica_num);

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
    started.replicas
}

async fn batch_clear(
    service: &MasterServiceImpl,
    client_id: Uuid,
    keys: &[&str],
    segment_name: &str,
) -> Vec<String> {
    MasterService::batch_replica_clear(
        service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: keys.iter().map(|key| (*key).to_string()).collect(),
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .cleared_keys
}

async fn object_exists(service: &MasterServiceImpl, key: &str) -> bool {
    MasterService::exist_key(
        service,
        Request::new(proto::ExistKeyRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .exists
}

async fn mount_put_start_parity_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 16 * 1024 * 1024,
            base_addr: 0x300000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_put_start_parity_nof_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) {
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(Uuid::new_v4())),
                name: segment_name.into(),
                base: 0x400000000,
                size: 16 * 1024 * 1024,
                te_endpoint: format!("nof://{segment_name}"),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap();
}

async fn start_memory_nof_parity_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    memory_segment: &str,
    nof_segment: &str,
) {
    let response = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                preferred_segment: memory_segment.into(),
                preferred_nof_segments: vec![nof_segment.into()],
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 2);
}

async fn get_memory_nof_parity_replicas(
    service: &MasterServiceImpl,
    key: &str,
) -> Vec<proto::ReplicaDescriptor> {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas
}

async fn mount_preference_parity_segments(
    service: &MasterServiceImpl,
    client_id: Uuid,
    names: &[&str],
) {
    for (index, name) in names.iter().enumerate() {
        MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: (*name).into(),
                size: 16 * 1024 * 1024,
                base_addr: 0x500000000 + index as u64 * 0x1000000,
                te_endpoint: (*name).into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn put_start_invalid_parameter_matrix_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "put-invalid:3333").await;
    let request = |slice_length, config| proto::PutStartRequest {
        client_id: Some(proto_uuid(client_id)),
        key: "test_key".into(),
        slice_length,
        tenant_id: String::new(),
        config: Some(config),
    };

    for (case, request) in [
        (
            "zero total replicas",
            request(
                1024,
                proto::ReplicateConfig {
                    replica_num: 0,
                    nof_replica_num: 0,
                    ..Default::default()
                },
            ),
        ),
        (
            "zero slice length",
            request(
                0,
                proto::ReplicateConfig {
                    replica_num: 1,
                    ..Default::default()
                },
            ),
        ),
        (
            "same-node preference with NoF",
            request(
                1024,
                proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 1,
                    prefer_alloc_in_same_node: true,
                    ..Default::default()
                },
            ),
        ),
    ] {
        let error = MasterService::put_start(&service, Request::new(request))
            .await
            .expect_err(case);
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{case}");
    }
}

#[tokio::test]
async fn one_plus_one_allows_available_memory_only_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "one-plus-one:3333").await;

    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_one_plus_one".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        response.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
}

#[tokio::test]
async fn one_plus_one_flexible_mode_also_allows_available_nof_only() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_nof_segment(&service, client_id, "one-plus-one-nof:3333").await;

    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_one_plus_one_nof".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        response.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::NofSsd as i32
    );
}

#[tokio::test]
async fn put_end_all_completes_memory_and_nof_replicas_parity() {
    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "put-end-all-memory:3333").await;
    mount_put_start_parity_nof_segment(&service, client_id, "put-end-all-nof:3333").await;
    start_memory_nof_parity_object(
        &service,
        client_id,
        "put-end-all-key",
        "put-end-all-memory:3333",
        "put-end-all-nof:3333",
    )
    .await;

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "put-end-all-key".into(),
            replica_type: Type::All as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let replicas = get_memory_nof_parity_replicas(&service, "put-end-all-key").await;
    assert!(replicas.iter().any(|replica| {
        replica.replica_type == Type::Memory as i32 && replica.status == Status::Complete as i32
    }));
    assert!(replicas.iter().any(|replica| {
        replica.replica_type == Type::NofSsd as i32 && replica.status == Status::Complete as i32
    }));
}

#[tokio::test]
async fn memory_put_end_leaves_nof_revokeable_parity() {
    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "split-put-end-memory:3333").await;
    mount_put_start_parity_nof_segment(&service, client_id, "split-put-end-nof:3333").await;
    start_memory_nof_parity_object(
        &service,
        client_id,
        "split-put-end-key",
        "split-put-end-memory:3333",
        "split-put-end-nof:3333",
    )
    .await;

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "split-put-end-key".into(),
            replica_type: Type::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let memory_only = get_memory_nof_parity_replicas(&service, "split-put-end-key").await;
    assert_eq!(memory_only.len(), 1);
    assert_eq!(memory_only[0].replica_type, Type::Memory as i32);
    assert_eq!(memory_only[0].status, Status::Complete as i32);

    MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "split-put-end-key".into(),
            replica_type: Type::NofSsd as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let final_replicas = get_memory_nof_parity_replicas(&service, "split-put-end-key").await;
    assert_eq!(final_replicas.len(), 1);
    assert_eq!(final_replicas[0].replica_type, Type::Memory as i32);
    assert_eq!(final_replicas[0].status, Status::Complete as i32);
}

#[tokio::test]
async fn put_start_end_flow_parity() {
    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let invalid_client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "put-flow:3333").await;

    let started = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "put-flow-key".into(),
            slice_length: 1024,
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
    assert!(!started.replicas.is_empty());
    assert_eq!(started.replicas[0].status, Status::Allocating as i32);

    let get_error = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "put-flow-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .expect_err("processing object must not be readable");
    assert_eq!(get_error.code(), tonic::Code::FailedPrecondition);
    let remove_error = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "put-flow-key".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .expect_err("processing object must not be removable");
    assert_eq!(remove_error.code(), tonic::Code::FailedPrecondition);

    for error in [
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(invalid_client_id)),
                key: "put-flow-key".into(),
                replica_type: Type::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .expect_err("foreign PutEnd must fail"),
        MasterService::put_revoke(
            &service,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(proto_uuid(invalid_client_id)),
                key: "put-flow-key".into(),
                replica_type: Type::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .expect_err("foreign PutRevoke must fail"),
    ] {
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "put-flow-key".into(),
            replica_type: Type::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let replicas = get_memory_nof_parity_replicas(&service, "put-flow-key").await;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].status, Status::Complete as i32);
}

#[tokio::test]
async fn singular_preferred_segment_put_parity() {
    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_preference_parity_segments(
        &service,
        client_id,
        &["segment_0", "segment_1", "segment_2"],
    )
    .await;
    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "singular-preferred-key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "segment_1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    assert_eq!(response.replicas[0].status, Status::Allocating as i32);
    assert_eq!(response.replicas[0].transport_endpoint, "segment_1");

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "singular-preferred-key".into(),
            replica_type: Type::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn multiple_preferred_segments_put_parity() {
    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_preference_parity_segments(
        &service,
        client_id,
        &["segment_0", "segment_1", "segment_2"],
    )
    .await;
    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "multiple-preferred-key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                preferred_segments: vec!["segment_0".into(), "segment_1".into()],
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 2);
    assert!(
        response
            .replicas
            .iter()
            .all(|replica| replica.status == Status::Allocating as i32)
    );
    let endpoints: std::collections::HashSet<_> = response
        .replicas
        .iter()
        .map(|replica| replica.transport_endpoint.as_str())
        .collect();
    assert_eq!(
        endpoints,
        std::collections::HashSet::from(["segment_0", "segment_1"])
    );

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "multiple-preferred-key".into(),
            replica_type: Type::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn batch_replica_clear_empty_input_parity() {
    let service = MasterServiceImpl::new(None, None);
    let cleared = batch_clear(&service, Uuid::new_v4(), &[], "").await;
    assert!(cleared.is_empty());
}

#[tokio::test]
async fn batch_replica_clear_missing_keys_parity() {
    let service = MasterServiceImpl::new(None, None);
    let cleared = batch_clear(
        &service,
        Uuid::new_v4(),
        &["missing_key1", "missing_key2"],
        "",
    )
    .await;
    assert!(cleared.is_empty());
}

#[tokio::test]
async fn batch_replica_clear_skips_active_lease_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(2000),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "active-lease:1", 0).await;
    put_complete_batch_clear_object(&service, client_id, "active-lease-key", 1, "active-lease:1")
        .await;

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "active-lease-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);

    assert!(
        batch_clear(&service, client_id, &["active-lease-key"], "")
            .await
            .is_empty()
    );
    assert!(object_exists(&service, "active-lease-key").await);
}

#[tokio::test]
async fn batch_replica_clear_rejects_nonowner_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let owner_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, owner_id, "owner:1", 0).await;
    put_complete_batch_clear_object(&service, owner_id, "owner-key", 1, "owner:1").await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert!(
        batch_clear(&service, Uuid::new_v4(), &["owner-key"], "")
            .await
            .is_empty()
    );
    assert!(object_exists(&service, "owner-key").await);
}

#[tokio::test]
async fn batch_replica_clear_all_segments_five_keys_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "all-segments:1", 0).await;
    let keys = [
        "clear-key-1",
        "clear-key-2",
        "clear-key-3",
        "clear-key-4",
        "clear-key-5",
    ];
    for key in keys {
        put_complete_batch_clear_object(&service, client_id, key, 1, "all-segments:1").await;
        assert!(object_exists(&service, key).await);
    }
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(batch_clear(&service, client_id, &keys, "").await, keys);
    for key in keys {
        assert!(!object_exists(&service, key).await);
    }
}

#[tokio::test]
async fn batch_replica_clear_does_not_affect_other_keys_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "clear-other:1", 0).await;

    put_complete_batch_clear_object(&service, client_id, "clear-target-key", 1, "clear-other:1")
        .await;
    put_complete_batch_clear_object(&service, client_id, "keep-key", 1, "clear-other:1").await;

    tokio::time::sleep(Duration::from_millis(60)).await;

    let cleared = batch_clear(&service, client_id, &["clear-target-key"], "").await;
    assert_eq!(cleared, ["clear-target-key"]);

    assert!(!object_exists(&service, "clear-target-key").await);
    assert!(object_exists(&service, "keep-key").await);

    let keep_replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "keep-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(keep_replicas.replicas.len(), 1);
}

#[tokio::test]
async fn batch_replica_clear_replicated_key_all_replicas_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    mount_batch_clear_segment(&service, client_id, "replica-all:1", 0).await;
    mount_batch_clear_segment(&service, client_id, "replica-all:2", 1).await;

    let key = "replicated-key";
    let started = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_string(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                preferred_segments: vec!["replica-all:1".into(), "replica-all:2".into()],
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), 2);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_string(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(60)).await;

    let cleared = batch_clear(&service, client_id, &[key], "").await;
    assert_eq!(cleared, vec![key.to_string()]);
    assert!(!object_exists(&service, key).await);
}

#[tokio::test]
async fn batch_replica_clear_specific_segment_replica_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    mount_batch_clear_segment(&service, client_id, "segment-a:1", 0).await;
    mount_batch_clear_segment(&service, client_id, "segment-b:1", 1).await;

    let key = "replica-segment-clear";
    let started = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_string(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                preferred_segments: vec!["segment-a:1".into(), "segment-b:1".into()],
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), 2);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_string(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(60)).await;

    let cleared = batch_clear(&service, client_id, &[key], "segment-a:1").await;
    assert_eq!(cleared, vec![key.to_string()]);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: key.to_string(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_ne!(replicas.replicas[0].segment_name, "segment-a:1");
}

#[tokio::test]
async fn batch_replica_clear_large_batch_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "large-batch:1", 0).await;

    let keys: Vec<String> = (0..50)
        .map(|index| format!("clear-large-{index}"))
        .collect();
    for key in &keys {
        put_complete_batch_clear_object(&service, client_id, key, 1, "large-batch:1").await;
    }

    tokio::time::sleep(Duration::from_millis(60)).await;

    let clear_keys: Vec<&str> = keys.iter().map(std::string::String::as_str).collect();
    let cleared = batch_clear(&service, client_id, &clear_keys, "").await;
    assert_eq!(cleared.len(), keys.len());
    for key in &keys {
        assert!(cleared.iter().any(|value| value == key));
        assert!(!object_exists(&service, key).await);
    }
}

#[tokio::test]
async fn batch_replica_clear_mixed_expired_and_active_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(200),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "mixed-expiry:1", 0).await;

    for key in ["expired-a", "expired-b"] {
        put_complete_batch_clear_object(&service, client_id, key, 1, "mixed-expiry:1").await;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;

    for key in ["active-a", "active-b"] {
        put_complete_batch_clear_object(&service, client_id, key, 1, "mixed-expiry:1").await;
    }

    assert!(object_exists(&service, "active-a").await);
    assert!(object_exists(&service, "active-b").await);

    let cleared = batch_clear(
        &service,
        client_id,
        &["expired-a", "expired-b", "active-a", "active-b"],
        "",
    )
    .await;
    assert_eq!(cleared.len(), 2);
    assert!(cleared.contains(&"expired-a".to_string()));
    assert!(cleared.contains(&"expired-b".to_string()));
    assert!(!cleared.contains(&"active-a".to_string()));
    assert!(!cleared.contains(&"active-b".to_string()));

    assert!(!object_exists(&service, "expired-a").await);
    assert!(!object_exists(&service, "expired-b").await);
    assert!(object_exists(&service, "active-a").await);
    assert!(object_exists(&service, "active-b").await);
}

#[tokio::test]
async fn batch_replica_clear_with_invalid_segment_name_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "invalid-seg:1", 0).await;

    put_complete_batch_clear_object(&service, client_id, "invalid-seg-key", 1, "invalid-seg:1")
        .await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let cleared = batch_clear(
        &service,
        client_id,
        &["invalid-seg-key"],
        "nonexistent_host:99999",
    )
    .await;
    assert!(cleared.is_empty());
    assert!(object_exists(&service, "invalid-seg-key").await);
}

#[tokio::test]
async fn batch_replica_clear_specific_segment_polling_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "specific:1", 0).await;
    mount_batch_clear_segment(&service, client_id, "specific:2", 1).await;
    let replicas =
        put_complete_batch_clear_object(&service, client_id, "specific-key", 1, "specific:1").await;
    assert_eq!(replicas[0].segment_name, "specific:1");

    tokio::time::sleep(Duration::from_millis(10)).await;
    let cleared = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cleared = batch_clear(&service, client_id, &["specific-key"], "specific:1").await;
            if !cleared.is_empty() {
                break cleared;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("named-segment replica did not become clearable within five seconds");

    assert_eq!(cleared, ["specific-key"]);
    assert!(!object_exists(&service, "specific-key").await);
}

#[tokio::test]
async fn batch_replica_clear_skips_empty_and_missing_strings_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "empty-strings:1", 0).await;
    put_complete_batch_clear_object(&service, client_id, "valid_key", 1, "empty-strings:1").await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(
        batch_clear(
            &service,
            client_id,
            &["", "valid_key", "", "another_empty"],
            "",
        )
        .await,
        ["valid_key"]
    );
}

#[tokio::test]
async fn batch_replica_clear_mixed_owner_missing_empty_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "mixed-owner:1", 0).await;
    mount_batch_clear_segment(&service, other_client_id, "mixed-owner:2", 1).await;
    for key in ["key1", "key2"] {
        put_complete_batch_clear_object(&service, client_id, key, 1, "mixed-owner:1").await;
    }
    put_complete_batch_clear_object(&service, other_client_id, "key3", 1, "mixed-owner:2").await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(
        batch_clear(
            &service,
            client_id,
            &["key1", "key2", "key3", "nonexistent", ""],
            "",
        )
        .await,
        ["key1", "key2"]
    );
    assert!(!object_exists(&service, "key1").await);
    assert!(!object_exists(&service, "key2").await);
    assert!(object_exists(&service, "key3").await);
}

#[tokio::test]
async fn put_start_preserves_default_and_non_default_object_data_types() {
    let service = MasterServiceImpl::new(None, None);
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "data-type:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for (index, data_type) in [
        proto::ObjectDataType::Unknown,
        proto::ObjectDataType::Weight,
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("data-type-{index}");
        let started = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.clone(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    data_type: data_type as i32,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 1);

        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.clone(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(replicas.replicas.len(), 1);
    }
}

#[tokio::test]
async fn test_batch_replica_clear_respects_client_and_segment_name() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig {
            lease_ttl: Duration::ZERO,
            ..Default::default()
        },
        Some(OpLogManager::new(Some(Box::new(InMemoryOpLog::new(64))), 0)),
    );
    let client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();

    for (index, (cid, name)) in [(client_id, "node-a:1"), (client_id, "node-b:1")]
        .into_iter()
        .enumerate()
    {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(cid)),
                segment_name: name.into(),
                size: 1024,
                base_addr: 0x100000000 + (index as u64 * 0x10000),
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch-clear-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
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
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 2);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch-clear-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(other_client_id)),
            segment_name: "node-c:1".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let sequence_before_clear = service.oplog_manager().latest_sequence();
    let cleared = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["batch-clear-key".into()],
            client_id: Some(proto_uuid(client_id)),
            segment_name: put.replicas[0].segment_name.clone(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(cleared.cleared_keys, vec!["batch-clear-key".to_string()]);
    assert_eq!(
        service.oplog_manager().latest_sequence(),
        sequence_before_clear + 1,
        "successful replica clear must publish one durable object image"
    );

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "batch-clear-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_ne!(
        replicas.replicas[0].segment_name,
        put.replicas[0].segment_name
    );

    let denied = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["batch-clear-key".into()],
            client_id: Some(proto_uuid(other_client_id)),
            segment_name: String::new(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(denied.cleared_keys.is_empty());

    let still_exists = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "batch-clear-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(still_exists.replicas.len(), 1);
}

#[tokio::test]
async fn test_timed_out_put_start_releases_dashmap_guard_before_remove() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_discard_timeout: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "stale-put:1".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let request = || {
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "stale-put-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "stale-put:1".into(),
                ..Default::default()
            }),
        })
    };
    MasterService::put_start(&service, request()).await.unwrap();

    let replacement = tokio::time::timeout(
        Duration::from_secs(1),
        MasterService::put_start(&service, request()),
    )
    .await
    .expect("stale PutStart cleanup must not deadlock")
    .unwrap()
    .into_inner();
    assert_eq!(replacement.replicas.len(), 1);
}

#[tokio::test]
async fn test_put_start_discard_timeout_removes_incomplete_memory_and_disk_replicas() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "discard-incomplete-replicas".into(),
        put_start_discard_timeout: Duration::from_millis(20),
        put_start_release_timeout: Duration::from_secs(5),
        reaper_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "discard-incomplete:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for (key, completed_type, discarded_type) in [
        (
            "discard-disk",
            proto::replica_descriptor::ReplicaType::Memory,
            proto::replica_descriptor::ReplicaType::Disk,
        ),
        (
            "discard-memory",
            proto::replica_descriptor::ReplicaType::Disk,
            proto::replica_descriptor::ReplicaType::Memory,
        ),
    ] {
        let started = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "discard-incomplete:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 2);

        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: completed_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(replicas.replicas.len(), 1);
        assert_eq!(replicas.replicas[0].replica_type, completed_type as i32);

        // Match C++ discarded_replicas_: a late completion is accepted as a
        // no-op and must not resurrect the discarded replica.
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: discarded_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let after_late_end = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(after_late_end.replicas.len(), 1);
        assert_eq!(
            after_late_end.replicas[0].replica_type,
            completed_type as i32
        );
    }
}

#[tokio::test]
async fn test_memory_and_global_disk_put_revoke_remove_parity() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "memory-disk-lifecycle".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "memory-disk-lifecycle:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    async fn start(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
        let response = MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "memory-disk-lifecycle:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(response.replicas.len(), 2);
    }

    async fn end(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        replica_type: proto::replica_descriptor::ReplicaType,
    ) {
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn revoke(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        replica_type: proto::replica_descriptor::ReplicaType,
    ) {
        MasterService::put_revoke(
            service,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn replicas(service: &MasterServiceImpl, key: &str) -> Vec<proto::ReplicaDescriptor> {
        MasterService::get_replica_list(
            service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas
    }

    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    start(&service, client_id, "complete-both").await;
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "complete-both".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    end(&service, client_id, "complete-both", Type::Memory).await;
    end(&service, client_id, "complete-both", Type::Disk).await;
    let complete = replicas(&service, "complete-both").await;
    assert_eq!(complete.len(), 2);
    assert!(
        complete
            .iter()
            .all(|replica| replica.status == Status::Complete as i32)
    );

    start(&service, client_id, "revoke-disk").await;
    end(&service, client_id, "revoke-disk", Type::Memory).await;
    revoke(&service, client_id, "revoke-disk", Type::Disk).await;
    let memory_only = replicas(&service, "revoke-disk").await;
    assert_eq!(memory_only.len(), 1);
    assert_eq!(memory_only[0].replica_type, Type::Memory as i32);

    start(&service, client_id, "revoke-memory").await;
    revoke(&service, client_id, "revoke-memory", Type::Memory).await;
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "revoke-memory".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    end(&service, client_id, "revoke-memory", Type::Disk).await;
    let disk_only = replicas(&service, "revoke-memory").await;
    assert_eq!(disk_only.len(), 1);
    assert_eq!(disk_only[0].replica_type, Type::Disk as i32);

    start(&service, client_id, "revoke-both").await;
    revoke(&service, client_id, "revoke-both", Type::Disk).await;
    revoke(&service, client_id, "revoke-both", Type::Memory).await;
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "revoke-both".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

    start(&service, client_id, "remove-both").await;
    end(&service, client_id, "remove-both", Type::Memory).await;
    end(&service, client_id, "remove-both", Type::Disk).await;
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "remove-both".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "remove-both".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn cpp_parity_global_disk_put_revoke_remove_exact_codes() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "global-disk-exact".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "global-disk-exact:1".into(),
            size: 64 * 1024 * 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    async fn start(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
    ) -> Vec<proto::ReplicaDescriptor> {
        MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "global-disk-exact:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas
    }

    async fn end(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        replica_type: proto::replica_descriptor::ReplicaType,
    ) {
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn revoke(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        replica_type: proto::replica_descriptor::ReplicaType,
    ) {
        MasterService::put_revoke(
            service,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn get(
        service: &MasterServiceImpl,
        key: &str,
    ) -> Result<Vec<proto::ReplicaDescriptor>, tonic::Status> {
        Ok(MasterService::get_replica_list(
            service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await?
        .into_inner()
        .replicas)
    }

    use proto::replica_descriptor::ReplicaType as Type;

    // PutEndBothReplica: PutStart yields exactly one Memory and one Disk.
    let both = start(&service, client_id, "exact-both").await;
    assert_eq!(both.len(), 2);
    let types = both
        .iter()
        .map(|replica| replica.replica_type)
        .collect::<std::collections::HashSet<_>>();
    assert!(types.contains(&(Type::Memory as i32)));
    assert!(types.contains(&(Type::Disk as i32)));
    end(&service, client_id, "exact-both", Type::Memory).await;
    end(&service, client_id, "exact-both", Type::Disk).await;
    assert_eq!(get(&service, "exact-both").await.unwrap().len(), 2);

    // PutRevokeDiskReplica: after Memory completion, revoking the processing
    // Disk leaves exactly one readable Memory replica.
    start(&service, client_id, "exact-revoke-disk").await;
    end(&service, client_id, "exact-revoke-disk", Type::Memory).await;
    let memory_only = get(&service, "exact-revoke-disk").await.unwrap();
    assert_eq!(memory_only.len(), 1);
    assert_eq!(memory_only[0].replica_type, Type::Memory as i32);
    revoke(&service, client_id, "exact-revoke-disk", Type::Disk).await;
    let after_revoke = get(&service, "exact-revoke-disk").await.unwrap();
    assert_eq!(after_revoke.len(), 1);
    assert_eq!(after_revoke[0].replica_type, Type::Memory as i32);

    // PutRevokeMemoryReplica: revoking Memory makes the object unreadable
    // (REPLICA_IS_NOT_READY), then Disk completion restores a Disk-only read.
    start(&service, client_id, "exact-revoke-memory").await;
    revoke(&service, client_id, "exact-revoke-memory", Type::Memory).await;
    assert_eq!(
        get(&service, "exact-revoke-memory")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    end(&service, client_id, "exact-revoke-memory", Type::Disk).await;
    let disk_only = get(&service, "exact-revoke-memory").await.unwrap();
    assert_eq!(disk_only.len(), 1);
    assert_eq!(disk_only[0].replica_type, Type::Disk as i32);

    // PutRevokeBothReplica: Disk revoke → REPLICA_IS_NOT_READY, then Memory
    // revoke → OBJECT_NOT_FOUND.
    start(&service, client_id, "exact-revoke-both").await;
    revoke(&service, client_id, "exact-revoke-both", Type::Disk).await;
    assert_eq!(
        get(&service, "exact-revoke-both").await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    revoke(&service, client_id, "exact-revoke-both", Type::Memory).await;
    assert_eq!(
        get(&service, "exact-revoke-both").await.unwrap_err().code(),
        tonic::Code::NotFound
    );

    // RemoveKey: non-force removal of a completed Memory+Disk object succeeds
    // and the key is then absent.
    start(&service, client_id, "exact-remove").await;
    end(&service, client_id, "exact-remove", Type::Memory).await;
    end(&service, client_id, "exact-remove", Type::Disk).await;
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "exact-remove".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        get(&service, "exact-remove").await.unwrap_err().code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn cpp_parity_single_disk_eviction_missing_key_is_not_found() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "evict-missing".into(),
        ..Default::default()
    });
    let error = MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "nonexistent_key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn cpp_parity_single_disk_eviction_rejects_memory_type() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "evict-memory-type".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "evict-memory-type:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "evict-key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "evict-memory-type:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "evict-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "evict-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let error = MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "evict-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn cpp_parity_single_global_disk_eviction_leaves_memory() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "evict-disk-keep-memory".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "evict-disk-keep-memory:1".into(),
            size: 64 * 1024 * 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "evict_disk_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "evict-disk-keep-memory:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
    for replica_type in [
        proto::replica_descriptor::ReplicaType::Memory,
        proto::replica_descriptor::ReplicaType::Disk,
    ] {
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "evict_disk_key".into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    let before = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evict_disk_key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(before.len(), 2);

    MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "evict_disk_key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evict_disk_key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0].replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn put_start_discard_release_and_eviction_timeline_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        cluster_id: "put-start-expiring".into(),
        put_start_discard_timeout: std::time::Duration::from_millis(200),
        put_start_release_timeout: std::time::Duration::from_millis(400),
        reaper_interval: std::time::Duration::from_millis(20),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    const SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
    const OBJECT_SIZE: u64 = 6 * 1024 * 1024;
    for index in 0..3 {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: format!("expiring-segment-{index}:1"),
                size: SEGMENT_SIZE,
                base_addr: 0x100000000 + index * SEGMENT_SIZE,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    let config = proto::ReplicateConfig {
        replica_num: 3,
        preferred_segment: "expiring-segment-0:1".into(),
        ..Default::default()
    };

    // First put succeeds with three PROCESSING replicas; a duplicate fails.
    let first = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_1".into(),
            slice_length: OBJECT_SIZE,
            tenant_id: String::new(),
            config: Some(config.clone()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first.replicas.len(), 3);
    let duplicate = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_1".into(),
            slice_length: OBJECT_SIZE,
            tenant_id: String::new(),
            config: Some(config.clone()),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(duplicate.code(), tonic::Code::AlreadyExists);

    // Wait for the discard window; the expired put is replaceable.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let second = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_1".into(),
            slice_length: OBJECT_SIZE,
            tenant_id: String::new(),
            config: Some(config.clone()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second.replicas.len(), 3);

    // Complete key_1 and protect it from eviction.
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_1".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "test_key_1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    // key_2 fails while the first attempt's replicas are pending release.
    let blocked = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_2".into(),
            slice_length: OBJECT_SIZE,
            tenant_id: String::new(),
            config: Some(config.clone()),
        }),
    )
    .await;
    if blocked.is_ok() {
        // With the first attempt already released (timing margin), complete
        // key_2 and still assert the accounting path below.
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key_2".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    } else {
        assert_eq!(blocked.unwrap_err().code(), tonic::Code::ResourceExhausted);
        // Wait past the release window, then key_2 succeeds.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let admitted = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key_2".into(),
                slice_length: OBJECT_SIZE,
                tenant_id: String::new(),
                config: Some(config.clone()),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(admitted.replicas.len(), 3);
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key_2".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "test_key_2".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(replicas.len(), 3);
}

#[tokio::test]
async fn test_global_disk_keeps_object_readable_after_memory_capacity_eviction() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "global-disk-eviction".into(),
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "global-disk-eviction:1".into(),
            size: 128,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let put = |key: &str| {
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "global-disk-eviction:1".into(),
                ..Default::default()
            }),
        })
    };
    MasterService::put_start(&service, put("evicted-to-disk"))
        .await
        .unwrap();
    for replica_type in [
        proto::replica_descriptor::ReplicaType::Memory,
        proto::replica_descriptor::ReplicaType::Disk,
    ] {
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "evicted-to-disk".into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        service.run_eviction_cycle_for_test(1),
        vec!["evicted-to-disk".to_string()]
    );
    let retained = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evicted-to-disk".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(retained.replicas.len(), 1);
    assert_eq!(
        retained.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Disk as i32
    );

    let replacement = MasterService::put_start(&service, put("reuses-memory-capacity"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replacement.replicas.len(), 2);
}

#[tokio::test]
async fn test_concurrent_initial_put_start_has_single_winner() {
    const WRITERS: usize = 32;

    let service = Arc::new(MasterServiceImpl::default());
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        service.as_ref(),
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "concurrent-put:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let barrier = Arc::new(Barrier::new(WRITERS));
    let mut writers = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        let service = Arc::clone(&service);
        let barrier = Arc::clone(&barrier);
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            MasterService::put_start(
                service.as_ref(),
                Request::new(proto::PutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: "single-winner".into(),
                    slice_length: 128,
                    tenant_id: String::new(),
                    config: Some(proto::ReplicateConfig {
                        replica_num: 1,
                        preferred_segment: "concurrent-put:1".into(),
                        ..Default::default()
                    }),
                }),
            )
            .await
        }));
    }

    let mut successes = 0;
    let mut already_exists = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(response) => {
                successes += 1;
                assert_eq!(response.into_inner().replicas.len(), 1);
            }
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::AlreadyExists);
                already_exists += 1;
            }
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(already_exists, WRITERS - 1);
}

#[tokio::test]
async fn test_overlapping_batch_put_start_uses_stable_lock_order() {
    let service = Arc::new(MasterServiceImpl::default());
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        service.as_ref(),
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "batch-lock-order:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let mut batches = Vec::new();
    for keys in [
        vec!["batch-a".to_string(), "batch-b".to_string()],
        vec!["batch-b".to_string(), "batch-a".to_string()],
    ] {
        let service = Arc::clone(&service);
        let barrier = Arc::clone(&barrier);
        batches.push(tokio::spawn(async move {
            barrier.wait().await;
            MasterService::batch_put_start(
                service.as_ref(),
                Request::new(proto::BatchPutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    keys,
                    slice_lengths: vec![128, 128],
                    config: Some(proto::ReplicateConfig {
                        replica_num: 1,
                        preferred_segment: "batch-lock-order:1".into(),
                        ..Default::default()
                    }),
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner()
        }));
    }

    let responses = tokio::time::timeout(Duration::from_secs(2), async {
        let mut responses = Vec::new();
        for batch in batches {
            responses.push(batch.await.unwrap());
        }
        responses
    })
    .await
    .expect("overlapping batches must not deadlock");

    let statuses = responses
        .iter()
        .flat_map(|response| response.results.iter().map(|result| result.status))
        .collect::<Vec<_>>();
    assert_eq!(statuses.iter().filter(|&&status| status == 0).count(), 2);
    assert_eq!(statuses.iter().filter(|&&status| status == -7).count(), 2);
}

#[tokio::test]
async fn test_hard_pinned_object_survives_eviction_cycle() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "hardpin:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for (key, with_hard_pin) in [("hard-key", true), ("normal-key", false)] {
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 512,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 0,
                    with_soft_pin: false,
                    with_hard_pin,
                    preferred_segment: String::new(),
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
            &service,
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

    let evicted = service.run_eviction_cycle_for_test(2);
    assert!(evicted.iter().any(|key| key == "normal-key"));
    assert!(!evicted.iter().any(|key| key == "hard-key"));

    let hard_key = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "hard-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(hard_key.replicas.len(), 1);

    let normal_key = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "normal-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await;
    assert!(normal_key.is_err());
}

#[tokio::test]
async fn test_copy_move_and_revoke_workflow() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    for (index, name) in ["copy-src:1", "copy-dst:1", "move-dst:1"]
        .into_iter()
        .enumerate()
    {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: name.into(),
                size: 4096,
                base_addr: 0x100000000 + (index as u64 * 0x10000),
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "copy-src:1".into(),
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
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let copy_started = MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-src:1".into(),
            targets: vec!["copy-dst:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(copy_started.targets.len(), 1);
    assert_eq!(copy_started.targets[0].segment_name, "copy-dst:1");

    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_copy = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_copy.replicas.len(), 2);

    let move_started = MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-src:1".into(),
            target: "move-dst:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(move_started.target.unwrap().segment_name, "move-dst:1");

    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_move = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_move.replicas.len(), 2);
    assert!(
        after_move
            .replicas
            .iter()
            .any(|r| r.segment_name == "copy-dst:1")
    );
    assert!(
        after_move
            .replicas
            .iter()
            .any(|r| r.segment_name == "move-dst:1")
    );
    assert!(
        !after_move
            .replicas
            .iter()
            .any(|r| r.segment_name == "copy-src:1")
    );

    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-dst:1".into(),
            targets: vec!["copy-src:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::copy_revoke(
        &service,
        Request::new(proto::CopyRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_revoke = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_revoke.replicas.len(), 2);
    assert!(
        !after_revoke
            .replicas
            .iter()
            .any(|r| r.segment_name == "copy-src:1")
    );
}

#[tokio::test]
async fn test_put_revoke_remove_all_and_storage_config() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: "/tmp/mooncake-root".into(),
        cluster_id: "cluster-a".into(),
        enable_disk_eviction: true,
        quota_bytes: 4096,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "revoke:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for key in ["remove-all-a", "remove-all-b"] {
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 0,
                    with_soft_pin: false,
                    with_hard_pin: false,
                    preferred_segment: "revoke:1".into(),
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
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "remove-all-tenant".into(),
            slice_length: 128,
            tenant_id: "tenant-a".into(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "revoke:1".into(),
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
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "remove-all-tenant".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "remove-all-tenant".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    // PutStart revoke-key without PutEnd so it stays in Allocating state for PutRevoke
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "revoke:1".into(),
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

    MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "revoke-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

    let removed = MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(removed.removed_count, 3);
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "remove-all-tenant".into(),
                tenant_id: "tenant-a".into(),
            }),
        )
        .await
        .is_err()
    );

    let storage = MasterService::get_storage_config(
        &service,
        Request::new(proto::GetStorageConfigRequest {}),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(storage.fs_dir, "/tmp/mooncake-root/cluster-a");
    assert!(storage.enable_disk_eviction);
    assert_eq!(storage.quota_bytes, 4096);
    assert!(!storage.enable_tenant_scope);
    assert_eq!(storage.memory_allocator, "offset");
    assert_eq!(storage.memory_segment_alignment, 1);
}

#[tokio::test]
async fn test_global_disk_only_put_lifecycle_uses_shared_file_descriptor() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "cluster-disk".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    let start = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "disk-only".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 0,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
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
    .unwrap()
    .into_inner();

    assert_eq!(start.replicas.len(), 1);
    let disk = &start.replicas[0];
    assert_eq!(
        disk.replica_type,
        proto::replica_descriptor::ReplicaType::Disk as i32
    );
    assert_eq!(disk.file_path, disk.segment_name);
    assert!(
        std::path::Path::new(&disk.file_path)
            .starts_with(root.path().join("cluster-disk/global-disk"))
    );
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "disk-only".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "disk-only".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let complete = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "disk-only".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(complete.replicas.len(), 1);
    assert_eq!(
        complete.replicas[0].status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );

    let eviction = MasterService::batch_evict_disk_replica(
        &service,
        Request::new(proto::BatchEvictDiskReplicaRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["disk-only".into(), "already-missing".into()],
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(eviction.statuses, [0, -1]);
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "disk-only".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn cpp_parity_global_disk_oversubscription_preserves_more_than_memory_capacity() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "global-disk-oversubscription".into(),
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    const SEGMENT_SIZE: u64 = 1024 * 1024 * 16 * 15; // 240 MiB, C++ EvictObject
    const OBJECT_SIZE: u64 = 1024 * 15; // 15 KiB
    const ATTEMPTS: usize = 1024 * 16 + 50; // 16,434
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "global-disk-oversubscription:1".into(),
            size: SEGMENT_SIZE,
            base_addr: 0x300000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let mut success_puts = 0usize;
    for index in 0..ATTEMPTS {
        let key = format!("test_key{index}");
        let started = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.clone(),
                slice_length: OBJECT_SIZE,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "global-disk-oversubscription:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await;
        match started {
            Ok(_) => {
                // PutEnd(MEMORY) then PutEnd(DISK) both succeed like C++.
                MasterService::put_end(
                    &service,
                    Request::new(proto::PutEndRequest {
                        client_id: Some(proto_uuid(client_id)),
                        key: key.clone(),
                        replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                        tenant_id: String::new(),
                    }),
                )
                .await
                .unwrap();
                MasterService::put_end(
                    &service,
                    Request::new(proto::PutEndRequest {
                        client_id: Some(proto_uuid(client_id)),
                        key,
                        replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                        tenant_id: String::new(),
                    }),
                )
                .await
                .unwrap();
                success_puts += 1;
            }
            Err(status) => {
                // Memory is full: C++ waits for the background eviction
                // worker; Rust advances one deterministic eviction cycle.
                assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                service.run_eviction_cycle_for_test(1);
            }
        }
    }
    // Eviction processing must let strictly more than the memory capacity of
    // 16,384 objects survive because every put also has a global Disk replica.
    assert!(success_puts > 1024 * 16, "success_puts = {success_puts}");

    let mut success_gets = 0usize;
    for index in 0..ATTEMPTS {
        let key = format!("test_key{index}");
        if MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key,
                tenant_id: String::new(),
            }),
        )
        .await
        .is_ok()
        {
            success_gets += 1;
        }
    }
    assert!(success_gets > 1024 * 16, "success_gets = {success_gets}");

    MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn cpp_parity_put_start_expires_exact_16_mib_flows() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "put-start-expires-16mib".into(),
        // C++ PutStartExpires runs with the default 10,000 ms key lease TTL;
        // the per-second Ping/GetReplicaList calls keep the key leased and
        // therefore protect it from automatic eviction while waiting.
        lease_ttl: Duration::from_secs(10),
        put_start_discard_timeout: Duration::from_millis(300),
        put_start_release_timeout: Duration::from_millis(500),
        reaper_interval: Duration::from_millis(20),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    const SEGMENT_SIZE: u64 = 1024 * 1024 * 16; // 16 MiB, C++ PutStartExpires
    const OBJECT_SIZE: u64 = 1024 * 1024 * 16;
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "put-start-expires:1".into(),
            size: SEGMENT_SIZE,
            base_addr: 0x300000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    use proto::replica_descriptor::ReplicaType as Type;
    for (discard_type, reserve_type) in [(Type::Disk, Type::Memory), (Type::Memory, Type::Disk)] {
        let started = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key".into(),
                slice_length: OBJECT_SIZE,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "put-start-expires:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 2);
        for replica in &started.replicas {
            assert_eq!(
                replica.status,
                proto::replica_descriptor::ReplicaStatus::Allocating as i32
            );
        }

        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key".into(),
                replica_type: reserve_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();

        // Grant and keep the key lease like C++'s Ping/GetReplicaList loop so
        // the automatic eviction worker cannot reclaim the full 16-MiB object.
        let keep_leased = || async {
            let _ = MasterService::get_replica_list(
                &service,
                Request::new(proto::GetReplicaListRequest {
                    key: "test_key".into(),
                    tenant_id: String::new(),
                }),
            )
            .await;
        };
        keep_leased().await;

        // Past the discard window the object keeps its completed replica, so a
        // second PutStart fails OBJECT_ALREADY_EXISTS (C++ PutStartExpires).
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            keep_leased().await;
        }
        let duplicate = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key".into(),
                slice_length: OBJECT_SIZE,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "put-start-expires:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(duplicate.code(), tonic::Code::AlreadyExists);

        // Past the release window a late completion of the discarded replica
        // is a no-op and the object still exposes exactly the reserve replica.
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            keep_leased().await;
        }
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "test_key".into(),
                replica_type: discard_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas;
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].replica_type, reserve_type as i32);

        MasterService::remove_all(
            &service,
            Request::new(proto::RemoveAllRequest {
                // C++ reaches RemoveAll only after the 10 s TTL lease expires;
                // force mirrors that cleanup without a wall-clock TTL wait.
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
}
