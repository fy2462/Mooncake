use mooncake_store_core::ReplicaType;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::MasterServiceImpl;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_offload_object_heartbeat_and_notify_offload_success() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "mem-a".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "offload-key".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "mem-a".into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "offload-key".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, ""),
        vec![1]
    );

    let heartbeat = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(heartbeat.objects.get("offload-key"), Some(&256));

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["offload-key".into(), "disk-only-key".into()],
            metadatas: vec![
                proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: "offload-key".len() as i64,
                    data_size: 256,
                    transport_endpoint: "holder-a".into(),
                },
                proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: "disk-only-key".len() as i64,
                    data_size: 512,
                    transport_endpoint: "holder-a".into(),
                },
            ],
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, ""),
        vec![0]
    );

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "disk-only-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_eq!(
        replicas.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::LocalDisk as i32
    );
    assert_eq!(
        replicas.replicas[0].holder_client_id.as_ref().unwrap().high,
        client_id.as_u64_pair().0
    );
}
