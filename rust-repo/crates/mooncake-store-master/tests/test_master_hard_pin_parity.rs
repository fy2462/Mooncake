mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

const SEGMENT: &str = "segment_0";
const MIB: u64 = 1024 * 1024;

async fn mount_segment(service: &MasterServiceImpl, owner: Uuid) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(owner)),
            segment_name: SEGMENT.into(),
            size: 16 * MIB,
            base_addr: 0x300000000,
            te_endpoint: SEGMENT.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn config(with_soft_pin: bool, with_hard_pin: bool) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        with_soft_pin,
        with_hard_pin,
        preferred_segment: SEGMENT.into(),
        ..Default::default()
    }
}

async fn put(
    service: &MasterServiceImpl,
    writer: Uuid,
    key: &str,
    size: u64,
    with_soft_pin: bool,
    with_hard_pin: bool,
) -> Result<(), tonic::Status> {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(writer)),
            key: key.into(),
            slice_length: size,
            config: Some(config(with_soft_pin, with_hard_pin)),
            tenant_id: String::new(),
        }),
    )
    .await?;
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(writer)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await?;
    Ok(())
}

async fn get(service: &MasterServiceImpl, key: &str) -> Result<(), tonic::Status> {
    let response = MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await?
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    Ok(())
}

async fn apply_pressure(service: &MasterServiceImpl, writer: Uuid, key_prefix: &str) {
    for index in 0..20 {
        let key = format!("{key_prefix}_{index}");
        let _ = put(service, writer, &key, MIB, false, false).await;
    }
}

#[tokio::test]
async fn hard_pin_object_not_evicted_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(200),
        ..Default::default()
    });
    let owner = Uuid::new_v4();
    let writer = Uuid::new_v4();
    mount_segment(&service, owner).await;

    put(&service, writer, "hard_pinned_model", MIB, false, true)
        .await
        .unwrap();
    apply_pressure(&service, writer, "normal_model").await;
    tokio::time::sleep(Duration::from_millis(700)).await;

    get(&service, "hard_pinned_model").await.unwrap();
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "hard_pinned_model".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "hard_pinned_model".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!exists.exists);
}

#[tokio::test]
async fn hard_pin_with_soft_pin_eviction_order_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(200),
        soft_pin_ttl: Duration::from_secs(10),
        allow_evict_soft_pinned_objects: true,
        eviction_ratio: 0.5,
        ..Default::default()
    });
    let owner = Uuid::new_v4();
    let writer = Uuid::new_v4();
    mount_segment(&service, owner).await;

    put(&service, writer, "hard_pinned", MIB, false, true)
        .await
        .unwrap();
    put(&service, writer, "soft_pinned", MIB, true, false)
        .await
        .unwrap();
    apply_pressure(&service, writer, "normal").await;
    tokio::time::sleep(Duration::from_millis(700)).await;

    get(&service, "hard_pinned").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hard_pin_default_is_false_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(5),
        ..Default::default()
    });
    let owner = Uuid::new_v4();
    let writer = Uuid::new_v4();
    mount_segment(&service, owner).await;

    put(&service, writer, "normal_key", 1024, false, false)
        .await
        .unwrap();
    get(&service, "normal_key").await.unwrap();
    put(&service, writer, "hard_pinned_key", 1024, false, true)
        .await
        .unwrap();
    get(&service, "hard_pinned_key").await.unwrap();
}
