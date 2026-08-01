mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::{Code, Request};
use uuid::Uuid;

fn leased_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(10),
        ..Default::default()
    })
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "segment_0".into(),
            size: 16 * 1024 * 1024,
            base_addr: 0x300000000,
            te_endpoint: "segment_0".into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_and_grant_lease(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 1024,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                ..Default::default()
            }),
            tenant_id: String::new(),
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
    assert!(exists(service, key).await);
}

async fn exists(service: &MasterServiceImpl, key: &str) -> bool {
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

#[tokio::test]
async fn force_remove_leased_object_parity() {
    let service = leased_service();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id).await;
    put_and_grant_lease(&service, client_id, "leased_key").await;

    let leased = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "leased_key".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(leased.code(), Code::FailedPrecondition);
    assert_eq!(leased.message(), "object has lease");

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "leased_key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let missing = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "leased_key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);
}

#[tokio::test]
async fn force_remove_by_regex_leased_objects_parity() {
    let service = leased_service();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id).await;
    let keys = (0..5)
        .map(|index| format!("force_regex_key_{index}"))
        .collect::<Vec<_>>();
    for key in &keys {
        put_and_grant_lease(&service, client_id, key).await;
    }

    let skipped = MasterService::remove_by_regex(
        &service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: "^force_regex_key_".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(skipped.removed_count, 0);
    for key in &keys {
        assert!(exists(&service, key).await);
    }

    let removed = MasterService::remove_by_regex(
        &service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: "^force_regex_key_".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(removed.removed_count, 5);
    for key in &keys {
        assert!(!exists(&service, key).await);
    }
}

#[tokio::test]
async fn force_remove_all_leased_objects_parity() {
    let service = leased_service();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id).await;
    let keys = (0..10)
        .map(|index| format!("force_all_key_{index}"))
        .collect::<Vec<_>>();
    for key in &keys {
        put_and_grant_lease(&service, client_id, key).await;
    }

    let skipped = MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(skipped.removed_count, 0);
    for key in &keys {
        assert!(exists(&service, key).await);
    }

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
    assert_eq!(removed.removed_count, 10);
    for key in &keys {
        assert!(!exists(&service, key).await);
    }
}
