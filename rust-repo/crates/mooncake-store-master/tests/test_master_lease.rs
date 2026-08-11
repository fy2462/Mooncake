//! C++ `MasterServiceTest.RemoveLeasedObject` / `RemoveAllLeasedObject` parity.

mod common;
use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::{Code, Request};
use uuid::Uuid;

fn new_service(lease_ttl_ms: u64) -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(lease_ttl_ms),
        ..Default::default()
    })
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 1024 * 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_object(service: &MasterServiceImpl, client_id: Uuid, key: &str, segment: &str) {
    let started = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: segment.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), 1);

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

async fn remove(service: &MasterServiceImpl, key: &str) -> Result<(), Code> {
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

async fn get_replica_list(service: &MasterServiceImpl, key: &str) -> Result<(), Code> {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn remove_all(service: &MasterServiceImpl) -> i64 {
    MasterService::remove_all(
        service,
        Request::new(proto::RemoveAllRequest {
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .removed_count
}

#[tokio::test]
async fn exist_and_get_refresh_remove_lease_matrix_parity() {
    const TTL_MS: u64 = 50;
    let service = new_service(TTL_MS);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "lease:1").await;

    // Sequence 1: ExistKey grants a lease; Remove fails, then succeeds after TTL.
    put_object(&service, client_id, "test_key", "lease:1").await;
    assert!(exists(&service, "test_key").await);
    assert_eq!(
        remove(&service, "test_key").await,
        Err(Code::FailedPrecondition)
    );
    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert_eq!(remove(&service, "test_key").await, Ok(()));

    // Sequence 2: successive ExistKey refreshes the lease.
    put_object(&service, client_id, "test_key", "lease:1").await;
    assert!(exists(&service, "test_key").await);
    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert!(exists(&service, "test_key").await);
    assert_eq!(
        remove(&service, "test_key").await,
        Err(Code::FailedPrecondition)
    );
    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert_eq!(remove(&service, "test_key").await, Ok(()));

    // Sequence 3: GetReplicaList grants a lease; Remove fails, then succeeds.
    put_object(&service, client_id, "test_key", "lease:1").await;
    assert_eq!(get_replica_list(&service, "test_key").await, Ok(()));
    assert_eq!(
        remove(&service, "test_key").await,
        Err(Code::FailedPrecondition)
    );
    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert_eq!(remove(&service, "test_key").await, Ok(()));

    // Sequence 4: successive GetReplicaList refreshes the lease.
    put_object(&service, client_id, "test_key", "lease:1").await;
    assert_eq!(get_replica_list(&service, "test_key").await, Ok(()));
    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert_eq!(get_replica_list(&service, "test_key").await, Ok(()));
    assert_eq!(
        remove(&service, "test_key").await,
        Err(Code::FailedPrecondition)
    );
    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert_eq!(remove(&service, "test_key").await, Ok(()));

    // Final object is gone.
    assert_eq!(
        get_replica_list(&service, "test_key").await,
        Err(Code::NotFound)
    );
}

#[tokio::test]
async fn remove_all_skips_then_removes_leased_half_parity() {
    const TTL_MS: u64 = 50;
    let service = new_service(TTL_MS);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "lease-all:1").await;

    for index in 0..10 {
        let key = format!("test_key{index}");
        put_object(&service, client_id, &key, "lease-all:1").await;
        if index >= 5 {
            assert!(exists(&service, &key).await);
        }
    }

    assert_eq!(remove_all(&service).await, 5);
    for index in 0..5 {
        assert!(!exists(&service, &format!("test_key{index}")).await);
    }

    tokio::time::sleep(Duration::from_millis(TTL_MS + 10)).await;
    assert_eq!(remove_all(&service).await, 5);
    for index in 5..10 {
        assert!(!exists(&service, &format!("test_key{index}")).await);
    }
}
