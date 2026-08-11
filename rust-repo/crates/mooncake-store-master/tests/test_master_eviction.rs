//! C++ `MasterServiceTest` eviction and soft-pin parity.

mod common;
use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

fn new_service(
    lease_ttl: Duration,
    soft_pin_ttl: Duration,
    allow_evict_soft_pinned: bool,
) -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl,
        soft_pin_ttl,
        allow_evict_soft_pinned_objects: allow_evict_soft_pinned,
        eviction_interval: Duration::from_millis(5),
        ..Default::default()
    })
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, size: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size,
            base_addr: 0x300000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    soft_pin: bool,
) -> bool {
    let started = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: soft_pin,
                ..Default::default()
            }),
        }),
    )
    .await;
    let Ok(response) = started else {
        return false;
    };
    if response.into_inner().replicas.is_empty() {
        return false;
    }
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
    true
}

async fn get_replica_list(service: &MasterServiceImpl, key: &str) -> bool {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .is_ok()
}

async fn remove_all(service: &MasterServiceImpl) -> i64 {
    MasterService::remove_all(
        service,
        Request::new(proto::RemoveAllRequest {
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .removed_count
}

#[tokio::test]
async fn service_eviction_admits_more_than_static_capacity_parity() {
    let service = new_service(Duration::from_millis(2000), Duration::from_secs(10), true);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16 * 15).await;

    let mut success_puts = 0usize;
    for index in 0..16_434 {
        let key = format!("test_key{index}");
        if put_complete(&service, client_id, &key, 1024 * 15, false).await {
            success_puts += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    assert!(success_puts > 16_384);
}

#[tokio::test]
async fn leased_objects_survive_pressure_parity() {
    let service = new_service(Duration::from_millis(500), Duration::from_secs(10), true);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16).await;

    let mut success_puts = 0usize;
    let mut failed_puts = 0usize;
    let mut leased_keys = Vec::new();
    for index in 0..26 {
        let key = format!("test_key{index}");
        if put_complete(&service, client_id, &key, 1024 * 1024, false).await {
            assert!(get_replica_list(&service, &key).await);
            leased_keys.push(key);
            success_puts += 1;
        } else {
            failed_puts += 1;
        }
    }
    assert!(success_puts > 0);
    assert!(failed_puts > 0);

    tokio::time::sleep(Duration::from_millis(50)).await;
    for key in &leased_keys {
        assert!(
            get_replica_list(&service, key).await,
            "{key} should survive"
        );
    }
}

#[tokio::test]
async fn soft_pin_does_not_block_explicit_remove_parity() {
    let service = new_service(Duration::from_millis(200), Duration::from_secs(10), true);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16).await;

    assert!(put_complete(&service, client_id, "test_key", 1024, true).await);
    assert!(
        MasterService::remove(
            &service,
            Request::new(proto::RemoveRequest {
                key: "test_key".into(),
                force: false,
                tenant_id: String::new(),
            }),
        )
        .await
        .is_ok()
    );

    assert!(put_complete(&service, client_id, "test_key", 1024, true).await);
    assert_eq!(remove_all(&service).await, 1);
}

#[tokio::test]
async fn soft_pins_are_second_priority_across_five_pressure_rounds_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(200),
        soft_pin_ttl: Duration::from_secs(10),
        allow_evict_soft_pinned_objects: true,
        eviction_ratio: 0.5,
        eviction_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16).await;

    for _ in 0..5 {
        for index in 0..2 {
            assert!(
                put_complete(
                    &service,
                    client_id,
                    &format!("pin_key{index}"),
                    1024 * 1024,
                    true,
                )
                .await
            );
        }

        let mut failed_puts = 0usize;
        for index in 0..20 {
            if !put_complete(
                &service,
                client_id,
                &format!("key{index}"),
                1024 * 1024,
                false,
            )
            .await
            {
                failed_puts += 1;
            }
        }
        assert!(failed_puts > 0);

        tokio::time::sleep(Duration::from_millis(1200)).await;
        for index in 0..2 {
            assert!(get_replica_list(&service, &format!("pin_key{index}")).await);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        remove_all(&service).await;
    }
}

#[tokio::test]
async fn soft_pinned_pressure_reuses_capacity_when_allowed_parity() {
    let service = new_service(Duration::from_millis(200), Duration::from_secs(10), true);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16).await;

    let mut success_puts = 0usize;
    for index in 0..66 {
        let key = format!("test_key{index}");
        if put_complete(&service, client_id, &key, 1024 * 1024, true).await {
            success_puts += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert!(success_puts > 16);
}

#[tokio::test]
async fn get_refreshes_expired_soft_pin_under_pressure_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(200),
        soft_pin_ttl: Duration::from_millis(1000),
        allow_evict_soft_pinned_objects: true,
        eviction_ratio: 0.5,
        eviction_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16).await;

    for _ in 0..3 {
        for index in 0..2 {
            assert!(
                put_complete(
                    &service,
                    client_id,
                    &format!("pin_key{index}"),
                    1024 * 1024,
                    true,
                )
                .await
            );
        }

        tokio::time::sleep(Duration::from_millis(1000)).await;
        for index in 0..2 {
            assert!(get_replica_list(&service, &format!("pin_key{index}")).await);
        }

        let mut failed_puts = 0usize;
        for index in 0..16 {
            if !put_complete(
                &service,
                client_id,
                &format!("key{index}"),
                1024 * 1024,
                false,
            )
            .await
            {
                failed_puts += 1;
            }
        }
        assert!(failed_puts > 0);

        tokio::time::sleep(Duration::from_millis(200)).await;
        for index in 0..2 {
            assert!(get_replica_list(&service, &format!("pin_key{index}")).await);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        remove_all(&service).await;
    }
}

#[tokio::test]
async fn disabled_soft_pin_eviction_caps_admission_and_preserves_keys_parity() {
    let service = new_service(Duration::from_millis(200), Duration::from_secs(10), false);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "test_segment", 1024 * 1024 * 16).await;

    let mut success_keys = Vec::new();
    for index in 0..66 {
        let key = format!("test_key{index}");
        if put_complete(&service, client_id, &key, 1024 * 1024, true).await {
            success_keys.push(key);
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert!(success_keys.len() <= 17);
    for key in &success_keys {
        assert!(
            get_replica_list(&service, key).await,
            "{key} should survive"
        );
    }
}
