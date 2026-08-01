mod common;

use common::proto_uuid;
use mooncake_store_master::allocator::AllocationStrategy;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::Request;
use uuid::Uuid;

fn local_first_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::LocalFirst,
        ..Default::default()
    })
}

async fn mount_host_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    host_id: &str,
    base_addr: u64,
    size: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size,
            base_addr,
            te_endpoint: segment_name.into(),
            protocol: String::new(),
            host_id: host_id.into(),
        }),
    )
    .await
    .unwrap();
}

async fn start_local_first_put(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    host_id: &str,
    preferred_segment: &str,
) -> proto::ReplicaDescriptor {
    let response = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                host_id: host_id.into(),
                preferred_segment: preferred_segment.into(),
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

async fn end_memory_put(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
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

#[tokio::test]
async fn local_first_put_prefers_writer_host_parity() {
    let service = local_first_service();
    let client_id = Uuid::new_v4();
    mount_host_segment(
        &service,
        client_id,
        "segment_host0",
        "host0",
        0x300000000,
        16 * 1024 * 1024,
    )
    .await;
    mount_host_segment(
        &service,
        client_id,
        "segment_host1",
        "host1",
        0x400000000,
        16 * 1024 * 1024,
    )
    .await;

    let replica =
        start_local_first_put(&service, client_id, "local_first_key", 1024, "host1", "").await;
    assert_eq!(replica.transport_endpoint, "segment_host1");
}

#[tokio::test]
async fn local_first_falls_back_to_next_ordered_host_parity() {
    let service = local_first_service();
    let client_id = Uuid::new_v4();
    mount_host_segment(
        &service,
        client_id,
        "segment_host0",
        "host0",
        0x300000000,
        16 * 1024 * 1024,
    )
    .await;
    mount_host_segment(
        &service,
        client_id,
        "segment_host2",
        "host2",
        0x400000000,
        16 * 1024 * 1024,
    )
    .await;

    let replica = start_local_first_put(
        &service,
        client_id,
        "ordered_fallback_key",
        1024,
        "host1",
        "",
    )
    .await;
    assert_eq!(replica.transport_endpoint, "segment_host2");
}

#[tokio::test]
async fn local_first_falls_back_when_local_segment_is_full_parity() {
    let service = local_first_service();
    let client_id = Uuid::new_v4();
    mount_host_segment(
        &service,
        client_id,
        "segment_host1",
        "host1",
        0x300000000,
        1024,
    )
    .await;
    mount_host_segment(
        &service,
        client_id,
        "segment_host2",
        "host2",
        0x400000000,
        16 * 1024 * 1024,
    )
    .await;

    let fill =
        start_local_first_put(&service, client_id, "fill_local_segment", 1024, "host1", "").await;
    assert_eq!(fill.transport_endpoint, "segment_host1");
    end_memory_put(&service, client_id, "fill_local_segment").await;

    let fallback = start_local_first_put(
        &service,
        client_id,
        "fallback_after_local_full",
        1,
        "host1",
        "",
    )
    .await;
    assert_eq!(fallback.transport_endpoint, "segment_host2");
}

#[tokio::test]
async fn explicit_preferred_segment_overrides_local_first_parity() {
    let service = local_first_service();
    let client_id = Uuid::new_v4();
    mount_host_segment(
        &service,
        client_id,
        "segment_host0",
        "host0",
        0x300000000,
        16 * 1024 * 1024,
    )
    .await;
    mount_host_segment(
        &service,
        client_id,
        "segment_host1",
        "host1",
        0x400000000,
        16 * 1024 * 1024,
    )
    .await;

    let replica = start_local_first_put(
        &service,
        client_id,
        "explicit_preferred_key",
        1024,
        "host1",
        "segment_host0",
    )
    .await;
    assert_eq!(replica.transport_endpoint, "segment_host0");
}

#[tokio::test]
async fn explicit_preferred_segment_falls_back_to_local_first_parity() {
    let service = local_first_service();
    let client_id = Uuid::new_v4();
    mount_host_segment(
        &service,
        client_id,
        "segment_host0",
        "host0",
        0x300000000,
        1024,
    )
    .await;
    mount_host_segment(
        &service,
        client_id,
        "segment_host1",
        "host1",
        0x400000000,
        16 * 1024 * 1024,
    )
    .await;

    let fill = start_local_first_put(
        &service,
        client_id,
        "fill_preferred",
        1024,
        "host1",
        "segment_host0",
    )
    .await;
    assert_eq!(fill.transport_endpoint, "segment_host0");
    end_memory_put(&service, client_id, "fill_preferred").await;

    let fallback = start_local_first_put(
        &service,
        client_id,
        "fallback_after_preferred_full",
        1,
        "host1",
        "segment_host0",
    )
    .await;
    assert_eq!(fallback.transport_endpoint, "segment_host1");
}
