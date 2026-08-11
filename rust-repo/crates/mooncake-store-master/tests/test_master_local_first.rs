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
async fn cpp_parity_host_ordered_segments_tracks_status_and_unmount() {
    let service = local_first_service();
    let client_id = Uuid::new_v4();

    async fn mount(
        service: &MasterServiceImpl,
        client_id: Uuid,
        name: &str,
        host: &str,
        base: u64,
    ) -> Uuid {
        let proto_id = MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: name.into(),
                size: 16 * 1024 * 1024,
                base_addr: base,
                te_endpoint: name.into(),
                protocol: String::new(),
                host_id: host.into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .expect("mount must return a segment id");
        Uuid::from_u64_pair(proto_id.high, proto_id.low)
    }

    let host1_segment_id = mount(&service, client_id, "host1_segment", "host1", 0x400000000).await;
    mount(&service, client_id, "host0_segment", "host0", 0x300000000).await;

    // Active host1 segment is the sole preferred candidate for host1 writes.
    let initial =
        start_local_first_put(&service, client_id, "status_key_initial", 1024, "host1", "").await;
    assert_eq!(initial.transport_endpoint, "host1_segment");

    // A drain job moves host1_segment to DRAINING; host-ordered selection now
    // excludes it and leaves host0_segment as the candidate.
    MasterService::create_drain_job(
        &service,
        Request::new(proto::CreateDrainJobRequest {
            segments: vec!["host1_segment".into()],
            target_segments: vec!["host0_segment".into()],
            max_concurrency: 1,
        }),
    )
    .await
    .unwrap();
    let during_drain =
        start_local_first_put(&service, client_id, "status_key_drain", 1024, "host1", "").await;
    assert_eq!(during_drain.transport_endpoint, "host0_segment");

    // After unmounting the host1 segment, only host0 remains.
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(host1_segment_id)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    let after_unmount = start_local_first_put(
        &service,
        client_id,
        "status_key_after_unmount",
        1024,
        "host1",
        "",
    )
    .await;
    assert_eq!(after_unmount.transport_endpoint, "host0_segment");
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
