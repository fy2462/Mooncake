mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::tenant_quota_policy_store::{
    TenantQuotaPolicySnapshot, save_tenant_quota_policy,
};
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{Code, Request};
use uuid::Uuid;

const ENDPOINTS_ENV: &str = "MOONCAKE_TENANT_QUOTA_ETCD_ENDPOINTS";
static NEXT_SEGMENT_BASE: AtomicU64 = AtomicU64::new(0x7_8000_0000);

struct LiveEtcdFixture {
    endpoints: String,
    cluster_id: String,
}

impl LiveEtcdFixture {
    fn new(label: &str) -> Option<Self> {
        let endpoints = std::env::var(ENDPOINTS_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty());
        let Some(endpoints) = endpoints else {
            eprintln!("skipping live tenant-quota parity; set {ENDPOINTS_ENV}");
            return None;
        };
        let fixture = Self {
            endpoints,
            cluster_id: format!(
                "tenant_quota_{label}_{}_{}",
                std::process::id(),
                Uuid::new_v4().simple()
            ),
        };
        fixture.delete_key().expect("clean etcd key before test");
        Some(fixture)
    }

    fn save(&self, producer_view_version: u64, policies: &[(&str, u64)]) {
        save_tenant_quota_policy(
            "etcd",
            &self.endpoints,
            &self.cluster_id,
            &TenantQuotaPolicySnapshot {
                producer_view_version,
                tenant_quotas: policies
                    .iter()
                    .map(|(tenant, quota)| ((*tenant).to_owned(), *quota))
                    .collect::<BTreeMap<_, _>>(),
            },
        )
        .expect("persist tenant-quota policy");
    }

    fn service(&self) -> MasterServiceImpl {
        MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_connector_type: "etcd".to_owned(),
            tenant_quota_connector_uri: self.endpoints.clone(),
            cluster_id: self.cluster_id.clone(),
            tenant_quota_pool_capacity_bytes: 2_000,
            lease_ttl: std::time::Duration::ZERO,
            ..Default::default()
        })
    }

    fn reload_without_tenant_b(&self, service: &MasterServiceImpl) {
        self.save(0, &[("tenant-a", 1_000)]);
        service.set_leadership_view_version(1);
        service
            .prepare_tenant_quota_leadership_term(1)
            .expect("advance connector term and apply replacement policy");
    }

    fn delete_key(&self) -> Result<(), String> {
        let endpoints = self
            .endpoints
            .split(';')
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let key = format!("mooncake-store/{}/tenant_quota_policy", self.cluster_id);
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("create cleanup runtime: {error}"))?
                .block_on(async move {
                    let mut client = etcd_client::Client::connect(endpoints, None)
                        .await
                        .map_err(|error| format!("connect cleanup client: {error}"))?;
                    client
                        .delete(key, None)
                        .await
                        .map_err(|error| format!("delete fixture key: {error}"))?;
                    Ok(())
                })
        })
        .join()
        .map_err(|_| "cleanup worker panicked".to_owned())?
    }
}

impl Drop for LiveEtcdFixture {
    fn drop(&mut self) {
        if let Err(error) = self.delete_key() {
            eprintln!("failed to clean tenant-quota fixture: {error}");
        }
    }
}

async fn mount_memory(service: &MasterServiceImpl, client_id: Uuid, segment: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment.to_owned(),
            size: 4_096,
            base_addr: NEXT_SEGMENT_BASE.fetch_add(0x10_000, Ordering::Relaxed),
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .expect("mount ordinary memory segment");
}

async fn put_memory(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment: &str,
    key: &str,
    size: u64,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_owned(),
            slice_length: size,
            tenant_id: "tenant-b".to_owned(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: segment.to_owned(),
                ..Default::default()
            }),
        }),
    )
    .await
    .expect("put start");
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_owned(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-b".to_owned(),
        }),
    )
    .await
    .expect("put end");
}

async fn report_unsolicited_disk(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: i64,
) -> Result<(), tonic::Status> {
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: size,
                transport_endpoint: "disk-endpoint".to_owned(),
            }],
            tasks: vec![proto::OffloadTaskItem {
                tenant_id: "tenant-b".to_owned(),
                key: key.to_owned(),
                size,
                generation_id: None,
            }],
            recovery_session_id: None,
        }),
    )
    .await
    .map(|_| ())
}

async fn force_remove(service: &MasterServiceImpl, key: &str) {
    MasterService::remove(
        service,
        Request::new(proto::RemoveRequest {
            key: key.to_owned(),
            force: true,
            tenant_id: "tenant-b".to_owned(),
        }),
    )
    .await
    .expect("force remove orphan");
}

#[tokio::test]
async fn cpp_parity_etcd_reload_preserves_classic_disk_only_orphan_until_cleanup() {
    let Some(fixture) = LiveEtcdFixture::new("disk_orphan") else {
        return;
    };
    fixture.save(0, &[("tenant-a", 1_000), ("tenant-b", 1_000)]);
    let service = fixture.service();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "classic-disk-orphan:1").await;
    report_unsolicited_disk(&service, client_id, "cold", 128)
        .await
        .expect("registered classic unsolicited offload");

    fixture.reload_without_tenant_b(&service);

    let orphan = service
        .get_tenant_quota_snapshot("tenant-b")
        .unwrap()
        .unwrap();
    assert!(!orphan.has_explicit_policy);
    assert_eq!(orphan.used_bytes, 0);
    assert_eq!(orphan.committed_count, 0);
    assert_eq!(orphan.metadata_object_count, 1);
    assert!(orphan.over_quota);
    force_remove(&service, "cold").await;
    assert!(
        service
            .get_tenant_quota_snapshot("tenant-b")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cpp_parity_etcd_removed_orphan_without_task_or_disk_session_rejects_offload() {
    let Some(fixture) = LiveEtcdFixture::new("reject_orphan") else {
        return;
    };
    fixture.save(0, &[("tenant-a", 1_000), ("tenant-b", 1_000)]);
    let service = fixture.service();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "rejected-orphan:1").await;
    put_memory(&service, client_id, "rejected-orphan:1", "warming", 128).await;
    fixture.reload_without_tenant_b(&service);
    assert!(
        !service
            .get_tenant_quota_snapshot("tenant-b")
            .unwrap()
            .unwrap()
            .has_explicit_policy
    );

    let error = report_unsolicited_disk(&service, client_id, "warming", 128)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(error.message(), "tenant not registered");
}

#[tokio::test]
async fn cpp_parity_etcd_reload_preserves_readable_memory_orphan_and_rejects_new_writes() {
    let Some(fixture) = LiveEtcdFixture::new("memory_orphan") else {
        return;
    };
    fixture.save(0, &[("tenant-a", 1_000), ("tenant-b", 1_000)]);
    let service = fixture.service();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "memory-orphan:1").await;
    put_memory(&service, client_id, "memory-orphan:1", "orphan-key", 100).await;

    fixture.reload_without_tenant_b(&service);

    let orphan = service
        .get_tenant_quota_snapshot("tenant-b")
        .unwrap()
        .unwrap();
    assert!(!orphan.has_explicit_policy);
    assert_eq!(orphan.requested_quota_bytes, 0);
    assert_eq!(orphan.effective_quota_bytes, 0);
    assert!(orphan.over_quota);
    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "orphan-key".to_owned(),
            tenant_id: "tenant-b".to_owned(),
        }),
    )
    .await
    .expect("orphan remains readable");

    let error = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "new-key".to_owned(),
            slice_length: 1,
            tenant_id: "tenant-b".to_owned(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "memory-orphan:1".to_owned(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(error.message(), "tenant not registered");
    force_remove(&service, "orphan-key").await;
}
