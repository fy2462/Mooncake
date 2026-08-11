mod common;

use common::proto_uuid;
use mooncake_store_master::ha::LoadedSnapshot;
use mooncake_store_master::metrics;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::storage_backend::StorageBackendType;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::path::PathBuf;
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

const SEGMENT_NAME: &str = "test_segment_restore_cache_totals";
const KEY: &str = "restore_cache_total_metric_key";

struct ResetCacheTotals {
    memory: i64,
    disk: i64,
}

impl Drop for ResetCacheTotals {
    fn drop(&mut self) {
        metrics::MEM_CACHE_TOTAL.set(self.memory);
        metrics::FILE_CACHE_TOTAL.set(self.disk);
    }
}

fn runtime_config(global_disk_root: &std::path::Path) -> MasterRuntimeConfig {
    MasterRuntimeConfig {
        storage_fs_dir: global_disk_root.to_string_lossy().into_owned(),
        cluster_id: "ssd-cache-total-snapshot-cluster".into(),
        enable_disk_eviction: true,
        quota_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    }
}

fn snapshot_service(
    snapshot_root: &std::path::Path,
    config: MasterRuntimeConfig,
) -> MasterServiceImpl {
    MasterServiceImpl::new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(PathBuf::from(snapshot_root)),
        config,
    )
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: SEGMENT_NAME.into(),
            size: 64 * 1024 * 1024,
            base_addr: 0x330000000,
            te_endpoint: SEGMENT_NAME.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn replica_list(service: &MasterServiceImpl) -> Vec<proto::ReplicaDescriptor> {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: KEY.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas
}

async fn save_snapshot_and_wait(service: &MasterServiceImpl) {
    let successes = metrics::SNAPSHOT_SUCCESS_COUNT.get();
    service.save_snapshot();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if metrics::SNAPSHOT_SUCCESS_COUNT.get() > successes {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native snapshot save must finish");
}

fn assert_durable_inventory_matches(first: &LoadedSnapshot, second: &LoadedSnapshot) {
    assert_eq!(second.objects.len(), first.objects.len());
    assert_eq!(second.segments.len(), first.segments.len());
    assert_eq!(second.tasks.len(), first.tasks.len());
    assert_eq!(
        second.local_disk_segments.len(),
        first.local_disk_segments.len()
    );

    let (first_key, first_object) = &first.objects[0];
    let (second_key, second_object) = &second.objects[0];
    assert_eq!(second_key, first_key);
    assert_eq!(second_object.size, first_object.size);
    assert_eq!(second_object.tenant_id, first_object.tenant_id);
    assert_eq!(second_object.user_key, first_object.user_key);
    assert_eq!(second_object.group_id, first_object.group_id);
    assert_eq!(second_object.replicas.len(), first_object.replicas.len());
    for (second_replica, first_replica) in second_object.replicas.iter().zip(&first_object.replicas)
    {
        assert_eq!(second_replica.segment_id, first_replica.segment_id);
        assert_eq!(second_replica.segment_name, first_replica.segment_name);
        assert_eq!(second_replica.offset, first_replica.offset);
        assert_eq!(second_replica.size, first_replica.size);
        assert_eq!(second_replica.status, first_replica.status);
        assert_eq!(second_replica.replica_type, first_replica.replica_type);
    }

    let first_segment = &first.segments[0];
    let second_segment = &second.segments[0];
    assert_eq!(second_segment.segment.id, first_segment.segment.id);
    assert_eq!(second_segment.segment.name, first_segment.segment.name);
    assert_eq!(second_segment.segment.size, first_segment.segment.size);
    assert_eq!(second_segment.used, first_segment.used);
    assert_eq!(second_segment.client_id, first_segment.client_id);
    assert_eq!(second_segment.status, first_segment.status);
}

#[tokio::test(flavor = "multi_thread")]
async fn cpp_parity_snapshot_ssd_restore_preserves_cache_total_metrics() {
    let base_memory_total = metrics::MEM_CACHE_TOTAL.get();
    let base_disk_total = metrics::FILE_CACHE_TOTAL.get();
    let _reset = ResetCacheTotals {
        memory: base_memory_total,
        disk: base_disk_total,
    };

    let root = tempfile::tempdir().unwrap();
    let snapshot_root = root.path().join("snapshot");
    let global_disk_root = root.path().join("global-disk");
    let config = runtime_config(&global_disk_root);
    let source = snapshot_service(&snapshot_root, config.clone());
    let client_id = Uuid::new_v4();
    mount_segment(&source, client_id).await;

    let started = MasterService::put_start(
        &source,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: KEY.into(),
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
    assert_eq!(started.replicas.len(), 2);

    for replica_type in [
        proto::replica_descriptor::ReplicaType::Memory,
        proto::replica_descriptor::ReplicaType::Disk,
    ] {
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: KEY.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_memory_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_disk_total + 1);

    assert_eq!(replica_list(&source).await.len(), 2);
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_memory_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_disk_total + 1);
    let first = source.capture_loaded_snapshot("first-native-save");
    save_snapshot_and_wait(&source).await;

    metrics::MEM_CACHE_TOTAL.set(base_memory_total);
    metrics::FILE_CACHE_TOTAL.set(base_disk_total);
    let restored = snapshot_service(&snapshot_root, config.clone());
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_memory_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_disk_total + 1);
    let second = restored.capture_loaded_snapshot("second-native-save");
    assert_durable_inventory_matches(&first, &second);

    mount_segment(&restored, client_id).await;
    let replicas = replica_list(&restored).await;
    assert_eq!(replicas.len(), 2);
    assert!(replicas.iter().all(|replica| {
        replica.status == proto::replica_descriptor::ReplicaStatus::Complete as i32
    }));
    assert!(replicas.iter().any(|replica| {
        replica.replica_type == proto::replica_descriptor::ReplicaType::Memory as i32
    }));
    assert!(replicas.iter().any(|replica| {
        replica.replica_type == proto::replica_descriptor::ReplicaType::Disk as i32
    }));

    save_snapshot_and_wait(&restored).await;
    metrics::MEM_CACHE_TOTAL.set(base_memory_total);
    metrics::FILE_CACHE_TOTAL.set(base_disk_total);
    let reloaded = snapshot_service(&snapshot_root, config);
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_memory_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_disk_total + 1);
    let third = reloaded.capture_loaded_snapshot("after-second-native-save");
    assert_durable_inventory_matches(&second, &third);

    mount_segment(&reloaded, client_id).await;
    assert_eq!(replica_list(&reloaded).await.len(), 2);
}
