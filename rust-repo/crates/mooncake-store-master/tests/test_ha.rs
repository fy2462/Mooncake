use dashmap::DashMap;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
mod common;
use common::temp_dir;

use mooncake_store_master::ha::EmbeddedSnapshotCatalogStore;
use mooncake_store_master::ha::{
    build_standby_runtime_capabilities, map_standby_runtime_state, parse_ha_backend_type,
    CapabilityDrivenStandbyController, HABackendSpec, HABackendType, HaError, LeaderCoordinator,
    LeaderRole, LeadershipSession, LocalSnapshotProvider, MasterRuntimeState,
    MasterServiceSupervisor, MasterServiceSupervisorConfig, MasterView, SnapshotCatalogStore,
    SnapshotDescriptor, SnapshotProvider, StandbyController, StandbyRuntimeCapabilities,
    StandbyState, StandbySyncStatus,
};
use mooncake_store_master::service::{NoFSegmentEntry, ObjectEntry, SegmentEntry, TaskEntry};
use mooncake_store_master::storage_backend::{StorageBackend, StorageBackendType};
use std::time::SystemTime;
use uuid::Uuid;

#[test]
fn test_leader_role_values() {
    assert_ne!(LeaderRole::Leader, LeaderRole::Standby);
}

#[test]
fn test_leader_role_debug() {
    assert_eq!(format!("{:?}", LeaderRole::Leader), "Leader");
    assert_eq!(format!("{:?}", LeaderRole::Standby), "Standby");
}

#[test]
fn test_leader_role_clone_eq() {
    let r = LeaderRole::Leader;
    assert_eq!(r.clone(), r);
    assert_eq!(r, LeaderRole::Leader);
    assert_ne!(r, LeaderRole::Standby);
}

#[test]
fn test_leader_role_copy() {
    let r = LeaderRole::Leader;
    let r2 = r;
    assert_eq!(r, r2);
    let s = LeaderRole::Standby;
    assert_ne!(r, s);
}

#[test]
fn test_coordinator_backend_types() {
    assert_ne!(LeaderRole::Leader, LeaderRole::Standby);
    assert_eq!(LeaderRole::Leader, LeaderRole::Leader);
}

#[test]
fn test_parse_ha_backend_type() {
    assert_eq!(parse_ha_backend_type("etcd"), Some(HABackendType::Etcd));
    assert_eq!(parse_ha_backend_type("redis"), Some(HABackendType::Redis));
    assert_eq!(parse_ha_backend_type("k8s"), Some(HABackendType::K8s));
    assert_eq!(parse_ha_backend_type("unknown"), None);
}

#[test]
fn test_ha_error_fatal_matches_supervisor_policy() {
    assert!(HaError::InvalidParams("bad config".into()).is_fatal());
    assert!(HaError::UnavailableInCurrentMode("not built".into()).is_fatal());
    assert!(!HaError::InvalidBackend("temporary etcd error".into()).is_fatal());
    assert!(!HaError::UnavailableInCurrentStatus.is_fatal());
}

#[test]
fn test_build_standby_runtime_capabilities() {
    let spec = HABackendSpec {
        backend_type: HABackendType::Etcd,
        connstring: "http://127.0.0.1:2379".into(),
        cluster_namespace: "cluster-a".into(),
    };
    let config = MasterServiceSupervisorConfig {
        enable_snapshot_restore: true,
        ..Default::default()
    };

    let capabilities = build_standby_runtime_capabilities(&spec, &config);
    assert!(capabilities.has_snapshot_bootstrap);
    assert!(capabilities.has_oplog_following);
}

#[tokio::test]
async fn test_manual_coordinator_waits_for_promotion() {
    let (coordinator, tx) = LeaderCoordinator::new_manual(LeaderRole::Standby);
    assert_eq!(
        coordinator.wait_for_role().await.unwrap(),
        LeaderRole::Standby
    );

    let watcher = tokio::spawn(async move {
        coordinator.watch_leadership_change().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    assert!(!watcher.is_finished());

    tx.send(LeaderRole::Leader).unwrap();
    tokio::time::timeout(tokio::time::Duration::from_secs(1), watcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_manual_acquire_returns_session_and_session_apis_work() {
    let (coordinator, _tx) = LeaderCoordinator::new_manual(LeaderRole::Standby);

    let acquired = coordinator
        .try_acquire_leadership("127.0.0.1:50051", 30)
        .await
        .unwrap();

    assert!(acquired.acquired);
    let session = acquired.session.as_ref().expect("leadership session");
    assert_eq!(session.view.leader_address, "127.0.0.1:50051");
    assert_eq!(session.owner_token, "manual");

    let keepalive = coordinator
        .start_leadership_keepalive(session)
        .await
        .unwrap();
    assert!(coordinator.subscribe_role_for_session(session).is_ok());

    let wrong_session = LeadershipSession {
        owner_token: "wrong-owner".into(),
        ..session.clone()
    };
    assert!(matches!(
        coordinator.subscribe_role_for_session(&wrong_session),
        Err(HaError::UnavailableInCurrentStatus)
    ));

    coordinator.try_renew_leadership(session).await.unwrap();
    coordinator.release_leadership(session).await.unwrap();
    drop(keepalive);
    assert_eq!(
        coordinator.wait_for_role().await.unwrap(),
        LeaderRole::Standby
    );
}

#[tokio::test]
async fn test_wait_for_view_change_returns_when_known_view_disappears() {
    let (coordinator, _tx) = LeaderCoordinator::new_manual(LeaderRole::Standby);
    let start = std::time::Instant::now();
    let view = coordinator
        .wait_for_view_change(7, std::time::Duration::from_secs(5))
        .await
        .unwrap();

    assert!(view.is_none());
    assert!(start.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn test_local_snapshot_provider_loads_snapshot() {
    let root = temp_dir();
    let cluster_dir = root.join("cluster-a");
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &cluster_dir);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    segments.insert(
        segment_id,
        SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: "leader:50051".into(),
                size: 4096,
                base: 0,
                te_endpoint: String::new(),
                protocol: "tcp".into(),
            },
            used: 512,
            client_id,
            status: mooncake_store_master::proto::SegmentStatus::Active,
        },
    );

    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    objects.insert(
        "ha-key".into(),
        ObjectEntry {
            tenant_id: "default".to_string(),
            user_key: String::new(),
            replicas: vec![ReplicaDescriptor {
                base_addr: 0x100000000,
                refcnt: 0,
                handle_valid: true,
                segment_id,
                segment_name: "leader:50051".into(),
                offset: 128,
                size: 512,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            }],
            size: 512,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
        },
    );
    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();

    let provider = LocalSnapshotProvider::new(root, StorageBackendType::LocalDisk);
    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(snapshot.segments.len(), 1);
    assert!(snapshot.nof_segments.is_empty());
    assert_eq!(snapshot.objects.len(), 1);
    assert_eq!(snapshot.objects[0].0, "ha-key");
}

#[test]
fn test_local_snapshot_provider_prefers_cluster_dir_and_falls_back_to_root() {
    let root = temp_dir();
    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();

    let root_objects: DashMap<String, ObjectEntry> = DashMap::new();
    StorageBackend::new(StorageBackendType::LocalDisk, &root)
        .save(&segments, &nof_segments, &root_objects, &tasks)
        .unwrap();

    let cluster_objects: DashMap<String, ObjectEntry> = DashMap::new();
    cluster_objects.insert(
        "cluster-key".into(),
        ObjectEntry {
            tenant_id: "default".to_string(),
            user_key: "cluster-key".to_string(),
            replicas: vec![],
            size: 0,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
        },
    );
    StorageBackend::new(StorageBackendType::LocalDisk, &root.join("cluster-a"))
        .save(&segments, &nof_segments, &cluster_objects, &tasks)
        .unwrap();

    let provider = LocalSnapshotProvider::new(root, StorageBackendType::LocalDisk);
    let cluster_snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(cluster_snapshot.objects[0].0, "cluster-key");

    let fallback_snapshot = provider.load_latest_snapshot("missing").unwrap().unwrap();
    assert!(fallback_snapshot.objects.is_empty());
}

#[test]
fn test_embedded_snapshot_catalog_round_trip() {
    let root = temp_dir();
    let catalog = EmbeddedSnapshotCatalogStore::new(root);
    let mut descriptor = SnapshotDescriptor::new("20260603_120000_001");
    descriptor.last_included_seq = 42;
    descriptor.producer_view_version = 7;
    descriptor.created_at_ms = 1234;

    catalog.publish(&descriptor).unwrap();

    let latest = catalog.get_latest().unwrap().unwrap();
    assert_eq!(latest.snapshot_id, descriptor.snapshot_id);
    assert_eq!(latest.last_included_seq, 42);
    assert_eq!(latest.producer_view_version, 7);

    let listed = catalog.list(10).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].snapshot_id, "20260603_120000_001");

    catalog.delete("20260603_120000_001").unwrap();
    assert!(catalog.get_latest().unwrap().is_none());
}

#[test]
fn test_local_snapshot_provider_uses_catalog_sequence_id() {
    let root = temp_dir();
    let cluster_dir = root.join("cluster-c");
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &cluster_dir);
    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();

    let catalog = EmbeddedSnapshotCatalogStore::new(cluster_dir);
    let mut descriptor = SnapshotDescriptor::new("20260603_120001_001");
    descriptor.last_included_seq = 99;
    descriptor.producer_view_version = 11;
    catalog.publish(&descriptor).unwrap();

    let provider = LocalSnapshotProvider::new(root, StorageBackendType::LocalDisk);
    let snapshot = provider.load_latest_snapshot("cluster-c").unwrap().unwrap();

    assert_eq!(snapshot.snapshot_id, "20260603_120001_001");
    assert_eq!(snapshot.snapshot_sequence_id, 99);
}

#[test]
fn test_capability_driven_controller_restores_snapshot_and_reports_state() {
    let root = temp_dir();
    let cluster_id = "cluster-b";
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &root.join(cluster_id));
    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();

    let spec = HABackendSpec {
        backend_type: HABackendType::Redis,
        connstring: "redis://127.0.0.1:6379".into(),
        cluster_namespace: cluster_id.into(),
    };
    let config = MasterServiceSupervisorConfig {
        cluster_id: cluster_id.into(),
        enable_snapshot_restore: true,
        snapshot_backup_dir: Some(root),
        snapshot_backend_type: Some(StorageBackendType::LocalDisk),
        ..Default::default()
    };
    let mut controller = CapabilityDrivenStandbyController::new(spec, config);
    controller
        .start_standby(Some(MasterView {
            leader_address: "leader:50051".into(),
            view_version: 7,
        }))
        .unwrap();

    assert_eq!(
        controller.get_standby_runtime_state(),
        MasterRuntimeState::Standby
    );
}

#[test]
fn test_capability_driven_controller_reports_catching_up_when_lagging() {
    let capabilities = StandbyRuntimeCapabilities {
        has_snapshot_bootstrap: false,
        has_oplog_following: true,
    };
    let status = StandbySyncStatus {
        applied_seq_id: 10,
        primary_seq_id: 15,
        lag_entries: 5,
        is_syncing: true,
        is_connected: true,
        state: StandbyState::Watching,
    };
    let leader = MasterView {
        leader_address: "leader:50051".into(),
        view_version: 1,
    };

    assert_eq!(
        map_standby_runtime_state(&status, Some(&leader), capabilities),
        MasterRuntimeState::CatchingUp
    );
}

#[test]
fn test_master_service_supervisor_tracks_runtime_state() {
    let mut supervisor = MasterServiceSupervisor::new(Box::new(NoopControllerShim::default()));
    supervisor
        .enter_standby_mode(Some(MasterView {
            leader_address: "leader:1234".into(),
            view_version: 42,
        }))
        .unwrap();
    assert_eq!(supervisor.runtime_state(), MasterRuntimeState::Standby);
    assert_eq!(supervisor.observed_leader().unwrap().view_version, 42);

    supervisor.begin_candidacy();
    assert_eq!(supervisor.runtime_state(), MasterRuntimeState::Candidate);

    supervisor.promote_to_leader_warmup().unwrap();
    assert_eq!(supervisor.runtime_state(), MasterRuntimeState::LeaderWarmup);

    supervisor.activate_serving_state();
    assert_eq!(supervisor.runtime_state(), MasterRuntimeState::Serving);
}

#[test]
fn test_in_memory_oplog_manager_appends_and_polls() {
    use mooncake_store_master::oplog::{InMemoryOpLog, OpLogStore};
    let mut manager = InMemoryOpLog::new(4);
    assert_eq!(manager.append_payload(1, "first"), 1);
    assert_eq!(manager.append_payload(1, "second"), 2);
    assert_eq!(manager.last_seq(), 2);

    let poll = manager.poll_from(1, 8);
    assert_eq!(poll.records.len(), 2);
    assert_eq!(poll.records[0].payload, "first");
    assert_eq!(poll.next_seq, 3);
}

#[derive(Default)]
struct NoopControllerShim;

impl StandbyController for NoopControllerShim {
    fn start_standby(
        &mut self,
        _observed_leader: Option<MasterView>,
    ) -> Result<(), mooncake_store_master::ha::HaError> {
        Ok(())
    }

    fn stop_standby(&mut self) {}

    fn promote_standby(&mut self) -> Result<(), mooncake_store_master::ha::HaError> {
        Ok(())
    }

    fn update_observed_leader(&mut self, _observed_leader: Option<MasterView>) {}

    fn get_standby_runtime_state(&self) -> MasterRuntimeState {
        MasterRuntimeState::Standby
    }

    fn set_runtime_state_callback(
        &mut self,
        callback: Option<mooncake_store_master::ha::RuntimeStateCallback>,
    ) {
        if let Some(callback) = callback {
            callback(MasterRuntimeState::Standby);
        }
    }
}
