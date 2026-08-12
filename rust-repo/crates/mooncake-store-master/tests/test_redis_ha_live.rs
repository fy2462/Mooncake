use mooncake_store_master::ha::{
    HaError, LeaderCoordinator, LocalFileSnapshotObjectStore, RedisSnapshotCatalogStore,
    SnapshotCatalogStore, SnapshotDescriptor, SnapshotObjectStore,
};
use std::sync::Arc;
use uuid::Uuid;

fn live_redis_enabled() -> bool {
    matches!(
        std::env::var("MOONCAKE_REDIS_E2E").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn live_redis_url() -> String {
    std::env::var("MOONCAKE_REDIS_E2E_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn test_namespace(suffix: &str) -> String {
    format!("ha-redis-test-{suffix}-{}", Uuid::new_v4().simple())
}

struct RedisSnapshotCatalogGuard {
    url: String,
    namespace: String,
}

impl Drop for RedisSnapshotCatalogGuard {
    fn drop(&mut self) {
        let Ok(client) = redis::Client::open(self.url.as_str()) else {
            return;
        };
        let Ok(mut connection) = client.get_connection() else {
            return;
        };
        let latest = format!("mooncake-store/{{{}}}/snapshot/latest", self.namespace);
        let index = format!("mooncake-store/{{{}}}/snapshot/index", self.namespace);
        let _ = redis::cmd("DEL")
            .arg(latest)
            .arg(index)
            .query::<()>(&mut connection);
    }
}

fn redis_snapshot_catalog(
    suffix: &str,
    root: &tempfile::TempDir,
) -> (
    RedisSnapshotCatalogGuard,
    Arc<LocalFileSnapshotObjectStore>,
    RedisSnapshotCatalogStore,
) {
    let url = live_redis_url();
    let namespace = test_namespace(suffix);
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = RedisSnapshotCatalogStore::new(&url, &namespace, object_store.clone()).unwrap();
    (
        RedisSnapshotCatalogGuard { url, namespace },
        object_store,
        catalog,
    )
}

#[test]
fn cpp_parity_redis_snapshot_catalog_get_latest_returns_empty_when_catalog_missing() {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let (_guard, _object_store, catalog) = redis_snapshot_catalog("snapshot-empty", &root);

    assert_eq!(catalog.get_latest().unwrap(), None);
}

#[test]
fn cpp_parity_redis_snapshot_catalog_publish_list_and_get_latest_round_trip() {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let (_guard, _object_store, catalog) = redis_snapshot_catalog("snapshot-round-trip", &root);
    let make_descriptor = |snapshot_id: &str| {
        let mut descriptor =
            SnapshotDescriptor::new_with_snapshot_root(catalog.get_snapshot_root(), snapshot_id);
        descriptor.last_included_seq = 42;
        descriptor.producer_view_version = 7;
        descriptor.created_at_ms = 1_700_000_000_000;
        descriptor
    };
    let first = make_descriptor("20240301_120000_001");
    let second = make_descriptor("20240302_120000_001");

    catalog.publish(&first).unwrap();
    catalog.publish(&second).unwrap();

    assert_eq!(catalog.get_latest().unwrap(), Some(second.clone()));
    assert_eq!(catalog.list(0).unwrap(), vec![second, first]);
}

#[test]
fn cpp_parity_redis_snapshot_catalog_missing_latest_descriptor_is_an_error() {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let (_guard, object_store, catalog) = redis_snapshot_catalog("snapshot-missing-latest", &root);
    let descriptor = SnapshotDescriptor::new_with_snapshot_root(
        catalog.get_snapshot_root(),
        "20240302_120000_001",
    );
    catalog.publish(&descriptor).unwrap();
    object_store
        .delete_objects_with_prefix(&format!("{}descriptor.txt", descriptor.object_prefix))
        .unwrap();

    assert!(matches!(catalog.get_latest(), Err(HaError::Snapshot(_))));
}

#[test]
fn cpp_parity_redis_snapshot_catalog_list_skips_missing_descriptor() {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let (_guard, object_store, catalog) = redis_snapshot_catalog("snapshot-missing-list", &root);
    let descriptor = SnapshotDescriptor::new_with_snapshot_root(
        catalog.get_snapshot_root(),
        "20240302_120000_001",
    );
    catalog.publish(&descriptor).unwrap();
    object_store
        .delete_objects_with_prefix(&format!("{}descriptor.txt", descriptor.object_prefix))
        .unwrap();

    assert!(catalog.list(0).unwrap().is_empty());
}

#[test]
fn cpp_parity_redis_snapshot_catalog_list_keeps_healthy_older_descriptor() {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let (_guard, object_store, catalog) = redis_snapshot_catalog("snapshot-healthy-list", &root);
    let mut older = SnapshotDescriptor::new_with_snapshot_root(
        catalog.get_snapshot_root(),
        "20240301_120000_001",
    );
    older.created_at_ms = 1_700_000_000_000;
    let mut newer = SnapshotDescriptor::new_with_snapshot_root(
        catalog.get_snapshot_root(),
        "20240302_120000_001",
    );
    newer.created_at_ms = 1_700_000_000_001;
    catalog.publish(&older).unwrap();
    catalog.publish(&newer).unwrap();
    object_store
        .delete_objects_with_prefix(&format!("{}descriptor.txt", newer.object_prefix))
        .unwrap();

    assert_eq!(catalog.list(0).unwrap(), vec![older]);
}

#[tokio::test]
async fn cpp_parity_high_availability_redis_test_highavailabilitytest_redisbasicmasterviewoperations()
 {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let url = live_redis_url();
    let namespace = test_namespace("basic");
    let coordinator = LeaderCoordinator::new_redis(&url, &namespace)
        .await
        .expect("create primary Redis coordinator");
    let contender = LeaderCoordinator::new_redis(&url, &namespace)
        .await
        .expect("create contending Redis coordinator");

    assert!(coordinator.read_current_view().await.unwrap().is_none());

    let acquired = coordinator
        .try_acquire_leadership("127.0.0.1:8899", 30)
        .await
        .unwrap();
    assert!(acquired.acquired);
    let session = acquired.session.expect("acquired Redis session");

    let current = coordinator
        .read_current_view()
        .await
        .unwrap()
        .expect("current Redis view");
    assert_eq!(current.leader_address, "127.0.0.1:8899");
    assert_eq!(current.view_version, session.view.view_version);

    coordinator.try_renew_leadership(&session).await.unwrap();

    let contended = contender
        .try_acquire_leadership("127.0.0.1:9900", 30)
        .await
        .unwrap();
    assert!(!contended.acquired);
    let observed = contended.view.expect("contended Redis view");
    assert_eq!(observed.leader_address, "127.0.0.1:8899");
    assert_eq!(observed.view_version, session.view.view_version);

    coordinator.release_leadership(&session).await.unwrap();
    assert!(contender.read_current_view().await.unwrap().is_none());
}

#[tokio::test]
async fn cpp_parity_high_availability_redis_test_highavailabilitytest_rediscanrestartrenewafterexplicitrelease()
 {
    if !live_redis_enabled() {
        eprintln!("skipping live Redis HA e2e; set MOONCAKE_REDIS_E2E=1 to enable");
        return;
    }

    let url = live_redis_url();
    let namespace = test_namespace("restart-renew");
    let coordinator = LeaderCoordinator::new_redis(&url, &namespace)
        .await
        .expect("create Redis coordinator");

    let first = coordinator
        .try_acquire_leadership("127.0.0.1:9933", 30)
        .await
        .unwrap();
    assert!(first.acquired);
    let first_session = first.session.expect("first Redis session");
    coordinator
        .try_renew_leadership(&first_session)
        .await
        .unwrap();
    coordinator
        .release_leadership(&first_session)
        .await
        .unwrap();
    assert!(coordinator.read_current_view().await.unwrap().is_none());

    let second = coordinator
        .try_acquire_leadership("127.0.0.1:9944", 30)
        .await
        .unwrap();
    assert!(second.acquired);
    let second_session = second.session.expect("second Redis session");
    assert_eq!(second_session.view.leader_address, "127.0.0.1:9944");
    assert!(second_session.view.view_version > first_session.view.view_version);
    assert_ne!(second_session.owner_token, first_session.owner_token);
    coordinator
        .try_renew_leadership(&second_session)
        .await
        .unwrap();
    coordinator
        .release_leadership(&second_session)
        .await
        .unwrap();
    assert!(coordinator.read_current_view().await.unwrap().is_none());
}
