use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::ha::{
    CatalogBackedSnapshotProvider, EmbeddedSnapshotCatalogStore, LocalFileSnapshotObjectStore,
};
use mooncake_store_master::snapshot_scheduler::{
    run_periodic_snapshots, wait_for_interval_or_shutdown,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn cpp_parity_snapshot_scheduler_shutdown_interrupts_interval_wait() {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut waiter =
        tokio::spawn(async move { wait_for_interval_or_shutdown(30, &mut shutdown_rx).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err(),
        "scheduler must be pending in its 30-second interval wait"
    );

    shutdown_tx.send(true).unwrap();
    let start = tokio::time::Instant::now();
    let should_snapshot = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("snapshot scheduler must not wait for the 30-second interval")
        .unwrap();

    assert!(!should_snapshot);
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn false_watch_update_does_not_trigger_snapshot() {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut waiter =
        tokio::spawn(async move { wait_for_interval_or_shutdown(30, &mut shutdown_rx).await });

    shutdown_tx.send(false).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err(),
        "a false watch update must keep the scheduler waiting"
    );
    shutdown_tx.send(true).unwrap();
    assert!(!waiter.await.unwrap());
}

#[tokio::test]
async fn cpp_parity_snapshot_scheduler_automatically_generates_latest_marker() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let publisher = CatalogBackedSnapshotProvider::new("", Box::new(catalog), object_store);
    let service = Arc::new(MasterServiceImpl::new(None, None));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let scheduler = tokio::spawn(run_periodic_snapshots(
        service,
        1,
        shutdown_rx,
        Some(publisher),
        0,
        3,
        false,
    ));
    let latest = root.path().join("mooncake_master_snapshot/latest.txt");

    let marker_result = tokio::time::timeout(Duration::from_secs(5), async {
        while !latest.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), scheduler)
        .await
        .expect("snapshot scheduler must stop promptly")
        .unwrap();

    marker_result.expect("periodic snapshot must publish latest.txt within five seconds");
    assert!(latest.is_file());
    assert!(!std::fs::read_to_string(latest).unwrap().trim().is_empty());
}
