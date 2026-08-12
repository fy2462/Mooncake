use mooncake_store_master::snapshot_scheduler::wait_for_interval_or_shutdown;
use std::time::Duration;

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
