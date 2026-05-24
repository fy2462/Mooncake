use mooncake_store_master::ha::{LeaderCoordinator, LeaderRole};

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

#[tokio::test]
async fn test_manual_coordinator_waits_for_promotion() {
    let (coordinator, tx) = LeaderCoordinator::new_manual(LeaderRole::Standby);
    assert_eq!(coordinator.wait_for_role().await.unwrap(), LeaderRole::Standby);

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
