use mooncake_store_master::ha::LeaderCoordinator;
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
