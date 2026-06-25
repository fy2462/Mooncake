use futures_util::{pin_mut, StreamExt};
use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use k8s_openapi::api::coordination::v1::Lease;
use kube::api::{Api, DeleteParams, PostParams, WatchEvent, WatchParams};
use kube::{Client, ResourceExt};
use mooncake_store_master::ha::LeaderCoordinator;
use std::sync::Once;
use std::time::Duration;
use uuid::Uuid;

static RUSTLS_PROVIDER: Once = Once::new();

fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

fn live_k8s_enabled() -> bool {
    matches!(
        std::env::var("MOONCAKE_K8S_E2E").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn live_namespace() -> String {
    std::env::var("MOONCAKE_K8S_E2E_NAMESPACE").unwrap_or_else(|_| "default".to_string())
}

fn live_lease_name() -> String {
    std::env::var("MOONCAKE_K8S_E2E_LEASE")
        .unwrap_or_else(|_| format!("mooncake-rust-k8s-ha-e2e-{}", Uuid::new_v4().simple()))
}

async fn cleanup_lease(api: &Api<Lease>, lease_name: &str) {
    match api.delete(lease_name, &DeleteParams::default()).await {
        Ok(_) => {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(kube::Error::Api(err)) if err.code == 404 => {}
        Err(err) => panic!("failed to delete test lease {lease_name}: {err}"),
    }
}

async fn assert_lease_rbac(client: Client, namespace: &str) {
    let reviews: Api<SelfSubjectAccessReview> = Api::all(client);
    for verb in ["get", "create", "update", "delete", "watch"] {
        let review = SelfSubjectAccessReview {
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    group: Some("coordination.k8s.io".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("leases".to_string()),
                    namespace: Some(namespace.to_string()),
                    verb: Some(verb.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let review = reviews
            .create(&PostParams::default(), &review)
            .await
            .unwrap_or_else(|err| panic!("SelfSubjectAccessReview for verb={verb} failed: {err}"));
        let status = review.status.unwrap_or_else(|| {
            panic!("SelfSubjectAccessReview for verb={verb} returned no status")
        });
        assert!(
            status.allowed,
            "K8s RBAC denies coordination.k8s.io/v1 leases verb={verb} in namespace={namespace}: reason={:?} evaluation_error={:?}",
            status.reason,
            status.evaluation_error
        );
    }
}

async fn wait_for_holder(
    api: &Api<Lease>,
    lease_name: &str,
    resource_version: String,
    expected_holder: &str,
) {
    let params = WatchParams::default()
        .fields(&format!("metadata.name={lease_name}"))
        .timeout(10);
    let stream = api
        .watch(&params, &resource_version)
        .await
        .expect("lease watch should open");
    pin_mut!(stream);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for lease holder {expected_holder}"
        );
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(WatchEvent::Added(lease)))) | Ok(Some(Ok(WatchEvent::Modified(lease)))) => {
                if lease
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.holder_identity.as_deref())
                    == Some(expected_holder)
                {
                    return;
                }
            }
            Ok(Some(Ok(WatchEvent::Deleted(_)))) => {}
            Ok(Some(Ok(WatchEvent::Bookmark(_)))) => {}
            Ok(Some(Ok(WatchEvent::Error(err)))) => panic!("lease watch error event: {err:?}"),
            Ok(Some(Err(err))) => panic!("lease watch stream error: {err}"),
            Ok(None) => panic!("lease watch stream ended before observing holder"),
            Err(_) => panic!("timed out waiting for lease watch event"),
        }
    }
}

#[tokio::test]
async fn test_k8s_ha_live_rbac_watch_backoff_e2e() {
    if !live_k8s_enabled() {
        eprintln!("skipping live K8s HA e2e; set MOONCAKE_K8S_E2E=1 to enable");
        return;
    }
    install_rustls_provider();

    let namespace = live_namespace();
    let lease_name = live_lease_name();
    let connstring = format!("{namespace}/{lease_name}");
    let client = Client::try_default()
        .await
        .expect("MOONCAKE_K8S_E2E=1 requires a usable kubeconfig or in-cluster config");
    assert_lease_rbac(client.clone(), &namespace).await;

    let api: Api<Lease> = Api::namespaced(client, &namespace);
    cleanup_lease(&api, &lease_name).await;

    let coordinator_a = LeaderCoordinator::new_k8s(&connstring, None).unwrap();
    let coordinator_b = LeaderCoordinator::new_k8s(&connstring, None).unwrap();
    let coordinator_watch = LeaderCoordinator::new_k8s(&connstring, None).unwrap();

    let acquired_a = coordinator_a
        .try_acquire_leadership("leader-a", 2)
        .await
        .unwrap();
    assert!(acquired_a.acquired);
    let session_a = acquired_a.session.clone().unwrap();
    let lease = api.get(&lease_name).await.unwrap();
    let resource_version = lease.resource_version().unwrap_or_default();
    assert_eq!(
        lease
            .spec
            .as_ref()
            .and_then(|spec| spec.holder_identity.as_deref()),
        Some("leader-a")
    );

    let contended = coordinator_b
        .try_acquire_leadership("leader-b", 2)
        .await
        .unwrap();
    assert!(!contended.acquired);
    assert_eq!(
        contended
            .view
            .as_ref()
            .map(|view| view.leader_address.as_str()),
        Some("leader-a")
    );

    coordinator_a
        .try_renew_leadership(&session_a)
        .await
        .unwrap();
    let keepalive = coordinator_a
        .start_leadership_keepalive(&session_a)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        coordinator_a
            .read_current_view()
            .await
            .unwrap()
            .map(|view| view.leader_address),
        Some("leader-a".to_string())
    );
    drop(keepalive);

    let known_version = session_a.view.view_version;
    let wait_for_view_change = tokio::spawn(async move {
        coordinator_watch
            .wait_for_view_change(known_version, Duration::from_secs(10))
            .await
    });
    coordinator_a.release_leadership(&session_a).await.unwrap();
    let mut delay = Duration::from_millis(100);
    let session_b = loop {
        let attempt = coordinator_b
            .try_acquire_leadership("leader-b", 2)
            .await
            .unwrap();
        if attempt.acquired {
            break attempt.session.unwrap();
        }
        tokio::time::sleep(delay).await;
        delay = std::cmp::min(delay * 2, Duration::from_secs(1));
    };

    let changed_view = wait_for_view_change
        .await
        .expect("K8s wait_for_view_change task should join")
        .expect("K8s wait_for_view_change should not fail")
        .expect("K8s wait_for_view_change should observe leader-b");
    assert_eq!(changed_view.leader_address, "leader-b");
    wait_for_holder(&api, &lease_name, resource_version, "leader-b").await;
    assert_eq!(
        coordinator_b
            .read_current_view()
            .await
            .unwrap()
            .map(|view| view.leader_address),
        Some("leader-b".to_string())
    );

    coordinator_b.release_leadership(&session_b).await.unwrap();
    cleanup_lease(&api, &lease_name).await;
}
