use k8s_openapi::api::coordination::v1::LeaseSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use mooncake_store_master::ha::HaError;
use mooncake_store_master::ha::coordinator::test_support::{
    k8s_backoff_delay_for_test, k8s_lease_expired_for_test, parse_k8s_lease_connstring_for_test,
};
use std::time::Duration;

#[test]
fn test_parse_k8s_lease_connstring_accepts_name_only() {
    let (namespace, lease_name) = parse_k8s_lease_connstring_for_test("mooncake-master").unwrap();

    assert_eq!(namespace, "default");
    assert_eq!(lease_name, "mooncake-master");
}

#[test]
fn test_parse_k8s_lease_connstring_accepts_namespace_and_name() {
    let (namespace, lease_name) = parse_k8s_lease_connstring_for_test("ns-a/lease-a").unwrap();

    assert_eq!(namespace, "ns-a");
    assert_eq!(lease_name, "lease-a");
}

#[test]
fn test_parse_k8s_lease_connstring_rejects_invalid_values() {
    for value in ["", "ns-a/", "/lease-a", "too/many/parts"] {
        assert!(matches!(
            parse_k8s_lease_connstring_for_test(value),
            Err(HaError::InvalidParams(_))
        ));
    }
}

#[test]
fn test_k8s_lease_expiration_uses_renew_time_and_ttl() {
    let now = chrono::Utc::now();
    let fresh = LeaseSpec {
        holder_identity: Some("leader-a".to_string()),
        lease_duration_seconds: Some(5),
        acquire_time: Some(MicroTime(now - chrono::Duration::seconds(30))),
        renew_time: Some(MicroTime(now - chrono::Duration::seconds(4))),
        lease_transitions: Some(3),
    };
    assert!(!k8s_lease_expired_for_test(&fresh, now));

    let expired = LeaseSpec {
        renew_time: Some(MicroTime(now - chrono::Duration::seconds(5))),
        ..fresh
    };
    assert!(k8s_lease_expired_for_test(&expired, now));
}

#[test]
fn test_k8s_backoff_is_bounded_exponential() {
    assert_eq!(k8s_backoff_delay_for_test(0), Duration::from_millis(50));
    assert_eq!(k8s_backoff_delay_for_test(1), Duration::from_millis(100));
    assert_eq!(k8s_backoff_delay_for_test(4), Duration::from_millis(500));
    assert_eq!(k8s_backoff_delay_for_test(20), Duration::from_millis(500));
}
