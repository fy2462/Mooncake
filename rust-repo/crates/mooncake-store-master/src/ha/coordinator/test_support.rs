use super::HaError;
use super::coordinator_k8s::{k8s_backoff_delay, k8s_lease_expired, parse_k8s_lease_connstring};
use k8s_openapi::api::coordination::v1::LeaseSpec;
use std::time::Duration;

pub fn parse_k8s_lease_connstring_for_test(connstring: &str) -> Result<(String, String), HaError> {
    parse_k8s_lease_connstring(connstring)
}

pub fn k8s_lease_expired_for_test(spec: &LeaseSpec, now: chrono::DateTime<chrono::Utc>) -> bool {
    k8s_lease_expired(spec, now)
}

pub fn k8s_backoff_delay_for_test(attempt: usize) -> Duration {
    k8s_backoff_delay(attempt)
}
