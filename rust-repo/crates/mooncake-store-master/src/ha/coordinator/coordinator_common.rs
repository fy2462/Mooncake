use crate::ha::types::{HaError, LeadershipSession};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(super) fn resolve_cluster_namespace(cluster_namespace: &str) -> String {
    if cluster_namespace.trim().is_empty() {
        std::env::var("MC_STORE_CLUSTER_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "mooncake".to_string())
    } else {
        cluster_namespace.to_string()
    }
}

pub(super) fn validate_session(session: &LeadershipSession) -> Result<(), HaError> {
    if session.owner_token.trim().is_empty() || session.lease_ttl == Duration::ZERO {
        return Err(HaError::InvalidParams(
            "leadership session owner token and lease ttl must be set".into(),
        ));
    }
    Ok(())
}

pub(super) fn clear_active_owner_token_if_matches(
    active_owner_token: &Arc<Mutex<Option<String>>>,
    owner_token: &str,
) {
    let mut active = active_owner_token
        .lock()
        .expect("active owner token mutex poisoned");
    if active.as_deref() == Some(owner_token) {
        *active = None;
    }
}
