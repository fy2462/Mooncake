use super::coordinator_common::{clear_active_owner_token_if_matches, validate_session};
use super::*;
use crate::ha::types::K8sPodIdentity;
use futures_util::{StreamExt, pin_mut};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Patch, PatchParams, PostParams, WatchEvent, WatchParams};
use kube::{Api, Client, ResourceExt};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tracing::{error, info, warn};

const K8S_OPERATION_MAX_ATTEMPTS: usize = 5;
const K8S_OPERATION_INITIAL_BACKOFF_MS: u64 = 50;
const K8S_OPERATION_MAX_BACKOFF_MS: u64 = 500;
const K8S_LEADER_LABEL_KEY: &str = "mooncake.io/store-role";
const K8S_LEADER_LABEL_VALUE: &str = "leader";
const K8S_LABEL_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(super) struct K8sLeaderLabelReconciler {
    desired_tx: watch::Sender<bool>,
}

impl K8sLeaderLabelReconciler {
    pub(super) fn new(pod_identity: K8sPodIdentity) -> Self {
        let (desired_tx, desired_rx) = watch::channel(false);
        tokio::spawn(run_k8s_label_reconciler(pod_identity, desired_rx));
        Self { desired_tx }
    }

    pub(super) fn set_leader(&self, desired: bool) {
        let _ = self.desired_tx.send(desired);
    }
}

pub(super) fn set_k8s_leader_label(
    label_reconciler: &Option<K8sLeaderLabelReconciler>,
    desired: bool,
) {
    if let Some(label_reconciler) = label_reconciler {
        label_reconciler.set_leader(desired);
    }
}

pub(super) fn parse_k8s_lease_connstring(connstring: &str) -> Result<(String, String), HaError> {
    let trimmed = connstring.trim();
    if trimmed.is_empty() {
        return Err(HaError::InvalidParams(
            "k8s HA backend requires lease name or namespace/lease-name".into(),
        ));
    }

    let parts = trimmed.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [lease_name] if !lease_name.trim().is_empty() => {
            Ok(("default".to_string(), lease_name.trim().to_string()))
        }
        [namespace, lease_name]
            if !namespace.trim().is_empty() && !lease_name.trim().is_empty() =>
        {
            Ok((namespace.trim().to_string(), lease_name.trim().to_string()))
        }
        _ => Err(HaError::InvalidParams(
            "k8s HA backend connstring must be lease-name or namespace/lease-name".into(),
        )),
    }
}

pub(super) async fn k8s_lease_api(namespace: &str) -> Result<Api<Lease>, HaError> {
    let client = Client::try_default()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("k8s client config: {e}")))?;
    Ok(Api::namespaced(client, namespace))
}

pub(super) async fn k8s_pod_api(namespace: &str) -> Result<Api<Pod>, HaError> {
    let client = Client::try_default()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("k8s client config: {e}")))?;
    Ok(Api::namespaced(client, namespace))
}

async fn run_k8s_label_reconciler(
    pod_identity: K8sPodIdentity,
    mut desired_rx: watch::Receiver<bool>,
) {
    // Drive the pod label toward the latest desired state. Transient K8s
    // failures keep `applied` unset so the worker retries every second.
    let mut applied: Option<bool> = None;
    loop {
        let desired = *desired_rx.borrow();
        if applied != Some(desired) {
            match apply_k8s_leader_label(&pod_identity, desired).await {
                Ok(()) => {
                    applied = Some(desired);
                    continue;
                }
                Err(e) => {
                    applied = None;
                    warn!(
                        "Failed to {} K8s leader pod label: {}",
                        if desired { "set" } else { "clear" },
                        e
                    );
                }
            }
        }

        tokio::select! {
            changed = desired_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(K8S_LABEL_RECONCILE_INTERVAL), if applied.is_none() => {}
        }
    }
}

async fn apply_k8s_leader_label(
    pod_identity: &K8sPodIdentity,
    desired: bool,
) -> Result<(), HaError> {
    let value = if desired {
        json!(K8S_LEADER_LABEL_VALUE)
    } else {
        serde_json::Value::Null
    };
    patch_k8s_pod_label(
        &pod_identity.namespace,
        &pod_identity.pod_name,
        K8S_LEADER_LABEL_KEY,
        value,
    )
    .await
}

async fn patch_k8s_pod_label(
    namespace: &str,
    pod_name: &str,
    label_key: &str,
    value: serde_json::Value,
) -> Result<(), HaError> {
    let api = k8s_pod_api(namespace).await?;
    let patch = json!({
        "metadata": {
            "labels": {
                label_key: value,
            },
        },
    });
    api.patch(pod_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(|e| HaError::InvalidBackend(format!("k8s patch pod label: {e}")))?;
    Ok(())
}

pub(super) async fn read_k8s_view(
    namespace: &str,
    lease_name: &str,
) -> Result<Option<MasterView>, HaError> {
    let api = k8s_lease_api(namespace).await?;
    match api.get(lease_name).await {
        Ok(lease) => Ok(view_from_k8s_lease(&lease)),
        Err(kube::Error::Api(err)) if err.code == 404 => Ok(None),
        Err(e) => Err(HaError::InvalidBackend(format!("k8s get lease: {e}"))),
    }
}

pub(super) async fn acquire_k8s_lease(
    namespace: &str,
    lease_name: &str,
    leader_address: &str,
    lease_ttl_secs: i64,
) -> Result<AcquireLeadershipResult, HaError> {
    if lease_ttl_secs <= 0 {
        return Err(HaError::InvalidParams(
            "k8s lease ttl must be positive".into(),
        ));
    }

    let api = k8s_lease_api(namespace).await?;
    for attempt in 0..K8S_OPERATION_MAX_ATTEMPTS {
        let now = chrono::Utc::now();
        match api.get(lease_name).await {
            Ok(mut lease) => {
                let spec = lease.spec.clone().unwrap_or_default();
                let holder = spec.holder_identity.clone().unwrap_or_default();
                let current_view = view_from_k8s_lease(&lease);
                if !holder.is_empty() && holder != leader_address && !k8s_lease_expired(&spec, now)
                {
                    return Ok(AcquireLeadershipResult {
                        acquired: false,
                        view: current_view,
                        session: None,
                    });
                }

                let holder_changed = holder != leader_address;
                let transitions =
                    spec.lease_transitions.unwrap_or_default() + if holder_changed { 1 } else { 0 };
                lease.spec = Some(LeaseSpec {
                    holder_identity: Some(leader_address.to_string()),
                    lease_duration_seconds: Some(lease_ttl_secs as i32),
                    acquire_time: if holder_changed {
                        Some(MicroTime(now))
                    } else {
                        spec.acquire_time.clone()
                    },
                    renew_time: Some(MicroTime(now)),
                    lease_transitions: Some(transitions),
                });

                match api
                    .replace(lease_name, &PostParams::default(), &lease)
                    .await
                {
                    Ok(lease) => {
                        let view = view_from_k8s_lease(&lease).unwrap_or(MasterView {
                            leader_address: leader_address.to_string(),
                            view_version: transitions as u64,
                        });
                        return Ok(k8s_acquired_result(
                            namespace,
                            lease_name,
                            view,
                            lease_ttl_secs,
                        ));
                    }
                    Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                        sleep_k8s_backoff(attempt).await;
                    }
                    Err(e) => {
                        return Err(HaError::InvalidBackend(format!("k8s replace lease: {e}")));
                    }
                }
            }
            Err(kube::Error::Api(err)) if err.code == 404 => {
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(lease_name.to_string()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(leader_address.to_string()),
                        lease_duration_seconds: Some(lease_ttl_secs as i32),
                        acquire_time: Some(MicroTime(now)),
                        renew_time: Some(MicroTime(now)),
                        lease_transitions: Some(1),
                    }),
                };
                match api.create(&PostParams::default(), &lease).await {
                    Ok(lease) => {
                        let view = view_from_k8s_lease(&lease).unwrap_or(MasterView {
                            leader_address: leader_address.to_string(),
                            view_version: 1,
                        });
                        return Ok(k8s_acquired_result(
                            namespace,
                            lease_name,
                            view,
                            lease_ttl_secs,
                        ));
                    }
                    Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                        sleep_k8s_backoff(attempt).await;
                    }
                    Err(kube::Error::Api(err)) if err.code == 409 => {
                        return Ok(AcquireLeadershipResult {
                            acquired: false,
                            view: read_k8s_view(namespace, lease_name).await?,
                            session: None,
                        });
                    }
                    Err(e) => {
                        return Err(HaError::InvalidBackend(format!("k8s create lease: {e}")));
                    }
                }
            }
            Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                sleep_k8s_backoff(attempt).await;
            }
            Err(e) => return Err(HaError::InvalidBackend(format!("k8s get lease: {e}"))),
        }
    }
    Err(HaError::InvalidBackend(format!(
        "k8s acquire lease exhausted {K8S_OPERATION_MAX_ATTEMPTS} attempts"
    )))
}

pub(super) async fn renew_k8s_lease(
    namespace: &str,
    lease_name: &str,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    validate_session(session)?;
    let api = k8s_lease_api(namespace).await?;
    for attempt in 0..K8S_OPERATION_MAX_ATTEMPTS {
        let mut lease = match api.get(lease_name).await {
            Ok(lease) => lease,
            Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                sleep_k8s_backoff(attempt).await;
                continue;
            }
            Err(e) => {
                return Err(HaError::InvalidBackend(format!(
                    "k8s get lease for renew: {e}"
                )));
            }
        };
        let mut spec = lease.spec.clone().unwrap_or_default();
        if spec.holder_identity.as_deref() != Some(&session.view.leader_address) {
            return Err(HaError::UnavailableInCurrentStatus);
        }
        spec.renew_time = Some(MicroTime(chrono::Utc::now()));
        spec.lease_duration_seconds = Some(session.lease_ttl.as_secs() as i32);
        lease.spec = Some(spec);
        match api
            .replace(lease_name, &PostParams::default(), &lease)
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                sleep_k8s_backoff(attempt).await;
            }
            Err(e) => return Err(HaError::InvalidBackend(format!("k8s renew lease: {e}"))),
        }
    }
    Err(HaError::InvalidBackend(format!(
        "k8s renew lease exhausted {K8S_OPERATION_MAX_ATTEMPTS} attempts"
    )))
}

pub(super) fn start_k8s_keepalive(
    namespace: &str,
    lease_name: &str,
    session: &LeadershipSession,
    role_tx: watch::Sender<LeaderRole>,
    active_owner_token: Arc<Mutex<Option<String>>>,
    label_reconciler: Option<K8sLeaderLabelReconciler>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), HaError> {
    validate_session(session)?;
    let owner_token = session.owner_token.clone();
    let namespace = namespace.to_string();
    let lease_name = lease_name.to_string();
    let session = session.clone();
    tokio::spawn(async move {
        let sleep_for = std::cmp::max(Duration::from_secs(1), session.lease_ttl / 2);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {
                    if let Err(e) = renew_k8s_lease(&namespace, &lease_name, &session).await {
                        error!("K8s lease keepalive failed: {}, leadership lost", e);
                        set_k8s_leader_label(&label_reconciler, false);
                        let _ = role_tx.send(LeaderRole::Standby);
                        clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                        return;
                    }
                }
                _ = &mut cancel_rx => {
                    info!("K8s leadership keepalive cancelled");
                    return;
                }
            }
        }
    });
    Ok(())
}

pub(super) async fn release_k8s_lease(
    namespace: &str,
    lease_name: &str,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    let api = k8s_lease_api(namespace).await?;
    for attempt in 0..K8S_OPERATION_MAX_ATTEMPTS {
        let mut lease = match api.get(lease_name).await {
            Ok(lease) => lease,
            Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                sleep_k8s_backoff(attempt).await;
                continue;
            }
            Err(e) => {
                return Err(HaError::InvalidBackend(format!(
                    "k8s get lease for release: {e}"
                )));
            }
        };
        let mut spec = lease.spec.clone().unwrap_or_default();
        if spec.holder_identity.as_deref() != Some(&session.view.leader_address) {
            return Err(HaError::UnavailableInCurrentStatus);
        }
        spec.holder_identity = None;
        spec.renew_time = Some(MicroTime(chrono::Utc::now()));
        lease.spec = Some(spec);
        match api
            .replace(lease_name, &PostParams::default(), &lease)
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) if is_k8s_retryable_error(&e) && has_k8s_retry_attempt(attempt) => {
                sleep_k8s_backoff(attempt).await;
            }
            Err(e) => return Err(HaError::InvalidBackend(format!("k8s release lease: {e}"))),
        }
    }
    Err(HaError::InvalidBackend(format!(
        "k8s release lease exhausted {K8S_OPERATION_MAX_ATTEMPTS} attempts"
    )))
}

pub(super) async fn wait_for_k8s_view_change(
    namespace: &str,
    lease_name: &str,
    known_version: u64,
    timeout: Duration,
) -> Result<Option<MasterView>, HaError> {
    let api = k8s_lease_api(namespace).await?;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut resource_version = match api.get(lease_name).await {
        Ok(lease) => {
            let current = view_from_k8s_lease(&lease);
            match current {
                Some(view) if view.view_version != known_version => return Ok(Some(view)),
                Some(_) | None => lease.resource_version().unwrap_or_default(),
            }
        }
        Err(kube::Error::Api(err)) if err.code == 404 && known_version != 0 => return Ok(None),
        Err(kube::Error::Api(err)) if err.code == 404 => "0".to_string(),
        Err(e) => {
            return Err(HaError::InvalidBackend(format!(
                "k8s get lease for watch: {e}"
            )));
        }
    };

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        let params = WatchParams::default()
            .fields(&format!("metadata.name={lease_name}"))
            .timeout(remaining.as_secs().max(1) as u32);
        let stream = tokio::time::timeout(remaining, api.watch(&params, &resource_version))
            .await
            .map_err(|_| HaError::InvalidBackend("k8s lease watch timed out".into()))?
            .map_err(|e| HaError::InvalidBackend(format!("k8s watch lease: {e}")))?;
        pin_mut!(stream);
        while let Some(event) = tokio::time::timeout(remaining, stream.next())
            .await
            .map_err(|_| HaError::InvalidBackend("k8s lease watch timed out".into()))?
        {
            match event.map_err(|e| HaError::InvalidBackend(format!("k8s lease watch: {e}")))? {
                WatchEvent::Added(lease) | WatchEvent::Modified(lease) => {
                    if let Some(rv) = lease.resource_version() {
                        resource_version = rv;
                    }
                    match view_from_k8s_lease(&lease) {
                        Some(view) if view.view_version != known_version => {
                            return Ok(Some(view));
                        }
                        Some(_) | None => {}
                    }
                }
                WatchEvent::Deleted(_) if known_version != 0 => return Ok(None),
                WatchEvent::Deleted(_) | WatchEvent::Bookmark(_) => {}
                WatchEvent::Error(err) => {
                    return Err(HaError::InvalidBackend(format!(
                        "k8s lease watch error: {err:?}"
                    )));
                }
            }
        }

        match read_k8s_view(namespace, lease_name).await? {
            Some(view) if view.view_version != known_version => return Ok(Some(view)),
            None if known_version != 0 => return Ok(None),
            _ => {}
        }
    }
}

pub(super) fn has_k8s_retry_attempt(attempt: usize) -> bool {
    attempt + 1 < K8S_OPERATION_MAX_ATTEMPTS
}

pub(super) fn is_k8s_retryable_error(err: &kube::Error) -> bool {
    match err {
        kube::Error::Api(api_err) => matches!(api_err.code, 409 | 429 | 500 | 502 | 503 | 504),
        _ => true,
    }
}

pub(super) fn k8s_backoff_delay(attempt: usize) -> Duration {
    let multiplier = 1_u64 << attempt.min(16);
    Duration::from_millis(
        (K8S_OPERATION_INITIAL_BACKOFF_MS * multiplier).min(K8S_OPERATION_MAX_BACKOFF_MS),
    )
}

pub(super) async fn sleep_k8s_backoff(attempt: usize) {
    tokio::time::sleep(k8s_backoff_delay(attempt)).await;
}

pub(super) fn view_from_k8s_lease(lease: &Lease) -> Option<MasterView> {
    let spec = lease.spec.as_ref()?;
    let leader_address = spec.holder_identity.as_ref()?.clone();
    if leader_address.trim().is_empty() {
        return None;
    }
    Some(MasterView {
        leader_address,
        view_version: spec.lease_transitions.unwrap_or_default() as u64,
    })
}

pub(super) fn k8s_lease_expired(spec: &LeaseSpec, now: chrono::DateTime<chrono::Utc>) -> bool {
    let ttl = spec.lease_duration_seconds.unwrap_or(0);
    if ttl <= 0 {
        return true;
    }
    match spec.renew_time.as_ref().or(spec.acquire_time.as_ref()) {
        Some(time) => now.signed_duration_since(time.0).num_seconds() >= ttl as i64,
        None => true,
    }
}

pub(super) fn k8s_acquired_result(
    namespace: &str,
    lease_name: &str,
    view: MasterView,
    lease_ttl_secs: i64,
) -> AcquireLeadershipResult {
    AcquireLeadershipResult {
        acquired: true,
        view: Some(view.clone()),
        session: Some(LeadershipSession {
            owner_token: format!("k8s:{namespace}/{lease_name}:{}", view.view_version),
            view,
            lease_ttl: Duration::from_secs(lease_ttl_secs as u64),
        }),
    }
}
