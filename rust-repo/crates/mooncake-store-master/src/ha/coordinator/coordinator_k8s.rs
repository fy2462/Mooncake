use super::coordinator_common::{
    SharedLeadershipSessionState, report_leadership_loss_if_active_with, validate_session,
};
use super::*;
use crate::ha::types::K8sPodIdentity;
use futures_util::{StreamExt, pin_mut};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Patch, PatchParams, PostParams, WatchEvent, WatchParams};
use kube::{Api, Client, ResourceExt};
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
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
        let pod_identity = Arc::new(pod_identity);
        let desired_tx =
            start_label_reconciler(true, K8S_LABEL_RECONCILE_INTERVAL, move |desired| {
                let pod_identity = Arc::clone(&pod_identity);
                async move { apply_k8s_leader_label(&pod_identity, desired).await }
            })
            .expect("the K8s label reconciler is enabled");
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

fn start_label_reconciler<Apply, ApplyFuture>(
    enabled: bool,
    retry_interval: Duration,
    apply: Apply,
) -> Option<watch::Sender<bool>>
where
    Apply: Fn(bool) -> ApplyFuture + Send + 'static,
    ApplyFuture: Future<Output = Result<(), HaError>> + Send + 'static,
{
    if !enabled {
        return None;
    }
    let (desired_tx, desired_rx) = watch::channel(false);
    tokio::spawn(run_label_reconciler(desired_rx, retry_interval, apply));
    Some(desired_tx)
}

async fn run_label_reconciler<Apply, ApplyFuture>(
    mut desired_rx: watch::Receiver<bool>,
    retry_interval: Duration,
    apply: Apply,
) where
    Apply: Fn(bool) -> ApplyFuture,
    ApplyFuture: Future<Output = Result<(), HaError>>,
{
    // Drive the pod label toward the latest desired state. Transient K8s
    // failures keep `applied` unset so the worker retries every second.
    let mut applied: Option<bool> = None;
    loop {
        let desired = *desired_rx.borrow();
        if applied != Some(desired) {
            match apply(desired).await {
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
            _ = tokio::time::sleep(retry_interval), if applied.is_none() => {}
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
        Ok(lease) => active_view_from_k8s_lease(&lease, chrono::Utc::now()),
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
    if leader_address.trim().is_empty() {
        return Err(HaError::InvalidParams(
            "k8s leader address must not be empty".into(),
        ));
    }
    let lease_ttl_secs_i32 = k8s_lease_ttl_i32(lease_ttl_secs)?;

    let api = k8s_lease_api(namespace).await?;
    for attempt in 0..K8S_OPERATION_MAX_ATTEMPTS {
        let now = chrono::Utc::now();
        match api.get(lease_name).await {
            Ok(mut lease) => {
                let spec = lease.spec.clone().unwrap_or_default();
                let current_view = view_from_k8s_lease(&lease)?;
                if !k8s_lease_available_for_acquisition(&spec, now) {
                    return Ok(AcquireLeadershipResult {
                        acquired: false,
                        view: current_view,
                        session: None,
                    });
                }

                // Every newly acquired session is a new term, including a
                // restarted pod that reuses the same advertised address.
                let transitions = next_k8s_lease_transition(spec.lease_transitions)?;
                lease.spec = Some(LeaseSpec {
                    holder_identity: Some(leader_address.to_string()),
                    lease_duration_seconds: Some(lease_ttl_secs_i32),
                    acquire_time: Some(MicroTime(now)),
                    renew_time: Some(MicroTime(now)),
                    lease_transitions: Some(transitions),
                });

                match api
                    .replace(lease_name, &PostParams::default(), &lease)
                    .await
                {
                    Ok(lease) => {
                        let view = validate_acquired_k8s_view(
                            view_from_k8s_lease(&lease)?,
                            leader_address,
                            transitions,
                        )?;
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
                        lease_duration_seconds: Some(lease_ttl_secs_i32),
                        acquire_time: Some(MicroTime(now)),
                        renew_time: Some(MicroTime(now)),
                        lease_transitions: Some(1),
                    }),
                };
                match api.create(&PostParams::default(), &lease).await {
                    Ok(lease) => {
                        let view = validate_acquired_k8s_view(
                            view_from_k8s_lease(&lease)?,
                            leader_address,
                            1,
                        )?;
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
        let mut spec = lease
            .spec
            .clone()
            .ok_or_else(|| HaError::InvalidBackend("k8s lease has no spec during renew".into()))?;
        validate_k8s_session_lease(&spec, session)?;
        let now = chrono::Utc::now();
        if k8s_lease_expired(&spec, now) {
            return Err(HaError::UnavailableInCurrentStatus);
        }
        spec.renew_time = Some(MicroTime(now));
        spec.lease_duration_seconds = Some(k8s_session_lease_ttl_i32(session)?);
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
    loss_tx: broadcast::Sender<LeadershipLossEvent>,
    session_state: SharedLeadershipSessionState,
    label_reconciler: Option<K8sLeaderLabelReconciler>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), HaError> {
    validate_session(session)?;
    let namespace = namespace.to_string();
    let lease_name = lease_name.to_string();
    let session = session.clone();
    tokio::spawn(async move {
        let sleep_for = k8s_keepalive_interval(session.lease_ttl);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {
                    if let Err(e) = renew_k8s_lease(&namespace, &lease_name, &session).await {
                        error!("K8s lease keepalive failed: {}, leadership lost", e);
                        report_leadership_loss_if_active_with(
                            &session_state,
                            &session,
                            &role_tx,
                            &loss_tx,
                            || set_k8s_leader_label(&label_reconciler, false),
                        );
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

fn k8s_keepalive_interval(lease_ttl: Duration) -> Duration {
    let ttl_ms = u64::try_from(lease_ttl.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis((ttl_ms / 3).clamp(100, 3_000))
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
        let mut spec = lease.spec.clone().ok_or_else(|| {
            HaError::InvalidBackend("k8s lease has no spec during release".into())
        })?;
        validate_k8s_session_lease(&spec, session)?;
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
            let current = active_view_from_k8s_lease(&lease, chrono::Utc::now())?;
            match current {
                Some(view) if view.view_version != known_version => return Ok(Some(view)),
                None if known_version != 0 => return Ok(None),
                Some(_) | None => lease.resource_version().ok_or_else(|| {
                    HaError::InvalidBackend("k8s lease has no resource version".into())
                })?,
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
        let stream =
            match tokio::time::timeout(remaining, api.watch(&params, &resource_version)).await {
                Ok(result) => {
                    result.map_err(|e| HaError::InvalidBackend(format!("k8s watch lease: {e}")))?
                }
                Err(_) => return Ok(None),
            };
        pin_mut!(stream);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let Some(event) = (match tokio::time::timeout(remaining, stream.next()).await {
                Ok(event) => event,
                Err(_) => return Ok(None),
            }) else {
                break;
            };
            match event.map_err(|e| HaError::InvalidBackend(format!("k8s lease watch: {e}")))? {
                WatchEvent::Added(lease) | WatchEvent::Modified(lease) => {
                    if let Some(rv) = lease.resource_version() {
                        resource_version = rv;
                    }
                    match active_view_from_k8s_lease(&lease, chrono::Utc::now())? {
                        Some(view) if view.view_version != known_version => {
                            return Ok(Some(view));
                        }
                        None if known_version != 0 => return Ok(None),
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

pub(super) fn view_from_k8s_lease(lease: &Lease) -> Result<Option<MasterView>, HaError> {
    let spec = lease
        .spec
        .as_ref()
        .ok_or_else(|| HaError::InvalidBackend("k8s lease has no spec".into()))?;
    let Some(leader_address) = spec.holder_identity.as_ref() else {
        return Ok(None);
    };
    if leader_address.trim().is_empty() {
        return Err(HaError::InvalidBackend(
            "k8s lease holder identity is empty".into(),
        ));
    }
    let transitions = spec.lease_transitions.ok_or_else(|| {
        HaError::InvalidBackend("k8s held lease has no transition version".into())
    })?;
    let view_version = u64::try_from(transitions)
        .ok()
        .filter(|version| *version > 0)
        .ok_or_else(|| {
            HaError::InvalidBackend(format!(
                "k8s held lease has invalid transition version: {transitions}"
            ))
        })?;
    Ok(Some(MasterView {
        leader_address: leader_address.clone(),
        view_version,
    }))
}

fn active_view_from_k8s_lease(
    lease: &Lease,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<MasterView>, HaError> {
    let view = view_from_k8s_lease(lease)?;
    let Some(view) = view else {
        return Ok(None);
    };
    let spec = lease
        .spec
        .as_ref()
        .ok_or_else(|| HaError::InvalidBackend("k8s lease has no spec".into()))?;
    if k8s_lease_expired(spec, now) {
        Ok(None)
    } else {
        Ok(Some(view))
    }
}

fn k8s_lease_ttl_i32(lease_ttl_secs: i64) -> Result<i32, HaError> {
    i32::try_from(lease_ttl_secs)
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| {
            HaError::InvalidParams(
                "k8s leadership lease TTL must be positive and fit i32 seconds".into(),
            )
        })
}

fn k8s_session_lease_ttl_i32(session: &LeadershipSession) -> Result<i32, HaError> {
    i32::try_from(session.lease_ttl.as_secs())
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| {
            HaError::InvalidParams(
                "k8s leadership session TTL must be positive whole seconds and fit i32".into(),
            )
        })
}

fn next_k8s_lease_transition(current: Option<i32>) -> Result<i32, HaError> {
    let current = current.unwrap_or_default();
    if current < 0 {
        return Err(HaError::InvalidBackend(format!(
            "k8s lease has negative transition version: {current}"
        )));
    }
    current
        .checked_add(1)
        .ok_or_else(|| HaError::InvalidBackend("k8s lease transition version is exhausted".into()))
}

fn validate_acquired_k8s_view(
    view: Option<MasterView>,
    expected_leader_address: &str,
    expected_transition: i32,
) -> Result<MasterView, HaError> {
    let view = view.ok_or_else(|| {
        HaError::InvalidBackend("k8s acquisition succeeded without a leader view".into())
    })?;
    if view.leader_address != expected_leader_address
        || view.view_version != expected_transition as u64
    {
        return Err(HaError::InvalidBackend(format!(
            "k8s acquired view mismatch: expected_address={expected_leader_address:?}, expected_transition={expected_transition}, actual={view:?}"
        )));
    }
    Ok(view)
}

fn validate_k8s_session_lease(
    spec: &LeaseSpec,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    if spec.holder_identity.as_deref() != Some(&session.view.leader_address) {
        return Err(HaError::UnavailableInCurrentStatus);
    }
    let transitions = spec.lease_transitions.ok_or_else(|| {
        HaError::InvalidBackend("k8s held lease has no transition version".into())
    })?;
    let transitions = u64::try_from(transitions).map_err(|_| {
        HaError::InvalidBackend("k8s held lease has negative transition version".into())
    })?;
    if transitions == 0 || transitions != session.view.view_version {
        return Err(HaError::UnavailableInCurrentStatus);
    }
    Ok(())
}

fn k8s_lease_available_for_acquisition(
    spec: &LeaseSpec,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    spec.holder_identity.is_none() || k8s_lease_expired(spec, now)
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

#[cfg(test)]
mod tests {
    use super::{
        active_view_from_k8s_lease, k8s_keepalive_interval, k8s_lease_available_for_acquisition,
        next_k8s_lease_transition, start_label_reconciler, validate_k8s_session_lease,
        view_from_k8s_lease,
    };
    use crate::ha::{HaError, LeadershipSession, MasterView};
    use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::watch;

    #[derive(Default)]
    struct FakeLabelBackendState {
        call_count: usize,
        fail_remaining: usize,
        commit_then_fail_remaining: usize,
        applied: Option<bool>,
        committed: Option<bool>,
    }

    #[derive(Clone, Default)]
    struct FakeLabelBackend(Arc<Mutex<FakeLabelBackendState>>);

    impl FakeLabelBackend {
        fn fail_next(&self, count: usize) {
            self.0.lock().unwrap().fail_remaining = count;
        }

        fn commit_then_fail_next(&self, count: usize) {
            self.0.lock().unwrap().commit_then_fail_remaining = count;
        }

        async fn apply(&self, desired: bool) -> Result<(), HaError> {
            let mut state = self.0.lock().unwrap();
            state.call_count += 1;
            if state.commit_then_fail_remaining > 0 {
                state.commit_then_fail_remaining -= 1;
                state.committed = Some(desired);
                return Err(HaError::InvalidBackend("injected ambiguous failure".into()));
            }
            if state.fail_remaining > 0 {
                state.fail_remaining -= 1;
                return Err(HaError::InvalidBackend("injected transient failure".into()));
            }
            state.committed = Some(desired);
            state.applied = Some(desired);
            Ok(())
        }

        fn call_count(&self) -> usize {
            self.0.lock().unwrap().call_count
        }

        fn applied(&self) -> Option<bool> {
            self.0.lock().unwrap().applied
        }

        fn committed(&self) -> Option<bool> {
            self.0.lock().unwrap().committed
        }

        async fn wait_for_applied(&self, expected: bool) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while self.applied() != Some(expected) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }

        async fn wait_for_committed(&self, expected: bool) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while self.committed() != Some(expected) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }
    }

    fn test_reconciler(
        enabled: bool,
        backend: FakeLabelBackend,
        retry_interval: Duration,
    ) -> Option<watch::Sender<bool>> {
        start_label_reconciler(enabled, retry_interval, move |desired| {
            let backend = backend.clone();
            async move { backend.apply(desired).await }
        })
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_converges_to_leader_after_transient_failures() {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(true, backend.clone(), Duration::from_millis(5)).unwrap();
        backend.wait_for_applied(false).await;
        let baseline = backend.call_count();
        backend.fail_next(3);

        desired.send(true).unwrap();

        backend.wait_for_applied(true).await;
        assert!(backend.call_count() >= baseline + 4);
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_clears_leader_after_transient_failures() {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(true, backend.clone(), Duration::from_millis(5)).unwrap();
        desired.send(true).unwrap();
        backend.wait_for_applied(true).await;

        backend.fail_next(3);
        desired.send(false).unwrap();

        backend.wait_for_applied(false).await;
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_latest_desired_state_wins() {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(true, backend.clone(), Duration::from_millis(5)).unwrap();

        desired.send(true).unwrap();
        desired.send(false).unwrap();

        backend.wait_for_applied(false).await;
        assert_eq!(backend.applied(), Some(false));
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_stops_calling_once_converged() {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(true, backend.clone(), Duration::from_millis(5)).unwrap();
        desired.send(true).unwrap();
        backend.wait_for_applied(true).await;

        let settled = backend.call_count();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(backend.call_count(), settled);
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_disabled_never_applies() {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(false, backend.clone(), Duration::from_millis(5));

        assert!(desired.is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(backend.call_count(), 0);
        assert_eq!(backend.applied(), None);
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_ambiguous_set_failure_does_not_leave_stale_leader()
    {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(true, backend.clone(), Duration::from_secs(10)).unwrap();
        backend.wait_for_applied(false).await;
        backend.commit_then_fail_next(1);
        desired.send(true).unwrap();
        backend.wait_for_committed(true).await;

        desired.send(false).unwrap();

        backend.wait_for_committed(false).await;
    }

    #[tokio::test]
    async fn cpp_parity_leader_label_reconciler_ambiguous_clear_failure_does_not_leave_missing_label()
     {
        let backend = FakeLabelBackend::default();
        let desired = test_reconciler(true, backend.clone(), Duration::from_secs(10)).unwrap();
        desired.send(true).unwrap();
        backend.wait_for_committed(true).await;

        backend.commit_then_fail_next(1);
        desired.send(false).unwrap();
        backend.wait_for_committed(false).await;

        desired.send(true).unwrap();

        backend.wait_for_committed(true).await;
    }

    fn lease(holder: Option<&str>, transitions: Option<i32>) -> Lease {
        Lease {
            spec: Some(LeaseSpec {
                holder_identity: holder.map(ToOwned::to_owned),
                lease_transitions: transitions,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn k8s_view_requires_a_positive_durable_term() {
        let view = view_from_k8s_lease(&lease(Some("leader-a"), Some(7)))
            .unwrap()
            .unwrap();
        assert_eq!(view.leader_address, "leader-a");
        assert_eq!(view.view_version, 7);
        assert!(
            view_from_k8s_lease(&lease(None, Some(7)))
                .unwrap()
                .is_none()
        );
        for malformed in [
            lease(Some(""), Some(7)),
            lease(Some("leader-a"), None),
            lease(Some("leader-a"), Some(0)),
            lease(Some("leader-a"), Some(-1)),
        ] {
            assert!(view_from_k8s_lease(&malformed).is_err());
        }
        assert!(view_from_k8s_lease(&Lease::default()).is_err());
    }

    #[test]
    fn every_k8s_acquisition_advances_the_term() {
        assert_eq!(next_k8s_lease_transition(None).unwrap(), 1);
        assert_eq!(next_k8s_lease_transition(Some(7)).unwrap(), 8);
        assert!(next_k8s_lease_transition(Some(-1)).is_err());
        assert!(next_k8s_lease_transition(Some(i32::MAX)).is_err());
    }

    #[test]
    fn live_k8s_lease_cannot_be_reacquired_by_the_same_address() {
        let now = chrono::Utc::now();
        let live = LeaseSpec {
            holder_identity: Some("leader-a".into()),
            lease_duration_seconds: Some(10),
            renew_time: Some(MicroTime(now)),
            lease_transitions: Some(7),
            ..Default::default()
        };
        assert!(!k8s_lease_available_for_acquisition(&live, now));

        let released = LeaseSpec {
            holder_identity: None,
            ..live.clone()
        };
        assert!(k8s_lease_available_for_acquisition(&released, now));

        let expired = LeaseSpec {
            renew_time: Some(MicroTime(now - chrono::Duration::seconds(10))),
            ..live
        };
        assert!(k8s_lease_available_for_acquisition(&expired, now));
    }

    #[test]
    fn expired_k8s_lease_is_not_a_discoverable_leader() {
        let now = chrono::Utc::now();
        let live = Lease {
            spec: Some(LeaseSpec {
                holder_identity: Some("leader-a".into()),
                lease_duration_seconds: Some(10),
                renew_time: Some(MicroTime(now)),
                lease_transitions: Some(7),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            active_view_from_k8s_lease(&live, now)
                .unwrap()
                .unwrap()
                .view_version,
            7
        );

        let expired = Lease {
            spec: Some(LeaseSpec {
                renew_time: Some(MicroTime(now - chrono::Duration::seconds(10))),
                ..live.spec.unwrap()
            }),
            ..Default::default()
        };
        assert!(active_view_from_k8s_lease(&expired, now).unwrap().is_none());
    }

    #[test]
    fn k8s_keepalive_interval_precedes_short_lease_expiry() {
        assert_eq!(
            k8s_keepalive_interval(Duration::from_secs(1)),
            Duration::from_millis(333)
        );
        assert_eq!(
            k8s_keepalive_interval(Duration::from_secs(30)),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn k8s_session_fences_on_address_or_term_mismatch() {
        let session = LeadershipSession {
            view: MasterView {
                leader_address: "leader-a".into(),
                view_version: 7,
            },
            owner_token: "k8s:ns/lease:7".into(),
            lease_ttl: Duration::from_secs(5),
        };
        assert!(
            validate_k8s_session_lease(
                lease(Some("leader-a"), Some(7)).spec.as_ref().unwrap(),
                &session,
            )
            .is_ok()
        );
        for stale in [
            lease(Some("leader-b"), Some(7)),
            lease(Some("leader-a"), Some(8)),
            lease(Some("leader-a"), None),
        ] {
            assert!(validate_k8s_session_lease(stale.spec.as_ref().unwrap(), &session).is_err());
        }
    }
}
