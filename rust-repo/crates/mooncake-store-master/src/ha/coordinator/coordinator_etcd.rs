use super::coordinator_common::{clear_active_owner_token_if_matches, resolve_cluster_namespace};
use crate::ha::types::{
    AcquireLeadershipResult, HaError, LeaderRole, LeadershipSession, MasterView,
};
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

pub(super) fn build_master_view_key(cluster_namespace: &str) -> String {
    let namespace = resolve_cluster_namespace(cluster_namespace);
    format!(
        "mooncake-store/{}/master_view",
        namespace.trim_end_matches('/')
    )
}

pub(super) async fn read_view(
    client: &etcd_client::Client,
    election_key: &str,
) -> Result<Option<MasterView>, HaError> {
    Ok(read_view_record(client, election_key)
        .await?
        .map(|(view, _lease_id)| view))
}

async fn read_view_record(
    client: &etcd_client::Client,
    election_key: &str,
) -> Result<Option<(MasterView, i64)>, HaError> {
    let mut client = client.clone();
    match client.get(election_key.as_bytes(), None).await {
        Ok(resp) => match resp.kvs().first() {
            Some(kv) => {
                let lease_id = kv.lease();
                if lease_id == 0 {
                    return Err(HaError::InvalidBackend(
                        "etcd master view is not attached to a lease".into(),
                    ));
                }
                decode_master_view(kv.value(), kv.mod_revision()).map(|view| Some((view, lease_id)))
            }
            None => Ok(None),
        },
        Err(e) => Err(HaError::InvalidBackend(format!(
            "etcd get master view: {e}"
        ))),
    }
}

fn decode_master_view(value: &[u8], mod_revision: i64) -> Result<MasterView, HaError> {
    let leader_address = std::str::from_utf8(value)
        .map_err(|error| {
            HaError::InvalidBackend(format!("etcd master view address is not UTF-8: {error}"))
        })?
        .to_string();
    if leader_address.trim().is_empty() {
        return Err(HaError::InvalidBackend(
            "etcd master view address is empty".into(),
        ));
    }
    let view_version = u64::try_from(mod_revision).map_err(|_| {
        HaError::InvalidBackend(format!(
            "etcd master view has negative revision: {mod_revision}"
        ))
    })?;
    if view_version == 0 {
        return Err(HaError::InvalidBackend(
            "etcd master view has zero revision".into(),
        ));
    }
    Ok(MasterView {
        leader_address,
        view_version,
    })
}

pub(super) async fn acquire(
    client: &etcd_client::Client,
    election_key: &str,
    leader_address: &str,
    lease_ttl_secs: i64,
) -> Result<AcquireLeadershipResult, HaError> {
    if leader_address.trim().is_empty() {
        return Err(HaError::InvalidParams(
            "etcd leader address must not be empty".into(),
        ));
    }
    if lease_ttl_secs <= 0 {
        return Err(HaError::InvalidParams(
            "etcd leadership lease TTL must be positive".into(),
        ));
    }
    let mut client = client.clone();
    let lease_resp = client
        .lease_grant(lease_ttl_secs, None)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd lease grant error: {e}")))?;
    let lease_id = lease_resp.id();
    if lease_id == 0 {
        return Err(HaError::InvalidBackend(
            "etcd lease grant returned an invalid zero lease ID".into(),
        ));
    }

    let put = TxnOp::put(
        election_key.as_bytes().to_vec(),
        leader_address.as_bytes().to_vec(),
        Some(PutOptions::new().with_lease(lease_id)),
    );
    let txn = Txn::new()
        .when([Compare::version(
            election_key.as_bytes().to_vec(),
            CompareOp::Equal,
            0,
        )])
        .and_then([put]);

    let resp = match client.txn(txn).await {
        Ok(response) => response,
        Err(error) => {
            // The transaction outcome can be ambiguous after a transport
            // error. Revoking our lease safely removes a write that did land.
            let _ = client.lease_revoke(lease_id).await;
            return Err(HaError::InvalidBackend(format!(
                "etcd create master view: {error}"
            )));
        }
    };
    if resp.succeeded() {
        let acquired_view = match read_view_record(&client, election_key).await {
            Ok(Some((view, actual_lease_id))) => {
                match validate_acquired_view(view, actual_lease_id, leader_address, lease_id) {
                    Ok(view) => view,
                    Err(error) => {
                        let _ = client.lease_revoke(lease_id).await;
                        return Err(error);
                    }
                }
            }
            Ok(None) => {
                let _ = client.lease_revoke(lease_id).await;
                return Err(HaError::InvalidBackend(
                    "etcd leadership transaction succeeded but master view is missing".into(),
                ));
            }
            Err(error) => {
                let _ = client.lease_revoke(lease_id).await;
                return Err(error);
            }
        };
        let session = LeadershipSession {
            view: acquired_view.clone(),
            owner_token: lease_id.to_string(),
            lease_ttl: Duration::from_secs(lease_ttl_secs as u64),
        };
        info!(
            "Leadership acquired: address={}, lease_id={}, view_version={}",
            leader_address, lease_id, acquired_view.view_version
        );
        Ok(AcquireLeadershipResult {
            acquired: true,
            view: Some(acquired_view),
            session: Some(session),
        })
    } else {
        let _ = client.lease_revoke(lease_id).await;
        Ok(AcquireLeadershipResult {
            acquired: false,
            // The failed compare proves another writer owned the key at
            // transaction commit time. Re-read after that point instead of
            // returning a view observed before the campaign.
            view: read_view(&client, election_key).await?,
            session: None,
        })
    }
}

fn validate_acquired_view(
    view: MasterView,
    actual_lease_id: i64,
    expected_leader_address: &str,
    expected_lease_id: i64,
) -> Result<MasterView, HaError> {
    if view.leader_address != expected_leader_address || actual_lease_id != expected_lease_id {
        return Err(HaError::InvalidBackend(format!(
            "etcd acquired master view identity mismatch: expected_address={expected_leader_address:?}, actual_address={:?}, expected_lease_id={expected_lease_id}, actual_lease_id={actual_lease_id}",
            view.leader_address
        )));
    }
    Ok(view)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_master_view, keepalive_ack_timeout, keepalive_interval, parse_lease_id,
        validate_acquired_view, validate_keepalive_ack,
    };
    use crate::ha::{LeadershipSession, MasterView};
    use std::time::Duration;

    #[test]
    fn master_view_decoder_is_fail_closed() {
        let view = decode_master_view(b"127.0.0.1:50051", 7).unwrap();
        assert_eq!(view.leader_address, "127.0.0.1:50051");
        assert_eq!(view.view_version, 7);
        assert!(decode_master_view(&[0xff], 7).is_err());
        assert!(decode_master_view(b"", 7).is_err());
        assert!(decode_master_view(b"127.0.0.1:50051", 0).is_err());
        assert!(decode_master_view(b"127.0.0.1:50051", -1).is_err());
    }

    #[test]
    fn keepalive_requires_matching_positive_acknowledgement() {
        assert!(validate_keepalive_ack(7, 7, 10).is_ok());
        assert!(validate_keepalive_ack(7, 8, 10).is_err());
        assert!(validate_keepalive_ack(7, 7, 0).is_err());
        assert!(validate_keepalive_ack(7, 7, -1).is_err());
        assert_eq!(
            keepalive_interval(Duration::from_secs(9)),
            Duration::from_secs(3)
        );
        assert_eq!(
            keepalive_ack_timeout(Duration::from_secs(2)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn acquired_view_is_bound_to_the_exact_lease_session() {
        let view = MasterView {
            leader_address: "127.0.0.1:50051".into(),
            view_version: 7,
        };
        assert_eq!(
            validate_acquired_view(view.clone(), 11, "127.0.0.1:50051", 11).unwrap(),
            view
        );
        assert!(validate_acquired_view(view.clone(), 12, "127.0.0.1:50051", 11).is_err());
        assert!(validate_acquired_view(view, 11, "127.0.0.1:50052", 11).is_err());
    }

    #[test]
    fn etcd_session_rejects_a_zero_lease_id() {
        let session = LeadershipSession {
            view: MasterView {
                leader_address: "127.0.0.1:50051".into(),
                view_version: 7,
            },
            owner_token: "0".into(),
            lease_ttl: Duration::from_secs(3),
        };
        assert!(parse_lease_id(&session).is_err());
    }
}

pub(super) fn start_keepalive(
    client: &etcd_client::Client,
    session: &LeadershipSession,
    role_tx: watch::Sender<LeaderRole>,
    active_owner_token: Arc<Mutex<Option<String>>>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), HaError> {
    super::coordinator_common::validate_session(session)?;
    let mut client = client.clone();
    let lease_id = parse_lease_id(session)?;
    let owner_token = session.owner_token.clone();
    let keepalive_interval = keepalive_interval(session.lease_ttl);
    let acknowledgement_timeout = keepalive_ack_timeout(session.lease_ttl);
    tokio::spawn(async move {
        let (mut keeper, mut stream) = match client.lease_keep_alive(lease_id).await {
            Ok(res) => res,
            Err(e) => {
                error!("Failed to create lease keepalive: {}", e);
                let _ = role_tx.send(LeaderRole::Standby);
                clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                return;
            }
        };
        if keeper.id() != lease_id {
            error!(
                "Lease keepalive opened for unexpected lease: expected_id={}, actual_id={}",
                lease_id,
                keeper.id()
            );
            let _ = role_tx.send(LeaderRole::Standby);
            clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
            return;
        }
        loop {
            tokio::select! {
                _ = tokio::time::sleep(keepalive_interval) => {
                    if let Err(e) = keeper.keep_alive().await {
                        error!("Lease keepalive error: {}, leadership lost", e);
                        let _ = role_tx.send(LeaderRole::Standby);
                        clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                        return;
                    }
                    let response = tokio::select! {
                        _ = &mut cancel_rx => {
                            info!("Leadership keepalive cancelled");
                            return;
                        }
                        response = tokio::time::timeout(
                            acknowledgement_timeout,
                            stream.message(),
                        ) => response,
                    };
                    match response {
                        Ok(Ok(Some(response)))
                            if validate_keepalive_ack(
                                lease_id,
                                response.id(),
                                response.ttl(),
                            )
                            .is_ok() => {}
                        Ok(Ok(Some(response))) => {
                            error!(
                                "Lease keepalive returned invalid acknowledgement: expected_id={}, actual_id={}, ttl={}",
                                lease_id,
                                response.id(),
                                response.ttl()
                            );
                            let _ = role_tx.send(LeaderRole::Standby);
                            clear_active_owner_token_if_matches(
                                &active_owner_token,
                                &owner_token,
                            );
                            return;
                        }
                        Ok(Ok(None)) => {
                            error!("Lease keepalive stream closed, leadership lost");
                            let _ = role_tx.send(LeaderRole::Standby);
                            clear_active_owner_token_if_matches(
                                &active_owner_token,
                                &owner_token,
                            );
                            return;
                        }
                        Ok(Err(error)) => {
                            error!("Lease keepalive acknowledgement failed: {error}");
                            let _ = role_tx.send(LeaderRole::Standby);
                            clear_active_owner_token_if_matches(
                                &active_owner_token,
                                &owner_token,
                            );
                            return;
                        }
                        Err(_) => {
                            error!("Lease keepalive acknowledgement timed out, leadership lost");
                            let _ = role_tx.send(LeaderRole::Standby);
                            clear_active_owner_token_if_matches(
                                &active_owner_token,
                                &owner_token,
                            );
                            return;
                        }
                    }
                }
                _ = &mut cancel_rx => {
                    info!("Leadership keepalive cancelled");
                    return;
                }
            }
        }
    });
    Ok(())
}

pub(super) async fn renew(
    client: &etcd_client::Client,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    super::coordinator_common::validate_session(session)?;
    let lease_id = parse_lease_id(session)?;
    let mut c = client.clone();
    let (mut keeper, mut stream) = c
        .lease_keep_alive(lease_id)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd keepalive: {e}")))?;
    if keeper.id() != lease_id {
        return Err(HaError::InvalidBackend(format!(
            "etcd keepalive opened for unexpected lease: expected_id={lease_id}, actual_id={}",
            keeper.id()
        )));
    }
    keeper
        .keep_alive()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd keepalive send: {e}")))?;
    let response = tokio::time::timeout(keepalive_ack_timeout(session.lease_ttl), stream.message())
        .await
        .map_err(|_| HaError::InvalidBackend("etcd keepalive acknowledgement timeout".into()))?
        .map_err(|e| HaError::InvalidBackend(format!("etcd keepalive acknowledgement: {e}")))?
        .ok_or_else(|| HaError::InvalidBackend("etcd keepalive stream closed".into()))?;
    validate_keepalive_ack(lease_id, response.id(), response.ttl())?;
    Ok(())
}

fn keepalive_interval(lease_ttl: Duration) -> Duration {
    let ttl_ms = u64::try_from(lease_ttl.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis((ttl_ms / 3).clamp(100, 3_000))
}

fn keepalive_ack_timeout(lease_ttl: Duration) -> Duration {
    let ttl_ms = u64::try_from(lease_ttl.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis((ttl_ms / 2).clamp(100, 3_000))
}

fn validate_keepalive_ack(
    expected_lease_id: i64,
    actual_lease_id: i64,
    ttl: i64,
) -> Result<(), HaError> {
    if actual_lease_id != expected_lease_id || ttl <= 0 {
        return Err(HaError::InvalidBackend(format!(
            "invalid etcd keepalive acknowledgement: expected_id={expected_lease_id}, actual_id={actual_lease_id}, ttl={ttl}"
        )));
    }
    Ok(())
}

pub(super) async fn release(
    client: &etcd_client::Client,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    let lease_id = parse_lease_id(session)?;
    let mut client = client.clone();
    client
        .lease_revoke(lease_id)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd lease revoke error: {e}")))?;
    info!("Leadership released via lease revoke");
    Ok(())
}

fn parse_lease_id(session: &LeadershipSession) -> Result<i64, HaError> {
    let lease_id = session
        .owner_token
        .parse::<i64>()
        .map_err(|e| HaError::InvalidParams(format!("invalid etcd owner token: {e}")))?;
    if lease_id == 0 {
        return Err(HaError::InvalidParams(
            "etcd owner token must contain a non-zero lease ID".into(),
        ));
    }
    Ok(lease_id)
}
