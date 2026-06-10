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
    let mut client = client.clone();
    match client.get(election_key.as_bytes(), None).await {
        Ok(resp) => match resp.kvs().first() {
            Some(kv) => {
                let addr = kv.value_str().unwrap_or("").to_string();
                Ok(Some(MasterView {
                    leader_address: addr,
                    view_version: kv.mod_revision() as u64,
                }))
            }
            None => Ok(None),
        },
        Err(e) => Err(HaError::InvalidBackend(format!(
            "etcd get master view: {e}"
        ))),
    }
}

pub(super) async fn acquire(
    client: &etcd_client::Client,
    election_key: &str,
    leader_address: &str,
    lease_ttl_secs: i64,
    current: Option<MasterView>,
) -> Result<AcquireLeadershipResult, HaError> {
    let mut client = client.clone();
    let lease_resp = client
        .lease_grant(lease_ttl_secs, None)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd lease grant error: {e}")))?;
    let lease_id = lease_resp.id();

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

    let resp = client
        .txn(txn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd create master view: {e}")))?;
    if resp.succeeded() {
        let acquired_view = read_view(&client, election_key)
            .await?
            .unwrap_or(MasterView {
                leader_address: leader_address.to_string(),
                view_version: 1,
            });
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
            view: current.or(read_view(&client, election_key).await?),
            session: None,
        })
    }
}

pub(super) fn start_keepalive(
    client: &etcd_client::Client,
    session: &LeadershipSession,
    role_tx: watch::Sender<LeaderRole>,
    active_owner_token: Arc<Mutex<Option<String>>>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), HaError> {
    let mut client = client.clone();
    let lease_id = parse_lease_id(session)?;
    let owner_token = session.owner_token.clone();
    tokio::spawn(async move {
        let (mut keeper, _stream) = match client.lease_keep_alive(lease_id).await {
            Ok(res) => res,
            Err(e) => {
                error!("Failed to create lease keepalive: {}", e);
                let _ = role_tx.send(LeaderRole::Standby);
                clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                return;
            }
        };
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(3)) => {
                    if let Err(e) = keeper.keep_alive().await {
                        error!("Lease keepalive error: {}, leadership lost", e);
                        let _ = role_tx.send(LeaderRole::Standby);
                        clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                        return;
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
    let lease_id = parse_lease_id(session)?;
    let mut c = client.clone();
    let (mut keeper, _stream) = c
        .lease_keep_alive(lease_id)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd keepalive: {e}")))?;
    keeper
        .keep_alive()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("etcd keepalive send: {e}")))?;
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
    session
        .owner_token
        .parse::<i64>()
        .map_err(|e| HaError::InvalidParams(format!("invalid etcd owner token: {e}")))
}
