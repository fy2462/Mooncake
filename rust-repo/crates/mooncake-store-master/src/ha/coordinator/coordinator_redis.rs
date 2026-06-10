use super::coordinator_common::{clear_active_owner_token_if_matches, validate_session};
use crate::ha::types::{
    AcquireLeadershipResult, HaError, LeaderRole, LeadershipSession, MasterView,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

const LEADER_ADDRESS_FIELD: &str = "leader_address";
const VIEW_VERSION_FIELD: &str = "view_version";
const OWNER_TOKEN_FIELD: &str = "owner_token";
const ACQUIRE_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local view_version = redis.call('INCR', KEYS[2])
redis.call(
  'HSET',
  KEYS[1],
  'leader_address', ARGV[1],
  'view_version', tostring(view_version),
  'owner_token', ARGV[3]
)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, view_version}
"#;
const RENEW_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
local owner_token = redis.call('HGET', KEYS[1], 'owner_token')
if not owner_token or owner_token ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"#;
const RELEASE_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
local owner_token = redis.call('HGET', KEYS[1], 'owner_token')
if not owner_token or owner_token ~= ARGV[1] then
  return -1
end
redis.call('DEL', KEYS[1])
return 1
"#;

pub(super) fn build_master_view_key(cluster_namespace: &str) -> String {
    let hash_tag = sanitize_hash_tag(cluster_namespace);
    format!("mooncake-store/{{{hash_tag}}}/master_view")
}

pub(super) fn build_view_version_key(cluster_namespace: &str) -> String {
    let hash_tag = sanitize_hash_tag(cluster_namespace);
    format!("mooncake-store/{{{hash_tag}}}/master_view_version")
}

pub(super) async fn read_view(
    client: &redis::Client,
    election_key: &str,
) -> Result<Option<MasterView>, HaError> {
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
    let (leader_address, view_version, owner_token): (
        Option<String>,
        Option<String>,
        Option<String>,
    ) = redis::cmd("HMGET")
        .arg(election_key)
        .arg(LEADER_ADDRESS_FIELD)
        .arg(VIEW_VERSION_FIELD)
        .arg(OWNER_TOKEN_FIELD)
        .query_async(&mut conn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis hmget master view: {e}")))?;

    match (leader_address, view_version, owner_token) {
        (None, None, None) => Ok(None),
        (Some(leader_address), Some(view_version), Some(owner_token))
            if !leader_address.is_empty() && !owner_token.is_empty() =>
        {
            let view_version = view_version
                .parse::<u64>()
                .map_err(|e| HaError::InvalidBackend(format!("redis parse view version: {e}")))?;
            Ok(Some(MasterView {
                leader_address,
                view_version,
            }))
        }
        _ => Err(HaError::InvalidBackend(
            "redis master view is partially populated".into(),
        )),
    }
}

pub(super) async fn acquire(
    client: &redis::Client,
    election_key: &str,
    view_version_key: &str,
    leader_address: &str,
    lease_ttl_secs: i64,
) -> Result<AcquireLeadershipResult, HaError> {
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
    let ttl_ms = (lease_ttl_secs * 1000) as usize;
    let owner_token = Uuid::new_v4().to_string();
    let result: Vec<i64> = redis::cmd("EVAL")
        .arg(ACQUIRE_SCRIPT)
        .arg(2)
        .arg(election_key)
        .arg(view_version_key)
        .arg(leader_address)
        .arg(ttl_ms)
        .arg(&owner_token)
        .query_async(&mut conn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis acquire: {e}")))?;
    if result.first().copied() == Some(1) {
        let view_version = result.get(1).copied().unwrap_or(1) as u64;
        let view = MasterView {
            leader_address: leader_address.to_string(),
            view_version,
        };
        let session = LeadershipSession {
            view: view.clone(),
            owner_token,
            lease_ttl: Duration::from_secs(lease_ttl_secs as u64),
        };
        Ok(AcquireLeadershipResult {
            acquired: true,
            view: Some(view),
            session: Some(session),
        })
    } else {
        Ok(AcquireLeadershipResult {
            acquired: false,
            view: read_view(client, election_key).await?,
            session: None,
        })
    }
}

pub(super) async fn start_keepalive(
    client: &redis::Client,
    election_key: &str,
    session: &LeadershipSession,
    role_tx: watch::Sender<LeaderRole>,
    active_owner_token: Arc<Mutex<Option<String>>>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), HaError> {
    let _conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            error!("Redis keepalive connection failed: {}", e);
            let _ = role_tx.send(LeaderRole::Standby);
            return Ok(());
        }
    };
    validate_session(session)?;
    let lease_ms = session.lease_ttl.as_millis() as i64;
    let owner_token = session.owner_token.clone();
    let ek = election_key.to_string();
    let client2 = client.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(3)) => {
                    if let Ok(mut c) = client2.get_multiplexed_async_connection().await {
                        let result: Result<i64, _> = redis::cmd("EVAL")
                            .arg(RENEW_SCRIPT)
                            .arg(1)
                            .arg(&ek)
                            .arg(&owner_token)
                            .arg(lease_ms)
                            .query_async(&mut c)
                            .await;
                        if result.ok() != Some(1) {
                            let _ = role_tx.send(LeaderRole::Standby);
                            clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                            return;
                        }
                    }
                }
                _ = &mut cancel_rx => {
                    info!("Redis leadership keepalive cancelled");
                    return;
                }
            }
        }
    });
    Ok(())
}

pub(super) async fn renew(
    client: &redis::Client,
    election_key: &str,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    validate_session(session)?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
    let renewed: i64 = redis::cmd("EVAL")
        .arg(RENEW_SCRIPT)
        .arg(1)
        .arg(election_key)
        .arg(&session.owner_token)
        .arg(session.lease_ttl.as_millis() as i64)
        .query_async(&mut conn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis renew: {e}")))?;
    if renewed == 1 {
        Ok(())
    } else {
        Err(HaError::UnavailableInCurrentStatus)
    }
}

pub(super) async fn release(
    client: &redis::Client,
    election_key: &str,
    session: &LeadershipSession,
) -> Result<(), HaError> {
    validate_session(session)?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
    let released: i64 = redis::cmd("EVAL")
        .arg(RELEASE_SCRIPT)
        .arg(1)
        .arg(election_key)
        .arg(&session.owner_token)
        .query_async(&mut conn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis release: {e}")))?;
    if released == -1 {
        return Err(HaError::InvalidParams(
            "redis leadership owner token mismatch".into(),
        ));
    }
    info!("Redis leadership released");
    Ok(())
}

fn sanitize_hash_tag(cluster_namespace: &str) -> String {
    let trimmed = cluster_namespace.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "mooncake".to_string()
    } else {
        trimmed.replace(['{', '}'], "_")
    }
}
