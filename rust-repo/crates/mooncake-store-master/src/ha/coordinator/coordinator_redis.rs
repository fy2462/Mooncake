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
    let ((leader_address, view_version, owner_token), ttl_ms): (
        (Option<String>, Option<String>, Option<String>),
        i64,
    ) = redis::pipe()
        .atomic()
        .cmd("HMGET")
        .arg(election_key)
        .arg(LEADER_ADDRESS_FIELD)
        .arg(VIEW_VERSION_FIELD)
        .arg(OWNER_TOKEN_FIELD)
        .cmd("PTTL")
        .arg(election_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis read master view: {e}")))?;

    decode_redis_view(leader_address, view_version, owner_token, ttl_ms)
}

fn decode_redis_view(
    leader_address: Option<String>,
    view_version: Option<String>,
    owner_token: Option<String>,
    ttl_ms: i64,
) -> Result<Option<MasterView>, HaError> {
    if matches!(ttl_ms, -2 | 0) {
        return Ok(None);
    }
    if ttl_ms == -1 {
        return Err(HaError::InvalidBackend(
            "redis master view is not attached to an expiry".into(),
        ));
    }
    if ttl_ms < -2 {
        return Err(HaError::InvalidBackend(format!(
            "redis master view has invalid PTTL result: {ttl_ms}"
        )));
    }

    match (leader_address, view_version, owner_token) {
        (Some(leader_address), Some(view_version), Some(owner_token))
            if !leader_address.trim().is_empty() && !owner_token.trim().is_empty() =>
        {
            let view_version = view_version
                .parse::<u64>()
                .map_err(|e| HaError::InvalidBackend(format!("redis parse view version: {e}")))?;
            if view_version == 0 {
                return Err(HaError::InvalidBackend(
                    "redis master view has zero version".into(),
                ));
            }
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
    if leader_address.trim().is_empty() {
        return Err(HaError::InvalidParams(
            "redis leader address must not be empty".into(),
        ));
    }
    let lease_ttl_secs = u64::try_from(lease_ttl_secs).map_err(|_| {
        HaError::InvalidParams("redis leadership lease TTL must be positive".into())
    })?;
    if lease_ttl_secs == 0 {
        return Err(HaError::InvalidParams(
            "redis leadership lease TTL must be positive".into(),
        ));
    }
    let ttl_ms = i64::try_from(
        lease_ttl_secs
            .checked_mul(1_000)
            .ok_or_else(|| HaError::InvalidParams("redis lease TTL overflows".into()))?,
    )
    .map_err(|_| HaError::InvalidParams("redis lease TTL exceeds i64 milliseconds".into()))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
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
    if let Some(view_version) = decode_acquire_result(&result)? {
        let view = MasterView {
            leader_address: leader_address.to_string(),
            view_version,
        };
        let session = LeadershipSession {
            view: view.clone(),
            owner_token,
            lease_ttl: Duration::from_secs(lease_ttl_secs),
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
    validate_session(session)?;
    let lease_ms = redis_lease_ms(session.lease_ttl)?;
    let keepalive_interval = redis_keepalive_interval(session.lease_ttl)?;
    let owner_token = session.owner_token.clone();
    if let Err(error) = client.get_multiplexed_async_connection().await {
        let _ = role_tx.send(LeaderRole::Standby);
        clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
        return Err(HaError::InvalidBackend(format!(
            "redis keepalive connect: {error}"
        )));
    }
    let ek = election_key.to_string();
    let client2 = client.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(keepalive_interval) => {
                    let mut connection = match client2.get_multiplexed_async_connection().await {
                        Ok(connection) => connection,
                        Err(error) => {
                            error!("Redis keepalive reconnect failed: {error}");
                            let _ = role_tx.send(LeaderRole::Standby);
                            clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                            return;
                        }
                    };
                    let result: Result<i64, _> = redis::cmd("EVAL")
                        .arg(RENEW_SCRIPT)
                        .arg(1)
                        .arg(&ek)
                        .arg(&owner_token)
                        .arg(lease_ms)
                        .query_async(&mut connection)
                        .await;
                    if result.ok() != Some(1) {
                        let _ = role_tx.send(LeaderRole::Standby);
                        clear_active_owner_token_if_matches(&active_owner_token, &owner_token);
                        return;
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
    let lease_ms = redis_lease_ms(session.lease_ttl)?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
    let renewed: i64 = redis::cmd("EVAL")
        .arg(RENEW_SCRIPT)
        .arg(1)
        .arg(election_key)
        .arg(&session.owner_token)
        .arg(lease_ms)
        .query_async(&mut conn)
        .await
        .map_err(|e| HaError::InvalidBackend(format!("redis renew: {e}")))?;
    match renewed {
        1 => Ok(()),
        0 => Err(HaError::UnavailableInCurrentStatus),
        value => Err(HaError::InvalidBackend(format!(
            "redis renew script returned invalid status: {value}"
        ))),
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
    match released {
        1 | 0 => {}
        -1 => {
            return Err(HaError::InvalidParams(
                "redis leadership owner token mismatch".into(),
            ));
        }
        value => {
            return Err(HaError::InvalidBackend(format!(
                "redis release script returned invalid status: {value}"
            )));
        }
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

fn decode_acquire_result(result: &[i64]) -> Result<Option<u64>, HaError> {
    match result {
        [0] => Ok(None),
        [1, view_version] if *view_version > 0 => Ok(Some(*view_version as u64)),
        _ => Err(HaError::InvalidBackend(format!(
            "redis acquire script returned malformed result: {result:?}"
        ))),
    }
}

fn redis_lease_ms(lease_ttl: Duration) -> Result<i64, HaError> {
    i64::try_from(lease_ttl.as_millis())
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| {
            HaError::InvalidParams(
                "redis lease TTL must be positive and fit i64 milliseconds".into(),
            )
        })
}

fn redis_keepalive_interval(lease_ttl: Duration) -> Result<Duration, HaError> {
    let lease_ms = u64::try_from(lease_ttl.as_millis())
        .map_err(|_| HaError::InvalidParams("redis lease TTL exceeds u64 milliseconds".into()))?;
    if lease_ms == 0 {
        return Err(HaError::InvalidParams(
            "redis leadership lease TTL must be positive".into(),
        ));
    }
    Ok(Duration::from_millis((lease_ms / 3).clamp(100, 3_000)))
}

#[cfg(test)]
mod tests {
    use super::{
        decode_acquire_result, decode_redis_view, redis_keepalive_interval, redis_lease_ms,
    };
    use std::time::Duration;

    #[test]
    fn acquire_result_decoder_requires_exact_positive_term() {
        assert_eq!(decode_acquire_result(&[0]).unwrap(), None);
        assert_eq!(decode_acquire_result(&[1, 7]).unwrap(), Some(7));
        for malformed in [&[][..], &[1][..], &[1, 0][..], &[1, -1][..], &[2][..]] {
            assert!(decode_acquire_result(malformed).is_err());
        }
    }

    #[test]
    fn redis_lease_conversion_and_keepalive_interval_are_bounded() {
        assert_eq!(redis_lease_ms(Duration::from_secs(5)).unwrap(), 5_000);
        assert_eq!(
            redis_keepalive_interval(Duration::from_secs(1)).unwrap(),
            Duration::from_millis(333)
        );
        assert_eq!(
            redis_keepalive_interval(Duration::from_secs(30)).unwrap(),
            Duration::from_secs(3)
        );
        assert!(redis_lease_ms(Duration::ZERO).is_err());
        assert!(redis_lease_ms(Duration::from_nanos(1)).is_err());
        assert!(redis_keepalive_interval(Duration::ZERO).is_err());
    }

    #[test]
    fn redis_view_requires_one_atomic_complete_expiring_record() {
        let view = decode_redis_view(
            Some("leader-a".into()),
            Some("7".into()),
            Some("owner-a".into()),
            5_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(view.leader_address, "leader-a");
        assert_eq!(view.view_version, 7);

        assert!(
            decode_redis_view(
                Some("leader-a".into()),
                Some("7".into()),
                Some("owner-a".into()),
                -1,
            )
            .is_err()
        );
        assert!(
            decode_redis_view(Some("leader-a".into()), None, Some("owner-a".into()), 5_000)
                .is_err()
        );
        assert!(
            decode_redis_view(
                Some("leader-a".into()),
                Some("7".into()),
                Some("owner-a".into()),
                -2,
            )
            .unwrap()
            .is_none()
        );
    }
}
