use super::types::{
    AcquireLeadershipResult, HaError, LeaderRole, LeadershipHandle, LeadershipSession, MasterView,
};
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

const REDIS_LEADER_ADDRESS_FIELD: &str = "leader_address";
const REDIS_VIEW_VERSION_FIELD: &str = "view_version";
const REDIS_OWNER_TOKEN_FIELD: &str = "owner_token";
const REDIS_ACQUIRE_SCRIPT: &str = r#"
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
const REDIS_RENEW_SCRIPT: &str = r#"
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
const REDIS_RELEASE_SCRIPT: &str = r#"
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

// LeaderCoordinator — leader election and keepalive via Etcd/Redis/Manual.
// LeaderCoordinator —— 核心选举基础设施（C++: ha_service.h）。

pub struct LeaderCoordinator {
    /// The backend performing the actual election. / 执行实际选举的后端。
    backend: CoordinatorBackend,
    /// Sender side of the role change watch channel. / 角色变更 watch channel 的发送端。
    role_tx: watch::Sender<LeaderRole>,
    /// Receiver side of the role change watch channel. / 角色变更 watch channel 的接收端。
    role_rx: watch::Receiver<LeaderRole>,
    /// Owner token currently tied to the keepalive session.
    active_owner_token: Arc<Mutex<Option<String>>>,
}

/// Supported coordinator backends. / 支持的协调器后端。
enum CoordinatorBackend {
    Etcd {
        client: etcd_client::Client,
        /// Key path used for the etcd election campaign.
        /// 用于 etcd election campaign 的 key 路径。
        election_key: String,
    },
    Redis {
        client: redis::Client,
        /// Key used for the Redis leader hash.
        election_key: String,
        /// Monotonic view-version counter key.
        view_version_key: String,
    },
    /// Manual mode: leadership is externally controlled via the watch channel.
    /// 手动模式：leadership 通过 watch channel 外部控制。
    Manual,
}

impl LeaderCoordinator {
    /// Shared constructor for all backends. / 所有后端的共享构造函数。
    fn with_backend(backend: CoordinatorBackend, initial_role: LeaderRole) -> Self {
        let (role_tx, role_rx) = watch::channel(initial_role);
        Self {
            backend,
            role_tx,
            role_rx,
            active_owner_token: Arc::new(Mutex::new(None)),
        }
    }

    /// Create an Etcd-backed coordinator. / 创建 Etcd 支持的协调器。
    pub async fn new_etcd(
        endpoints: Vec<String>,
        cluster_namespace: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let client = etcd_client::Client::connect(endpoints, None).await?;
        Ok(Self::with_backend(
            CoordinatorBackend::Etcd {
                client,
                election_key: build_master_view_key(cluster_namespace),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a Redis-backed coordinator. / 创建 Redis 支持的协调器。
    pub async fn new_redis(
        connstring: &str,
        cluster_namespace: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let client = redis::Client::open(connstring)?;
        let namespace = resolve_cluster_namespace(cluster_namespace);
        Ok(Self::with_backend(
            CoordinatorBackend::Redis {
                client,
                election_key: build_redis_master_view_key(&namespace),
                view_version_key: build_redis_view_version_key(&namespace),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a Manual-mode coordinator with external role control.
    /// Returns both the coordinator and a Sender for external role manipulation.
    ///
    /// 创建手动模式的协调器，支持外部角色控制。
    /// 返回协调器和用于外部角色操作的 Sender。
    pub fn new_manual(initial_role: LeaderRole) -> (Self, watch::Sender<LeaderRole>) {
        let (role_tx, role_rx) = watch::channel(initial_role);
        (
            Self {
                backend: CoordinatorBackend::Manual,
                role_tx: role_tx.clone(),
                role_rx,
                active_owner_token: Arc::new(Mutex::new(None)),
            },
            role_tx,
        )
    }

    /// Read current leader view.
    /// 从后端读取当前 Leader 视图。
    pub async fn read_current_view(&self) -> Result<Option<MasterView>, HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd {
                client,
                election_key,
            } => {
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
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => {
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
                    .arg(REDIS_LEADER_ADDRESS_FIELD)
                    .arg(REDIS_VIEW_VERSION_FIELD)
                    .arg(REDIS_OWNER_TOKEN_FIELD)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| {
                        HaError::InvalidBackend(format!("redis hmget master view: {e}"))
                    })?;

                match (leader_address, view_version, owner_token) {
                    (None, None, None) => Ok(None),
                    (Some(leader_address), Some(view_version), Some(owner_token))
                        if !leader_address.is_empty() && !owner_token.is_empty() =>
                    {
                        let view_version = view_version.parse::<u64>().map_err(|e| {
                            HaError::InvalidBackend(format!("redis parse view version: {e}"))
                        })?;
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
            CoordinatorBackend::Manual => Ok(None),
        }
    }

    /// Try to acquire leadership via the backend.
    ///
    /// 尝试通过后端获取 Leader 权。
    /// - Etcd: grant lease + campaign. / 授予租约 + campaign。
    /// - Redis: SET NX PX (atomic compare-and-set with TTL).
    ///   SET NX PX（带 TTL 的原子比较并设置）。
    /// - Manual: always succeeds. / 始终成功。
    pub async fn try_acquire_leadership(
        &self,
        leader_address: &str,
        lease_ttl_secs: i64,
    ) -> Result<AcquireLeadershipResult, HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd {
                client,
                election_key,
            } => {
                let mut client = client.clone();
                let current = self.read_current_view().await?;

                // Step 1: Grant a TTL lease. / 第 1 步：授予 TTL 租约。
                let lease_resp = client
                    .lease_grant(lease_ttl_secs, None)
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("etcd lease grant error: {e}")))?;
                let lease_id = lease_resp.id();

                // Step 2: create master view key with the lease, matching C++.
                let put = TxnOp::put(
                    election_key.clone().into_bytes(),
                    leader_address.as_bytes().to_vec(),
                    Some(PutOptions::new().with_lease(lease_id)),
                );
                let txn = Txn::new()
                    .when([Compare::version(
                        election_key.clone().into_bytes(),
                        CompareOp::Equal,
                        0,
                    )])
                    .and_then([put]);

                let resp = client.txn(txn).await.map_err(|e| {
                    HaError::InvalidBackend(format!("etcd create master view: {e}"))
                })?;
                if resp.succeeded() {
                    let acquired_view = self.read_current_view().await?.unwrap_or(MasterView {
                        leader_address: leader_address.to_string(),
                        view_version: 1,
                    });
                    let session = LeadershipSession {
                        view: acquired_view.clone(),
                        owner_token: lease_id.to_string(),
                        lease_ttl: Duration::from_secs(lease_ttl_secs as u64),
                    };
                    let _ = self.role_tx.send(LeaderRole::Leader);
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
                        view: current.or(self.read_current_view().await?),
                        session: None,
                    })
                }
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key,
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let ttl_ms = (lease_ttl_secs * 1000) as usize;
                let owner_token = Uuid::new_v4().to_string();
                let result: Vec<i64> = redis::cmd("EVAL")
                    .arg(REDIS_ACQUIRE_SCRIPT)
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
                    let _ = self.role_tx.send(LeaderRole::Leader);
                    Ok(AcquireLeadershipResult {
                        acquired: true,
                        view: Some(view),
                        session: Some(session),
                    })
                } else {
                    Ok(AcquireLeadershipResult {
                        acquired: false,
                        view: self.read_current_view().await?,
                        session: None,
                    })
                }
            }
            CoordinatorBackend::Manual => {
                // Manual mode: instant leadership. / 手动模式：立即成为 leader。
                let _ = self.role_tx.send(LeaderRole::Leader);
                let view = MasterView {
                    leader_address: leader_address.to_string(),
                    view_version: 1,
                };
                Ok(AcquireLeadershipResult {
                    acquired: true,
                    view: Some(view.clone()),
                    session: Some(LeadershipSession {
                        view,
                        owner_token: "manual".into(),
                        lease_ttl: Duration::ZERO,
                    }),
                })
            }
        }
    }

    /// Start a background keepalive task. On failure, the leader is demoted
    /// to Standby via the watch channel.
    ///
    /// 启动 Leader 续约后台任务。
    /// - Etcd: lease_keep_alive stream with 3s interval.
    ///   lease_keep_alive 流，3s 间隔。
    /// - Redis: PEXPIRE with 3s interval. / PEXPIRE，3s 间隔。
    /// - 续约失败时自动降级为 Standby，通过 role_tx channel 通知。
    pub async fn start_leadership_keepalive(
        &self,
        session: &LeadershipSession,
    ) -> Result<LeadershipHandle, HaError> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let mut client = client.clone();
                let role_tx = self.role_tx.clone();
                let lease_id = parse_etcd_lease_id(session)?;
                self.set_active_owner_token(Some(session.owner_token.clone()));
                let active_owner_token = self.active_owner_token.clone();
                let owner_token = session.owner_token.clone();
                tokio::spawn(async move {
                    // Create the keepalive stream from the lease.
                    // 从租约创建 keepalive 流。
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
                            // Renew every 3 seconds. / 每 3 秒续约一次。
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
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => {
                let _conn = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Redis keepalive connection failed: {}", e);
                        let _ = self.role_tx.send(LeaderRole::Standby);
                        return Ok(LeadershipHandle::new(cancel_tx));
                    }
                };
                validate_session(session)?;
                let lease_ms = session.lease_ttl.as_millis() as i64;
                let owner_token = session.owner_token.clone();
                self.set_active_owner_token(Some(owner_token.clone()));
                let role_tx = self.role_tx.clone();
                let ek = election_key.clone();
                let client2 = client.clone();
                let active_owner_token = self.active_owner_token.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            // Renew every 3 seconds via PEXPIRE. / 每 3 秒通过 PEXPIRE 续约。
                            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                                if let Ok(mut c) = client2.get_multiplexed_async_connection().await {
                                    let result: Result<i64, _> = redis::cmd("EVAL")
                                        .arg(REDIS_RENEW_SCRIPT)
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
            }
            CoordinatorBackend::Manual => {
                self.set_active_owner_token(Some(session.owner_token.clone()));
            }
        }
        Ok(LeadershipHandle::new(cancel_tx))
    }

    /// Attempt a single lease renewal. Returns Ok(()) on success.
    /// Used by the warmup loop and serve preflight check.
    /// C++ equivalent: `LeaderCoordinator::RenewLeadership(session)`.
    ///
    /// 尝试单次租约续期。成功返回 Ok(())。
    /// 用于预热循环和 serve 前飞行检查。
    pub async fn try_renew_leadership(&self, session: &LeadershipSession) -> Result<(), HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let lease_id = parse_etcd_lease_id(session)?;
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
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => {
                validate_session(session)?;
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let renewed: i64 = redis::cmd("EVAL")
                    .arg(REDIS_RENEW_SCRIPT)
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
            CoordinatorBackend::Manual => Ok(()),
        }
    }

    /// Release leadership gracefully. / 优雅释放 Leader 权。
    /// - Etcd: revokes the session lease.
    /// - Redis: deletes the leader hash only if owner_token matches.
    /// - Manual: sends Standby via watch channel. / 通过 watch channel 发送 Standby。
    pub async fn release_leadership(&self, session: &LeadershipSession) -> Result<(), HaError> {
        self.ensure_release_session_matches(session)?;
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let lease_id = parse_etcd_lease_id(session)?;
                let mut client = client.clone();
                client.lease_revoke(lease_id).await.map_err(|e| {
                    HaError::InvalidBackend(format!("etcd lease revoke error: {e}"))
                })?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                self.set_active_owner_token(None);
                info!("Leadership released via lease revoke");
                Ok(())
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => {
                validate_session(session)?;
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let released: i64 = redis::cmd("EVAL")
                    .arg(REDIS_RELEASE_SCRIPT)
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
                let _ = self.role_tx.send(LeaderRole::Standby);
                self.set_active_owner_token(None);
                info!("Redis leadership released");
                Ok(())
            }
            CoordinatorBackend::Manual => {
                let _ = self.role_tx.send(LeaderRole::Standby);
                self.set_active_owner_token(None);
                Ok(())
            }
        }
    }

    /// Wait for a view change: poll every 200ms, return when `view_version`
    /// differs from `known_version`, or when the timeout expires.
    ///
    /// 等待 Leader 视图变更（直到超时），每 200ms 轮询一次，
    /// 检测到 view_version 变化即返回，超时返回 None。
    pub async fn wait_for_view_change(
        &self,
        known_version: u64,
        timeout: Duration,
    ) -> Result<Option<MasterView>, HaError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.read_current_view().await? {
                Some(view) if view.view_version != known_version => return Ok(Some(view)),
                Some(_) => {}
                None if known_version != 0 => return Ok(None),
                None => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            // 200ms poll interval / 200ms 轮询间隔
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Subscribe to leadership role changes produced by keepalive/election.
    /// 订阅由续约/选举产生的角色变化。
    pub fn subscribe_role(&self) -> watch::Receiver<LeaderRole> {
        self.role_rx.clone()
    }

    /// Subscribe to role changes for a specific active leadership session.
    pub fn subscribe_role_for_session(
        &self,
        session: &LeadershipSession,
    ) -> Result<watch::Receiver<LeaderRole>, HaError> {
        self.ensure_active_session(session)?;
        if *self.role_rx.borrow() != LeaderRole::Leader {
            return Err(HaError::UnavailableInCurrentStatus);
        }
        Ok(self.role_rx.clone())
    }

    /// Wait for a role assignment from the backend. Returns the current role.
    /// 等待后端分配角色。返回当前角色。
    pub async fn wait_for_role(&self) -> Result<LeaderRole, Box<dyn std::error::Error>> {
        match &self.backend {
            CoordinatorBackend::Etcd { .. } => {
                info!("Etcd leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Redis { .. } => {
                info!("Redis leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Manual => Ok(*self.role_rx.borrow()),
        }
    }

    /// Block until leadership changes, returning only when this node becomes
    /// the Leader. Uses the watch channel rather than polling.
    ///
    /// 阻塞等待 Leader 角色变更（通过 watch channel），仅在成为 Leader 时返回。
    /// 使用 watch channel 而非轮询。
    pub async fn watch_leadership_change(&self) {
        // If already leader, return immediately. / 如果已是 leader，立即返回。
        if *self.role_rx.borrow() == LeaderRole::Leader {
            return;
        }
        let mut role_rx = self.role_rx.clone();
        loop {
            if role_rx.changed().await.is_err() {
                return;
            }
            if *role_rx.borrow() == LeaderRole::Leader {
                info!("Leadership changed: this instance became leader");
                return;
            }
        }
    }

    /// Test-only: inject a role change via the watch channel.
    /// 仅限测试：通过 watch channel 注入角色变更。
    pub fn set_role_for_test(&self, role: LeaderRole) {
        let _ = self.role_tx.send(role);
    }

    fn set_active_owner_token(&self, token: Option<String>) {
        *self
            .active_owner_token
            .lock()
            .expect("active owner token mutex poisoned") = token;
    }

    fn ensure_active_session(&self, session: &LeadershipSession) -> Result<(), HaError> {
        match self
            .active_owner_token
            .lock()
            .expect("active owner token mutex poisoned")
            .as_ref()
        {
            Some(token) if token == &session.owner_token => Ok(()),
            _ => Err(HaError::UnavailableInCurrentStatus),
        }
    }

    fn ensure_release_session_matches(&self, session: &LeadershipSession) -> Result<(), HaError> {
        match self
            .active_owner_token
            .lock()
            .expect("active owner token mutex poisoned")
            .as_ref()
        {
            Some(token) if token != &session.owner_token => Err(HaError::InvalidParams(
                "leadership session owner token mismatch".into(),
            )),
            _ => Ok(()),
        }
    }
}

fn build_master_view_key(cluster_namespace: &str) -> String {
    let namespace = resolve_cluster_namespace(cluster_namespace);
    format!(
        "mooncake-store/{}/master_view",
        namespace.trim_end_matches('/')
    )
}

fn resolve_cluster_namespace(cluster_namespace: &str) -> String {
    if cluster_namespace.trim().is_empty() {
        std::env::var("MC_STORE_CLUSTER_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "mooncake".to_string())
    } else {
        cluster_namespace.to_string()
    }
}

fn build_redis_master_view_key(cluster_namespace: &str) -> String {
    let hash_tag = sanitize_redis_hash_tag(cluster_namespace);
    format!("mooncake-store/{{{hash_tag}}}/master_view")
}

fn build_redis_view_version_key(cluster_namespace: &str) -> String {
    let hash_tag = sanitize_redis_hash_tag(cluster_namespace);
    format!("mooncake-store/{{{hash_tag}}}/master_view_version")
}

fn sanitize_redis_hash_tag(cluster_namespace: &str) -> String {
    let trimmed = cluster_namespace.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "mooncake".to_string()
    } else {
        trimmed.replace(['{', '}'], "_")
    }
}

fn parse_etcd_lease_id(session: &LeadershipSession) -> Result<i64, HaError> {
    session
        .owner_token
        .parse::<i64>()
        .map_err(|e| HaError::InvalidParams(format!("invalid etcd owner token: {e}")))
}

fn validate_session(session: &LeadershipSession) -> Result<(), HaError> {
    if session.owner_token.trim().is_empty() || session.lease_ttl == Duration::ZERO {
        return Err(HaError::InvalidParams(
            "leadership session owner token and lease ttl must be set".into(),
        ));
    }
    Ok(())
}

fn clear_active_owner_token_if_matches(
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
