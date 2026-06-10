use super::types::{
    AcquireLeadershipResult, HaError, LeaderRole, LeadershipHandle, LeadershipSession, MasterView,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tracing::info;

mod coordinator_common;
mod coordinator_etcd;
mod coordinator_k8s;
mod coordinator_redis;
#[doc(hidden)]
pub mod test_support;

use coordinator_common::{resolve_cluster_namespace, validate_session};
use coordinator_k8s::{
    acquire_k8s_lease, parse_k8s_lease_connstring, read_k8s_view, release_k8s_lease,
    renew_k8s_lease, start_k8s_keepalive, wait_for_k8s_view_change,
};

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
    K8s {
        namespace: String,
        lease_name: String,
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
                election_key: coordinator_etcd::build_master_view_key(cluster_namespace),
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
                election_key: coordinator_redis::build_master_view_key(&namespace),
                view_version_key: coordinator_redis::build_view_version_key(&namespace),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a Kubernetes Lease-backed coordinator. The Kubernetes client is
    /// loaded lazily by each operation so construction remains testable without
    /// a kubeconfig or in-cluster environment.
    pub fn new_k8s(connstring: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let (namespace, lease_name) = parse_k8s_lease_connstring(connstring)?;
        Ok(Self::with_backend(
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
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
            } => coordinator_etcd::read_view(client, election_key).await,
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => coordinator_redis::read_view(client, election_key).await,
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => read_k8s_view(namespace, lease_name).await,
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
                let current = self.read_current_view().await?;
                let acquired = coordinator_etcd::acquire(
                    client,
                    election_key,
                    leader_address,
                    lease_ttl_secs,
                    current,
                )
                .await?;
                if acquired.acquired {
                    let _ = self.role_tx.send(LeaderRole::Leader);
                }
                Ok(acquired)
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key,
            } => {
                let acquired = coordinator_redis::acquire(
                    client,
                    election_key,
                    view_version_key,
                    leader_address,
                    lease_ttl_secs,
                )
                .await?;
                if acquired.acquired {
                    let _ = self.role_tx.send(LeaderRole::Leader);
                }
                Ok(acquired)
            }
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => {
                let acquired =
                    acquire_k8s_lease(namespace, lease_name, leader_address, lease_ttl_secs)
                        .await?;
                if let Some(session) = acquired.session.as_ref() {
                    self.set_active_owner_token(Some(session.owner_token.clone()));
                    let _ = self.role_tx.send(LeaderRole::Leader);
                }
                Ok(acquired)
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
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                self.set_active_owner_token(Some(session.owner_token.clone()));
                coordinator_etcd::start_keepalive(
                    client,
                    session,
                    self.role_tx.clone(),
                    self.active_owner_token.clone(),
                    cancel_rx,
                )?;
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => {
                validate_session(session)?;
                let owner_token = session.owner_token.clone();
                self.set_active_owner_token(Some(owner_token.clone()));
                coordinator_redis::start_keepalive(
                    client,
                    election_key,
                    session,
                    self.role_tx.clone(),
                    self.active_owner_token.clone(),
                    cancel_rx,
                )
                .await?;
            }
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => {
                let owner_token = session.owner_token.clone();
                self.set_active_owner_token(Some(owner_token.clone()));
                start_k8s_keepalive(
                    namespace,
                    lease_name,
                    session,
                    self.role_tx.clone(),
                    self.active_owner_token.clone(),
                    cancel_rx,
                )?;
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
                coordinator_etcd::renew(client, session).await
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => coordinator_redis::renew(client, election_key, session).await,
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => renew_k8s_lease(namespace, lease_name, session).await,
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
                coordinator_etcd::release(client, session).await?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                self.set_active_owner_token(None);
                Ok(())
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
                view_version_key: _,
            } => {
                coordinator_redis::release(client, election_key, session).await?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                self.set_active_owner_token(None);
                Ok(())
            }
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => {
                validate_session(session)?;
                release_k8s_lease(namespace, lease_name, session).await?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                self.set_active_owner_token(None);
                info!("K8s leadership released");
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
        if let CoordinatorBackend::K8s {
            namespace,
            lease_name,
        } = &self.backend
        {
            return wait_for_k8s_view_change(namespace, lease_name, known_version, timeout).await;
        }

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
            CoordinatorBackend::K8s { .. } => {
                info!("K8s leader election initialized");
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
