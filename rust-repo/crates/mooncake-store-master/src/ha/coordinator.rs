use super::types::{AcquireLeadershipResult, HaError, LeaderRole, LeadershipHandle, MasterView};
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

// LeaderCoordinator — leader election and keepalive via Etcd/Redis/K8s/Manual.
// LeaderCoordinator —— 核心选举基础设施（C++: ha_service.h）。

pub struct LeaderCoordinator {
    /// The backend performing the actual election. / 执行实际选举的后端。
    backend: CoordinatorBackend,
    /// Sender side of the role change watch channel. / 角色变更 watch channel 的发送端。
    role_tx: watch::Sender<LeaderRole>,
    /// Receiver side of the role change watch channel. / 角色变更 watch channel 的接收端。
    role_rx: watch::Receiver<LeaderRole>,
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
        /// Key used for SET NX PX. / 用于 SET NX PX 的 key。
        election_key: String,
    },
    K8s {
        /// Kubernetes namespace. / Kubernetes 命名空间。
        namespace: String,
        /// Kubernetes Lease resource name. / Kubernetes Lease 资源名称。
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
    pub async fn new_redis(connstring: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = redis::Client::open(connstring)?;
        Ok(Self::with_backend(
            CoordinatorBackend::Redis {
                client,
                election_key: "mooncake:master:leader".to_string(),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a K8s Lease-backed coordinator. / 创建 K8s Lease 支持的协调器。
    pub async fn new_k8s(
        namespace: &str,
        lease_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::with_backend(
            CoordinatorBackend::K8s {
                namespace: namespace.to_string(),
                lease_name: lease_name.to_string(),
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
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let result: Option<String> = redis::cmd("GET")
                    .arg(election_key)
                    .query_async(&mut conn)
                    .await
                    .ok();
                if let Some(ref v) = result {
                    // Value format: "address|version" / 值格式："address|version"
                    let parts: Vec<&str> = v.splitn(2, '|').collect();
                    return Ok(Some(MasterView {
                        leader_address: parts.first().copied().unwrap_or("").to_string(),
                        view_version: parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Try to acquire leadership via the backend.
    ///
    /// 尝试通过后端获取 Leader 权。
    /// - Etcd: grant lease + campaign. / 授予租约 + campaign。
    /// - Redis: SET NX PX (atomic compare-and-set with TTL).
    ///   SET NX PX（带 TTL 的原子比较并设置）。
    /// - Manual: always succeeds. / 始终成功。
    /// - K8s: not yet implemented. / 尚未实现。
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
                    let _ = self.role_tx.send(LeaderRole::Leader);
                    info!(
                        "Leadership acquired: address={}, lease_id={}, view_version={}",
                        leader_address, lease_id, acquired_view.view_version
                    );
                    Ok(AcquireLeadershipResult {
                        acquired: true,
                        view: Some(acquired_view),
                        lease_id: Some(lease_id),
                    })
                } else {
                    let _ = client.lease_revoke(lease_id).await;
                    Ok(AcquireLeadershipResult {
                        acquired: false,
                        view: current.or(self.read_current_view().await?),
                        lease_id: None,
                    })
                }
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let ttl_ms = (lease_ttl_secs * 1000) as usize;
                // Redis value format: "leader_address|view_version"
                // Redis 值格式："leader_address|view_version"
                let value = format!("{}|{}", leader_address, "1");
                let result: Option<String> = redis::cmd("SET")
                    .arg(election_key)
                    .arg(&value)
                    .arg("NX") // only if not exists / 仅当不存在时
                    .arg("PX") // TTL in milliseconds / TTL 以毫秒计
                    .arg(ttl_ms)
                    .query_async(&mut conn)
                    .await
                    .ok();
                if result.as_deref() == Some("OK") {
                    let _ = self.role_tx.send(LeaderRole::Leader);
                    Ok(AcquireLeadershipResult {
                        acquired: true,
                        view: Some(MasterView {
                            leader_address: leader_address.to_string(),
                            view_version: 1,
                        }),
                        lease_id: Some(lease_ttl_secs), // TTL seconds stored as lease_id
                    })
                } else {
                    Ok(AcquireLeadershipResult {
                        acquired: false,
                        view: self.read_current_view().await?,
                        lease_id: None,
                    })
                }
            }
            CoordinatorBackend::Manual => {
                // Manual mode: instant leadership. / 手动模式：立即成为 leader。
                let _ = self.role_tx.send(LeaderRole::Leader);
                Ok(AcquireLeadershipResult {
                    acquired: true,
                    view: Some(MasterView {
                        leader_address: leader_address.to_string(),
                        view_version: 1,
                    }),
                    lease_id: None,
                })
            }
            _ => Err(HaError::InvalidBackend(
                "backend does not support leadership acquisition".into(),
            )),
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
        lease_id: i64,
    ) -> Result<LeadershipHandle, HaError> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let mut client = client.clone();
                let role_tx = self.role_tx.clone();
                tokio::spawn(async move {
                    // Create the keepalive stream from the lease.
                    // 从租约创建 keepalive 流。
                    let (mut keeper, _stream) = match client.lease_keep_alive(lease_id).await {
                        Ok(res) => res,
                        Err(e) => {
                            error!("Failed to create lease keepalive: {}", e);
                            let _ = role_tx.send(LeaderRole::Standby);
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
            } => {
                let _conn = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Redis keepalive connection failed: {}", e);
                        let _ = self.role_tx.send(LeaderRole::Standby);
                        return Ok(LeadershipHandle::new(cancel_tx));
                    }
                };
                let lease_ms = lease_id * 1000;
                let role_tx = self.role_tx.clone();
                let ek = election_key.clone();
                let client2 = client.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            // Renew every 3 seconds via PEXPIRE. / 每 3 秒通过 PEXPIRE 续约。
                            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                                if let Ok(mut c) = client2.get_multiplexed_async_connection().await {
                                    let result: Result<(), _> = redis::cmd("PEXPIRE")
                                        .arg(&ek)
                                        .arg(lease_ms)
                                        .query_async(&mut c)
                                        .await;
                                    if result.is_err() {
                                        let _ = role_tx.send(LeaderRole::Standby);
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
            // No keepalive needed for K8s (Lease handles it) or Manual.
            // K8s（Lease 自行处理）或 Manual 无需续约。
            _ => {}
        }
        Ok(LeadershipHandle::new(cancel_tx))
    }

    /// Attempt a single lease renewal. Returns Ok(()) on success.
    /// Used by the warmup loop and serve preflight check.
    /// C++ equivalent: `LeaderCoordinator::RenewLeadership(session)`.
    ///
    /// 尝试单次租约续期。成功返回 Ok(())。
    /// 用于预热循环和 serve 前飞行检查。
    pub async fn try_renew_leadership(&self, lease_id: i64) -> Result<(), HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
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
            CoordinatorBackend::Redis { client: _, .. } => {
                // Redis no-op: SET NX PX expires automatically.
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Release leadership gracefully. / 优雅释放 Leader 权。
    /// - Etcd: calls resign. / 调用 resign。
    /// - Redis: DEL the election key. / DEL 选举 key。
    /// - Manual: sends Standby via watch channel. / 通过 watch channel 发送 Standby。
    pub async fn release_leadership(&self, lease_id: i64) -> Result<(), HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let mut client = client.clone();
                client.lease_revoke(lease_id).await.map_err(|e| {
                    HaError::InvalidBackend(format!("etcd lease revoke error: {e}"))
                })?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                info!("Leadership released via lease revoke");
                Ok(())
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(election_key)
                    .query_async(&mut conn)
                    .await;
                let _ = self.role_tx.send(LeaderRole::Standby);
                info!("Redis leadership released");
                Ok(())
            }
            CoordinatorBackend::Manual => {
                let _ = self.role_tx.send(LeaderRole::Standby);
                Ok(())
            }
            _ => Err(HaError::InvalidBackend(
                "backend does not support leadership release".into(),
            )),
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
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            if let Some(view) = self.read_current_view().await? {
                if view.view_version != known_version {
                    return Ok(Some(view));
                }
            }
            // 200ms poll interval / 200ms 轮询间隔
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait for a role assignment from the backend. Returns the current role.
    /// 等待后端分配角色。返回当前角色。
    pub async fn wait_for_role(&self) -> Result<LeaderRole, Box<dyn std::error::Error>> {
        match &self.backend {
            CoordinatorBackend::Etcd { .. } => {
                info!("Etcd leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::K8s { namespace, lease_name } => Err(format!(
                "K8s Lease election is not implemented in Rust coordinator: namespace={namespace}, lease={lease_name}"
            )
            .into()),
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
}

fn build_master_view_key(cluster_namespace: &str) -> String {
    let namespace = if cluster_namespace.trim().is_empty() {
        std::env::var("MC_STORE_CLUSTER_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "mooncake".to_string())
    } else {
        cluster_namespace.to_string()
    };
    format!(
        "mooncake-store/{}/master_view",
        namespace.trim_end_matches('/')
    )
}
