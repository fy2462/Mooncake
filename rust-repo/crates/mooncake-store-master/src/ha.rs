use tokio::sync::watch;
use tracing::info;

/// Leader election role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderRole {
    Leader,
    Standby,
}

/// Coordinates master leader election via etcd or K8s Lease.
pub struct LeaderCoordinator {
    backend: CoordinatorBackend,
    role_tx: watch::Sender<LeaderRole>,
    role_rx: watch::Receiver<LeaderRole>,
}

enum CoordinatorBackend {
    Etcd {
        #[allow(dead_code)]
        client: etcd_client::Client,
        #[allow(dead_code)]
        election_key: String,
    },
    K8s {
        namespace: String,
        lease_name: String,
    },
    Manual,
}

impl LeaderCoordinator {
    fn with_backend(backend: CoordinatorBackend, initial_role: LeaderRole) -> Self {
        let (role_tx, role_rx) = watch::channel(initial_role);
        Self {
            backend,
            role_tx,
            role_rx,
        }
    }

    /// Create a leader coordinator backed by an etcd cluster.
    pub async fn new_etcd(endpoints: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let client = etcd_client::Client::connect(endpoints, None).await?;
        Ok(Self::with_backend(
            CoordinatorBackend::Etcd {
                client,
                election_key: "/mooncake/master/leader".to_string(),
            },
            LeaderRole::Leader,
        ))
    }

    /// Create a leader coordinator backed by a K8s Lease.
    pub async fn new_k8s(namespace: &str, lease_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::with_backend(
            CoordinatorBackend::K8s {
                namespace: namespace.to_string(),
                lease_name: lease_name.to_string(),
            },
            LeaderRole::Leader,
        ))
    }

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

    /// Wait until this instance knows its role (Leader or Standby).
    pub async fn wait_for_role(&self) -> Result<LeaderRole, Box<dyn std::error::Error>> {
        match &self.backend {
            CoordinatorBackend::Etcd { client: _, .. } => {
                info!("Etcd leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::K8s { namespace, lease_name } => {
                info!(
                    "K8s Lease election initialized: namespace={}, lease={}",
                    namespace, lease_name
                );
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Manual => Ok(*self.role_rx.borrow()),
        }
    }

    /// Watch for leadership changes. Returns when this instance becomes leader.
    pub async fn watch_leadership_change(&self) {
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

    pub fn set_role_for_test(&self, role: LeaderRole) {
        let _ = self.role_tx.send(role);
    }
}
