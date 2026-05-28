use crate::service::ObjectEntry;
use crate::service::NoFSegmentEntry;
use crate::service::TaskEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use mooncake_store_core::Segment;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{error, info, warn};

pub type RuntimeStateCallback = Arc<dyn Fn(MasterRuntimeState) + Send + Sync>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HaError {
    #[error("invalid ha backend: {0}")]
    InvalidBackend(String),
    #[error("snapshot error: {0}")]
    Snapshot(String),
    #[error("unavailable in current status")]
    UnavailableInCurrentStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderRole {
    Leader,
    Standby,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HABackendType {
    Unknown,
    Etcd,
    Redis,
    K8s,
}

impl HABackendType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Etcd => "etcd",
            Self::Redis => "redis",
            Self::K8s => "k8s",
        }
    }
}

pub fn parse_ha_backend_type(value: &str) -> Option<HABackendType> {
    match value {
        "etcd" => Some(HABackendType::Etcd),
        "redis" => Some(HABackendType::Redis),
        "k8s" => Some(HABackendType::K8s),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HABackendSpec {
    pub backend_type: HABackendType,
    pub connstring: String,
    pub cluster_namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterView {
    pub leader_address: String,
    pub view_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterRuntimeState {
    Starting,
    Standby,
    Candidate,
    Recovering,
    CatchingUp,
    LeaderWarmup,
    Serving,
}

impl MasterRuntimeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Standby => "standby",
            Self::Candidate => "candidate",
            Self::Recovering => "recovering",
            Self::CatchingUp => "catching_up",
            Self::LeaderWarmup => "leader_warmup",
            Self::Serving => "serving",
        }
    }

    pub fn role(self) -> &'static str {
        match self {
            Self::LeaderWarmup | Self::Serving => "leader",
            Self::Starting
            | Self::Standby
            | Self::Candidate
            | Self::Recovering
            | Self::CatchingUp => "standby",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandbyState {
    Stopped,
    Connecting,
    Syncing,
    Recovering,
    Reconnecting,
    Failed,
    Watching,
    Promoting,
    Promoted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandbySyncStatus {
    pub applied_seq_id: u64,
    pub primary_seq_id: u64,
    pub lag_entries: u64,
    pub is_syncing: bool,
    pub is_connected: bool,
    pub state: StandbyState,
}

impl Default for StandbySyncStatus {
    fn default() -> Self {
        Self {
            applied_seq_id: 0,
            primary_seq_id: 0,
            lag_entries: 0,
            is_syncing: false,
            is_connected: false,
            state: StandbyState::Stopped,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    pub snapshot_id: String,
    pub snapshot_sequence_id: u64,
    pub segments: Vec<Segment>,
    pub nof_segments: Vec<NoFSegmentEntry>,
    pub objects: Vec<(String, ObjectEntry)>,
    pub tasks: Vec<TaskEntry>,
}

pub trait SnapshotProvider: Send + Sync {
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError>;
}

pub struct NoopSnapshotProvider;

impl SnapshotProvider for NoopSnapshotProvider {
    fn load_latest_snapshot(&self, _cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        Ok(None)
    }
}

pub struct LocalSnapshotProvider {
    root_dir: PathBuf,
    backend_type: StorageBackendType,
}

impl LocalSnapshotProvider {
    pub fn new(root_dir: PathBuf, backend_type: StorageBackendType) -> Self {
        Self {
            root_dir,
            backend_type,
        }
    }
}

impl SnapshotProvider for LocalSnapshotProvider {
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        let dir = if cluster_id.is_empty() {
            self.root_dir.clone()
        } else {
            self.root_dir.join(cluster_id)
        };
        let backend = StorageBackend::new(self.backend_type, &dir);
        let Some((segments, nof_segments, objects, tasks)) = backend
            .load()
            .map_err(|error| HaError::Snapshot(error.to_string()))?
        else {
            return Ok(None);
        };

        let snapshot_path = dir.join("master_snapshot.json");
        let snapshot_id = std::fs::metadata(&snapshot_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
            .map(|ts| format!("snapshot-{}", ts.as_millis()))
            .unwrap_or_else(|| "snapshot-latest".to_string());

        Ok(Some(LoadedSnapshot {
            snapshot_id,
            snapshot_sequence_id: 0,
            segments: segments.into_iter().map(|s| s.segment).collect(),
            nof_segments,
            objects,
            tasks,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct MasterServiceSupervisorConfig {
    pub local_hostname: String,
    pub cluster_id: String,
    pub enable_snapshot_restore: bool,
    pub snapshot_backup_dir: Option<PathBuf>,
    pub snapshot_backend_type: Option<StorageBackendType>,
}

impl Default for MasterServiceSupervisorConfig {
    fn default() -> Self {
        Self {
            local_hostname: "localhost".to_string(),
            cluster_id: "default".to_string(),
            enable_snapshot_restore: false,
            snapshot_backup_dir: None,
            snapshot_backend_type: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StandbyRuntimeCapabilities {
    pub has_snapshot_bootstrap: bool,
    pub has_oplog_following: bool,
}

pub fn build_standby_runtime_capabilities(
    spec: &HABackendSpec,
    config: &MasterServiceSupervisorConfig,
) -> StandbyRuntimeCapabilities {
    StandbyRuntimeCapabilities {
        has_snapshot_bootstrap: config.enable_snapshot_restore,
        has_oplog_following: spec.backend_type == HABackendType::Etcd,
    }
}

pub fn map_standby_runtime_state(
    status: &StandbySyncStatus,
    observed_leader: Option<&MasterView>,
    capabilities: StandbyRuntimeCapabilities,
) -> MasterRuntimeState {
    match status.state {
        StandbyState::Stopped => MasterRuntimeState::Standby,
        StandbyState::Connecting
        | StandbyState::Syncing
        | StandbyState::Recovering
        | StandbyState::Reconnecting
        | StandbyState::Failed => MasterRuntimeState::Recovering,
        StandbyState::Watching => {
            if capabilities.has_oplog_following
                && observed_leader.is_some()
                && status.lag_entries > 0
            {
                MasterRuntimeState::CatchingUp
            } else {
                MasterRuntimeState::Standby
            }
        }
        StandbyState::Promoting | StandbyState::Promoted => MasterRuntimeState::LeaderWarmup,
    }
}

pub trait StandbyController: Send {
    fn start_standby(&mut self, observed_leader: Option<MasterView>) -> Result<(), HaError>;
    fn stop_standby(&mut self);
    fn promote_standby(&mut self) -> Result<(), HaError>;
    fn update_observed_leader(&mut self, observed_leader: Option<MasterView>);
    fn get_standby_runtime_state(&self) -> MasterRuntimeState;
    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>);
}

pub struct NoopStandbyController {
    callback: Option<RuntimeStateCallback>,
}

impl NoopStandbyController {
    pub fn new() -> Self {
        Self { callback: None }
    }
}

impl Default for NoopStandbyController {
    fn default() -> Self {
        Self::new()
    }
}

impl StandbyController for NoopStandbyController {
    fn start_standby(&mut self, _observed_leader: Option<MasterView>) -> Result<(), HaError> {
        if let Some(callback) = &self.callback {
            callback(MasterRuntimeState::Standby);
        }
        Ok(())
    }

    fn stop_standby(&mut self) {}

    fn promote_standby(&mut self) -> Result<(), HaError> {
        Ok(())
    }

    fn update_observed_leader(&mut self, _observed_leader: Option<MasterView>) {}

    fn get_standby_runtime_state(&self) -> MasterRuntimeState {
        MasterRuntimeState::Standby
    }

    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>) {
        self.callback = callback;
        if let Some(callback) = &self.callback {
            callback(MasterRuntimeState::Standby);
        }
    }
}

pub struct CapabilityDrivenStandbyController {
    config: MasterServiceSupervisorConfig,
    capabilities: StandbyRuntimeCapabilities,
    snapshot_provider: Box<dyn SnapshotProvider>,
    observed_leader: Option<MasterView>,
    standby_running: bool,
    promoted: bool,
    last_error: Option<HaError>,
    sync_status: StandbySyncStatus,
    loaded_snapshot: Option<LoadedSnapshot>,
    callback: Option<RuntimeStateCallback>,
    last_reported_runtime_state: Option<MasterRuntimeState>,
}

impl CapabilityDrivenStandbyController {
    pub fn new(spec: HABackendSpec, config: MasterServiceSupervisorConfig) -> Self {
        let capabilities = build_standby_runtime_capabilities(&spec, &config);
        let snapshot_provider: Box<dyn SnapshotProvider> = if capabilities.has_snapshot_bootstrap {
            match (&config.snapshot_backup_dir, config.snapshot_backend_type) {
                (Some(dir), Some(backend_type)) => {
                    Box::new(LocalSnapshotProvider::new(dir.clone(), backend_type))
                }
                _ => Box::new(NoopSnapshotProvider),
            }
        } else {
            Box::new(NoopSnapshotProvider)
        };
        Self::with_snapshot_provider(config, capabilities, snapshot_provider)
    }

    pub fn with_snapshot_provider(
        config: MasterServiceSupervisorConfig,
        capabilities: StandbyRuntimeCapabilities,
        snapshot_provider: Box<dyn SnapshotProvider>,
    ) -> Self {
        Self {
            config,
            capabilities,
            snapshot_provider,
            observed_leader: None,
            standby_running: false,
            promoted: false,
            last_error: None,
            sync_status: StandbySyncStatus::default(),
            loaded_snapshot: None,
            callback: None,
            last_reported_runtime_state: None,
        }
    }

    pub fn sync_status(&self) -> &StandbySyncStatus {
        &self.sync_status
    }

    pub fn loaded_snapshot(&self) -> Option<&LoadedSnapshot> {
        self.loaded_snapshot.as_ref()
    }

    pub fn update_sync_status_for_test(&mut self, status: StandbySyncStatus) {
        self.sync_status = status;
        self.notify_runtime_state_if_changed();
    }

    fn notify_runtime_state_if_changed(&mut self) {
        let runtime_state = self.get_standby_runtime_state();
        if self.last_reported_runtime_state == Some(runtime_state) {
            return;
        }
        self.last_reported_runtime_state = Some(runtime_state);
        if let Some(callback) = &self.callback {
            callback(runtime_state);
        }
    }
}

impl StandbyController for CapabilityDrivenStandbyController {
    fn start_standby(&mut self, observed_leader: Option<MasterView>) -> Result<(), HaError> {
        self.observed_leader = observed_leader;
        if self.standby_running {
            self.notify_runtime_state_if_changed();
            return Ok(());
        }

        self.promoted = false;
        self.sync_status = StandbySyncStatus {
            is_connected: self.observed_leader.is_some(),
            is_syncing: self.capabilities.has_oplog_following,
            state: if self.capabilities.has_snapshot_bootstrap {
                StandbyState::Recovering
            } else if self.capabilities.has_oplog_following {
                StandbyState::Watching
            } else {
                StandbyState::Stopped
            },
            ..Default::default()
        };

        if self.capabilities.has_snapshot_bootstrap {
            self.loaded_snapshot = self
                .snapshot_provider
                .load_latest_snapshot(&self.config.cluster_id)?;
            if let Some(snapshot) = &self.loaded_snapshot {
                self.sync_status.applied_seq_id = snapshot.snapshot_sequence_id;
                self.sync_status.primary_seq_id = snapshot.snapshot_sequence_id;
            }
        } else {
            self.loaded_snapshot = None;
        }

        self.sync_status.state = if self.capabilities.has_oplog_following {
            StandbyState::Watching
        } else if self.capabilities.has_snapshot_bootstrap {
            StandbyState::Watching
        } else {
            StandbyState::Stopped
        };

        self.standby_running = true;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
        Ok(())
    }

    fn stop_standby(&mut self) {
        self.standby_running = false;
        self.promoted = false;
        self.sync_status = StandbySyncStatus::default();
        self.loaded_snapshot = None;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
    }

    fn promote_standby(&mut self) -> Result<(), HaError> {
        if !self.standby_running {
            let error = self
                .last_error
                .clone()
                .unwrap_or(HaError::UnavailableInCurrentStatus);
            self.last_error = Some(error.clone());
            return Err(error);
        }
        if self.capabilities.has_oplog_following && self.sync_status.lag_entries > 0 {
            self.last_error = Some(HaError::UnavailableInCurrentStatus);
            return Err(HaError::UnavailableInCurrentStatus);
        }

        self.sync_status.state = StandbyState::Promoted;
        self.promoted = true;
        self.standby_running = false;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
        Ok(())
    }

    fn update_observed_leader(&mut self, observed_leader: Option<MasterView>) {
        self.observed_leader = observed_leader;
        self.sync_status.is_connected = self.observed_leader.is_some();
        self.notify_runtime_state_if_changed();
    }

    fn get_standby_runtime_state(&self) -> MasterRuntimeState {
        if self.promoted {
            return MasterRuntimeState::LeaderWarmup;
        }
        if !self.standby_running {
            return MasterRuntimeState::Standby;
        }
        map_standby_runtime_state(
            &self.sync_status,
            self.observed_leader.as_ref(),
            self.capabilities,
        )
    }

    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>) {
        self.callback = callback;
        self.last_reported_runtime_state = None;
        self.notify_runtime_state_if_changed();
    }
}

pub struct MasterServiceSupervisor {
    runtime_state: Arc<Mutex<MasterRuntimeState>>,
    observed_leader: Arc<Mutex<Option<MasterView>>>,
    standby_controller: Box<dyn StandbyController>,
}

impl MasterServiceSupervisor {
    pub fn new(mut standby_controller: Box<dyn StandbyController>) -> Self {
        let runtime_state = Arc::new(Mutex::new(MasterRuntimeState::Starting));
        let runtime_state_for_callback = runtime_state.clone();
        standby_controller.set_runtime_state_callback(Some(Arc::new(move |state| {
            *runtime_state_for_callback
                .lock()
                .expect("supervisor runtime state mutex poisoned") = state;
        })));

        Self {
            runtime_state,
            observed_leader: Arc::new(Mutex::new(None)),
            standby_controller,
        }
    }

    pub fn enter_standby_mode(
        &mut self,
        observed_leader: Option<MasterView>,
    ) -> Result<(), HaError> {
        *self
            .observed_leader
            .lock()
            .expect("supervisor observed leader mutex poisoned") = observed_leader.clone();
        self.standby_controller.start_standby(observed_leader)
    }

    pub fn begin_candidacy(&self) {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::Candidate;
    }

    pub fn promote_to_leader_warmup(&mut self) -> Result<(), HaError> {
        self.standby_controller.promote_standby()?;
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::LeaderWarmup;
        Ok(())
    }

    pub fn activate_serving_state(&self) {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::Serving;
    }

    pub fn runtime_state(&self) -> MasterRuntimeState {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned")
    }

    pub fn observed_leader(&self) -> Option<MasterView> {
        self.observed_leader
            .lock()
            .expect("supervisor observed leader mutex poisoned")
            .clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpLogRecord {
    pub seq: u64,
    pub producer_view_version: u64,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpLogPollResult {
    pub records: Vec<OpLogRecord>,
    pub next_seq: u64,
    pub timed_out: bool,
}

/// Result of an attempt to acquire leadership.
#[derive(Debug, Clone)]
pub struct AcquireLeadershipResult {
    pub acquired: bool,
    pub view: Option<MasterView>,
    pub lease_id: Option<i64>,
}

/// Handle for actively held leadership — cancels keepalive on drop.
pub struct LeadershipHandle {
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for LeadershipHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}

pub struct LeaderCoordinator {
    backend: CoordinatorBackend,
    role_tx: watch::Sender<LeaderRole>,
    role_rx: watch::Receiver<LeaderRole>,
}

enum CoordinatorBackend {
    Etcd {
        client: etcd_client::Client,
        election_key: String,
    },
    Redis {
        client: redis::Client,
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

    pub async fn new_etcd(endpoints: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let client = etcd_client::Client::connect(endpoints, None).await?;
        Ok(Self::with_backend(
            CoordinatorBackend::Etcd {
                client,
                election_key: "/mooncake/master/leader".to_string(),
            },
            LeaderRole::Standby,
        ))
    }

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

    /// Read the current master view from etcd election.
    pub async fn read_current_view(&self) -> Result<Option<MasterView>, HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd {
                client,
                election_key,
            } => {
                let mut client = client.clone();
                match client.leader(election_key.clone()).await {
                    Ok(resp) => match resp.kv() {
                        Some(kv) => {
                            let addr = kv.value_str().unwrap_or("").to_string();
                            Ok(Some(MasterView {
                                leader_address: addr,
                                view_version: kv.version() as u64,
                            }))
                        }
                        None => Ok(None),
                    },
                    Err(_) => Ok(None),
                }
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client.get_multiplexed_async_connection().await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let result: Option<String> = redis::cmd("GET")
                    .arg(election_key)
                    .query_async(&mut conn)
                    .await
                    .ok();
                if let Some(ref v) = result {
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

    /// Try to acquire leadership using etcd election API (campaign).
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

                // Grant a TTL lease
                let lease_resp = client.lease_grant(lease_ttl_secs, None).await.map_err(|e| {
                    HaError::InvalidBackend(format!("etcd lease grant error: {e}"))
                })?;
                let lease_id = lease_resp.id();

                // Campaign for leadership
                let name = election_key.clone();
                let value = leader_address.to_string();
                match client.campaign(name, value, lease_id).await {
                    Ok(resp) => {
                        let acquired = resp
                            .leader()
                            .and_then(|l| l.name_str().ok())
                            .map(|n| n == leader_address)
                            .unwrap_or(false);
                        if acquired {
                            let _ = self.role_tx.send(LeaderRole::Leader);
                            info!(
                                "Leadership acquired: address={}, lease_id={}",
                                leader_address, lease_id
                            );
                            Ok(AcquireLeadershipResult {
                                acquired: true,
                                view: Some(MasterView {
                                    leader_address: leader_address.to_string(),
                                    view_version: 1,
                                }),
                                lease_id: Some(lease_id),
                            })
                        } else {
                            Ok(AcquireLeadershipResult {
                                acquired: false,
                                view: current,
                                lease_id: None,
                            })
                        }
                    }
                    Err(e) => {
                        warn!("etcd campaign failed: {}", e);
                        Ok(AcquireLeadershipResult {
                            acquired: false,
                            view: current,
                            lease_id: None,
                        })
                    }
                }
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client.get_multiplexed_async_connection().await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let ttl_ms = (lease_ttl_secs * 1000) as usize;
                let value = format!("{}|{}", leader_address, "1");
                let result: Option<String> = redis::cmd("SET")
                    .arg(election_key)
                    .arg(&value)
                    .arg("NX")
                    .arg("PX")
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
                        lease_id: Some(lease_ttl_secs),
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

    /// Start keepalive task for the given lease. Handle stops on drop.
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
                    // Create the keepalive stream
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
                        return Ok(LeadershipHandle { cancel_tx: Some(cancel_tx) });
                    }
                };
                let lease_ms = lease_id * 1000;
                let role_tx = self.role_tx.clone();
                let ek = election_key.clone();
                let client2 = client.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
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
            _ => {}
        }
        Ok(LeadershipHandle {
            cancel_tx: Some(cancel_tx),
        })
    }

    /// Release leadership via etcd resign.
    pub async fn release_leadership(&self, _lease_id: i64) -> Result<(), HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let mut client = client.clone();
                client.resign(None).await.map_err(|e| {
                    HaError::InvalidBackend(format!("etcd resign error: {e}"))
                })?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                info!("Leadership released via resign");
                Ok(())
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client.get_multiplexed_async_connection().await
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

    /// Poll for a view change from known_version, up to timeout.
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
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub async fn wait_for_role(&self) -> Result<LeaderRole, Box<dyn std::error::Error>> {
        match &self.backend {
            CoordinatorBackend::Etcd { .. } => {
                info!("Etcd leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => {
                info!(
                    "K8s Lease election initialized: namespace={}, lease={}",
                    namespace, lease_name
                );
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Redis { .. } => {
                info!("Redis leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Manual => Ok(*self.role_rx.borrow()),
        }
    }

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

