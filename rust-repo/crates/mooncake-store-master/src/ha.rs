use crate::service::ObjectEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use mooncake_store_core::Segment;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use thiserror::Error;
use tokio::sync::watch;
use tracing::info;

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
    pub objects: Vec<(String, ObjectEntry)>,
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
        let Some((segments, objects)) = backend
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
            segments,
            objects,
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub struct InMemoryOpLogManager {
    buffer: VecDeque<OpLogRecord>,
    last_seq: u64,
    max_entries: usize,
}

impl InMemoryOpLogManager {
    pub fn new(max_entries: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            last_seq: 0,
            max_entries: max_entries.max(1),
        }
    }

    pub fn append(&mut self, producer_view_version: u64, payload: impl Into<String>) -> u64 {
        self.last_seq += 1;
        if self.buffer.len() >= self.max_entries {
            self.buffer.pop_front();
        }
        self.buffer.push_back(OpLogRecord {
            seq: self.last_seq,
            producer_view_version,
            payload: payload.into(),
        });
        self.last_seq
    }

    pub fn get_last_sequence_id(&self) -> u64 {
        self.last_seq
    }

    pub fn poll_from(&self, next_seq: u64, max_records: usize) -> OpLogPollResult {
        let records = self
            .buffer
            .iter()
            .filter(|record| record.seq >= next_seq)
            .take(max_records)
            .cloned()
            .collect::<Vec<_>>();
        let next_seq = records
            .last()
            .map(|record| record.seq + 1)
            .unwrap_or(next_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
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

    pub async fn new_k8s(
        namespace: &str,
        lease_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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

    pub async fn wait_for_role(&self) -> Result<LeaderRole, Box<dyn std::error::Error>> {
        match &self.backend {
            CoordinatorBackend::Etcd { client: _, .. } => {
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
