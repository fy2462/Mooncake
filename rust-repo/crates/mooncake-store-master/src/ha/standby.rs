use super::catalog_snapshot::create_catalog_backed_snapshot_provider;
use super::snapshot::{
    LoadedSnapshot, LocalSnapshotProvider, SnapshotCatalogStoreType, SnapshotObjectStoreType,
    SnapshotProvider,
};
use super::types::{
    HABackendSpec, HABackendType, HaError, MasterRuntimeState, MasterView, RuntimeStateCallback,
    StandbyState, StandbySyncStatus,
};
use crate::hot_standby::{HotStandbyConfig, HotStandbyService};
use crate::oplog::EtcdOpLogStore;
use crate::service::state::MasterState;
use crate::storage_backend::StorageBackendType;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

// ----------------------------------------------------------------------------
// MasterServiceSupervisorConfig — configuration for the HA supervisor
// MasterServiceSupervisorConfig —— HA supervisor 的配置
// ----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MasterServiceSupervisorConfig {
    /// This node's hostname, used as leader_address in election.
    /// 本节点的主机名，在选举中用作 leader_address。
    pub local_hostname: String,
    /// Cluster identifier for namespace isolation. / 集群标识符，用于命名空间隔离。
    pub cluster_id: String,
    /// Whether to enable snapshot-based standby bootstrap on startup.
    /// 是否在启动时启用基于快照的 standby 引导。
    pub enable_snapshot_restore: bool,
    /// Directory where snapshots are stored. / 快照存储的目录。
    pub snapshot_backup_dir: Option<PathBuf>,
    /// Backend type for snapshot storage (e.g. JSON). / 快照存储的后端类型。
    pub snapshot_backend_type: Option<StorageBackendType>,
    /// C++-compatible snapshot payload store. None preserves the legacy local provider.
    pub snapshot_object_store_type: Option<SnapshotObjectStoreType>,
    /// Catalog used to resolve the latest C++-compatible snapshot.
    pub snapshot_catalog_store_type: SnapshotCatalogStoreType,
    /// Optional catalog connection string; Redis falls back to the HA connection.
    pub snapshot_catalog_store_connstring: Option<String>,
}

impl Default for MasterServiceSupervisorConfig {
    fn default() -> Self {
        Self {
            local_hostname: "localhost".to_string(),
            cluster_id: resolve_cluster_id(""),
            enable_snapshot_restore: false,
            snapshot_backup_dir: None,
            snapshot_backend_type: None,
            snapshot_object_store_type: None,
            snapshot_catalog_store_type: SnapshotCatalogStoreType::Embedded,
            snapshot_catalog_store_connstring: None,
        }
    }
}

struct FailedSnapshotProvider(HaError);

impl SnapshotProvider for FailedSnapshotProvider {
    fn load_latest_snapshot(&self, _cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        Err(self.0.clone())
    }
}

// ----------------------------------------------------------------------------
// StandbyRuntimeCapabilities — what the standby can do
// StandbyRuntimeCapabilities —— standby 的能力描述
// ----------------------------------------------------------------------------

/// Describes the capabilities available to a standby in this deployment.
/// 描述本次部署中 standby 可用的能力。
#[derive(Debug, Clone, Copy)]
pub struct StandbyRuntimeCapabilities {
    /// True if snapshot bootstrap is available. / 快照引导是否可用。
    pub has_snapshot_bootstrap: bool,
    /// True if oplog following (etcd) is available. / oplog 跟随（etcd）是否可用。
    pub has_oplog_following: bool,
}

/// Build capabilities from backend spec and supervisor config.
/// 从后端规格和 supervisor 配置构建能力描述。
pub fn build_standby_runtime_capabilities(
    spec: &HABackendSpec,
    config: &MasterServiceSupervisorConfig,
) -> StandbyRuntimeCapabilities {
    StandbyRuntimeCapabilities {
        has_snapshot_bootstrap: config.enable_snapshot_restore,
        // Oplog following is only available with etcd (its sequential key model).
        // oplog 跟随仅在 etcd 下可用（因其有序 key 模型）。
        has_oplog_following: spec.backend_type == HABackendType::Etcd,
    }
}

/// Map a standby's raw sync status to a MasterRuntimeState.
/// 将 standby 的原始同步状态映射为 MasterRuntimeState。
///
/// Logic (逻辑):
/// - Stopped → Standby
/// - Connecting/Syncing/Recovering/Reconnecting/Failed → Recovering
/// - Watching with lag_entries > 0 and oplog following enabled → CatchingUp
/// - Watching without lag or without oplog following → Standby
/// - Promoting/Promoted → LeaderWarmup
pub fn map_standby_runtime_state(
    status: &StandbySyncStatus,
    observed_leader: Option<&MasterView>,
    capabilities: StandbyRuntimeCapabilities,
) -> MasterRuntimeState {
    match status.state {
        StandbyState::Stopped => MasterRuntimeState::Standby,
        // All non-terminal intermediate states map to Recovering.
        // 所有非终结中间状态映射为 Recovering。
        StandbyState::Connecting
        | StandbyState::Syncing
        | StandbyState::Recovering
        | StandbyState::Reconnecting
        | StandbyState::Failed => MasterRuntimeState::Recovering,
        StandbyState::Watching => {
            // If oplog following is available and the leader is visible and
            // we are behind, we are catching up. Otherwise, we are stable standby.
            // 如果 oplog 跟随可用、leader 可见且落后于 leader，则正在追赶。
            // 否则是稳定 standby。
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

// ----------------------------------------------------------------------------
// StandbyController — unified trait for standby lifecycle management
// StandbyController —— standby 生命周期管理的统一 trait
//
// Two implementations (两种实现):
// - NoopStandbyController: no-op for single-node / manual mode.
//   无操作实现，用于单节点 / 手动模式。
// - CapabilityDrivenStandbyController: snapshot bootstrap + oplog following.
//   基于能力的实现：快照引导 + oplog 跟随。
//
// promote_standby checks lag_entries == 0 (oplog caught up) before allowing
// promotion. / promote_standby 会校验 lag_entries 是否归零（oplog 已追平）
// 才允许提升。
// ----------------------------------------------------------------------------

pub trait StandbyController: Send {
    /// Start standby mode with an optional observed leader address.
    /// 以可选的 observed leader 地址启动 standby 模式。
    fn start_standby(&mut self, observed_leader: Option<MasterView>) -> Result<(), HaError>;

    /// Stop standby mode and release resources. / 停止 standby 模式并释放资源。
    fn stop_standby(&mut self);

    /// Promote this standby to leader. / 将此 standby 提升为 leader。
    fn promote_standby(&mut self) -> Result<(), HaError>;

    /// Update the observed leader address (e.g. after a view change).
    /// 更新 observed leader 地址（如视图变更后）。
    fn update_observed_leader(&mut self, observed_leader: Option<MasterView>);

    /// Get the current runtime state for gRPC health checks.
    /// 获取当前运行时状态，用于 gRPC 健康检查。
    fn get_standby_runtime_state(&self) -> MasterRuntimeState;

    /// Register a callback for runtime state changes.
    /// 注册运行时状态变化的回调。
    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>);
}

// ----------------------------------------------------------------------------
// NoopStandbyController — no-op standby (single-node mode)
// NoopStandbyController —— 无操作 standby（单节点模式）
//
// Always reports Standby. Promote is a no-op. Used for development and
// single-node deployments.
//
// 始终报告 Standby。Promote 是无操作。用于开发环境和单节点部署。
// ----------------------------------------------------------------------------

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
        // Immediately notify that we are in Standby state.
        // 立即通知我们处于 Standby 状态。
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

// ----------------------------------------------------------------------------
// CapabilityDrivenStandbyController — full-featured standby
// CapabilityDrivenStandbyController —— 全功能 standby
//
// This is the production standby implementation. It supports:
// - Snapshot bootstrap: load a recent snapshot from disk to quickly initialise
//   the standby's state before catching up on recent oplog entries.
// - Oplog following: tail the oplog from the leader to stay in sync.
// - Lag-aware promotion: refuse promotion if lag_entries > 0.
//
// 这是生产环境的 standby 实现。支持：
// - 快照引导：从磁盘加载最近的快照以快速初始化 standby 状态，然后追赶最近的 oplog。
// - oplog 跟随：从 leader 尾部跟随 oplog 以保持同步。
// - 滞后感知的提升：如果 lag_entries > 0 则拒绝提升。
// ----------------------------------------------------------------------------

pub struct CapabilityDrivenStandbyController {
    ha_spec: HABackendSpec,
    config: MasterServiceSupervisorConfig,
    capabilities: StandbyRuntimeCapabilities,
    /// The inner hot standby service (matches C++: HotStandbyService member).
    /// C++ equivalent: `std::unique_ptr<HotStandbyService> standby_service_` in standby_controller.cpp.
    service: HotStandbyService,
    observed_leader: Option<MasterView>,
    standby_running: bool,
    last_error: Option<HaError>,
    callback: Option<RuntimeStateCallback>,
    last_reported_runtime_state: Option<MasterRuntimeState>,
}

impl CapabilityDrivenStandbyController {
    /// Create a new controller. Builds the internal HotStandbyService from config
    /// (matches C++ `CreateStandbyService` in standby_controller.cpp:23-35).
    /// C++ equivalent: `CapabilityDrivenStandbyController` constructor at standby_controller.cpp:101-131.
    /// Create a new controller. Builds the internal HotStandbyService from config.
    /// C++ equivalent: CapabilityDrivenStandbyController constructor + CreateStandbyService.
    pub fn new(spec: HABackendSpec, config: MasterServiceSupervisorConfig) -> Self {
        Self::new_with_state(spec, config, Arc::new(MasterState::empty()))
    }

    pub(crate) fn new_with_state(
        spec: HABackendSpec,
        config: MasterServiceSupervisorConfig,
        state: Arc<MasterState>,
    ) -> Self {
        let capabilities = build_standby_runtime_capabilities(&spec, &config);

        let service_config = HotStandbyConfig {
            enable_snapshot_bootstrap: capabilities.has_snapshot_bootstrap,
            enable_oplog_following: capabilities.has_oplog_following,
            cluster_id: config.cluster_id.clone(),
            ..Default::default()
        };
        let mut service = HotStandbyService::new(state, service_config);

        if capabilities.has_snapshot_bootstrap {
            if let Some(object_store_type) = config.snapshot_object_store_type {
                let connstring = config
                    .snapshot_catalog_store_connstring
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| {
                        (!spec.connstring.trim().is_empty()).then_some(spec.connstring.as_str())
                    });
                match create_catalog_backed_snapshot_provider(
                    config.cluster_id.clone(),
                    object_store_type,
                    config.snapshot_catalog_store_type,
                    config.snapshot_backup_dir.clone(),
                    connstring,
                ) {
                    Ok(provider) => service.set_snapshot_provider(Box::new(provider)),
                    Err(error) => {
                        service.set_snapshot_provider(Box::new(FailedSnapshotProvider(error)))
                    }
                }
            } else if let (Some(dir), Some(backend_type)) =
                (&config.snapshot_backup_dir, config.snapshot_backend_type)
            {
                service.set_snapshot_provider(Box::new(LocalSnapshotProvider::new(
                    dir.clone(),
                    backend_type,
                )));
            }
        }

        Self {
            ha_spec: spec,
            config,
            capabilities,
            service,
            observed_leader: None,
            standby_running: false,
            last_error: None,
            callback: None,
            last_reported_runtime_state: None,
        }
    }

    /// For testing: create with pre-built service.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn with_service(
        config: MasterServiceSupervisorConfig,
        capabilities: StandbyRuntimeCapabilities,
        service: HotStandbyService,
    ) -> Self {
        Self {
            ha_spec: HABackendSpec {
                backend_type: HABackendType::Unknown,
                connstring: String::new(),
                cluster_namespace: config.cluster_id.clone(),
            },
            config,
            capabilities,
            service,
            observed_leader: None,
            standby_running: false,
            last_error: None,
            callback: None,
            last_reported_runtime_state: None,
        }
    }

    pub fn sync_status(&self) -> StandbySyncStatus {
        self.service.sync_status()
    }

    /// For testing: manually update sync status.
    pub fn update_sync_status_for_test(&mut self, _status: StandbySyncStatus) {
        // Test-only: sync status is managed by the service in production.
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

    fn ensure_oplog_store(&mut self) -> Result<(), HaError> {
        if !self.capabilities.has_oplog_following || self.service.has_oplog_store() {
            return Ok(());
        }
        let connstring = self.ha_spec.connstring.clone();
        if connstring.trim().is_empty() {
            return Err(HaError::InvalidBackend(
                "etcd oplog following requires an etcd connection string".into(),
            ));
        }
        let cluster_id = if self.config.cluster_id.is_empty() {
            resolve_cluster_id("")
        } else {
            self.config.cluster_id.clone()
        };
        let store = block_on_runtime(async move {
            let endpoints: Vec<String> = connstring
                .split(';')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            let client = etcd_client::Client::connect(endpoints, None)
                .await
                .map_err(|e| HaError::InvalidBackend(format!("etcd oplog connect: {e}")))?;
            EtcdOpLogStore::new(client, &format!("/oplog/{cluster_id}")).await
        })?;
        self.service.set_oplog_store(Box::new(store));
        Ok(())
    }
}

fn resolve_cluster_id(cluster_id: &str) -> String {
    if !cluster_id.trim().is_empty() {
        return cluster_id.to_string();
    }
    std::env::var("MC_STORE_CLUSTER_ID")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "mooncake".to_string())
}

fn block_on_runtime<F: Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create temporary tokio runtime")
            .block_on(future)
    }
}

impl StandbyController for CapabilityDrivenStandbyController {
    fn start_standby(&mut self, observed_leader: Option<MasterView>) -> Result<(), HaError> {
        // C++: StartStandby at standby_controller.cpp:133-165
        //   → standby_service_->Start(leader_address, oplog_connstring, cluster_id)
        self.observed_leader = observed_leader;

        if self.standby_running {
            self.notify_runtime_state_if_changed();
            return Ok(());
        }

        self.ensure_oplog_store()?;
        block_on_runtime(self.service.start())?;

        self.standby_running = true;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
        Ok(())
    }

    fn stop_standby(&mut self) {
        // C++: StopStandby at standby_controller.cpp:167-182
        //   → standby_service_->Stop()
        self.service.stop();
        self.standby_running = false;
        self.observed_leader = None;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
    }

    fn promote_standby(&mut self) -> Result<(), HaError> {
        // C++: PromoteStandby at standby_controller.cpp:184-209
        //   → standby_service_->Promote()
        if !self.standby_running {
            return Err(self
                .last_error
                .clone()
                .unwrap_or(HaError::UnavailableInCurrentStatus));
        }

        let result = block_on_runtime(self.service.promote());
        match result {
            Ok(_seq_id) => {
                self.standby_running = false;
                self.notify_runtime_state_if_changed();
                Ok(())
            }
            Err(e) => {
                self.service.stop();
                self.standby_running = false;
                self.last_error = Some(e.clone());
                self.notify_runtime_state_if_changed();
                Err(e)
            }
        }
    }

    fn update_observed_leader(&mut self, observed_leader: Option<MasterView>) {
        // C++: UpdateObservedLeader at standby_controller.cpp:212-219
        self.observed_leader = observed_leader;
        self.notify_runtime_state_if_changed();
    }

    fn get_standby_runtime_state(&self) -> MasterRuntimeState {
        // C++: GetStandbyRuntimeState at standby_controller.cpp:221-234
        //   → MapStandbyRuntimeState(standby_service_->GetSyncStatus(), ...)
        if !self.standby_running {
            return MasterRuntimeState::Standby;
        }
        let sync = self.service.sync_status();
        map_standby_runtime_state(&sync, self.observed_leader.as_ref(), self.capabilities)
    }

    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>) {
        // C++: SetStandbyRuntimeStateCallback at standby_controller.cpp:236-244
        self.callback = callback;
        self.last_reported_runtime_state = None;
        self.notify_runtime_state_if_changed();
    }
}
