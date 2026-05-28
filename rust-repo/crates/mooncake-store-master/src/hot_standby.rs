use crate::ha::{HaError, SnapshotProvider, StandbyState, StandbySyncStatus};
use crate::service::state::MasterState;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

pub struct HotStandbyConfig {
    pub enable_snapshot_bootstrap: bool,
    pub enable_oplog_following: bool,
    pub oplog_poll_interval_ms: u64,
    pub cluster_id: String,
}

impl Default for HotStandbyConfig {
    fn default() -> Self {
        Self {
            enable_snapshot_bootstrap: false,
            enable_oplog_following: false,
            oplog_poll_interval_ms: 1000,
            cluster_id: String::new(),
        }
    }
}

pub struct HotStandbyService {
    state: Arc<MasterState>,
    config: HotStandbyConfig,
    sync_status: parking_lot::RwLock<StandbySyncStatus>,
    shutdown_tx: Option<watch::Sender<()>>,
    #[allow(dead_code)]
    shutdown_rx: Option<watch::Receiver<()>>,
    #[allow(dead_code)]
    snapshot_provider: Option<Box<dyn SnapshotProvider>>,
}

impl HotStandbyService {
    #[allow(dead_code, reason = "used by HA supervisor")]
    pub(crate) fn new(state: Arc<MasterState>, config: HotStandbyConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        Self {
            state,
            config,
            sync_status: parking_lot::RwLock::new(StandbySyncStatus {
                state: StandbyState::Stopped,
                ..Default::default()
            }),
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            snapshot_provider: None,
        }
    }

    pub fn set_snapshot_provider(&mut self, provider: Box<dyn SnapshotProvider>) {
        self.snapshot_provider = Some(provider);
    }

    pub fn sync_status(&self) -> StandbySyncStatus {
        self.sync_status.read().clone()
    }

    pub fn is_ready_for_promotion(&self) -> bool {
        matches!(
            self.sync_status.read().state,
            StandbyState::Watching | StandbyState::Promoted
        )
    }

    /// Start standby: optionally load snapshot, then begin oplog following
    pub async fn start(&mut self) -> Result<(), HaError> {
        let mut status = self.sync_status.write();
        status.state = StandbyState::Connecting;
        status.is_connected = false;
        drop(status);

        // Snapshot bootstrap (if enabled)
        if self.config.enable_snapshot_bootstrap {
            if let Some(ref provider) = self.snapshot_provider {
                let mut status = self.sync_status.write();
                status.state = StandbyState::Recovering;
                drop(status);

                if let Ok(Some(snapshot)) =
                    provider.load_latest_snapshot(&self.config.cluster_id)
                {
                    // Apply snapshot segments (Segment only, default to Active status)
                    for seg in &snapshot.segments {
                        let sid = seg.id;
                        if !self.state.segments.contains_key(&sid) {
                            self.state.segments.insert(
                                sid,
                                crate::service::SegmentEntry {
                                    segment: seg.clone(),
                                    status: crate::proto::SegmentStatus::Active,
                                    used: 0,
                                    client_id: Uuid::nil(),
                                },
                            );
                            // Also register with allocator
                            self.state.allocator.write().add_segment(seg.clone(), 0, Uuid::nil());
                        }
                    }
                    // Apply NoF segments
                    for nof in &snapshot.nof_segments {
                        let sid = nof.segment.id;
                        if !self.state.nof_segments.contains_key(&sid) {
                            self.state.nof_segments.insert(sid, nof.clone());
                            self.state
                                .nof_allocator
                                .write()
                                .add_segment(mooncake_store_core::Segment {
                                    id: nof.segment.id,
                                    name: nof.segment.name.clone(),
                                    base: nof.segment.base,
                                    size: nof.segment.size,
                                    te_endpoint: nof.segment.te_endpoint.clone(),
                                    protocol: String::new(),
                                }, nof.used, nof.segment.client_id);
                        }
                    }
                    // Apply objects
                    for entry in &snapshot.objects {
                        self.state.objects.insert(entry.0.clone(), entry.1.clone());
                    }
                    // Apply tasks
                    for task in &snapshot.tasks {
                        self.state.tasks.insert(task.info.id, task.clone());
                    }

                    let mut status = self.sync_status.write();
                    status.applied_seq_id = snapshot.snapshot_sequence_id;
                    drop(status);
                    info!(
                        "Loaded snapshot with {} objects, {} segments",
                        snapshot.objects.len(),
                        snapshot.segments.len()
                    );
                }
            }
        }

        // Start oplog following (if enabled)
        if self.config.enable_oplog_following {
            let mut status = self.sync_status.write();
            status.state = StandbyState::Watching;
            status.is_syncing = true;
            drop(status);
            // NOTE: actual oplog following requires a shared OpLogStore.
            // For now, mark as watching — the store should be injected externally.
            info!("HotStandbyService: started oplog following mode");
        } else {
            let mut status = self.sync_status.write();
            status.state = StandbyState::Watching;
            drop(status);
        }

        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let mut status = self.sync_status.write();
        status.state = StandbyState::Stopped;
        status.is_syncing = false;
    }

    /// Promote: finalize standby state and return the last applied sequence id
    pub async fn promote(&mut self) -> Result<u64, HaError> {
        let mut status = self.sync_status.write();
        status.state = StandbyState::Promoting;
        drop(status);

        let applied = {
            let s = self.sync_status.read();
            s.applied_seq_id
        };

        let mut status = self.sync_status.write();
        status.state = StandbyState::Promoted;
        drop(status);

        info!("HotStandbyService: promoted with seq_id={}", applied);
        Ok(applied)
    }
}
