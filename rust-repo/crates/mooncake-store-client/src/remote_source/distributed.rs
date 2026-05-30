use std::time::Duration;

use tokio::time::sleep;
use tonic::transport::Channel;
use tracing;

use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};
use crate::proto::{self, master_service_client::MasterServiceClient};
use crate::MissHandler;
use crate::RemoteSourceConfig;

/// Wraps [`MissHandler`] with cross-node coordination via the Master.
///
/// On a cache miss, first asks the Master via `AcquireRemotePull`:
/// - **PULL**: this node fetches from the remote source, then calls `CompleteRemotePull`
/// - **WAIT**: another node is already fetching; poll-retry the store `get` until data appears
/// - **ABANDON**: remote source is disabled; return NotFound
pub struct DistributedMissHandler<S: RemoteSource> {
    inner: MissHandler<S>,
    master: MasterServiceClient<Channel>,
    client_id: uuid::Uuid,
    /// Max retries when in WAIT state before giving up.
    max_wait_retries: usize,
    /// Base backoff between retries in the WAIT state.
    wait_backoff: Duration,
}

impl<S: RemoteSource + 'static> DistributedMissHandler<S> {
    pub fn new(
        inner: MissHandler<S>,
        master: MasterServiceClient<Channel>,
        client_id: uuid::Uuid,
    ) -> Self {
        Self {
            inner,
            master,
            client_id,
            max_wait_retries: 10,
            wait_backoff: Duration::from_millis(100),
        }
    }

    pub fn with_wait_policy(mut self, max_retries: usize, backoff: Duration) -> Self {
        self.max_wait_retries = max_retries;
        self.wait_backoff = backoff;
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }

    pub fn config(&self) -> &RemoteSourceConfig {
        self.inner.config()
    }

    /// Handle a cache miss with cross-node coordination.
    ///
    /// # Flow
    /// 1. Ask Master: "should I pull key X from remote source?"
    /// 2. If PULL → fetch from S3/local-FS, `PutStart`/`PutEnd` into store, `CompleteRemotePull`
    /// 3. If WAIT → another node is pulling; retry until data is in the store
    /// 4. If ABANDON → remote source disabled, return NotFound
    pub async fn handle_miss(
        &self,
        key: &str,
        data_callback: impl Fn(&[u8]),
    ) -> RemoteSourceResult<Vec<u8>> {
        if !self.inner.is_enabled() {
            return Err(RemoteSourceError::NotFound(key.to_string()));
        }

        // Step 1: Acquire pull rights from master
        let action = self.acquire_pull(key).await?;

        match action {
            proto::RemotePullAction::Pull => {
                // Step 2a: This node fetches from remote source
                let result = self.inner.handle_miss(key).await;

                // Notify master of completion
                let _ = self
                    .complete_pull(
                        key,
                        result.is_ok(),
                        result.as_ref().map_or(0, |d| d.len() as u64),
                    )
                    .await;

                // Call the data callback so the caller can PutStart/PutEnd into the store
                if let Ok(ref data) = result {
                    data_callback(data);
                }

                result
            }
            proto::RemotePullAction::Wait => {
                // Step 2b: Another node is pulling — retry until they finish
                self.wait_and_retry(key).await
            }
            proto::RemotePullAction::Abandon => Err(RemoteSourceError::NotFound(key.to_string())),
        }
    }

    /// Ask the master for pull permission.
    async fn acquire_pull(&self, key: &str) -> RemoteSourceResult<proto::RemotePullAction> {
        let mut master = self.master.clone();
        let request = proto::AcquireRemotePullRequest {
            client_id: Some(proto::Uuid {
                high: self.client_id.as_u64_pair().0,
                low: self.client_id.as_u64_pair().1,
            }),
            key: key.to_string(),
        };

        let response = master
            .acquire_remote_pull(request)
            .await
            .map_err(|e| RemoteSourceError::Internal(format!("acquire_remote_pull failed: {e}")))?;

        let action: proto::RemotePullAction = match response.into_inner().action {
            0 => proto::RemotePullAction::Pull,
            1 => proto::RemotePullAction::Wait,
            _ => proto::RemotePullAction::Abandon,
        };

        tracing::debug!(key = %key, action = ?action, "remote pull coordination");
        Ok(action)
    }

    /// Notify master that the pull completed (or failed).
    async fn complete_pull(
        &self,
        key: &str,
        success: bool,
        data_size: u64,
    ) -> RemoteSourceResult<()> {
        let mut master = self.master.clone();
        let _ = master
            .complete_remote_pull(proto::CompleteRemotePullRequest {
                client_id: Some(proto::Uuid {
                    high: self.client_id.as_u64_pair().0,
                    low: self.client_id.as_u64_pair().1,
                }),
                key: key.to_string(),
                success,
                data_size,
            })
            .await;
        Ok(())
    }

    /// When another node is pulling, poll-retry the normal get path.
    /// The pulling node will have put data into the store via PutEnd,
    /// so subsequent GetReplicaList calls should find it.
    async fn wait_and_retry(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        let mut backoff = self.wait_backoff;
        for attempt in 1..=self.max_wait_retries {
            sleep(backoff).await;

            // Try the inner handler again — if the pulling node already stored
            // the data, this will find it (via normal RDMA read or local cache).
            match self.inner.handle_miss(key).await {
                Ok(data) => return Ok(data),
                Err(RemoteSourceError::NotFound(_)) => {
                    // Not yet available — keep waiting
                }
                Err(e) => return Err(e),
            }

            backoff = (backoff * 2).min(Duration::from_secs(5));
            tracing::debug!(key = %key, attempt, "waiting for remote pull by another node");
        }

        // Last attempt via the inner handler
        self.inner.handle_miss(key).await
    }

    /// Access the inner MissHandler and source.
    pub fn inner(&self) -> &MissHandler<S> {
        &self.inner
    }
}
