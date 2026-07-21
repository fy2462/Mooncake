//! # Distributed Miss Handler — 分布式未命中处理
//!
//! 在 [`MissHandler`] 之上增加跨节点协调机制，通过 Master 服务避免重复的远程获取。
//! (Extends [`MissHandler`] with cross-node coordination via the Master service to avoid
//! duplicate remote fetches across nodes.)
//!
//! ## 问题 (Problem)
//!
//! 当多个 Mooncake 节点同时遇到相同的缓存未命中时，如果不加协调，每个节点都会各自从
//! 远程源（S3/本地FS）获取数据，造成：
//! - 重复的对外请求 (duplicate egress requests)
//! - 浪费带宽和远程源配额 (wasted bandwidth and remote source quota)
//! - 存储中写入重复数据 (duplicate PutIntoStore operations)
//!
//! ## 解决方案 (Solution)
//!
//! 通过 Master 服务实现分布式协调，使用三态协议 (three-state protocol)：
//!
//! ```text
//! Client Node                    Master                    Other Nodes
//!     │                             │                           │
//!     ├──AcquireRemotePull(key)──→  │                           │
//!     │                             ├── 决策 (decide)           │
//!     │←── PULL action ──────────── │                           │
//!     │                             │                           │
//!     ├── source.get(key) ──→ S3    │                           │
//!     │←── data ──────────── S3     │                           │
//!     │                             │                           │
//!     ├── PutStart/PutEnd ──→ Store │                           │
//!     ├── CompleteRemotePull(key)──→│                           │
//!     │                             │                           │
//!     │                             │    (other node retries)   │
//!     │                             │←── GetReplicaList(key) ───┤
//!     │                             │──→ found! ───────────────→│
//! ```
//!
//! ## 三态协议 (Three-State Protocol)
//!
//! | Action | 含义 (Meaning) | 处理方式 (Handling) |
//! |---|---|---|
//! | `PULL` | 本节点负责从远程源获取 | 执行 `handle_miss` → 写入 store → `CompleteRemotePull` |
//! | `WAIT` | 另一节点正在获取 | 指数退避重试，直到数据出现在 store 中 |
//! | `ABANDON` | 远程源已禁用 | 直接返回 `NotFound` |
//!
//! ## 与 MissHandler 的关系 (Relationship with MissHandler)
//!
//! `DistributedMissHandler` 包装 `MissHandler`，在调用 `inner.handle_miss` 之前
//! 先通过 Master 进行协调。协调后才执行实际获取，获取完成后通知 Master。

use std::time::Duration;

use tokio::time::sleep;
use tonic::transport::Channel;
use tracing;

use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};
use crate::MissHandler;
use crate::RemoteSourceConfig;
use crate::proto::{self, master_service_client::MasterServiceClient};

/// 包装 [`MissHandler`] 增加 Master 协调的分布式未命中处理器。
/// (Wraps [`MissHandler`] with cross-node coordination via the Master.)
///
/// ## 字段 (Fields)
/// - `inner`: 内部 MissHandler，负责实际的远程获取和缓存
/// - `master`: Master gRPC 客户端，用于协调多节点的远程获取
/// - `client_id`: 当前节点的唯一标识符 (UUID)
/// - `max_wait_retries`: WAIT 状态下的最大重试次数（默认 10）
/// - `wait_backoff`: 重试退避的基础间隔（默认 100ms，指数增长）
///
/// ## 工作流 (Workflow)
/// 在缓存未命中时：
/// 1. 询问 Master: "key X 应该由我获取吗？" (via `AcquireRemotePull`)
/// 2. PULL → 从 S3/本地FS 获取, `PutStart`/`PutEnd` 写入 store, 通知 Master `CompleteRemotePull`
/// 3. WAIT → 另一节点正在获取; 轮询重试直到数据进入 store
/// 4. ABANDON → 远程源已禁用, 返回 NotFound
pub struct DistributedMissHandler<S: RemoteSource> {
    inner: MissHandler<S>,
    /// Master gRPC 客户端，用于分布式协调
    master: MasterServiceClient<Channel>,
    /// 当前节点的 UUID
    client_id: uuid::Uuid,
    /// WAIT 状态下最大重试次数 (max retries when in WAIT state before giving up)
    max_wait_retries: usize,
    /// WAIT 状态下重试的基础退避间隔 (base backoff between retries in WAIT state)
    wait_backoff: Duration,
}

impl<S: RemoteSource + 'static> DistributedMissHandler<S> {
    /// 创建分布式未命中处理器。
    /// (Create a DistributedMissHandler.)
    ///
    /// ## 参数 (Parameters)
    /// - `inner`: 已配置的 MissHandler 实例
    /// - `master`: 连接到 Master 的 gRPC 客户端
    /// - `client_id`: 本节点的 UUID（用于 Master 区分不同节点）
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

    /// 自定义 WAIT 状态下的重试策略。
    /// (Customize the retry policy for the WAIT state.)
    ///
    /// 退避策略: `wait_backoff * 2^(attempt-1)`, 最大上限 5 秒。
    pub fn with_wait_policy(mut self, max_retries: usize, backoff: Duration) -> Self {
        self.max_wait_retries = max_retries;
        self.wait_backoff = backoff;
        self
    }

    /// 返回远程源是否启用。
    /// (Returns whether the remote source is enabled.)
    pub fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }

    /// 返回配置。
    /// (Returns the remote source config.)
    pub fn config(&self) -> &RemoteSourceConfig {
        self.inner.config()
    }

    /// 处理缓存未命中，加入跨节点协调。
    /// (Handle a cache miss with cross-node coordination.)
    ///
    /// ## 流程 (Flow — see module docs for diagram)
    /// 1. 若未启用 → 直接返回 NotFound
    /// 2. 向 Master 发送 `AcquireRemotePull` 请求 → 获得 action (PULL/WAIT/ABANDON)
    /// 3. PULL → `inner.handle_miss(key)` → 通过 `data_callback` 让调用者写入 store → `CompleteRemotePull`
    /// 4. WAIT → `wait_and_retry(key)` 指数退避轮询
    /// 5. ABANDON → 返回 NotFound
    ///
    /// ## 参数 (Parameters)
    /// - `key`: 要获取的 key
    /// - `data_callback`: 当本节点完成远程获取后调用，用于将数据写入 store（PutStart/PutEnd）
    ///
    /// # Flow
    /// 1. Ask Master: "should I pull key X from remote source?"
    /// 2. If PULL — fetch from S3/local-FS, `PutStart`/`PutEnd` into store, `CompleteRemotePull`
    /// 3. If WAIT — another node is pulling; retry until data is in the store
    /// 4. If ABANDON — remote source disabled, return NotFound
    pub async fn handle_miss(
        &self,
        key: &str,
        data_callback: impl Fn(&[u8]),
    ) -> RemoteSourceResult<Vec<u8>> {
        if !self.inner.is_enabled() {
            return Err(RemoteSourceError::NotFound(key.to_string()));
        }

        // Step 1: 向 Master 申请获取权限 (acquire pull rights from master)
        let action = self.acquire_pull(key).await?;

        match action {
            proto::RemotePullAction::Pull => {
                // Step 2a: 本节点负责获取 (this node fetches from remote source)
                let result = self.inner.handle_miss(key).await;

                // 通知 Master 获取完成（成功或失败）
                // Notify master of completion
                let _ = self
                    .complete_pull(
                        key,
                        result.is_ok(),
                        result.as_ref().map_or(0, |d| d.len() as u64),
                    )
                    .await;

                // 通过回调让调用者将数据写入 store (PutStart/PutEnd)
                // Call the data callback so the caller can PutStart/PutEnd into the store
                if let Ok(ref data) = result {
                    data_callback(data);
                }

                result
            }
            proto::RemotePullAction::Wait => {
                // Step 2b: 另一个节点正在获取 → 等待重试
                // Another node is pulling — retry until they finish
                self.wait_and_retry(key).await
            }
            proto::RemotePullAction::Abandon => {
                // 远程源已禁用 (remote source disabled)
                Err(RemoteSourceError::NotFound(key.to_string()))
            }
        }
    }

    /// 向 Master 请求获取权限。
    /// (Ask the master for pull permission.)
    ///
    /// 将 client UUID 编入请求中，Master 据此判断是否有其他节点正在获取同一 key。
    async fn acquire_pull(&self, key: &str) -> RemoteSourceResult<proto::RemotePullAction> {
        let mut master = self.master.clone();
        let request = proto::AcquireRemotePullRequest {
            client_id: Some(proto::Uuid {
                high: self.client_id.as_u64_pair().0,
                low: self.client_id.as_u64_pair().1,
            }),
            key: key.to_string(),
            tenant_id: String::new(),
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

    /// 通知 Master 获取操作已完成。
    /// (Notify master that the pull completed — or failed.)
    ///
    /// Master 可以用此信息更新其状态，以便其他等待的节点知道数据已就绪。
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
                tenant_id: String::new(),
                success,
                data_size,
            })
            .await;
        Ok(())
    }

    /// 当另一节点正在获取时，轮询重试直到数据进入 store。
    /// (When another node is pulling, poll-retry the normal get path.)
    ///
    /// ## 重试策略 (Retry Strategy)
    /// - 指数退避: `backoff * 2^(attempt-1)`，上限 5 秒
    /// - 每次重试调用 `inner.handle_miss(key)` — 这会重新走热缓存 → 本地 store 路径
    /// - 获取成功后数据已在热缓存中，后续访问直接命中
    /// - NotFound 继续重试，其他错误直接返回
    ///
    /// The pulling node will have put data into the store via PutEnd,
    /// so subsequent GetReplicaList calls should find it.
    async fn wait_and_retry(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        let mut backoff = self.wait_backoff;
        for attempt in 1..=self.max_wait_retries {
            sleep(backoff).await;

            // 重试内部 handler — 如果获取节点已经将数据写入 store，
            // 这次调用应该能通过普通路径找到数据
            // Try the inner handler again — if the pulling node already stored
            // the data, this will find it (via normal RDMA read or local cache).
            match self.inner.handle_miss(key).await {
                Ok(data) => return Ok(data),
                Err(RemoteSourceError::NotFound(_)) => {
                    // 数据尚未就绪 → 继续等待
                    // Not yet available — keep waiting
                }
                Err(e) => return Err(e),
            }

            // 指数退避，上限 5 秒
            // Exponential backoff with 5-second cap
            backoff = (backoff * 2).min(Duration::from_secs(5));
            tracing::debug!(key = %key, attempt, "waiting for remote pull by another node");
        }

        // 最后一次尝试 (last attempt via the inner handler)
        self.inner.handle_miss(key).await
    }

    /// 获取内部 MissHandler 的引用。
    /// (Access the inner MissHandler and source.)
    pub fn inner(&self) -> &MissHandler<S> {
        &self.inner
    }
}
