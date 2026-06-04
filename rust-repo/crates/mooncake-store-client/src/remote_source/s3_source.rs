//! # S3 Remote Source — AWS S3 远程源
//!
//! 将 AWS S3（或 MinIO 等兼容存储）作为远程数据源。
//! (Uses AWS S3 — or compatible stores like MinIO — as a remote data source.)
//!
//! ## 条件编译 (Conditional Compilation)
//! 此模块仅在启用 `s3` feature flag 时编译。
//! (This module only compiles when the `s3` feature flag is enabled.)
//!
//! ## Key 映射 (Key Mapping)
//! S3 object key = `{prefix}{key}`
//! - `prefix` 从配置中读取（如 `"cache/"`），不会自动追加 `/`
//! - 若 `prefix` 为空，则直接使用 key 作为 object key
//!
//! ## 凭证优先级 (Credential Resolution)
//! 1. 显式配置的 `access_key_id` / `secret_access_key`
//! 2. 环境变量: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
//! 3. IAM 实例角色 / `~/.aws/credentials`
//!
//! ## 并发策略 (Concurrency Strategy)
//! `prefetch_keys` 使用 `tokio::task::JoinSet` 实现有界并发（默认最多 8 个并行请求）。

use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::config::{Credentials, Region};

use super::config::S3Config;
use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};

/// 从 AWS S3（或兼容存储）获取对象的远程源。
/// (Fetches objects from AWS S3 or compatible stores like MinIO.)
///
/// ## 字段 (Fields)
/// - `client`: AWS S3 SDK 客户端
/// - `bucket`: 存储桶名称
/// - `prefix`: key 前缀，拼接到每个 key 之前
/// - `request_timeout`: 每个 GetObject 请求的超时时间（默认 30 秒）
///
/// ## Key mapping: `{prefix}{key}`
/// The optional `prefix` acts as a directory within the bucket.
/// A trailing `/` is NOT automatically appended.
///
/// # Credentials (in priority order)
/// 1. `access_key_id` / `secret_access_key` from [`S3Config`]
/// 2. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
/// 3. IAM instance profile / `~/.aws/credentials`
pub struct S3RemoteSource {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    /// 单个 GetObject 请求的超时时间 (timeout for individual GetObject requests)
    request_timeout: Duration,
}

impl S3RemoteSource {
    /// 从给定的 S3Config 构建 S3 客户端。
    /// (Build an S3 client from the given config.)
    ///
    /// ## 配置优先级 (Configuration Resolution)
    /// - 若提供了 `access_key_id` + `secret_access_key`，使用显式凭证
    /// - 若提供了 `endpoint`，使用自定义端点 URL + path-style 访问（适配 MinIO）
    /// - 否则使用 AWS SDK 默认凭证链
    pub async fn new(config: &S3Config) -> RemoteSourceResult<Self> {
        let region = Region::new(config.region.clone());

        let mut sdk_config =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

        // Apply explicit credentials when provided
        // 若显式提供了凭证，则覆盖默认凭证链
        if let (Some(key), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
            let credentials = Credentials::new(
                key.clone(),
                secret.clone(),
                None, // session token
                None, // expiry
                "mooncake-s3",
            );
            sdk_config = sdk_config.credentials_provider(credentials);
        }

        // Custom endpoint for S3-compatible stores (e.g. MinIO, Ceph RGW)
        // 自定义端点用于 MinIO / Ceph RGW 等兼容存储
        let is_custom_endpoint = config.endpoint.is_some();
        if let Some(ref endpoint) = config.endpoint {
            sdk_config = sdk_config.endpoint_url(endpoint);
        }

        let sdk_config = sdk_config.load().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if is_custom_endpoint {
            // 非 AWS 端点通常需要 path-style 访问（而非 virtual-hosted style）
            // Non-AWS endpoints typically require path-style access
            s3_config_builder = s3_config_builder.force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            request_timeout: Duration::from_secs(30),
        })
    }

    /// 设置自定义请求超时（默认 30 秒）。
    /// (Set a custom request timeout — default is 30 seconds.)
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// 将逻辑 key 转换为 S3 object key：`{prefix}{key}`。
    /// (Returns the S3 object key for a given logical key: `{prefix}{key}`.)
    fn object_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{key}", self.prefix)
        }
    }

    /// 读取 GetObject 响应的完整 body 到 `Vec<u8>`。
    /// (Read the full body of a GetObject response into Vec<u8>.)
    async fn read_body(
        output: aws_sdk_s3::operation::get_object::GetObjectOutput,
    ) -> RemoteSourceResult<Vec<u8>> {
        let body = output.body;
        let data = body.collect().await.map_err(|e| {
            RemoteSourceError::Internal(format!("failed to read S3 response body: {e}"))
        })?;
        Ok(data.to_vec())
    }
}

#[async_trait]
impl RemoteSource for S3RemoteSource {
    /// 从 S3 获取单个 key。
    /// (Fetch a single key from S3.)
    ///
    /// ## 错误处理 (Error Handling)
    /// - 超时 → `Timeout`
    /// - key 不存在 (NoSuchKey) → `NotFound`
    /// - 其他 S3 错误 → `Internal`
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        let object_key = self.object_key(key);

        let result = tokio::time::timeout(self.request_timeout, async {
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&object_key)
                .send()
                .await
        })
        .await
        .map_err(|_| RemoteSourceError::Timeout(self.request_timeout))?;

        match result {
            Ok(output) => Self::read_body(output).await,
            Err(err) => {
                let service_err = err.into_service_error();
                if service_err.is_no_such_key() {
                    Err(RemoteSourceError::NotFound(key.to_string()))
                } else {
                    Err(RemoteSourceError::Internal(format!(
                        "S3 GetObject failed for {object_key}: {service_err}"
                    )))
                }
            }
        }
    }

    /// 批量获取：使用 `tokio::task::JoinSet` 实现有界并发。
    /// (Batch fetch: issues parallel GetObject calls with bounded concurrency.)
    ///
    /// ## 并发控制 (Concurrency Control)
    /// 默认最多 8 个并行请求。每完成一个请求立即启动下一个，
    /// 确保同时处于飞行状态的请求数不超过 `max_concurrent`。
    async fn prefetch_keys(&self, keys: &[String]) -> Vec<RemoteSourceResult<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let max_concurrent = 8usize;
        let futs: Vec<_> = keys
            .iter()
            .map(|key| {
                let object_key = self.object_key(key);
                let bucket = self.bucket.clone();
                let client = self.client.clone();
                let timeout = self.request_timeout;
                let key_clone = key.clone();
                async move {
                    // 带超时的 S3 GetObject 请求 (GetObject with timeout)
                    let result = tokio::time::timeout(timeout, async {
                        client
                            .get_object()
                            .bucket(&bucket)
                            .key(&object_key)
                            .send()
                            .await
                    })
                    .await;

                    match result {
                        Ok(Ok(output)) => Self::read_body(output).await,
                        Ok(Err(err)) => {
                            let service_err = err.into_service_error();
                            if service_err.is_no_such_key() {
                                Err(RemoteSourceError::NotFound(key_clone))
                            } else {
                                Err(RemoteSourceError::Internal(format!(
                                    "S3 GetObject failed: {service_err}"
                                )))
                            }
                        }
                        Err(_) => Err(RemoteSourceError::Timeout(timeout)),
                    }
                }
            })
            .collect();

        // Execute with bounded concurrency using tokio JoinSet
        // 使用 JoinSet 执行有界并发：最多 max_concurrent 个任务同时运行
        let mut results = Vec::with_capacity(futs.len());
        let mut set = tokio::task::JoinSet::new();
        for fut in futs {
            set.spawn(fut);
            if set.len() >= max_concurrent {
                if let Some(res) = set.join_next().await {
                    results.push(res.unwrap_or(Err(RemoteSourceError::Internal(
                        "prefetch task panicked".to_string(),
                    ))));
                }
            }
        }
        // 等待剩余任务完成 (drain remaining tasks)
        while let Some(res) = set.join_next().await {
            results.push(res.unwrap_or(Err(RemoteSourceError::Internal(
                "prefetch task panicked".to_string(),
            ))));
        }
        results
    }
}
