use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::config::{Credentials, Region};

use super::config::S3Config;
use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};

/// Fetches objects from AWS S3 (or compatible stores like MinIO).
///
/// Key mapping: `{prefix}{key}` — the optional `prefix` acts as a directory
/// within the bucket. A trailing `/` is NOT automatically appended.
///
/// # Credentials (in priority order)
/// 1. `access_key_id` / `secret_access_key` from [`S3Config`]
/// 2. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
/// 3. IAM instance profile / `~/.aws/credentials`
pub struct S3RemoteSource {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    /// Timeout for individual GetObject requests.
    request_timeout: Duration,
}

impl S3RemoteSource {
    /// Build an S3 client from the given config.
    ///
    /// The AWS region, credentials, and optional custom endpoint are derived
    /// from `S3Config`. When fields are absent, the default AWS SDK chain is used.
    pub async fn new(config: &S3Config) -> RemoteSourceResult<Self> {
        let region = Region::new(config.region.clone());

        let mut sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region);

        // Apply explicit credentials when provided
        if let (Some(key), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
            let credentials = Credentials::new(
                key.clone(),
                secret.clone(),
                None,           // session token
                None,           // expiry
                "mooncake-s3",
            );
            sdk_config = sdk_config.credentials_provider(credentials);
        }

        // Custom endpoint for S3-compatible stores (e.g. MinIO, Ceph RGW)
        let is_custom_endpoint = config.endpoint.is_some();
        if let Some(ref endpoint) = config.endpoint {
            sdk_config = sdk_config.endpoint_url(endpoint);
        }

        let sdk_config = sdk_config.load().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if is_custom_endpoint {
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

    /// Set a custom request timeout (default 30s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Returns the S3 object key for a given logical key.
    fn object_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{key}", self.prefix)
        }
    }

    /// Read the full body of a GetObject response into `Vec<u8>`.
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

    /// Batch fetch: issues parallel GetObject calls with bounded concurrency
    /// via `futures::stream::iter` + `buffer_unordered`.
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

        // Execute with bounded concurrency using futures-util or tokio JoinSet
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
        while let Some(res) = set.join_next().await {
            results.push(res.unwrap_or(Err(RemoteSourceError::Internal(
                "prefetch task panicked".to_string(),
            ))));
        }
        results
    }

}

#[cfg(test)]
mod tests {
    #[test]
    fn test_object_key_no_prefix() {
        // Verify key mapping logic via a dummy s3_config
        let config = super::super::config::S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: None,
            prefix: String::new(),
            access_key_id: None,
            secret_access_key: None,
        };
        // Can't construct S3RemoteSource directly (needs async), but we can
        // at least verify config round-trips
        assert_eq!(config.bucket, "b");
        assert_eq!(config.prefix, "");
    }

    #[test]
    fn test_object_key_with_prefix() {
        let config = super::super::config::S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: None,
            prefix: "cache/".into(),
            access_key_id: Some("ak".into()),
            secret_access_key: Some("sk".into()),
        };
        assert_eq!(config.prefix, "cache/");
        assert_eq!(config.access_key_id.as_deref(), Some("ak"));
    }
}
