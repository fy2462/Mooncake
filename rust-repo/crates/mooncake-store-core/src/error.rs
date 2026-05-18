use std::ffi::NulError;
use std::str::Utf8Error;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("operation failed with code {0}")]
    OperationFailed(i32),

    #[error("null handle returned")]
    NullHandle,

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("object already exists: {0}")]
    ObjectExists(String),

    #[error("no available storage handle")]
    NoAvailableHandle,

    #[error("replica is not ready")]
    ReplicaNotReady,

    #[error("invalid parameters: {0}")]
    InvalidParams(String),

    #[error("client not found: {0}")]
    ClientNotFound(String),

    #[error("segment not found: {0}")]
    SegmentNotFound(String),

    #[error("service unavailable")]
    ServiceUnavailable,

    #[error("etcd error: {0}")]
    EtcdError(String),

    #[error("redis error: {0}")]
    RedisError(String),

    #[error("K8s error: {0}")]
    K8sError(String),

    #[error("S3 error: {0}")]
    S3Error(String),

    #[cfg(feature = "transfer-engine")]
    #[error("transfer engine error: {0}")]
    TransferEngine(#[from] transfer_engine_ffi::TransferEngineError),

    #[error("UTF-8 error: {0}")]
    InvalidUtf8(#[from] Utf8Error),

    #[error("NUL byte in C string: {0}")]
    NulError(#[from] NulError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Internal(String),
}

pub type StoreResult<T> = Result<T, StoreError>;
