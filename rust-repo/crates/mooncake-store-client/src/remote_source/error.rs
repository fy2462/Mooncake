use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, thiserror::Error)]
pub enum RemoteSourceError {
    #[error("key not found: {0}")]
    NotFound(String),

    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),

    #[error("timeout after {0:?}")]
    Timeout(Duration),

    #[error("rate limited")]
    RateLimited,

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for RemoteSourceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(Arc::new(e))
    }
}

pub type RemoteSourceResult<T> = Result<T, RemoteSourceError>;
