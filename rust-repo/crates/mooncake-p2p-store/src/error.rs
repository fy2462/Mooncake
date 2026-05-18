#[derive(Debug, thiserror::Error)]
pub enum P2pStoreError {
    #[error("invalid arguments")]
    InvalidArgument,

    #[error("payload already opened")]
    PayloadOpened,

    #[error("payload not opened")]
    PayloadNotOpened,

    #[error("payload not found")]
    PayloadNotFound,

    #[error("transfer engine error")]
    TransferEngine,

    #[error("metadata store error: {0}")]
    MetadataError(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
