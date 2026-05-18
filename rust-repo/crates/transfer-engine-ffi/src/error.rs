#[derive(Debug, thiserror::Error)]
pub enum TransferEngineError {
    #[error("Transfer Engine returned null handle")]
    NullHandle,

    #[error("Transfer Engine operation failed with code {0}")]
    OperationFailed(i32),

    #[error("Failed to install transport protocol")]
    InstallTransportFailed,

    #[error("Invalid status code: {0}")]
    InvalidStatus(i32),

    #[error("Invalid opcode: {0}")]
    InvalidOpcode(i32),

    #[error("Null pointer")]
    NullPointer,

    #[error("UTF-8 error: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("NUL byte in C string: {0}")]
    NulError(#[from] std::ffi::NulError),

    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
}

pub type TransferEngineResult<T> = Result<T, TransferEngineError>;
