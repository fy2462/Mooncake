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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_handle_display() {
        assert_eq!(TransferEngineError::NullHandle.to_string(), "Transfer Engine returned null handle");
    }

    #[test]
    fn test_operation_failed_display() {
        assert_eq!(TransferEngineError::OperationFailed(42).to_string(), "Transfer Engine operation failed with code 42");
        assert_eq!(TransferEngineError::OperationFailed(-1).to_string(), "Transfer Engine operation failed with code -1");
        assert_eq!(TransferEngineError::OperationFailed(0).to_string(), "Transfer Engine operation failed with code 0");
    }

    #[test]
    fn test_install_transport_failed_display() {
        assert_eq!(TransferEngineError::InstallTransportFailed.to_string(), "Failed to install transport protocol");
    }

    #[test]
    fn test_invalid_status_display() {
        assert_eq!(TransferEngineError::InvalidStatus(99).to_string(), "Invalid status code: 99");
    }

    #[test]
    fn test_invalid_opcode_display() {
        assert_eq!(TransferEngineError::InvalidOpcode(7).to_string(), "Invalid opcode: 7");
    }

    #[test]
    fn test_null_pointer_display() {
        assert_eq!(TransferEngineError::NullPointer.to_string(), "Null pointer");
    }

    #[test]
    fn test_debug_format() {
        let err = TransferEngineError::NullHandle;
        assert!(format!("{:?}", err).contains("NullHandle"));
        let err = TransferEngineError::OperationFailed(-3);
        assert!(format!("{:?}", err).contains("OperationFailed"));
    }

    #[test]
    #[allow(invalid_from_utf8)]
    fn test_from_utf8_error() {
        let invalid_utf8: [u8; 1] = [0x80];
        let err = std::str::from_utf8(&invalid_utf8).unwrap_err();
        let te_err: TransferEngineError = err.into();
        assert!(te_err.to_string().contains("UTF-8 error"));
    }

    #[test]
    fn test_from_nul_error() {
        let err = std::ffi::CString::new(b"hel\0lo" as &[u8]).unwrap_err();
        let te_err: TransferEngineError = err.into();
        assert!(te_err.to_string().contains("NUL byte"));
    }

    #[test]
    fn test_from_try_from_int_error() {
        let result: Result<u8, _> = 256i32.try_into();
        let err = result.unwrap_err();
        let te_err: TransferEngineError = err.into();
        assert!(te_err.to_string().contains("Integer conversion error"));
    }

    #[test]
    fn test_result_type() {
        let ok: TransferEngineResult<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: TransferEngineResult<i32> = Err(TransferEngineError::NullPointer);
        assert!(err.is_err());
    }

    #[test]
    fn test_error_comparison() {
        assert!(matches!(TransferEngineError::NullHandle, TransferEngineError::NullHandle));
        assert!(matches!(TransferEngineError::OperationFailed(5), TransferEngineError::OperationFailed(5)));
        assert!(!matches!(TransferEngineError::OperationFailed(5), TransferEngineError::OperationFailed(6)));
    }
}
