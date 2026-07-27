//! Error types for the Transfer Engine FFI layer.
//! Transfer Engine FFI 层的错误类型。
//!
//! # Error Mapping Strategy / 错误映射策略
//!
//! The C++ Transfer Engine communicates errors through multiple channels:
//! - **Null pointer returns**: e.g., `createTransferEngine` returns NULL on failure.
//! - **Integer return codes**: most functions return 0 on success, non-zero on error.
//! - **Negative segment IDs**: `openSegment` returns a negative value on failure.
//!
//! These are unified into `TransferEngineError` variants for idiomatic Rust
//! error handling via `thiserror`.
//!
//! C++ Transfer Engine 通过多种渠道传递错误：
//! - **空指针返回**：如 `createTransferEngine` 失败时返回 NULL。
//! - **整数返回码**：大多数函数成功返回 0，失败返回非零值。
//! - **负数段 ID**：`openSegment` 失败时返回负数。
//!
//! 这些被统一为 `TransferEngineError` 变体，通过 `thiserror` 提供符合
//! Rust 习惯的错误处理。

#[derive(Debug, thiserror::Error)]
pub enum TransferEngineError {
    /// The C++ `createTransferEngine` returned a null pointer.
    /// This typically indicates a memory allocation failure or invalid
    /// connection parameters in the C++ layer.
    /// C++ `createTransferEngine` 返回了空指针。
    /// 通常表示 C++ 层内存分配失败或连接参数无效。
    #[error("Transfer Engine returned null handle")]
    NullHandle,

    /// A generic C++ operation failed with the given integer error code.
    /// The meaning of the code depends on the specific C++ function called.
    /// Most C API functions return 0 on success and a non-zero error code
    /// on failure. This variant captures that code for debugging.
    /// 通用 C++ 操作失败，携带整数错误码。
    /// 错误码的含义取决于调用的具体 C++ 函数。
    /// 大多数 C API 函数成功返回 0，失败返回非零错误码。
    /// 此变体捕获该错误码以供调试。
    #[error("Transfer Engine operation failed with code {0}")]
    OperationFailed(i32),

    /// A named native operation failed; preserves both ABI entry point and code.
    #[error("Transfer Engine operation {operation} failed with code {code}")]
    NativeOperationFailed { operation: &'static str, code: i32 },

    /// A batch handle was passed to a different engine instance.
    #[error("Transfer Engine batch belongs to a different engine instance")]
    BatchOwnershipMismatch,

    /// A batch handle was used after its native allocation was released.
    #[error("Transfer Engine batch was already released")]
    BatchAlreadyReleased,

    /// Native tasks in the batch are still accessing their payloads.
    #[error("Transfer Engine batch is still busy")]
    BatchBusy,

    /// The native engine returned an error that does not prove the batch has
    /// stopped accessing its registered payload.
    #[error("Transfer Engine batch quiescence could not be proven; native resources were retained")]
    QuiescenceUnproven,

    /// A typed memory registration was malformed or used outside its bounds.
    #[error("Invalid Transfer Engine memory registration: {0}")]
    InvalidMemoryRegistration(String),

    /// A typed memory registration belongs to a different engine instance.
    #[error("Transfer Engine memory registration belongs to a different engine instance")]
    MemoryRegistrationOwnershipMismatch,

    /// A typed memory registration was used after successful unregistration.
    #[error("Transfer Engine memory registration was already released")]
    MemoryRegistrationAlreadyReleased,

    /// Region leases still exist and may be used by native transfers.
    #[error(
        "Transfer Engine memory registration generation {generation} is busy with {active_leases} active region lease(s)"
    )]
    MemoryRegistrationBusy {
        generation: u64,
        active_leases: usize,
    },

    /// A safe registered transfer already owns the registration's exclusive
    /// native-access claim.
    #[error(
        "Transfer Engine memory registration generation {generation} already has an in-flight registered transfer"
    )]
    MemoryRegistrationInFlight { generation: u64 },

    /// Native notification metadata was internally inconsistent.
    #[error("Invalid native notification buffer: {0}")]
    InvalidNotificationBuffer(&'static str),

    /// A native NIC statistic did not contain a terminated device name.
    #[error("Invalid NIC device name: {0}")]
    InvalidDeviceName(&'static str),

    /// The native count kept changing and no stable snapshot could be read.
    #[error("Transfer Engine operation {0} returned an unstable result count")]
    UnstableResultCount(&'static str),

    /// `installTransport` returned a null transport handle.
    /// This occurs when the requested transport protocol (e.g., RDMA) is
    /// not available or when the topology matrix is malformed.
    /// `installTransport` 返回了空传输句柄。
    /// 当请求的传输协议（如 RDMA）不可用或拓扑矩阵格式错误时发生。
    #[error("Failed to install transport protocol")]
    InstallTransportFailed,

    /// An unknown or invalid transfer status code was received from the C++ layer.
    /// The raw integer value is preserved for debugging.
    /// 从 C++ 层收到了未知或无效的传输状态码。
    /// 保留原始整数值以供调试。
    #[error("Invalid status code: {0}")]
    InvalidStatus(i32),

    /// An unknown or invalid opcode value was encountered when converting
    /// from the C++ integer representation to the Rust `Opcode` enum.
    /// 从 C++ 整数表示转换为 Rust `Opcode` 枚举时遇到了未知或无效的操作码值。
    #[error("Invalid opcode: {0}")]
    InvalidOpcode(i32),

    /// A required pointer argument was null.
    /// 必需的指针参数为空。
    #[error("Null pointer")]
    NullPointer,

    /// A string received from the C++ layer contained invalid UTF-8.
    /// This can happen if the C++ code passes raw bytes that are not
    /// valid UTF-8 sequences.
    /// 从 C++ 层接收的字符串包含无效的 UTF-8。
    /// 当 C++ 代码传递了非有效 UTF-8 序列的原始字节时可能发生。
    #[error("UTF-8 error: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    /// A CString could not be created because the input contained a NUL byte.
    /// C strings are NUL-terminated; interior NUL bytes are not allowed.
    /// 无法创建 CString，因为输入包含 NUL 字节。
    /// C 字符串以 NUL 结尾；不允许字符串内部出现 NUL 字节。
    #[error("NUL byte in C string: {0}")]
    NulError(#[from] std::ffi::NulError),

    /// An integer conversion failed (e.g., usize to u32 on a 64-bit platform
    /// where the value exceeds u32::MAX).
    /// 整数转换失败（如在 64 位平台上将超过 u32::MAX 的值转为 u32）。
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
}

/// Result type alias for Transfer Engine operations.
/// Transfer Engine 操作的 Result 类型别名。
///
/// All fallible Transfer Engine methods return this type.
/// 所有可能失败的 Transfer Engine 方法都返回此类型。
pub type TransferEngineResult<T> = Result<T, TransferEngineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_handle_display() {
        assert_eq!(
            TransferEngineError::NullHandle.to_string(),
            "Transfer Engine returned null handle"
        );
    }

    #[test]
    fn test_operation_failed_display() {
        assert_eq!(
            TransferEngineError::OperationFailed(42).to_string(),
            "Transfer Engine operation failed with code 42"
        );
        assert_eq!(
            TransferEngineError::OperationFailed(-1).to_string(),
            "Transfer Engine operation failed with code -1"
        );
        assert_eq!(
            TransferEngineError::OperationFailed(0).to_string(),
            "Transfer Engine operation failed with code 0"
        );
    }

    #[test]
    fn test_install_transport_failed_display() {
        assert_eq!(
            TransferEngineError::InstallTransportFailed.to_string(),
            "Failed to install transport protocol"
        );
    }

    #[test]
    fn test_invalid_status_display() {
        assert_eq!(
            TransferEngineError::InvalidStatus(99).to_string(),
            "Invalid status code: 99"
        );
    }

    #[test]
    fn test_invalid_opcode_display() {
        assert_eq!(
            TransferEngineError::InvalidOpcode(7).to_string(),
            "Invalid opcode: 7"
        );
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
        assert!(matches!(
            TransferEngineError::NullHandle,
            TransferEngineError::NullHandle
        ));
        assert!(matches!(
            TransferEngineError::OperationFailed(5),
            TransferEngineError::OperationFailed(5)
        ));
        assert!(!matches!(
            TransferEngineError::OperationFailed(5),
            TransferEngineError::OperationFailed(6)
        ));
    }
}
