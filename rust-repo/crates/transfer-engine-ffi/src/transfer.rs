//! Transfer types for the Transfer Engine FFI layer.
//! Transfer Engine FFI 层的传输类型。
//!
//! # Key Types Overview / 关键类型概述
//!
//! | Type | Purpose / 用途 |
//! |------|---------------|
//! | `BatchId` | Legacy copyable batch token / 旧版可复制批次令牌 |
//! | `OwnedBatchId` | Engine-bound owned batch allocation / 绑定引擎的批次所有权 |
//! | `Opcode` | Read from remote or Write to remote / 从远程读取或写入远程 |
//! | `TransferRequest` | A single read/write operation descriptor / 单个读/写操作描述符 |
//! | `TransferStatusEnum` | Current state of a transfer task / 传输任务的当前状态 |
//! | `TransferStatus` | Status + bytes transferred / 状态 + 已传输字节数 |
//! | `NotifyMsg` | Out-of-band notification between peers / 节点间带外通知 |
//!
//! # Batch Semantics / 批次语义
//!
//! Transfers in Mooncake are batched:
//! 1. **allocate_batch_id(batch_size)** — reserve a batch slot of N requests.
//!    预留 N 个请求的批次槽位。
//! 2. **submit_transfer(batch_id, requests)** — enqueue requests atomically.
//!    原子性地将请求入队。
//! 3. **get_transfer_status(batch_id, task_id)** — poll individual tasks by index.
//!    按索引轮询单个任务。
//! 4. **free_batch_id(batch_id)** — release after all tasks reach terminal state.
//!    所有任务达到终态后释放。
//!
//! A batch is an ordered container: each request gets a task_id equal to its
//! position in the submitted slice (0-indexed). You must poll each task
//! individually. The batch is not freed until all tasks complete.
//! 批次是一个有序容器：每个请求获得与其在提交切片中位置相等的 task_id（从 0 开始）。
//! 必须单独轮询每个任务。直到所有任务完成才能释放批次。

use std::ffi::c_void;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::ffi;
use crate::segment::SegmentId;
use crate::{TransferEngineError, TransferEngineResult};

/// Legacy copyable Transfer Engine batch token.
///
/// This type remains for source compatibility with callers outside the Rust
/// Store replacement. New Store code must use [`OwnedBatchId`], which binds the
/// native allocation to one engine and prevents safe duplication/double-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchId(pub u64);

/// Owned wrapper for a Transfer Engine batch allocation.
/// Transfer Engine 批次分配的所有权封装。
///
/// A batch groups multiple transfer requests that are submitted together.
/// The C++ engine allocates internal resources for each batch. You must
/// call `free_batch_id` after all transfers in the batch complete.
/// 批次将多个传输请求分组并一起提交。
/// C++ 引擎为每个批次分配内部资源。必须等批次中所有传输完成后调用 `free_batch_id`。
///
/// The native value is deliberately private and this handle is neither
/// `Copy` nor `Clone`: in the classic engine it is an encoded native pointer.
/// Safe Rust therefore cannot forge a batch or duplicate ownership and free
/// the same native allocation twice.
///
/// 原生值刻意保持私有，且此句柄既不是 `Copy` 也不是 `Clone`：在 classic
/// engine 中它实际编码了原生指针。因此 safe Rust 无法伪造批次，也无法复制
/// 所有权后重复释放同一原生对象。
#[derive(Debug)]
pub struct OwnedBatchId {
    raw: u64,
    owner_id: u64,
    released: bool,
}

static NEXT_ENGINE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_engine_instance_id() -> u64 {
    NEXT_ENGINE_INSTANCE_ID.fetch_add(1, Ordering::Relaxed)
}

impl OwnedBatchId {
    pub(crate) fn allocated(raw: u64, owner_id: u64) -> Self {
        Self {
            raw,
            owner_id,
            released: false,
        }
    }

    /// Return the opaque native value for diagnostics or foreign-language
    /// tokenization. It cannot be converted back into an `OwnedBatchId` by safe
    /// Rust.
    pub fn as_raw(&self) -> u64 {
        self.raw
    }

    /// Whether the native batch allocation has already been released.
    pub fn is_released(&self) -> bool {
        self.released
    }

    pub(crate) fn validate_for(&self, owner_id: u64) -> TransferEngineResult<u64> {
        if self.owner_id != owner_id {
            return Err(TransferEngineError::BatchOwnershipMismatch);
        }
        if self.released {
            return Err(TransferEngineError::BatchAlreadyReleased);
        }
        Ok(self.raw)
    }

    pub(crate) fn mark_released(&mut self) {
        self.released = true;
    }
}

/// Transfer operation type: read from remote or write to remote.
/// 传输操作类型：从远程读取或写入远程。
///
/// # Read (Opcode::Read)
///
/// Pull data from a remote segment into local memory. The source buffer
/// will be filled with data from `target_id` at `target_offset`.
/// 从远程段拉取数据到本地内存。源缓冲区将被来自 `target_id` 在 `target_offset`
/// 处的数据填充。
///
/// ```text
///   local memory (source) ◄── RDMA READ ── remote segment (target_id, target_offset)
/// ```
///
/// # Write (Opcode::Write)
///
/// Push data from local memory to a remote segment. Data at `source`
/// is written to `target_id` at `target_offset`.
/// 将数据从本地内存推送到远程段。`source` 处的数据被写入到 `target_id` 的
/// `target_offset` 处。
///
/// ```text
///   local memory (source) ──► RDMA WRITE ── remote segment (target_id, target_offset)
/// ```
///
/// # RDMA/TCP Buffer Semantics / RDMA/TCP 缓冲区语义
///
/// For both Read and Write:
/// - **source**: A local memory pointer. Must point to registered memory.
///   本地内存指针。必须指向已注册的内存。
/// - **target_id**: The segment ID of the remote peer (from `open_segment`).
///   远程节点的段 ID（来自 `open_segment`）。
/// - **target_offset**: Byte offset within the remote segment.
///   远程段内的字节偏移量。
/// - **length**: Number of bytes to transfer.
///   要传输的字节数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Opcode {
    /// Read data from remote segment into local buffer.
    /// 从远程段读取数据到本地缓冲区。
    Read = ffi::OPCODE_READ as i32,
    /// Write data from local buffer to remote segment.
    /// 将数据从本地缓冲区写入远程段。
    Write = ffi::OPCODE_WRITE as i32,
}

impl Opcode {
    /// Convert a raw C integer to an `Opcode`.
    /// Returns `None` if the value does not match a known opcode.
    /// 将原始 C 整数转换为 `Opcode`。
    /// 如果值与已知操作码不匹配，返回 `None`。
    pub fn from_i32(v: i32) -> Option<Self> {
        match v as u32 {
            ffi::OPCODE_READ => Some(Self::Read),
            ffi::OPCODE_WRITE => Some(Self::Write),
            _ => None,
        }
    }
}

/// A notification message exchanged between transfer engine peers.
/// 传输引擎节点间交换的通知消息。
///
/// Notifications provide an out-of-band communication channel. They can
/// be attached to transfers via `submit_transfer_with_notify` or sent
/// standalone via `gen_notify_in_engine`.
/// 通知提供了带外通信通道。可通过 `submit_transfer_with_notify` 附加到传输中，
/// 或通过 `gen_notify_in_engine` 独立发送。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyMsg {
    /// Name of the sender or target (used for routing).
    /// 发送者或目标的名称（用于路由）。
    pub name: String,
    /// The notification payload (arbitrary string).
    /// 通知负载（任意字符串）。
    pub msg: String,
}

/// An owned buffer of notification messages received from the engine.
/// 从引擎接收的通知消息的拥有缓冲区。
///
/// Returned by `get_notifs_from_engine`. The underlying C buffer is
/// freed after the Rust types are constructed.
/// 由 `get_notifs_from_engine` 返回。底层 C 缓冲区在构造 Rust 类型后释放。
#[derive(Debug)]
pub struct NotifyMsgBuf {
    pub messages: Vec<NotifyMsg>,
}

/// A single transfer request within a batch.
/// 批次中的单个传输请求。
///
/// Each request describes one RDMA/TCP read or write operation.
/// The fields map directly to the C `transfer_request_t` struct.
/// 每个请求描述一个 RDMA/TCP 读或写操作。
/// 字段直接映射到 C 的 `transfer_request_t` 结构体。
///
/// # Field Semantics / 字段语义
///
/// - **opcode**: `Read` to pull data, `Write` to push data.
///   `Read` 拉取数据，`Write` 推送数据。
/// - **source**: Local memory pointer (must be registered). For Read, this is
///   the destination buffer; for Write, it is the data source.
///   本地内存指针（必须已注册）。对于读取，这是目标缓冲区；对于写入，这是数据源。
/// - **target_id**: Remote segment ID obtained via `open_segment`.
///   通过 `open_segment` 获取的远程段 ID。
/// - **target_offset**: Byte offset within the remote segment to read from or write to.
///   远程段内要读取或写入的字节偏移量。
/// - **length**: Number of bytes to transfer. Must not exceed the registered
///   memory region size on either side.
///   要传输的字节数。不得超过任一侧注册的内存区域大小。
#[derive(Debug, Clone)]
pub struct TransferRequest {
    pub opcode: Opcode,
    pub source: *mut c_void,
    pub target_id: SegmentId,
    pub target_offset: u64,
    pub length: u64,
}

// SAFETY: TransferRequest holds raw pointers but is only used as an ephemeral
// descriptor passed to the C API. The actual memory safety is guaranteed by
// the caller ensuring the pointed-to memory remains valid during the transfer.
// 安全性：TransferRequest 持有原始指针，但仅作为传递给 C API 的临时描述符使用。
// 实际的内存安全由调用者保证，调用者需确保指向的内存在传输期间保持有效。
unsafe impl Send for TransferRequest {}
unsafe impl Sync for TransferRequest {}

/// Transfer task status codes.
/// 传输任务状态码。
///
/// These mirror the C++ `transfer_status_code_t` enum. Each code represents
/// a stage in the lifecycle of a transfer task.
/// 这些映射 C++ 的 `transfer_status_code_t` 枚举。每个代码表示传输任务生命周期中的一个阶段。
///
/// # State Machine / 状态机
///
/// ```text
///                       ┌──────────┐
///                       │  WAITING │  (initial state / 初始状态)
///                       └────┬─────┘
///                            │ dispatched / 已调度
///                       ┌────▼─────┐
///                       │  PENDING │  (in progress / 进行中)
///                       └────┬─────┘
///              ┌─────────────┼──────────────┐
///              ▼             ▼              ▼
///        ┌──────────┐ ┌──────────┐   ┌──────────┐
///        │COMPLETED │ │  FAILED  │   │ CANCELED │
///        └──────────┘ └──────────┘   └──────────┘
///              │             │              │
///              ▼             ▼              ▼
///              ┌─────────────────────────────┐
///              │         TIMEOUT             │
///              └─────────────────────────────┘
/// ```
///
/// Terminal states (终态): `Completed`, `Failed`, `Canceled`, `Timeout`.
/// Non-terminal states (非终态): `Waiting`, `Pending`, `Invalid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TransferStatusEnum {
    /// Task has been created but not yet dispatched.
    /// 任务已创建但尚未调度。
    Waiting = ffi::STATUS_WAITING as i32,
    /// Task has been dispatched and is being processed by the transport layer.
    /// 任务已调度，正在由传输层处理。
    Pending = ffi::STATUS_PENDING as i32,
    /// Invalid task (e.g., invalid batch_id or task_id).
    /// 无效任务（如无效的 batch_id 或 task_id）。
    Invalid = ffi::STATUS_INVALID as i32,
    /// Task was canceled before completion.
    /// 任务在完成前被取消。
    Canceled = ffi::STATUS_CANCELED as i32,
    /// Transfer completed successfully.
    /// 传输成功完成。
    Completed = ffi::STATUS_COMPLETED as i32,
    /// Transfer timed out.
    /// 传输超时。
    Timeout = ffi::STATUS_TIMEOUT as i32,
    /// Transfer failed due to an error (connection loss, memory error, etc.).
    /// 传输因错误而失败（连接丢失、内存错误等）。
    Failed = ffi::STATUS_FAILED as i32,
}

impl TransferStatusEnum {
    /// Convert a raw C integer to a `TransferStatusEnum`.
    /// Unknown values default to `Invalid`.
    /// 将原始 C 整数转换为 `TransferStatusEnum`。
    /// 未知值默认为 `Invalid`。
    pub fn from_i32(v: i32) -> Self {
        match v as u32 {
            ffi::STATUS_WAITING => Self::Waiting,
            ffi::STATUS_PENDING => Self::Pending,
            ffi::STATUS_INVALID => Self::Invalid,
            ffi::STATUS_CANCELED => Self::Canceled,
            ffi::STATUS_COMPLETED => Self::Completed,
            ffi::STATUS_TIMEOUT => Self::Timeout,
            ffi::STATUS_FAILED => Self::Failed,
            _ => Self::Invalid,
        }
    }

    /// Returns `true` if this status is terminal.
    /// 如果此状态是终态，返回 `true`。
    ///
    /// Terminal states mean the transfer has finished (successfully or not).
    /// The polling loop should exit when this returns `true`.
    /// 终态表示传输已结束（无论成功与否）。
    /// 当此方法返回 `true` 时，轮询循环应退出。
    ///
    /// Non-terminal states (`Waiting`, `Pending`, `Invalid`) require
    /// continued polling to determine the final outcome.
    /// 非终态（`Waiting`、`Pending`、`Invalid`）需要继续轮询以确定最终结果。
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Timeout
        )
    }
}

/// Result of polling a single transfer within a batch.
/// 轮询批次中单个传输的结果。
///
/// Returned by `get_transfer_status`. Contains both the current state
/// and a progress indicator.
/// 由 `get_transfer_status` 返回。包含当前状态和进度指示器。
#[derive(Debug, Clone)]
pub struct TransferStatus {
    /// Current status of the transfer task.
    /// 传输任务的当前状态。
    pub status: TransferStatusEnum,
    /// Number of bytes transferred so far. For completed transfers, this
    /// should equal the requested `length`. For in-progress transfers,
    /// this represents partial progress.
    /// 到目前为止已传输的字节数。对于已完成的传输，此值应等于请求的 `length`。
    /// 对于进行中的传输，表示部分进度。
    pub transferred_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // BatchId
    // =========================================================================

    #[test]
    fn test_legacy_batch_id_remains_copyable_for_external_callers() {
        let id = BatchId(42);
        let copied = id;
        assert_eq!(id, copied);
    }

    #[test]
    fn test_legacy_and_owned_batch_api_shapes_remain_distinct() {
        let _: fn(&crate::TransferEngine, usize) -> TransferEngineResult<BatchId> =
            crate::TransferEngine::allocate_batch_id;
        let _: fn(&crate::TransferEngine, usize) -> TransferEngineResult<OwnedBatchId> =
            crate::TransferEngine::allocate_owned_batch_id;
        let _: unsafe fn(
            &crate::TransferEngine,
            &OwnedBatchId,
            &[TransferRequest],
        ) -> TransferEngineResult<()> = crate::TransferEngine::submit_owned_transfer;
    }

    #[test]
    fn test_batch_id_exposes_only_opaque_raw_value() {
        let id = OwnedBatchId::allocated(42, 7);
        assert_eq!(id.as_raw(), 42);
        assert_eq!(id.validate_for(7).unwrap(), 42);
    }

    #[test]
    fn test_batch_id_rejects_another_engine() {
        let id = OwnedBatchId::allocated(99, 7);
        assert!(matches!(
            id.validate_for(8),
            Err(TransferEngineError::BatchOwnershipMismatch)
        ));
    }

    #[test]
    fn test_batch_id_cannot_be_reused_after_release() {
        let mut id = OwnedBatchId::allocated(5, 7);
        id.mark_released();
        assert!(matches!(
            id.validate_for(7),
            Err(TransferEngineError::BatchAlreadyReleased)
        ));
    }

    // =========================================================================
    // Opcode
    // =========================================================================

    #[test]
    fn test_opcode_values() {
        assert_ne!(Opcode::Read, Opcode::Write);
    }

    #[test]
    fn test_opcode_from_i32_valid() {
        assert_eq!(Opcode::from_i32(Opcode::Read as i32), Some(Opcode::Read));
        assert_eq!(Opcode::from_i32(Opcode::Write as i32), Some(Opcode::Write));
    }

    #[test]
    fn test_opcode_from_i32_invalid() {
        assert_eq!(Opcode::from_i32(99), None);
        assert_eq!(Opcode::from_i32(-1), None);
        assert_eq!(Opcode::from_i32(2), None);
    }

    #[test]
    fn test_opcode_clone_copy() {
        let op = Opcode::Read;
        assert_eq!(op, op.clone());
        let op2 = op;
        assert_eq!(op, op2);
    }

    #[test]
    fn test_opcode_debug() {
        let s = format!("{:?}", Opcode::Read);
        assert!(!s.is_empty());
        let s = format!("{:?}", Opcode::Write);
        assert!(!s.is_empty());
    }

    // =========================================================================
    // TransferRequest
    // =========================================================================

    #[test]
    fn test_transfer_request_creation() {
        let req = TransferRequest {
            opcode: Opcode::Write,
            source: std::ptr::null_mut(),
            target_id: SegmentId(1),
            target_offset: 0x1000,
            length: 4096,
        };
        assert_eq!(req.opcode, Opcode::Write);
        assert_eq!(req.target_id, SegmentId(1));
        assert_eq!(req.target_offset, 0x1000);
        assert_eq!(req.length, 4096);
    }

    #[test]
    fn test_transfer_request_read() {
        let buf: u8 = 0;
        let req = TransferRequest {
            opcode: Opcode::Read,
            source: &buf as *const u8 as *mut c_void,
            target_id: SegmentId(7),
            target_offset: 0,
            length: 128,
        };
        assert_eq!(req.opcode, Opcode::Read);
        assert_eq!(req.source, &buf as *const u8 as *mut c_void);
        assert_eq!(req.target_id.0, 7);
    }

    #[test]
    fn test_transfer_request_clone() {
        let req = TransferRequest {
            opcode: Opcode::Read,
            source: 0x1000 as *mut c_void,
            target_id: SegmentId(3),
            target_offset: 64,
            length: 512,
        };
        let cloned = req.clone();
        assert_eq!(cloned.opcode, req.opcode);
        assert_eq!(cloned.target_id, req.target_id);
        assert_eq!(cloned.target_offset, req.target_offset);
        assert_eq!(cloned.length, req.length);
    }

    // =========================================================================
    // TransferStatusEnum
    // =========================================================================

    #[test]
    fn test_transfer_status_enum_from_i32_all() {
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Waiting as i32),
            TransferStatusEnum::Waiting
        );
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Pending as i32),
            TransferStatusEnum::Pending
        );
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Invalid as i32),
            TransferStatusEnum::Invalid
        );
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Canceled as i32),
            TransferStatusEnum::Canceled
        );
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Completed as i32),
            TransferStatusEnum::Completed
        );
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Timeout as i32),
            TransferStatusEnum::Timeout
        );
        assert_eq!(
            TransferStatusEnum::from_i32(TransferStatusEnum::Failed as i32),
            TransferStatusEnum::Failed
        );
    }

    #[test]
    fn test_transfer_status_enum_from_i32_unknown() {
        assert_eq!(
            TransferStatusEnum::from_i32(99),
            TransferStatusEnum::Invalid
        );
        assert_eq!(
            TransferStatusEnum::from_i32(-5),
            TransferStatusEnum::Invalid
        );
    }

    #[test]
    fn test_transfer_status_enum_is_terminal() {
        assert!(TransferStatusEnum::Completed.is_terminal());
        assert!(TransferStatusEnum::Failed.is_terminal());
        assert!(TransferStatusEnum::Canceled.is_terminal());
        assert!(TransferStatusEnum::Timeout.is_terminal());
        assert!(!TransferStatusEnum::Waiting.is_terminal());
        assert!(!TransferStatusEnum::Pending.is_terminal());
        assert!(!TransferStatusEnum::Invalid.is_terminal());
    }

    #[test]
    fn test_transfer_status_enum_clone_eq() {
        for status in &[
            TransferStatusEnum::Waiting,
            TransferStatusEnum::Pending,
            TransferStatusEnum::Completed,
            TransferStatusEnum::Failed,
        ] {
            assert_eq!(*status, status.clone());
        }
    }

    #[test]
    fn test_transfer_status_enum_debug() {
        assert!(format!("{:?}", TransferStatusEnum::Completed).contains("Completed"));
        assert!(format!("{:?}", TransferStatusEnum::Failed).contains("Failed"));
        assert!(format!("{:?}", TransferStatusEnum::Waiting).contains("Waiting"));
    }

    // =========================================================================
    // TransferStatus
    // =========================================================================

    #[test]
    fn test_transfer_status_completed() {
        let st = TransferStatus {
            status: TransferStatusEnum::Completed,
            transferred_bytes: 4096,
        };
        assert_eq!(st.status, TransferStatusEnum::Completed);
        assert_eq!(st.transferred_bytes, 4096);
        assert!(st.status.is_terminal());
    }

    #[test]
    fn test_transfer_status_waiting() {
        let st = TransferStatus {
            status: TransferStatusEnum::Waiting,
            transferred_bytes: 0,
        };
        assert_eq!(st.transferred_bytes, 0);
        assert!(!st.status.is_terminal());
    }

    #[test]
    fn test_transfer_status_invalid() {
        let st = TransferStatus {
            status: TransferStatusEnum::Invalid,
            transferred_bytes: 0,
        };
        assert_eq!(st.status, TransferStatusEnum::Invalid);
        assert!(!st.status.is_terminal());
    }

    #[test]
    fn test_transfer_status_debug() {
        let st = TransferStatus {
            status: TransferStatusEnum::Completed,
            transferred_bytes: 1024,
        };
        let s = format!("{:?}", st);
        assert!(s.contains("Completed"));
        assert!(s.contains("1024"));
    }

    #[test]
    fn test_transfer_status_clone() {
        let st = TransferStatus {
            status: TransferStatusEnum::Pending,
            transferred_bytes: 500,
        };
        let cloned = st.clone();
        assert_eq!(cloned.status, st.status);
        assert_eq!(cloned.transferred_bytes, st.transferred_bytes);
    }

    // =========================================================================
    // NotifyMsg
    // =========================================================================

    #[test]
    fn test_notify_msg_creation() {
        let nm = NotifyMsg {
            name: "sender".to_string(),
            msg: "hello".to_string(),
        };
        assert_eq!(nm.name, "sender");
        assert_eq!(nm.msg, "hello");
    }

    #[test]
    fn test_notify_msg_clone_eq() {
        let a = NotifyMsg {
            name: "n1".into(),
            msg: "m1".into(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_notify_msg_debug() {
        let nm = NotifyMsg {
            name: "peer".into(),
            msg: "done".into(),
        };
        let s = format!("{:?}", nm);
        assert!(s.contains("peer"));
        assert!(s.contains("done"));
    }

    #[test]
    fn test_notify_msg_buf_creation() {
        let buf = NotifyMsgBuf {
            messages: vec![NotifyMsg {
                name: "a".into(),
                msg: "x".into(),
            }],
        };
        assert_eq!(buf.messages.len(), 1);
        assert_eq!(buf.messages[0].name, "a");
    }

    #[test]
    fn test_notify_msg_buf_empty() {
        let buf = NotifyMsgBuf { messages: vec![] };
        assert!(buf.messages.is_empty());
    }

    #[test]
    fn test_notify_msg_buf_debug() {
        let buf = NotifyMsgBuf { messages: vec![] };
        assert!(format!("{:?}", buf).contains("NotifyMsgBuf"));
    }
}
