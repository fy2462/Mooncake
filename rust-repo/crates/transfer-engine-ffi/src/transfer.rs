use std::ffi::c_void;

use crate::ffi;
use crate::segment::SegmentId;

/// Wrapper for a Transfer Engine batch ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchId(pub u64);

/// Transfer operation type: read from remote or write to remote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Opcode {
    Read = ffi::OPCODE_READ as i32,
    Write = ffi::OPCODE_WRITE as i32,
}

impl Opcode {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v as u32 {
            ffi::OPCODE_READ => Some(Self::Read),
            ffi::OPCODE_WRITE => Some(Self::Write),
            _ => None,
        }
    }
}

/// A notification message exchanged between transfer engine peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyMsg {
    pub name: String,
    pub msg: String,
}

/// An owned buffer of notification messages received from the engine.
#[derive(Debug)]
pub struct NotifyMsgBuf {
    pub messages: Vec<NotifyMsg>,
}

/// A single transfer request within a batch.
#[derive(Debug, Clone)]
pub struct TransferRequest {
    pub opcode: Opcode,
    pub source: *mut c_void,
    pub target_id: SegmentId,
    pub target_offset: u64,
    pub length: u64,
}

// TransferRequest holds raw pointers; mark as Send/Sync.
unsafe impl Send for TransferRequest {}
unsafe impl Sync for TransferRequest {}

/// Transfer task status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TransferStatusEnum {
    Waiting = ffi::STATUS_WAITING as i32,
    Pending = ffi::STATUS_PENDING as i32,
    Invalid = ffi::STATUS_INVALID as i32,
    Canceled = ffi::STATUS_CANCELED as i32,
    Completed = ffi::STATUS_COMPLETED as i32,
    Timeout = ffi::STATUS_TIMEOUT as i32,
    Failed = ffi::STATUS_FAILED as i32,
}

impl TransferStatusEnum {
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

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Timeout
        )
    }
}

/// Result of polling a single transfer within a batch.
#[derive(Debug, Clone)]
pub struct TransferStatus {
    pub status: TransferStatusEnum,
    pub transferred_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // BatchId
    // =========================================================================

    #[test]
    fn test_batch_id_creation() {
        assert_eq!(BatchId(0).0, 0);
        assert_eq!(BatchId(42).0, 42);
        assert_eq!(BatchId(u64::MAX).0, u64::MAX);
    }

    #[test]
    fn test_batch_id_clone_eq() {
        let id = BatchId(99);
        assert_eq!(id, id.clone());
        assert_eq!(id, BatchId(99));
        assert_ne!(id, BatchId(100));
    }

    #[test]
    fn test_batch_id_debug() {
        assert_eq!(format!("{:?}", BatchId(5)), "BatchId(5)");
    }

    #[test]
    fn test_batch_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(BatchId(1));
        set.insert(BatchId(2));
        set.insert(BatchId(1));
        assert_eq!(set.len(), 2);
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
