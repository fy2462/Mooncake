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
