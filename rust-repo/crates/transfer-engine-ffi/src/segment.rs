use std::fmt;

/// Wrapper for Transfer Engine segment ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId(pub i32);

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "segment#{}", self.0)
    }
}

/// Description of a memory segment for RDMA / NVMe-oF.
#[derive(Debug, Clone)]
pub enum SegmentDesc {
    Rdma {
        addr: usize,
        size: u64,
        location: String,
    },
    Nvmeof {
        file_path: String,
        subsystem_name: String,
        proto: String,
        ip: String,
        port: u64,
    },
}
