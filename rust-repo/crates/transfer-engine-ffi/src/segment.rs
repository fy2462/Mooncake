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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_id_creation() {
        let id = SegmentId(42);
        assert_eq!(id.0, 42);
    }

    #[test]
    fn test_segment_id_display() {
        assert_eq!(SegmentId(0).to_string(), "segment#0");
        assert_eq!(SegmentId(99).to_string(), "segment#99");
        assert_eq!(SegmentId(-1).to_string(), "segment#-1");
    }

    #[test]
    fn test_segment_id_clone() {
        let id = SegmentId(7);
        assert_eq!(id, id.clone());
    }

    #[test]
    fn test_segment_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(SegmentId(1));
        set.insert(SegmentId(2));
        set.insert(SegmentId(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_segment_id_debug() {
        assert_eq!(format!("{:?}", SegmentId(5)), "SegmentId(5)");
    }

    #[test]
    fn test_segment_desc_rdma() {
        let desc = SegmentDesc::Rdma {
            addr: 0xDEAD,
            size: 4096,
            location: "cpu:0".into(),
        };
        match desc {
            SegmentDesc::Rdma {
                addr,
                size,
                location,
            } => {
                assert_eq!(addr, 0xDEAD);
                assert_eq!(size, 4096);
                assert_eq!(location, "cpu:0");
            }
            _ => panic!("expected Rdma"),
        }
    }

    #[test]
    fn test_segment_desc_nvmeof() {
        let desc = SegmentDesc::Nvmeof {
            file_path: "/dev/nvme0n1".into(),
            subsystem_name: "nqn.2024-01".into(),
            proto: "tcp".into(),
            ip: "10.0.0.1".into(),
            port: 4420,
        };
        match desc {
            SegmentDesc::Nvmeof {
                file_path,
                subsystem_name,
                proto,
                ip,
                port,
            } => {
                assert_eq!(file_path, "/dev/nvme0n1");
                assert_eq!(subsystem_name, "nqn.2024-01");
                assert_eq!(proto, "tcp");
                assert_eq!(ip, "10.0.0.1");
                assert_eq!(port, 4420);
            }
            _ => panic!("expected Nvmeof"),
        }
    }

    #[test]
    fn test_segment_desc_clone() {
        let desc = SegmentDesc::Rdma {
            addr: 0x1000,
            size: 8192,
            location: "cuda:0".into(),
        };
        let cloned = desc.clone();
        match cloned {
            SegmentDesc::Rdma {
                addr,
                size,
                location,
            } => {
                assert_eq!(addr, 0x1000);
                assert_eq!(size, 8192);
                assert_eq!(location, "cuda:0");
            }
            _ => panic!("expected Rdma"),
        }
    }

    #[test]
    fn test_segment_desc_debug() {
        let desc = SegmentDesc::Rdma {
            addr: 0x42,
            size: 1024,
            location: "cpu:0".into(),
        };
        let s = format!("{:?}", desc);
        assert!(s.contains("Rdma"));
    }
}
