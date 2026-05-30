//! Segment types for the Transfer Engine FFI layer.
//! Transfer Engine FFI 层的段类型。
//!
//! # What is a Segment? / 什么是段（Segment）？
//!
//! A "segment" is a logical handle that represents a remote memory region
//! accessible via RDMA or NVMe-oF. When a node registers local memory and
//! advertises it through the metadata service, other nodes can "open" that
//! segment to obtain a `SegmentId`. This ID is then used in `TransferRequest`
//! to specify the target of a read or write operation.
//!
//! "段" (Segment) 是一个逻辑句柄，表示可通过 RDMA 或 NVMe-oF 访问的远程内存区域。
//! 当节点注册本地内存并通过元数据服务发布后，其他节点可以"打开"该段以获取 `SegmentId`。
//! 此 ID 随后在 `TransferRequest` 中用于指定读或写操作的目标。
//!
//! ## Segment Lifecycle / 段生命周期
//!
//! ```text
//! Register memory ──► advertise via metadata ──► remote opens segment
//!      (local)            (etcd/P2PHANDSHAKE)        (get SegmentId)
//!
//! Use in transfers ──► close segment ──► unregister memory
//! ```
//!
//! ## SegmentDesc / 段描述符
//!
//! `SegmentDesc` describes the type and location of a memory segment.
//! Two variants exist:
//! - **Rdma**: Direct memory access via RDMA NICs. Used for CPU and GPU memory.
//! - **Nvmeof**: NVMe over Fabrics, used for accessing remote NVMe storage devices.
//!
//! `SegmentDesc` 描述内存段的类型和位置。存在两种变体：
//! - **Rdma**: 通过 RDMA 网卡直接内存访问。用于 CPU 和 GPU 内存。
//! - **Nvmeof**: NVMe over Fabrics，用于访问远程 NVMe 存储设备。

use std::fmt;

/// Wrapper for Transfer Engine segment ID.
/// Transfer Engine 段 ID 的封装。
///
/// This is a newtype over `i32` that represents a unique identifier for an
/// opened remote segment. The ID is returned by `open_segment` and consumed
/// by `close_segment` and `TransferRequest`.
/// 这是对 `i32` 的新类型封装，表示已打开的远程段的唯一标识符。
/// 该 ID 由 `open_segment` 返回，由 `close_segment` 和 `TransferRequest` 消费。
///
/// A negative value typically indicates an error from the C++ layer.
/// 负数通常表示来自 C++ 层的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId(pub i32);

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "segment#{}", self.0)
    }
}

/// Description of a memory segment for RDMA / NVMe-oF.
/// RDMA / NVMe-oF 内存段的描述。
///
/// This enum captures the metadata needed to describe a memory region
/// that can be shared with remote peers. The actual memory registration
/// is handled by `register_local_memory` on the TransferEngine.
/// 此枚举捕获描述可与远程节点共享的内存区域所需的元数据。
/// 实际的内存注册由 TransferEngine 上的 `register_local_memory` 处理。
#[derive(Debug, Clone)]
pub enum SegmentDesc {
    /// An RDMA-accessible memory segment.
    /// 可通过 RDMA 访问的内存段。
    ///
    /// RDMA segments allow zero-copy data transfer between nodes.
    /// The memory can be on CPU (`cpu:0`) or GPU (`cuda:0`).
    /// RDMA 段允许节点间零拷贝数据传输。
    /// 内存可以在 CPU (`cpu:0`) 或 GPU (`cuda:0`) 上。
    Rdma {
        /// Starting virtual address of the memory region.
        /// 内存区域的起始虚拟地址。
        addr: usize,
        /// Size of the memory region in bytes.
        /// 内存区域的大小（字节）。
        size: u64,
        /// Device location string, e.g. "cpu:0", "cuda:0".
        /// 设备位置字符串，如 "cpu:0"、"cuda:0"。
        location: String,
    },
    /// An NVMe-over-Fabrics memory segment.
    /// NVMe over Fabrics 内存段。
    ///
    /// NVMe-oF segments provide access to remote NVMe storage devices
    /// over a fabric network (TCP, RDMA, etc.).
    /// NVMe-oF 段通过光纤网络（TCP、RDMA 等）提供对远程 NVMe 存储设备的访问。
    Nvmeof {
        /// Path to the NVMe device file, e.g. "/dev/nvme0n1".
        /// NVMe 设备文件路径，如 "/dev/nvme0n1"。
        file_path: String,
        /// NVMe subsystem NQN (NVMe Qualified Name), e.g. "nqn.2024-01".
        /// NVMe 子系统 NQN（NVMe 限定名称），如 "nqn.2024-01"。
        subsystem_name: String,
        /// Transport protocol, e.g. "tcp", "rdma".
        /// 传输协议，如 "tcp"、"rdma"。
        proto: String,
        /// Target IP address.
        /// 目标 IP 地址。
        ip: String,
        /// Target port number.
        /// 目标端口号。
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
