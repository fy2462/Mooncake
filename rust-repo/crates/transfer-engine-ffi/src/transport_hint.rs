use crate::TransferRequest;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i32)]
pub enum TransportHint {
    #[default]
    Unspecified = 0,
    Tcp = 1,
    Rdma = 2,
    NvmeOf = 3,
    Shm = 4,
    Nvlink = 5,
    Mnnvl = 6,
    Gds = 7,
    IoUring = 8,
    Ascend = 9,
    SunriseLink = 10,
    Tpu = 11,
    Efa = 12,
    Cxi = 13,
    Ub = 14,
    UbShmem = 15,
}

impl TransportHint {
    pub fn as_protocol(self) -> Option<&'static str> {
        match self {
            Self::Unspecified => None,
            Self::Tcp => Some("tcp"),
            Self::Rdma => Some("rdma"),
            Self::NvmeOf => Some("nvmeof"),
            Self::Shm => Some("shm"),
            Self::Nvlink => Some("nvlink"),
            Self::Mnnvl => Some("mnnvl"),
            Self::Gds => Some("gds"),
            Self::IoUring => Some("io_uring"),
            Self::Ascend => Some("ascend"),
            Self::SunriseLink => Some("sunrise_link"),
            Self::Tpu => Some("tpu"),
            Self::Efa => Some("efa"),
            Self::Cxi => Some("cxi"),
            Self::Ub => Some("ub"),
            Self::UbShmem => Some("ubshmem"),
        }
    }

    pub fn from_protocol(protocol: &str) -> Self {
        match protocol.to_ascii_lowercase().as_str() {
            "" | "auto" | "default" | "unspecified" => Self::Unspecified,
            "tcp" => Self::Tcp,
            "rdma" | "roce" | "ib" | "infiniband" => Self::Rdma,
            "nof" | "nvmeof" | "nvme-o-f" | "nvme-of" => Self::NvmeOf,
            "shm" | "shared-memory" => Self::Shm,
            "nvlink" | "nvlink_intra" => Self::Nvlink,
            "mnnvl" => Self::Mnnvl,
            "gds" => Self::Gds,
            "io_uring" | "iouring" => Self::IoUring,
            "ascend" | "ascend_direct" => Self::Ascend,
            "sunrise_link" | "sunrise-link" => Self::SunriseLink,
            "tpu" => Self::Tpu,
            "efa" => Self::Efa,
            "cxi" => Self::Cxi,
            "ub" => Self::Ub,
            "ubshmem" | "ub-shmem" => Self::UbShmem,
            _ => Self::Unspecified,
        }
    }

    pub fn merge(self, fallback: Self) -> Self {
        if self == Self::Unspecified {
            fallback
        } else {
            self
        }
    }
}

impl FromStr for TransportHint {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_protocol(s))
    }
}

#[derive(Debug, Clone)]
pub struct HintedTransferRequest {
    pub request: TransferRequest,
    pub transport_hint: TransportHint,
}

impl HintedTransferRequest {
    pub fn new(request: TransferRequest, transport_hint: TransportHint) -> Self {
        Self {
            request,
            transport_hint,
        }
    }

    pub fn without_hint(request: TransferRequest) -> Self {
        Self::new(request, TransportHint::Unspecified)
    }

    pub fn with_default_hint(mut self, fallback: TransportHint) -> Self {
        self.transport_hint = self.transport_hint.merge(fallback);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{HintedTransferRequest, TransportHint};
    use crate::{Opcode, SegmentId, TransferRequest};
    use std::ffi::c_void;

    fn request() -> TransferRequest {
        TransferRequest {
            opcode: Opcode::Read,
            source: 0x1000 as *mut c_void,
            target_id: SegmentId(7),
            target_offset: 64,
            length: 4096,
        }
    }

    #[test]
    fn protocol_names_cover_cpp_transport_selector_values() {
        assert_eq!(TransportHint::from_protocol("tcp"), TransportHint::Tcp);
        assert_eq!(TransportHint::from_protocol("RDMA"), TransportHint::Rdma);
        assert_eq!(TransportHint::from_protocol("roce"), TransportHint::Rdma);
        assert_eq!(TransportHint::from_protocol("nof"), TransportHint::NvmeOf);
        assert_eq!(
            TransportHint::from_protocol("nvme-of"),
            TransportHint::NvmeOf
        );
        assert_eq!(
            TransportHint::from_protocol("nvlink_intra"),
            TransportHint::Nvlink
        );
        assert_eq!(TransportHint::from_protocol("mnnvl"), TransportHint::Mnnvl);
        assert_eq!(TransportHint::from_protocol("gds"), TransportHint::Gds);
        assert_eq!(
            TransportHint::from_protocol("io_uring"),
            TransportHint::IoUring
        );
        assert_eq!(
            TransportHint::from_protocol("ascend_direct"),
            TransportHint::Ascend
        );
        assert_eq!(
            TransportHint::from_protocol("sunrise-link"),
            TransportHint::SunriseLink
        );
        assert_eq!(TransportHint::from_protocol("tpu"), TransportHint::Tpu);
        assert_eq!(TransportHint::from_protocol("efa"), TransportHint::Efa);
        assert_eq!(TransportHint::from_protocol("cxi"), TransportHint::Cxi);
        assert_eq!(TransportHint::from_protocol("ub"), TransportHint::Ub);
        assert_eq!(
            TransportHint::from_protocol("ub-shmem"),
            TransportHint::UbShmem
        );
        assert_eq!(
            TransportHint::from_protocol("unknown"),
            TransportHint::Unspecified
        );
    }

    #[test]
    fn protocol_roundtrip_returns_native_selector_name() {
        assert_eq!(TransportHint::Tcp.as_protocol(), Some("tcp"));
        assert_eq!(TransportHint::Rdma.as_protocol(), Some("rdma"));
        assert_eq!(TransportHint::NvmeOf.as_protocol(), Some("nvmeof"));
        assert_eq!(TransportHint::Mnnvl.as_protocol(), Some("mnnvl"));
        assert_eq!(TransportHint::Gds.as_protocol(), Some("gds"));
        assert_eq!(TransportHint::IoUring.as_protocol(), Some("io_uring"));
        assert_eq!(TransportHint::Ascend.as_protocol(), Some("ascend"));
        assert_eq!(
            TransportHint::SunriseLink.as_protocol(),
            Some("sunrise_link")
        );
        assert_eq!(TransportHint::Tpu.as_protocol(), Some("tpu"));
        assert_eq!(TransportHint::Efa.as_protocol(), Some("efa"));
        assert_eq!(TransportHint::Cxi.as_protocol(), Some("cxi"));
        assert_eq!(TransportHint::Ub.as_protocol(), Some("ub"));
        assert_eq!(TransportHint::UbShmem.as_protocol(), Some("ubshmem"));
        assert_eq!(TransportHint::Unspecified.as_protocol(), None);
    }

    #[test]
    fn merge_uses_fallback_only_when_unspecified() {
        assert_eq!(
            TransportHint::Unspecified.merge(TransportHint::Tcp),
            TransportHint::Tcp
        );
        assert_eq!(
            TransportHint::Rdma.merge(TransportHint::Tcp),
            TransportHint::Rdma
        );
    }

    #[test]
    fn hinted_request_preserves_abi_request_shape() {
        let hinted =
            HintedTransferRequest::without_hint(request()).with_default_hint(TransportHint::Rdma);
        assert_eq!(hinted.transport_hint, TransportHint::Rdma);
        assert_eq!(hinted.request.target_id, SegmentId(7));
        assert_eq!(hinted.request.length, 4096);
    }
}
