use super::nof_probe::{NoFTransportKind, NoFTransportSpec, parse_nof_transport_spec};
use spdk_rs::{DmaBuf, libspdk};
use std::ffi::{CString, c_char, c_void};
use std::net::IpAddr;
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

const COMPLETION_PENDING: u16 = u16::MAX;

trait ProbeOps {
    fn initialize(&mut self) -> Result<(), String>;
    fn connect(&mut self, spec: &NoFTransportSpec) -> Result<(), String>;
    fn select_namespace(&mut self, ns_id: u32) -> Result<u32, String>;
    fn allocate_qpair(&mut self) -> Result<(), String>;
    fn allocate_buffer(&mut self, size: u32) -> Result<(), String>;
    fn submit_read(&mut self) -> Result<(), String>;
    fn poll_completion(&mut self) -> Result<Option<u16>, String>;
    fn quiesce(&mut self);
    fn cleanup(&mut self);
}

pub(super) fn probe_nof_endpoint_with_spdk_rs(
    endpoint: &str,
    timeout: Duration,
) -> Result<(), String> {
    let spec = parse_nof_transport_spec(endpoint)?.ok_or_else(|| {
        "transport_id_parse_fail: structured NVMe-oF endpoint required".to_string()
    })?;
    run_probe(&mut RawProbeOps::default(), &spec, timeout)
}

fn run_probe(
    ops: &mut dyn ProbeOps,
    spec: &NoFTransportSpec,
    timeout: Duration,
) -> Result<(), String> {
    let result = (|| {
        ops.initialize()?;
        ops.connect(spec).map_err(|e| {
            if e.starts_with("transport_id_parse_fail:") {
                e
            } else {
                format!("open_fail: {e}")
            }
        })?;
        let block_size = ops
            .select_namespace(spec.ns)
            .map_err(|e| format!("open_fail: {e}"))?;
        if block_size == 0 {
            return Err("invalid_block_size".to_string());
        }
        ops.allocate_qpair()
            .map_err(|e| format!("open_fail: alloc qpair: {e}"))?;
        ops.allocate_buffer(block_size)
            .map_err(|e| format!("probe_buffer_alloc_fail: {e}"))?;
        ops.submit_read().map_err(|e| format!("submit_fail: {e}"))?;

        let deadline = Instant::now() + timeout;
        loop {
            match ops.poll_completion() {
                Ok(Some(status)) if completion_succeeded(status) => return Ok(()),
                Ok(Some(status)) => return Err(format!("completion_error: status={status}")),
                Err(e) => return Err(format!("completion_error: {e}")),
                Ok(None) if Instant::now() >= deadline => {
                    return Err("completion_timeout".to_string());
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(1)),
            }
        }
    })();
    ops.quiesce();
    ops.cleanup();
    result
}

fn completion_succeeded(status: u16) -> bool {
    // Bit 0 is the phase tag. Bits 1..=8 and 9..=11 are SC and SCT.
    status & 0x0ffe == 0
}

static SPDK_ENV: OnceLock<Result<(), String>> = OnceLock::new();

#[derive(Default)]
struct RawProbeOps {
    thread: *mut libspdk::spdk_thread,
    ctrlr: *mut libspdk::spdk_nvme_ctrlr,
    namespace: *mut libspdk::spdk_nvme_ns,
    qpair: *mut libspdk::spdk_nvme_qpair,
    buffer: Option<DmaBuf>,
    completion: Box<AtomicU16>,
}

impl ProbeOps for RawProbeOps {
    fn initialize(&mut self) -> Result<(), String> {
        SPDK_ENV
            .get_or_init(initialize_spdk)
            .as_ref()
            .map_err(Clone::clone)?;
        let name = CString::new("mooncake-nof-probe").unwrap();
        self.thread = unsafe { libspdk::spdk_thread_create(name.as_ptr(), ptr::null()) };
        if self.thread.is_null() {
            return Err("spdk_thread_init_fail: spdk_thread_create returned null".to_string());
        }
        unsafe { libspdk::spdk_set_thread(self.thread) };
        Ok(())
    }

    fn connect(&mut self, spec: &NoFTransportSpec) -> Result<(), String> {
        let trid = transport_id(spec)?;
        self.ctrlr = unsafe { libspdk::spdk_nvme_connect(&trid, ptr::null(), 0) };
        if self.ctrlr.is_null() {
            Err("controller not found".to_string())
        } else {
            Ok(())
        }
    }

    fn select_namespace(&mut self, ns_id: u32) -> Result<u32, String> {
        if ns_id == 0 || ns_id > unsafe { libspdk::spdk_nvme_ctrlr_get_num_ns(self.ctrlr) } {
            return Err(format!("namespace {ns_id} is not active"));
        }
        self.namespace = unsafe { libspdk::spdk_nvme_ctrlr_get_ns(self.ctrlr, ns_id) };
        if self.namespace.is_null() || !unsafe { libspdk::spdk_nvme_ns_is_active(self.namespace) } {
            return Err(format!("namespace {ns_id} is not active"));
        }
        Ok(unsafe { libspdk::spdk_nvme_ns_get_sector_size(self.namespace) })
    }

    fn allocate_qpair(&mut self) -> Result<(), String> {
        self.qpair = unsafe { libspdk::spdk_nvme_ctrlr_alloc_io_qpair(self.ctrlr, ptr::null(), 0) };
        if self.qpair.is_null() {
            Err("allocation failed".to_string())
        } else {
            Ok(())
        }
    }

    fn allocate_buffer(&mut self, size: u32) -> Result<(), String> {
        self.buffer = Some(DmaBuf::new(size.into(), size.into()).map_err(|e| e.to_string())?);
        Ok(())
    }

    fn submit_read(&mut self) -> Result<(), String> {
        self.completion.store(COMPLETION_PENDING, Ordering::Release);
        let buffer = self
            .buffer
            .as_mut()
            .expect("buffer allocated before submit");
        let rc = unsafe {
            libspdk::spdk_nvme_ns_cmd_read(
                self.namespace,
                self.qpair,
                buffer.as_mut_ptr(),
                0,
                1,
                Some(read_complete),
                self.completion.as_ref() as *const AtomicU16 as *mut c_void,
                0,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(format!("errno={rc}"))
        }
    }

    fn poll_completion(&mut self) -> Result<Option<u16>, String> {
        let rc = unsafe { libspdk::spdk_nvme_qpair_process_completions(self.qpair, 0) };
        if rc < 0 {
            return Err(format!("process_completions={rc}"));
        }
        match self.completion.load(Ordering::Acquire) {
            COMPLETION_PENDING => Ok(None),
            status => Ok(Some(status)),
        }
    }

    fn quiesce(&mut self) {
        unsafe {
            if !self.qpair.is_null() {
                libspdk::spdk_nvme_ctrlr_free_io_qpair(self.qpair);
                self.qpair = ptr::null_mut();
            }
        }
    }

    fn cleanup(&mut self) {
        self.quiesce();
        self.buffer.take();
        unsafe {
            if !self.ctrlr.is_null() {
                libspdk::spdk_nvme_detach(self.ctrlr);
                self.ctrlr = ptr::null_mut();
            }
            if !self.thread.is_null() {
                libspdk::spdk_set_thread(self.thread);
                libspdk::spdk_thread_exit(self.thread);
                while !libspdk::spdk_thread_is_exited(self.thread) {
                    libspdk::spdk_thread_poll(self.thread, 0, 0);
                }
                libspdk::spdk_thread_destroy(self.thread);
                libspdk::spdk_set_thread(ptr::null_mut());
                self.thread = ptr::null_mut();
            }
        }
    }
}

fn initialize_spdk() -> Result<(), String> {
    let name = CString::new("mooncake").unwrap();
    let mut opts: libspdk::spdk_env_opts = unsafe { std::mem::zeroed() };
    unsafe {
        libspdk::spdk_env_opts_init(&mut opts);
        opts.opts_size = std::mem::size_of::<libspdk::spdk_env_opts>() as _;
        opts.name = name.as_ptr();
        libspdk::spdk_log_set_print_level(libspdk::SPDK_LOG_NOTICE);
        let rc = libspdk::spdk_env_init(&opts);
        if rc != 0 {
            return Err(format!("spdk_env_init_fail: errno={rc}"));
        }
        let rc = libspdk::spdk_thread_lib_init(None, 0);
        if rc != 0 {
            return Err(format!("spdk_thread_init_fail: errno={rc}"));
        }
    }
    Ok(())
}

fn transport_id(spec: &NoFTransportSpec) -> Result<libspdk::spdk_nvme_transport_id, String> {
    let mut trid: libspdk::spdk_nvme_transport_id = unsafe { std::mem::zeroed() };
    trid.trtype = match spec.kind {
        NoFTransportKind::Tcp => libspdk::SPDK_NVME_TRANSPORT_TCP,
        NoFTransportKind::Rdma => libspdk::SPDK_NVME_TRANSPORT_RDMA,
    };
    trid.adrfam = match spec.traddr.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => libspdk::SPDK_NVMF_ADRFAM_IPV4,
        Ok(IpAddr::V6(_)) => libspdk::SPDK_NVMF_ADRFAM_IPV6,
        Err(e) => {
            return Err(format!(
                "transport_id_parse_fail: invalid traddr {}: {e}",
                spec.traddr
            ));
        }
    };
    copy_field(
        &mut trid.trstring,
        match spec.kind {
            NoFTransportKind::Tcp => "TCP",
            NoFTransportKind::Rdma => "RDMA",
        },
        "trtype",
    )?;
    copy_field(&mut trid.traddr, &spec.traddr, "traddr")?;
    copy_field(&mut trid.trsvcid, &spec.trsvcid, "trsvcid")?;
    copy_field(&mut trid.subnqn, &spec.subnqn, "subnqn")?;
    Ok(trid)
}

fn copy_field(field: &mut [c_char], value: &str, name: &str) -> Result<(), String> {
    if value.len() >= field.len() {
        return Err(format!("transport_id_parse_fail: {name} is too long"));
    }
    for (slot, byte) in field.iter_mut().zip(value.bytes()) {
        *slot = byte as c_char;
    }
    Ok(())
}

unsafe extern "C" fn read_complete(ctx: *mut c_void, cpl: *const libspdk::spdk_nvme_cpl) {
    let status = if cpl.is_null() {
        2
    } else {
        unsafe { (*cpl).__bindgen_anon_1.status_raw }
    };
    unsafe { &*(ctx as *const AtomicU16) }.store(status, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeOps {
        initialize: Result<(), String>,
        connect: Result<(), String>,
        block_size: Result<u32, String>,
        qpair: Result<(), String>,
        buffer: Result<(), String>,
        submit: Result<(), String>,
        completions: Vec<Result<Option<u16>, String>>,
        selected_ns: Option<u32>,
        outstanding: bool,
        cleanup_while_outstanding: bool,
        cleaned: bool,
    }

    impl Default for FakeOps {
        fn default() -> Self {
            Self {
                initialize: Ok(()),
                connect: Ok(()),
                block_size: Ok(4096),
                qpair: Ok(()),
                buffer: Ok(()),
                submit: Ok(()),
                completions: vec![Ok(Some(0))],
                selected_ns: None,
                outstanding: false,
                cleanup_while_outstanding: false,
                cleaned: false,
            }
        }
    }

    impl ProbeOps for FakeOps {
        fn initialize(&mut self) -> Result<(), String> {
            self.initialize.clone()
        }
        fn connect(&mut self, _: &NoFTransportSpec) -> Result<(), String> {
            self.connect.clone()
        }
        fn select_namespace(&mut self, ns_id: u32) -> Result<u32, String> {
            self.selected_ns = Some(ns_id);
            self.block_size.clone()
        }
        fn allocate_qpair(&mut self) -> Result<(), String> {
            self.qpair.clone()
        }
        fn allocate_buffer(&mut self, _: u32) -> Result<(), String> {
            self.buffer.clone()
        }
        fn submit_read(&mut self) -> Result<(), String> {
            let result = self.submit.clone();
            if result.is_ok() {
                self.outstanding = true;
            }
            result
        }
        fn poll_completion(&mut self) -> Result<Option<u16>, String> {
            let result = self.completions.remove(0);
            if matches!(result, Ok(Some(_))) {
                self.outstanding = false;
            }
            result
        }
        fn quiesce(&mut self) {
            self.outstanding = false;
        }
        fn cleanup(&mut self) {
            self.cleanup_while_outstanding = self.outstanding;
            self.cleaned = true;
        }
    }

    fn tcp_spec() -> NoFTransportSpec {
        NoFTransportSpec {
            kind: NoFTransportKind::Tcp,
            traddr: "127.0.0.1".into(),
            trsvcid: "4420".into(),
            subnqn: "nqn.test".into(),
            ns: 7,
        }
    }

    #[test]
    fn pinned_spdk_rs_public_types_compile() {
        fn assert_type<T>() {}

        assert_type::<DmaBuf>();
        assert_type::<spdk_rs::Thread>();
        assert_type::<libspdk::spdk_nvme_transport_id>();
    }

    #[test]
    fn transport_conversion_preserves_tcp_rdma_and_namespace() {
        let tcp = transport_id(&tcp_spec()).unwrap();
        assert_eq!(tcp.trtype, libspdk::SPDK_NVME_TRANSPORT_TCP);
        let mut rdma = tcp_spec();
        rdma.kind = NoFTransportKind::Rdma;
        assert_eq!(
            transport_id(&rdma).unwrap().trtype,
            libspdk::SPDK_NVME_TRANSPORT_RDMA
        );
        assert_eq!(rdma.ns, 7);
    }

    #[test]
    fn transport_conversion_selects_ipv6_address_family() {
        let mut spec = tcp_spec();
        spec.traddr = "2001:db8::1".into();
        assert_eq!(
            transport_id(&spec).unwrap().adrfam,
            libspdk::SPDK_NVMF_ADRFAM_IPV6
        );
    }

    #[test]
    fn adapter_propagates_namespace_and_cleans_up_after_success() {
        let mut ops = FakeOps::default();
        run_probe(&mut ops, &tcp_spec(), Duration::from_millis(1)).unwrap();
        assert_eq!(ops.selected_ns, Some(7));
        assert!(ops.cleaned);
    }

    #[test]
    fn adapter_ignores_the_completion_phase_tag() {
        let mut ops = FakeOps {
            completions: vec![Ok(Some(1))],
            ..Default::default()
        };
        run_probe(&mut ops, &tcp_spec(), Duration::from_millis(1)).unwrap();
        assert!(!ops.cleanup_while_outstanding);
    }

    #[test]
    fn adapter_reports_initialization_attach_and_allocation_failures() {
        let cases = [
            (
                FakeOps {
                    initialize: Err("spdk_env_init_fail".into()),
                    ..Default::default()
                },
                "spdk_env_init_fail",
            ),
            (
                FakeOps {
                    connect: Err("not found".into()),
                    ..Default::default()
                },
                "open_fail",
            ),
            (
                FakeOps {
                    qpair: Err("no qpair".into()),
                    ..Default::default()
                },
                "open_fail",
            ),
            (
                FakeOps {
                    buffer: Err("no dma".into()),
                    ..Default::default()
                },
                "probe_buffer_alloc_fail",
            ),
        ];
        for (mut ops, expected) in cases {
            assert!(
                run_probe(&mut ops, &tcp_spec(), Duration::from_millis(1))
                    .unwrap_err()
                    .contains(expected)
            );
            assert!(ops.cleaned);
        }
    }

    #[test]
    fn adapter_reports_submit_and_completion_failures_and_cleans_up() {
        let mut submit = FakeOps {
            submit: Err("busy".into()),
            ..Default::default()
        };
        assert!(
            run_probe(&mut submit, &tcp_spec(), Duration::from_millis(1))
                .unwrap_err()
                .contains("submit_fail")
        );
        assert!(submit.cleaned);

        let mut completion = FakeOps {
            completions: vec![Ok(Some(5))],
            ..Default::default()
        };
        assert!(
            run_probe(&mut completion, &tcp_spec(), Duration::from_millis(1))
                .unwrap_err()
                .contains("completion_error")
        );
        assert!(completion.cleaned);

        let mut parse = FakeOps {
            connect: Err("transport_id_parse_fail: invalid traddr".into()),
            ..Default::default()
        };
        assert_eq!(
            run_probe(&mut parse, &tcp_spec(), Duration::from_millis(1)).unwrap_err(),
            "transport_id_parse_fail: invalid traddr"
        );
    }

    #[test]
    fn adapter_rejects_zero_block_size_and_times_out() {
        let mut zero = FakeOps {
            block_size: Ok(0),
            ..Default::default()
        };
        assert_eq!(
            run_probe(&mut zero, &tcp_spec(), Duration::ZERO).unwrap_err(),
            "invalid_block_size"
        );
        assert!(zero.cleaned);

        let mut timeout = FakeOps {
            completions: vec![Ok(None)],
            ..Default::default()
        };
        assert_eq!(
            run_probe(&mut timeout, &tcp_spec(), Duration::ZERO).unwrap_err(),
            "completion_timeout"
        );
        assert!(!timeout.cleanup_while_outstanding);
        assert!(timeout.cleaned);
    }
}
