#[cfg(not(feature = "spdk-nof-probe"))]
use std::net::{TcpStream, ToSocketAddrs};
use std::process::Command;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NoFTransportKind {
    Tcp,
    Rdma,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NoFTransportSpec {
    pub kind: NoFTransportKind,
    pub traddr: String,
    pub trsvcid: String,
    pub subnqn: String,
    pub ns: u32,
}

pub(crate) fn probe_nof_endpoint(te_endpoint: &str, timeout: Duration) -> Result<(), String> {
    let endpoint = te_endpoint.trim();
    if endpoint.is_empty() {
        return Err("NoF transport endpoint is empty".to_string());
    }
    if let Some(command) = nof_probe_command_from_env() {
        return run_nof_probe_command(&command, endpoint, timeout);
    }
    probe_nof_endpoint_default(endpoint, timeout)
}

#[cfg(feature = "spdk-nof-probe")]
fn probe_nof_endpoint_default(endpoint: &str, timeout: Duration) -> Result<(), String> {
    super::spdk_rs_probe::probe_nof_endpoint_with_spdk_rs(endpoint, timeout)
}

#[cfg(not(feature = "spdk-nof-probe"))]
fn probe_nof_endpoint_default(endpoint: &str, timeout: Duration) -> Result<(), String> {
    probe_nof_endpoint_with_tcp(endpoint, timeout)
}

fn nof_probe_command_from_env() -> Option<String> {
    std::env::var("MOONCAKE_NOF_PROBE_COMMAND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn run_nof_probe_command(command: &str, endpoint: &str, timeout: Duration) -> Result<(), String> {
    let mut child = Command::new(command)
        .arg(endpoint)
        .arg(timeout.as_millis().to_string())
        .spawn()
        .map_err(|e| format!("start NoF probe command {command}: {e}"))?;
    let deadline = Instant::now() + timeout;
    loop {
        match child
            .try_wait()
            .map_err(|e| format!("poll NoF probe command {command}: {e}"))?
        {
            Some(status) if status.success() => return Ok(()),
            Some(status) => {
                return Err(format!(
                    "NoF probe command {command} failed for endpoint {endpoint}: {status}"
                ));
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "completion_timeout: NoF probe command {command} timed out for endpoint {endpoint}"
                ));
            }
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

#[cfg(not(feature = "spdk-nof-probe"))]
fn probe_nof_endpoint_with_tcp(endpoint: &str, timeout: Duration) -> Result<(), String> {
    let mut last_error = None;
    let target = parse_nof_probe_target(endpoint)?;
    let addrs = target
        .to_socket_addrs()
        .map_err(|e| format!("resolve {target} from NoF endpoint {endpoint}: {e}"))?;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                return Ok(());
            }
            Err(e) => last_error = Some(e),
        }
    }

    Err(match last_error {
        Some(e) => format!("connect {target} from NoF endpoint {endpoint}: {e}"),
        None => format!("resolve {target}: no addresses"),
    })
}

#[cfg(any(not(feature = "spdk-nof-probe"), test))]
fn parse_nof_probe_target(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("NoF transport endpoint is empty".to_string());
    }
    if let Some(spec) = parse_nof_transport_spec(endpoint)? {
        return Ok(format!("{}:{}", spec.traddr, spec.trsvcid));
    }
    if let Some((scheme, rest)) = endpoint.split_once("://") {
        let scheme = scheme.trim().to_ascii_lowercase();
        if scheme != "tcp" && scheme != "nvme+tcp" {
            return Err(format!("unsupported NoF transport scheme {scheme}"));
        }
        let authority = rest
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default()
            .trim();
        if authority.is_empty() {
            return Err(format!("NoF endpoint {endpoint} has empty authority"));
        }
        return Ok(authority.to_string());
    }
    Ok(endpoint.to_string())
}

pub(crate) fn parse_nof_transport_spec(endpoint: &str) -> Result<Option<NoFTransportSpec>, String> {
    if let Some(spec) = parse_nof_uri_transport_spec(endpoint)? {
        return Ok(Some(spec));
    }

    let mut trtype = None;
    let mut traddr = None;
    let mut trsvcid = None;
    let mut subnqn = None;
    let mut ns = None;
    for token in endpoint.split(|c: char| c.is_ascii_whitespace() || c == ',') {
        let Some((key, value)) = split_nof_key_value(token) else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "trtype" if !value.is_empty() => trtype = Some(value.to_ascii_lowercase()),
            "traddr" if !value.is_empty() => traddr = Some(value.to_string()),
            "trsvcid" if !value.is_empty() => trsvcid = Some(value.to_string()),
            "subnqn" if !value.is_empty() => subnqn = Some(value.to_string()),
            "ns" if !value.is_empty() => {
                ns = Some(value.parse::<u32>().map_err(|e| {
                    format!("invalid NoF namespace id {value} in endpoint {endpoint}: {e}")
                })?);
            }
            _ => {}
        }
    }
    let saw_transport_fields = trtype.is_some()
        || traddr.is_some()
        || trsvcid.is_some()
        || subnqn.is_some()
        || ns.is_some();
    if !saw_transport_fields {
        return Ok(None);
    }
    let kind = match trtype.as_deref().unwrap_or("tcp") {
        "tcp" => NoFTransportKind::Tcp,
        "rdma" => NoFTransportKind::Rdma,
        other => return Err(format!("unsupported NoF transport type {other}")),
    };
    let Some(traddr) = traddr else {
        return Err(format!("NoF endpoint {endpoint} is missing traddr"));
    };
    let Some(trsvcid) = trsvcid else {
        return Err(format!("NoF endpoint {endpoint} is missing trsvcid"));
    };
    Ok(Some(NoFTransportSpec {
        kind,
        traddr,
        trsvcid,
        subnqn: subnqn.unwrap_or_default(),
        ns: ns.unwrap_or(1),
    }))
}

fn parse_nof_uri_transport_spec(endpoint: &str) -> Result<Option<NoFTransportSpec>, String> {
    let Some((scheme, rest)) = endpoint.trim().split_once("://") else {
        return Ok(None);
    };
    let kind = match scheme.trim().to_ascii_lowercase().as_str() {
        "tcp" | "nvme+tcp" => NoFTransportKind::Tcp,
        "rdma" | "nvme+rdma" => NoFTransportKind::Rdma,
        other => return Err(format!("unsupported NoF transport scheme {other}")),
    };
    let (authority, path) = rest
        .split_once('/')
        .map(|(authority, path)| (authority, path))
        .unwrap_or((rest, ""));
    let (traddr, trsvcid) = split_host_port(authority.trim())?;
    let subnqn = path
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    Ok(Some(NoFTransportSpec {
        kind,
        traddr,
        trsvcid,
        subnqn,
        ns: 1,
    }))
}

fn split_host_port(authority: &str) -> Result<(String, String), String> {
    if authority.is_empty() {
        return Err("NoF endpoint has empty authority".to_string());
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let Some((host, tail)) = rest.split_once(']') else {
            return Err(format!(
                "NoF endpoint authority {authority} has invalid IPv6 host"
            ));
        };
        let Some(port) = tail.strip_prefix(':') else {
            return Err(format!(
                "NoF endpoint authority {authority} is missing port"
            ));
        };
        if host.is_empty() || port.is_empty() {
            return Err(format!(
                "NoF endpoint authority {authority} is missing host or port"
            ));
        }
        return Ok((host.to_string(), port.to_string()));
    }
    let Some((host, port)) = authority.rsplit_once(':') else {
        return Err(format!(
            "NoF endpoint authority {authority} is missing port"
        ));
    };
    if host.is_empty() || port.is_empty() {
        return Err(format!(
            "NoF endpoint authority {authority} is missing host or port"
        ));
    }
    Ok((host.to_string(), port.to_string()))
}

fn split_nof_key_value(token: &str) -> Option<(&str, &str)> {
    token
        .split_once('=')
        .or_else(|| token.split_once(':'))
        .filter(|(key, _)| !key.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::{
        NoFTransportKind, parse_nof_probe_target, parse_nof_transport_spec, probe_nof_endpoint,
        run_nof_probe_command,
    };
    use std::time::Duration;

    #[test]
    fn test_probe_nof_endpoint_rejects_empty_endpoint() {
        let err = probe_nof_endpoint(" ", Duration::from_millis(10)).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    #[cfg(not(feature = "spdk-nof-probe"))]
    fn test_probe_nof_endpoint_rejects_unresolvable_endpoint() {
        let err =
            probe_nof_endpoint("not a socket address", Duration::from_millis(10)).unwrap_err();
        assert!(err.contains("resolve"));
    }

    #[test]
    fn test_parse_nof_probe_target_accepts_common_endpoint_forms() {
        assert_eq!(
            parse_nof_probe_target("127.0.0.1:4420").unwrap(),
            "127.0.0.1:4420"
        );
        assert_eq!(
            parse_nof_probe_target("tcp://127.0.0.1:4420/nqn.2014-08.org.nvmexpress").unwrap(),
            "127.0.0.1:4420"
        );
        assert_eq!(
            parse_nof_probe_target("trtype=tcp adrfam=ipv4 traddr=127.0.0.1 trsvcid=4420").unwrap(),
            "127.0.0.1:4420"
        );
    }

    #[test]
    fn test_parse_nof_transport_spec_accepts_cpp_style_endpoint() {
        let spec = parse_nof_transport_spec(
            "trtype:TCP adrfam:IPv4 traddr:127.0.0.1 trsvcid:4420 subnqn:nqn.2014-08.org.nvmexpress:uuid ns:3",
        )
        .unwrap()
        .unwrap();

        assert_eq!(spec.kind, NoFTransportKind::Tcp);
        assert_eq!(spec.traddr, "127.0.0.1");
        assert_eq!(spec.trsvcid, "4420");
        assert_eq!(spec.subnqn, "nqn.2014-08.org.nvmexpress:uuid");
        assert_eq!(spec.ns, 3);
    }

    #[test]
    fn test_parse_nof_transport_spec_accepts_equals_style_endpoint() {
        let spec =
            parse_nof_transport_spec("trtype=rdma traddr=10.0.0.1 trsvcid=4420 subnqn=nqn.test")
                .unwrap()
                .unwrap();

        assert_eq!(spec.kind, NoFTransportKind::Rdma);
        assert_eq!(spec.traddr, "10.0.0.1");
        assert_eq!(spec.trsvcid, "4420");
        assert_eq!(spec.subnqn, "nqn.test");
        assert_eq!(spec.ns, 1);
    }

    #[test]
    fn test_parse_nof_transport_spec_accepts_uri_endpoint() {
        let spec =
            parse_nof_transport_spec("nvme+tcp://127.0.0.1:4420/nqn.2014-08.org.nvmexpress:uuid")
                .unwrap()
                .unwrap();

        assert_eq!(spec.kind, NoFTransportKind::Tcp);
        assert_eq!(spec.traddr, "127.0.0.1");
        assert_eq!(spec.trsvcid, "4420");
        assert_eq!(spec.subnqn, "nqn.2014-08.org.nvmexpress:uuid");
        assert_eq!(spec.ns, 1);
    }

    #[test]
    fn test_parse_nof_transport_spec_accepts_ipv6_uri_endpoint() {
        let spec = parse_nof_transport_spec("nvme+tcp://[::1]:4420/nqn.test")
            .unwrap()
            .unwrap();

        assert_eq!(spec.traddr, "::1");
        assert_eq!(spec.trsvcid, "4420");
        assert_eq!(spec.subnqn, "nqn.test");
    }

    #[test]
    fn test_parse_nof_probe_target_rejects_partial_key_value_endpoint() {
        let err = parse_nof_probe_target("trtype=tcp traddr=127.0.0.1").unwrap_err();
        assert!(err.contains("trsvcid"));
    }

    #[cfg(unix)]
    #[test]
    fn test_run_nof_probe_command_reports_success_and_failure() {
        run_nof_probe_command(
            "/usr/bin/true",
            "tcp://127.0.0.1:4420",
            Duration::from_secs(1),
        )
        .unwrap();
        let err = run_nof_probe_command(
            "/usr/bin/false",
            "tcp://127.0.0.1:4420",
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(err.contains("failed"));
    }
}
