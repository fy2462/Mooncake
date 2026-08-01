use mooncake_store_core::{StoreError, error::StoreResult};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_MIN_PORT: u16 = 12_300;
const DEFAULT_MAX_PORT: u16 = 14_300;
const DEFAULT_SETUP_RETRIES: usize = 20;
const BINDER_ATTEMPTS_PER_SETUP: usize = 20;
const MIN_ALLOWED_PORT: u16 = 1024;
const EPHEMERAL_PORT_START: u16 = 32_768;
const EPHEMERAL_PORT_END: u16 = 60_999;

pub(super) struct ResolvedClientEndpoint {
    pub(super) server_name: String,
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) reservation: Option<PortReservation>,
}

pub(crate) struct PortReservation {
    _socket: Socket,
}

impl ResolvedClientEndpoint {
    pub(super) fn from_explicit(server_name: &str) -> StoreResult<Self> {
        let parsed = ParsedClientEndpoint::parse(server_name)?;
        let port = parsed.port.ok_or_else(|| {
            StoreError::InvalidParams(
                "external Transfer Engine local_host must include an explicit port".to_string(),
            )
        })?;
        Ok(Self {
            server_name: format_host_port(&parsed.host, port),
            host: parsed.host,
            port,
            reservation: None,
        })
    }

    pub(super) fn from_environment(server_name: &str) -> StoreResult<Self> {
        let parsed = ParsedClientEndpoint::parse(server_name)?;
        if let Some(port) = parsed.port {
            return Ok(Self {
                server_name: format_host_port(&parsed.host, port),
                host: parsed.host,
                port,
                reservation: None,
            });
        }

        let (min_port, max_port) = validated_port_range(
            parse_env_u16("MC_STORE_CLIENT_MIN_PORT"),
            parse_env_u16("MC_STORE_CLIENT_MAX_PORT"),
        );
        let retries = validated_setup_retries(parse_env_usize("MC_STORE_CLIENT_SETUP_RETRIES"));
        reserve_endpoint(parsed.host, min_port, max_port, retries)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedClientEndpoint {
    host: String,
    port: Option<u16>,
}

impl ParsedClientEndpoint {
    fn parse(server_name: &str) -> StoreResult<Self> {
        let server_name = server_name.trim();
        if server_name.is_empty() {
            return Err(StoreError::InvalidParams(
                "local_host must not be empty".to_string(),
            ));
        }

        if let Some(bracketed) = server_name.strip_prefix('[') {
            let closing = bracketed.find(']').ok_or_else(|| {
                StoreError::InvalidParams(format!(
                    "invalid bracketed IPv6 local_host: {server_name}"
                ))
            })?;
            let host = &bracketed[..closing];
            if host.is_empty() {
                return Err(StoreError::InvalidParams(
                    "local_host must not contain an empty IPv6 address".to_string(),
                ));
            }
            let suffix = &bracketed[closing + 1..];
            let port = if suffix.is_empty() {
                None
            } else {
                let raw_port = suffix.strip_prefix(':').ok_or_else(|| {
                    StoreError::InvalidParams(format!(
                        "invalid bracketed IPv6 local_host suffix: {server_name}"
                    ))
                })?;
                Some(parse_explicit_port(raw_port, server_name)?)
            };
            return Ok(Self {
                host: host.to_string(),
                port,
            });
        }

        let colon_count = server_name.bytes().filter(|byte| *byte == b':').count();
        match colon_count {
            0 => Ok(Self {
                host: server_name.to_string(),
                port: None,
            }),
            1 => {
                let (host, raw_port) = server_name.rsplit_once(':').unwrap();
                if host.is_empty() {
                    return Err(StoreError::InvalidParams(
                        "local_host must not contain an empty hostname".to_string(),
                    ));
                }
                Ok(Self {
                    host: host.to_string(),
                    port: Some(parse_explicit_port(raw_port, server_name)?),
                })
            }
            _ => Ok(Self {
                // An unbracketed string with multiple colons is an IPv6 host
                // without an explicit port. IPv6 endpoints with a port must
                // use the standard `[address]:port` form.
                host: server_name.to_string(),
                port: None,
            }),
        }
    }
}

fn parse_explicit_port(raw_port: &str, server_name: &str) -> StoreResult<u16> {
    raw_port
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "local_host contains an invalid explicit port: {server_name}"
            ))
        })
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn parse_env_u16(name: &str) -> Option<u16> {
    std::env::var(name).ok()?.parse::<u16>().ok()
}

fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse::<usize>().ok()
}

fn validated_port_range(min_port: Option<u16>, max_port: Option<u16>) -> (u16, u16) {
    let min_port = min_port.unwrap_or(DEFAULT_MIN_PORT);
    let max_port = max_port.unwrap_or(DEFAULT_MAX_PORT);
    let valid = |port: u16| {
        port >= MIN_ALLOWED_PORT && !(EPHEMERAL_PORT_START..=EPHEMERAL_PORT_END).contains(&port)
    };
    if valid(min_port) && valid(max_port) && min_port <= max_port {
        (min_port, max_port)
    } else {
        tracing::warn!(
            min_port,
            max_port,
            default_min_port = DEFAULT_MIN_PORT,
            default_max_port = DEFAULT_MAX_PORT,
            "invalid MC_STORE_CLIENT port range; using C++ defaults"
        );
        (DEFAULT_MIN_PORT, DEFAULT_MAX_PORT)
    }
}

fn validated_setup_retries(retries: Option<usize>) -> usize {
    retries
        .filter(|retries| *retries > 0)
        .unwrap_or(DEFAULT_SETUP_RETRIES)
}

fn reserve_endpoint(
    host: String,
    min_port: u16,
    max_port: u16,
    retries: usize,
) -> StoreResult<ResolvedClientEndpoint> {
    let range_len = usize::from(max_port - min_port) + 1;
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos() as usize)
        ^ std::process::id() as usize;
    let attempts = retries
        .saturating_mul(BINDER_ATTEMPTS_PER_SETUP)
        .min(range_len);
    let mut last_error = None;

    for attempt in 0..attempts {
        let offset = (seed + attempt) % range_len;
        let port = min_port + offset as u16;
        match reserve_port(port) {
            Ok(reservation) => {
                return Ok(ResolvedClientEndpoint {
                    server_name: format_host_port(&host, port),
                    host,
                    port,
                    reservation: Some(reservation),
                });
            }
            Err(error) => last_error = Some(error),
        }
    }

    Err(StoreError::Internal(format!(
        "failed to reserve a client port in {min_port}..={max_port} after {retries} setup retries ({attempts} bind attempts){}",
        last_error.map_or_else(String::new, |error| format!(": {error}"))
    )))
}

fn reserve_port(port: u16) -> std::io::Result<PortReservation> {
    // Match C++ AutoPortBinder: bind an IPv4 stream socket without listening,
    // and retain it for the full client lifetime solely as a port reservation.
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    let address = SockAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));
    socket.bind(&address)?;
    Ok(PortReservation { _socket: socket })
}

#[cfg(test)]
mod tests {
    use super::{
        ParsedClientEndpoint, ResolvedClientEndpoint, format_host_port, reserve_endpoint,
        validated_port_range, validated_setup_retries,
    };
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

    #[test]
    fn parses_hostname_ipv4_and_ipv6_endpoints() {
        assert_eq!(
            ParsedClientEndpoint::parse("node-a").unwrap(),
            ParsedClientEndpoint {
                host: "node-a".to_string(),
                port: None,
            }
        );
        assert_eq!(
            ParsedClientEndpoint::parse("10.0.0.1:12300").unwrap(),
            ParsedClientEndpoint {
                host: "10.0.0.1".to_string(),
                port: Some(12300),
            }
        );
        assert_eq!(
            ParsedClientEndpoint::parse("2001:db8::1").unwrap(),
            ParsedClientEndpoint {
                host: "2001:db8::1".to_string(),
                port: None,
            }
        );
        assert_eq!(
            ParsedClientEndpoint::parse("[2001:db8::1]:12300").unwrap(),
            ParsedClientEndpoint {
                host: "2001:db8::1".to_string(),
                port: Some(12300),
            }
        );
        assert_eq!(
            format_host_port("2001:db8::1", 12300),
            "[2001:db8::1]:12300"
        );
    }

    #[test]
    fn explicit_and_bare_ipv4_derive_bindable_rpc_host() {
        for server_name in ["127.0.0.1:18007", "127.0.0.1"] {
            let endpoint = ResolvedClientEndpoint::from_environment(server_name).unwrap();
            assert_eq!(endpoint.host, "127.0.0.1");
            let listener = TcpListener::bind((endpoint.host.as_str(), 0)).unwrap();
            assert_eq!(listener.local_addr().unwrap().ip(), Ipv4Addr::LOCALHOST);
        }
    }

    #[test]
    fn rejects_malformed_explicit_endpoints() {
        assert!(ParsedClientEndpoint::parse("").is_err());
        assert!(ParsedClientEndpoint::parse("node:0").is_err());
        assert!(ParsedClientEndpoint::parse("node:not-a-port").is_err());
        assert!(ParsedClientEndpoint::parse("[2001:db8::1").is_err());
        assert!(ParsedClientEndpoint::parse("[2001:db8::1]junk").is_err());
    }

    #[test]
    fn external_engine_endpoint_requires_an_explicit_port() {
        assert!(ResolvedClientEndpoint::from_explicit("node-a").is_err());
        assert!(ResolvedClientEndpoint::from_explicit("2001:db8::1").is_err());
        assert_eq!(
            ResolvedClientEndpoint::from_explicit("node-a:12300")
                .unwrap()
                .server_name,
            "node-a:12300"
        );
    }

    #[test]
    fn invalid_or_ephemeral_range_uses_cpp_defaults() {
        assert_eq!(
            validated_port_range(Some(12300), Some(14300)),
            (12300, 14300)
        );
        assert_eq!(
            validated_port_range(Some(14300), Some(12300)),
            (12300, 14300)
        );
        assert_eq!(
            validated_port_range(Some(40000), Some(41000)),
            (12300, 14300)
        );
        assert_eq!(validated_port_range(Some(80), Some(100)), (12300, 14300));
        assert_eq!(validated_setup_retries(None), 20);
        assert_eq!(validated_setup_retries(Some(0)), 20);
        assert_eq!(validated_setup_retries(Some(7)), 7);
    }

    #[test]
    fn port_reservation_is_exclusive_and_releases_on_drop() {
        let probe = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let endpoint = reserve_endpoint("127.0.0.1".into(), port, port, 1).unwrap();
        assert_eq!(endpoint.port, port);
        assert!(TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).is_err());
        drop(endpoint);
        assert!(TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).is_ok());
    }

    #[test]
    fn port_reservation_detects_reuseaddr_listener_conflict() {
        let listener = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        listener.set_reuse_address(true).unwrap();
        listener
            .bind(&socket2::SockAddr::from(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                0,
            )))
            .unwrap();
        listener.listen(1).unwrap();
        let port = listener.local_addr().unwrap().as_socket().unwrap().port();

        assert!(reserve_endpoint("127.0.0.1".into(), port, port, 1).is_err());
        drop(listener);
        assert!(reserve_endpoint("127.0.0.1".into(), port, port, 1).is_ok());
    }

    #[test]
    fn multiple_reservations_choose_distinct_ports_in_requested_range() {
        let first = reserve_endpoint("127.0.0.1".into(), 20_000, 20_100, 20).unwrap();
        let second = reserve_endpoint("127.0.0.1".into(), 20_000, 20_100, 20).unwrap();

        assert!((20_000..=20_100).contains(&first.port));
        assert!((20_000..=20_100).contains(&second.port));
        assert_ne!(first.port, second.port);
    }
}
