//! C++-compatible Store configuration and network utility helpers.

use regex::Regex;
use std::ffi::CStr;
use std::net::Ipv4Addr;
use std::sync::OnceLock;

pub fn byte_size_to_string(bytes: u64) -> String {
    if bytes == i64::MAX as u64 {
        return "infinite".to_string();
    }
    const UNITS: [(u64, &str); 4] = [
        (1_u64 << 40, "TB"),
        (1_u64 << 30, "GB"),
        (1_u64 << 20, "MB"),
        (1_u64 << 10, "KB"),
    ];
    for (divisor, unit) in UNITS {
        if bytes >= divisor {
            return format!("{:.2} {unit}", bytes as f64 / divisor as f64);
        }
    }
    format!("{bytes} B")
}

pub fn try_string_to_byte_size(input: &str) -> Option<u64> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    if input == "infinite" {
        return Some(u64::MAX);
    }
    static NUMBER: OnceLock<Regex> = OnceLock::new();
    let number = NUMBER.get_or_init(|| {
        Regex::new(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
            .expect("byte-size number regex is valid")
    });
    let numeric = number.find(input)?;
    let value = numeric.as_str().parse::<f64>().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let unit = input[numeric.end()..].trim_start().to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "" | "B" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let bytes = value * multiplier;
    (bytes <= u64::MAX as f64).then_some(bytes as u64)
}

pub fn string_to_bool(input: &str) -> Option<bool> {
    match input.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub fn get_interface_ipv4_address(interface_name: &str) -> Result<String, String> {
    if interface_name.is_empty() {
        return Err("network interface name is empty".to_string());
    }
    let mut interfaces = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut interfaces) } != 0 {
        return Err(format!(
            "getifaddrs failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    struct InterfaceList(*mut libc::ifaddrs);
    impl Drop for InterfaceList {
        fn drop(&mut self) {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let interfaces = InterfaceList(interfaces);
    let mut current = interfaces.0;
    let mut found = false;
    let mut is_up = false;
    while !current.is_null() {
        let entry = unsafe { &*current };
        if !entry.ifa_name.is_null()
            && unsafe { CStr::from_ptr(entry.ifa_name) }.to_bytes() == interface_name.as_bytes()
        {
            found = true;
            if entry.ifa_flags & libc::IFF_UP as u32 != 0 {
                is_up = true;
                if !entry.ifa_addr.is_null()
                    && unsafe { (*entry.ifa_addr).sa_family as i32 } == libc::AF_INET
                {
                    let address = unsafe { &*(entry.ifa_addr.cast::<libc::sockaddr_in>()) };
                    return Ok(Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()).to_string());
                }
            }
        }
        current = entry.ifa_next;
    }
    if !found {
        Err(format!(
            "network interface '{interface_name}' was not found"
        ))
    } else if !is_up {
        Err(format!("network interface '{interface_name}' is down"))
    } else {
        Err(format!(
            "network interface '{interface_name}' has no IPv4 address"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_size_formatting_matches_cpp() {
        let cases = [
            (999, "999 B"),
            (2048, "2.00 KB"),
            (5 * 1024 * 1024 + 1234, "5.00 MB"),
            (15_u64 * 1024 * 1024 * 1024, "15.00 GB"),
            (0, "0 B"),
            (1, "1 B"),
            (1024, "1.00 KB"),
            (1024 * 1024, "1.00 MB"),
            (1024_u64 * 1024 * 1024, "1.00 GB"),
            (1024_u64 * 1024 * 1024 * 1024, "1.00 TB"),
            (15 * 1024 + 134, "15.13 KB"),
            (15 * 1024 * 1024 + 44048, "15.04 MB"),
        ];
        for (bytes, expected) in cases {
            assert_eq!(byte_size_to_string(bytes), expected);
        }
        assert_eq!(byte_size_to_string(i64::MAX as u64), "infinite");
    }

    #[test]
    fn byte_size_parsing_matches_cpp() {
        assert_eq!(try_string_to_byte_size("16 MB"), Some(16 * 1024 * 1024));
        assert_eq!(try_string_to_byte_size("1.5g"), Some(3 * (1_u64 << 29)));
        assert_eq!(try_string_to_byte_size("0"), Some(0));
        assert_eq!(try_string_to_byte_size("infinite"), Some(u64::MAX));
        assert_eq!(try_string_to_byte_size("-5"), None);
        assert_eq!(try_string_to_byte_size("16XB"), None);
    }

    #[test]
    fn tri_state_bool_parsing_matches_cpp() {
        for value in ["1", "true", "YES", " on "] {
            assert_eq!(string_to_bool(value), Some(true));
        }
        for value in ["0", "false", "No", " off "] {
            assert_eq!(string_to_bool(value), Some(false));
        }
        assert_eq!(string_to_bool("maybe"), None);
        assert_eq!(string_to_bool(""), None);
    }

    #[test]
    fn loopback_interface_resolves_to_ipv4() {
        assert_eq!(get_interface_ipv4_address("lo").unwrap(), "127.0.0.1");
    }

    #[test]
    fn missing_interface_reports_not_found_context() {
        let error = get_interface_ipv4_address("mooncake_missing_if").unwrap_err();
        assert!(error.contains("not found"));
        assert!(error.contains("mooncake_missing_if"));
    }
}
