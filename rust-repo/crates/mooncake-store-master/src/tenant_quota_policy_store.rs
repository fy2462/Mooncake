use crate::TenantId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_CLUSTER_ID: &str = "mooncake_cluster";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuotaPolicySnapshot {
    #[serde(default)]
    pub tenant_quotas: BTreeMap<String, u64>,
}

pub fn load_tenant_quota_policy(
    connector_type: &str,
    connector_uri: &str,
    cluster_id: &str,
) -> Result<TenantQuotaPolicySnapshot, String> {
    match connector_type {
        "file" => {
            require_tenant_quota_connector_uri("file", connector_uri)?;
            load_file_policy(connector_uri)
        }
        "etcd" => {
            require_tenant_quota_connector_uri("etcd", connector_uri)?;
            load_etcd_policy(connector_uri, cluster_id)
        }
        other => Err(format!("unsupported tenant quota connector type: {other}")),
    }
}

pub fn save_tenant_quota_policy(
    connector_type: &str,
    connector_uri: &str,
    cluster_id: &str,
    snapshot: &TenantQuotaPolicySnapshot,
) -> Result<(), String> {
    match connector_type {
        "file" => {
            require_tenant_quota_connector_uri("file", connector_uri)?;
            save_file_policy(connector_uri, snapshot)
        }
        "etcd" => {
            require_tenant_quota_connector_uri("etcd", connector_uri)?;
            save_etcd_policy(connector_uri, cluster_id, snapshot)
        }
        other => Err(format!("unsupported tenant quota connector type: {other}")),
    }
}

fn require_tenant_quota_connector_uri(
    connector_type: &str,
    connector_uri: &str,
) -> Result<(), String> {
    if connector_uri.trim().is_empty() {
        return Err(format!(
            "tenant quota {connector_type} connector requires a non-empty uri"
        ));
    }
    Ok(())
}

fn load_file_policy(path: &str) -> Result<TenantQuotaPolicySnapshot, String> {
    if !Path::new(path).exists() {
        return Ok(TenantQuotaPolicySnapshot::default());
    }
    let contents = fs::read_to_string(path).map_err(|e| format!("failed to read {path}: {e}"))?;
    if contents.trim().is_empty() {
        return Ok(TenantQuotaPolicySnapshot::default());
    }
    parse_tenant_quota_policy_yaml(&contents).map_err(|e| format!("failed to parse {path}: {e}"))
}

fn save_file_policy(path: &str, snapshot: &TenantQuotaPolicySnapshot) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
    }
    let yaml = format_tenant_quota_policy_yaml(snapshot);
    write_atomic_file(path, yaml.as_bytes())
}

fn load_etcd_policy(
    endpoints: &str,
    cluster_id: &str,
) -> Result<TenantQuotaPolicySnapshot, String> {
    let key = build_tenant_quota_etcd_key(cluster_id)?;
    let endpoints = parse_etcd_endpoints(endpoints)?;
    let content = run_etcd_blocking(move || async move {
        let mut client = etcd_client::Client::connect(endpoints, None)
            .await
            .map_err(|e| format!("failed to connect tenant quota etcd store: {e}"))?;
        let response = client.get(key.as_bytes(), None).await.map_err(|e| {
            format!("failed to load tenant quota policy from etcd key '{key}': {e}")
        })?;
        response
            .kvs()
            .first()
            .map(|kv| decode_etcd_policy_content(&key, kv.value()))
            .transpose()
    })?;
    match content {
        Some(content) => parse_tenant_quota_policy_yaml(&content),
        None => Ok(TenantQuotaPolicySnapshot::default()),
    }
}

fn decode_etcd_policy_content(key: &str, value: &[u8]) -> Result<String, String> {
    String::from_utf8(value.to_vec()).map_err(|error| {
        format!("tenant quota policy from etcd key '{key}' is not valid UTF-8: {error}")
    })
}

fn save_etcd_policy(
    endpoints: &str,
    cluster_id: &str,
    snapshot: &TenantQuotaPolicySnapshot,
) -> Result<(), String> {
    let key = build_tenant_quota_etcd_key(cluster_id)?;
    let endpoints = parse_etcd_endpoints(endpoints)?;
    let content = format_tenant_quota_policy_yaml(snapshot);
    run_etcd_blocking(move || async move {
        let mut client = etcd_client::Client::connect(endpoints, None)
            .await
            .map_err(|e| format!("failed to connect tenant quota etcd store: {e}"))?;
        client
            .put(key.as_bytes(), content.as_bytes(), None)
            .await
            .map_err(|e| format!("failed to save tenant quota policy to etcd key '{key}': {e}"))?;
        Ok(())
    })
}

fn run_etcd_blocking<F, Fut, T>(operation: F) -> Result<T, String>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, String>> + Send + 'static,
    T: Send + 'static,
{
    thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create tenant quota etcd runtime: {e}"))?
            .block_on(operation())
    })
    .join()
    .map_err(|_| "tenant quota etcd worker panicked".to_string())?
}

fn parse_etcd_endpoints(endpoints: &str) -> Result<Vec<String>, String> {
    let endpoints = endpoints
        .split(';')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err("tenant quota etcd connector requires a non-empty uri".to_string());
    }
    Ok(endpoints)
}

fn build_tenant_quota_etcd_key(cluster_id: &str) -> Result<String, String> {
    Ok(format!(
        "mooncake-store/{}/tenant_quota_policy",
        normalize_cluster_id_for_etcd_key(cluster_id)?
    ))
}

fn normalize_cluster_id_for_etcd_key(cluster_id: &str) -> Result<String, String> {
    let mut normalized = if cluster_id.is_empty() {
        DEFAULT_CLUSTER_ID.to_string()
    } else {
        cluster_id.to_string()
    };
    while normalized.ends_with('/') {
        normalized.pop();
    }
    if normalized.is_empty() {
        normalized = DEFAULT_CLUSTER_ID.to_string();
    }
    if !is_valid_cluster_id_component(&normalized) {
        return Err(format!(
            "invalid tenant quota etcd cluster_id '{cluster_id}'"
        ));
    }
    Ok(normalized)
}

fn is_valid_cluster_id_component(cluster_id: &str) -> bool {
    !cluster_id.is_empty()
        && cluster_id.len() <= 128
        && cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn write_atomic_file(path: &str, contents: &[u8]) -> Result<(), String> {
    let target = Path::new(path);
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let tmp_path = make_temp_path(target);
    let mut tmp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .map_err(|e| format!("failed to open {}: {e}", tmp_path.display()))?;

    if let Err(error) = tmp.write_all(contents).and_then(|_| tmp.sync_all()) {
        let _ = fs::remove_file(&tmp_path);
        return Err(format!("failed to write {}: {error}", tmp_path.display()));
    }
    drop(tmp);

    if let Err(error) = fs::rename(&tmp_path, target) {
        let _ = fs::remove_file(&tmp_path);
        return Err(format!(
            "failed to rename {} to {path}: {error}",
            tmp_path.display()
        ));
    }

    if let Some(parent) = parent {
        let _ = File::open(parent).and_then(|dir| dir.sync_all());
    }
    Ok(())
}

fn make_temp_path(target: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    let thread_hash = hasher.finish();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tenant_quota_policy");
    target.with_file_name(format!(
        "{file_name}.tmp.{}.{}.{}",
        std::process::id(),
        thread_hash,
        now
    ))
}

fn parse_tenant_quota_policy_yaml(contents: &str) -> Result<TenantQuotaPolicySnapshot, String> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(contents).map_err(|e| format!("invalid YAML: {e}"))?;
    if value.get("tenant_quotas").is_some() {
        return parse_legacy_tenant_quota_policy(value);
    }
    parse_cpp_tenant_quota_policy(value)
}

fn parse_legacy_tenant_quota_policy(
    value: serde_yaml::Value,
) -> Result<TenantQuotaPolicySnapshot, String> {
    let snapshot: TenantQuotaPolicySnapshot = serde_yaml::from_value(value)
        .map_err(|e| format!("invalid legacy tenant quota policy: {e}"))?;
    let mut tenant_quotas = BTreeMap::new();
    for (tenant_id, quota) in snapshot.tenant_quotas {
        if quota == 0 {
            return Err(format!(
                "invalid quota for tenant '{tenant_id}': quota must be positive"
            ));
        }
        let tenant_id = normalize_policy_tenant_id(&tenant_id)?;
        if tenant_quotas.insert(tenant_id.clone(), quota).is_some() {
            return Err(format!("duplicate tenant name '{tenant_id}'"));
        }
    }
    Ok(TenantQuotaPolicySnapshot { tenant_quotas })
}

fn parse_cpp_tenant_quota_policy(
    value: serde_yaml::Value,
) -> Result<TenantQuotaPolicySnapshot, String> {
    let root = value
        .as_mapping()
        .ok_or_else(|| "tenant quota policy must be a YAML map".to_string())?;
    let version = root
        .get(serde_yaml::Value::String("version".to_string()))
        .ok_or_else(|| "tenant quota policy version is required".to_string())?
        .as_i64()
        .ok_or_else(|| "invalid version".to_string())?;
    if version != 1 {
        return Err(format!(
            "unsupported tenant quota policy version: {version}"
        ));
    }

    let tenants = root
        .get(serde_yaml::Value::String("tenants".to_string()))
        .ok_or_else(|| "tenants must be a YAML sequence".to_string())?
        .as_sequence()
        .ok_or_else(|| "tenants must be a YAML sequence".to_string())?;

    let mut tenant_quotas = BTreeMap::new();
    for entry in tenants {
        let entry = entry
            .as_mapping()
            .ok_or_else(|| "tenant entry must be a YAML map".to_string())?;
        let name = entry
            .get(serde_yaml::Value::String("name".to_string()))
            .and_then(serde_yaml::Value::as_str)
            .ok_or_else(|| "tenant name is required".to_string())?;
        let quota = entry
            .get(serde_yaml::Value::String("quota".to_string()))
            .ok_or_else(|| "tenant quota is required".to_string())
            .and_then(parse_quota_value)
            .map_err(|e| format!("invalid quota for tenant '{name}': {e}"))?;
        let tenant_id = normalize_policy_tenant_id(name)?;
        if tenant_quotas.insert(tenant_id.clone(), quota).is_some() {
            return Err(format!("duplicate tenant name '{tenant_id}'"));
        }
    }

    Ok(TenantQuotaPolicySnapshot { tenant_quotas })
}

fn parse_quota_value(value: &serde_yaml::Value) -> Result<u64, String> {
    match value {
        serde_yaml::Value::Number(number) => number
            .as_u64()
            .filter(|quota| *quota > 0)
            .ok_or_else(|| "quota must be positive".to_string()),
        serde_yaml::Value::String(value) => parse_tenant_quota_bytes(value),
        _ => Err("tenant quota must be a scalar".to_string()),
    }
}

fn parse_tenant_quota_bytes(value: &str) -> Result<u64, String> {
    if value.is_empty() {
        return Err("quota must not be empty".to_string());
    }
    let digits = value
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits == 0 {
        return Err("quota must start with an integer".to_string());
    }
    let number = value[..digits]
        .parse::<u64>()
        .map_err(|_| "quota integer overflows uint64".to_string())?;
    if number == 0 {
        return Err("quota must be positive".to_string());
    }
    let multiplier = match &value[digits..] {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024_u64.pow(2),
        "GB" => 1024_u64.pow(3),
        "TB" => 1024_u64.pow(4),
        unit => return Err(format!("unsupported quota unit '{unit}'")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "quota byte value overflows uint64".to_string())
}

fn normalize_policy_tenant_id(tenant_id: &str) -> Result<String, String> {
    TenantId::new(tenant_id.to_owned())
        .map(TenantId::into_string)
        .map_err(|_| format!("invalid tenant name '{tenant_id}'"))
}

fn format_tenant_quota_policy_yaml(snapshot: &TenantQuotaPolicySnapshot) -> String {
    let mut out = String::from("version: 1\n\n");
    if snapshot.tenant_quotas.is_empty() {
        out.push_str("tenants: []\n");
        return out;
    }
    out.push_str("tenants:\n");
    for (tenant_id, quota) in &snapshot.tenant_quotas {
        out.push_str("  - name: ");
        out.push_str(&quote_yaml_double_quoted_scalar(tenant_id));
        out.push('\n');
        out.push_str("    quota: ");
        out.push_str(&quota.to_string());
        out.push('\n');
    }
    out
}

fn quote_yaml_double_quoted_scalar(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\0' => out.push_str("\\0"),
            '\u{0007}' => out.push_str("\\a"),
            '\u{0008}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{000b}' => out.push_str("\\v"),
            '\u{000c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            ch if ch < ' ' || ch == '\u{007f}' => {
                out.push_str(&format!("\\x{:02X}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpp_tenant_quota_yaml_with_units() {
        let snapshot = parse_tenant_quota_policy_yaml(
            r#"
version: 1
tenants:
  - name: tenant-a
    quota: 1KB
  - name: tenant-b
    quota: "2MB"
"#,
        )
        .unwrap();

        assert_eq!(snapshot.tenant_quotas["tenant-a"], 1024);
        assert_eq!(snapshot.tenant_quotas["tenant-b"], 2 * 1024 * 1024);
    }

    #[test]
    fn formats_cpp_compatible_tenant_quota_yaml() {
        let snapshot = TenantQuotaPolicySnapshot {
            tenant_quotas: BTreeMap::from([
                ("tenant-a".to_string(), 1024),
                ("tenant\"b".to_string(), 2048),
            ]),
        };

        let yaml = format_tenant_quota_policy_yaml(&snapshot);
        assert_eq!(
            yaml,
            "version: 1\n\ntenants:\n  - name: \"tenant\\\"b\"\n    quota: 2048\n  - name: \"tenant-a\"\n    quota: 1024\n"
        );
        assert_eq!(parse_tenant_quota_policy_yaml(&yaml).unwrap(), snapshot);
    }

    #[test]
    fn rejects_invalid_cpp_tenant_quota_policy() {
        let err = parse_tenant_quota_policy_yaml(
            r#"
version: 1
tenants:
  - name: _internal
    quota: 1GB
"#,
        )
        .unwrap_err();
        assert!(err.contains("invalid tenant name"));

        let err = parse_tenant_quota_policy_yaml(
            r#"
version: 1
tenants:
  - name: tenant-a
    quota: 0
"#,
        )
        .unwrap_err();
        assert!(err.contains("quota must be positive"));
    }

    #[test]
    fn reads_legacy_rust_tenant_quota_yaml() {
        let snapshot = parse_tenant_quota_policy_yaml(
            r#"
tenant_quotas:
  tenant-a: 4096
"#,
        )
        .unwrap();

        assert_eq!(snapshot.tenant_quotas["tenant-a"], 4096);
    }

    #[test]
    fn file_save_uses_cpp_yaml_and_cleans_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tenant-policy.yaml");
        let path_str = path.to_string_lossy();
        let snapshot = TenantQuotaPolicySnapshot {
            tenant_quotas: BTreeMap::from([("tenant-a".to_string(), 4096)]),
        };

        save_file_policy(&path_str, &snapshot).unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents,
            "version: 1\n\ntenants:\n  - name: \"tenant-a\"\n    quota: 4096\n"
        );
        assert_eq!(load_file_policy(&path_str).unwrap(), snapshot);
        let leftovers = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
            .count();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn builds_cpp_compatible_tenant_quota_etcd_key() {
        assert_eq!(
            build_tenant_quota_etcd_key("cluster-a").unwrap(),
            "mooncake-store/cluster-a/tenant_quota_policy"
        );
        assert_eq!(
            build_tenant_quota_etcd_key("cluster-a///").unwrap(),
            "mooncake-store/cluster-a/tenant_quota_policy"
        );
        assert_eq!(
            build_tenant_quota_etcd_key("").unwrap(),
            "mooncake-store/mooncake_cluster/tenant_quota_policy"
        );
        assert!(build_tenant_quota_etcd_key("../bad").is_err());
        assert!(build_tenant_quota_etcd_key("bad/cluster").is_err());
    }

    #[test]
    fn parses_tenant_quota_etcd_endpoints() {
        assert_eq!(
            parse_etcd_endpoints("127.0.0.1:2379; http://etcd:2379 ;").unwrap(),
            vec!["127.0.0.1:2379", "http://etcd:2379"]
        );
        assert!(parse_etcd_endpoints(" ; ").is_err());
    }

    #[test]
    fn rejects_invalid_etcd_cluster_id_before_connecting() {
        let err = load_tenant_quota_policy("etcd", "127.0.0.1:2379", "bad/cluster").unwrap_err();
        assert!(err.contains("invalid tenant quota etcd cluster_id"));
    }

    #[test]
    fn rejects_invalid_utf8_etcd_policy_bytes_with_key_context() {
        let key = "mooncake-store/cluster-a/tenant_quota_policy";
        let error = decode_etcd_policy_content(key, &[0xff, 0xfe]).unwrap_err();

        assert!(error.contains(key), "{error}");
        assert!(error.contains("valid UTF-8"), "{error}");
        assert!(!error.contains('\u{fffd}'), "{error}");
    }
}
