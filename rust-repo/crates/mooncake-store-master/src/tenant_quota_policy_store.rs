use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuotaPolicySnapshot {
    #[serde(default)]
    pub tenant_quotas: BTreeMap<String, u64>,
}

pub fn load_tenant_quota_policy(
    connector_type: &str,
    connector_uri: &str,
) -> Result<TenantQuotaPolicySnapshot, String> {
    if connector_uri.trim().is_empty() {
        return Ok(TenantQuotaPolicySnapshot::default());
    }
    match connector_type {
        "file" => load_file_policy(connector_uri),
        "etcd" => Err("Rust master tenant quota etcd connector is not implemented yet".to_string()),
        other => Err(format!("unsupported tenant quota connector type: {other}")),
    }
}

pub fn save_tenant_quota_policy(
    connector_type: &str,
    connector_uri: &str,
    snapshot: &TenantQuotaPolicySnapshot,
) -> Result<(), String> {
    if connector_uri.trim().is_empty() {
        return Ok(());
    }
    match connector_type {
        "file" => save_file_policy(connector_uri, snapshot),
        "etcd" => Err("Rust master tenant quota etcd connector is not implemented yet".to_string()),
        other => Err(format!("unsupported tenant quota connector type: {other}")),
    }
}

fn load_file_policy(path: &str) -> Result<TenantQuotaPolicySnapshot, String> {
    if !Path::new(path).exists() {
        return Ok(TenantQuotaPolicySnapshot::default());
    }
    let contents = fs::read_to_string(path).map_err(|e| format!("failed to read {path}: {e}"))?;
    if contents.trim().is_empty() {
        return Ok(TenantQuotaPolicySnapshot::default());
    }
    serde_yaml::from_str(&contents).map_err(|e| format!("failed to parse {path}: {e}"))
}

fn save_file_policy(path: &str, snapshot: &TenantQuotaPolicySnapshot) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
    }
    let yaml =
        serde_yaml::to_string(snapshot).map_err(|e| format!("failed to encode policy: {e}"))?;
    fs::write(path, yaml).map_err(|e| format!("failed to write {path}: {e}"))
}
