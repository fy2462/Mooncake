//! Configuration loading for mooncake-conductor.
//!
//! Reads a JSON config file specified by `CONDUCTOR_CONFIG_PATH`.
//! Mirrors the Go conductor's config parsing in main.go.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use tracing::{error, info, warn};

use crate::types::ServiceConfig;

/// Top-level JSON config file structure.
#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    kvevent_instance: HashMap<String, ServiceRaw>,
    #[serde(default)]
    http_server_port: Option<u16>,
}

/// Raw per-instance entry from config JSON.
#[derive(Debug, Deserialize)]
struct ServiceRaw {
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    replay_endpoint: String,
    #[serde(rename = "type", default)]
    type_str: String,
    #[serde(default)]
    modelname: String,
    #[serde(default)]
    lora_name: String,
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    instance_id: String,
    #[serde(default)]
    block_size: i64,
    #[serde(default)]
    dp_rank: i64,
    #[serde(default)]
    additionalsalt: String,
}

/// Parsed conductor configuration.
#[derive(Debug, Clone)]
pub struct ConductorConfig {
    pub services: Vec<ServiceConfig>,
    pub http_port: u16,
}

/// Map raw service type string to canonical form.
fn map_service_type(s: &str) -> Option<String> {
    match s {
        "vLLM" => Some("vLLM".to_string()),
        "Mooncake" => Some("Mooncake".to_string()),
        _ => None,
    }
}

/// Load config from `CONDUCTOR_CONFIG_PATH` env var.
///
/// Returns `(Vec<ServiceConfig>, http_port)`.
pub fn load_config() -> ConductorConfig {
    let config_path = std::env::var("CONDUCTOR_CONFIG_PATH")
        .unwrap_or_else(|_| "/root/conductor_config.json".into());

    let path = Path::new(&config_path);
    if !path.exists() {
        warn!(
            "Config file does not exist: {}. Starting with empty config.",
            config_path
        );
        return ConductorConfig {
            services: Vec::new(),
            http_port: 13333,
        };
    }

    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to read config file {}: {}", config_path, e);
            std::process::exit(1);
        }
    };

    let cfg: ConfigFile = match serde_json::from_str(&data) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to parse config JSON: {}", e);
            std::process::exit(1);
        }
    };

    let http_port = cfg.http_server_port.unwrap_or(13333);
    let mut services = Vec::with_capacity(cfg.kvevent_instance.len());

    for (name, raw) in cfg.kvevent_instance {
        let service_type = match map_service_type(&raw.type_str) {
            Some(t) => t,
            None => {
                error!("Unknown service type: {}", raw.type_str);
                continue;
            }
        };

        // Go: map key is used as instance_id if the instance_id field is empty
        let instance_id = if raw.instance_id.is_empty() {
            name.clone()
        } else {
            raw.instance_id
        };

        services.push(ServiceConfig {
            endpoint: raw.endpoint,
            replay_endpoint: raw.replay_endpoint,
            service_type,
            model_name: raw.modelname,
            lora_name: raw.lora_name,
            tenant_id: raw.tenant_id,
            instance_id,
            block_size: raw.block_size,
            dp_rank: raw.dp_rank,
            additional_salt: raw.additionalsalt,
        });
    }

    info!(
        "Loaded config: {} services, HTTP port {}",
        services.len(),
        http_port
    );

    ConductorConfig {
        services,
        http_port,
    }
}

/// Parse log level from `CONDUCTOR_LOG_LEVEL` env var.
pub fn parse_log_level() -> String {
    std::env::var("CONDUCTOR_LOG_LEVEL").unwrap_or_else(|_| "info".into())
}
