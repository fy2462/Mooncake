// ============================================================================
// Configuration loading for mooncake-conductor.
// mooncake-conductor 的配置加载。
//
// Reads a JSON config file specified by `CONDUCTOR_CONFIG_PATH` env var.
// Mirrors the Go conductor's config parsing in main.go.
// 从 `CONDUCTOR_CONFIG_PATH` 环境变量指定的 JSON 文件加载配置，
// 对应 Go conductor main.go 中的配置解析。
// ============================================================================

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use tracing::{error, info, warn};

use crate::types::ServiceConfig;

// ----------------------------------------------------------------------------
// Config file schema / 配置文件结构
// ----------------------------------------------------------------------------

/// Top-level JSON config file structure matching the Go conductor format.
/// 顶层 JSON 配置文件结构，匹配 Go conductor 格式。
#[derive(Debug, Deserialize)]
struct ConfigFile {
    /// Map of instance name → service configuration.
    /// 实例名称 → 服务配置的映射。
    #[serde(default)]
    kvevent_instance: HashMap<String, ServiceRaw>,
    /// Optional HTTP server port (default: 13333).
    /// 可选的 HTTP 服务器端口（默认: 13333）。
    #[serde(default)]
    http_server_port: Option<u16>,
}

/// Raw per-instance entry from the config JSON, before normalization.
/// 配置文件中的原始单实例条目，规范化前。
#[derive(Debug, Deserialize)]
struct ServiceRaw {
    /// ZMQ PUB endpoint. / ZMQ PUB 端点。
    #[serde(default)]
    endpoint: String,
    /// ZMQ DEALER replay endpoint. / ZMQ DEALER 重放端点。
    #[serde(default)]
    replay_endpoint: String,
    /// Service type string: "vLLM" or "Mooncake". / 服务类型字符串。
    #[serde(rename = "type", default)]
    type_str: String,
    /// Model name. / 模型名称。
    #[serde(default)]
    modelname: String,
    /// LoRA name. / LoRA 名称。
    #[serde(default)]
    lora_name: String,
    /// Tenant ID. / 租户 ID。
    #[serde(default)]
    tenant_id: String,
    /// Instance ID (falls back to the JSON key if empty).
    /// 实例 ID（为空时回退到 JSON key）。
    #[serde(default)]
    instance_id: String,
    /// Block size in tokens. / 块大小。
    #[serde(default)]
    block_size: i64,
    /// Data parallel rank. / 数据并行 rank。
    #[serde(default)]
    dp_rank: i64,
    /// Additional hash salt. / 额外哈希盐值。
    #[serde(default)]
    additionalsalt: String,
}

// ----------------------------------------------------------------------------
// Parsed config / 解析后配置
// ----------------------------------------------------------------------------

/// Fully parsed and normalized conductor configuration.
/// 完全解析并规范化后的 conductor 配置。
#[derive(Debug, Clone)]
pub struct ConductorConfig {
    /// List of service configurations for ZMQ subscriptions.
    /// ZMQ 订阅的服务配置列表。
    pub services: Vec<ServiceConfig>,
    /// HTTP server listen port. / HTTP 服务器监听端口。
    pub http_port: u16,
}

// ----------------------------------------------------------------------------
// Helpers / 辅助函数
// ----------------------------------------------------------------------------

/// Map raw service type string to canonical form.
/// 将原始服务类型字符串映射为规范形式。
fn map_service_type(s: &str) -> Option<String> {
    match s {
        "vLLM" => Some("vLLM".to_string()),
        "Mooncake" => Some("Mooncake".to_string()),
        _ => None,
    }
}

// ----------------------------------------------------------------------------
// load_config / 加载配置
// ----------------------------------------------------------------------------

/// Load config from `CONDUCTOR_CONFIG_PATH` env var (defaults to
/// `/root/conductor_config.json`). Returns a `ConductorConfig` with parsed
/// services and HTTP port.
///
/// 从 `CONDUCTOR_CONFIG_PATH` 环境变量加载配置（默认为 `/root/conductor_config.json`）。
/// 返回包含解析后服务和 HTTP 端口的 `ConductorConfig`。
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
        // Go 风格：如果 instance_id 字段为空，用 map key 作为 instance_id
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

/// Parse log level from `CONDUCTOR_LOG_LEVEL` env var (default: "info").
/// 从 `CONDUCTOR_LOG_LEVEL` 环境变量解析日志级别（默认: "info"）。
pub fn parse_log_level() -> String {
    std::env::var("CONDUCTOR_LOG_LEVEL").unwrap_or_else(|_| "info".into())
}
