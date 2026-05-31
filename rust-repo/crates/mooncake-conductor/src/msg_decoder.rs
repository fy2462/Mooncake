// ============================================================================
// MessagePack Decoder — ZMQ 事件 MessagePack 解码器
//
// Decodes ZMQ MessagePack payloads into EventBatch structures. Supports two
// publisher protocols:
//   - Mooncake: BlockStoreEvent format with replica lists and mooncake keys.
//   - vLLM:     BlockStored format with DP rank and medium info.
//
// 将 ZMQ MessagePack 负载解码为 EventBatch 结构。支持两种发布者协议：
//   - Mooncake: BlockStoreEvent 格式，含副本列表和 mooncake key。
//   - vLLM:     BlockStored 格式，含 DP rank 和介质信息。
//
// Ported from Go: mooncake-conductor/conductor-ctrl/zmq/msg_decoder.go
// ============================================================================

use rmpv::Value;
use tracing::{debug, warn};

use crate::types::*;

// ============================================================================
// Low-level parse helpers / 底层解析辅助函数
// ============================================================================

/// Parse a MessagePack value as i64, accepting Integer/F64/F32 types.
/// 将 MessagePack 值解析为 i64，接受 Integer/F64/F32 类型。
fn parse_i64(v: &Value) -> Result<i64, String> {
    match v {
        Value::Integer(i) => i
            .as_i64()
            .ok_or_else(|| format!("cannot convert to i64: {:?}", i)),
        Value::F64(f) => Ok(*f as i64),
        Value::F32(f) => Ok(*f as i64),
        _ => Err(format!("unsupported i64 type: {:?}", v)),
    }
}

/// Parse a MessagePack value as u64, accepting Integer/F64/F32 types.
/// 将 MessagePack 值解析为 u64。
fn parse_u64(v: &Value) -> Result<u64, String> {
    match v {
        Value::Integer(i) => i
            .as_u64()
            .ok_or_else(|| format!("cannot convert to u64: {:?}", i)),
        Value::F64(f) => Ok(*f as u64),
        Value::F32(f) => Ok(*f as u64),
        _ => Err(format!("unsupported u64 type: {:?}", v)),
    }
}

/// Parse a MessagePack array into Vec<u64>. / 将 MessagePack 数组解析为 Vec<u64>。
fn parse_u64_array(v: &Value) -> Result<Vec<u64>, String> {
    match v {
        Value::Array(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for item in arr {
                result.push(parse_u64(item)?);
            }
            Ok(result)
        }
        _ => Err(format!("expected array, got {:?}", v)),
    }
}

/// Parse a MessagePack array into Vec<i32>. / 将 MessagePack 数组解析为 Vec<i32>。
fn parse_i32_array(v: &Value) -> Result<Vec<i32>, String> {
    match v {
        Value::Array(arr) => {
            let mut result = Vec::with_capacity(arr.len());
            for item in arr {
                let val = match item {
                    Value::Integer(i) => i.as_i64().unwrap_or(0) as i32,
                    Value::F64(f) => *f as i32,
                    Value::F32(f) => *f as i32,
                    _ => return Err(format!("unsupported i32 type: {:?}", item)),
                };
                result.push(val);
            }
            Ok(result)
        }
        _ => Err(format!("expected array, got {:?}", v)),
    }
}

/// Safely convert a MessagePack value to String, with lenient fallbacks.
/// 安全地将 MessagePack 值转换为 String，提供宽松的回退。
fn safe_get_string(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) => s
            .as_str()
            .map(|s| s.to_string())
            .ok_or("string not valid UTF-8".into()),
        Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                Ok(n.to_string())
            } else if let Some(n) = i.as_u64() {
                Ok(n.to_string())
            } else {
                Err("bad integer".into())
            }
        }
        Value::F64(f) => Ok(f.to_string()),
        Value::F32(f) => Ok(f.to_string()),
        Value::Nil => Ok(String::new()),
        _ => {
            warn!("Unexpected type in string field: {:?}", v);
            Ok(format!("{:?}", v))
        }
    }
}

// ============================================================================
// Mooncake-specific parsers / Mooncake 专用解析器
// ============================================================================

/// Parse a Mooncake-style uint64: string with fallback to integer.
/// Mooncake encodes some uint64 values as strings for large numbers.
/// Mooncake 将某些 uint64 值编码为字符串以支持大数值。
fn parse_mooncake_uint64(v: &Value) -> Result<u64, String> {
    match v {
        Value::String(s) => {
            let s = s.as_str().unwrap_or("");
            if s.is_empty() {
                Ok(0)
            } else {
                s.parse::<u64>()
                    .map_err(|e| format!("parse uint64 from '{}': {}", s, e))
            }
        }
        Value::Integer(i) => {
            if let Some(n) = i.as_u64() {
                Ok(n)
            } else if let Some(n) = i.as_i64() {
                if n < 0 {
                    Err(format!("negative value {}", n))
                } else {
                    Ok(n as u64)
                }
            } else {
                Err("bad integer".into())
            }
        }
        Value::Nil => Ok(0),
        _ => Err(format!("unsupported mooncake uint64 type: {:?}", v)),
    }
}

/// Parse Mooncake parent block hash: can be a comma-separated string, array,
/// or single value (Nil → empty vec).
/// 解析 Mooncake 父块哈希：可以是逗号分隔的字符串、数组或单值（Nil → 空 vec）。
fn parse_mooncake_parent_uint64(v: &Value) -> Result<Vec<u64>, String> {
    match v {
        Value::Nil => Ok(vec![]),
        Value::String(s) => {
            let s = s.as_str().unwrap_or("");
            if s.is_empty() {
                return Ok(vec![]);
            }
            s.split(|c: char| c == ',' || c.is_whitespace())
                .filter(|p| !p.is_empty())
                .map(|p| {
                    p.parse::<u64>()
                        .map_err(|e| format!("parse '{}': {}", p, e))
                })
                .collect()
        }
        Value::Array(arr) => arr.iter().map(parse_single_uint64).collect(),
        _ => Ok(vec![parse_single_uint64(v)?]),
    }
}

/// Parse a single uint64 value with strict validation.
/// 解析单个 uint64 值，进行严格验证。
fn parse_single_uint64(v: &Value) -> Result<u64, String> {
    match v {
        Value::Integer(i) => {
            if let Some(n) = i.as_u64() {
                Ok(n)
            } else if let Some(n) = i.as_i64() {
                if n < 0 {
                    Err(format!("negative value {}", n))
                } else {
                    Ok(n as u64)
                }
            } else {
                Err("bad integer".into())
            }
        }
        Value::F64(f) => {
            if *f < 0.0 || *f != (*f as u64) as f64 {
                Err(format!("float {} invalid for uint64", f))
            } else {
                Ok(*f as u64)
            }
        }
        Value::F32(f) => {
            let f = *f as f64;
            if f < 0.0 || f != (f as u64) as f64 {
                Err(format!("float {} invalid for uint64", f))
            } else {
                Ok(f as u64)
            }
        }
        Value::String(s) => {
            let s = s.as_str().unwrap_or("");
            if s.is_empty() {
                Err("empty string".into())
            } else {
                s.parse::<u64>().map_err(|e| format!("parse: {}", e))
            }
        }
        Value::Nil => Err("nil for uint64".into()),
        _ => Err(format!("unsupported type {:?}", v)),
    }
}

/// Convert a nested MessagePack array to Vec<Vec<String>> (replica list).
/// 将嵌套的 MessagePack 数组转换为 Vec<Vec<String>>（副本列表）。
fn convert_to_replica_list(v: &Value) -> Result<Vec<Vec<String>>, String> {
    match v {
        Value::Array(outer) => outer
            .iter()
            .map(|item| match item {
                Value::Array(inner) => inner.iter().map(|e| safe_get_string(e)).collect(),
                _ => Err(format!("item is not array, got {:?}", item)),
            })
            .collect(),
        _ => Err(format!("expected array, got {:?}", v)),
    }
}

// ============================================================================
// Event-specific parsers / 事件特定解析器
// ============================================================================

/// Parse a Mooncake BlockStoreEvent from a MessagePack value array.
/// Field layout: [event_type, mooncake_key, replica_list, _, block_size,
///                 block_hashes, parent_block_hash, token_ids]
///
/// 从 MessagePack 值数组解析 Mooncake BlockStoreEvent。
fn parse_mooncake_block_stored(data: &[Value]) -> Result<KVEventData, String> {
    let mooncake_key = data
        .get(1)
        .map(safe_get_string)
        .unwrap_or_else(|| Err("missing mooncake_key".into()))?;
    let replica_list = data
        .get(2)
        .map(convert_to_replica_list)
        .unwrap_or_else(|| Err("missing replica_list".into()))?;
    let block_size = data
        .get(4)
        .map(parse_i64)
        .unwrap_or_else(|| Err("missing block_size".into()))?;
    let block_hashes = data
        .get(5)
        .map(parse_mooncake_parent_uint64)
        .unwrap_or_else(|| Err("missing block_hashes".into()))?;
    let parent_block_hash = data.get(6).map(parse_mooncake_uint64).unwrap_or(Ok(0))?;
    let token_ids = data
        .get(7)
        .map(parse_i32_array)
        .unwrap_or_else(|| Err("missing token_ids".into()))?;

    Ok(KVEventData::BlockStored(BlockStoredEvent {
        block_hashes,
        token_ids,
        parent_block_hash,
        block_size,
        mooncake_key,
        replica_list,
        model_name: String::new(),
        lora_id: 0,
        lora_name: String::new(),
        pod_name: String::new(),
        medium: String::new(),
    }))
}

/// Parse a vLLM BlockStored event from a MessagePack value array.
/// Field layout: [event_type, block_hashes, parent_block_hash, token_ids,
///                 block_size, _, medium]
///
/// 从 MessagePack 值数组解析 vLLM BlockStored 事件。
fn parse_vllm_block_stored(data: &[Value]) -> Result<KVEventData, String> {
    for (i, elem) in data.iter().enumerate() {
        debug!("parse_vllm_block_stored: idx={}, type={:?}", i, elem);
    }

    let block_hashes = data
        .get(1)
        .map(parse_u64_array)
        .unwrap_or_else(|| Err("missing block_hashes".into()))?;

    let parent_block_hash = match data.get(2) {
        Some(Value::Nil) | None => 0u64,
        Some(v) => parse_u64(v).map_err(|e| format!("parent_block_hash: {}", e))?,
    };

    let token_ids = data
        .get(3)
        .map(parse_i32_array)
        .unwrap_or_else(|| Err("missing token_ids".into()))?;

    let block_size = data
        .get(4)
        .map(parse_i64)
        .unwrap_or_else(|| Err("missing block_size".into()))?;

    let medium = data
        .get(6)
        .map(safe_get_string)
        .unwrap_or(Ok(String::new()))?;

    Ok(KVEventData::BlockStored(BlockStoredEvent {
        block_hashes,
        token_ids,
        parent_block_hash,
        block_size,
        mooncake_key: String::new(),
        replica_list: vec![],
        model_name: String::new(),
        lora_id: 0,
        lora_name: String::new(),
        pod_name: String::new(),
        medium,
    }))
}

// ============================================================================
// Common batch decoder / 通用批次解码器
// ============================================================================

/// Parser descriptor: binds a source identifier to a parse function.
/// 解析器描述符：将来源标识符绑定到解析函数。
struct Parser {
    source: &'static str,
    parse_fn: fn(&[Value]) -> Result<KVEventData, String>,
    expected_length: usize,
}

/// vLLM protocol parser: events are wrapped in a 3-element outer array
/// [topic, events[], dp_rank].
/// vLLM 协议解析器：事件包装在 3 元素外层数组 [topic, events[], dp_rank] 中。
static VLLM_PARSER: Parser = Parser {
    source: SOURCE_VLLM,
    parse_fn: |data| {
        let event_type = safe_get_string(data.get(0).unwrap_or(&Value::Nil))?;
        match event_type.as_str() {
            "BlockStored" => parse_vllm_block_stored(data),
            _ => Err(format!("unknown vllm event: {}", event_type)),
        }
    },
    expected_length: 3,
};

/// Mooncake protocol parser: events are wrapped in a 2-element outer array
/// [topic, events[]].
/// Mooncake 协议解析器：事件包装在 2 元素外层数组 [topic, events[]] 中。
static MOONCAKE_PARSER: Parser = Parser {
    source: SOURCE_MOONCAKE,
    parse_fn: |data| {
        let event_type = safe_get_string(data.get(0).unwrap_or(&Value::Nil))?;
        match event_type.as_str() {
            "BlockStoreEvent" => parse_mooncake_block_stored(data),
            _ => Err(format!("unknown mooncake event: {}", event_type)),
        }
    },
    expected_length: 2,
};

/// Shared decode logic: parse msgpack, extract events array, and dispatch to
/// the protocol-specific parser for each event.
///
/// 共享解码逻辑：解析 msgpack，提取事件数组，将每个事件分派给协议特定的解析器。
fn decode_common(data: &[u8], parser: &Parser) -> Result<EventBatch, String> {
    if !data.is_empty() {
        debug!("First byte of payload: {:02x}", data[0]);
    }

    let value = rmpv::decode::read_value(&mut &data[..])
        .map_err(|e| format!("msgpack decode failed: {}", e))?;

    let arr = value.as_array().ok_or("expected msgpack array")?;

    // Validate outer array structure. / 验证外层数组结构。
    if arr.len() != parser.expected_length {
        return Err(format!(
            "expected {}-element array, got {}",
            parser.expected_length,
            arr.len()
        ));
    }

    let events_arr = arr
        .get(1)
        .and_then(|v| v.as_array())
        .ok_or("invalid events type")?;

    if events_arr.is_empty() {
        warn!("Received empty event list");
    }

    // DP rank is present only in vLLM protocol (3-element array).
    // DP rank 仅在 vLLM 协议中存在（3 元素数组）。
    let dp_rank: i64 = if parser.expected_length == 3 {
        parse_i64(arr.get(2).unwrap_or(&Value::Nil)).unwrap_or(-1)
    } else {
        -1
    };

    let mut events = Vec::with_capacity(events_arr.len());

    for raw_event in events_arr {
        let event_slice = raw_event
            .as_array()
            .ok_or_else(|| format!("event is not array: {:?}", raw_event))?;
        let event = (parser.parse_fn)(event_slice)?;
        events.push(event);
    }

    Ok(EventBatch {
        source: parser.source.to_string(),
        events,
        data_parallel_rank: dp_rank,
    })
}

// ============================================================================
// Public API / 公开 API
// ============================================================================

/// Decode a vLLM event batch from msgpack bytes.
/// 从 msgpack 字节解码 vLLM 事件批次。
pub fn decode_vllm_event_batch(data: &[u8]) -> Result<EventBatch, String> {
    decode_common(data, &VLLM_PARSER)
}

/// Decode a Mooncake event batch from msgpack bytes.
/// 从 msgpack 字节解码 Mooncake 事件批次。
pub fn decode_mooncake_event_batch(data: &[u8]) -> Result<EventBatch, String> {
    decode_common(data, &MOONCAKE_PARSER)
}
