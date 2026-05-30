//! MessagePack decoder for ZMQ event batches.
//!
//! Ported from Go: zmq/msg_decoder.go

use rmpv::Value;
use tracing::{debug, warn};

use crate::types::*;

// --- Parse helpers ---

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

// --- Mooncake parser ---

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

// --- vLLM parser ---

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

// --- Common batch decoder ---

struct Parser {
    source: &'static str,
    parse_fn: fn(&[Value]) -> Result<KVEventData, String>,
    expected_length: usize,
}

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

fn decode_common(data: &[u8], parser: &Parser) -> Result<EventBatch, String> {
    if !data.is_empty() {
        debug!("First byte of payload: {:02x}", data[0]);
    }

    let value = rmpv::decode::read_value(&mut &data[..])
        .map_err(|e| format!("msgpack decode failed: {}", e))?;

    let arr = value.as_array().ok_or("expected msgpack array")?;

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

/// Decode a vLLM event batch from msgpack bytes.
pub fn decode_vllm_event_batch(data: &[u8]) -> Result<EventBatch, String> {
    decode_common(data, &VLLM_PARSER)
}

/// Decode a Mooncake event batch from msgpack bytes.
pub fn decode_mooncake_event_batch(data: &[u8]) -> Result<EventBatch, String> {
    decode_common(data, &MOONCAKE_PARSER)
}

