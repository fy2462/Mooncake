//! Integration tests for msg_decoder module.
//!
//! Tests decoding of vLLM and Mooncake event batches from msgpack-encoded bytes.

use mooncake_conductor::msg_decoder::{decode_mooncake_event_batch, decode_vllm_event_batch};
use mooncake_conductor::types::{EventType, SOURCE_MOONCAKE, SOURCE_VLLM};
use rmpv::Value;

#[test]
fn test_decode_vllm_event_batch() {
    let batch = Value::Array(vec![
        Value::Integer(1700000000i64.into()),
        Value::Array(vec![Value::Array(vec![
            Value::String("BlockStored".into()),
            Value::Array(vec![
                Value::Integer(100u64.into()),
                Value::Integer(200u64.into()),
            ]),
            Value::Integer(5000000000u64.into()),
            Value::Array(vec![
                Value::Integer(10000000i64.into()),
                Value::Integer(2i64.into()),
                Value::Integer(3i64.into()),
            ]),
            Value::Integer(1024i64.into()),
        ])]),
        Value::String("ok".into()),
    ]);

    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &batch).unwrap();

    let result = decode_vllm_event_batch(&buf).unwrap();
    assert_eq!(result.source, SOURCE_VLLM);
    assert_eq!(result.events.len(), 1);
    assert_eq!(result.events[0].event_type(), EventType::BlockStored);
}

#[test]
fn test_decode_mooncake_event_batch() {
    let batch = Value::Array(vec![
        Value::Integer(1700000000i64.into()),
        Value::Array(vec![Value::Array(vec![
            Value::String("BlockStoreEvent".into()),
            Value::String("mooncake-key-123".into()),
            Value::Array(vec![
                Value::Array(vec![
                    Value::String("replica1".into()),
                    Value::String("replica2".into()),
                ]),
                Value::Array(vec![Value::String("replica3".into())]),
            ]),
            Value::Nil,
            Value::Integer(1024i64.into()),
            Value::Array(vec![
                Value::Integer(100u64.into()),
                Value::Integer(200u64.into()),
            ]),
            Value::Integer(50u64.into()),
            Value::Array(vec![
                Value::Integer(1i64.into()),
                Value::Integer(2i64.into()),
                Value::Integer(3i64.into()),
            ]),
        ])]),
    ]);

    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &batch).unwrap();

    let result = decode_mooncake_event_batch(&buf).unwrap();
    assert_eq!(result.source, SOURCE_MOONCAKE);
    assert_eq!(result.events.len(), 1);
}

#[test]
fn test_decode_vllm_invalid() {
    let batch = Value::Array(vec![Value::Integer(1700000000i64.into())]);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &batch).unwrap();
    assert!(decode_vllm_event_batch(&buf).is_err());
}

#[test]
fn test_decode_mooncake_invalid() {
    let batch = Value::Array(vec![Value::Integer(1700000000i64.into())]);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &batch).unwrap();
    assert!(decode_mooncake_event_batch(&buf).is_err());
}
