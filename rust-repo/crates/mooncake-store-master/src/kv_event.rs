use crate::TenantId;
use rmpv::Value;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info};

const MAX_BATCH_SIZE: usize = 64;
const ZMQ_SEND_HWM: i32 = 10_000;

#[derive(Debug, Clone)]
pub struct KvEventConfig {
    pub enabled: bool,
    pub bind_endpoint: String,
    pub model_name: String,
    pub backend_id: String,
    pub tenant_id: String,
    pub additional_salt: String,
    pub lora_name: String,
    pub block_size: u32,
    pub dp_rank: u32,
    pub emit_legacy_compat: bool,
    pub emit_object_key: bool,
    pub queue_capacity: usize,
}

impl Default for KvEventConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_endpoint: String::new(),
            model_name: String::new(),
            backend_id: String::new(),
            tenant_id: "default".to_string(),
            additional_salt: String::new(),
            lora_name: String::new(),
            block_size: 0,
            dp_rank: 0,
            emit_legacy_compat: true,
            emit_object_key: true,
            queue_capacity: 65_536,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KvEventStats {
    pub published_batches: u64,
    pub published_events: u64,
    pub dropped_events: u64,
    pub skipped_unparsed_keys: u64,
}

#[derive(Debug, Clone)]
pub struct KvEventStatus {
    pub enabled: bool,
    pub bind_endpoint: String,
    pub backend_id: String,
    pub stats: KvEventStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KvEventKind {
    Stored,
    Removed,
}

#[derive(Debug, Clone)]
struct PendingEvent {
    kind: KvEventKind,
    object_key: String,
    medium: String,
    tenant_id: TenantId,
    group_id: String,
}

#[derive(Debug)]
struct Shared {
    queue: Mutex<VecDeque<PendingEvent>>,
    cv: Condvar,
    stop: AtomicBool,
    runtime_enabled: AtomicBool,
    next_event_id: AtomicU64,
    next_zmq_sequence: AtomicU64,
    published_batches: AtomicU64,
    published_events: AtomicU64,
    dropped_events: AtomicU64,
    skipped_unparsed_keys: AtomicU64,
}

impl Shared {
    fn new(enabled: bool) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
            runtime_enabled: AtomicBool::new(enabled),
            next_event_id: AtomicU64::new(1),
            next_zmq_sequence: AtomicU64::new(1),
            published_batches: AtomicU64::new(0),
            published_events: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            skipped_unparsed_keys: AtomicU64::new(0),
        }
    }

    fn stats(&self) -> KvEventStats {
        KvEventStats {
            published_batches: self.published_batches.load(Ordering::Relaxed),
            published_events: self.published_events.load(Ordering::Relaxed),
            dropped_events: self.dropped_events.load(Ordering::Relaxed),
            skipped_unparsed_keys: self.skipped_unparsed_keys.load(Ordering::Relaxed),
        }
    }
}

pub struct KvEventPublisher {
    config: KvEventConfig,
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl KvEventPublisher {
    pub fn new(config: KvEventConfig) -> Self {
        let enabled = config.enabled
            && !config.bind_endpoint.trim().is_empty()
            && !config.backend_id.trim().is_empty();
        let shared = Arc::new(Shared::new(enabled));
        let worker = if enabled {
            let config_for_thread = config.clone();
            let shared_for_thread = Arc::clone(&shared);
            Some(thread::spawn(move || {
                worker_loop(config_for_thread, shared_for_thread);
            }))
        } else {
            None
        };

        Self {
            config,
            shared,
            worker: Mutex::new(worker),
        }
    }

    pub fn enabled(&self) -> bool {
        self.shared.runtime_enabled.load(Ordering::Acquire)
    }

    pub fn publish_stored(
        &self,
        object_key: &str,
        medium: &str,
        tenant_id: &TenantId,
        group_id: &str,
    ) {
        self.enqueue(PendingEvent {
            kind: KvEventKind::Stored,
            object_key: object_key.to_string(),
            medium: medium.to_string(),
            tenant_id: tenant_id.clone(),
            group_id: group_id.to_string(),
        });
    }

    pub fn publish_removed(
        &self,
        object_key: &str,
        medium: &str,
        tenant_id: &TenantId,
        group_id: &str,
    ) {
        self.enqueue(PendingEvent {
            kind: KvEventKind::Removed,
            object_key: object_key.to_string(),
            medium: medium.to_string(),
            tenant_id: tenant_id.clone(),
            group_id: group_id.to_string(),
        });
    }

    pub fn status(&self) -> KvEventStatus {
        KvEventStatus {
            enabled: self.enabled(),
            bind_endpoint: self.config.bind_endpoint.clone(),
            backend_id: self.config.backend_id.clone(),
            stats: self.shared.stats(),
        }
    }

    fn enqueue(&self, event: PendingEvent) {
        if !self.enabled() {
            return;
        }
        {
            let mut queue = self.shared.queue.lock().unwrap();
            if self.config.queue_capacity > 0 && queue.len() >= self.config.queue_capacity {
                queue.pop_front();
                self.shared.dropped_events.fetch_add(1, Ordering::Relaxed);
                self.shared
                    .next_zmq_sequence
                    .fetch_add(1, Ordering::Relaxed);
            }
            queue.push_back(event);
        }
        self.shared.cv.notify_one();
    }
}

impl Drop for KvEventPublisher {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.shared.cv.notify_all();
        if let Some(handle) = self.worker.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

pub fn parse_seq_hash_from_object_key(object_key: &str) -> Option<u64> {
    if object_key.is_empty() {
        return None;
    }
    let (digits, radix) = object_key
        .strip_prefix("0x")
        .or_else(|| object_key.strip_prefix("0X"))
        .map_or((object_key, 10), |rest| (rest, 16));
    if digits.is_empty() {
        return None;
    }
    u64::from_str_radix(digits, radix).ok()
}

fn worker_loop(config: KvEventConfig, shared: Arc<Shared>) {
    let context = zmq::Context::new();
    let socket = match context.socket(zmq::PUB) {
        Ok(socket) => socket,
        Err(error) => {
            error!("kv_events: failed to create ZMQ PUB socket: {}", error);
            shared.runtime_enabled.store(false, Ordering::Release);
            return;
        }
    };
    let _ = socket.set_sndhwm(ZMQ_SEND_HWM);
    let _ = socket.set_linger(0);
    if let Err(error) = socket.bind(&config.bind_endpoint) {
        error!(
            "kv_events: zmq bind failed for {}: {}",
            config.bind_endpoint, error
        );
        shared.runtime_enabled.store(false, Ordering::Release);
        return;
    }
    info!(
        "kv_events publisher enabled on {} backend_id={}",
        config.bind_endpoint, config.backend_id
    );

    loop {
        let batch = take_next_batch(&shared);
        if batch.is_empty() {
            if shared.stop.load(Ordering::Acquire) {
                break;
            }
            continue;
        }
        publish_batch(&config, &shared, &socket, batch);
    }

    loop {
        let batch = drain_batch(&shared);
        if batch.is_empty() {
            break;
        }
        publish_batch(&config, &shared, &socket, batch);
    }
}

fn take_next_batch(shared: &Shared) -> Vec<PendingEvent> {
    let mut queue = shared.queue.lock().unwrap();
    while queue.is_empty() && !shared.stop.load(Ordering::Acquire) {
        queue = shared.cv.wait(queue).unwrap();
    }
    take_batch_locked(&mut queue)
}

fn drain_batch(shared: &Shared) -> Vec<PendingEvent> {
    let mut queue = shared.queue.lock().unwrap();
    take_batch_locked(&mut queue)
}

fn take_batch_locked(queue: &mut VecDeque<PendingEvent>) -> Vec<PendingEvent> {
    let mut batch = Vec::with_capacity(MAX_BATCH_SIZE.min(queue.len()));
    while batch.len() < MAX_BATCH_SIZE {
        let Some(event) = queue.pop_front() else {
            break;
        };
        batch.push(event);
    }
    batch
}

fn publish_batch(
    config: &KvEventConfig,
    shared: &Shared,
    socket: &zmq::Socket,
    batch: Vec<PendingEvent>,
) {
    let Some((payload, encoded_events)) = encode_event_batch(config, shared, &batch) else {
        return;
    };
    let seq = shared.next_zmq_sequence.fetch_add(1, Ordering::Relaxed);
    let seq_bytes = seq.to_be_bytes();
    let send_result = socket
        .send("", zmq::SNDMORE)
        .and_then(|_| socket.send(seq_bytes.as_slice(), zmq::SNDMORE))
        .and_then(|_| socket.send(payload.as_slice(), 0));

    match send_result {
        Ok(()) => {
            shared.published_batches.fetch_add(1, Ordering::Relaxed);
            shared
                .published_events
                .fetch_add(encoded_events as u64, Ordering::Relaxed);
        }
        Err(error) => {
            shared
                .dropped_events
                .fetch_add(encoded_events as u64, Ordering::Relaxed);
            error!("kv_events: failed to send batch: {}", error);
        }
    }
}

fn encode_event_batch(
    config: &KvEventConfig,
    shared: &Shared,
    batch: &[PendingEvent],
) -> Option<(Vec<u8>, usize)> {
    let timestamp_ms = current_unix_time_ms();
    let mut events = Vec::with_capacity(batch.len());
    for pending in batch {
        let seq_hash = parse_seq_hash_from_object_key(&pending.object_key);
        if seq_hash.is_none() {
            shared.skipped_unparsed_keys.fetch_add(1, Ordering::Relaxed);
            if !config.emit_object_key || pending.object_key.is_empty() {
                continue;
            }
        }
        let event_id = shared.next_event_id.fetch_add(1, Ordering::Relaxed);
        events.push(build_event_value(
            config,
            pending,
            seq_hash,
            event_id,
            timestamp_ms,
        ));
    }
    if events.is_empty() {
        return None;
    }
    let encoded_events = events.len();
    let value = Value::Array(vec![
        Value::from(timestamp_ms),
        Value::Array(events),
        Value::from(0u32),
    ]);
    let mut bytes = Vec::new();
    rmpv::encode::write_value(&mut bytes, &value).ok()?;
    Some((bytes, encoded_events))
}

fn build_event_value(
    config: &KvEventConfig,
    pending: &PendingEvent,
    seq_hash: Option<u64>,
    event_id: u64,
    timestamp_ms: i64,
) -> Value {
    let is_stored = pending.kind == KvEventKind::Stored;
    let mut fields = Vec::new();
    push_field(&mut fields, "event_id", Value::from(event_id));
    push_field(&mut fields, "timestamp", Value::from(timestamp_ms));
    push_field(
        &mut fields,
        "event_type",
        Value::from(if is_stored { "stored" } else { "removed" }),
    );
    if config.emit_legacy_compat {
        push_field(
            &mut fields,
            "type",
            Value::from(if is_stored {
                "BlockStored"
            } else {
                "BlockRemoved"
            }),
        );
    }
    push_field(&mut fields, "model_name", Value::Nil);
    push_field(&mut fields, "block_size", Value::Nil);
    push_field(&mut fields, "additional_salt", Value::Nil);
    push_field(&mut fields, "lora_name", Value::Nil);
    push_field(
        &mut fields,
        "tenant_id",
        Value::from(pending.tenant_id.as_str()),
    );
    push_field(
        &mut fields,
        "backend_id",
        Value::from(config.backend_id.as_str()),
    );
    push_field(&mut fields, "group_id", optional_string(&pending.group_id));
    push_field(&mut fields, "medium", optional_string(&pending.medium));
    push_field(&mut fields, "dp_rank", Value::Nil);
    if config.emit_object_key {
        push_field(
            &mut fields,
            "object_key",
            Value::from(pending.object_key.as_str()),
        );
    }
    let seq_hashes = seq_hash.map_or_else(Vec::new, |hash| vec![Value::from(hash)]);
    push_field(&mut fields, "seq_hashes", Value::Array(seq_hashes.clone()));
    if config.emit_legacy_compat {
        push_field(&mut fields, "block_hashes", Value::Array(seq_hashes));
    }
    if is_stored {
        push_field(&mut fields, "base_block_idx", Value::from(0u32));
        push_field(&mut fields, "parent_hash", Value::Nil);
        push_field(&mut fields, "token_ids", Value::Nil);
        if config.emit_legacy_compat {
            push_field(&mut fields, "parent_block_hash", Value::Nil);
        }
    } else {
        push_field(&mut fields, "base_block_idx", Value::Nil);
    }
    Value::Map(fields)
}

fn push_field(fields: &mut Vec<(Value, Value)>, key: &str, value: Value) {
    fields.push((Value::from(key), value));
}

fn optional_string(value: &str) -> Value {
    if value.is_empty() {
        Value::Nil
    } else {
        Value::from(value)
    }
}

fn current_unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_parse_seq_hash_from_object_key_matches_cpp() {
        assert_eq!(parse_seq_hash_from_object_key("42"), Some(42));
        assert_eq!(parse_seq_hash_from_object_key("0x2a"), Some(42));
        assert_eq!(parse_seq_hash_from_object_key("0X2A"), Some(42));
        assert_eq!(parse_seq_hash_from_object_key("42abc"), None);
        assert_eq!(parse_seq_hash_from_object_key(""), None);
    }

    #[test]
    fn disabled_publisher_is_noop_and_keeps_counters_zero() {
        let publisher = KvEventPublisher::new(KvEventConfig {
            enabled: false,
            ..Default::default()
        });
        let tenant_id = TenantId::default();

        assert!(!publisher.enabled());
        publisher.publish_stored("42", "cpu", &tenant_id, "");
        publisher.publish_removed("42", "cpu", &tenant_id, "");

        let stats = publisher.status().stats;
        assert_eq!(stats.published_events, 0);
        assert_eq!(stats.published_batches, 0);
        assert_eq!(stats.dropped_events, 0);
    }

    #[test]
    fn test_encode_event_batch_matches_rfc_shape() {
        let config = KvEventConfig {
            enabled: true,
            bind_endpoint: "inproc://unused".to_string(),
            model_name: "ignored-model".to_string(),
            backend_id: "backend-a".to_string(),
            tenant_id: "ignored-tenant".to_string(),
            additional_salt: "ignored-salt".to_string(),
            lora_name: "ignored-lora".to_string(),
            block_size: 16,
            dp_rank: 2,
            ..Default::default()
        };
        let shared = Shared::new(true);
        let pending = PendingEvent {
            kind: KvEventKind::Stored,
            object_key: "0x2a".to_string(),
            medium: "cpu".to_string(),
            tenant_id: TenantId::new("tenant-a".to_string()).unwrap(),
            group_id: "group-a".to_string(),
        };

        let (bytes, encoded_events) = encode_event_batch(&config, &shared, &[pending]).unwrap();
        assert_eq!(encoded_events, 1);
        let decoded = rmpv::decode::read_value(&mut bytes.as_slice()).unwrap();
        let Value::Array(batch) = decoded else {
            panic!("expected batch array");
        };
        assert_eq!(batch.len(), 3);
        let Value::Array(events) = &batch[1] else {
            panic!("expected event array");
        };
        let Value::Map(fields) = &events[0] else {
            panic!("expected event map");
        };

        assert_eq!(map_string(fields, "event_type").as_deref(), Some("stored"));
        assert_eq!(map_string(fields, "type").as_deref(), Some("BlockStored"));
        assert_eq!(map_string(fields, "tenant_id").as_deref(), Some("tenant-a"));
        assert_eq!(
            map_string(fields, "backend_id").as_deref(),
            Some("backend-a")
        );
        assert_eq!(map_string(fields, "group_id").as_deref(), Some("group-a"));
        assert_eq!(map_string(fields, "medium").as_deref(), Some("cpu"));
        assert_eq!(map_string(fields, "object_key").as_deref(), Some("0x2a"));
        assert!(map_is_nil(fields, "model_name"));
        assert!(map_is_nil(fields, "block_size"));
        assert!(map_is_nil(fields, "additional_salt"));
        assert!(map_is_nil(fields, "lora_name"));
        assert!(map_is_nil(fields, "dp_rank"));
        assert_eq!(map_array_len(fields, "seq_hashes"), Some(1));
        assert_eq!(map_array_len(fields, "block_hashes"), Some(1));
    }

    #[test]
    fn test_publish_sglang_object_key_over_zmq() {
        let endpoint = local_tcp_endpoint();
        let object_key = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855_0_k";
        let group_id =
            "sglang-hicache:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let publisher = KvEventPublisher::new(KvEventConfig {
            enabled: true,
            bind_endpoint: endpoint.clone(),
            backend_id: "mooncake-test".to_string(),
            queue_capacity: 64,
            ..Default::default()
        });
        assert!(publisher.enabled());

        let context = zmq::Context::new();
        let subscriber = context.socket(zmq::SUB).unwrap();
        subscriber.set_subscribe(b"").unwrap();
        subscriber.set_rcvtimeo(200).unwrap();
        subscriber.connect(&endpoint).unwrap();
        let tenant_id = TenantId::new("tenant-a".to_string()).unwrap();
        let frames = (0..20)
            .find_map(|_| {
                publisher.publish_stored(object_key, "cpu", &tenant_id, group_id);
                subscriber.recv_multipart(0).ok()
            })
            .expect("subscriber did not receive an event after PUB/SUB subscription retries");

        assert_eq!(frames.len(), 3);
        assert!(frames[0].is_empty());
        assert_eq!(frames[1].len(), std::mem::size_of::<u64>());
        assert!(u64::from_be_bytes(frames[1].as_slice().try_into().unwrap()) >= 1);

        let decoded = rmpv::decode::read_value(&mut frames[2].as_slice()).unwrap();
        let fields = first_event_fields(&decoded);
        assert_eq!(map_string(fields, "event_type").as_deref(), Some("stored"));
        assert_eq!(
            map_string(fields, "backend_id").as_deref(),
            Some("mooncake-test")
        );
        assert_eq!(map_string(fields, "tenant_id").as_deref(), Some("tenant-a"));
        assert_eq!(map_string(fields, "group_id").as_deref(), Some(group_id));
        assert_eq!(
            map_string(fields, "object_key").as_deref(),
            Some(object_key)
        );
        assert_eq!(map_array_len(fields, "seq_hashes"), Some(0));

        let stats = wait_for_stats(&publisher, |stats| stats.published_events >= 1);
        assert!(stats.published_events >= 1);
        assert!(stats.published_batches >= 1);
        assert_eq!(stats.dropped_events, 0);
        assert_eq!(stats.skipped_unparsed_keys, stats.published_events);
    }

    #[test]
    fn cpp_parity_kv_event_publisher_test_cpp_kveventpublishertest_publishessglangobjectkeyoverzmq_a0abdd7c()
     {
        let ipc_root = tempfile::tempdir().unwrap();
        let endpoint = format!("ipc://{}", ipc_root.path().join("kv-events.sock").display());
        let object_key = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855_0_k";
        let group_id =
            "sglang-hicache:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let publisher = KvEventPublisher::new(KvEventConfig {
            enabled: true,
            bind_endpoint: endpoint.clone(),
            backend_id: "mooncake-test".to_string(),
            emit_object_key: true,
            emit_legacy_compat: true,
            queue_capacity: 64,
            ..Default::default()
        });
        assert!(publisher.enabled());

        let context = zmq::Context::new();
        let subscriber = context.socket(zmq::SUB).unwrap();
        subscriber.set_subscribe(b"").unwrap();
        subscriber.set_rcvtimeo(2_000).unwrap();
        subscriber.connect(&endpoint).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let tenant_id = TenantId::new("tenant-a".to_string()).unwrap();
        publisher.publish_stored(object_key, "cpu", &tenant_id, group_id);
        let frames = subscriber
            .recv_multipart(0)
            .expect("single SGLang publish must reach the synchronized subscriber");

        assert_eq!(frames.len(), 3);
        assert!(frames[0].is_empty());
        assert_eq!(frames[1].len(), std::mem::size_of::<u64>());
        assert_eq!(
            u64::from_be_bytes(frames[1].as_slice().try_into().unwrap()),
            1
        );

        let decoded = rmpv::decode::read_value(&mut frames[2].as_slice()).unwrap();
        let Value::Array(batch) = &decoded else {
            panic!("expected three-element event batch");
        };
        assert_eq!(batch.len(), 3);
        let Value::Array(events) = &batch[1] else {
            panic!("expected event array");
        };
        assert_eq!(events.len(), 1);
        let Value::Map(fields) = &events[0] else {
            panic!("expected event map");
        };
        assert_eq!(map_string(fields, "event_type").as_deref(), Some("stored"));
        assert_eq!(
            map_string(fields, "backend_id").as_deref(),
            Some("mooncake-test")
        );
        assert_eq!(map_string(fields, "tenant_id").as_deref(), Some("tenant-a"));
        assert_eq!(map_string(fields, "group_id").as_deref(), Some(group_id));
        assert_eq!(
            map_string(fields, "object_key").as_deref(),
            Some(object_key)
        );
        assert_eq!(map_array_len(fields, "seq_hashes"), Some(0));

        let stats = wait_for_stats(&publisher, |stats| {
            stats.published_events == 1 && stats.published_batches == 1
        });
        assert_eq!(stats.published_events, 1);
        assert_eq!(stats.published_batches, 1);
        assert_eq!(stats.dropped_events, 0);
        assert_eq!(stats.skipped_unparsed_keys, 1);
    }

    #[test]
    fn test_queue_capacity_drops_oldest_and_reserves_sequence_gap() {
        let publisher = KvEventPublisher {
            config: KvEventConfig {
                enabled: true,
                bind_endpoint: "unused".to_string(),
                backend_id: "backend-a".to_string(),
                queue_capacity: 2,
                ..Default::default()
            },
            shared: Arc::new(Shared::new(true)),
            worker: Mutex::new(None),
        };

        let tenant_id = TenantId::default();
        publisher.publish_stored("1", "cpu", &tenant_id, "");
        publisher.publish_stored("2", "cpu", &tenant_id, "");
        publisher.publish_stored("3", "cpu", &tenant_id, "");

        let queue = publisher.shared.queue.lock().unwrap();
        let keys = queue
            .iter()
            .map(|event| event.object_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["2", "3"]);
        drop(queue);
        assert_eq!(publisher.status().stats.dropped_events, 1);
        assert_eq!(
            publisher.shared.next_zmq_sequence.load(Ordering::Relaxed),
            2
        );
    }

    fn local_tcp_endpoint() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("tcp://{addr}")
    }

    fn wait_for_stats(
        publisher: &KvEventPublisher,
        predicate: impl Fn(KvEventStats) -> bool,
    ) -> KvEventStats {
        for _ in 0..50 {
            let stats = publisher.status().stats;
            if predicate(stats) {
                return stats;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        publisher.status().stats
    }

    fn first_event_fields(decoded: &Value) -> &[(Value, Value)] {
        let Value::Array(batch) = decoded else {
            panic!("expected batch array");
        };
        let Value::Array(events) = &batch[1] else {
            panic!("expected event array");
        };
        let Value::Map(fields) = &events[0] else {
            panic!("expected event map");
        };
        fields
    }

    fn map_string(fields: &[(Value, Value)], key: &str) -> Option<String> {
        fields.iter().find_map(|(k, v)| {
            (k.as_str() == Some(key)).then(|| v.as_str().map(ToString::to_string))?
        })
    }

    fn map_array_len(fields: &[(Value, Value)], key: &str) -> Option<usize> {
        fields.iter().find_map(|(k, v)| {
            (k.as_str() == Some(key)).then(|| match v {
                Value::Array(values) => Some(values.len()),
                _ => None,
            })?
        })
    }

    fn map_is_nil(fields: &[(Value, Value)], key: &str) -> bool {
        fields
            .iter()
            .any(|(k, v)| k.as_str() == Some(key) && matches!(v, Value::Nil))
    }
}
