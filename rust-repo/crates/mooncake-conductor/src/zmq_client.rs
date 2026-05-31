// ============================================================================
// ZMQ Client — ZMQ 客户端
//
// Manages ZMQ SUB/DEALER socket connections to KV event publishers.
// Each ZmqClient instance subscribes to a single inference engine endpoint,
// decodes incoming MessagePack batches, and dispatches events to an
// EventHandler callback.
//
// 管理与 KV 事件发布者的 ZMQ SUB/DEALER socket 连接。每个 ZmqClient 实例
// 订阅单个推理引擎端点，解码传入的 MessagePack 批次，并将事件分派给
// EventHandler 回调。
//
// Connection lifecycle / 连接生命周期:
//   1. connect() — create SUB socket + optional DEALER replay socket.
//      connect() —— 创建 SUB socket + 可选的 DEALER 重放 socket。
//   2. run_loop() — poll SUB socket, decode batches, invoke handler.
//      run_loop() —— 轮询 SUB socket，解码批次，调用处理器。
//   3. On disconnect: reconnect with backoff, optionally request replay.
//      断开时：带退避重连，可选地请求重放。
//   4. stop() — set running flag false, close sockets, exit loop.
//      stop() —— 设置 running 标志为 false，关闭 socket，退出循环。
//
// Ported from Go: mooncake-conductor/conductor-ctrl/zmq/zmq_client.go
// ============================================================================

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{debug, error, info, warn};
use zmq::{Context, SocketType, POLLIN};

use crate::msg_decoder::{decode_mooncake_event_batch, decode_vllm_event_batch};
use crate::types::*;

// ----------------------------------------------------------------------------
// ZMQ Client Configuration / ZMQ 客户端配置
// ----------------------------------------------------------------------------

/// Configuration for a single ZMQ client instance.
/// 单个 ZMQ 客户端实例的配置。
#[derive(Debug, Clone)]
pub struct ZmqClientConfig {
    /// Unique key for this cache pool (instance_id|tenant_id|dp_rank).
    /// 此缓存池的唯一 key（instance_id|tenant_id|dp_rank）。
    pub cache_pool_key: String,
    /// ZMQ PUB socket endpoint (e.g. "tcp://host:5556").
    /// ZMQ PUB socket 端点。
    pub endpoint: String,
    /// ZMQ DEALER socket endpoint for gap replay.
    /// ZMQ DEALER socket 端点，用于断点重放。
    pub replay_endpoint: String,
    /// Model name for logging context. / 用于日志上下文的模型名称。
    pub model_name: String,
    /// Poll timeout for the SUB socket. / SUB socket 的轮询超时。
    pub poll_timeout: Duration,
    /// Timeout for replay requests. / 重放请求的超时。
    pub replay_timeout: Duration,
    /// Delay between reconnect attempts. / 重连尝试之间的延迟。
    pub reconnect_delay: Duration,
}

impl Default for ZmqClientConfig {
    fn default() -> Self {
        Self {
            cache_pool_key: String::new(),
            endpoint: String::new(),
            replay_endpoint: String::new(),
            model_name: String::new(),
            poll_timeout: Duration::from_millis(100),
            replay_timeout: Duration::from_secs(5),
            reconnect_delay: Duration::from_secs(1),
        }
    }
}

/// Validate that a ZMQ config has the required fields.
/// 验证 ZMQ 配置是否包含必需字段。
pub fn validate_config(config: &ZmqClientConfig) -> Result<(), String> {
    if config.endpoint.is_empty() {
        return Err("endpoint is required".to_string());
    }
    Ok(())
}

// ----------------------------------------------------------------------------
// Event Handler trait / 事件处理器 trait
// ----------------------------------------------------------------------------

/// Callback trait for processing decoded KV events.
/// The ZMQ client calls this for each event in a decoded batch.
///
/// 处理解码后 KV 事件的回调 trait。ZMQ 客户端为解码批次中的每个事件调用此方法。
pub trait EventHandler: Send + Sync {
    /// Handle a single KV event with its data-parallel rank.
    /// 处理单个 KV 事件及其数据并行 rank。
    fn handle_event(&self, event: &KVEventData, dp_rank: i64);
}

// ----------------------------------------------------------------------------
// Internal connection state / 内部连接状态
// ----------------------------------------------------------------------------

/// Mutable connection state protected by a Mutex.
/// 由 Mutex 保护的可变连接状态。
struct Inner {
    /// SUB socket for receiving event batches. / 用于接收事件批次的 SUB socket。
    sub_socket: Option<zmq::Socket>,
    /// DEALER socket for requesting replay of missed events.
    /// 用于请求重放错过事件的 DEALER socket。
    replay_socket: Option<zmq::Socket>,
    /// Whether the sockets are currently connected. / socket 当前是否已连接。
    connected: bool,
    /// Last received sequence number (-1 = no events received yet).
    /// 最后接收到的序列号（-1 = 尚未收到事件）。
    last_seq: i64,
}

impl Inner {
    /// Release all sockets and reset connection state.
    /// 释放所有 socket 并重置连接状态。
    fn cleanup(&mut self) {
        self.sub_socket = None;
        self.replay_socket = None;
        self.connected = false;
    }
}

// ============================================================================
// ZmqClient —  ZMQ 客户端主结构
// ============================================================================

/// A ZMQ client that subscribes to a single KV event publisher.
/// Runs an event loop in a background thread.
///
/// 订阅单个 KV 事件发布者的 ZMQ 客户端。在后台线程中运行事件循环。
pub struct ZmqClient {
    /// Client configuration. / 客户端配置。
    config: ZmqClientConfig,
    /// Event handler callback. / 事件处理器回调。
    handler: Arc<dyn EventHandler>,
    /// Mutable connection state. / 可变连接状态。
    inner: Mutex<Inner>,
    /// Whether the event loop is running. / 事件循环是否正在运行。
    running: AtomicBool,
    /// Shared ZMQ context for socket creation. / 用于创建 socket 的共享 ZMQ 上下文。
    zmq_ctx: Context,
}

impl ZmqClient {
    /// Create a new ZMQ client. Does not connect until start() is called.
    /// 创建新的 ZMQ 客户端。在调用 start() 之前不会连接。
    pub fn new(config: ZmqClientConfig, handler: Arc<dyn EventHandler>) -> Result<Self, String> {
        Ok(Self {
            config,
            handler,
            inner: Mutex::new(Inner {
                sub_socket: None,
                replay_socket: None,
                connected: false,
                last_seq: -1,
            }),
            running: AtomicBool::new(false),
            zmq_ctx: Context::new(),
        })
    }

    /// Connect to the publisher and start the event loop flag.
    /// 连接到发布者并启动事件循环标志。
    pub fn start(&self) -> Result<(), String> {
        self.connect()?;
        self.running.store(true, Ordering::SeqCst);
        info!("ZMQ client started, service={}", self.config.cache_pool_key);
        Ok(())
    }

    /// Signal the event loop to stop and close all sockets.
    /// 通知事件循环停止并关闭所有 socket。
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.inner.lock().unwrap().cleanup();
        info!("ZMQ client stopped, service={}", self.config.cache_pool_key);
    }

    /// Check if the event loop is currently running. / 检查事件循环是否正在运行。
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Main event loop. Runs in a dedicated background thread.
    /// Blocks on poll; on disconnect, attempts reconnect with backoff.
    ///
    /// 主事件循环。在专用后台线程中运行。
    /// 阻塞轮询；断开连接时带退避尝试重连。
    pub fn run_loop(&self) {
        loop {
            if !self.is_running() {
                return;
            }

            let connected = self.inner.lock().unwrap().connected;

            if !connected {
                self.handle_reconnect();
                continue;
            }

            if let Err(e) = self.consume() {
                error!(
                    "Consume error, svc={}, err={}",
                    self.config.cache_pool_key, e
                );
                self.inner.lock().unwrap().connected = false;
            }
        }
    }

    // ------------------------------------------------------------------------
    // Reconnect logic / 重连逻辑
    // ------------------------------------------------------------------------

    /// Attempt to reconnect after a disconnection. Waits for the configured
    /// reconnect delay, then re-establishes the connection. If a sequence
    /// number was tracked, requests replay of missed events.
    ///
    /// 断开后尝试重连。等待配置的重连延迟，然后重新建立连接。
    /// 如果追踪了序列号，请求重放错过的事件。
    fn handle_reconnect(&self) {
        info!(
            "Reconnecting, svc={}, delay={:?}",
            self.config.cache_pool_key, self.config.reconnect_delay
        );
        std::thread::sleep(self.config.reconnect_delay);

        if !self.is_running() {
            return;
        }

        match self.connect() {
            Ok(()) => {
                let last_seq = self.inner.lock().unwrap().last_seq;
                if last_seq >= 0 {
                    info!(
                        "Reconnected, svc={}, resuming_from={}",
                        self.config.cache_pool_key,
                        last_seq + 1
                    );
                    if let Err(e) = self.request_replay(last_seq + 1) {
                        warn!(
                            "Replay request failed, svc={}, err={}",
                            self.config.cache_pool_key, e
                        );
                    }
                }
            }
            Err(e) => error!(
                "Reconnect failed, svc={}, err={}",
                self.config.cache_pool_key, e
            ),
        }
    }

    // ------------------------------------------------------------------------
    // Connection establishment / 建立连接
    // ------------------------------------------------------------------------

    /// Create and connect SUB + DEALER sockets. Idempotent if already connected.
    /// 创建并连接 SUB + DEALER socket。已连接时幂等返回。
    fn connect(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.connected {
            return Ok(());
        }
        inner.cleanup();

        // Create SUB socket for receiving event batches.
        // 创建 SUB socket 用于接收事件批次。
        let sub = self
            .zmq_ctx
            .socket(SocketType::SUB)
            .map_err(|e| format!("SUB socket: {}", e))?;
        sub.set_ipv6(true).map_err(|e| format!("IPv6 SUB: {}", e))?;
        sub.connect(&self.config.endpoint)
            .map_err(|e| format!("SUB connect {}: {}", self.config.endpoint, e))?;
        // Subscribe to all topics (empty prefix = wildcard).
        // 订阅所有 topic（空前缀 = 通配）。
        sub.set_subscribe(b"")
            .map_err(|e| format!("SUB subscribe: {}", e))?;

        // Create DEALER socket for requesting replay of missed events.
        // 创建 DEALER socket 用于请求重放错过的事件。
        let replay = self
            .zmq_ctx
            .socket(SocketType::DEALER)
            .map_err(|e| format!("DEALER socket: {}", e))?;
        replay
            .set_ipv6(true)
            .map_err(|e| format!("IPv6 DEALER: {}", e))?;
        replay
            .connect(&self.config.replay_endpoint)
            .map_err(|e| format!("DEALER connect {}: {}", self.config.replay_endpoint, e))?;

        inner.sub_socket = Some(sub);
        inner.replay_socket = Some(replay);
        inner.connected = true;

        info!(
            "Connected to publisher, svc={}, endpoint={}",
            self.config.cache_pool_key, self.config.endpoint
        );
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Message consumption / 消息消费
    // ------------------------------------------------------------------------

    /// Poll the SUB socket and process one message batch if available.
    /// ZMQ message format: [topic (bytes), seq (8 bytes BE), payload (msgpack)].
    ///
    /// 轮询 SUB socket，如果有可用消息则处理一个批次。
    /// ZMQ 消息格式：[topic (bytes), seq (8 字节大端), payload (msgpack)]。
    fn consume(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        let sub = inner.sub_socket.as_ref().ok_or("SUB socket is nil")?;

        // Poll with short timeout to allow periodic running checks.
        // 短超时轮询，允许定期检查 running 标志。
        let mut poll_items = [sub.as_poll_item(POLLIN)];
        let timeout = self.config.poll_timeout.as_millis() as i64;

        let polled = zmq::poll(&mut poll_items, timeout).map_err(|e| format!("poll: {}", e))?;
        if polled == 0 {
            return Ok(()); // timeout, no data / 超时，无数据
        }

        // Read frames (non-blocking; poll already indicated data is ready).
        // 读取帧（非阻塞；poll 已指示数据就绪）。
        let frames = sub
            .recv_multipart(zmq::DONTWAIT)
            .map_err(|e| format!("recv: {}", e))?;

        // ZMQ multipart message: [topic, sequence_number, payload]
        // ZMQ 多部分消息：[topic, 序列号, payload]
        if frames.len() < 3 {
            return Err(format!("need 3 frames, got {}", frames.len()));
        }

        let topic = frames[0].clone();
        let seq_bytes = frames[1].clone();
        let payload = frames[2].clone();

        // Sequence number is 8 bytes big-endian int64.
        // 序列号为 8 字节大端 int64。
        if seq_bytes.len() != 8 {
            return Err("invalid seq len".into());
        }
        let seq = i64::from_be_bytes(seq_bytes[..8].try_into().map_err(|_| "seq parse")?);

        // Gap detection: warn if we missed messages since last poll.
        // 间隙检测：如果自上次轮询以来错过了消息则警告。
        let last = inner.last_seq;
        if last != -1 && seq > last + 1 {
            warn!(
                "Gap: svc={}, missed={}, last={}, cur={}",
                self.config.cache_pool_key,
                seq - last - 1,
                last,
                seq
            );
        }
        inner.last_seq = seq;
        drop(inner); // release lock before calling handler / 调用 handler 前释放锁

        let topic_str = String::from_utf8_lossy(&topic);
        debug!("topic={}", topic_str);

        // Route to protocol-specific decoder based on ZMQ topic.
        // 根据 ZMQ topic 路由到协议特定的解码器。
        let batch = match topic_str.as_ref() {
            "mooncake" => decode_mooncake_event_batch(&payload),
            _ => decode_vllm_event_batch(&payload),
        }
        .map_err(|e| format!("decode: {}", e))?;

        // Set pod_name on each event for traceability.
        // 为每个事件设置 pod_name 以便追踪。
        for mut event in batch.events {
            if let KVEventData::BlockStored(ref mut e) = event {
                e.pod_name = self.config.cache_pool_key.clone();
            }
            self.handler.handle_event(&event, batch.data_parallel_rank);
        }

        debug!(
            "Batch processed, svc={}, seq={}",
            self.config.cache_pool_key, seq
        );
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Replay request / 重放请求
    // ------------------------------------------------------------------------

    /// Request replay of events starting from `from_seq` via the DEALER socket.
    /// Used to recover missed events after a reconnection.
    ///
    /// 通过 DEALER socket 请求从 `from_seq` 开始重放事件。
    /// 用于在重连后恢复错过的事件。
    fn request_replay(&self, from_seq: i64) -> Result<(), String> {
        let inner = self.inner.lock().unwrap();
        let replay = inner.replay_socket.as_ref().ok_or("no replay socket")?;

        // Send replay request: 8-byte big-endian sequence number.
        // 发送重放请求：8 字节大端序列号。
        let req = (from_seq as u64).to_be_bytes().to_vec();
        replay
            .send(&req, 0)
            .map_err(|e| format!("send replay: {}", e))?;

        replay
            .set_rcvtimeo(self.config.replay_timeout.as_millis() as i32)
            .map_err(|e| format!("rcvtimeo: {}", e))?;

        let resp = replay
            .recv_bytes(0)
            .map_err(|e| format!("recv replay resp: {}", e))?;
        info!(
            "Replay requested, svc={}, from={}, resp_len={}",
            self.config.cache_pool_key,
            from_seq,
            resp.len()
        );
        Ok(())
    }
}
