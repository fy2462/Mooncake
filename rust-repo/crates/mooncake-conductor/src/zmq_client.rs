//! ZMQ client for receiving KV events.
//!
//! Ported from Go: zmq/zmq_client.go

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{debug, error, info, warn};
use zmq::{Context, SocketType, POLLIN};

use crate::msg_decoder::{decode_mooncake_event_batch, decode_vllm_event_batch};
use crate::types::*;

// --- Config ---

#[derive(Debug, Clone)]
pub struct ZmqClientConfig {
    pub cache_pool_key: String,
    pub endpoint: String,
    pub replay_endpoint: String,
    pub model_name: String,
    pub poll_timeout: Duration,
    pub replay_timeout: Duration,
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

pub fn validate_config(config: &ZmqClientConfig) -> Result<(), String> {
    if config.endpoint.is_empty() {
        return Err("endpoint is required".to_string());
    }
    Ok(())
}

// --- Event handler trait ---

pub trait EventHandler: Send + Sync {
    fn handle_event(&self, event: &KVEventData, dp_rank: i64);
}

// --- ZMQ Client ---

struct Inner {
    sub_socket: Option<zmq::Socket>,
    replay_socket: Option<zmq::Socket>,
    connected: bool,
    last_seq: i64,
}

impl Inner {
    fn cleanup(&mut self) {
        self.sub_socket = None;
        self.replay_socket = None;
        self.connected = false;
    }
}

pub struct ZmqClient {
    config: ZmqClientConfig,
    handler: Arc<dyn EventHandler>,
    inner: Mutex<Inner>,
    running: AtomicBool,
    zmq_ctx: Context,
}

impl ZmqClient {
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

    pub fn start(&self) -> Result<(), String> {
        self.connect()?;
        self.running.store(true, Ordering::SeqCst);
        info!("ZMQ client started, service={}", self.config.cache_pool_key);
        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.inner.lock().unwrap().cleanup();
        info!("ZMQ client stopped, service={}", self.config.cache_pool_key);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Main event loop. Runs in a background thread.
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

    fn connect(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.connected {
            return Ok(());
        }
        inner.cleanup();

        // Create SUB socket
        let sub = self
            .zmq_ctx
            .socket(SocketType::SUB)
            .map_err(|e| format!("SUB socket: {}", e))?;
        sub.set_ipv6(true).map_err(|e| format!("IPv6 SUB: {}", e))?;
        sub.connect(&self.config.endpoint)
            .map_err(|e| format!("SUB connect {}: {}", self.config.endpoint, e))?;
        sub.set_subscribe(b"")
            .map_err(|e| format!("SUB subscribe: {}", e))?;

        // Create DEALER socket for replay
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

    fn consume(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        let sub = inner.sub_socket.as_ref().ok_or("SUB socket is nil")?;

        // Poll for data with a short timeout
        let mut poll_items = [sub.as_poll_item(POLLIN)];
        let timeout = self.config.poll_timeout.as_millis() as i64;

        let polled = zmq::poll(&mut poll_items, timeout).map_err(|e| format!("poll: {}", e))?;
        if polled == 0 {
            return Ok(());
        }

        // Read frames (non-blocking; poll already indicated data is ready)
        let frames = sub
            .recv_multipart(zmq::DONTWAIT)
            .map_err(|e| format!("recv: {}", e))?;

        if frames.len() < 3 {
            return Err(format!("need 3 frames, got {}", frames.len()));
        }

        let topic = frames[0].clone();
        let seq_bytes = frames[1].clone();
        let payload = frames[2].clone();

        if seq_bytes.len() != 8 {
            return Err("invalid seq len".into());
        }
        let seq = i64::from_be_bytes(seq_bytes[..8].try_into().map_err(|_| "seq parse")?);

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
        drop(inner); // release lock before calling handler

        let topic_str = String::from_utf8_lossy(&topic);
        debug!("topic={}", topic_str);

        let batch = match topic_str.as_ref() {
            "mooncake" => decode_mooncake_event_batch(&payload),
            _ => decode_vllm_event_batch(&payload),
        }
        .map_err(|e| format!("decode: {}", e))?;

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

    fn request_replay(&self, from_seq: i64) -> Result<(), String> {
        let inner = self.inner.lock().unwrap();
        let replay = inner.replay_socket.as_ref().ok_or("no replay socket")?;

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
