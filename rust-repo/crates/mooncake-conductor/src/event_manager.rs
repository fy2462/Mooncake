// ============================================================================
// Event Manager — 事件管理器
//
// Coordinates all ZMQ subscriptions, the shared prefix index, and the HTTP API
// state. Each configured service gets a dedicated ZMQ client that feeds events
// into a KVEventHandler → PrefixCacheTable pipeline.
//
// 协调所有 ZMQ 订阅、共享前缀索引和 HTTP API 状态。每个配置的服务
// 获得一个专用的 ZMQ 客户端，将事件送入 KVEventHandler → PrefixCacheTable 管道。
//
// Lifecycle / 生命周期:
//   1. new() — create manager with services config & HTTP port.
//      new() —— 用服务配置和 HTTP 端口创建管理器。
//   2. start() — spawn ZMQ subscriber threads for each service.
//      start() —— 为每个服务启动 ZMQ 订阅线程。
//   3. stop() — signal all subscribers to stop, join threads.
//      stop() —— 通知所有订阅者停止，等待线程结束。
//
// Ported from Go: mooncake-conductor/conductor-ctrl/kvevent/event_manager.go
// ============================================================================

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use dashmap::DashMap;
use parking_lot::Mutex;
use tracing::{error, info};

use crate::event_handler::KVEventHandler;
use crate::http_api;
use crate::prefix_index::PrefixCacheTable;
use crate::types::*;
use crate::zmq_client::{self, ZmqClient, ZmqClientConfig};

/// Central orchestrator for the Conductor service.
/// Coordinates ZMQ subscriptions, the prefix index, and HTTP API state.
///
/// Conductor 服务的中央协调器。协调 ZMQ 订阅、前缀索引和 HTTP API 状态。
pub struct EventManager {
    /// Configured services (from JSON config). / 配置的服务列表（来自 JSON 配置）。
    services: Vec<ServiceConfig>,
    /// HTTP server listen port. / HTTP 服务器监听端口。
    http_port: u16,
    /// Shared state for the HTTP API handlers. / HTTP API 处理器的共享状态。
    http_state: Arc<http_api::AppState>,
    /// Shared prefix cache index. / 共享前缀缓存索引。
    indexer: Arc<PrefixCacheTable>,
    /// Active ZMQ subscribers, keyed by service_key. / 活跃的 ZMQ 订阅者，按 service_key 索引。
    subscribers: DashMap<String, Arc<ZmqClient>>,
    /// Currently active service configs. / 当前活跃的服务配置。
    active_configs: DashMap<String, ServiceConfig>,
    /// Shutdown flag: when true, all subscriber loops exit.
    /// 关闭标志：为 true 时所有订阅循环退出。
    stopped: AtomicBool,
    /// Handles for background ZMQ threads, joined on stop().
    /// 后台 ZMQ 线程的句柄，在 stop() 时等待结束。
    thread_handles: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl EventManager {
    /// Create a new EventManager with the given services and HTTP port.
    /// 用给定的服务列表和 HTTP 端口创建新的 EventManager。
    pub fn new(services: Vec<ServiceConfig>, http_port: u16) -> Self {
        let indexer = Arc::new(PrefixCacheTable::new());
        let http_state = Arc::new(http_api::AppState::new(indexer.clone()));

        Self {
            services,
            http_port,
            http_state,
            indexer,
            subscribers: DashMap::new(),
            active_configs: DashMap::new(),
            stopped: AtomicBool::new(false),
            thread_handles: Mutex::new(Vec::new()),
        }
    }

    /// Return a clone of the HTTP API state for use with axum.
    /// 返回 HTTP API 状态的克隆，供 axum 使用。
    pub fn http_state(&self) -> Arc<http_api::AppState> {
        self.http_state.clone()
    }

    /// Return the configured HTTP port. / 返回配置的 HTTP 端口。
    pub fn http_port(&self) -> u16 {
        self.http_port
    }

    /// Start all ZMQ subscriptions. Each service gets a dedicated subscriber
    /// thread. Failures for individual services are logged but don't prevent
    /// other services from starting.
    ///
    /// 启动所有 ZMQ 订阅。每个服务获得一个专用的订阅线程。
    /// 单个服务的失败会被记录但不阻止其他服务启动。
    pub fn start(&self) -> Result<(), String> {
        info!("Starting KV Event Manager...");

        let mut success = 0;
        let mut failures = 0;

        for svc in &self.services {
            match self.subscribe_to_service(svc) {
                Ok(()) => success += 1,
                Err(e) => {
                    error!(
                        "Failed to subscribe: type={}, instance={}, endpoint={}, error={}",
                        svc.service_type, svc.instance_id, svc.endpoint, e
                    );
                    failures += 1;
                }
            }
        }

        info!(
            "Static KV Event Manager started. success={}, failed={}",
            success, failures
        );
        Ok(())
    }

    /// Stop all ZMQ subscriptions and wait for all threads to complete.
    /// 停止所有 ZMQ 订阅并等待所有线程完成。
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        info!("Stopping Conductor KV Event Manager...");

        for entry in self.subscribers.iter() {
            entry.value().stop();
            info!("Stopped subscription: svc_key={}", entry.key());
        }

        let handles: Vec<thread::JoinHandle<()>> = self.thread_handles.lock().drain(..).collect();
        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Subscribe to a single service: create ZMQ client, event handler, and
    /// spawn a background thread for the ZMQ event loop. Deduplicates by
    /// service key (instance_id + tenant_id + dp_rank).
    ///
    /// 订阅单个服务：创建 ZMQ 客户端、事件处理器，并启动后台线程运行 ZMQ 事件循环。
    /// 通过 service key（instance_id + tenant_id + dp_rank）去重。
    fn subscribe_to_service(&self, svc: &ServiceConfig) -> Result<(), String> {
        let svc_key = make_service_key(&svc.instance_id, &svc.tenant_id, svc.dp_rank);

        // Already subscribed — skip. / 已订阅 —— 跳过。
        if self.subscribers.contains_key(&svc_key) {
            return Ok(());
        }

        if svc.endpoint.is_empty() {
            return Err("endpoint is required".into());
        }

        // Create per-service event handler bound to the shared indexer.
        // 创建绑定到共享索引的每服务事件处理器。
        let handler = Arc::new(KVEventHandler {
            instance_id: svc.instance_id.clone(),
            model_name: svc.model_name.clone(),
            lora_name: svc.lora_name.clone(),
            block_size: svc.block_size,
            additional_salt: svc.additional_salt.clone(),
            tenant_id: svc.tenant_id.clone(),
            indexer: self.indexer.clone(),
        });

        // Adapt KVEventHandler to the ZMQ EventHandler trait via wrapper.
        // 通过 wrapper 将 KVEventHandler 适配到 ZMQ EventHandler trait。
        let zmq_handler: Arc<dyn crate::zmq_client::EventHandler> =
            Arc::new(KVEventHandlerWrapper(handler));

        let zmq_config = ZmqClientConfig {
            cache_pool_key: svc_key.clone(),
            endpoint: svc.endpoint.clone(),
            replay_endpoint: svc.replay_endpoint.clone(),
            model_name: svc.model_name.clone(),
            ..Default::default()
        };

        zmq_client::validate_config(&zmq_config)?;

        let client = Arc::new(ZmqClient::new(zmq_config, zmq_handler)?);
        client.start()?;

        self.subscribers.insert(svc_key.clone(), client.clone());
        self.active_configs.insert(svc_key.clone(), svc.clone());

        // Register instance in tenant → instance map for broadcast queries.
        // 在 tenant → instance 映射中注册实例，用于广播查询。
        {
            let mut map = self.http_state.tenant_instance_map.write();
            map.entry(svc.tenant_id.clone())
                .or_default()
                .insert(svc.instance_id.clone(), ());
        }

        // Spawn background thread for the ZMQ event loop.
        // 启动后台线程运行 ZMQ 事件循环。
        let client_for_thread = client.clone();
        let handle = thread::spawn(move || {
            client_for_thread.run_loop();
        });
        self.thread_handles.lock().push(handle);

        info!(
            "Subscribed: type={}, key={}, instance={}, tenant={}, endpoint={}",
            svc.service_type, svc_key, svc.instance_id, svc.tenant_id, svc.endpoint
        );

        Ok(())
    }
}

// ----------------------------------------------------------------------------
// Wrapper: KVEventHandler → zmq_client::EventHandler trait
// Wrapper：将 KVEventHandler 适配到 zmq_client::EventHandler trait
// ----------------------------------------------------------------------------

/// Adapter struct that wraps `KVEventHandler` to implement the
/// `zmq_client::EventHandler` trait, allowing the ZMQ client to
/// call into the event handler through a trait object.
///
/// 适配器结构体，包装 `KVEventHandler` 以实现 `zmq_client::EventHandler` trait，
/// 使 ZMQ 客户端能通过 trait object 调用事件处理器。
struct KVEventHandlerWrapper(Arc<KVEventHandler>);

impl crate::zmq_client::EventHandler for KVEventHandlerWrapper {
    fn handle_event(&self, event: &KVEventData, dp_rank: i64) {
        self.0.handle_event(event, dp_rank);
    }
}
