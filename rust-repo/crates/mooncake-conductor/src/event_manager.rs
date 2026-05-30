//! Event manager: coordinates ZMQ clients and prefix index operations.
//!
//! Mirrors the Go EventManager in kvevent/event_manager.go.
//! HTTP routing lives separately in http_api.rs.

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

/// Coordinates all ZMQ subscriptions, the prefix index, and the HTTP API state.
pub struct EventManager {
    services: Vec<ServiceConfig>,
    http_port: u16,
    http_state: Arc<http_api::AppState>,
    indexer: Arc<PrefixCacheTable>,
    subscribers: DashMap<String, Arc<ZmqClient>>,
    active_configs: DashMap<String, ServiceConfig>,
    stopped: AtomicBool,
    thread_handles: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl EventManager {
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

    /// Return the HTTP API state for use with axum.
    pub fn http_state(&self) -> Arc<http_api::AppState> {
        self.http_state.clone()
    }

    /// Return the HTTP port.
    pub fn http_port(&self) -> u16 {
        self.http_port
    }

    /// Start all ZMQ subscriptions.
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

    /// Stop all ZMQ subscriptions and wait for threads.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        info!("Stopping Conductor KV Event Manager...");

        for entry in self.subscribers.iter() {
            entry.value().stop();
            info!("Stopped subscription: svc_key={}", entry.key());
        }

        let handles: Vec<thread::JoinHandle<()>> =
            self.thread_handles.lock().drain(..).collect();
        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Subscribe to a single service: create ZMQ client, spawn thread.
    fn subscribe_to_service(&self, svc: &ServiceConfig) -> Result<(), String> {
        let svc_key = make_service_key(&svc.instance_id, &svc.tenant_id, svc.dp_rank);

        if self.subscribers.contains_key(&svc_key) {
            return Ok(());
        }

        if svc.endpoint.is_empty() {
            return Err("endpoint is required".into());
        }

        let handler = Arc::new(KVEventHandler {
            instance_id: svc.instance_id.clone(),
            model_name: svc.model_name.clone(),
            lora_name: svc.lora_name.clone(),
            block_size: svc.block_size,
            additional_salt: svc.additional_salt.clone(),
            tenant_id: svc.tenant_id.clone(),
            indexer: self.indexer.clone(),
        });

        // Adapt KVEventHandler to the ZMQ EventHandler trait
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

        // Update tenant instance map in http_state
        {
            let mut map = self.http_state.tenant_instance_map.write();
            map.entry(svc.tenant_id.clone())
                .or_default()
                .insert(svc.instance_id.clone(), ());
        }

        // Spawn background thread for the ZMQ event loop
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

// --- Wrapper to adapt KVEventHandler to zmq_client::EventHandler trait ---

struct KVEventHandlerWrapper(Arc<KVEventHandler>);

impl crate::zmq_client::EventHandler for KVEventHandlerWrapper {
    fn handle_event(&self, event: &KVEventData, dp_rank: i64) {
        self.0.handle_event(event, dp_rank);
    }
}
