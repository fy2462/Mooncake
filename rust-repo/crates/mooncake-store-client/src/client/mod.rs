pub(crate) mod write;
pub(crate) mod read;
pub(crate) mod remove;
pub(crate) mod upsert;
pub(crate) mod tasks;
pub(crate) mod storage;
pub(crate) mod transfer;

use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::sync::Arc;
use tonic::transport::Channel;
use transfer_engine_ffi::TransferEngine;
use uuid::Uuid;

use crate::proto;

// ---------------------------------------------------------------------------
// BufferHandle
// ---------------------------------------------------------------------------

pub struct BufferHandle {
    pub data: Vec<u8>,
    pub key: String,
    pub size: usize,
}

// ---------------------------------------------------------------------------
// MooncakeClient
// ---------------------------------------------------------------------------

pub struct MooncakeClient {
    pub(crate) master: proto::master_service_client::MasterServiceClient<Channel>,
    pub(crate) engine: Arc<TransferEngine>,
    pub(crate) client_id: Uuid,
    pub(crate) local_hostname: String,
    pub(crate) local_buffer: Vec<u8>,
    pub(crate) registered_buffers: RwLock<HashMap<usize, (usize, String)>>,
    pub(crate) tear_down: Arc<RwLock<bool>>,
    /// 本地已挂载 segment 的传输端点集合，用于 SelectBestReplica 本地性检查。
    /// C++ 等价：Client::GetLocalEndpoints() → segment.te_endpoint。
    pub(crate) local_endpoints: RwLock<HashSet<String>>,
}

impl MooncakeClient {
    pub async fn create(
        master_addr: &str,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
    ) -> StoreResult<Self> {
        let master_url = format!("http://{master_addr}");
        let channel = Channel::from_shared(master_url)
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut master =
            proto::master_service_client::MasterServiceClient::new(channel);

        let parts: Vec<&str> = local_host.split(':').collect();
        let ip = parts.first().copied().unwrap_or(local_host);
        let port: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);

        let engine = TransferEngine::create(
            metadata_conn_string,
            local_host,
            ip,
            port,
            true,
        )?;

        if protocol != "tcp" {
            engine.install_transport(protocol, Some(device))?;
        } else {
            engine.install_transport("tcp", None)?;
        }

        engine.discover_topology()?;

        let engine = Arc::new(engine);

        let local_buffer = vec![0u8; local_buffer_size as usize];
        unsafe {
            engine.register_local_memory(
                local_buffer.as_ptr() as *mut c_void,
                local_buffer_size as usize,
                "cpu:0",
                true,
            )?;
        }

        let client_id = Uuid::new_v4();

        if global_segment_size > 0 {
            let request = proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: local_host.to_string(),
                size: global_segment_size,
            };
            master.mount_segment(request).await.map_err(|e| StoreError::Internal(e.to_string()))?;
        }

        // 将当前节点的 hostname 注册为本地端点（传输地址），
        // 用于 SelectBestReplica 的本地性优先判断。
        // C++ 等价：Client::GetLocalEndpoints() 返回所有已挂载 segment 的 te_endpoint。
        let mut endpoints = HashSet::new();
        endpoints.insert(local_host.to_string());

        Ok(Self {
            master,
            engine,
            client_id,
            local_hostname: local_host.to_string(),
            local_buffer,
            registered_buffers: RwLock::new(HashMap::new()),
            tear_down: Arc::new(RwLock::new(false)),
            local_endpoints: RwLock::new(endpoints),
        })
    }

    // -----------------------------------------------------------------------
    // Simple accessors
    // -----------------------------------------------------------------------

    pub fn get_hostname(&self) -> String {
        self.local_hostname.clone()
    }

    pub async fn health_check(&mut self) -> StoreResult<()> {
        let request = proto::PingRequest {
            client_id: Some(self.client_id_proto()),
            mounted_segments: vec![],
        };
        self.master
            .ping(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// 注册一个本地传输端点（例如新挂载 segment 的 te_endpoint）。
    /// C++ 等价：mounted_segments_ 中 segment.te_endpoint 被加入 GetLocalEndpoints()。
    pub fn register_local_endpoint(&self, endpoint: &str) {
        self.local_endpoints.write().insert(endpoint.to_string());
    }

    /// 取消注册一个本地传输端点（例如 segment 卸载时）。
    pub fn unregister_local_endpoint(&self, endpoint: &str) {
        self.local_endpoints.write().remove(endpoint);
    }

    pub fn is_closed(&self) -> bool {
        *self.tear_down.read()
    }

    pub async fn tear_down_all(&mut self) -> StoreResult<()> {
        *self.tear_down.write() = true;

        // unregister local buffer
        unsafe { let _ = self.engine.unregister_local_memory(self.local_buffer.as_ptr() as *mut c_void); }

        // unregister all user-registered buffers
        let ptrs: Vec<usize> = self.registered_buffers.read().keys().copied().collect();
        for ptr in &ptrs {
            unsafe { let _ = self.engine.unregister_local_memory(*ptr as *mut c_void); }
        }
        self.registered_buffers.write().clear();
        Ok(())
    }
}
