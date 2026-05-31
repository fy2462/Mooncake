// ============================================================================
// Mooncake Conductor — KV Cache Prefix Index Service
// Mooncake Conductor —— KV 缓存前缀索引服务
//
// The Conductor is a standalone service that maintains a prefix-cache index
// for distributed KV-cache systems (vLLM / Mooncake Store). It subscribes to
// ZMQ event streams published by inference engines, decodes block-stored and
// block-removed events, and builds an in-memory prefix-hash index that can
// answer cache-hit queries.
//
// Conductor 是一个独立服务，为分布式 KV 缓存系统（vLLM / Mooncake Store）
// 维护前缀缓存索引。它订阅推理引擎发布的 ZMQ 事件流，解码 block-stored 和
// block-removed 事件，构建内存中的前缀哈希索引，用于应答缓存命中查询。
//
// Architecture / 架构:
//
//   Inference Engine ──ZMQ PUB──→ Conductor
//                                     │
//   ┌─────────────────────────────────┼──────────────────────┐
//   │  zmq_client                     │                      │
//   │  (SUB socket)                   │                      │
//   │      ↓                          │                      │
//   │  msg_decoder                    │                      │
//   │  (MessagePack → EventBatch)     │                      │
//   │      ↓                          │                      │
//   │  event_handler                  │                      │
//   │  (KVEvent → StoredEvent)       │                      │
//   │      ↓                          │                      │
//   │  prefix_index                   │                      │
//   │  (PrefixCacheTable)             │                      │
//   │      ↓                          │                      │
//   │  http_api                       │                      │
//   │  (POST /query, /register)       │                      │
//   └─────────────────────────────────┼──────────────────────┘
//                                     │
//                            HTTP clients (vLLM frontend)
//
// Module map / 模块映射:
//
// | Module          | Go source                          | Purpose |
// |-----------------|------------------------------------|---------|
// | `types`         | common/types.go, zmq/event_type.go | 核心类型定义 |
// | `config`        | main.go (config parsing)           | JSON 配置加载 |
// | `zmq_client`    | zmq/zmq_client.go                  | ZMQ SUB/DEALER 连接管理 |
// | `msg_decoder`   | zmq/msg_decoder.go                 | MessagePack 解码 |
// | `event_handler` | kvevent/event_handler.go           | KV 事件 → 前缀索引操作 |
// | `event_manager` | kvevent/event_manager.go           | 订阅协调 + 线程管理 |
// | `prefix_index`  | kvevent/prefix_index.go            | 前缀哈希索引 + 缓存命中计算 |
// | `http_api`      | (HTTP routes in event_manager.go)  | Axum HTTP API 服务器 |
//
// Ported from Go: mooncake-conductor/conductor-ctrl/
// ============================================================================

pub mod config;
pub mod event_handler;
pub mod event_manager;
pub mod http_api;
pub mod msg_decoder;
pub mod prefix_index;
pub mod types;
pub mod zmq_client;
