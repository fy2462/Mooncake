//! # Mooncake P2P Store — Distributed Checkpoint Storage
//! Mooncake P2P 存储 — 分布式检查点存储
//!
//! The P2P Store is a distributed storage layer built on top of the Mooncake
//! Transfer Engine. It enables peer-to-peer checkpoint sharing across nodes
//! in a cluster, using either RDMA or TCP for data transfer and etcd for
//! metadata coordination.
//!
//! P2P Store 是构建在 Mooncake Transfer Engine 之上的分布式存储层。
//! 它支持集群节点间的点对点检查点共享，使用 RDMA 或 TCP 进行数据传输，
//! 使用 etcd 进行元数据协调。
//!
//! ## Architecture / 架构
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │                  P2pStore                    │
//! │  ┌──────────┐  ┌──────────┐  ┌────────────┐ │
//! │  │ catalog   │  │ metadata │  │   engine    │ │
//! │  │(local map)│  │(etcd txns)│  │(TransferEng)│ │
//! │  └──────────┘  └──────────┘  └────────────┘ │
//! └──────────────────────────────────────────────┘
//!          │               │              │
//!          ▼               ▼              ▼
//!    Local memory     etcd cluster    RDMA/TCP
//!      tracking       (metadata)      (transfer)
//! ```
//!
//! ## Key Components / 关键组件
//!
//! - **P2pStore**: The main store struct. Manages local memory catalog,
//!   metadata synchronization via etcd, and data transfer via TransferEngine.
//!   主存储结构体。管理本地内存目录、通过 etcd 进行元数据同步、
//!   以及通过 TransferEngine 进行数据传输。
//!
//! - **MetadataStore**: An etcd-backed key-value store for sharing payload
//!   metadata (shard locations, sizes, replicas) across the cluster.
//!   基于 etcd 的键值存储，用于在集群间共享负载元数据（分片位置、大小、副本）。
//!
//! - **Payload**: Represents a named piece of data distributed across shards
//!   on multiple nodes. Each shard has a "gold" (primary) location and
//!   optional "replica" locations.
//!   表示分布在多个节点分片上的命名数据块。每个分片有一个 "gold"（主）位置
//!   和可选的 "replica"（副本）位置。
//!
//! ## Data Flow / 数据流
//!
//! **Register (write path / 写入路径):**
//! 1. Register local memory with TransferEngine
//! 2. Split into shards, create Location entries
//! 3. Construct Payload metadata
//! 4. Write to etcd via MetadataStore
//! 5. Track in local catalog
//!
//! **Get Replica (read path / 读取路径):**
//! 1. Look up Payload metadata from etcd
//! 2. Register local buffers for receiving data
//! 3. For each shard, pick a location (with retry)
//! 4. Open remote segment, submit RDMA Read, poll, close
//!
//! ## Constraints / 约束
//!
//! - `MAX_CHUNK_SIZE`: 4 GiB — maximum size of a single transfer chunk.
//!   最大单次传输块大小。
//! - `METADATA_KEY_PREFIX`: "mooncake/checkpoint/" — etcd key namespace.
//!   etcd 键命名空间。
//! - MetadataStore uses etcd transactions for atomic compare-and-swap updates.
//!   MetadataStore 使用 etcd 事务进行原子比较并交换更新。

pub mod error;
pub mod metadata;
pub mod store;

pub use error::P2pStoreError;
pub use metadata::{Location, MetadataStore, Payload, PayloadInfo, Shard, METADATA_KEY_PREFIX};
pub use store::{Buffer, P2pStore, MAX_CHUNK_SIZE};
