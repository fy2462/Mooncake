//! # Engram Store — 嵌入表存储抽象
//!
//! 在 [`MooncakeClient`] 之上提供面向 ML 嵌入表（embedding table）查找的高层 API。
//! (High-level API for ML embedding table lookups built on top of [`MooncakeClient`].)
//!
//! ## 与 MooncakeClient 的区别 (Differences from MooncakeClient)
//!
//! | 方面 (Aspect) | MooncakeClient | EngramStore |
//! |---|---|---|
//! | 抽象层级 | 低层：直接操作 key/value/offset | 高层：按 head/row_id 操作嵌入表 |
//! | 使用场景 | 通用分布式 KV 存储 | ML 推理时的嵌入表查找 |
//! | 数据模型 | 不透明字节流 | 结构化的 embedding table (head × vocab × dim) |
//! | Key 管理 | 用户自行管理 | 自动生成 `engram:l{layer}:h{head}` 格式 key |
//!
//! ## 数据模型 (Data Model)
//!
//! ```text
//! Embedding Table:
//!    num_heads × table_vocab_sizes[head] × embedding_dim × sizeof(f32)
//! ```
//!
//! 每个 head 对应一个独立的嵌入表，存储在 Mooncake 中以 `engram:l{layer}:h{head_id}` 为 key。
//! `lookup_rows` 按 `[batch, sequence, heads]` 三维索引读取嵌入向量。
//!
//! ## 核心 Trait: EngramClient
//!
//! 抽象存储后端接口，使得 `EngramStore` 不直接耦合 `MooncakeClient`。
//! 默认实现: `impl EngramClient for MooncakeClient`，将高层嵌入操作转换为底层 RDMA 读写。

use crate::client::MooncakeClient;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicateConfig, StoreError};
use std::future::Future;
use std::pin::Pin;

mod ops;

/// 类型别名：用于 trait 中返回 future 的 Pin<Box<Future>> 模式。
/// (Type alias for the Pin<Box<Future>> pattern used in trait async methods.)
type ClientFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Engram 存储配置。
/// (Configuration for the engram store.)
///
/// ## 字段 (Fields)
/// - `table_vocab_sizes`: 每个 head 的词汇表大小 `[vocab_head0, vocab_head1, ...]`
/// - `embedding_dim`: 每个 token 的嵌入向量维度
/// - `buffer_location`: RDMA 缓冲区位置（如 `"cpu:0"`）
#[derive(Debug, Clone)]
pub struct EngramStoreConfig {
    /// 每个 attention head 的词汇表大小 (vocabulary size for each head)
    pub table_vocab_sizes: Vec<i64>,
    /// 嵌入向量维度 (embedding vector dimension, e.g. 64, 128, 768)
    pub embedding_dim: usize,
    /// 缓冲区位置标识符，如 `"cpu:0"` (buffer location for RDMA registration, e.g. "cpu:0")
    pub buffer_location: String,
}

impl Default for EngramStoreConfig {
    fn default() -> Self {
        Self {
            table_vocab_sizes: vec![1024],
            embedding_dim: 64,
            buffer_location: "cpu:0".to_string(),
        }
    }
}

/// 嵌入存储后端抽象。
/// (Abstract embedding storage backend.)
///
/// 定义了嵌入表操作所需的最小接口。任何实现此 trait 的类型都可以作为
/// [`EngramStore`] 的后端——目前仅有 [`MooncakeClient`] 的实现。
///
/// ## 方法概览 (Method Overview)
/// - `batch_is_exist`: 检查一组 key 是否存在
/// - `batch_put_from`: 批量写入嵌入数据
/// - `get_into_ranges`: 按 offset 范围读取嵌入数据（核心读取操作）
/// - `remove`: 删除指定 key 的嵌入数据
pub trait EngramClient {
    /// 检查多个 key 是否存在。
    /// (Check whether multiple keys exist in the store.)
    fn batch_is_exist<'a>(
        &'a mut self,
        keys: &'a [String],
    ) -> ClientFuture<'a, StoreResult<Vec<bool>>>;

    /// 批量写入数据。
    /// (Batch write buffers to the store.)
    ///
    /// `config` 控制副本数和一致性级别 (controls replica count and consistency)。
    fn batch_put_from<'a>(
        &'a mut self,
        keys: &'a [String],
        buffers: &'a [&'a [u8]],
        config: Option<ReplicateConfig>,
    ) -> ClientFuture<'a, StoreResult<Vec<i32>>>;

    /// 按范围读取：从 store 中读取数据到本地 `buffer` 的指定 offset 位置。
    /// (Read data from store into local buffer at specified offsets.)
    ///
    /// 参数维度: `[batch, heads, tokens]`，每个 token 指定 `(dst_offset, src_offset, size)`。
    fn get_into_ranges<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
        keys: &'a [Vec<String>],
        dst_offsets: &'a [Vec<Vec<usize>>],
        src_offsets: &'a [Vec<Vec<usize>>],
        sizes: &'a [Vec<Vec<usize>>],
    ) -> ClientFuture<'a, StoreResult<Vec<Vec<Vec<i64>>>>>;

    /// 删除指定 key 的数据。
    /// (Remove data for the given key.)
    fn remove<'a>(&'a mut self, key: &'a str) -> ClientFuture<'a, StoreResult<()>>;
}

/// MooncakeClient 的 EngramClient 适配实现。
/// (Adapter: implements EngramClient for MooncakeClient.)
///
/// 将高层嵌入操作映射到 MooncakeClient 的底层 API：
/// - `batch_put_from`: 使用安全 slice API 批量写入
/// - `get_into_ranges`: 使用 copy-based 安全范围读取
impl EngramClient for MooncakeClient {
    fn batch_is_exist<'a>(
        &'a mut self,
        keys: &'a [String],
    ) -> ClientFuture<'a, StoreResult<Vec<bool>>> {
        Box::pin(async move { MooncakeClient::batch_is_exist(self, keys).await })
    }

    fn batch_put_from<'a>(
        &'a mut self,
        keys: &'a [String],
        buffers: &'a [&'a [u8]],
        config: Option<ReplicateConfig>,
    ) -> ClientFuture<'a, StoreResult<Vec<i32>>> {
        Box::pin(async move { MooncakeClient::batch_put(self, keys, buffers, config).await })
    }

    fn get_into_ranges<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
        keys: &'a [Vec<String>],
        dst_offsets: &'a [Vec<Vec<usize>>],
        src_offsets: &'a [Vec<Vec<usize>>],
        sizes: &'a [Vec<Vec<usize>>],
    ) -> ClientFuture<'a, StoreResult<Vec<Vec<Vec<i64>>>>> {
        Box::pin(async move {
            let mut buffers = [buffer];
            MooncakeClient::get_into_ranges_copy(
                self,
                &mut buffers,
                keys,
                dst_offsets,
                src_offsets,
                sizes,
            )
            .await
        })
    }

    fn remove<'a>(&'a mut self, key: &'a str) -> ClientFuture<'a, StoreResult<()>> {
        Box::pin(async move { MooncakeClient::remove(self, key, false).await })
    }
}

/// 嵌入表存储：封装存储后端 + 嵌入表元数据。
/// (Embedding table store: wraps a storage backend with embedding table metadata.)
///
/// ## 泛型参数 (Generic Parameter)
/// - `C`: 实现 [`EngramClient`] 的存储后端类型
///
/// ## 字段 (Fields)
/// - `store`: 底层存储后端（通常是 `MooncakeClient`）
/// - `table_vocab_sizes`: 每个 head 的词汇表大小
/// - `embedding_dim`: 嵌入向量维度
/// - `embed_keys`: 自动生成的 key 列表 `["engram:l{layer}:h0", ...]`
pub struct EngramStore<C> {
    store: C,
    table_vocab_sizes: Vec<i64>,
    embedding_dim: usize,
    /// 自动生成的 key: `engram:l{layer_id}:h{head_id}`
    /// (Auto-generated store keys, one per head)
    embed_keys: Vec<String>,
}

impl<C: EngramClient> EngramStore<C> {
    /// 创建一个新的 EngramStore。
    /// (Create a new EngramStore.)
    ///
    /// ## 参数验证 (Validation)
    /// - `table_vocab_sizes` 不能为空
    /// - `embedding_dim` 必须大于 0
    /// - 所有 `table_vocab_sizes` 的值必须为正数
    ///
    /// ## Key 生成 (Key Generation)
    /// 格式: `engram:l{layer_id}:h{head_id}`，例如 `engram:l0:h0`, `engram:l0:h1`
    pub fn new(layer_id: i32, config: EngramStoreConfig, store: C) -> StoreResult<Self> {
        if config.table_vocab_sizes.is_empty() {
            return Err(StoreError::InvalidParams(
                "EngramStoreConfig.table_vocab_sizes must not be empty".to_string(),
            ));
        }
        if config.embedding_dim == 0 {
            return Err(StoreError::InvalidParams(
                "EngramStoreConfig.embedding_dim must be positive".to_string(),
            ));
        }
        if config.table_vocab_sizes.iter().any(|&size| size <= 0) {
            return Err(StoreError::InvalidParams(
                "EngramStoreConfig.table_vocab_sizes must contain only positive values".to_string(),
            ));
        }

        let embed_keys = (0..config.table_vocab_sizes.len())
            .map(|head_id| format!("engram:l{layer_id}:h{head_id}"))
            .collect::<Vec<_>>();

        Ok(Self {
            store,
            table_vocab_sizes: config.table_vocab_sizes,
            embedding_dim: config.embedding_dim,
            embed_keys,
        })
    }

    /// 返回每个 head 的词汇表大小。
    /// (Returns the vocabulary size for each head.)
    pub fn get_table_vocab_sizes(&self) -> &[i64] {
        &self.table_vocab_sizes
    }

    /// 返回所有嵌入表的存储 key。
    /// (Returns the store keys for all embedding tables.)
    pub fn get_store_keys(&self) -> &[String] {
        &self.embed_keys
    }

    /// 返回 attention head 的数量。
    /// (Returns the number of attention heads.)
    pub fn get_num_heads(&self) -> usize {
        self.table_vocab_sizes.len()
    }

    /// 返回嵌入向量的维度。
    /// (Returns the embedding dimension.)
    pub fn get_embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    /// 消费 EngramStore，返回底层存储后端。
    /// (Consume the EngramStore and return the inner storage backend.)
    pub fn into_inner(self) -> C {
        self.store
    }

    /// 查找嵌入行：根据 row_ids 读取嵌入向量到 output_buffer。
    /// (Lookup embedding rows: read embedding vectors into output_buffer by row_ids.)
    ///
    /// ## 参数 (Parameters)
    /// - `row_ids`: 形状 `[batch][sequence][num_heads]`，每元素为 vocab 中的 row index
    /// - `output_buffer`: 输出缓冲区，大小需 ≥ `batch × sequence × num_heads × dim × sizeof(f32)`
    ///
    /// ## 流程 (Flow)
    /// 1. 验证输入维度
    /// 2. 展平 row_ids → 连续的 flat 数组
    /// 3. 调用 `lookup_rows_contiguous`
    pub async fn lookup_rows(
        &mut self,
        row_ids: &[Vec<Vec<i64>>],
        output_buffer: &mut [u8],
    ) -> StoreResult<()> {
        if row_ids.is_empty() || row_ids[0].is_empty() {
            return Err(StoreError::InvalidParams(
                "row_ids must not be empty".to_string(),
            ));
        }

        let batch = row_ids.len();
        let sequence = row_ids[0].len();
        let num_heads = self.get_num_heads();
        let mut flat_row_ids = Vec::with_capacity(batch * sequence * num_heads);

        for batch_entry in row_ids {
            if batch_entry.len() != sequence {
                return Err(StoreError::InvalidParams(
                    "row_ids must have consistent sequence length".to_string(),
                ));
            }
            for token_entry in batch_entry {
                if token_entry.len() != num_heads {
                    return Err(StoreError::InvalidParams(format!(
                        "row_ids inner head dimension must equal {num_heads}"
                    )));
                }
                flat_row_ids.extend_from_slice(token_entry);
            }
        }

        self.lookup_rows_contiguous(&flat_row_ids, batch, sequence, output_buffer)
            .await
    }

    /// 使用连续的 row_ids 数组进行嵌入查找。
    /// (Lookup embeddings using a contiguous row_ids array.)
    ///
    /// ## 参数 (Parameters)
    /// - `row_ids`: 展平的一维数组，布局为 `[B * L * H]`
    /// - `batch`, `sequence`: 批次和序列维度
    /// - `output_buffer`: 输出缓冲区
    ///
    /// ## 查找逻辑 (Lookup Logic)
    /// 对每个 `(batch, token, head)` 三元组：
    /// 1. 取 `row_id = row_ids[(batch * L + token) * H + head]`
    /// 2. 验证 `0 <= row_id < table_vocab_sizes[head]`
    /// 3. 构建 offset 描述: `dst_offset`, `src_offset = row_id * dim * sizeof(f32)`, `size = dim * sizeof(f32)`
    /// 4. 通过 `get_into_ranges` 批量读取
    ///
    /// ## 错误处理 (Error Handling)
    /// - row_id 越界 → 清零输出缓冲区 + 返回错误
    /// - 短读（bytes_read < 预期）→ 清零 + 返回错误
    /// - register/unregister 失败 → 清零 + 返回错误
    pub async fn lookup_rows_contiguous(
        &mut self,
        row_ids: &[i64],
        batch: usize,
        sequence: usize,
        output_buffer: &mut [u8],
    ) -> StoreResult<()> {
        let num_heads = self.get_num_heads();
        if batch == 0 || sequence == 0 {
            return Err(StoreError::InvalidParams(
                "batch and sequence must be positive".to_string(),
            ));
        }
        if row_ids.len() != batch * sequence * num_heads {
            return Err(StoreError::InvalidParams(
                "row_ids length does not match [B, L, H]".to_string(),
            ));
        }

        // 每行 embedding 的字节数 (bytes per embedding row)
        let row_bytes = self.embedding_dim * std::mem::size_of::<f32>();
        let expected_size = batch
            .checked_mul(sequence)
            .and_then(|v| v.checked_mul(num_heads))
            .and_then(|v| v.checked_mul(row_bytes))
            .ok_or_else(|| StoreError::InvalidParams("lookup size overflow".to_string()))?;

        if output_buffer.len() < expected_size {
            return Err(StoreError::InvalidParams(format!(
                "output buffer too small: need {expected_size}, got {}",
                output_buffer.len()
            )));
        }

        // 辅助闭包：当查找失败时清空输出并返回错误。
        // (Helper closure: on failure, zero-fill output and return the error.)
        let fail_lookup = |output: &mut [u8], err: StoreError| -> StoreResult<()> {
            output[..expected_size].fill(0);
            Err(err)
        };

        // 构建 get_into_ranges 的参数结构
        // 维度: [batch=1][heads][tokens]
        let mut all_keys = vec![Vec::with_capacity(num_heads)];
        let mut all_dst_offsets = vec![Vec::with_capacity(num_heads)];
        let mut all_src_offsets = vec![Vec::with_capacity(num_heads)];
        let mut all_sizes = vec![Vec::with_capacity(num_heads)];
        for head_id in 0..num_heads {
            all_keys[0].push(self.embed_keys[head_id].clone());
            all_dst_offsets[0].push(Vec::with_capacity(batch * sequence));
            all_src_offsets[0].push(Vec::with_capacity(batch * sequence));
            all_sizes[0].push(Vec::with_capacity(batch * sequence));
        }

        // 填充每个 token 的读取范围 (populate read ranges for each token)
        for batch_id in 0..batch {
            for token_id in 0..sequence {
                let token_index = batch_id * sequence + token_id;
                let row_offset = token_index * num_heads;
                for head_id in 0..num_heads {
                    let idx = row_ids[row_offset + head_id];
                    if idx < 0 || idx >= self.table_vocab_sizes[head_id] {
                        return fail_lookup(
                            output_buffer,
                            StoreError::InvalidParams(format!(
                                "row id {idx} out of range for head {head_id}"
                            )),
                        );
                    }

                    all_dst_offsets[0][head_id].push((row_offset + head_id) * row_bytes);
                    all_src_offsets[0][head_id].push(idx as usize * row_bytes);
                    all_sizes[0][head_id].push(row_bytes);
                }
            }
        }

        // 执行批量读取 (execute batch read)
        let results = self
            .store
            .get_into_ranges(
                output_buffer,
                &all_keys,
                &all_dst_offsets,
                &all_src_offsets,
                &all_sizes,
            )
            .await
            .inspect_err(|_| output_buffer[..expected_size].fill(0))?;
        // 验证返回结果形状 (validate result shape)
        if results.len() != 1 || results[0].len() != num_heads {
            return fail_lookup(
                output_buffer,
                StoreError::Internal("unexpected get_into_ranges result shape".to_string()),
            );
        }

        // 验证每个 head 的每个 token 都读取了正确的字节数
        // (verify correct byte count per token per head)
        for head_id in 0..num_heads {
            if results[0][head_id].len() != all_sizes[0][head_id].len() {
                return fail_lookup(
                    output_buffer,
                    StoreError::Internal(format!(
                        "unexpected get_into_ranges result count for head {head_id}"
                    )),
                );
            }
            for bytes_read in &results[0][head_id] {
                if *bytes_read != row_bytes as i64 {
                    return fail_lookup(
                        output_buffer,
                        StoreError::Internal(format!(
                            "short read for head {head_id}: expected {row_bytes}, got {bytes_read}"
                        )),
                    );
                }
            }
        }

        Ok(())
    }
}
