use crate::client::MooncakeClient;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicateConfig, StoreError};
use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;

type ClientFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug, Clone)]
pub struct EngramStoreConfig {
    pub table_vocab_sizes: Vec<i64>,
    pub embedding_dim: usize,
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

pub trait EngramClient {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn register_buffer(
        &self,
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> StoreResult<()>;

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()>;

    fn batch_is_exist<'a>(&'a mut self, keys: &'a [String]) -> ClientFuture<'a, StoreResult<Vec<bool>>>;

    fn batch_put_from<'a>(
        &'a mut self,
        keys: &'a [String],
        buffers: &'a [*mut c_void],
        sizes: &'a [usize],
        config: Option<ReplicateConfig>,
    ) -> ClientFuture<'a, StoreResult<Vec<i32>>>;

    fn get_into_ranges<'a>(
        &'a mut self,
        buffers: &'a [*mut c_void],
        keys: &'a [Vec<String>],
        dst_offsets: &'a [Vec<Vec<usize>>],
        src_offsets: &'a [Vec<Vec<usize>>],
        sizes: &'a [Vec<Vec<usize>>],
    ) -> ClientFuture<'a, StoreResult<Vec<Vec<Vec<i64>>>>>;

    fn remove<'a>(&'a mut self, key: &'a str) -> ClientFuture<'a, StoreResult<()>>;
}

impl EngramClient for MooncakeClient {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn register_buffer(
        &self,
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> StoreResult<()> {
        unsafe { MooncakeClient::register_buffer(self, buffer, size, location) }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()> {
        unsafe { MooncakeClient::unregister_buffer(self, buffer) }
    }

    fn batch_is_exist<'a>(&'a mut self, keys: &'a [String]) -> ClientFuture<'a, StoreResult<Vec<bool>>> {
        Box::pin(async move { MooncakeClient::batch_is_exist(self, keys).await })
    }

    fn batch_put_from<'a>(
        &'a mut self,
        keys: &'a [String],
        buffers: &'a [*mut c_void],
        sizes: &'a [usize],
        config: Option<ReplicateConfig>,
    ) -> ClientFuture<'a, StoreResult<Vec<i32>>> {
        Box::pin(async move { unsafe { MooncakeClient::batch_put_from(self, keys, buffers, sizes, config).await } })
    }

    fn get_into_ranges<'a>(
        &'a mut self,
        buffers: &'a [*mut c_void],
        keys: &'a [Vec<String>],
        dst_offsets: &'a [Vec<Vec<usize>>],
        src_offsets: &'a [Vec<Vec<usize>>],
        sizes: &'a [Vec<Vec<usize>>],
    ) -> ClientFuture<'a, StoreResult<Vec<Vec<Vec<i64>>>>> {
        Box::pin(async move {
            unsafe { MooncakeClient::get_into_ranges(self, buffers, keys, dst_offsets, src_offsets, sizes).await }
        })
    }

    fn remove<'a>(&'a mut self, key: &'a str) -> ClientFuture<'a, StoreResult<()>> {
        Box::pin(async move { MooncakeClient::remove(self, key).await })
    }
}

pub struct EngramStore<C> {
    store: C,
    table_vocab_sizes: Vec<i64>,
    embedding_dim: usize,
    embed_keys: Vec<String>,
    buffer_location: String,
}

impl<C: EngramClient> EngramStore<C> {
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
            buffer_location: config.buffer_location,
        })
    }

    pub fn get_table_vocab_sizes(&self) -> &[i64] {
        &self.table_vocab_sizes
    }

    pub fn get_store_keys(&self) -> &[String] {
        &self.embed_keys
    }

    pub fn get_num_heads(&self) -> usize {
        self.table_vocab_sizes.len()
    }

    pub fn get_embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    pub fn into_inner(self) -> C {
        self.store
    }

    pub async fn lookup_rows(
        &mut self,
        row_ids: &[Vec<Vec<i64>>],
        output_buffer: &mut [u8],
    ) -> StoreResult<()> {
        if row_ids.is_empty() || row_ids[0].is_empty() {
            return Err(StoreError::InvalidParams("row_ids must not be empty".to_string()));
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

        let fail_lookup = |output: &mut [u8], err: StoreError| -> StoreResult<()> {
            output[..expected_size].fill(0);
            Err(err)
        };

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

        let buffer_ptr = output_buffer.as_mut_ptr() as *mut c_void;
        self.store
            .register_buffer(buffer_ptr, expected_size, &self.buffer_location)
            .map_err(|err| {
                output_buffer[..expected_size].fill(0);
                err
            })?;

        let buffers = [buffer_ptr];
        let result = self
            .store
            .get_into_ranges(
                &buffers,
                &all_keys,
                &all_dst_offsets,
                &all_src_offsets,
                &all_sizes,
            )
            .await;
        let unregister_result = self.store.unregister_buffer(buffer_ptr);

        let results = match result {
            Ok(results) => results,
            Err(err) => {
                return fail_lookup(
                    output_buffer,
                    unregister_result.err().unwrap_or(err),
                )
            }
        };
        if let Err(err) = unregister_result {
            return fail_lookup(output_buffer, err);
        }
        if results.len() != 1 || results[0].len() != num_heads {
            return fail_lookup(
                output_buffer,
                StoreError::Internal("unexpected get_into_ranges result shape".to_string()),
            );
        }

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

    pub async fn remove_from_store(&mut self) -> StoreResult<usize> {
        let mut removed = 0usize;
        let mut first_error = None;

        for key in &self.embed_keys {
            match self.store.remove(key).await {
                Ok(()) => removed += 1,
                Err(StoreError::KeyNotFound(_)) => {}
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(removed),
        }
    }

    pub async fn populate(&mut self, embedding_buffers: &[&[u8]]) -> StoreResult<()> {
        if embedding_buffers.len() != self.embed_keys.len() {
            return Err(StoreError::InvalidParams(format!(
                "embedding_buffers length must equal number of heads ({})",
                self.embed_keys.len()
            )));
        }

        let mut sizes = Vec::with_capacity(embedding_buffers.len());
        for (head_id, buffer) in embedding_buffers.iter().enumerate() {
            let expected = self.table_vocab_sizes[head_id] as usize
                * self.embedding_dim
                * std::mem::size_of::<f32>();
            if buffer.len() != expected {
                return Err(StoreError::InvalidParams(format!(
                    "buffer size mismatch for head {head_id}: expected {expected}, got {}",
                    buffer.len()
                )));
            }
            sizes.push(buffer.len());
        }

        let exists_results = self.store.batch_is_exist(&self.embed_keys).await?;
        if exists_results.len() != self.embed_keys.len() {
            return Err(StoreError::Internal(
                "batch_is_exist returned unexpected result length".to_string(),
            ));
        }
        for (head_id, exists) in exists_results.iter().enumerate() {
            if *exists {
                return Err(StoreError::ObjectExists(self.embed_keys[head_id].clone()));
            }
        }

        let mut buffer_ptrs = Vec::with_capacity(embedding_buffers.len());
        for (index, buffer) in embedding_buffers.iter().enumerate() {
            let buffer_ptr = buffer.as_ptr() as *mut c_void;
            if let Err(err) = self
                .store
                .register_buffer(buffer_ptr, buffer.len(), &self.buffer_location)
            {
                for registered in &buffer_ptrs {
                    let _ = self.store.unregister_buffer(*registered);
                }
                return Err(StoreError::Internal(format!(
                    "failed to register embedding buffer {index}: {err}"
                )));
            }
            buffer_ptrs.push(buffer_ptr);
        }

        let put_results = self
            .store
            .batch_put_from(
                &self.embed_keys,
                &buffer_ptrs,
                &sizes,
                Some(ReplicateConfig::default()),
            )
            .await;
        let unregister_errors = buffer_ptrs
            .iter()
            .filter_map(|buffer_ptr| self.store.unregister_buffer(*buffer_ptr).err())
            .collect::<Vec<_>>();

        let put_results = put_results?;
        let put_succeeded = put_results.len() == self.embed_keys.len()
            && put_results.iter().all(|result| *result == 0);

        if !put_succeeded || !unregister_errors.is_empty() {
            for key in &self.embed_keys {
                let _ = self.store.remove(key).await;
            }
            if let Some(err) = unregister_errors.into_iter().next() {
                return Err(StoreError::Internal(format!(
                    "populate rolled back because buffer cleanup failed: {err}"
                )));
            }
            return Err(StoreError::Internal(format!(
                "populate failed with statuses: {:?}",
                put_results
            )));
        }

        Ok(())
    }
}
