use mooncake_store_client::engram::{EngramClient, EngramStore, EngramStoreConfig};
use mooncake_store_core::{ReplicateConfig, StoreError};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MockState {
    exists_results: Vec<bool>,
    batch_put_statuses: Vec<i32>,
    get_into_ranges_result: Option<Vec<Vec<Vec<i64>>>>,
    removed_keys: Vec<String>,
    registrations: Vec<usize>,
    unregistrations: Vec<usize>,
    get_keys: Vec<Vec<String>>,
    get_dst_offsets: Vec<Vec<Vec<usize>>>,
    get_src_offsets: Vec<Vec<Vec<usize>>>,
    get_sizes: Vec<Vec<Vec<usize>>>,
    unregister_failures: VecDeque<StoreError>,
}

#[derive(Default)]
struct MockClient {
    state: Arc<Mutex<MockState>>,
}

impl MockClient {
    fn with_state(state: MockState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }
}

impl EngramClient for MockClient {
    fn register_buffer(
        &self,
        buffer: &[u8],
        _location: &str,
    ) -> mooncake_store_core::error::StoreResult<()> {
        self.state
            .lock()
            .unwrap()
            .registrations
            .push(buffer.as_ptr() as usize);
        Ok(())
    }

    fn unregister_buffer(&self, buffer: &[u8]) -> mooncake_store_core::error::StoreResult<()> {
        let mut state = self.state.lock().unwrap();
        state.unregistrations.push(buffer.as_ptr() as usize);
        if let Some(err) = state.unregister_failures.pop_front() {
            return Err(err);
        }
        Ok(())
    }

    fn batch_is_exist<'a>(
        &'a mut self,
        _keys: &'a [String],
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<Vec<bool>>> + 'a>>
    {
        Box::pin(async move { Ok(self.state.lock().unwrap().exists_results.clone()) })
    }

    fn batch_put_from<'a>(
        &'a mut self,
        _keys: &'a [String],
        _buffers: &'a [&'a [u8]],
        _config: Option<ReplicateConfig>,
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<Vec<i32>>> + 'a>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().batch_put_statuses.clone()) })
    }

    fn get_into_ranges<'a>(
        &'a mut self,
        _buffer: &'a mut [u8],
        keys: &'a [Vec<String>],
        dst_offsets: &'a [Vec<Vec<usize>>],
        src_offsets: &'a [Vec<Vec<usize>>],
        sizes: &'a [Vec<Vec<usize>>],
    ) -> Pin<
        Box<dyn Future<Output = mooncake_store_core::error::StoreResult<Vec<Vec<Vec<i64>>>>> + 'a>,
    > {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.get_keys = keys.to_vec();
            state.get_dst_offsets = dst_offsets.to_vec();
            state.get_src_offsets = src_offsets.to_vec();
            state.get_sizes = sizes.to_vec();
            Ok(state.get_into_ranges_result.clone().unwrap_or_default())
        })
    }

    fn remove<'a>(
        &'a mut self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<()>> + 'a>> {
        self.state
            .lock()
            .unwrap()
            .removed_keys
            .push(key.to_string());
        Box::pin(std::future::ready(Ok(())))
    }
}

#[test]
fn test_engram_store_builds_expected_keys() {
    let store = EngramStore::new(
        3,
        EngramStoreConfig {
            table_vocab_sizes: vec![4, 8],
            embedding_dim: 16,
            buffer_location: "cpu:0".into(),
        },
        MockClient::default(),
    )
    .unwrap();

    assert_eq!(
        store.get_store_keys(),
        &["engram:l3:h0".to_string(), "engram:l3:h1".to_string()]
    );
    assert_eq!(store.get_num_heads(), 2);
    assert_eq!(store.get_embedding_dim(), 16);
}

#[tokio::test]
async fn test_engram_lookup_rows_contiguous_builds_range_layout() {
    let mut state = MockState::default();
    state.get_into_ranges_result = Some(vec![vec![vec![64; 4]; 2]; 1]);

    let mut store = EngramStore::new(
        0,
        EngramStoreConfig {
            table_vocab_sizes: vec![16, 32],
            embedding_dim: 16,
            buffer_location: "cpu:0".into(),
        },
        MockClient::with_state(state),
    )
    .unwrap();

    let mut output = vec![0u8; 2 * 2 * 2 * 64];
    let row_ids: Vec<i64> = vec![0, 0, 5, 10, 1, 0, 3, 20];

    store
        .lookup_rows_contiguous(&row_ids, 2, 2, &mut output)
        .await
        .unwrap();

    let s = store.into_inner();
    let s = s.state.lock().unwrap();
    assert_eq!(s.registrations.len(), 1);
    assert_eq!(s.unregistrations.len(), 1);
    assert_eq!(s.get_src_offsets[0][0].len(), 4);
    assert_eq!(s.get_src_offsets[0][1].len(), 4);
    assert_eq!(
        s.get_src_offsets[0][0],
        vec![0 * 64, 5 * 64, 1 * 64, 3 * 64]
    );
    assert_eq!(
        s.get_src_offsets[0][1],
        vec![0 * 64, 10 * 64, 0 * 64, 20 * 64]
    );
}

#[tokio::test]
async fn test_engram_remove_from_store_counts_successes() {
    let mock = MockClient::default();

    let mut store = EngramStore::new(
        0,
        EngramStoreConfig {
            table_vocab_sizes: vec![8, 16],
            embedding_dim: 32,
            buffer_location: "cpu:0".into(),
        },
        mock,
    )
    .unwrap();

    let removed = store.remove_from_store().await.unwrap();
    assert_eq!(removed, 2);
    let s = store.into_inner();
    assert_eq!(
        s.state.lock().unwrap().removed_keys,
        vec!["engram:l0:h0".to_string(), "engram:l0:h1".to_string()]
    );
}

#[tokio::test]
async fn test_engram_populate_rolls_back_when_unregister_fails() {
    let mut state = MockState::default();
    state.exists_results = vec![false];
    state.batch_put_statuses = vec![0];
    state.unregister_failures =
        VecDeque::from(vec![StoreError::Internal("unregister failed".to_string())]);

    let mut store = EngramStore::new(
        0,
        EngramStoreConfig {
            table_vocab_sizes: vec![2],
            embedding_dim: 4,
            buffer_location: "cpu:0".into(),
        },
        MockClient::with_state(state),
    )
    .unwrap();

    let embed_data = vec![0u8; 2 * 4 * 4];
    let result = store.populate(&[&embed_data]).await;
    assert!(result.is_err());

    let s = store.into_inner();
    let s = s.state.lock().unwrap();
    assert_eq!(s.removed_keys, vec!["engram:l0:h0"]);
}
