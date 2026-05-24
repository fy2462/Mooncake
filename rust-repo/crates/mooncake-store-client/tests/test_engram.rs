use mooncake_store_client::engram::{EngramClient, EngramStore, EngramStoreConfig};
use mooncake_store_core::{ReplicateConfig, StoreError};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MockState {
    exists_results: Vec<bool>,
    batch_put_statuses: Vec<i32>,
    get_into_ranges_result: Option<Vec<Vec<Vec<i64>>>>,
    removed_keys: Vec<String>,
    registrations: Vec<(*mut c_void, usize, String)>,
    unregistrations: Vec<*mut c_void>,
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
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> mooncake_store_core::error::StoreResult<()> {
        self.state
            .lock()
            .unwrap()
            .registrations
            .push((buffer, size, location.to_string()));
        Ok(())
    }

    fn unregister_buffer(&self, buffer: *mut c_void) -> mooncake_store_core::error::StoreResult<()> {
        let mut state = self.state.lock().unwrap();
        state.unregistrations.push(buffer);
        if let Some(err) = state.unregister_failures.pop_front() {
            return Err(err);
        }
        Ok(())
    }

    fn batch_is_exist<'a>(
        &'a mut self,
        _keys: &'a [String],
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<Vec<bool>>> + 'a>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().exists_results.clone()) })
    }

    fn batch_put_from<'a>(
        &'a mut self,
        _keys: &'a [String],
        _buffers: &'a [*mut c_void],
        _sizes: &'a [usize],
        _config: Option<ReplicateConfig>,
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<Vec<i32>>> + 'a>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().batch_put_statuses.clone()) })
    }

    fn get_into_ranges<'a>(
        &'a mut self,
        _buffers: &'a [*mut c_void],
        keys: &'a [Vec<String>],
        dst_offsets: &'a [Vec<Vec<usize>>],
        src_offsets: &'a [Vec<Vec<usize>>],
        sizes: &'a [Vec<Vec<usize>>],
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<Vec<Vec<Vec<i64>>>>> + 'a>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.get_keys = keys.to_vec();
            state.get_dst_offsets = dst_offsets.to_vec();
            state.get_src_offsets = src_offsets.to_vec();
            state.get_sizes = sizes.to_vec();
            Ok(state
                .get_into_ranges_result
                .clone()
                .unwrap_or_default())
        })
    }

    fn remove<'a>(
        &'a mut self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = mooncake_store_core::error::StoreResult<()>> + 'a>> {
        Box::pin(async move {
            self.state.lock().unwrap().removed_keys.push(key.to_string());
            Ok(())
        })
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
    let mock = MockClient::with_state(MockState {
        get_into_ranges_result: Some(vec![vec![vec![8, 8], vec![8, 8]]]),
        ..Default::default()
    });
    let shared = mock.state.clone();
    let mut store = EngramStore::new(
        1,
        EngramStoreConfig {
            table_vocab_sizes: vec![4, 4],
            embedding_dim: 2,
            buffer_location: "cpu:test".into(),
        },
        mock,
    )
    .unwrap();

    let row_ids = vec![1, 2, 3, 0];
    let mut output = vec![0u8; 2 * 1 * 2 * 2 * std::mem::size_of::<f32>()];
    store
        .lookup_rows_contiguous(&row_ids, 2, 1, &mut output)
        .await
        .unwrap();

    let state = shared.lock().unwrap();
    assert_eq!(
        state.get_keys,
        vec![vec!["engram:l1:h0".to_string(), "engram:l1:h1".to_string()]]
    );
    assert_eq!(state.get_dst_offsets, vec![vec![vec![0, 16], vec![8, 24]]]);
    assert_eq!(state.get_src_offsets, vec![vec![vec![8, 24], vec![16, 0]]]);
    assert_eq!(state.get_sizes, vec![vec![vec![8, 8], vec![8, 8]]]);
    assert_eq!(state.registrations.len(), 1);
    assert_eq!(state.registrations[0].2, "cpu:test");
    assert_eq!(state.unregistrations.len(), 1);
}

#[tokio::test]
async fn test_engram_populate_rolls_back_when_unregister_fails() {
    let mock = MockClient::with_state(MockState {
        exists_results: vec![false, false],
        batch_put_statuses: vec![0, 0],
        unregister_failures: VecDeque::from([StoreError::Internal(
            "cleanup failed".to_string(),
        )]),
        ..Default::default()
    });
    let shared = mock.state.clone();
    let mut store = EngramStore::new(
        7,
        EngramStoreConfig {
            table_vocab_sizes: vec![2, 2],
            embedding_dim: 2,
            buffer_location: "cpu:0".into(),
        },
        mock,
    )
    .unwrap();

    let head0 = vec![0u8; 2 * 2 * std::mem::size_of::<f32>()];
    let head1 = vec![1u8; 2 * 2 * std::mem::size_of::<f32>()];
    let err = store.populate(&[&head0, &head1]).await.unwrap_err();

    assert!(err.to_string().contains("cleanup failed"));
    let state = shared.lock().unwrap();
    assert_eq!(
        state.removed_keys,
        vec!["engram:l7:h0".to_string(), "engram:l7:h1".to_string()]
    );
    assert_eq!(state.registrations.len(), 2);
    assert_eq!(state.unregistrations.len(), 2);
}

#[tokio::test]
async fn test_engram_remove_from_store_counts_successes() {
    let mut store = EngramStore::new(
        5,
        EngramStoreConfig {
            table_vocab_sizes: vec![1, 1, 1],
            embedding_dim: 4,
            buffer_location: "cpu:0".into(),
        },
        MockClient::default(),
    )
    .unwrap();

    let removed = store.remove_from_store().await.unwrap();
    assert_eq!(removed, 3);
}
