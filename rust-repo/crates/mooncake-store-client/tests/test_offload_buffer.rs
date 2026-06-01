use mooncake_store_client::offload::buffer::{OffloadBatch, OffloadBufferPool};

#[test]
fn test_register_returns_unique_ids() {
    let pool = OffloadBufferPool::new();
    let id1 = pool.register(OffloadBatch {
        buffers: vec![vec![1, 2, 3]],
    });
    let id2 = pool.register(OffloadBatch {
        buffers: vec![vec![4, 5]],
    });
    assert_ne!(id1, id2);
    assert!(id1 > 0);
    assert!(id2 > id1);
}

#[test]
fn test_release_returns_batch() {
    let pool = OffloadBufferPool::new();
    let data = vec![1u8, 2, 3, 4];
    let batch_id = pool.register(OffloadBatch {
        buffers: vec![data.clone()],
    });
    let released = pool.release(batch_id);
    assert!(released.is_some());
    assert_eq!(released.unwrap().buffers, vec![data]);
}

#[test]
fn test_release_twice_returns_none_second() {
    let pool = OffloadBufferPool::new();
    let batch_id = pool.register(OffloadBatch {
        buffers: vec![vec![1]],
    });
    assert!(pool.release(batch_id).is_some());
    assert!(pool.release(batch_id).is_none());
}

#[test]
fn test_release_unknown_id_returns_none() {
    let pool = OffloadBufferPool::new();
    assert!(pool.release(9999).is_none());
}

#[test]
fn test_multiple_registers_and_releases() {
    let pool = OffloadBufferPool::new();
    let ids: Vec<u64> = (0..10)
        .map(|i| {
            pool.register(OffloadBatch {
                buffers: vec![vec![i as u8; 1024]],
            })
        })
        .collect();
    assert_eq!(ids.len(), 10);
    for (i, &id) in ids.iter().enumerate() {
        if i % 2 == 0 {
            let batch = pool.release(id);
            assert!(batch.is_some());
            assert_eq!(batch.unwrap().buffers[0].len(), 1024);
        }
    }
    for (i, &id) in ids.iter().enumerate() {
        let batch = pool.release(id);
        if i % 2 == 0 {
            assert!(batch.is_none());
        } else {
            assert!(batch.is_some());
        }
    }
}
