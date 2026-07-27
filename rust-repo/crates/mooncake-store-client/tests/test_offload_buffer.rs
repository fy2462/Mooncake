use mooncake_store_client::offload::buffer::OffloadBufferPool;
use std::time::Duration;

#[test]
fn test_pool_rejects_invalid_limits() {
    assert!(
        OffloadBufferPool::with_limits(0, Duration::from_secs(1), Duration::from_secs(1)).is_err()
    );
    assert!(OffloadBufferPool::with_limits(1, Duration::ZERO, Duration::from_secs(1)).is_err());
    assert!(OffloadBufferPool::with_limits(1, Duration::from_secs(1), Duration::ZERO).is_err());
    assert!(
        OffloadBufferPool::with_limits(1, Duration::from_nanos(1), Duration::from_secs(1)).is_err()
    );
}

#[test]
fn test_reservation_counts_capacity_and_drop_rolls_back() {
    let pool =
        OffloadBufferPool::with_limits(8, Duration::from_secs(1), Duration::from_secs(1)).unwrap();
    let reservation = pool.try_reserve(8).unwrap();
    assert_eq!(pool.retained_bytes(), 8);
    assert!(pool.try_reserve(1).is_err());
    drop(reservation);
    assert_eq!(pool.retained_bytes(), 0);
    assert!(pool.try_reserve(8).is_ok());
}

#[test]
fn test_failed_oversized_reservation_does_not_change_accounting() {
    let pool =
        OffloadBufferPool::with_limits(8, Duration::from_secs(1), Duration::from_secs(1)).unwrap();
    assert!(pool.try_reserve(9).is_err());
    assert_eq!(pool.retained_bytes(), 0);
    assert_eq!(pool.active_batch_count(), 0);
}

#[test]
fn test_zero_sized_reservation_is_rejected() {
    let pool =
        OffloadBufferPool::with_limits(8, Duration::from_secs(1), Duration::from_secs(1)).unwrap();
    assert!(pool.try_reserve(0).is_err());
    assert_eq!(pool.retained_bytes(), 0);
}

#[test]
fn test_release_unknown_batch_is_idempotent() {
    let pool =
        OffloadBufferPool::with_limits(8, Duration::from_secs(1), Duration::from_secs(1)).unwrap();
    assert!(!pool.release(9999));
    assert!(!pool.release(9999));
}
