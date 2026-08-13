use mooncake_store_client::DummyHotCache;

#[test]
fn cpp_parity_dummy_client_shm_mapping_establishes_zero_copy_hot_buffer() {
    let cache = DummyHotCache::new_shared(32 * 1024 * 1024).unwrap();
    let key = "shm_verify";
    let data = vec![b'S'; 16 * 1024 * 1024];
    cache.put(key, &data).unwrap();

    let buffer = cache
        .get_buffer(key)
        .expect("warmed key must resolve from the shared hot cache");
    assert_eq!(buffer.size, data.len());
    assert!(
        cache.is_hot_cache_ptr(buffer.ptr),
        "warmed buffer must be backed by the shared mapping"
    );

    let actual = unsafe { std::slice::from_raw_parts(buffer.ptr as *const u8, buffer.size) };
    assert_eq!(actual, data.as_slice());
}
