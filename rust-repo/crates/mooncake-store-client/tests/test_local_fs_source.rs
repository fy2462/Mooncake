use std::io::Write;

use mooncake_store_client::{LocalFsSource, RemoteSource, RemoteSourceError};

fn setup_temp_dir() -> (tempfile::TempDir, LocalFsSource) {
    let dir = tempfile::TempDir::new().unwrap();
    let source = LocalFsSource::new(dir.path());
    (dir, source)
}

#[tokio::test]
async fn test_local_fs_get_missing() {
    let (_dir, source) = setup_temp_dir();
    let result = source.get("nonexistent_key").await;
    assert!(matches!(result, Err(RemoteSourceError::NotFound(_))));
}

#[tokio::test]
async fn test_local_fs_put_and_get() {
    let (dir, source) = setup_temp_dir();
    let path = source.key_path("test_key");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(b"test_value").unwrap();
    drop(f);

    let data = source.get("test_key").await.unwrap();
    assert_eq!(data, b"test_value");

    // Cleanup
    let _ = dir;
}
