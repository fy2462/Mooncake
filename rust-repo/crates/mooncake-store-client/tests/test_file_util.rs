use mooncake_store_client::file_util::{
    acquire_read_lock, acquire_write_lock, ensure_dir_exists, save_binary_to_file,
    save_string_to_file,
};
use std::io::Read;

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_savestringtofile_basic() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("test.txt");
    save_string_to_file("hello mooncake", &path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"hello mooncake");
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_savestringtofile_createssubdirectories() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a/b/c/nested.txt");
    assert!(!path.parent().unwrap().exists());
    save_string_to_file("nested content", &path).unwrap();
    assert!(path.parent().unwrap().is_dir());
    assert_eq!(std::fs::read(&path).unwrap(), b"nested content");
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_savestringtofile_emptycontent() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("empty.txt");
    save_string_to_file("", &path).unwrap();
    assert!(path.is_file());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), b"");
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_savebinarytofile_basic() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bin.dat");
    let data = [0, 1, 2, 128, 254, 255];
    save_binary_to_file(&data, &path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), data);
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_savebinarytofile_createssubdirectories() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("x/y/z/deep.dat");
    assert!(!path.parent().unwrap().exists());
    save_binary_to_file(&[42], &path).unwrap();
    assert!(path.parent().unwrap().is_dir());
    assert_eq!(std::fs::read(&path).unwrap(), [42]);
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_ensuredirexists_createnew() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("new_dir");
    assert!(!path.exists());
    ensure_dir_exists(&path).unwrap();
    assert!(path.is_dir());
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_ensuredirexists_alreadyexists() {
    let root = tempfile::tempdir().unwrap();
    ensure_dir_exists(root.path()).unwrap();
    assert!(root.path().is_dir());
}

#[test]
fn cpp_parity_file_util_test_cpp_fileutiltest_ensuredirexists_pathisfile_returnserror() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("a_file");
    std::fs::write(&path, b"data").unwrap();
    assert!(path.is_file());
    assert!(ensure_dir_exists(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"data");
}

#[test]
fn cpp_parity_posix_file_test_cpp_posixfiletest_filelocking() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("empty.dat");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    {
        let lock = acquire_write_lock(&file).unwrap();
        assert!(lock.is_locked());
        let mut exact = [0_u8; 10];
        let error = (&file).read_exact(&mut exact).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    {
        let lock = acquire_read_lock(&file).unwrap();
        assert!(lock.is_locked());
    }
}
