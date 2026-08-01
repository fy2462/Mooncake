//! Portable filesystem helpers shared by LocalDisk-oriented client paths.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Scoped advisory file lock. Dropping the guard releases the lock.
#[must_use = "the file lock is released when its guard is dropped"]
pub struct FileLock<'a> {
    file: &'a File,
    locked: bool,
}

impl FileLock<'_> {
    pub fn is_locked(&self) -> bool {
        self.locked
    }
}

impl Drop for FileLock<'_> {
    fn drop(&mut self) {
        if self.locked {
            let _ = fs2::FileExt::unlock(self.file);
            self.locked = false;
        }
    }
}

/// Acquire an exclusive advisory lock held until the returned guard is dropped.
pub fn acquire_write_lock(file: &File) -> io::Result<FileLock<'_>> {
    fs2::FileExt::lock_exclusive(file)?;
    Ok(FileLock { file, locked: true })
}

/// Acquire a shared advisory lock held until the returned guard is dropped.
pub fn acquire_read_lock(file: &File) -> io::Result<FileLock<'_>> {
    fs2::FileExt::lock_shared(file)?;
    Ok(FileLock { file, locked: true })
}

/// Ensure `path` exists as a directory, recursively creating absent parents.
pub fn ensure_dir_exists(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("path exists but is not a directory: {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir_all(path),
        Err(error) => Err(error),
    }
}

fn save_bytes_to_file(data: &[u8], path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir_exists(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(data)?;
    file.flush()
}

/// Write UTF-8 string bytes to `path`, truncating an existing file.
pub fn save_string_to_file(content: &str, path: impl AsRef<Path>) -> io::Result<()> {
    save_bytes_to_file(content.as_bytes(), path.as_ref())
}

/// Write arbitrary bytes to `path`, truncating an existing file.
pub fn save_binary_to_file(data: &[u8], path: impl AsRef<Path>) -> io::Result<()> {
    save_bytes_to_file(data, path.as_ref())
}
