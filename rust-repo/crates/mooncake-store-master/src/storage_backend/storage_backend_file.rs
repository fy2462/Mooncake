use super::*;

/// Wraps fs::File with optional HF3FS fd registration for RAII cleanup.
/// 封装 fs::File，针对 Hf3fs 后端额外持有 fd 注册句柄，防止文件被 3FS 提前回收。
///
/// For HF3FS backend, holds an Hf3fsRegistration to keep the fd valid with
/// the 3FS client. For LocalDisk/FilePerKey, registration is None.
/// 对于 HF3FS 后端，持有 Hf3fsRegistration 以保持 fd 对 3FS 客户端有效。
/// 对于 LocalDisk/FilePerKey，registration 为 None。
pub(super) struct BackendFile {
    file: fs::File,
    /// RAII guard: holding this keeps the 3FS fd valid until drop.
    /// RAII 守卫：持有此对象保证 3FS fd 在 drop 前有效。
    _hf3fs_registration: Option<hf3fs::Hf3fsRegistration>,
}

impl BackendFile {
    /// Create a new file for writing, registering with HF3FS if needed.
    /// 创建新文件用于写入，必要时注册 HF3FS。
    pub(super) fn create(
        path: &Path,
        backend_type: StorageBackendType,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = fs::File::create(path)?;
        let registration = match backend_type {
            StorageBackendType::LocalDisk
            | StorageBackendType::FilePerKey
            | StorageBackendType::Bucket
            | StorageBackendType::OffsetAllocator
            | StorageBackendType::Distributed => None,
            StorageBackendType::Hf3fs => Some(hf3fs::register_fd(file.as_raw_fd())?),
        };
        Ok(Self {
            file,
            _hf3fs_registration: registration,
        })
    }

    /// Open an existing file for reading, registering with HF3FS if needed.
    /// 打开已有文件用于读取，必要时注册 HF3FS。
    pub(super) fn open(
        path: &Path,
        backend_type: StorageBackendType,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = fs::File::open(path)?;
        let registration = match backend_type {
            StorageBackendType::LocalDisk
            | StorageBackendType::FilePerKey
            | StorageBackendType::Bucket
            | StorageBackendType::OffsetAllocator
            | StorageBackendType::Distributed => None,
            StorageBackendType::Hf3fs => Some(hf3fs::register_fd(file.as_raw_fd())?),
        };
        Ok(Self {
            file,
            _hf3fs_registration: registration,
        })
    }

    /// Flush buffered data and fsync to disk.
    /// 刷新缓冲数据并 fsync 到磁盘。
    pub(super) fn sync_all(&self) -> Result<(), std::io::Error> {
        self.file.sync_all()
    }
}

impl Read for BackendFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.file.read(buf)
    }
}

impl Write for BackendFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.file.flush()
    }
}
