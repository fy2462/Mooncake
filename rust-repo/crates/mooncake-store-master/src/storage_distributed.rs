use crate::hf3fs;
use libc::{RTLD_NOW, c_char, c_int, c_void, dlclose, dlerror, dlopen, dlsym};
use std::fs;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

type StorageResult<T> = Result<T, Box<dyn std::error::Error>>;
const HF3FS_IOV_SIZE: usize = 32 << 20;
const HF3FS_IOR_ENTRIES: c_int = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
}

pub trait FileSystemAdapter: Send + Sync {
    fn init(&self, mount_path: &Path) -> StorageResult<()>;
    fn shutdown(&self) -> StorageResult<()>;
    fn name(&self) -> &'static str;
    fn write_file(&self, path: &Path, data: &[u8]) -> StorageResult<usize>;
    fn read_file(&self, path: &Path) -> StorageResult<Vec<u8>>;
    fn delete_file(&self, path: &Path) -> StorageResult<()>;
    fn file_exists(&self, path: &Path) -> StorageResult<bool>;
    fn list_files(&self, dir: &Path) -> StorageResult<Vec<String>>;

    fn get_file_size(&self, path: &Path) -> StorageResult<u64> {
        Ok(fs::metadata(path)?.len())
    }

    fn list_files_with_info(&self, dir: &Path) -> StorageResult<Vec<FileInfo>> {
        let mut result = Vec::new();
        for name in self.list_files(dir)? {
            let path = dir.join(&name);
            if let Ok(size) = self.get_file_size(&path) {
                result.push(FileInfo { name, size });
            }
        }
        Ok(result)
    }
}
#[derive(Debug, Default)]
pub struct PosixFsAdapter;

impl FileSystemAdapter for PosixFsAdapter {
    fn init(&self, mount_path: &Path) -> StorageResult<()> {
        fs::create_dir_all(mount_path)?;
        Ok(())
    }
    fn shutdown(&self) -> StorageResult<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "posix"
    }
    fn write_file(&self, path: &Path, data: &[u8]) -> StorageResult<usize> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(path)?;
        file.write_all(data)?;
        Ok(data.len())
    }
    fn read_file(&self, path: &Path) -> StorageResult<Vec<u8>> {
        Ok(fs::read(path)?)
    }

    fn delete_file(&self, path: &Path) -> StorageResult<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn file_exists(&self, path: &Path) -> StorageResult<bool> {
        Ok(path.exists())
    }

    fn list_files(&self, dir: &Path) -> StorageResult<Vec<String>> {
        let mut result = Vec::new();
        if !dir.exists() {
            return Ok(result);
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                result.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Ok(result)
    }
}

#[derive(Default)]
pub struct Hf3fsAdapter {
    mount_path: RwLock<Option<PathBuf>>,
}

impl Hf3fsAdapter {
    fn mount_path(&self) -> StorageResult<PathBuf> {
        self.mount_path
            .read()
            .map_err(|_| "hf3fs mount path lock poisoned")?
            .clone()
            .ok_or_else(|| "hf3fs adapter is not initialized".into())
    }

    fn open_registered_for_write(&self, path: &Path) -> StorageResult<Hf3fsRegisteredFile> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = fs::File::create(path)?;
        let registration = hf3fs::register_fd(file.as_raw_fd())?;
        Ok(Hf3fsRegisteredFile {
            file,
            _registration: registration,
            cleanup_path: Some(path.to_path_buf()),
        })
    }

    fn open_registered_for_read(&self, path: &Path) -> StorageResult<Hf3fsRegisteredFile> {
        let file = fs::File::open(path)?;
        let registration = hf3fs::register_fd(file.as_raw_fd())?;
        Ok(Hf3fsRegisteredFile {
            file,
            _registration: registration,
            cleanup_path: None,
        })
    }
}

struct Hf3fsRegisteredFile {
    file: fs::File,
    _registration: hf3fs::Hf3fsRegistration,
    cleanup_path: Option<PathBuf>,
}

impl Hf3fsRegisteredFile {
    fn mark_success(&mut self) {
        self.cleanup_path = None;
    }
}

impl Drop for Hf3fsRegisteredFile {
    fn drop(&mut self) {
        if let Some(path) = self.cleanup_path.take() {
            if let Err(error) = fs::remove_file(&path) {
                tracing::warn!(
                    "failed to clean up partial hf3fs write {}: {}",
                    path.display(),
                    error
                );
            }
        }
    }
}

impl FileSystemAdapter for Hf3fsAdapter {
    fn init(&self, mount_path: &Path) -> StorageResult<()> {
        fs::create_dir_all(mount_path)?;
        *self
            .mount_path
            .write()
            .map_err(|_| "hf3fs mount path lock poisoned")? = Some(mount_path.to_path_buf());
        Ok(())
    }

    fn shutdown(&self) -> StorageResult<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "hf3fs"
    }

    fn write_file(&self, path: &Path, data: &[u8]) -> StorageResult<usize> {
        let mount_path = self.mount_path()?;
        let mut registered = self.open_registered_for_write(path)?;
        Hf3fsUsrbioResource::new(&mount_path, false)?.write_all(&mut registered.file, data)?;
        registered.file.sync_all()?;
        registered.mark_success();
        Ok(data.len())
    }

    fn read_file(&self, path: &Path) -> StorageResult<Vec<u8>> {
        let mount_path = self.mount_path()?;
        let mut registered = self.open_registered_for_read(path)?;
        let len = registered.file.metadata()?.len() as usize;
        let data =
            Hf3fsUsrbioResource::new(&mount_path, true)?.read_exact(&mut registered.file, len)?;
        Ok(data)
    }

    fn delete_file(&self, path: &Path) -> StorageResult<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn file_exists(&self, path: &Path) -> StorageResult<bool> {
        Ok(path.exists())
    }

    fn list_files(&self, dir: &Path) -> StorageResult<Vec<String>> {
        PosixFsAdapter::default().list_files(dir)
    }
}

#[repr(C)]
struct Hf3fsIov {
    base: *mut u8,
    iovh: *mut c_void,
    id: [c_char; 16],
    mount_point: [c_char; 256],
    size: usize,
    block_size: usize,
    numa: c_int,
}

#[repr(C)]
struct Hf3fsIor {
    iov: Hf3fsIov,
    iorh: *mut c_void,
    mount_point: [c_char; 256],
    for_read: bool,
    io_depth: c_int,
    priority: c_int,
    timeout: c_int,
    flags: u64,
}

#[repr(C)]
struct Hf3fsCqe {
    index: i32,
    reserved: i32,
    result: i64,
    userdata: *const c_void,
}

type IovCreateFn = unsafe extern "C" fn(*mut Hf3fsIov, *const c_char, usize, usize, c_int) -> c_int;
type IovDestroyFn = unsafe extern "C" fn(*mut Hf3fsIov);
type IorCreate4Fn = unsafe extern "C" fn(
    *mut Hf3fsIor,
    *const c_char,
    c_int,
    bool,
    c_int,
    c_int,
    c_int,
    u64,
) -> c_int;
type IorDestroyFn = unsafe extern "C" fn(*mut Hf3fsIor);
type PrepIoFn = unsafe extern "C" fn(
    *const Hf3fsIor,
    *const Hf3fsIov,
    bool,
    *mut c_void,
    c_int,
    usize,
    u64,
    *const c_void,
) -> c_int;
type SubmitIosFn = unsafe extern "C" fn(*const Hf3fsIor) -> c_int;
type WaitForIosFn = unsafe extern "C" fn(
    *const Hf3fsIor,
    *mut Hf3fsCqe,
    c_int,
    c_int,
    *const libc::timespec,
) -> c_int;

struct Hf3fsUsrbioApi {
    handle: *mut c_void,
    iovcreate: IovCreateFn,
    iovdestroy: IovDestroyFn,
    iorcreate4: IorCreate4Fn,
    iordestroy: IorDestroyFn,
    prep_io: PrepIoFn,
    submit_ios: SubmitIosFn,
    wait_for_ios: WaitForIosFn,
}

impl Drop for Hf3fsUsrbioApi {
    fn drop(&mut self) {
        unsafe {
            if !self.handle.is_null() {
                dlclose(self.handle);
            }
        }
    }
}

impl Hf3fsUsrbioApi {
    fn load() -> StorageResult<Self> {
        let mut last_error = None;
        for candidate in hf3fs_library_candidates() {
            let path = std::ffi::CString::new(candidate.clone())?;
            let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW) };
            if handle.is_null() {
                last_error = Some(format!("{}: {}", candidate, last_dl_error()));
                continue;
            }
            let api = unsafe {
                let loaded: StorageResult<Self> = Ok(Self {
                    handle,
                    iovcreate: load_symbol(handle, "hf3fs_iovcreate")?,
                    iovdestroy: load_symbol(handle, "hf3fs_iovdestroy")?,
                    iorcreate4: load_symbol(handle, "hf3fs_iorcreate4")?,
                    iordestroy: load_symbol(handle, "hf3fs_iordestroy")?,
                    prep_io: load_symbol(handle, "hf3fs_prep_io")?,
                    submit_ios: load_symbol(handle, "hf3fs_submit_ios")?,
                    wait_for_ios: load_symbol(handle, "hf3fs_wait_for_ios")?,
                });
                loaded
            };
            match api {
                Ok(api) => return Ok(api),
                Err(error) => {
                    unsafe { dlclose(handle) };
                    last_error = Some(format!("{}: {}", candidate, error));
                }
            }
        }
        Err(format!(
            "unable to load hf3fs USRBIO symbols; set HF3FS_LIBRARY_PATH if needed ({})",
            last_error.unwrap_or_else(|| "no candidate library path succeeded".to_string())
        )
        .into())
    }
}

struct Hf3fsUsrbioResource {
    api: Hf3fsUsrbioApi,
    iov: Hf3fsIov,
    ior: Hf3fsIor,
}

impl Hf3fsUsrbioResource {
    fn new(mount_path: &Path, for_read: bool) -> StorageResult<Self> {
        let api = Hf3fsUsrbioApi::load()?;
        let mount = std::ffi::CString::new(mount_path.to_string_lossy().as_bytes())?;
        let mut iov = unsafe { std::mem::zeroed::<Hf3fsIov>() };
        let mut ior = unsafe { std::mem::zeroed::<Hf3fsIor>() };
        let ret = unsafe { (api.iovcreate)(&mut iov, mount.as_ptr(), HF3FS_IOV_SIZE, 0, -1) };
        if ret < 0 {
            return Err(format!("hf3fs_iovcreate failed with code {ret}").into());
        }
        let ret = unsafe {
            (api.iorcreate4)(
                &mut ior,
                mount.as_ptr(),
                HF3FS_IOR_ENTRIES,
                for_read,
                0,
                0,
                -1,
                0,
            )
        };
        if ret < 0 {
            unsafe { (api.iovdestroy)(&mut iov) };
            return Err(format!("hf3fs_iorcreate4 failed with code {ret}").into());
        }
        Ok(Self { api, iov, ior })
    }

    fn write_all(&mut self, file: &mut fs::File, data: &[u8]) -> StorageResult<()> {
        let mut written = 0;
        while written < data.len() {
            let chunk = std::cmp::min(data.len() - written, HF3FS_IOV_SIZE);
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr().add(written), self.iov.base, chunk);
            }
            let bytes = self.submit(file, false, written, chunk)?;
            if bytes == 0 {
                return Err("hf3fs write made no progress".into());
            }
            written += bytes;
        }
        Ok(())
    }

    fn read_exact(&mut self, file: &mut fs::File, len: usize) -> StorageResult<Vec<u8>> {
        let mut data = vec![0; len];
        let mut read = 0;
        while read < len {
            let chunk = std::cmp::min(len - read, HF3FS_IOV_SIZE);
            let bytes = self.submit(file, true, read, chunk)?;
            if bytes == 0 {
                return Err("hf3fs read made no progress".into());
            }
            unsafe {
                std::ptr::copy_nonoverlapping(self.iov.base, data.as_mut_ptr().add(read), bytes);
            }
            read += bytes;
        }
        Ok(data)
    }

    fn submit(
        &mut self,
        file: &mut fs::File,
        for_read: bool,
        offset: usize,
        len: usize,
    ) -> StorageResult<usize> {
        let ret = unsafe {
            (self.api.prep_io)(
                &self.ior,
                &self.iov,
                for_read,
                self.iov.base.cast(),
                file.as_raw_fd(),
                offset,
                len as u64,
                std::ptr::null(),
            )
        };
        if ret < 0 {
            return Err(format!("hf3fs_prep_io failed with code {ret}").into());
        }
        let ret = unsafe { (self.api.submit_ios)(&self.ior) };
        if ret < 0 {
            return Err(format!("hf3fs_submit_ios failed with code {ret}").into());
        }
        let mut cqe = Hf3fsCqe {
            index: 0,
            reserved: 0,
            result: 0,
            userdata: std::ptr::null(),
        };
        let ret = unsafe { (self.api.wait_for_ios)(&self.ior, &mut cqe, 1, 1, std::ptr::null()) };
        if ret < 0 || cqe.result < 0 {
            return Err(format!(
                "hf3fs_wait_for_ios failed with code {ret}, result {}",
                cqe.result
            )
            .into());
        }
        Ok(cqe.result as usize)
    }
}

impl Drop for Hf3fsUsrbioResource {
    fn drop(&mut self) {
        unsafe {
            (self.api.iordestroy)(&mut self.ior);
            (self.api.iovdestroy)(&mut self.iov);
        }
    }
}

fn hf3fs_library_candidates() -> Vec<String> {
    if let Ok(path) = std::env::var("HF3FS_LIBRARY_PATH") {
        if !path.is_empty() {
            return vec![path];
        }
    }
    vec![
        "libhf3fs_api_shared.so".to_string(),
        "/usr/lib/libhf3fs_api_shared.so".to_string(),
        "/usr/local/lib/libhf3fs_api_shared.so".to_string(),
        "libhf3fs_api_shared.dylib".to_string(),
        "/usr/local/lib/libhf3fs_api_shared.dylib".to_string(),
    ]
}

fn last_dl_error() -> String {
    unsafe {
        let error = dlerror();
        if error.is_null() {
            "unknown dlopen error".to_string()
        } else {
            std::ffi::CStr::from_ptr(error)
                .to_string_lossy()
                .into_owned()
        }
    }
}

unsafe fn load_symbol<T>(handle: *mut c_void, name: &str) -> StorageResult<T> {
    let name = std::ffi::CString::new(name)?;
    // SAFETY: the caller guarantees that `handle` is a live `dlopen` handle;
    // `name` is a valid NUL-terminated C string.
    let symbol = unsafe { dlsym(handle, name.as_ptr()) };
    if symbol.is_null() {
        return Err(format!(
            "missing hf3fs symbol {}: {}",
            name.to_string_lossy(),
            last_dl_error()
        )
        .into());
    }
    // SAFETY: each caller selects `T` to match the named C function's ABI.
    Ok(unsafe { std::mem::transmute_copy(&symbol) })
}

pub fn create_filesystem_adapter(adapter_type: &str) -> StorageResult<Box<dyn FileSystemAdapter>> {
    match adapter_type {
        "posix" | "local" | "local-disk" => Ok(Box::<PosixFsAdapter>::default()),
        "hf3fs" => Ok(Box::<Hf3fsAdapter>::default()),
        other => Err(format!("unsupported distributed fs_adapter_type: {other}").into()),
    }
}
