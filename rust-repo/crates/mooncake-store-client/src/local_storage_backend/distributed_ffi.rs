//! Safe adapter boundary for HF3FS USRBIO.
//!
//! All native symbol loading, raw C layouts, fd registration and unsafe
//! buffer access are confined to this module. The distributed storage backend
//! consumes only [`DistributedFileSystem`].

use libc::{RTLD_NOW, c_char, c_int, c_void, dlclose, dlerror, dlopen, dlsym};
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::RwLock;
use std::ffi::{CStr, CString};
use std::fs;
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const HF3FS_IOV_SIZE: usize = 32 << 20;
const HF3FS_IOR_ENTRIES: c_int = 16;

pub(super) trait DistributedFileSystem: Send + Sync {
    fn init(&self, mount_path: &Path) -> StoreResult<()>;
    fn name(&self) -> &'static str;
    fn write_file(&self, path: &Path, data: &[u8]) -> StoreResult<()>;
    fn read_file(&self, path: &Path) -> StoreResult<Vec<u8>>;
    fn delete_file(&self, path: &Path) -> StoreResult<()>;
    fn file_exists(&self, path: &Path) -> StoreResult<bool>;
    fn list_files(&self, directory: &Path) -> StoreResult<Vec<String>>;
}

pub(super) fn create_hf3fs_adapter() -> Arc<dyn DistributedFileSystem> {
    Arc::new(Hf3fsAdapter::default())
}

#[cfg(test)]
pub(super) fn create_posix_test_adapter() -> Arc<dyn DistributedFileSystem> {
    Arc::new(PosixTestAdapter)
}

#[cfg(test)]
struct PosixTestAdapter;

#[cfg(test)]
impl DistributedFileSystem for PosixTestAdapter {
    fn init(&self, mount_path: &Path) -> StoreResult<()> {
        fs::create_dir_all(mount_path)?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "posix-test"
    }

    fn write_file(&self, path: &Path, data: &[u8]) -> StoreResult<()> {
        use std::io::Write;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    }

    fn read_file(&self, path: &Path) -> StoreResult<Vec<u8>> {
        Ok(fs::read(path)?)
    }

    fn delete_file(&self, path: &Path) -> StoreResult<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn file_exists(&self, path: &Path) -> StoreResult<bool> {
        match fs::symlink_metadata(path) {
            Ok(metadata) => Ok(metadata.file_type().is_file()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn list_files(&self, directory: &Path) -> StoreResult<Vec<String>> {
        list_regular_files(directory)
    }
}

#[derive(Default)]
struct Hf3fsAdapter {
    mount_path: RwLock<Option<PathBuf>>,
}

impl Hf3fsAdapter {
    fn mount_path(&self) -> StoreResult<PathBuf> {
        self.mount_path
            .read()
            .clone()
            .ok_or_else(|| StoreError::Internal("HF3FS adapter is not initialized".to_string()))
    }

    fn open_registered(
        &self,
        path: &Path,
        write: bool,
        api: Arc<Hf3fsNativeApi>,
    ) -> StoreResult<RegisteredFile> {
        let file = if write {
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?
        } else {
            fs::File::open(path)?
        };
        let registration = match Hf3fsRegistration::new(file.as_raw_fd(), api) {
            Ok(registration) => registration,
            Err(error) => {
                if write {
                    let _ = fs::remove_file(path);
                }
                return Err(error);
            }
        };
        Ok(RegisteredFile {
            registration: Some(registration),
            file,
            cleanup_path: write.then(|| path.to_path_buf()),
        })
    }
}

impl DistributedFileSystem for Hf3fsAdapter {
    fn init(&self, mount_path: &Path) -> StoreResult<()> {
        fs::create_dir_all(mount_path)?;
        *self.mount_path.write() = Some(mount_path.to_path_buf());
        Ok(())
    }

    fn name(&self) -> &'static str {
        "hf3fs"
    }

    fn write_file(&self, path: &Path, data: &[u8]) -> StoreResult<()> {
        let mount_path = self.mount_path()?;
        let api = Arc::new(Hf3fsNativeApi::load()?);
        let mut registered = self.open_registered(path, true, Arc::clone(&api))?;
        Hf3fsUsrbioResource::new(api, &mount_path, false)?.write_all(&mut registered.file, data)?;
        registered.file.sync_all()?;
        registered.mark_success();
        Ok(())
    }

    fn read_file(&self, path: &Path) -> StoreResult<Vec<u8>> {
        let mount_path = self.mount_path()?;
        let api = Arc::new(Hf3fsNativeApi::load()?);
        let mut registered = self.open_registered(path, false, Arc::clone(&api))?;
        let length = usize::try_from(registered.file.metadata()?.len()).map_err(|_| {
            StoreError::InvalidParams(format!(
                "HF3FS object is too large for this process: {}",
                path.display()
            ))
        })?;
        Hf3fsUsrbioResource::new(api, &mount_path, true)?.read_exact(&mut registered.file, length)
    }

    fn delete_file(&self, path: &Path) -> StoreResult<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn file_exists(&self, path: &Path) -> StoreResult<bool> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(StoreError::InvalidParams(
                format!("HF3FS object path is a symlink: {}", path.display()),
            )),
            Ok(metadata) => Ok(metadata.is_file()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn list_files(&self, directory: &Path) -> StoreResult<Vec<String>> {
        list_regular_files(directory)
    }
}

fn list_regular_files(directory: &Path) -> StoreResult<Vec<String>> {
    let mut result = Vec::new();
    match fs::read_dir(directory) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_symlink() {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed namespace contains a symlink: {}",
                        entry.path().display()
                    )));
                }
                if !file_type.is_file() {
                    return Err(StoreError::InvalidParams(format!(
                        "distributed bucket contains a non-file entry: {}",
                        entry.path().display()
                    )));
                }
                let name = entry.file_name().into_string().map_err(|_| {
                    StoreError::InvalidParams(format!(
                        "distributed namespace contains a non-UTF-8 filename: {}",
                        entry.path().display()
                    ))
                })?;
                result.push(name);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    result.sort_unstable();
    Ok(result)
}

struct RegisteredFile {
    // Registration must be dropped before the file descriptor is closed.
    registration: Option<Hf3fsRegistration>,
    file: fs::File,
    cleanup_path: Option<PathBuf>,
}

impl RegisteredFile {
    fn mark_success(&mut self) {
        self.cleanup_path = None;
    }
}

impl Drop for RegisteredFile {
    fn drop(&mut self) {
        if let Some(path) = self.cleanup_path.take()
            && let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %path.display(),
                %error,
                "failed to remove partial HF3FS write"
            );
        }
        // Make the fd lifecycle explicit instead of relying on struct field
        // drop order.
        drop(self.registration.take());
    }
}

struct Hf3fsRegistration {
    fd: RawFd,
    api: Arc<Hf3fsNativeApi>,
}

impl Hf3fsRegistration {
    fn new(fd: RawFd, api: Arc<Hf3fsNativeApi>) -> StoreResult<Self> {
        let result = unsafe { (api.reg_fd)(fd, 0) };
        if result > 0 {
            return Err(StoreError::Internal(format!(
                "hf3fs_reg_fd failed for fd {fd} with code {result}"
            )));
        }
        Ok(Self { fd, api })
    }
}

impl Drop for Hf3fsRegistration {
    fn drop(&mut self) {
        let result = unsafe { (self.api.dereg_fd)(self.fd) };
        if result > 0 {
            tracing::warn!(
                fd = self.fd,
                result,
                "hf3fs_dereg_fd failed while closing registered file"
            );
        }
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

type RegFdFn = unsafe extern "C" fn(c_int, c_int) -> c_int;
type DeregFdFn = unsafe extern "C" fn(c_int) -> c_int;
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

struct Hf3fsNativeApi {
    handle: *mut c_void,
    reg_fd: RegFdFn,
    dereg_fd: DeregFdFn,
    iovcreate: IovCreateFn,
    iovdestroy: IovDestroyFn,
    iorcreate4: IorCreate4Fn,
    iordestroy: IorDestroyFn,
    prep_io: PrepIoFn,
    submit_ios: SubmitIosFn,
    wait_for_ios: WaitForIosFn,
}

impl Hf3fsNativeApi {
    fn load() -> StoreResult<Self> {
        let mut failures = Vec::new();
        for candidate in hf3fs_library_candidates() {
            let path = CString::new(candidate.as_bytes()).map_err(|_| {
                StoreError::InvalidParams(format!("HF3FS library path contains NUL: {candidate:?}"))
            })?;
            let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW) };
            if handle.is_null() {
                failures.push(format!("{candidate}: {}", last_dl_error()));
                continue;
            }
            let loaded = (|| unsafe {
                Ok::<Self, StoreError>(Self {
                    handle,
                    reg_fd: load_symbol(handle, "hf3fs_reg_fd")?,
                    dereg_fd: load_symbol(handle, "hf3fs_dereg_fd")?,
                    iovcreate: load_symbol(handle, "hf3fs_iovcreate")?,
                    iovdestroy: load_symbol(handle, "hf3fs_iovdestroy")?,
                    iorcreate4: load_symbol(handle, "hf3fs_iorcreate4")?,
                    iordestroy: load_symbol(handle, "hf3fs_iordestroy")?,
                    prep_io: load_symbol(handle, "hf3fs_prep_io")?,
                    submit_ios: load_symbol(handle, "hf3fs_submit_ios")?,
                    wait_for_ios: load_symbol(handle, "hf3fs_wait_for_ios")?,
                })
            })();
            match loaded {
                Ok(api) => return Ok(api),
                Err(error) => {
                    unsafe {
                        dlclose(handle);
                    }
                    failures.push(format!("{candidate}: {error}"));
                }
            }
        }
        Err(StoreError::Internal(format!(
            "unable to load HF3FS USRBIO library; set HF3FS_LIBRARY_PATH if needed ({})",
            failures.join("; ")
        )))
    }
}

impl Drop for Hf3fsNativeApi {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                dlclose(self.handle);
            }
        }
    }
}

struct Hf3fsUsrbioResource {
    api: Arc<Hf3fsNativeApi>,
    iov: Hf3fsIov,
    ior: Hf3fsIor,
}

impl Hf3fsUsrbioResource {
    fn new(api: Arc<Hf3fsNativeApi>, mount_path: &Path, for_read: bool) -> StoreResult<Self> {
        let mount = mount_path.to_str().ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "HF3FS mount path is not valid UTF-8: {}",
                mount_path.display()
            ))
        })?;
        let mount = CString::new(mount)?;
        let mut iov = unsafe { std::mem::zeroed::<Hf3fsIov>() };
        let mut ior = unsafe { std::mem::zeroed::<Hf3fsIor>() };
        let result = unsafe { (api.iovcreate)(&mut iov, mount.as_ptr(), HF3FS_IOV_SIZE, 0, -1) };
        if result < 0 {
            return Err(StoreError::Internal(format!(
                "hf3fs_iovcreate failed with code {result}"
            )));
        }
        if iov.base.is_null() || iov.size < HF3FS_IOV_SIZE {
            unsafe {
                (api.iovdestroy)(&mut iov);
            }
            return Err(StoreError::Internal(format!(
                "hf3fs_iovcreate returned an invalid buffer: base={:?}, size={}",
                iov.base, iov.size
            )));
        }
        let result = unsafe {
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
        if result < 0 {
            unsafe {
                (api.iovdestroy)(&mut iov);
            }
            return Err(StoreError::Internal(format!(
                "hf3fs_iorcreate4 failed with code {result}"
            )));
        }
        Ok(Self { api, iov, ior })
    }

    fn write_all(&mut self, file: &mut fs::File, data: &[u8]) -> StoreResult<()> {
        let mut written = 0;
        while written < data.len() {
            let requested = (data.len() - written).min(HF3FS_IOV_SIZE);
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr().add(written), self.iov.base, requested);
            }
            let completed = self.submit(file, false, written, requested)?;
            if completed == 0 || completed > requested {
                return Err(StoreError::Internal(format!(
                    "HF3FS write completed {completed} bytes for a {requested}-byte request"
                )));
            }
            written = written.checked_add(completed).ok_or_else(|| {
                StoreError::Internal("HF3FS write byte count overflow".to_string())
            })?;
        }
        Ok(())
    }

    fn read_exact(&mut self, file: &mut fs::File, length: usize) -> StoreResult<Vec<u8>> {
        let mut output = vec![0; length];
        let mut read = 0;
        while read < length {
            let requested = (length - read).min(HF3FS_IOV_SIZE);
            let completed = self.submit(file, true, read, requested)?;
            if completed == 0 || completed > requested {
                return Err(StoreError::Internal(format!(
                    "HF3FS read completed {completed} bytes for a {requested}-byte request"
                )));
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.iov.base,
                    output.as_mut_ptr().add(read),
                    completed,
                );
            }
            read = read.checked_add(completed).ok_or_else(|| {
                StoreError::Internal("HF3FS read byte count overflow".to_string())
            })?;
        }
        Ok(output)
    }

    fn submit(
        &mut self,
        file: &mut fs::File,
        for_read: bool,
        offset: usize,
        length: usize,
    ) -> StoreResult<usize> {
        let result = unsafe {
            (self.api.prep_io)(
                &self.ior,
                &self.iov,
                for_read,
                self.iov.base.cast(),
                file.as_raw_fd(),
                offset,
                length as u64,
                std::ptr::null(),
            )
        };
        if result < 0 {
            return Err(StoreError::Internal(format!(
                "hf3fs_prep_io failed with code {result}"
            )));
        }
        let result = unsafe { (self.api.submit_ios)(&self.ior) };
        if result < 0 {
            return Err(StoreError::Internal(format!(
                "hf3fs_submit_ios failed with code {result}"
            )));
        }
        let mut completion = Hf3fsCqe {
            index: 0,
            reserved: 0,
            result: 0,
            userdata: std::ptr::null(),
        };
        let result =
            unsafe { (self.api.wait_for_ios)(&self.ior, &mut completion, 1, 1, std::ptr::null()) };
        if result < 0 || completion.result < 0 {
            return Err(StoreError::Internal(format!(
                "hf3fs_wait_for_ios failed with code {result}, result {}",
                completion.result
            )));
        }
        usize::try_from(completion.result).map_err(|_| {
            StoreError::Internal(format!(
                "HF3FS completion byte count is out of range: {}",
                completion.result
            ))
        })
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
    if let Ok(path) = std::env::var("HF3FS_LIBRARY_PATH")
        && !path.is_empty()
    {
        return vec![path];
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
            CStr::from_ptr(error).to_string_lossy().into_owned()
        }
    }
}

unsafe fn load_symbol<T>(handle: *mut c_void, name: &str) -> StoreResult<T> {
    let name = CString::new(name)?;
    let symbol = unsafe { dlsym(handle, name.as_ptr()) };
    if symbol.is_null() {
        return Err(StoreError::Internal(format!(
            "missing HF3FS symbol {}: {}",
            name.to_string_lossy(),
            last_dl_error()
        )));
    }
    // SAFETY: callers bind each symbol to the matching declaration from the
    // HF3FS C API. Function pointers have the same size as `dlsym` results on
    // supported Unix platforms.
    Ok(unsafe { std::mem::transmute_copy(&symbol) })
}
