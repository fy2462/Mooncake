use libc::{c_char, c_int, c_void, dlclose, dlerror, dlopen, dlsym, RTLD_NOW};
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::RawFd;
use std::sync::{Arc, OnceLock, RwLock};

type RegFdFn = unsafe extern "C" fn(fd: c_int, flags: c_int) -> c_int;
type DeregFdFn = unsafe extern "C" fn(fd: c_int) -> c_int;

pub trait Hf3fsApi: Send + Sync {
    fn reg_fd(&self, fd: RawFd, flags: c_int) -> io::Result<c_int>;
    fn dereg_fd(&self, fd: RawFd) -> io::Result<c_int>;
}

struct LoadedHf3fsApi {
    handle: *mut c_void,
    reg_fd: RegFdFn,
    dereg_fd: DeregFdFn,
}

unsafe impl Send for LoadedHf3fsApi {}
unsafe impl Sync for LoadedHf3fsApi {}

impl Drop for LoadedHf3fsApi {
    fn drop(&mut self) {
        unsafe {
            if !self.handle.is_null() {
                dlclose(self.handle);
            }
        }
    }
}

impl Hf3fsApi for LoadedHf3fsApi {
    fn reg_fd(&self, fd: RawFd, flags: c_int) -> io::Result<c_int> {
        Ok(unsafe { (self.reg_fd)(fd, flags) })
    }

    fn dereg_fd(&self, fd: RawFd) -> io::Result<c_int> {
        Ok(unsafe { (self.dereg_fd)(fd) })
    }
}

pub struct Hf3fsRegistration {
    fd: RawFd,
    api: Arc<dyn Hf3fsApi>,
}

impl Drop for Hf3fsRegistration {
    fn drop(&mut self) {
        if let Err(error) = self.api.dereg_fd(self.fd) {
            tracing::warn!("failed to deregister hf3fs fd {}: {}", self.fd, error);
        }
    }
}

fn api_override_cell() -> &'static RwLock<Option<Arc<dyn Hf3fsApi>>> {
    static CELL: OnceLock<RwLock<Option<Arc<dyn Hf3fsApi>>>> = OnceLock::new();
    CELL.get_or_init(|| RwLock::new(None))
}

#[doc(hidden)]
pub fn set_api_override_for_test(api: Option<Arc<dyn Hf3fsApi>>) {
    let mut guard = api_override_cell()
        .write()
        .expect("hf3fs api override lock poisoned");
    *guard = api;
}

fn last_dl_error() -> String {
    unsafe {
        let error = dlerror();
        if error.is_null() {
            return "unknown dlopen error".to_string();
        }
        CStr::from_ptr(error).to_string_lossy().into_owned()
    }
}

unsafe fn load_symbol<T>(handle: *mut c_void, name: &CStr) -> io::Result<T> {
    let symbol = dlsym(handle, name.as_ptr());
    if symbol.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "missing hf3fs symbol {}: {}",
                name.to_string_lossy(),
                last_dl_error()
            ),
        ));
    }
    Ok(std::mem::transmute_copy(&symbol))
}

fn candidate_library_paths() -> Vec<String> {
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

fn load_api() -> io::Result<Arc<dyn Hf3fsApi>> {
    let reg_fd_name = CString::new("hf3fs_reg_fd").expect("valid hf3fs symbol");
    let dereg_fd_name = CString::new("hf3fs_dereg_fd").expect("valid hf3fs symbol");
    let mut last_error = None;

    for candidate in candidate_library_paths() {
        let path = CString::new(candidate.clone()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid hf3fs library path: {}", candidate),
            )
        })?;

        let handle = unsafe { dlopen(path.as_ptr() as *const c_char, RTLD_NOW) };
        if handle.is_null() {
            last_error = Some(format!("{}: {}", candidate, last_dl_error()));
            continue;
        }

        let reg_fd = match unsafe { load_symbol::<RegFdFn>(handle, &reg_fd_name) } {
            Ok(symbol) => symbol,
            Err(error) => {
                unsafe { dlclose(handle) };
                last_error = Some(format!("{}: {}", candidate, error));
                continue;
            }
        };
        let dereg_fd = match unsafe { load_symbol::<DeregFdFn>(handle, &dereg_fd_name) } {
            Ok(symbol) => symbol,
            Err(error) => {
                unsafe { dlclose(handle) };
                last_error = Some(format!("{}: {}", candidate, error));
                continue;
            }
        };

        return Ok(Arc::new(LoadedHf3fsApi {
            handle,
            reg_fd,
            dereg_fd,
        }));
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "unable to load hf3fs library; set HF3FS_LIBRARY_PATH if needed ({})",
            last_error.unwrap_or_else(|| "no candidate library path succeeded".to_string())
        ),
    ))
}

fn resolve_api() -> io::Result<Arc<dyn Hf3fsApi>> {
    if let Some(api) = api_override_cell()
        .read()
        .expect("hf3fs api override lock poisoned")
        .clone()
    {
        return Ok(api);
    }
    load_api()
}

pub fn register_fd(fd: RawFd) -> io::Result<Hf3fsRegistration> {
    let api = resolve_api()?;
    let result = api.reg_fd(fd, 0)?;
    if result > 0 {
        return Err(io::Error::other(format!(
            "hf3fs_reg_fd failed for fd {} with code {}",
            fd, result
        )));
    }
    Ok(Hf3fsRegistration { fd, api })
}
