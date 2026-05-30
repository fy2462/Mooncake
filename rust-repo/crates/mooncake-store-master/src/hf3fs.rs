// =============================================================================
// HF3FS (3FS) Filesystem Integration — HF3FS (3FS) 文件系统集成
// =============================================================================
// Provides integration with the HF3FS (3FS) distributed filesystem via dynamic
// loading of the native C library (libhf3fs_api_shared.so).
// 通过动态加载原生 C 库（libhf3fs_api_shared.so）提供与 HF3FS (3FS) 分布式
// 文件系统的集成。
//
// Purpose / 目的:
// HF3FS is a high-performance distributed filesystem. File descriptors opened on
// HF3FS must be explicitly registered with the HF3FS client library to ensure
// proper I/O path routing. This module handles dlopen/dlsym-based loading of the
// HF3FS API and provides RAII-based fd registration/deregistration.
// HF3FS 是高性能分布式文件系统。在 HF3FS 上打开的文件描述符必须显式注册到
// HF3FS 客户端库，以确保正确的 I/O 路径路由。本模块处理基于 dlopen/dlsym 的
// HF3FS API 加载，并提供基于 RAII 的 fd 注册/注销。
//
// Key types / 关键类型:
// - Hf3fsApi trait: abstraction for reg_fd / dereg_fd (testable via mock).
//   Hf3fsApi trait：reg_fd / dereg_fd 的抽象（可通过 mock 测试）。
// - Hf3fsRegistration: RAII guard that deregisters the fd on drop.
//   Hf3fsRegistration：RAII 守卫，在 drop 时注销 fd。
// - LoadedHf3fsApi: concrete implementation via dynamically loaded C symbols.
//   LoadedHf3fsApi：通过动态加载 C 符号的具体实现。

use libc::{c_char, c_int, c_void, dlclose, dlerror, dlopen, dlsym, RTLD_NOW};
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::RawFd;
use std::sync::{Arc, OnceLock, RwLock};

// C function pointer types for the HF3FS API
// HF3FS API 的 C 函数指针类型
type RegFdFn = unsafe extern "C" fn(fd: c_int, flags: c_int) -> c_int;
type DeregFdFn = unsafe extern "C" fn(fd: c_int) -> c_int;

/// Abstract interface to the HF3FS API, enabling test mocking.
/// HF3FS API 的抽象接口，允许测试 mock。
pub trait Hf3fsApi: Send + Sync {
    /// Register a file descriptor with the HF3FS client library.
    /// 向 HF3FS 客户端库注册文件描述符。
    /// Returns 0 on success, non-zero on error.
    /// 成功返回 0，错误返回非 0。
    fn reg_fd(&self, fd: RawFd, flags: c_int) -> io::Result<c_int>;
    /// Deregister a file descriptor from the HF3FS client library.
    /// 从 HF3FS 客户端库注销文件描述符。
    fn dereg_fd(&self, fd: RawFd) -> io::Result<c_int>;
}

/// Concrete implementation: loads the HF3FS shared library and resolves symbols.
/// 具体实现：加载 HF3FS 共享库并解析符号。
struct LoadedHf3fsApi {
    /// Handle returned by dlopen; dlclose'd on drop.
    /// dlopen 返回的句柄；在 drop 时 dlclose。
    handle: *mut c_void,
    reg_fd: RegFdFn,
    dereg_fd: DeregFdFn,
}

// SAFETY: LoadedHf3fsApi uses raw C pointers but is only accessed from Rust threads.
// The underlying HF3FS library is expected to be thread-safe.
// 安全性：LoadedHf3fsApi 使用原始 C 指针，但仅由 Rust 线程访问。
// 底层 HF3FS 库预期是线程安全的。
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

/// RAII guard that holds an HF3FS fd registration.
/// RAII 守卫，持有 HF3FS fd 注册。
///
/// When this struct is dropped, it automatically calls dereg_fd on the
/// associated file descriptor, ensuring cleanup even on error paths.
/// 当此结构体被 drop 时，自动对关联的文件描述符调用 dereg_fd，
/// 确保即使在错误路径上也能清理。
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

// =============================================================================
// API Resolution / API 解析
// =============================================================================

/// Global override cell for injecting mock APIs during tests.
/// 全局覆盖单元，用于在测试期间注入 mock API。
/// Uses OnceLock + RwLock for lazy initialization and safe mutation.
/// 使用 OnceLock + RwLock 实现延迟初始化和安全修改。
fn api_override_cell() -> &'static RwLock<Option<Arc<dyn Hf3fsApi>>> {
    static CELL: OnceLock<RwLock<Option<Arc<dyn Hf3fsApi>>>> = OnceLock::new();
    CELL.get_or_init(|| RwLock::new(None))
}

/// Set a test API override. When set, resolve_api() returns this instead of
/// loading the real library.
/// 设置测试 API 覆盖。设置后，resolve_api() 返回此 mock 而非加载真实库。
#[doc(hidden)]
pub fn set_api_override_for_test(api: Option<Arc<dyn Hf3fsApi>>) {
    let mut guard = api_override_cell()
        .write()
        .expect("hf3fs api override lock poisoned");
    *guard = api;
}

/// Get the last dlerror message as a Rust string.
/// 获取最近的 dlerror 消息作为 Rust 字符串。
fn last_dl_error() -> String {
    unsafe {
        let error = dlerror();
        if error.is_null() {
            return "unknown dlopen error".to_string();
        }
        CStr::from_ptr(error).to_string_lossy().into_owned()
    }
}

/// Safely load a symbol from a dlopen'd handle, returning a typed function pointer.
/// 安全地从 dlopen'd 句柄加载符号，返回类型化的函数指针。
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

/// Candidate library paths to try when loading the HF3FS shared library.
/// 加载 HF3FS 共享库时尝试的候选路径列表。
///
/// Priority / 优先级:
/// 1. HF3FS_LIBRARY_PATH environment variable (if set).
///    HF3FS_LIBRARY_PATH 环境变量（若设置）。
/// 2. System library paths (libhf3fs_api_shared.so / .dylib).
///    系统库路径（libhf3fs_api_shared.so / .dylib）。
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

/// Attempt to load the HF3FS shared library and resolve the required symbols.
/// 尝试加载 HF3FS 共享库并解析所需符号。
///
/// Tries each candidate path in order; on success, returns the loaded API.
/// On failure, returns an error with details about each failed path.
/// 按顺序尝试每个候选路径；成功时返回加载的 API。
/// 失败时返回包含每个失败路径详情的错误。
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

        // Attempt to load the shared library with RTLD_NOW (resolve all symbols immediately)
        // 尝试使用 RTLD_NOW 加载共享库（立即解析所有符号）
        let handle = unsafe { dlopen(path.as_ptr() as *const c_char, RTLD_NOW) };
        if handle.is_null() {
            last_error = Some(format!("{}: {}", candidate, last_dl_error()));
            continue;
        }

        // Resolve the two required symbols: hf3fs_reg_fd and hf3fs_dereg_fd
        // 解析两个必需的符号：hf3fs_reg_fd 和 hf3fs_dereg_fd
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

/// Resolve the HF3FS API: returns the test override if set, otherwise tries to
/// load the real library.
/// 解析 HF3FS API：若设置了测试覆盖则返回之，否则尝试加载真实库。
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

/// Public entry point: register a file descriptor with the HF3FS library.
/// 公开入口：向 HF3FS 库注册文件描述符。
///
/// Returns an Hf3fsRegistration that automatically deregisters the fd on drop.
/// 返回一个 Hf3fsRegistration，在其 drop 时自动注销 fd。
///
/// Usage / 用法:
/// ```ignore
/// let file = std::fs::File::open("/hf3fs/path/to/file")?;
/// let _reg = hf3fs::register_fd(file.as_raw_fd())?;
/// // ... use file ... (registration auto-deregisters on drop)
/// ```
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
