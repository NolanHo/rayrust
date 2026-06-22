//! Object reference — a handle to a future result in the Ray object store.
//!
//! ## Async architecture
//!
//! `get_async()` uses Ray's native `CoreWorker::GetAsync` API (non-blocking).
//! When the object arrives, CoreWorker's io_context thread calls our C
//! callback, which writes to an eventfd. The Rust side polls the eventfd
//! via `tokio::io::AsyncFd` — zero threads blocked while waiting.
//! After the eventfd fires, a fast `Get()` (instant, object is local)
//! runs in `spawn_blocking` to extract the data.

use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

use tokio::io::unix::AsyncFd;

use crate::error::RayError;
use crate::serialize::deserialize;

/// A reference to an object stored in the Ray object store.
#[derive(Debug, Clone)]
pub struct ObjectRef<T> {
    pub(crate) id: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T> ObjectRef<T> {
    pub(crate) fn from_id(id: Vec<u8>) -> Self {
        ObjectRef { id, _marker: PhantomData }
    }

    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// Retrieve the object value (blocks until ready).
    pub fn get(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        crate::runtime::get_raw(&self.id).and_then(|bytes| deserialize(&bytes))
    }

    /// Retrieve the object value with a timeout (in milliseconds).
    pub fn get_timeout(&self, timeout_ms: i32) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        crate::runtime::get_raw_timeout(&self.id, timeout_ms)
            .and_then(|bytes| deserialize(&bytes))
    }

    /// Asynchronously retrieve the object value.
    ///
    /// Uses Ray's native `CoreWorker::GetAsync` — non-blocking.
    /// Zero threads blocked while waiting for the object to arrive.
    pub async fn get_async(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.get_timeout_async(std::time::Duration::from_secs(300)).await
    }

    /// Asynchronously retrieve the object value with a custom timeout.
    pub async fn get_timeout_async(&self, timeout: std::time::Duration) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let id = self.id.clone();

        // Start async get — calls CoreWorker::GetAsync (non-blocking).
        // GetAsync posts a callback to CoreWorker's io_context and returns
        // immediately. No thread is blocked.
        let handle = AsyncGetHandle::new(id);

        let efd = handle.eventfd();
        if efd < 0 {
            return Err(RayError::Ffi("invalid eventfd".into()));
        }

        // Poll eventfd — zero threads blocked.
        // The async block only captures `efd` (i32) and no raw pointers,
        // so the resulting Future is Send.
        let ready = poll_eventfd(efd, timeout).await;
        drop(handle);

        if !ready {
            return Err(RayError::Runtime(format!(
                "get_async timed out after {:?}", timeout
            )));
        }

        // Object is now local — fast Get (instant) via spawn_blocking
        let id_for_get = self.id.clone();
        let bytes = tokio::task::spawn_blocking(move || crate::runtime::get_raw(&id_for_get))
            .await
            .map_err(|e| RayError::Runtime(format!("spawn_blocking join error: {}", e)))??;

        deserialize(&bytes)
    }

    /// Cast the phantom type parameter.
    pub fn cast<U>(self) -> ObjectRef<U> {
        ObjectRef::from_id(self.id)
    }
}

impl<T> From<Vec<u8>> for ObjectRef<T> {
    fn from(id: Vec<u8>) -> Self {
        ObjectRef::from_id(id)
    }
}

impl<T> From<ObjectRef<T>> for Vec<u8> {
    fn from(obj: ObjectRef<T>) -> Vec<u8> {
        obj.id
    }
}

// ── RAII handle for async get ─────────────────────────────────

/// Wraps the raw C pointer. Send+Sync because the pointer is only
/// accessed from spawn_blocking (start) and Drop (destroy), never
/// concurrently.
struct AsyncGetHandle(*mut std::ffi::c_void);

unsafe impl Send for AsyncGetHandle {}
unsafe impl Sync for AsyncGetHandle {}

impl AsyncGetHandle {
    fn new(id: Vec<u8>) -> Self {
        let id_ptr = id.as_ptr() as *const std::os::raw::c_char;
        let id_len = id.len();
        let ptr = unsafe { rayrust_sys::ray_async_get_start(id_ptr, id_len) };
        AsyncGetHandle(ptr)
    }

    fn eventfd(&self) -> i32 {
        if self.0.is_null() {
            return -1;
        }
        unsafe { rayrust_sys::ray_async_get_fd(self.0) }
    }
}

impl Drop for AsyncGetHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { rayrust_sys::ray_async_get_destroy(self.0) }
        }
    }
}

/// Poll an eventfd using tokio::io::AsyncFd.
/// Returns `true` if the object is ready, `false` on timeout.
///
/// This function captures only `efd: i32` — no raw pointers — so the
/// returned Future is Send and can be used with JoinSet / tokio::join!.
async fn poll_eventfd(efd: RawFd, timeout: std::time::Duration) -> bool {
    // dup the fd so AsyncFd can own it without closing the original
    let dup_fd = unsafe { libc::dup(efd) };
    if dup_fd < 0 {
        return false;
    }

    // Ensure non-blocking
    let flags = unsafe { libc::fcntl(dup_fd, libc::F_GETFL) };
    unsafe { libc::fcntl(dup_fd, libc::F_SETFL, flags | libc::O_NONBLOCK); }

    let file = unsafe { std::fs::File::from_raw_fd(dup_fd) };
    let async_fd = match AsyncFd::new(file) {
        Ok(fd) => fd,
        Err(_) => return false,
    };

    let poll = async {
        loop {
            let mut guard = match async_fd.readable().await {
                Ok(g) => g,
                Err(_) => return false,
            };

            // Read 8 bytes from eventfd (non-blocking)
            let mut buf = [0u8; 8];
            let raw_fd = guard.get_ref().as_raw_fd();
            let ret = unsafe {
                libc::read(raw_fd, buf.as_mut_ptr() as *mut libc::c_void, 8)
            };

            if ret > 0 {
                return true;
            }

            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                guard.clear_ready();
                continue;
            }

            return false;
        }
    };

    tokio::time::timeout(timeout, poll).await.unwrap_or(false)
}
