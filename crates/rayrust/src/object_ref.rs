//! Object reference — a handle to a future result in the Ray object store.
//!
//! ## Async architecture
//!
//! `get_async()` uses a polling thread + eventfd + AsyncFd pattern:
//! - C++ polling thread: `Get(timeout=100ms)` loop, signals via eventfd
//! - Rust side: `tokio::io::AsyncFd` polls eventfd — zero threads blocked
//! - After eventfd fires: fast `Get()` (instant, object is local) + deserialize
//!
//! For cross-language (Python) results, the data has a 9-byte XLANG header
//! that is stripped before deserialization.

use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

use tokio::io::unix::AsyncFd;

use crate::error::RayError;
use crate::serialize::{deserialize, deserialize_xlang};

/// A reference to an object stored in the Ray object store.
#[derive(Debug, Clone)]
pub struct ObjectRef<T> {
    pub(crate) id: Vec<u8>,
    pub(crate) is_xlang: bool,
    _marker: PhantomData<T>,
}

impl<T> ObjectRef<T> {
    pub(crate) fn from_id(id: Vec<u8>) -> Self {
        ObjectRef { id, is_xlang: false, _marker: PhantomData }
    }

    pub(crate) fn from_id_xlang(id: Vec<u8>) -> Self {
        ObjectRef { id, is_xlang: true, _marker: PhantomData }
    }

    /// Get the raw object ID as bytes.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// Get the object ID as a hex string (for debugging).
    pub fn id_hex(&self) -> String {
        self.id.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Retrieve the object value (blocks until ready).
    pub fn get(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        let bytes = crate::runtime::get_raw(&self.id)?;
        if self.is_xlang {
            deserialize_xlang(&bytes)
        } else {
            deserialize(&bytes)
        }
    }

    /// Retrieve the object value with a timeout (in milliseconds).
    pub fn get_timeout(&self, timeout_ms: i32) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        let bytes = crate::runtime::get_raw_timeout(&self.id, timeout_ms)?;
        if self.is_xlang {
            deserialize_xlang(&bytes)
        } else {
            deserialize(&bytes)
        }
    }

    /// Asynchronously retrieve the object value.
    ///
    /// Uses polling thread + eventfd + AsyncFd — zero tokio threads blocked.
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

        let handle = AsyncGetHandle::new(id);

        let efd = handle.eventfd();
        if efd < 0 {
            return Err(RayError::Ffi("invalid eventfd".into()));
        }

        let ready = poll_eventfd(efd, timeout).await;
        drop(handle);

        if !ready {
            return Err(RayError::Runtime(format!(
                "get_async timed out after {:?}", timeout
            )));
        }

        // Object is now local — fast Get (instant)
        let id_for_get = self.id.clone();
        let is_xlang = self.is_xlang;
        let bytes = tokio::task::spawn_blocking(move || crate::runtime::get_raw(&id_for_get))
            .await
            .map_err(|e| RayError::Runtime(format!("spawn_blocking join error: {}", e)))??;

        if is_xlang {
            deserialize_xlang(&bytes)
        } else {
            deserialize(&bytes)
        }
    }

    /// Cast the phantom type parameter.
    pub fn cast<U>(self) -> ObjectRef<U> {
        ObjectRef {
            id: self.id,
            is_xlang: self.is_xlang,
            _marker: PhantomData,
        }
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
        if self.0.is_null() { return -1; }
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

async fn poll_eventfd(efd: RawFd, timeout: std::time::Duration) -> bool {
    let dup_fd = unsafe { libc::dup(efd) };
    if dup_fd < 0 { return false; }

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

            let mut buf = [0u8; 8];
            let raw_fd = guard.get_ref().as_raw_fd();
            let ret = unsafe {
                libc::read(raw_fd, buf.as_mut_ptr() as *mut libc::c_void, 8)
            };

            if ret > 0 { return true; }

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
