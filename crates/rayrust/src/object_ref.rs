//! Object reference — a handle to a future result in the Ray object store.
//!
//! The object ID is stored as `Vec<u8>` because Ray's ObjectID::Binary()
//! may contain null bytes.
//!
//! Both sync (`get()`) and async (`get_async()`) methods are provided.
//! The async variant wraps the blocking C++ `Get` call in
//! `tokio::task::spawn_blocking`. This is the standard pattern for
//! bridging blocking FFI into async Rust — `spawn_blocking` runs the
//! call on a dedicated thread pool that is separate from the async
//! runtime's worker threads, so other async tasks keep running.
//!
//! An optional `timeout` can be applied via `get_timeout_async()` which
//! wraps the future in `tokio::time::timeout`. Dropping the future
//! (e.g. via `select!`) cancels the await, though the underlying
//! `spawn_blocking` thread will continue until `Get` returns.

use std::marker::PhantomData;

use crate::error::RayError;
use crate::serialize::deserialize;

/// Default timeout for async get: 30 seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

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
    /// Wraps the blocking C++ `Get` call in `tokio::task::spawn_blocking`.
    /// This uses a dedicated blocking thread pool (separate from the async
    /// runtime's worker threads), so other async tasks continue running
    /// while waiting for the Ray object.
    ///
    /// A default timeout of 30 seconds is applied. If the object is not
    /// available within that time, `Err(RayError::Runtime(...))` is returned.
    /// Use `get_timeout_async()` to customize the timeout.
    ///
    /// # Cancellation
    /// Dropping the returned future cancels the `await`, but the underlying
    /// `spawn_blocking` thread will continue until `Get` returns. This is a
    /// limitation of bridging blocking C++ FFI into async Rust.
    pub async fn get_async(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.get_timeout_async(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    /// Asynchronously retrieve the object value with a custom timeout.
    ///
    /// The timeout wraps the entire `spawn_blocking(Get)` future via
    /// `tokio::time::timeout`. If the timeout expires, the future is
    /// dropped (cancelled at the async level), but the blocking thread
    /// may continue running until `Get` returns.
    pub async fn get_timeout_async(&self, timeout: std::time::Duration) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let id = self.id.clone();
        let join = tokio::task::spawn_blocking(move || crate::runtime::get_raw(&id));

        match tokio::time::timeout(timeout, join).await {
            Ok(Ok(bytes)) => {
                let bytes = bytes?;
                deserialize(&bytes)
            }
            Ok(Err(join_err)) => Err(RayError::Runtime(format!(
                "spawn_blocking join error: {}",
                join_err
            ))),
            Err(_) => Err(RayError::Runtime(format!(
                "get_async timed out after {:?}",
                timeout
            ))),
        }
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
