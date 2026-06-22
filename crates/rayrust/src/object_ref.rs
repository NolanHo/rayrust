//! Object reference — a handle to a future result in the Ray object store.
//!
//! The object ID is stored as `Vec<u8>` because Ray's ObjectID::Binary()
//! may contain null bytes.
//!
//! Both sync (`get()`) and async (`get_async()`) methods are provided.
//! The async variants use `tokio::task::spawn_blocking` to avoid blocking
//! the tokio runtime during Ray's blocking C++ FFI calls.

use std::marker::PhantomData;

use crate::error::RayError;
use crate::serialize::deserialize;

/// A reference to an object stored in the Ray object store.
///
/// Similar to Python's `ray.ObjectRef`. The object may or may not be
/// available yet — call `get()` (sync) or `get_async()` (async) to
/// retrieve the value.
#[derive(Debug, Clone)]
pub struct ObjectRef<T> {
    pub(crate) id: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T> ObjectRef<T> {
    /// Create an ObjectRef from a raw binary object ID.
    pub(crate) fn from_id(id: Vec<u8>) -> Self {
        ObjectRef {
            id,
            _marker: PhantomData,
        }
    }

    /// Get the raw object ID as bytes.
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
    /// Pass -1 for infinite wait.
    pub fn get_timeout(&self, timeout_ms: i32) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned,
    {
        crate::runtime::get_raw_timeout(&self.id, timeout_ms)
            .and_then(|bytes| deserialize(&bytes))
    }

    /// Asynchronously retrieve the object value.
    ///
    /// This wraps the blocking C++ `Get` call in `tokio::task::spawn_blocking`,
    /// allowing other tasks to run on the tokio runtime while waiting.
    pub async fn get_async(&self) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let id = self.id.clone();
        let bytes = tokio::task::spawn_blocking(move || crate::runtime::get_raw(&id))
            .await
            .map_err(|e| RayError::Runtime(format!("spawn_blocking join error: {}", e)))??;
        deserialize(&bytes)
    }

    /// Asynchronously retrieve the object value with a timeout.
    pub async fn get_timeout_async(&self, timeout_ms: i32) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let id = self.id.clone();
        let bytes =
            tokio::task::spawn_blocking(move || crate::runtime::get_raw_timeout(&id, timeout_ms))
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
