//! Ray runtime — init, shutdown, put, get, wait, task/actor operations.
//!
//! IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
//! We store them as `Vec<u8>` internally, not as Rust `String`.

use std::os::raw::c_int;

use rayrust_sys::{self, build_args_array, to_cstring, CBytesGuard, RayBytes};

use crate::error::RayError;
use crate::object_ref::ObjectRef;
use crate::serialize::serialize;

// ─── Config ───────────────────────────────────────────────────

/// Configuration for connecting to a Ray cluster.
#[derive(Debug, Clone, Default)]
pub struct RayConfig {
    /// Address of the head node, e.g. "192.168.42.141:6379".
    /// If empty, uses local mode.
    pub address: String,
    /// Run in local mode (single process, no cluster).
    pub local_mode: bool,
    /// Code search path for dynamic libraries (worker side).
    pub code_search_path: Vec<String>,
    /// Namespace for this job.
    pub namespace: String,
    /// Node IP address to use when registering with the cluster.
    /// If empty, the C++ SDK auto-detects (may fail with multiple NICs).
    pub node_ip: String,
}

impl RayConfig {
    /// Create a new config pointing to a remote cluster.
    pub fn new(address: impl Into<String>) -> Self {
        RayConfig {
            address: address.into(),
            ..Default::default()
        }
    }

    /// Use local mode (no cluster, single process).
    pub fn local() -> Self {
        RayConfig {
            local_mode: true,
            ..Default::default()
        }
    }

    /// Set the node IP address.
    pub fn node_ip(mut self, ip: impl Into<String>) -> Self {
        self.node_ip = ip.into();
        self
    }

    /// Set the code search path (directories or .so files for the worker).
    /// On the C++ SDK side, paths are joined with ':' and passed as
    /// `--ray_code_search_path`.
    pub fn code_search_path(mut self, paths: Vec<String>) -> Self {
        self.code_search_path = paths;
        self
    }
}

// ─── Lifecycle ────────────────────────────────────────────────

/// Initialize Ray runtime.
///
/// # Arguments
/// * `address` - Head node address, e.g. "192.168.42.141:6379".
///   Pass empty string for local mode.
pub fn init(address: &str) -> Result<(), RayError> {
    init_with_config(&RayConfig {
        address: address.to_string(),
        ..Default::default()
    })
}

/// Initialize Ray runtime with full config.
pub fn init_with_config(config: &RayConfig) -> Result<(), RayError> {
    let address_c = to_cstring(&config.address);
    let local_mode = if config.local_mode { 1 } else { 0 };
    let node_ip_c = to_cstring(&config.node_ip);
    let code_search_path_c = to_cstring(&config.code_search_path.join(":"));

    let ret = unsafe {
        rayrust_sys::ray_init(
            address_c.as_ptr(),
            local_mode,
            node_ip_c.as_ptr(),
            code_search_path_c.as_ptr(),
        )
    };
    if ret != 0 {
        return Err(RayError::Runtime(format!(
            "ray_init failed (code {})",
            ret
        )));
    }
    Ok(())
}

/// Check if Ray is initialized.
pub fn is_initialized() -> bool {
    unsafe { rayrust_sys::ray_is_initialized() }
}

/// Shutdown Ray runtime.
pub fn shutdown() {
    unsafe { rayrust_sys::ray_shutdown() }
}

// ─── Object Store ─────────────────────────────────────────────

/// Put an object into the object store.
/// Returns an ObjectRef that can be retrieved with `get`.
pub fn put<T: serde::Serialize>(value: &T) -> ObjectRef<T> {
    let data = serialize(value).expect("failed to serialize value for ray::put");
    put_raw(&data)
        .map(ObjectRef::from_id)
        .unwrap_or_else(|e| panic!("ray::put failed: {}", e))
}

/// Get an object from the object store.
/// Blocks until the object is available.
pub fn get<T: serde::de::DeserializeOwned>(obj_ref: &ObjectRef<T>) -> Result<T, RayError> {
    obj_ref.get()
}

/// Wait for objects to be locally available.
///
/// # Arguments
/// * `object_refs` - References to wait for.
/// * `num_objects` - Minimum number of objects to wait for.
/// * `timeout_ms` - Timeout in milliseconds. -1 for infinite.
///
/// # Returns
/// A tuple of (ready, unready) object references.
pub fn wait<T: Clone>(
    object_refs: &[ObjectRef<T>],
    num_objects: usize,
    timeout_ms: i32,
) -> Result<(Vec<ObjectRef<T>>, Vec<ObjectRef<T>>), RayError> {
    let ids: Vec<RayBytes> = object_refs
        .iter()
        .map(|r| RayBytes {
            data: r.id.as_ptr() as *const std::os::raw::c_char,
            len: r.id.len(),
        })
        .collect();

    let result = unsafe {
        rayrust_sys::ray_wait(
            ids.as_ptr(),
            ids.len(),
            num_objects as c_int,
            timeout_ms,
        )
    };

    if result.is_null() {
        return Err(RayError::Ffi("ray_wait returned null".into()));
    }

    let mut ready = Vec::new();
    let mut unready = Vec::new();
    for (i, obj) in object_refs.iter().enumerate() {
        let is_ready = unsafe { *result.add(i) };
        if is_ready {
            ready.push(obj.clone());
        } else {
            unready.push(obj.clone());
        }
    }
    unsafe { rayrust_sys::ray_free_bools(result) };

    Ok((ready, unready))
}

// ─── Raw object store operations ──────────────────────────────

/// Put raw bytes into the object store. Returns the binary object ID.
pub(crate) fn put_raw(data: &[u8]) -> Result<Vec<u8>, RayError> {
    let bytes = unsafe {
        rayrust_sys::ray_put(data.as_ptr() as *const std::os::raw::c_char, data.len())
    };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| RayError::Ffi("ray_put returned null".into()))?;
    Ok(guard.as_slice().to_vec())
}

/// Get raw bytes from the object store by binary ID. Blocks until ready.
pub(crate) fn get_raw(id: &[u8]) -> Result<Vec<u8>, RayError> {
    get_raw_timeout(id, -1)
}

/// Get raw bytes from the object store by binary ID with timeout.
pub(crate) fn get_raw_timeout(id: &[u8], timeout_ms: i32) -> Result<Vec<u8>, RayError> {
    let bytes = unsafe {
        rayrust_sys::ray_get(
            id.as_ptr() as *const std::os::raw::c_char,
            id.len(),
            timeout_ms as c_int,
        )
    };
    let guard = CBytesGuard::from(bytes).ok_or_else(|| {
        RayError::ObjectNotFound(format!("<{} bytes>", id.len()))
    })?;
    Ok(guard.as_slice().to_vec())
}

// ─── Task ─────────────────────────────────────────────────────

/// Call a remote task by function name.
/// Returns an ObjectRef for the result.
pub(crate) fn task_call_inner(func_name: &str, args: &[&[u8]]) -> Result<ObjectRef<()>, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);

    let bytes = unsafe {
        rayrust_sys::ray_task_call(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| RayError::Ffi(format!("ray_task_call '{}' returned null", func_name)))?;
    Ok(ObjectRef::from_id(guard.as_slice().to_vec()))
}

// ─── Actor ────────────────────────────────────────────────────

/// A handle to a remote actor.
#[derive(Debug, Clone)]
pub struct ActorHandle {
    pub(crate) id: Vec<u8>,
}

impl ActorHandle {
    /// Get the actor's ID as raw bytes.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// Kill the actor.
    pub fn kill(&self, no_restart: bool) {
        actor_kill_inner(&self.id, no_restart);
    }
}

/// Create an actor by factory function name.
pub(crate) fn actor_create_inner(func_name: &str, args: &[&[u8]]) -> Result<ActorHandle, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);

    let bytes = unsafe {
        rayrust_sys::ray_actor_create(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| RayError::Ffi(format!("ray_actor_create '{}' returned null", func_name)))?;
    Ok(ActorHandle { id: guard.as_slice().to_vec() })
}

/// Call a method on an actor by binary actor ID and function name.
pub(crate) fn actor_call_inner(
    actor_id: &[u8],
    func_name: &str,
    args: &[&[u8]],
) -> Result<ObjectRef<()>, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);

    let bytes = unsafe {
        rayrust_sys::ray_actor_call(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| RayError::Ffi(format!("ray_actor_call '{}.{}' returned null", actor_id.len(), func_name)))?;
    Ok(ObjectRef::from_id(guard.as_slice().to_vec()))
}

/// Kill an actor.
pub(crate) fn actor_kill_inner(actor_id: &[u8], no_restart: bool) {
    unsafe {
        rayrust_sys::ray_actor_kill(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            no_restart,
        )
    };
}

// ─── Misc ─────────────────────────────────────────────────────

/// Returns true if the current actor was restarted.
pub fn was_current_actor_restarted() -> bool {
    unsafe { rayrust_sys::ray_was_current_actor_restarted() }
}

/// Get the namespace of this job.
pub fn get_namespace() -> Result<String, RayError> {
    let bytes = unsafe { rayrust_sys::ray_get_namespace() };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| RayError::Ffi("ray_get_namespace returned null".into()))?;
    String::from_utf8(guard.as_slice().to_vec())
        .map_err(|e| RayError::Ffi(format!("namespace not valid UTF-8: {}", e)))
}
