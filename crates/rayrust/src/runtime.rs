//! Ray runtime — init, shutdown, put, get, wait, task/actor operations.
//!
//! IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
//! We store them as `Vec<u8>` internally, not as Rust `String`.

use std::os::raw::c_int;

use rayrust_sys::{self, build_args_array, to_cstring, CBytesGuard, RayBytes};

use crate::error::RayError;
use crate::object_ref::ObjectRef;
use crate::serialize::serialize;

/// Build an FFI error with the C-side last_error message if available.
fn ffi_error(context: &str) -> RayError {
    let msg = rayrust_sys::last_error()
        .unwrap_or_else(|| "unknown FFI error".to_string());
    RayError::Ffi(format!("{}: {}", context, msg))
}

/// Build an FFI error with context and a formatted detail string.
fn ffi_error_detail(context: &str, detail: impl std::fmt::Display) -> RayError {
    let msg = rayrust_sys::last_error()
        .unwrap_or_else(|| "unknown FFI error".to_string());
    RayError::Ffi(format!("{} ({}): {}", context, detail, msg))
}

/// Build a JSON string from resource (name, value) pairs.
/// E.g. [("CPU", 2.0), ("GPU", 0.5)] → `{"CPU":2.0,"GPU":0.5}`
fn build_resources_json(resources: &[(&str, f64)]) -> String {
    if resources.is_empty() {
        return String::new();
    }
    let mut parts = Vec::new();
    for (name, value) in resources {
        // Format value: integers without decimal, floats with
        let val_str = if *value == value.trunc() {
            format!("{}", *value as i64)
        } else {
            format!("{}", value)
        };
        parts.push(format!("\"{}\":{}", name, val_str));
    }
    format!("{{{}}}", parts.join(","))
}

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
    /// Runtime environment JSON string (e.g. `{"pip": ["pkg1", "pkg2"]}`).
    /// If empty, no runtime_env is set.
    pub runtime_env: String,
    /// Directory for Ray logs. If empty, uses default.
    pub log_dir: String,
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
    pub fn code_search_path(mut self, paths: Vec<String>) -> Self {
        self.code_search_path = paths;
        self
    }

    /// Set the runtime environment JSON string.
    /// Example: `{"pip": ["numpy", "pandas"]}` or `{"env_vars": {"FOO": "bar"}}`
    pub fn runtime_env(mut self, json: impl Into<String>) -> Self {
        self.runtime_env = json.into();
        self
    }

    /// Set the log directory.
    pub fn log_dir(mut self, dir: impl Into<String>) -> Self {
        self.log_dir = dir.into();
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
    let runtime_env_c = to_cstring(&config.runtime_env);
    let log_dir_c = to_cstring(&config.log_dir);

    let ret = unsafe {
        rayrust_sys::ray_init(
            address_c.as_ptr(),
            local_mode,
            node_ip_c.as_ptr(),
            code_search_path_c.as_ptr(),
            runtime_env_c.as_ptr(),
            log_dir_c.as_ptr(),
        )
    };
    if ret != 0 {
        return Err(RayError::Runtime(format!(
            "ray_init failed (code {}): {}",
            ret,
            rayrust_sys::last_error().unwrap_or_default()
        )));
    }

    // Warn if C++ SDK (default_worker) is not installed on this node.
    // This is needed for cluster-mode Rust actors — raylet launches default_worker
    // on worker nodes, and if it's missing, actors will silently die (NODE_DIED).
    if !config.local_mode {
        check_cpp_sdk();
    }

    Ok(())
}

/// Check if the Ray C++ SDK is properly installed on this node.
/// Prints a warning if `default_worker` is missing — this node cannot run C++ actors.
fn check_cpp_sdk() {
    let ray_path = std::process::Command::new("python3")
        .args(["-c", "import ray, os; print(os.path.dirname(ray.__file__))"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());

    if let Some(ray_dir) = ray_path {
        let default_worker = std::path::Path::new(&ray_dir)
            .join("cpp")
            .join("default_worker");
        if !default_worker.exists() {
            eprintln!("\n⚠️  WARNING: Ray C++ SDK not found on this node (missing {}).", default_worker.display());
            eprintln!("   Rust actors require 'pip install ray[cpp]' on ALL worker nodes.");
            eprintln!("   Without it, actor_create() succeeds but actor_call() hangs (NODE_DIED).");
            eprintln!("   Use local mode (RAY_ADDRESS=local) if C++ SDK is unavailable.\n");
        } else {
            // SDK exists on driver node — remind user to check worker nodes too
            eprintln!("ℹ️  Note: Rust actors in cluster mode require 'pip install ray[cpp]' on ALL worker nodes,");
            eprintln!("   not just this node. If actor_call() hangs, check that worker nodes have the C++ SDK.");
        }
    }
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

/// Put a value into the object store with XLANG header wrapping.
///
/// Use this when the data will be consumed by Python via pass-by-reference
/// (task_call_python with is_ref=true). Python's xlang deserialization
/// expects the 9-byte XLANG header before the msgpack payload.
///
/// The returned ObjectRef ID can be passed to `task_call_python` as a
/// reference arg, avoiding re-serialization of large data per task call.
pub fn put_xlang<T: serde::Serialize>(value: &T) -> ObjectRef<T> {
    let data = crate::serialize::serialize_xlang(value)
        .expect("failed to serialize_xlang value for ray::put_xlang");
    put_raw(&data)
        .map(ObjectRef::from_id_xlang)
        .unwrap_or_else(|e| panic!("ray::put_xlang failed: {}", e))
}

/// Asynchronously put an object into the object store.
pub async fn put_async<T>(value: T) -> Result<ObjectRef<T>, RayError>
where
    T: serde::Serialize + Send + 'static,
{
    let bytes = tokio::task::spawn_blocking(move || serialize(&value))
        .await
        .map_err(|e| RayError::Runtime(format!("serialize join error: {}", e)))??;
    let id = tokio::task::spawn_blocking(move || put_raw(&bytes))
        .await
        .map_err(|e| RayError::Runtime(format!("put_raw join error: {}", e)))??;
    Ok(ObjectRef::from_id(id))
}

/// Get an object from the object store.
/// Blocks until the object is available.
pub fn get<T: serde::de::DeserializeOwned>(obj_ref: &ObjectRef<T>) -> Result<T, RayError> {
    obj_ref.get()
}

/// Asynchronously get an object from the object store.
pub async fn get_async<T>(obj_ref: &ObjectRef<T>) -> Result<T, RayError>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    obj_ref.get_async().await
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
        return Err(ffi_error("ray_wait"));
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
        .ok_or_else(|| ffi_error("ray_put"))?;
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

/// Check if a single object is locally available (non-blocking poll).
/// Uses Ray's `Wait` with a timeout. Returns `true` if the object is ready.
#[allow(dead_code)]
pub(crate) fn wait_raw(id: &[u8], timeout_ms: i32) -> Result<bool, RayError> {
    let ray_bytes = RayBytes {
        data: id.as_ptr() as *const std::os::raw::c_char,
        len: id.len(),
    };
    let ids = [ray_bytes];

    let result = unsafe {
        rayrust_sys::ray_wait(
            ids.as_ptr(),
            1,
            1 as c_int,
            timeout_ms,
        )
    };

    if result.is_null() {
        return Err(ffi_error("ray_wait"));
    }

    let is_ready = unsafe { *result };
    unsafe { rayrust_sys::ray_free_bools(result) };

    Ok(is_ready)
}

// ─── Task ─────────────────────────────────────────────────────

/// Call a remote task by function name.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
/// `is_ref` can be empty slice to treat all args as values.
pub(crate) fn task_call_inner(
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<ObjectRef<()>, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_task_call(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_task_call", func_name))?;
    Ok(ObjectRef::from_id(guard.as_slice().to_vec()))
}

/// Call a remote task with resource requirements.
/// `resources` is a slice of (name, value) pairs, e.g. [("CPU", 2.0), ("GPU", 0.5)].
pub(crate) fn task_call_with_resources_inner(
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
    resources: &[(&str, f64)],
) -> Result<ObjectRef<()>, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };
    let resources_json = if resources.is_empty() {
        std::ptr::null()
    } else {
        let json = build_resources_json(resources);
        // Leak the CString to keep it alive during the FFI call
        Box::leak(to_cstring(&json).into_boxed_c_str()).as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_task_call_with_resources(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
            resources_json,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_task_call_with_resources", func_name))?;
    Ok(ObjectRef::from_id(guard.as_slice().to_vec()))
}

/// Asynchronously call a remote task by function name.
pub(crate) async fn task_call_inner_async(
    func_name: String,
    args: Vec<Vec<u8>>,
    is_ref: Vec<bool>,
) -> Result<ObjectRef<()>, RayError> {
    tokio::task::spawn_blocking(move || {
        let args_ref: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        task_call_inner(&func_name, &args_ref, &is_ref)
    })
    .await
    .map_err(|e| RayError::Runtime(format!("task_call join error: {}", e)))?
}

/// Call a Python remote function.
/// `module` is the Python module name, `function` is the function name.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
/// Returns an ObjectRef with is_xlang=true (results need xlang header stripping).
pub(crate) fn task_call_python_inner(
    module: &str,
    function: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<Vec<u8>, RayError> {
    let module_c = to_cstring(module);
    let func_c = to_cstring(function);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_task_call_python(
            module_c.as_ptr(),
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_task_call_python", &format!("{}.{}", module, function)))?;
    Ok(guard.as_slice().to_vec())
}

// ─── Actor ────────────────────────────────────────────────────

/// A handle to a remote actor.
#[derive(Debug, Clone)]
pub struct ActorHandle {
    pub(crate) id: Vec<u8>,
    pub(crate) is_python: bool,
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
/// `is_ref` marks which args are ObjectRef IDs (pass by reference).
pub(crate) fn actor_create_inner(
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<ActorHandle, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_actor_create(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_create", func_name))?;
    Ok(ActorHandle { id: guard.as_slice().to_vec(), is_python: false })
}

/// Create an actor with resource requirements.
pub(crate) fn actor_create_with_resources_inner(
    func_name: &str,
    args: &[&[u8]],
    resources: &[(&str, f64)],
) -> Result<ActorHandle, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let resources_json = if resources.is_empty() {
        std::ptr::null()
    } else {
        let json = build_resources_json(resources);
        Box::leak(to_cstring(&json).into_boxed_c_str()).as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_actor_create_with_resources(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            resources_json,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_create_with_resources", func_name))?;
    Ok(ActorHandle { id: guard.as_slice().to_vec(), is_python: false })
}

/// Create a Python actor.
pub(crate) fn actor_create_python_inner(module: &str, class: &str, args: &[&[u8]]) -> Result<ActorHandle, RayError> {
    let module_c = to_cstring(module);
    let class_c = to_cstring(class);
    let args_arr = build_args_array(args);

    let bytes = unsafe {
        rayrust_sys::ray_actor_create_python(
            module_c.as_ptr(),
            class_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_create_python", &format!("{}.{}", module, class)))?;
    Ok(ActorHandle { id: guard.as_slice().to_vec(), is_python: true })
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
        .ok_or_else(|| ffi_error_detail("ray_actor_call", func_name))?;
    Ok(ObjectRef::from_id(guard.as_slice().to_vec()))
}

/// Asynchronously call a method on an actor.
pub(crate) async fn actor_call_inner_async(
    actor_id: Vec<u8>,
    func_name: String,
    args: Vec<Vec<u8>>,
) -> Result<ObjectRef<()>, RayError> {
    tokio::task::spawn_blocking(move || {
        let args_ref: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        actor_call_inner(&actor_id, &func_name, &args_ref)
    })
    .await
    .map_err(|e| RayError::Runtime(format!("actor_call join error: {}", e)))?
}

/// Call a method on a Python actor.
/// `is_ref[i] = true` means args[i] is a binary ObjectRef ID (pass by reference).
pub(crate) fn actor_call_python_inner(
    actor_id: &[u8],
    method_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
) -> Result<Vec<u8>, RayError> {
    let method_c = to_cstring(method_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };

    let bytes = unsafe {
        rayrust_sys::ray_actor_call_python(
            actor_id.as_ptr() as *const std::os::raw::c_char,
            actor_id.len(),
            method_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            is_ref_ptr,
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_call_python", method_name))?;
    Ok(guard.as_slice().to_vec())
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

// ─── Placement Group ──────────────────────────────────────────

/// Create a placement group.
/// `bundles_json` is a JSON array like `[{"CPU":1},{"CPU":1}]`.
/// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
pub(crate) fn placement_group_create_inner(
    name: &str,
    bundles_json: &str,
    strategy: i32,
) -> Result<Vec<u8>, RayError> {
    let name_c = to_cstring(name);
    let json_c = to_cstring(bundles_json);
    let bytes = unsafe {
        rayrust_sys::ray_placement_group_create(
            name_c.as_ptr(),
            json_c.as_ptr(),
            strategy,
        )
    };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error("ray_placement_group_create"))?;
    Ok(guard.as_slice().to_vec())
}

/// Remove a placement group by binary ID.
pub(crate) fn placement_group_remove_inner(group_id: &[u8]) {
    unsafe {
        rayrust_sys::ray_placement_group_remove(
            group_id.as_ptr() as *const std::os::raw::c_char,
            group_id.len(),
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
    if !is_initialized() {
        return Ok(String::new());
    }
    let bytes = unsafe { rayrust_sys::ray_get_namespace() };
    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error("ray_get_namespace"))?;
    String::from_utf8(guard.as_slice().to_vec())
        .map_err(|e| RayError::Ffi(format!("namespace not valid UTF-8: {}", e)))
}

/// Get a named actor by name.
/// `namespace` can be empty string for current namespace.
/// Returns Ok(Some(handle)) if found, Ok(None) if not found.
pub fn get_actor(name: &str, namespace: &str) -> Result<Option<ActorHandle>, RayError> {
    let name_c = to_cstring(name);
    let ns_c = to_cstring(namespace);
    let bytes = unsafe { rayrust_sys::ray_get_actor(name_c.as_ptr(), ns_c.as_ptr()) };
    let guard = CBytesGuard::from(bytes);
    match guard {
        Some(g) => {
            let id = g.as_slice().to_vec();
            if id.is_empty() {
                Ok(None)
            } else {
                Ok(Some(ActorHandle { id, is_python: false }))
            }
        }
        None => Ok(None),
    }
}

/// Cancel a remote task by object ID.
/// `force_kill` kills the worker process if true.
/// `recursive` cancels dependent tasks.
pub fn cancel(obj_id: &[u8], force_kill: bool, recursive: bool) -> Result<(), RayError> {
    let ret = unsafe {
        rayrust_sys::ray_cancel(
            obj_id.as_ptr() as *const std::os::raw::c_char,
            obj_id.len(),
            force_kill,
            recursive,
        )
    };
    if ret != 0 {
        return Err(RayError::Runtime(format!("ray_cancel failed (code {})", ret)));
    }
    Ok(())
}

/// Get multiple objects from the object store.
/// Blocks until all objects are available.
pub fn get_many<T: serde::de::DeserializeOwned>(refs: &[ObjectRef<T>]) -> Result<Vec<T>, RayError> {
    let ids: Vec<RayBytes> = refs.iter().map(|r| RayBytes {
        data: r.id().as_ptr() as *const std::os::raw::c_char,
        len: r.id().len(),
    }).collect();

    let result = unsafe { rayrust_sys::ray_get_many(ids.as_ptr(), ids.len(), -1) };
    if result.is_null() {
        return Err(ffi_error("ray_get_many"));
    }

    let mut results = Vec::with_capacity(refs.len());
    for i in 0..refs.len() {
        let bytes = unsafe { &*result.add(i) };
        if bytes.data.is_null() {
            unsafe { rayrust_sys::ray_free_bytes_array(result, refs.len()) };
            return Err(RayError::ObjectNotFound(format!("object {} not found", i)));
        }
        let slice = unsafe { std::slice::from_raw_parts(bytes.data as *const u8, bytes.len) };
        let val = if refs[i].is_xlang {
            crate::serialize::deserialize_xlang(slice)?
        } else {
            crate::serialize::deserialize(slice)?
        };
        results.push(val);
    }
    unsafe { rayrust_sys::ray_free_bytes_array(result, refs.len()) };
    Ok(results)
}
