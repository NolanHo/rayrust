//! Ray runtime — init, shutdown, put, get, wait, task/actor operations.
//!
//! IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
//! We store them as `Vec<u8>` internally, not as Rust `String`.

use std::os::raw::c_int;

use rayrust_sys::{self, build_args_array, to_cstring, CBytesGuard, RayBytes};

use crate::error::RayError;
use crate::object_ref::ObjectRef;
use crate::serialize::serialize;

/// Result of `wait()`: (ready, unready) object references.
pub type WaitResult<T> = Result<(Vec<ObjectRef<T>>, Vec<ObjectRef<T>>), RayError>;

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

// ─── Config ───────────────────────────────────────────────────

/// Actor lifetime determines whether an actor outlives the job that created it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum ActorLifetime {
    /// The actor is destroyed when the job that created it ends (default).
    #[default]
    NonDetached = 0,
    /// The actor outlives the job that created it; must be killed manually.
    Detached = 1,
}

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
    /// Default actor lifetime for actors created in this job.
    /// `Detached` actors outlive the job that created them.
    pub default_actor_lifetime: ActorLifetime,
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

    /// Set the default actor lifetime.
    /// Pass `ActorLifetime::Detached` so actors outlive the job.
    pub fn default_actor_lifetime(mut self, lifetime: ActorLifetime) -> Self {
        self.default_actor_lifetime = lifetime;
        self
    }

    /// Set the job-level namespace for named actors.
    /// Named actors are registered in this namespace by default.
    /// Use `ActorOptions::ray_namespace()` to override per-actor.
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = ns.into();
        self
    }
}

// ─── Ray Context (RAII) ────────────────────────────────────────

/// Ray runtime context — owns the init→shutdown lifecycle.
///
/// Created via [`Ray::connect`] or [`Ray::local`]. When dropped, the
/// Ray runtime is automatically shut down. All Ray operations (put,
/// get, task_call, actor_create, …) are methods on this struct.
///
/// `Ray` is `!Clone` to prevent double-shutdown. Pass `&Ray` to
/// share it across call sites.
///
/// # Example
/// ```no_run
/// use rayrust::prelude::*;
///
/// let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379")).unwrap();
/// let obj = ray.put(&42i32);
/// let val: i32 = obj.get().unwrap();
/// // drop(ray) → automatic shutdown
/// ```
pub struct Ray {
    _priv: (),
}

impl Ray {
    /// Connect to a Ray cluster with the given config.
    ///
    /// Returns a `Ray` context that auto-shuts-down on drop.
    pub fn connect(config: &RayConfig) -> Result<Self, RayError> {
        init_with_config(config)?;
        Ok(Ray { _priv: () })
    }

    /// Connect in local mode (no cluster, single process).
    pub fn local() -> Result<Self, RayError> {
        Self::connect(&RayConfig::local())
    }

    // ── Object Store ──────────────────────────────────────────

    /// Put an object into the object store.
    pub fn put<T: serde::Serialize>(&self, value: &T) -> Result<ObjectRef<T>, RayError> {
        put(value)
    }

    /// Put a value with XLANG header wrapping (for Python pass-by-reference).
    pub fn put_xlang<T: serde::Serialize>(&self, value: &T) -> Result<ObjectRef<T>, RayError> {
        put_xlang(value)
    }

    /// Asynchronously put an object into the object store.
    pub async fn put_async<T>(&self, value: T) -> Result<ObjectRef<T>, RayError>
    where
        T: serde::Serialize + Send + 'static,
    {
        put_async(value).await
    }

    /// Get an object from the object store (blocks until ready).
    pub fn get<T: serde::de::DeserializeOwned>(&self, obj_ref: &ObjectRef<T>) -> Result<T, RayError> {
        obj_ref.get()
    }

    /// Asynchronously get an object from the object store.
    pub async fn get_async<T>(&self, obj_ref: &ObjectRef<T>) -> Result<T, RayError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        obj_ref.get_async().await
    }

    /// Wait for objects to be locally available.
    pub fn wait<T: Clone>(
        &self,
        object_refs: &[ObjectRef<T>],
        num_objects: usize,
        timeout_ms: i32,
    ) -> WaitResult<T> {
        wait(object_refs, num_objects, timeout_ms)
    }

    /// Get multiple objects from the object store (blocks until all ready).
    pub fn get_many<T: serde::de::DeserializeOwned>(
        &self,
        refs: &[ObjectRef<T>],
    ) -> Result<Vec<T>, RayError> {
        get_many(refs)
    }

    // ── Tasks ─────────────────────────────────────────────────

    /// Call a remote task by function name.
    /// `is_ref[i] = true` means args[i] is an ObjectRef ID (pass by reference).
    /// Pass `&TaskOptions::new()` for no resource requirements.
    pub fn task_call(
        &self,
        func_name: &str,
        args: &[&[u8]],
        is_ref: &[bool],
        opts: &TaskOptions,
    ) -> Result<ObjectRef<()>, RayError> {
        if opts.has_resources() {
            task_call_with_options_inner(func_name, args, is_ref, opts)
        } else {
            task_call_inner(func_name, args, is_ref)
        }
    }

    /// Asynchronously call a remote task.
    ///
    /// Returns a `'static` future — does not borrow `&self`.
    /// This allows the future to be spawned on `JoinSet` or similar.
    /// Pass `&TaskOptions::new()` for no resource requirements.
    pub fn task_call_async(
        &self,
        func_name: &str,
        args: Vec<Vec<u8>>,
        is_ref: Vec<bool>,
        opts: &TaskOptions,
    ) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
        let func_name = func_name.to_string();
        let has_resources = opts.has_resources();
        let resources_json = if has_resources {
            Some(opts.resources_json())
        } else {
            None
        };
        async move {
            if has_resources {
                // For async + resources, we use spawn_blocking on the sync FFI path
                // (the C ABI for resources is sync-only).
                tokio::task::spawn_blocking(move || {
                    let args_ref: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
                    let is_ref_ref: &[bool] = if is_ref.is_empty() { &[] } else { &is_ref };
                    let func_c = to_cstring(&func_name);
                    let args_arr = build_args_array(&args_ref);
                    let is_ref_ptr = if is_ref_ref.is_empty() {
                        std::ptr::null()
                    } else {
                        is_ref_ref.as_ptr()
                    };
                    let resources_json_c = to_cstring(&resources_json.unwrap_or_default());
                    let bytes = unsafe {
                        rayrust_sys::ray_task_call_with_resources(
                            func_c.as_ptr(),
                            args_arr.as_ptr(),
                            args_arr.len(),
                            is_ref_ptr,
                            resources_json_c.as_ptr(),
                        )
                    };
                    let guard = CBytesGuard::from(bytes)
                        .ok_or_else(|| ffi_error_detail("ray_task_call_with_resources", &func_name))?;
                    Ok(ObjectRef::from_id(guard.as_slice().to_vec()))
                })
                .await
                .map_err(|e| RayError::Runtime(format!("task_call join error: {}", e)))?
            } else {
                task_call_inner_async(func_name, args, is_ref).await
            }
        }
    }

    /// Call a Python remote function.
    pub fn task_call_python(
        &self,
        module: &str,
        function: &str,
        args: &[&[u8]],
        is_ref: &[bool],
    ) -> Result<ObjectRef<()>, RayError> {
        let id = task_call_python_inner(module, function, args, is_ref)?;
        Ok(ObjectRef::from_id_xlang(id))
    }

    // ── Actors ────────────────────────────────────────────────

    /// Create an actor with full creation options.
    ///
    /// Pass `&ActorOptions::new()` for defaults (no name, no restarts, etc.).
    pub fn actor_create(
        &self,
        func_name: &str,
        args: &[&[u8]],
        opts: &ActorOptions,
    ) -> Result<ActorHandle, RayError> {
        actor_create_with_options_inner(func_name, args, opts)
    }

    /// Create a Python actor with full creation options.
    pub fn actor_create_python(
        &self,
        module: &str,
        class: &str,
        args: &[&[u8]],
        opts: &ActorOptions,
    ) -> Result<ActorHandle, RayError> {
        actor_create_python_with_options_inner(module, class, args, opts)
    }

    /// Call a method on an actor (sync).
    pub fn actor_call(
        &self,
        actor_id: &[u8],
        func_name: &str,
        args: &[&[u8]],
    ) -> Result<ObjectRef<()>, RayError> {
        actor_call_inner(actor_id, func_name, args)
    }

    /// Asynchronously call a method on an actor.
    ///
    /// Returns a `'static` future — does not borrow `&self`.
    /// This allows the future to be spawned on `JoinSet` or similar.
    pub fn actor_call_async(
        &self,
        actor_id: &[u8],
        func_name: &str,
        args: Vec<Vec<u8>>,
    ) -> impl std::future::Future<Output = Result<ObjectRef<()>, RayError>> + Send + 'static {
        let actor_id = actor_id.to_vec();
        let func_name = func_name.to_string();
        async move {
            actor_call_inner_async(actor_id, func_name, args).await
        }
    }

    /// Call a method on a Python actor.
    pub fn actor_call_python(
        &self,
        actor_id: &[u8],
        method_name: &str,
        args: &[&[u8]],
        is_ref: &[bool],
    ) -> Result<ObjectRef<()>, RayError> {
        let id = actor_call_python_inner(actor_id, method_name, args, is_ref)?;
        Ok(ObjectRef::from_id_xlang(id))
    }

    /// Kill an actor by its handle.
    /// Returns `Ok(())` on success, `Err` if the C++ side reports an error.
    pub fn kill_actor(&self, handle: &ActorHandle, no_restart: bool) -> Result<(), RayError> {
        rayrust_sys::clear_error();
        actor_kill_inner(handle.id(), no_restart);
        // C ABI returns void, but sets last_error on exception.
        // Best-effort: check if an error was set.
        if let Some(err) = rayrust_sys::last_error() {
            if !err.is_empty() {
                return Err(RayError::Ffi(format!("ray_actor_kill: {}", err)));
            }
        }
        Ok(())
    }

    // ── Placement Groups ──────────────────────────────────────

    /// Create a placement group.
    /// `bundles_json` is a JSON array like `[{"CPU":1},{"CPU":1}]`.
    /// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
    pub fn placement_group_create(
        &self,
        name: &str,
        bundles_json: &str,
        strategy: i32,
    ) -> Result<Vec<u8>, RayError> {
        placement_group_create_inner(name, bundles_json, strategy)
    }

    /// Remove a placement group by binary ID.
    pub fn placement_group_remove(&self, group_id: &[u8]) {
        placement_group_remove_inner(group_id);
    }

    // ── Misc ──────────────────────────────────────────────────

    /// Get the namespace of this job.
    pub fn namespace(&self) -> Result<String, RayError> {
        get_namespace()
    }

    /// Get a named actor by name and namespace.
    pub fn get_actor(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<Option<ActorHandle>, RayError> {
        get_actor(name, namespace)
    }

    /// Cancel a remote task by object ID.
    pub fn cancel(&self, obj_id: &[u8], force_kill: bool, recursive: bool) -> Result<(), RayError> {
        cancel(obj_id, force_kill, recursive)
    }

    /// Returns true if the current actor was restarted.
    pub fn was_current_actor_restarted(&self) -> bool {
        was_current_actor_restarted()
    }

    /// Check if Ray is initialized.
    pub fn is_initialized(&self) -> bool {
        is_initialized()
    }
}

impl Drop for Ray {
    fn drop(&mut self) {
        if is_initialized() {
            shutdown();
        }
    }
}

// ─── Lifecycle ────────────────────────────────────────────────

/// Initialize Ray runtime with full config.
pub(crate) fn init_with_config(config: &RayConfig) -> Result<(), RayError> {
    let address_c = to_cstring(&config.address);
    let local_mode = if config.local_mode { 1 } else { 0 };
    let node_ip_c = to_cstring(&config.node_ip);
    let code_search_path_c = to_cstring(&config.code_search_path.join(":"));
    let runtime_env_c = to_cstring(&config.runtime_env);
    let log_dir_c = to_cstring(&config.log_dir);

    let namespace_c = to_cstring(&config.namespace);

    let ret = unsafe {
        rayrust_sys::ray_init(
            address_c.as_ptr(),
            local_mode,
            node_ip_c.as_ptr(),
            code_search_path_c.as_ptr(),
            runtime_env_c.as_ptr(),
            log_dir_c.as_ptr(),
            config.default_actor_lifetime as i32,
            namespace_c.as_ptr(),
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
pub(crate) fn is_initialized() -> bool {
    unsafe { rayrust_sys::ray_is_initialized() }
}

/// Shutdown Ray runtime.
pub(crate) fn shutdown() {
    unsafe { rayrust_sys::ray_shutdown() }
}

// ─── Object Store ─────────────────────────────────────────────

/// Put an object into the object store.
/// Returns an ObjectRef that can be retrieved with `get`.
pub(crate) fn put<T: serde::Serialize>(value: &T) -> Result<ObjectRef<T>, RayError> {
    let data = serialize(value)?;
    let id = put_raw(&data)?;
    Ok(ObjectRef::from_id(id))
}

/// Put a value into the object store with XLANG header wrapping.
///
/// Use this when the data will be consumed by Python via pass-by-reference
/// (task_call_python with is_ref=true). Python's xlang deserialization
/// expects the 9-byte XLANG header before the msgpack payload.
///
/// The returned ObjectRef ID can be passed to `task_call_python` as a
/// reference arg, avoiding re-serialization of large data per task call.
pub(crate) fn put_xlang<T: serde::Serialize>(value: &T) -> Result<ObjectRef<T>, RayError> {
    let data = crate::serialize::serialize_xlang(value)?;
    let id = put_raw(&data)?;
    Ok(ObjectRef::from_id_xlang(id))
}

/// Asynchronously put an object into the object store.
pub(crate) async fn put_async<T>(value: T) -> Result<ObjectRef<T>, RayError>
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

/// Wait for objects to be locally available.
///
/// # Arguments
/// * `object_refs` - References to wait for.
/// * `num_objects` - Minimum number of objects to wait for.
/// * `timeout_ms` - Timeout in milliseconds. -1 for infinite.
///
/// # Returns
/// A tuple of (ready, unready) object references.
pub(crate) fn wait<T: Clone>(
    object_refs: &[ObjectRef<T>],
    num_objects: usize,
    timeout_ms: i32,
) -> WaitResult<T> {
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

/// Call a remote task with resource requirements via TaskOptions.
pub(crate) fn task_call_with_options_inner(
    func_name: &str,
    args: &[&[u8]],
    is_ref: &[bool],
    opts: &TaskOptions,
) -> Result<ObjectRef<()>, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let is_ref_ptr = if is_ref.is_empty() {
        std::ptr::null()
    } else {
        is_ref.as_ptr()
    };
    let resources_json_c = if opts.has_resources() {
        Some(to_cstring(&opts.resources_json()))
    } else {
        None
    };
    let resources_json = resources_json_c.as_ref()
        .map(|c| c.as_ptr())
        .unwrap_or(std::ptr::null());

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
        .ok_or_else(|| ffi_error_detail("ray_task_call_python", format!("{}.{}", module, function)))?;
    Ok(guard.as_slice().to_vec())
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
}

// ─── Actor Creation Options (builder) ─────────────────────────

/// Escape a string for safe inclusion as a JSON string value.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Hex-encode raw bytes (for binary-safe transport inside JSON).
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Build a resources JSON string from (name, value) pairs.
fn resources_to_json(resources: &[(String, f64)]) -> String {
    if resources.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = resources.iter().map(|(n, v)| {
        let val = if *v == v.trunc() {
            format!("{}", *v as i64)
        } else {
            format!("{}", v)
        };
        format!("\"{}\":{}", json_escape(n), val)
    }).collect();
    format!("{{{}}}", parts.join(","))
}

// ─── Task Options (builder) ────────────────────────────────────

/// Builder for Ray `CallOptions`.
///
/// Mirrors the C++ SDK's `CallOptions` struct. Use the builder methods
/// to set resource requirements, then pass to `Ray::task_call`.
///
/// # Example
/// ```no_run
/// use rayrust::prelude::*;
/// # let ray = Ray::local().unwrap();
/// let opts = TaskOptions::new().resource("CPU", 2.0).resource("GPU", 1.0);
/// let obj = ray.task_call("my_task", &[], &[], &opts).unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct TaskOptions {
    /// Resource requirements, e.g. `[("CPU", 2.0), ("GPU", 0.5)]`.
    pub resources: Vec<(String, f64)>,
}

impl TaskOptions {
    /// Create a new builder with no options (all defaults).
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a resource requirement, e.g. `("CPU", 2.0)`.
    pub fn resource(mut self, name: impl Into<String>, value: f64) -> Self {
        self.resources.push((name.into(), value));
        self
    }

    /// Set resource requirements as a slice of `(name, value)` pairs.
    pub fn resources(mut self, resources: Vec<(String, f64)>) -> Self {
        self.resources = resources;
        self
    }

    /// Whether any resources are set.
    pub fn has_resources(&self) -> bool {
        !self.resources.is_empty()
    }

    /// Serialize resources to JSON for the C ABI.
    fn resources_json(&self) -> String {
        resources_to_json(&self.resources)
    }
}

// ─── Actor Creation Options (builder) ─────────────────────────
///
/// Exposes all fields of the C++ SDK's `ActorCreationOptions`:
/// - `name` — named actor (looked up later via [`get_actor`])
/// - `ray_namespace` — namespace for named actor lookup
/// - `resources` — CPU/GPU requirements
/// - `max_restarts` — restart on failure (default 0 = no restart)
/// - `max_concurrency` — max concurrent method calls (default 1)
/// - `serialized_runtime_env_info` — runtime environment info string
/// - `placement_group` — bind actor to a placement group bundle
///
/// # Example
/// ```no_run
/// use rayrust::ActorOptions;
/// let opts = ActorOptions::new()
///     .name("my_actor")
///     .max_restarts(3)
///     .max_concurrency(10)
///     .resource("CPU", 2.0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct ActorOptions {
    /// Named actor name. Empty = anonymous actor.
    pub name: String,
    /// Namespace for named actor lookup. Empty = current namespace.
    pub ray_namespace: String,
    /// Resource requirements, e.g. `[("CPU", 1.0), ("GPU", 0.5)]`.
    pub resources: Vec<(String, f64)>,
    /// Maximum number of restarts on failure. 0 = no restart (default).
    pub max_restarts: i32,
    /// Maximum number of concurrent method calls. 1 = serial (default).
    pub max_concurrency: i32,
    /// Serialized runtime environment info string.
    pub serialized_runtime_env_info: String,
    /// Hex-encoded placement group ID (binary). Empty = no placement group.
    pub placement_group_id: Vec<u8>,
    /// Bundle index within the placement group.
    pub bundle_index: i32,
}

impl ActorOptions {
    /// Create a new builder with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the actor name (named actor).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the ray namespace for named actor lookup.
    pub fn ray_namespace(mut self, ns: impl Into<String>) -> Self {
        self.ray_namespace = ns.into();
        self
    }

    /// Add a resource requirement, e.g. `("CPU", 2.0)`.
    pub fn resource(mut self, name: impl Into<String>, value: f64) -> Self {
        self.resources.push((name.into(), value));
        self
    }

    /// Set resource requirements as a slice of `(name, value)` pairs.
    pub fn resources(mut self, resources: Vec<(String, f64)>) -> Self {
        self.resources = resources;
        self
    }

    /// Set the maximum number of restarts on failure.
    pub fn max_restarts(mut self, n: i32) -> Self {
        self.max_restarts = n;
        self
    }

    /// Set the maximum number of concurrent method calls.
    pub fn max_concurrency(mut self, n: i32) -> Self {
        self.max_concurrency = n;
        self
    }

    /// Set the serialized runtime environment info string.
    pub fn runtime_env_info(mut self, info: impl Into<String>) -> Self {
        self.serialized_runtime_env_info = info.into();
        self
    }

    /// Bind the actor to a placement group bundle.
    /// `group_id` is the binary ID returned by `placement_group_create`.
    /// `bundle_index` is the index of the bundle within the group.
    pub fn placement_group(mut self, group_id: Vec<u8>, bundle_index: i32) -> Self {
        self.placement_group_id = group_id;
        self.bundle_index = bundle_index;
        self
    }

    /// Serialize to a JSON string for the C ABI `options_json` parameter.
    /// Returns empty string if no options are set (all defaults).
    fn to_json(&self) -> String {
        let mut parts: Vec<String> = Vec::new();

        if !self.name.is_empty() {
            parts.push(format!("\"name\":\"{}\"", json_escape(&self.name)));
        }
        if !self.ray_namespace.is_empty() {
            parts.push(format!("\"ray_namespace\":\"{}\"", json_escape(&self.ray_namespace)));
        }
        if !self.resources.is_empty() {
            parts.push(format!("\"resources\":{}", resources_to_json(&self.resources)));
        }
        if self.max_restarts != 0 {
            parts.push(format!("\"max_restarts\":{}", self.max_restarts));
        }
        // max_concurrency default is 1; only include if different
        if self.max_concurrency != 1 {
            parts.push(format!("\"max_concurrency\":{}", self.max_concurrency));
        }
        if !self.serialized_runtime_env_info.is_empty() {
            parts.push(format!(
                "\"serialized_runtime_env_info\":\"{}\"",
                json_escape(&self.serialized_runtime_env_info)
            ));
        }
        if !self.placement_group_id.is_empty() {
            parts.push(format!(
                "\"placement_group_id\":\"{}\",\"bundle_index\":{}",
                hex_encode(&self.placement_group_id),
                self.bundle_index
            ));
        }

        if parts.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", parts.join(","))
        }
    }
}

/// Create an actor with full creation options.
pub(crate) fn actor_create_with_options_inner(
    func_name: &str,
    args: &[&[u8]],
    options: &ActorOptions,
) -> Result<ActorHandle, RayError> {
    let func_c = to_cstring(func_name);
    let args_arr = build_args_array(args);
    let options_json = to_cstring(&options.to_json());

    let bytes = unsafe {
        rayrust_sys::ray_actor_create_with_options(
            func_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            if options_json.as_bytes().is_empty() {
                std::ptr::null()
            } else {
                options_json.as_ptr()
            },
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail("ray_actor_create_with_options", func_name))?;
    Ok(ActorHandle { id: guard.as_slice().to_vec() })
}

/// Create a Python actor with full creation options.
pub(crate) fn actor_create_python_with_options_inner(
    module: &str,
    class: &str,
    args: &[&[u8]],
    options: &ActorOptions,
) -> Result<ActorHandle, RayError> {
    let module_c = to_cstring(module);
    let class_c = to_cstring(class);
    let args_arr = build_args_array(args);
    let options_json = to_cstring(&options.to_json());

    let bytes = unsafe {
        rayrust_sys::ray_actor_create_python_with_options(
            module_c.as_ptr(),
            class_c.as_ptr(),
            args_arr.as_ptr(),
            args_arr.len(),
            if options_json.as_bytes().is_empty() {
                std::ptr::null()
            } else {
                options_json.as_ptr()
            },
        )
    };

    let guard = CBytesGuard::from(bytes)
        .ok_or_else(|| ffi_error_detail(
            "ray_actor_create_python_with_options",
            format!("{}.{}", module, class),
        ))?;
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
pub(crate) fn was_current_actor_restarted() -> bool {
    unsafe { rayrust_sys::ray_was_current_actor_restarted() }
}

/// Get the namespace of this job.
pub(crate) fn get_namespace() -> Result<String, RayError> {
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
pub(crate) fn get_actor(name: &str, namespace: &str) -> Result<Option<ActorHandle>, RayError> {
    let name_c = to_cstring(name);
    let ns_c = to_cstring(namespace);
    rayrust_sys::clear_error();
    let bytes = unsafe { rayrust_sys::ray_get_actor(name_c.as_ptr(), ns_c.as_ptr()) };
    let guard = CBytesGuard::from(bytes);
    match guard {
        Some(g) => {
            let id = g.as_slice().to_vec();
            if id.is_empty() {
                Ok(None)
            } else {
                Ok(Some(ActorHandle { id }))
            }
        }
        None => {
            // C returns null both for "not found" and "error".
            // Check last_error to distinguish.
            if let Some(err) = rayrust_sys::last_error() {
                if !err.is_empty() {
                    return Err(RayError::Ffi(format!("ray_get_actor: {}", err)));
                }
            }
            Ok(None)
        }
    }
}

/// Cancel a remote task by object ID.
/// `force_kill` kills the worker process if true.
/// `recursive` cancels dependent tasks.
pub(crate) fn cancel(obj_id: &[u8], force_kill: bool, recursive: bool) -> Result<(), RayError> {
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
pub(crate) fn get_many<T: serde::de::DeserializeOwned>(refs: &[ObjectRef<T>]) -> Result<Vec<T>, RayError> {
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
