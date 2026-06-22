//! rayrust — Rust SDK for Ray distributed computing.
//!
//! This crate wraps the Ray C++ SDK via a C ABI layer, providing
//! idiomatic Rust APIs for Ray's core distributed primitives:
//! - Object store (put/get/wait) — sync and async
//! - Remote tasks — sync and async
//! - Cross-language calls (Python tasks + actors)
//! - Actors
//! - Placement groups

pub mod error;
pub mod object_ref;
pub mod runtime;
pub mod serialize;

/// Re-export of the raw FFI crate for macro-generated code.
pub use rayrust_sys as sys;

/// Re-export ctor for macro-generated code.
pub use ctor;

pub use error::RayError;
pub use object_ref::ObjectRef;
pub use runtime::{
    get_namespace, init, init_with_config, is_initialized, put, put_async, get, get_async, wait,
    was_current_actor_restarted, shutdown, ActorHandle, RayConfig,
};
pub use serialize::{deserialize, serialize};

/// Re-export the proc macro.
pub use rayrust_macros::remote;

// ─── Task / Actor convenience wrappers (crate root) ───────────

/// Call a remote task by function name.
pub fn task_call(func_name: &str, args: &[&[u8]]) -> Result<ObjectRef<()>, RayError> {
    crate::runtime::task_call_inner(func_name, args)
}

/// Asynchronously call a remote task by function name.
pub async fn task_call_async(func_name: &str, args: Vec<Vec<u8>>) -> Result<ObjectRef<()>, RayError> {
    crate::runtime::task_call_inner_async(func_name.to_string(), args).await
}

/// Call a Python remote function.
/// `module` is the Python module name, `function` is the function name.
pub fn task_call_python(module: &str, function: &str, args: &[&[u8]]) -> Result<ObjectRef<()>, RayError> {
    crate::runtime::task_call_python_inner(module, function, args)
}

/// Create an actor by factory function name.
pub fn actor_create(func_name: &str, args: &[&[u8]]) -> Result<ActorHandle, RayError> {
    crate::runtime::actor_create_inner(func_name, args)
}

/// Create a Python actor.
/// `module` is the Python module, `class` is the Python class name.
pub fn actor_create_python(module: &str, class: &str, args: &[&[u8]]) -> Result<ActorHandle, RayError> {
    crate::runtime::actor_create_python_inner(module, class, args)
}

/// Call a method on an actor.
pub fn actor_call(actor_id: &[u8], func_name: &str, args: &[&[u8]]) -> Result<ObjectRef<()>, RayError> {
    crate::runtime::actor_call_inner(actor_id, func_name, args)
}

/// Call a method on a Python actor.
/// `method_name` is the Python method name (without `self`).
pub fn actor_call_python(actor_id: &[u8], method_name: &str, args: &[&[u8]]) -> Result<ObjectRef<()>, RayError> {
    crate::runtime::actor_call_python_inner(actor_id, method_name, args)
}

/// Kill an actor.
pub fn actor_kill(actor_id: &[u8], no_restart: bool) {
    crate::runtime::actor_kill_inner(actor_id, no_restart);
}

/// Convenience module for common imports.
pub mod prelude {
    pub use crate::error::RayError;
    pub use crate::object_ref::ObjectRef;
    pub use crate::runtime::{
        ActorHandle, RayConfig, init, init_with_config, is_initialized,
        put, put_async, get, get_async, wait, shutdown,
    };
    pub use crate::serialize::{deserialize, serialize};
    pub use crate::{
        actor_call, actor_create, actor_kill, get_namespace,
        task_call, task_call_async, was_current_actor_restarted,
        task_call_python, actor_create_python, actor_call_python,
    };
    pub use rayrust_macros::remote;
}
