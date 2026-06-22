//! rayrust — Rust SDK for Ray distributed computing.
//!
//! This crate wraps the Ray C++ SDK via a C ABI layer, providing
//! idiomatic Rust APIs for Ray's core distributed primitives:
//! - Object store (put/get/wait)
//! - Remote tasks
//! - Actors
//! - Placement groups
//!
//! # Quick Start
//! ```no_run
//! use rayrust::prelude::*;
//!
//! fn main() -> Result<(), RayError> {
//!     ray::init("192.168.42.141:6379")?;
//!
//!     let obj = ray::put(&42i32);
//!     let val: i32 = ray::get(&obj)?;
//!     assert_eq!(val, 42);
//!
//!     ray::shutdown();
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod object_ref;
pub mod runtime;
pub mod serialize;

/// Re-export of the raw FFI crate for macro-generated code.
pub use rayrust_sys as sys;

pub use error::RayError;
pub use object_ref::ObjectRef;
pub use runtime::{
    get_namespace, init, init_with_config, is_initialized, put, get, wait,
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

/// Create an actor by factory function name.
pub fn actor_create(func_name: &str, args: &[&[u8]]) -> Result<ActorHandle, RayError> {
    crate::runtime::actor_create_inner(func_name, args)
}

/// Call a method on an actor.
pub fn actor_call(actor_id: &[u8], func_name: &str, args: &[&[u8]]) -> Result<ObjectRef<()>, RayError> {
    crate::runtime::actor_call_inner(actor_id, func_name, args)
}

/// Kill an actor.
pub fn actor_kill(actor_id: &[u8], no_restart: bool) {
    crate::runtime::actor_kill_inner(actor_id, no_restart);
}

/// Convenience module for common imports.
pub mod prelude {
    pub use crate::error::RayError;
    pub use crate::object_ref::ObjectRef;
    pub use crate::runtime::{ActorHandle, RayConfig, init, init_with_config, is_initialized, put, get, wait, shutdown};
    pub use crate::serialize::{deserialize, serialize};
    pub use crate::{actor_call, actor_create, actor_kill, get_namespace, task_call, was_current_actor_restarted};
    pub use rayrust_macros::remote;
}
