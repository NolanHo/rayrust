//! rayrust — Rust SDK for Ray distributed computing.
//!
//! This crate wraps the Ray C++ SDK via a C ABI layer, providing
//! idiomatic Rust APIs for Ray's core distributed primitives:
//! - Object store (put/get/wait) — sync and async
//! - Remote tasks — sync and async
//! - Cross-language calls (Python tasks + actors)
//! - Actors
//! - Placement groups
//!
//! ## Quick start
//! ```no_run
//! use rayrust::prelude::*;
//!
//! let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;
//! let obj = ray.put(&42i32)?;
//! let val: i32 = obj.get()?;
//! // drop(ray) → automatic shutdown
//! # Ok::<(), rayrust::RayError>(())
//! ```

pub mod async_runtime;
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
pub use runtime::{ActorHandle, ActorLifetime, ActorOptions, Ray, RayConfig, TaskOptions};

/// Re-export block_on_async for the `#[remote]` proc macro.
pub use async_runtime::block_on_async;

/// Re-export rmpv so users can use `rmpv::Value` for dynamic deserialization
/// without adding rmpv as their own dependency.
pub use rmpv;

/// Re-export the proc macros.
pub use rayrust_macros::{actor, remote};

// ── Serialization (pub — used by macro callbacks in worker process) ──

pub use serialize::{deserialize, deserialize_value, deserialize_xlang, deserialize_xlang_value, serialize, serialize_xlang};

/// Register a Rust actor member function with Ray's FunctionManager.
/// This is used by the `#[rayrust::actor]` macro for member function registration.
/// Must be called before `Ray::connect` or before the first actor call.
pub fn register_member_function(
    func_name: &str,
    callback: extern "C" fn(u64, *const rayrust_sys::RayBytes, usize) -> rayrust_sys::RayBytes,
) {
    let name_c = rayrust_sys::to_cstring(func_name);
    unsafe {
        rayrust_sys::ray_register_member_function(name_c.as_ptr(), callback);
    }
}

/// Convenience module for common imports.
pub mod prelude {
    pub use crate::error::RayError;
    pub use crate::object_ref::ObjectRef;
    pub use crate::runtime::{ActorHandle, ActorLifetime, ActorOptions, Ray, RayConfig, TaskOptions};
    pub use crate::serialize::{
        deserialize, deserialize_value, deserialize_xlang, deserialize_xlang_value,
        serialize, serialize_xlang,
    };
    pub use crate::{register_member_function, rmpv};
    pub use rayrust_macros::{actor, remote};
}
