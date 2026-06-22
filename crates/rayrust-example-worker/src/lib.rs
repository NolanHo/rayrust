//! Example Rust cdylib worker for Ray cluster mode.
//!
//! This crate compiles to `librayrust_worker.so`.
//! When the Ray C++ worker process loads this .so via `boost::dll`:
//!   1. `#[ctor]` constructors auto-register each `#[remote]` function
//!   2. `boost::dll` finds `TaskExecutionHandler` / `GetRemoteFunctions` /
//!      `InitRayRuntime` (re-exported from `libray_api.so` via linking)
//!   3. `GetRemoteFunctions()` returns the registered functions
//!   4. Worker can now execute Rust remote tasks!
//!
//! Build:
//! ```bash
//! cargo build --release -p rayrust-example-worker
//! # Output: target/release/librayrust_worker.so
//! ```

use rayrust::remote;

/// Simple addition task.
#[remote]
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Greeting task.
#[remote]
pub fn greet(name: String) -> String {
    format!("Hello, {} from Rust worker!", name)
}

/// Multiply task.
#[remote]
pub fn multiply(a: i64, b: i64) -> i64 {
    a * b
}
