//! Example Rust cdylib worker for Ray cluster mode.
//!
//! This crate compiles to `librayrust_worker.so`.
//! When the Ray C++ worker process loads this .so via `boost::dll`:
//!   1. `#[ctor]` constructors auto-register each `#[remote]` function
//!   2. `boost::dll` finds `TaskExecutionHandler` / `GetRemoteFunctions` /
//!      `InitRayRuntime` (re-exported from `libray_api.so` via linking)
//!   3. `GetRemoteFunctions()` returns the registered functions
//!   4. Worker can now execute Rust remote tasks and actors!

use rayrust::remote;
use rayrust::serialize;
use rayrust::sys::{to_cstring, RayBytes};
use std::alloc::{alloc, Layout};
use std::ptr;

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

/// CPU-intensive: sum of 0..n.
#[remote]
pub fn compute(n: i64) -> i64 {
    (0..n).sum()
}

/// Async task: simulates I/O with sleep, then returns sum.
#[remote]
pub async fn async_sum(a: i64, b: i64) -> i64 {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    a + b
}

// ─── Rust Actor: Counter (using #[rayrust::actor] macro) ───────

/// A simple counter actor.
struct Counter {
    value: i64,
}

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn get(&self) -> i64 {
        self.value
    }

    fn reset(&mut self) {
        self.value = 0;
    }
}

// ─── Legacy helpers (for backward compat with manual actor code) ─────────

/// Helper: serialize result to heap-allocated RayBytes
fn to_ray_bytes(data: &[u8]) -> RayBytes {
    let layout = Layout::array::<u8>(data.len()).unwrap();
    let buf = unsafe { alloc(layout) };
    if !buf.is_null() {
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), buf, data.len()) };
    }
    RayBytes {
        data: buf as *const std::os::raw::c_char,
        len: data.len(),
    }
}

/// Helper: read arg by index
fn read_arg<T: serde::de::DeserializeOwned>(args: *const RayBytes, idx: usize) -> Option<T> {
    let raw = unsafe { std::slice::from_raw_parts((*args.add(idx)).data as *const u8, (*args.add(idx)).len) };
    rayrust::deserialize(raw).ok()
}
