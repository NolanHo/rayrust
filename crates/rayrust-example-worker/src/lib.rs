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

// ─── Rust Actor: Counter ──────────────────────────────────────

struct Counter {
    value: i64,
}

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

/// Factory: creates a Counter, returns the raw pointer as uint64_t.
#[no_mangle]
pub extern "C" fn __rayrust_actor_factory_counter(
    args: *const RayBytes,
    arg_count: usize,
) -> RayBytes {
    let start: i64 = if arg_count > 0 {
        read_arg(args, 0).unwrap_or(0)
    } else {
        0
    };

    let counter = Box::new(Counter { value: start });
    let ptr_val = Box::into_raw(counter) as u64;

    let result = serialize(&ptr_val).expect("failed to serialize actor ptr");
    to_ray_bytes(&result)
}

/// Register the factory as a normal remote function.
#[ctor::ctor]
fn __register_counter_factory() {
    let name = "__rayrust_actor_factory_counter";
    let name_c = to_cstring(name);
    unsafe {
        rayrust::sys::ray_register_function(name_c.as_ptr(), __rayrust_actor_factory_counter);
    }
}

/// Member function: Counter::increment(n)
#[no_mangle]
pub extern "C" fn __rayrust_member_counter_increment(
    actor_ptr: u64,
    args: *const RayBytes,
    arg_count: usize,
) -> RayBytes {
    let counter = unsafe { &mut *(actor_ptr as *mut Counter) };

    let n: i64 = if arg_count > 0 {
        read_arg(args, 0).unwrap_or(1)
    } else {
        1
    };

    counter.value += n;
    let result = serialize(&counter.value).expect("failed to serialize result");
    to_ray_bytes(&result)
}

/// Member function: Counter::get()
#[no_mangle]
pub extern "C" fn __rayrust_member_counter_get(
    actor_ptr: u64,
    _args: *const RayBytes,
    _arg_count: usize,
) -> RayBytes {
    let counter = unsafe { &*(actor_ptr as *const Counter) };
    let result = serialize(&counter.value).expect("failed to serialize result");
    to_ray_bytes(&result)
}

/// Register member functions.
#[ctor::ctor]
fn __register_counter_members() {
    let reg = |name: &str, cb: extern "C" fn(u64, *const RayBytes, usize) -> RayBytes| {
        let name_c = to_cstring(name);
        unsafe {
            rayrust::sys::ray_register_member_function(name_c.as_ptr(), cb);
        }
    };
    reg("__rayrust_actor_factory_counter::increment", __rayrust_member_counter_increment);
    reg("__rayrust_actor_factory_counter::get", __rayrust_member_counter_get);
}
