//! FFI bindings to the Ray C++ SDK via a C ABI wrapper.
//!
//! This crate provides raw `unsafe` bindings. The `rayrust` crate wraps
//! these in safe Rust APIs.
//!
//! IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
//! All ID parameters use `ray_bytes_t` (ptr + len), not null-terminated strings.

use std::ffi::CString;
use std::os::raw::{c_char, c_int};

// ─── C struct definitions ─────────────────────────────────────

#[repr(C)]
pub struct RayBytes {
    pub data: *const c_char,
    pub len: usize,
}

// ─── extern "C" function declarations ─────────────────────────

extern "C" {
    // Lifecycle
    pub fn ray_init(address: *const c_char, local_mode: c_int, node_ip: *const c_char) -> c_int;
    pub fn ray_is_initialized() -> bool;
    pub fn ray_shutdown();

    // Object Store
    pub fn ray_put(data: *const c_char, len: usize) -> RayBytes;
    pub fn ray_get(id_data: *const c_char, id_len: usize, timeout_ms: c_int) -> RayBytes;
    pub fn ray_wait(
        ids: *const RayBytes,
        count: usize,
        num_objects: c_int,
        timeout_ms: c_int,
    ) -> *mut bool;

    // Task
    pub fn ray_task_call(
        func_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
    ) -> RayBytes;

    // Actor
    pub fn ray_actor_create(
        func_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
    ) -> RayBytes;
    pub fn ray_actor_call(
        actor_id_data: *const c_char,
        actor_id_len: usize,
        func_name: *const c_char,
        args: *const RayBytes,
        arg_count: usize,
    ) -> RayBytes;
    pub fn ray_actor_kill(actor_id_data: *const c_char, actor_id_len: usize, no_restart: bool);

    // Placement Group
    pub fn ray_placement_group_create(
        name: *const c_char,
        bundles_json: *const c_char,
        strategy: c_int,
    ) -> RayBytes;
    pub fn ray_placement_group_remove(group_id_data: *const c_char, group_id_len: usize);

    // Misc
    pub fn ray_was_current_actor_restarted() -> bool;
    pub fn ray_get_namespace() -> RayBytes;

    // Memory Management
    pub fn ray_free_bytes(ptr: *mut RayBytes);
    pub fn ray_free_bools(ptr: *mut bool);
}

// ─── Safe wrapper helpers ─────────────────────────────────────

/// RAII guard for a C-allocated byte buffer (ray_bytes_t).
/// Frees the underlying memory on Drop.
pub struct CBytesGuard {
    inner: RayBytes,
}

impl CBytesGuard {
    pub fn from(inner: RayBytes) -> Option<Self> {
        if inner.data.is_null() {
            None
        } else {
            Some(Self { inner })
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.inner.data as *const u8, self.inner.len) }
    }

    pub fn into_vec(self) -> Vec<u8> {
        let v = self.as_slice().to_vec();
        // Don't run Drop's free — we moved the data.
        // Actually std::mem::forget is needed if we move out, but
        // since we already copied the data into a Vec, just let Drop free it.
        // Drop will free the original C buffer, which is fine.
        v
    }
}

impl Drop for CBytesGuard {
    fn drop(&mut self) {
        unsafe { ray_free_bytes(&mut self.inner) }
    }
}

/// Helper to convert a Rust string to CString, handling null.
pub fn to_cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("").unwrap())
}

/// Build a `Vec<RayBytes>` from a slice of byte slices.
/// The returned vector must be kept alive for the duration of the FFI call.
pub fn build_args_array(args: &[&[u8]]) -> Vec<RayBytes> {
    args.iter()
        .map(|b| RayBytes {
            data: b.as_ptr() as *const c_char,
            len: b.len(),
        })
        .collect()
}
