//! Persistent tokio runtime for `#[remote]` async functions.
//!
//! The `#[remote]` proc macro generates C-compatible callbacks that are invoked
//! by Ray's C++ worker threads. These threads have no tokio context, so we need
//! a runtime to `block_on` async futures. Instead of creating a new runtime per
//! call (expensive — allocates threads, I/O driver, etc.), we use a single
//! global multi-threaded runtime created once via `OnceLock`.
//!
//! ## Why this is safe
//!
//! - `block_on_async` is ONLY called from C++ worker callbacks (no tokio context).
//! - Async remote fns use `.await` (get_async) or FFI (get, task_call) internally
//!   — neither re-enters `block_on_async`.
//! - The global `Runtime` is `Send + Sync`, so multiple C++ worker threads
//!   calling `block_on_async` concurrently is safe.

use std::sync::OnceLock;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Get the global tokio runtime, creating it on first access.
fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create global tokio runtime for rayrust async remote fn")
    })
}

/// Block on a future using the persistent global runtime.
///
/// Called by the `#[remote]` proc macro for `async fn` callbacks.
/// The future runs on the global multi-threaded runtime, so `spawn_blocking`
/// and other tokio features work correctly inside remote tasks.
pub fn block_on_async<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    runtime().block_on(future)
}
