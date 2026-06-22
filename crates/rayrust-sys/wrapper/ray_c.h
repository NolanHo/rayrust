/// C ABI wrapper header — bridges Ray C++ SDK templates to plain C functions.
///
/// The Ray C++ SDK uses heavy C++ templates (Put<T>, Task<F>, etc.) that have no
/// stable ABI. This header exposes a minimal C interface that the Rust FFI layer
/// can call. The C++ source (ray_c.cc) wraps `ray::internal::GetRayRuntime()` directly,
/// bypassing the template API.
///
/// IMPORTANT: Ray object IDs are binary strings that may contain null bytes.
/// All functions that return or accept IDs use `ray_bytes_t` (ptr + len)
/// instead of null-terminated `char*` to be binary-safe.

#pragma once

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Byte buffer: data pointer + length. Binary-safe.
typedef struct {
    const char *data;
    size_t len;
} ray_bytes_t;

// ─── Lifecycle ────────────────────────────────────────────────

/// Initialize Ray runtime.
/// `address` is "ip:port" of the head node (e.g. "192.168.42.141:6379").
/// Pass NULL or empty string for local mode.
/// `node_ip` is the IP address of this node as seen by the cluster.
///   Pass NULL to auto-detect.
/// `code_search_path` is a colon-separated list of directories or .so files
///   for the worker to search for remote functions. Pass NULL to skip.
/// Returns 0 on success, -1 on failure.
int ray_init(const char *address, int local_mode, const char *node_ip,
             const char *code_search_path);

/// Returns true if ray::Init has been called.
bool ray_is_initialized(void);

/// Shutdown Ray runtime.
void ray_shutdown(void);

// ─── Object Store ─────────────────────────────────────────────

/// Put a serialized object into the object store.
/// `data` is msgpack-serialized bytes.
/// Returns a ray_bytes_t containing the binary object ID.
/// Caller must free with ray_free_bytes.
ray_bytes_t ray_put(const char *data, size_t len);

/// Get a single object from the object store.
/// `id_data`/`id_len` is the binary object ID.
/// `timeout_ms` is -1 for infinite wait.
/// Returns a ray_bytes_t containing the serialized object data.
/// On error, returns ray_bytes_t with data=NULL.
ray_bytes_t ray_get(const char *id_data, size_t id_len, int timeout_ms);

/// Wait for objects to be locally available.
/// `ids` is an array of ray_bytes_t (binary object IDs).
/// `count` is the number of IDs.
/// `num_objects` is the minimum number to wait for.
/// `timeout_ms` is -1 for infinite wait.
/// Returns a heap-allocated bool array (one per input ID).
/// Caller must free with ray_free_bools.
bool *ray_wait(const ray_bytes_t *ids, size_t count, int num_objects, int timeout_ms);

// ─── Task ─────────────────────────────────────────────────────

/// Call a remote task by function name.
/// `func_name` is a null-terminated C string (function names are ASCII).
/// `args` is an array of msgpack-serialized arguments.
/// Returns a ray_bytes_t containing the binary object ID of the result.
/// On error, returns ray_bytes_t with data=NULL.
ray_bytes_t ray_task_call(const char *func_name,
                           const ray_bytes_t *args,
                           size_t arg_count);

/// Call a Python remote function.
/// `module_name` and `function_name` are null-terminated C strings.
/// `args` are msgpack-serialized arguments (will be wrapped with xlang header).
ray_bytes_t ray_task_call_python(const char *module_name,
                                  const char *function_name,
                                  const ray_bytes_t *args,
                                  size_t arg_count);

// ─── Actor ────────────────────────────────────────────────────

/// Create an actor by calling a factory function.
/// Returns a ray_bytes_t containing the binary actor ID.
ray_bytes_t ray_actor_create(const char *func_name,
                              const ray_bytes_t *args,
                              size_t arg_count);

/// Create a Python actor.
/// `module_name` is the Python module, `class_name` is the Python class.
ray_bytes_t ray_actor_create_python(const char *module_name,
                                      const char *class_name,
                                      const ray_bytes_t *args,
                                      size_t arg_count);

/// Call a method on an actor.
/// `actor_id_data`/`actor_id_len` is the binary actor ID.
/// Returns a ray_bytes_t containing the binary object ID of the result.
ray_bytes_t ray_actor_call(const char *actor_id_data, size_t actor_id_len,
                            const char *func_name,
                            const ray_bytes_t *args,
                            size_t arg_count);

/// Call a method on a Python actor.
/// `method_name` is the Python method name (without `self`).
ray_bytes_t ray_actor_call_python(const char *actor_id_data, size_t actor_id_len,
                                    const char *method_name,
                                    const ray_bytes_t *args,
                                    size_t arg_count);

/// Kill an actor.
/// `actor_id_data`/`actor_id_len` is the binary actor ID.
void ray_actor_kill(const char *actor_id_data, size_t actor_id_len, bool no_restart);

// ─── Placement Group ──────────────────────────────────────────

/// Create a placement group.
/// `name` is a null-terminated string (can be NULL).
/// `bundles_json` is a JSON array of bundle objects.
/// `strategy` is 0=PACK, 1=SPREAD, 2=STRICT_PACK, 3=STRICT_SPREAD.
/// Returns a ray_bytes_t containing the group ID.
ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy);

/// Remove a placement group by ID.
/// `group_id_data`/`group_id_len` is the binary group ID.
void ray_placement_group_remove(const char *group_id_data, size_t group_id_len);

// ─── Misc ─────────────────────────────────────────────────────

/// Returns true if the current actor was restarted.
bool ray_was_current_actor_restarted(void);

/// Get the namespace of this job.
/// Returns a ray_bytes_t containing the namespace string.
ray_bytes_t ray_get_namespace(void);

// ─── Function Registration ────────────────────────────────────

/// Callback type for a Rust remote function.
/// Receives an array of msgpack-serialized argument buffers.
/// Returns a heap-allocated ray_bytes_t containing the msgpack-serialized result.
/// The caller (C wrapper) will free the returned data with free().
typedef ray_bytes_t (*ray_func_callback_t)(const ray_bytes_t *args, size_t arg_count);

/// Register a Rust function as a Ray remote task.
/// Must be called before ray_init (or at least before the first task call).
/// `func_name` is the name used with ray_task_call.
/// `callback` is the C callback that will be invoked when the task executes.
void ray_register_function(const char *func_name, ray_func_callback_t callback);

// ─── Async Get (CoreWorker::GetAsync + eventfd) ──────────────

/// Opaque handle for an async get request.
/// Internally holds an eventfd and a pending result buffer.
typedef struct ray_async_get ray_async_get_t;

/// Start an async get for an object.
/// Returns a heap-allocated ray_async_get_t, or NULL on error.
/// The caller must free it with ray_async_get_destroy.
ray_async_get_t *ray_async_get_start(const char *id_data, size_t id_len);

/// Get the eventfd file descriptor for polling.
/// Returns -1 on error.
int ray_async_get_fd(const ray_async_get_t *handle);

/// Check if the result is ready (non-blocking).
/// Returns 1 if ready, 0 if not, -1 on error.
int ray_async_get_is_ready(const ray_async_get_t *handle);

/// Get the result data (only valid if is_ready returns 1).
/// Returns a ray_bytes_t. On error, data=NULL.
ray_bytes_t ray_async_get_result(const ray_async_get_t *handle);

/// Destroy the handle and free resources.
void ray_async_get_destroy(ray_async_get_t *handle);

// ─── Memory Management ────────────────────────────────────────

/// Free a ray_bytes_t returned by any ray_* function.
void ray_free_bytes(ray_bytes_t *ptr);

/// Free a bool array returned by ray_wait.
void ray_free_bools(bool *ptr);

#ifdef __cplusplus
}
#endif
