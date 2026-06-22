/// C ABI wrapper implementation — bridges Ray C++ SDK to plain C functions.
///
/// This file wraps `ray::internal::GetRayRuntime()` methods directly,
/// bypassing the C++ template API. Serialization uses msgpack.
///
/// IMPORTANT: Ray object/actor IDs are binary strings that may contain
/// null bytes. All ID parameters use (ptr, len) pairs, and all ID
/// returns use ray_bytes_t (ptr + len).

#include "ray_c.h"

#include <ray/api.h>
#include <ray/api/ray_runtime.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/serializer.h>
#include <ray/api/function_manager.h>
#include <ray/api/internal_api.h>

// Declare the exported function from libray_api.so.
// This returns the SAME FunctionManager singleton that the worker uses,
// avoiding the "singleton split" problem where FunctionManager::Instance()
// in our translation unit creates a separate instance from the one in libray_api.so.
//
// IMPORTANT: Do NOT use extern "C" here. GetFunctionManager is a C++ function.
// With extern "C", the linker resolves the symbol to the BOOST_DLL_ALIAS
// variable (V type, in .data section), not the actual function (T type, in .text).
// Calling a data address as code → SIGSEGV.
namespace ray { namespace internal {
    FunctionManager &GetFunctionManager();
}}

#include <cstring>
#include <msgpack.hpp>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

// ─── Function Registration ────────────────────────────────────

// Global map of Rust function callbacks, indexed by function name.
// Uses a function-local static to guarantee initialization order:
// the map is constructed on first call, which is always before any
// #[ctor] registration (which calls ray_register_function).
static std::unordered_map<std::string, ray_func_callback_t> &get_rust_functions() {
    static std::unordered_map<std::string, ray_func_callback_t> map;
    return map;
}

static msgpack::sbuffer invoke_rust_function(const std::string &func_name,
                                              const ray::internal::ArgsBufferList &args_buffer) {
    auto &fns = get_rust_functions();
    auto it = fns.find(func_name);
    if (it == fns.end()) {
        throw std::runtime_error("Rust function not found: " + func_name);
    }

    // Build ray_bytes_t array from ArgsBufferList
    std::vector<ray_bytes_t> args_arr;
    args_arr.reserve(args_buffer.size());
    for (const auto &buf : args_buffer) {
        args_arr.push_back(ray_bytes_t{buf.data(), buf.size()});
    }

    // Call the Rust callback
    ray_bytes_t result = it->second(args_arr.data(), args_arr.size());

    // Copy result into msgpack::sbuffer and free the C-allocated data
    msgpack::sbuffer sbuf(result.len);
    sbuf.write(result.data, result.len);
    std::free(const_cast<char *>(result.data));

    return sbuf;
}

void ray_register_function(const char *func_name, ray_func_callback_t callback) {
    std::string name(func_name);
    auto &fns = get_rust_functions();
    fns[name] = callback;

    // Use the exported GetFunctionManager() from libray_api.so instead of
    // FunctionManager::Instance(). The inline Instance() in our translation unit
    // would create a separate singleton from the one in libray_api.so,
    // causing a "singleton split" where registrations are invisible to the worker.
    auto &fm = ray::internal::GetFunctionManager();
    auto [map_ref, _] = fm.GetRemoteFunctions();
    auto &map = const_cast<ray::internal::RemoteFunctionMap_t &>(map_ref);

    map.emplace(name, [name](const ray::internal::ArgsBufferList &args) -> msgpack::sbuffer {
        return invoke_rust_function(name, args);
    });
}

// ─── Helpers ──────────────────────────────────────────────────

/// Copy a std::string (may contain null bytes) into a heap-allocated ray_bytes_t.
static ray_bytes_t dup_bytes(const std::string &s) {
    char *data = static_cast<char *>(std::malloc(s.size()));
    if (!data) return ray_bytes_t{nullptr, 0};
    std::memcpy(data, s.data(), s.size());
    return ray_bytes_t{data, s.size()};
}

// ─── Lifecycle ────────────────────────────────────────────────

int ray_init(const char *address, int local_mode, const char *node_ip,
             const char *code_search_path) {
    ray::RayConfig config;
    if (address && address[0] != '\0') {
        config.address = address;
    }
    config.local_mode = local_mode != 0;

    // Set code search path for worker to find .so with remote functions.
    // The C++ SDK uses ':' as separator (like PATH).
    if (code_search_path && code_search_path[0] != '\0') {
        std::string paths(code_search_path);
        size_t start = 0, end;
        while ((end = paths.find(':', start)) != std::string::npos) {
            if (end > start) {
                config.code_search_path.push_back(paths.substr(start, end - start));
            }
            start = end + 1;
        }
        if (start < paths.size()) {
            config.code_search_path.push_back(paths.substr(start));
        }
    }

    // Build argv to pass node_ip_address via command-line flags.
    int argc = 1;
    std::vector<std::string> arg_strings;
    std::vector<const char *> arg_ptrs;

    arg_strings.push_back("rayrust");
    arg_ptrs.push_back(arg_strings[0].c_str());

    if (node_ip && node_ip[0] != '\0') {
        arg_strings.push_back("--ray_node_ip_address");
        arg_strings.push_back(node_ip);
        arg_ptrs.push_back(arg_strings[1].c_str());
        arg_ptrs.push_back(arg_strings[2].c_str());
        argc = 3;
    }

    try {
        ray::Init(config, argc, const_cast<char**>(arg_ptrs.data()));
        return 0;
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_init error: %s\n", e.what());
        return -1;
    }
}

bool ray_is_initialized(void) {
    return ray::IsInitialized();
}

void ray_shutdown(void) {
    ray::Shutdown();
}

// ─── Object Store ─────────────────────────────────────────────

ray_bytes_t ray_put(const char *data, size_t len) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        // The data from Rust is already msgpack-serialized (by rmp-serde).
        // Write it directly into the sbuffer without wrapping in pack_bin,
        // because the C++ SDK stores the buffer as-is and returns it as-is
        // on Get. Wrapping would add an extra Bin8 marker that breaks
        // deserialization on the Rust side.
        auto buffer = std::make_shared<msgpack::sbuffer>(len);
        buffer->write(data, len);

        std::string id = runtime->Put(buffer);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_put error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

ray_bytes_t ray_get(const char *id_data, size_t id_len, int timeout_ms) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string id(id_data, id_len);
        auto buffer = runtime->Get(id, timeout_ms);
        if (!buffer) return ray_bytes_t{nullptr, 0};

        char *data = static_cast<char *>(std::malloc(buffer->size()));
        if (!data) return ray_bytes_t{nullptr, 0};
        std::memcpy(data, buffer->data(), buffer->size());
        return ray_bytes_t{data, buffer->size()};
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_get error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

bool *ray_wait(const ray_bytes_t *ids, size_t count, int num_objects, int timeout_ms) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return nullptr;

        std::vector<std::string> id_vec;
        id_vec.reserve(count);
        for (size_t i = 0; i < count; i++) {
            id_vec.emplace_back(ids[i].data, ids[i].len);
        }

        auto results = runtime->Wait(id_vec, num_objects, timeout_ms);

        bool *out = static_cast<bool *>(std::malloc(count * sizeof(bool)));
        if (!out) return nullptr;
        for (size_t i = 0; i < count && i < results.size(); i++) {
            out[i] = results[i];
        }
        return out;
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_wait error: %s\n", e.what());
        return nullptr;
    }
}

// ─── Task ─────────────────────────────────────────────────────

/// Helper: build TaskArg vector for CPP tasks (raw msgpack, no wrapping).
static std::vector<ray::internal::TaskArg>
build_task_args_cpp(const ray_bytes_t *args, size_t arg_count) {
    std::vector<ray::internal::TaskArg> task_args;
    task_args.reserve(arg_count);
    for (size_t i = 0; i < arg_count; i++) {
        ray::internal::TaskArg arg;
        arg.buf = msgpack::sbuffer(args[i].len);
        arg.buf->write(args[i].data, args[i].len);
        task_args.push_back(std::move(arg));
    }
    return task_args;
}

/// Helper: build TaskArg vector for Python tasks (xlang wrapped).
/// Args from Rust are ALREADY msgpack-serialized by rmp-serde.
/// We must NOT call Serializer::Serialize on them again — that would
/// wrap the data in pack_bin/bin8, which Python's msgpack.unpackb
/// cannot decode (expects the original msgpack type markers).
///
/// XLANG format per C++ SDK's Arguments::WrapArgsImpl:
///   [dummy_kwargs (RAW metadata)]  +  [xlang_header + raw_data (XLANG metadata)]
/// where xlang_header = [msgpack_int(data_len)] [zero_padding to XLANG_HEADER_LEN]
static std::vector<ray::internal::TaskArg>
build_task_args_python(const ray_bytes_t *args, size_t arg_count) {
    std::vector<ray::internal::TaskArg> task_args;
    task_args.reserve(arg_count * 2);
    for (size_t i = 0; i < arg_count; i++) {
        // 1. Dummy kwargs (for Python)
        ray::internal::TaskArg dummy;
        dummy.buf = msgpack::sbuffer(ray::internal::METADATA_STR_DUMMY.size());
        dummy.buf->write(ray::internal::METADATA_STR_DUMMY.data(),
                         ray::internal::METADATA_STR_DUMMY.size());
        dummy.meta_str = ray::internal::METADATA_STR_RAW;
        task_args.push_back(std::move(dummy));

        // 2. Data with xlang header
        //    data_len encoded as msgpack int, then zero-padded to XLANG_HEADER_LEN
        //    Then raw msgpack bytes (already serialized by Rust's rmp-serde)
        auto len_buf = ray::internal::Serializer::Serialize(args[i].len);

        ray::internal::TaskArg xlang_arg;
        xlang_arg.buf = msgpack::sbuffer(ray::internal::XLANG_HEADER_LEN + args[i].len);
        xlang_arg.buf->write(len_buf.data(), len_buf.size());
        for (size_t j = len_buf.size(); j < ray::internal::XLANG_HEADER_LEN; ++j) {
            xlang_arg.buf->write("", 1);
        }
        // Write raw msgpack bytes directly — no Serializer::Serialize wrapping
        xlang_arg.buf->write(args[i].data, args[i].len);
        xlang_arg.meta_str = ray::internal::METADATA_STR_XLANG;
        task_args.push_back(std::move(xlang_arg));
    }
    return task_args;
}

ray_bytes_t ray_task_call(const char *func_name,
                           const ray_bytes_t *args,
                           size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder{std::move(fn)};

        auto task_args = build_task_args_cpp(args, arg_count);

        ray::internal::CallOptions options;
        std::string id = runtime->Call(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_task_call error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

/// Call a Python remote function.
/// `module_name` and `function_name` are null-terminated C strings.
/// `args` are msgpack-serialized arguments (will be wrapped with xlang header).
ray_bytes_t ray_task_call_python(const char *module_name,
                                  const char *function_name,
                                  const ray_bytes_t *args,
                                  size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), std::string(function_name),
            "", ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count);

        ray::internal::CallOptions options;
        std::string id = runtime->Call(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_task_call_python error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

// ─── Actor ────────────────────────────────────────────────────

ray_bytes_t ray_actor_create(const char *func_name,
                              const ray_bytes_t *args,
                              size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder("", std::string(func_name),
                                                   std::string(func_name),
                                                   ray::internal::LangType::CPP);

        auto task_args = build_task_args_cpp(args, arg_count);

        ray::internal::ActorCreationOptions options;
        std::string id = runtime->CreateActor(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_create error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

/// Create a Python actor.
/// `module_name` is the Python module, `class_name` is the Python class.
ray_bytes_t ray_actor_create_python(const char *module_name,
                                      const char *class_name,
                                      const ray_bytes_t *args,
                                      size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder(
            std::string(module_name), "__init__",
            std::string(class_name), ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count);

        ray::internal::ActorCreationOptions options;
        std::string id = runtime->CreateActor(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_create_python error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

ray_bytes_t ray_actor_call(const char *actor_id_data, size_t actor_id_len,
                            const char *func_name,
                            const ray_bytes_t *args,
                            size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder("", std::string(func_name),
                                                   std::string(func_name),
                                                   ray::internal::LangType::CPP);

        auto task_args = build_task_args_cpp(args, arg_count);

        std::string actor_id(actor_id_data, actor_id_len);
        ray::internal::CallOptions options;
        std::string id = runtime->CallActor(holder, actor_id, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_call error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

/// Call a method on a Python actor.
/// `method_name` is the Python method name (without `self`).
ray_bytes_t ray_actor_call_python(const char *actor_id_data, size_t actor_id_len,
                                    const char *method_name,
                                    const ray_bytes_t *args,
                                    size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::internal::RemoteFunctionHolder holder(
            "", std::string(method_name), "",
            ray::internal::LangType::PYTHON);

        auto task_args = build_task_args_python(args, arg_count);

        std::string actor_id(actor_id_data, actor_id_len);
        ray::internal::CallOptions options;
        std::string id = runtime->CallActor(holder, actor_id, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_call_python error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

void ray_actor_kill(const char *actor_id_data, size_t actor_id_len, bool no_restart) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) {
            std::string actor_id(actor_id_data, actor_id_len);
            runtime->KillActor(actor_id, no_restart);
        }
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_kill error: %s\n", e.what());
    }
}

// ─── Placement Group ──────────────────────────────────────────

// nlohmann/json is shipped with the Ray C++ SDK headers.
#include <nlohmann/json.hpp>

ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::PlacementGroupCreationOptions options;
        if (name) options.name = name;
        options.strategy = static_cast<ray::PlacementStrategy>(strategy);

        // Parse bundles from JSON: [{"CPU": 1}, {"GPU": 1}]
        if (bundles_json && bundles_json[0] != '\0') {
            using json = nlohmann::json;
            auto j = json::parse(std::string(bundles_json));
            for (auto &bundle : j) {
                std::unordered_map<std::string, double> resources;
                for (auto it = bundle.begin(); it != bundle.end(); ++it) {
                    resources[it.key()] = it.value().get<double>();
                }
                options.bundles.push_back(resources);
            }
        }

        auto group = runtime->CreatePlacementGroup(options);
        return dup_bytes(group.GetID());
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_placement_group_create error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

void ray_placement_group_remove(const char *group_id_data, size_t group_id_len) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) {
            std::string group_id(group_id_data, group_id_len);
            runtime->RemovePlacementGroup(group_id);
        }
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_placement_group_remove error: %s\n", e.what());
    }
}

// ─── Misc ─────────────────────────────────────────────────────

bool ray_was_current_actor_restarted(void) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) return runtime->WasCurrentActorRestarted();
    } catch (...) {}
    return false;
}

ray_bytes_t ray_get_namespace(void) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (runtime) return dup_bytes(runtime->GetNamespace());
    } catch (...) {}
    return ray_bytes_t{nullptr, 0};
}

ray_bytes_t ray_get_actor(const char *name, const char *ray_namespace) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string ns_str = ray_namespace ? std::string(ray_namespace) : "";
        std::string actor_id = runtime->GetActorId(std::string(name), ns_str);
        if (actor_id.empty()) return ray_bytes_t{nullptr, 0};
        return dup_bytes(actor_id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_get_actor error: %s\n", e.what());
        return ray_bytes_t{nullptr, 0};
    }
}

int ray_cancel(const char *id_data, size_t id_len, bool force_kill, bool recursive) {
    // CoreWorker::CancelTask is not directly exposed via RayRuntime.
    // The C++ SDK doesn't have a Cancel method on RayRuntime.
    // This requires calling CoreWorker directly — skip for now.
    (void)id_data; (void)id_len; (void)force_kill; (void)recursive;
    fprintf(stderr, "ray_cancel: not yet implemented (requires CoreWorker::CancelTask)\n");
    return -1;
}

ray_bytes_t *ray_get_many(const ray_bytes_t *ids, size_t count, int timeout_ms) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return nullptr;

        std::vector<std::string> id_vec;
        id_vec.reserve(count);
        for (size_t i = 0; i < count; i++) {
            id_vec.emplace_back(ids[i].data, ids[i].len);
        }

        auto results = runtime->Get(id_vec, timeout_ms);

        auto *out = static_cast<ray_bytes_t *>(std::malloc(count * sizeof(ray_bytes_t)));
        if (!out) return nullptr;

        for (size_t i = 0; i < count && i < results.size(); i++) {
            if (results[i]) {
                char *data = static_cast<char *>(std::malloc(results[i]->size()));
                if (data) {
                    std::memcpy(data, results[i]->data(), results[i]->size());
                    out[i] = ray_bytes_t{data, results[i]->size()};
                } else {
                    out[i] = ray_bytes_t{nullptr, 0};
                }
            } else {
                out[i] = ray_bytes_t{nullptr, 0};
            }
        }
        return out;
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_get_many error: %s\n", e.what());
        return nullptr;
    }
}

void ray_free_bytes_array(ray_bytes_t *array, size_t count) {
    if (!array) return;
    for (size_t i = 0; i < count; i++) {
        if (array[i].data) std::free(const_cast<char *>(array[i].data));
    }
    std::free(array);
}

// ─── Memory Management ────────────────────────────────────────

void ray_free_bytes(ray_bytes_t *ptr) {
    if (ptr && ptr->data) {
        std::free(const_cast<char *>(ptr->data));
        ptr->data = nullptr;
        ptr->len = 0;
    }
}

void ray_free_bools(bool *ptr) {
    if (ptr) std::free(ptr);
}

// ─── Async Get (CoreWorker::GetAsync + eventfd) ──────────────
//
// This bypasses the C++ SDK's blocking Get() and calls
// CoreWorker::GetAsync() directly, which is non-blocking.
// When the object arrives, CoreWorker's io_context thread calls
// our callback, which writes to an eventfd. The Rust side polls
// the eventfd via tokio::io::AsyncFd — zero threads blocked.
//
// We do NOT include ray/core_worker/core_worker.h — it pulls in
// protobuf/gRPC/absl headers not available in the pip package.
// Instead we forward-declare the minimal types needed and the
// linker resolves the symbols from libray_api.so.
//
// Hybrid approach: GetAsync notifies us when the object is ready,
// then the Rust side does a fast blocking Get() (instant, since the
// object is already in the local store). No thread is blocked
// waiting for the object to arrive from a remote node.

#include <sys/eventfd.h>
#include <unistd.h>
#include <thread>

// ── Forward declarations matching Ray Core ABI ──
//
// These types must be in the correct namespaces so that C++ name
// mangling produces the same symbol names as libray_api.so.
// ObjectID is a simple POD type (28 bytes, #pragma pack(1), no vtable).
// RayObject is opaque — we only store the shared_ptr pointer.
// CoreWorker::GetAsync takes a std::function callback.

namespace ray {
    class RayObject;  // opaque — we never call methods on it

    // ObjectID: matches ray::ObjectID ABI.
    // The real type inherits BaseID<ObjectID> which is #pragma pack(1)
    // with a uint8_t[28] member. No vtable, no virtual methods.
    // We declare it with 28 bytes of storage so sizeof matches.
    // Name mangling only cares about namespace::classname, not inheritance.
    class ObjectID {
        char _data[28];
    };
}

namespace ray::core {
    // Match CoreWorker::SetResultCallback type.
    using SetResultCallback =
        std::function<void(std::shared_ptr<ray::RayObject>, ray::ObjectID, void*)>;

    class CoreWorker {
    public:
        void GetAsync(const ray::ObjectID&, SetResultCallback, void*);
    };

    class CoreWorkerProcess {
    public:
        static CoreWorker& GetCoreWorker();
    };
}

struct ray_async_get {
    int efd;
    bool ready;
    bool error;
};

// Callback invoked by CoreWorker's io_context thread when object arrives.
// NOTE: This callback is registered via CoreWorker::GetAsync but in practice
// the io_context in driver mode may not fire it. The polling thread in
// ray_async_get_start is the actual notification mechanism.
static void ray_async_callback(std::shared_ptr<ray::RayObject> obj,
                                ray::ObjectID /*id*/,
                                void *user_data) {
    auto *handle = static_cast<ray_async_get *>(user_data);
    if (!obj) {
        handle->error = true;
    }
    handle->ready = true;

    uint64_t val = 1;
    if (write(handle->efd, &val, sizeof(val)) < 0) {
        // eventfd write failed — nothing we can do
    }
}

ray_async_get_t *ray_async_get_start(const char *id_data, size_t id_len) {
    try {
        int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0) return nullptr;

        auto *handle = new ray_async_get{efd, false, false};

        // Copy the object ID for the polling thread
        std::string id_str(id_data, id_len);

        // Start a lightweight polling thread that uses Get(timeout=100ms)
        // to check if the object is available. This is NOT the same as
        // spawn_blocking(Get(-1)) — each poll blocks at most 100ms,
        // and the thread releases between polls.
        //
        // The thread signals via eventfd when the object is ready.
        // The Rust side uses AsyncFd to poll the eventfd — zero tokio
        // threads blocked.
        std::thread([handle, id_str]() {
            auto runtime = ray::internal::GetRayRuntime();
            if (!runtime) {
                handle->error = true;
                uint64_t val = 1;
                write(handle->efd, &val, sizeof(val));
                return;
            }

            while (!handle->ready && !handle->error) {
                try {
                    // timeout=100ms: returns immediately if object is local,
                    // blocks at most 100ms if not.
                    auto buf = runtime->Get(id_str, 100);
                    if (buf) {
                        handle->ready = true;
                        break;
                    }
                } catch (...) {
                    // Not ready yet — retry
                }
            }

            uint64_t val = 1;
            write(handle->efd, &val, sizeof(val));
        }).detach();

        return handle;
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_async_get_start error: %s\n", e.what());
        return nullptr;
    }
}

int ray_async_get_fd(const ray_async_get_t *handle) {
    return handle ? handle->efd : -1;
}

int ray_async_get_is_ready(const ray_async_get_t *handle) {
    if (!handle) return -1;
    return handle->ready ? 1 : (handle->error ? -1 : 0);
}

ray_bytes_t ray_async_get_result(const ray_async_get_t *handle) {
    // Not used — Rust side calls get_raw() after eventfd fires.
    (void)handle;
    return ray_bytes_t{nullptr, 0};
}

void ray_async_get_destroy(ray_async_get_t *handle) {
    if (handle) {
        if (handle->efd >= 0) close(handle->efd);
        delete handle;
    }
}
