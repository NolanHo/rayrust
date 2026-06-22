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

#include <cstring>
#include <msgpack.hpp>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

// ─── Function Registration ────────────────────────────────────

// Global map of Rust function callbacks, indexed by function name.
// The C++ lambda in FunctionManager captures the function name and
// looks up the callback here.
static std::unordered_map<std::string, ray_func_callback_t> g_rust_functions;

static msgpack::sbuffer invoke_rust_function(const std::string &func_name,
                                              const ray::internal::ArgsBufferList &args_buffer) {
    auto it = g_rust_functions.find(func_name);
    if (it == g_rust_functions.end()) {
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
    g_rust_functions[name] = callback;

    // Register with FunctionManager. GetRemoteFunctions() returns const refs
    // to the internal maps, but the underlying maps are non-const members.
    // We const_cast to get mutable access and insert our callback.
    auto &fm = ray::internal::FunctionManager::Instance();
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

int ray_init(const char *address, int local_mode, const char *node_ip) {
    ray::RayConfig config;
    if (address && address[0] != '\0') {
        config.address = address;
    }
    config.local_mode = local_mode != 0;

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

ray_bytes_t ray_task_call(const char *func_name,
                           const ray_bytes_t *args,
                           size_t arg_count) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        std::string fn(func_name);
        ray::internal::RemoteFunctionHolder holder{std::move(fn)};

        std::vector<ray::internal::TaskArg> task_args;
        task_args.reserve(arg_count);
        for (size_t i = 0; i < arg_count; i++) {
            ray::internal::TaskArg arg;
            arg.buf = msgpack::sbuffer(args[i].len);
            arg.buf->write(args[i].data, args[i].len);
            task_args.push_back(std::move(arg));
        }

        ray::internal::CallOptions options;
        std::string id = runtime->Call(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_task_call error: %s\n", e.what());
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

        std::vector<ray::internal::TaskArg> task_args;
        task_args.reserve(arg_count);
        for (size_t i = 0; i < arg_count; i++) {
            ray::internal::TaskArg arg;
            arg.buf = msgpack::sbuffer(args[i].len);
            arg.buf->write(args[i].data, args[i].len);
            task_args.push_back(std::move(arg));
        }

        ray::internal::ActorCreationOptions options;
        std::string id = runtime->CreateActor(holder, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_create error: %s\n", e.what());
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

        std::vector<ray::internal::TaskArg> task_args;
        task_args.reserve(arg_count);
        for (size_t i = 0; i < arg_count; i++) {
            ray::internal::TaskArg arg;
            arg.buf = msgpack::sbuffer(args[i].len);
            arg.buf->write(args[i].data, args[i].len);
            task_args.push_back(std::move(arg));
        }

        std::string actor_id(actor_id_data, actor_id_len);
        ray::internal::CallOptions options;
        std::string id = runtime->CallActor(holder, actor_id, task_args, options);
        return dup_bytes(id);
    } catch (const std::exception &e) {
        fprintf(stderr, "ray_actor_call error: %s\n", e.what());
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

ray_bytes_t ray_placement_group_create(const char *name,
                                        const char *bundles_json,
                                        int strategy) {
    try {
        auto runtime = ray::internal::GetRayRuntime();
        if (!runtime) return ray_bytes_t{nullptr, 0};

        ray::PlacementGroupCreationOptions options;
        if (name) options.name = name;
        options.strategy = static_cast<ray::PlacementStrategy>(strategy);

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
