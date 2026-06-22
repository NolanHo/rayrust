# rayrust

Rust SDK for [Ray](https://ray.io) distributed computing — wraps the Ray C++ SDK via FFI.

## Status

🚧 **PoC (Proof of Concept)** — core Put/Get + remote task (local mode) verified against a live Ray cluster.

| Feature | Local Mode | Cluster Mode |
|---|---|---|
| `ray::init` | ✅ | ✅ |
| `ray::put` / `ray::get` | ✅ | ✅ |
| `ray::wait` | ✅ (API ready) | ✅ (API ready) |
| `ray::get_namespace` | ✅ | ✅ |
| `#[ray::remote]` task | ✅ `add(1,2)=3`, `greet("Ray")="Hello, Ray!"` | ✅ `add(10,32)=42`, `multiply(7,6)=42` |
| Actor | ⚠️ FFI ready, untested | ⚠️ FFI ready, untested |
| Placement Group | ⚠️ FFI ready, untested | ⚠️ FFI ready, untested |

### Remote task in cluster mode

Cluster mode is **fully working**. The approach:

1. Compile Rust remote functions into a `cdylib` (`.so`) using `rayrust-example-worker` as a template
2. `#[rayrust::remote]` generates a `#[ctor]` that auto-registers the function when the `.so` is loaded
3. The driver passes the `.so` path via `code_search_path` in `RayConfig`
4. The Ray worker process `dlopen`s the `.so`, `#[ctor]` fires, functions are registered in `FunctionManager`
5. Worker calls `GetRemoteFunctions()` → finds the Rust functions → executes them

Key implementation details:
- **`#[ctor]` auto-registration**: Functions are registered at `.so` load time, before the worker calls `GetRemoteFunctions()`
- **`GetFunctionManager()` not `FunctionManager::Instance()`**: The inline `Instance()` creates a separate singleton per translation unit. Using the exported `GetFunctionManager()` from `libray_api.so` ensures the worker sees our registrations.
- **`--no-as-needed` linker flag**: Forces `libray_api.so` into the `.so`'s NEEDED list so `boost::dll` can find `TaskExecutionHandler` etc. transitively
- **Meyers singleton for `g_rust_functions`**: Function-local static avoids the "static initialization order fiasco" — `#[ctor]` may fire before global constructors

## Architecture

```
┌─────────────────────────────────────────────┐
│           Rust 用户代码                       │
│  #[rayrust::remote]                          │
│  fn add(a: i32, b: i32) -> i32 { a + b }     │
│  rayrust::put(&42i32)                        │
├─────────────────────────────────────────────┤
│        rayrust (safe Rust API)               │
│  - ObjectRef<T> / ActorHandle               │
│  - serialize / deserialize (rmp-serde)        │
│  - #[remote] proc macro                      │
├─────────────────────────────────────────────┤
│        rayrust-sys (FFI bindings)             │
│  - extern "C" declarations                   │
│  - build.rs (cc + link libray_api.so)        │
├─────────────────────────────────────────────┤
│     ray_c.h / ray_c.cc (C ABI wrapper)       │
│  - Type-erased C interface                   │
│  - Wraps ray::internal::GetRayRuntime()      │
├─────────────────────────────────────────────┤
│        libray_api.so (Ray C++ SDK)           │
├─────────────────────────────────────────────┤
│        Ray Core (raylet / GCS / object store) │
└─────────────────────────────────────────────┘
```

### Why wrap C++ instead of native rewrite?

- **libray_api.so** is prebuilt and ships with `pip install ray[cpp]` — no compilation needed
- Ray Core protocol (GCS, raylet, object store) is complex; reimplementation is months of work
- C++ SDK already handles cluster connection, worker registration, serialization
- We bypass the template API and call `RayRuntime` directly for a thin, stable FFI layer

### Key design decisions

- **C ABI wrapper** (`ray_c.h/cc`): C++ templates (`Put<T>`, `Task<F>`) have no stable ABI. A thin C interface type-erases them.
- **Binary-safe IDs**: Ray `ObjectID::Binary()` may contain null bytes. All ID parameters use `(ptr, len)` pairs, not null-terminated strings.
- **`_GLIBCXX_USE_CXX11_ABI=0`**: `libray_api.so` is built with Bazel which sets the old C++ ABI. The wrapper must match to avoid `std::string` memory layout mismatch.
- **`--ray_node_ip_address`**: Required when the node has multiple NICs. Auto-detection picks the wrong interface.
- **Function registration**: `FunctionManager::GetRemoteFunctions()` returns const refs to internal maps. We `const_cast` to insert Rust callbacks — a pragmatic approach that avoids `#define private public` (which breaks standard library headers).
- **Serialization**: Rust uses `rmp-serde` (msgpack via serde). C++ SDK uses `msgpack::pack`. Both produce raw msgpack — compatible without extra wrapping.

## Quick Start

### Prerequisites

```bash
# Install Ray with C++ SDK
pip install "ray[cpp]"

# Find the ray[cpp] path
python3 -c "import ray,os,sys; [print(os.path.join(p,'ray','cpp')) for p in sys.path if os.path.exists(os.path.join(p,'ray','cpp','lib','libray_api.so'))]"
```

### Build

```bash
export RAY_CPP_DIR=/path/to/site-packages/ray/cpp
cargo build --example hello_ray
```

### Run against a Ray cluster

```bash
export RAY_CPP_DIR=/path/to/site-packages/ray/cpp
export RAY_ADDRESS=192.168.42.141:6379
export RAY_NODE_IP=192.168.42.106    # IP of this node as seen by the cluster
export LD_LIBRARY_PATH=$RAY_CPP_DIR/lib:$LD_LIBRARY_PATH

cargo run --example hello_ray
```

### Example output

```
Connecting to Ray cluster at 192.168.42.141:6379 (node_ip=192.168.42.106) ...
✓ Ray initialized

--- Put / Get ---
Put 42i32 → ObjectRef(id_len=28)
Get → 42 ✓

--- Put / Get String ---
Get String → hello ray from rust! ✓

--- Namespace ---
Namespace: ea25ec74d033068b85a4edea355bfb8fd388eb52706bb5d8a788b303

--- Shutdown ---
✓ Ray shutdown
```

## API

### Sync

```rust
use rayrust::prelude::*;

let config = RayConfig::new("192.168.42.141:6379").node_ip("192.168.42.106");
rayrust::init_with_config(&config)?;

let obj = rayrust::put(&42i32);
let val: i32 = rayrust::get(&obj)?;
rayrust::shutdown();
```

### Async (tokio)

```rust
use rayrust::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RayError> {
    rayrust::init("192.168.42.141:6379")?;

    // Async put / get
    let obj = rayrust::put_async(42i32).await?;
    let val: i32 = obj.get_async().await?;

    // Concurrent tasks with tokio::join!
    #[rayrust::remote]
    fn add(a: i32, b: i32) -> i32 { a + b }

    let (r1, r2) = tokio::join!(
        add_remote_async(1, 2),
        add_remote_async(3, 4),
    );
    let (v1, v2) = tokio::join!(
        r1?.get_async(),
        r2?.get_async(),
    );

    rayrust::shutdown();
    Ok(())
}
```

Async methods wrap blocking C++ FFI calls in `tokio::task::spawn_blocking`,
so the tokio runtime stays responsive while waiting for Ray operations.

## Workspace structure

```
rayrust/
├── Cargo.toml                 # Workspace root
├── crates/
│   ├── rayrust-sys/            # FFI bindings + C ABI wrapper
│   │   ├── Cargo.toml
│   │   ├── build.rs            # Compiles ray_c.cc, links libray_api.so
│   │   ├── wrapper/
│   │   │   ├── ray_c.h         # C ABI header
│   │   │   └── ray_c.cc         # C ABI implementation (wraps C++ SDK)
│   │   └── src/
│   │       └── lib.rs          # extern "C" declarations + safe guards
│   ├── rayrust-macros/         # Proc macros
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── lib.rs          # #[remote] attribute macro
│   └── rayrust/                # Safe Rust API
│       ├── Cargo.toml
│       ├── examples/
│       │   └── hello_ray.rs    # Cluster smoke test
│       └── src/
│           ├── lib.rs          # Re-exports + convenience functions
│           ├── error.rs        # RayError type
│           ├── object_ref.rs   # ObjectRef<T>
│           ├── runtime.rs      # init/put/get/wait/task/actor
│           └── serialize.rs    # msgpack bridge (rmp-serde)
```

## Known limitations

1. **Remote tasks in cluster mode**: ✅ **Solved!** Rust functions compiled into a `cdylib` `.so` are auto-registered via `#[ctor]` when the Ray worker loads the `.so`. See `rayrust-example-worker` crate and `cluster_remote_task` example.
2. **C++ SDK feature ceiling**: This SDK wraps the C++ SDK, so it inherits its limitations. No Ray Serve, Ray Train, Ray Tune, or RLlib.
3. **Synchronous + Asynchronous API**: Both sync (`get()`) and async (`get_async()`) APIs are provided. Async wraps blocking C++ FFI calls in `tokio::task::spawn_blocking`.
4. **Cross-language calls**: FFI layer supports calling Python/Java tasks, but not yet exposed in the safe API.

## License

Apache-2.0 (same as Ray)
