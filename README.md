# rayrust

Rust SDK for [Ray](https://ray.io) distributed computing — wraps the Ray C++ SDK via FFI.

## Status

🚧 **PoC (Proof of Concept)** — core Put/Get verified against a live Ray cluster.

| Feature | Status |
|---|---|
| `ray::init` (cluster mode) | ✅ |
| `ray::put` / `ray::get` | ✅ |
| `ray::wait` | ✅ (API ready, untested) |
| `ray::get_namespace` | ✅ |
| `#[ray::remote]` task | ⚠️ Macro ready, needs C++ worker with `RAY_REMOTE` registration |
| Actor | ⚠️ FFI layer ready, untested |
| Placement Group | ⚠️ FFI layer ready, untested |

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

```rust
use rayrust::prelude::*;

// Init
let config = RayConfig::new("192.168.42.141:6379")
    .node_ip("192.168.42.106");
rayrust::init_with_config(&config)?;

// Put / Get
let obj = rayrust::put(&42i32);
let val: i32 = rayrust::get(&obj)?;
assert_eq!(val, 42);

// String
let obj = rayrust::put(&"hello".to_string());
let val: String = rayrust::get(&obj)?;

// Remote task (requires C++ worker with RAY_REMOTE registration)
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

let arg1 = rayrust::serialize(&1i32)?;
let arg2 = rayrust::serialize(&2i32)?;
let obj_ref = rayrust::task_call("add", &[&arg1, &arg2])?;
let result: i32 = obj_ref.cast().get()?;

rayrust::shutdown();
```

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

1. **Remote tasks**: The `#[ray::remote]` macro generates the driver-side caller, but the worker process needs functions registered via C++ `RAY_REMOTE` macro in a shared library. A Rust-native registration mechanism is planned.
2. **C++ SDK feature ceiling**: This SDK wraps the C++ SDK, so it inherits its limitations. No Ray Serve, Ray Train, Ray Tune, or RLlib.
3. **Synchronous API**: `get()` blocks. Async wrappers (tokio) are planned.
4. **Cross-language calls**: FFI layer supports calling Python/Java tasks, but not yet exposed in the safe API.

## License

Apache-2.0 (same as Ray)
