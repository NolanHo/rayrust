# rayrust

Rust SDK for [Ray](https://ray.io) distributed computing — wraps the Ray C++ SDK via FFI.

## Status

✅ **Core features complete** — verified against a live Ray cluster.

| Feature | Local Mode | Cluster Mode |
|---|---|---|
| `ray::init` / `shutdown` | ✅ | ✅ |
| `ray::put` / `ray::get` | ✅ | ✅ |
| `ray::wait` | ✅ | ✅ |
| `ray::get_namespace` | ✅ | ✅ |
| `ray::get_many` (batch get) | ✅ | ✅ |
| `#[ray::remote]` sync task | ✅ | ✅ cdylib + `#[ctor]` auto-registration |
| `#[ray::remote]` async task | ✅ | ✅ tokio runtime in callback |
| `get_async` (non-blocking) | ✅ | ✅ polling thread + eventfd + AsyncFd |
| Rust Actor (factory + methods) | ✅ `Counter.increment(5)=105` | ✅ FFI ready |
| Python task (xlang) | ✅ | ✅ auto xlang deserialization |
| Python actor (xlang) | ✅ | ✅ create + call + kill |
| `ray::get_actor` (named actor) | ✅ | ✅ |
| `ray::cancel` | ✅ | ✅ `CoreWorker::CancelTask` |
| `ray::kill` | ✅ | ✅ |
| PlacementGroup | ✅ | ✅ create + remove + bundles JSON |
| ObjectRef as task param | ✅ `is_ref` array | ✅ `TaskArgByReference` |
| `runtime_env` | ✅ JSON string | ✅ |
| `log_dir` | ✅ | ✅ |
| `id_hex()` debug helper | ✅ | ✅ |
| `is_initialized()` | ✅ | ✅ |

### Async architecture

`get_async()` uses a **polling thread + eventfd + AsyncFd** pattern:
- C++ polling thread: `Get(timeout=100ms)` loop, signals via eventfd
- Rust side: `tokio::io::AsyncFd` polls eventfd — **zero tokio threads blocked**
- After eventfd fires: fast `Get()` (instant, object is local) + deserialize

For cross-language (Python) results, the data has a 9-byte XLANG header
that is automatically stripped before deserialization via the `is_xlang` flag
on `ObjectRef<T>`.

### `#[rayrust::remote]` macro

Supports both sync and async functions:
```rust
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[rayrust::remote]
async fn async_sum(a: i64, b: i64) -> i64 {
    tokio::time::sleep(Duration::from_millis(50)).await;
    a + b
}
```

Generates:
- C-compatible callback (deserialize args → call function → serialize result)
- `{name}_register()` — register with Ray's `FunctionManager`
- `{name}_remote()` — sync caller
- `{name}_remote_async()` — async caller (tokio)
- `#[ctor]` auto-registration at `.so` load time

### Rust Actor pattern

Actors use a factory + member function registration:

```rust
struct Counter { value: i64 }

// Factory: creates Box<Counter>, returns raw pointer as u64
#[no_mangle]
extern "C" fn factory(args: *const RayBytes, n: usize) -> RayBytes { ... }

// Member function: receives actor pointer + args
#[no_mangle]
extern "C" fn increment(ptr: u64, args: *const RayBytes, n: usize) -> RayBytes { ... }

// Register via #[ctor]
rayrust::ray_register_function("factory", factory);
rayrust::ray_register_member_function("factory::increment", increment);
```

### Remote task in cluster mode

1. Compile Rust remote functions into a `cdylib` (`.so`) via `rayrust-example-worker`
2. `#[rayrust::remote]` generates a `#[ctor]` that auto-registers at `.so` load time
3. Driver passes `.so` path via `code_search_path` in `RayConfig`
4. Ray worker `dlopen`s the `.so`, `#[ctor]` fires, `GetRemoteFunctions()` returns registered functions

Key implementation details:
- **`#[ctor]` auto-registration**: Functions registered at `.so` load time
- **`GetFunctionManager()`**: Uses exported function from `libray_api.so` to avoid singleton split
- **Meyers singleton for `g_rust_functions`**: Avoids static init order fiasco with `#[ctor]`
- **`--no-as-needed`**: Forces `libray_api.so` into cdylib's NEEDED list
- **`_GLIBCXX_USE_CXX11_ABI=0`**: Matches Bazel-built `libray_api.so`
- **Binary-safe IDs**: Ray ObjectIDs may contain null bytes — all use `(ptr, len)` pairs
- **Forward declarations**: `CoreWorker::CancelTask`, `CoreWorker::GetAsync` etc. declared without including `core_worker.h` (avoids protobuf/gRPC/absl deps)

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
│  - #[remote] proc macro (sync + async fn)    │
│  - get_async (eventfd + AsyncFd)             │
├─────────────────────────────────────────────┤
│        rayrust-sys (FFI bindings)             │
│  - extern "C" declarations                   │
│  - build.rs (cc + link libray_api.so)        │
├─────────────────────────────────────────────┤
│     ray_c.h / ray_c.cc (C ABI wrapper)       │
│  - Type-erased C interface                   │
│  - Wraps ray::internal::GetRayRuntime()      │
│  - xlang arg wrapping (build_task_args_python)│
│  - Member function registration               │
│  - CoreWorker forward declarations            │
├─────────────────────────────────────────────┤
│        libray_api.so (Ray C++ SDK)           │
├─────────────────────────────────────────────┤
│        Ray Core (raylet / GCS / object store) │
└─────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

```bash
pip install "ray[cpp]"
export RAY_CPP_DIR=$(python3 -c "import ray,os,sys; [print(os.path.join(p,'ray','cpp')) for p in sys.path if os.path.exists(os.path.join(p,'ray','cpp','lib','libray_api.so'))]")
```

### Build

```bash
# Build the worker .so (for cluster mode remote tasks)
cargo build --release -p rayrust-example-worker

# Build examples
cargo build --example full_test
```

### Run against a Ray cluster

```bash
export RAY_ADDRESS=192.168.42.141:6379
export RAY_NODE_IP=192.168.42.106
export RAY_WORKER_SO=$(pwd)/target/release/librayrust_worker.so
export LD_LIBRARY_PATH=$RAY_CPP_DIR/lib:$LD_LIBRARY_PATH

cargo run --example full_test
```

### Run in local mode (no cluster)

```bash
cargo run --example test_local
```

### Example output (local mode)

```
✓ Ray initialized (local mode)
add(3,4)=7 ✓
async_sum(10,20)=30 ✓ (async fn)
Rust Counter actor created ✓
Counter.increment(5)=105 ✓
Counter.get()=105 ✓
killed ✓
```

### Example output (cluster mode)

```
✓ Ray initialized
wait: 2 ready, 0 unready ✓
add(3, 4) = 7 ✓
get_many: [100, 200, 300] ✓
Python add(5, 3) = 8 ✓ (auto xlang deserialization)
Python Counter actor created ✓
Counter.increment(5) = 15 ✓ (auto xlang)
PlacementGroup created ✓
PlacementGroup removed ✓
ObjectRef id_hex: 00ffffffffffff... ✓
✓ Ray shutdown
```

## API

### Sync

```rust
use rayrust::prelude::*;

let config = RayConfig::new("192.168.42.141:6379")
    .node_ip("192.168.42.106")
    .code_search_path(vec!["/path/to/librayrust_worker.so".to_string()])
    .runtime_env(r#"{"pip": ["numpy"]}"#)
    .log_dir("/tmp/ray");
rayrust::init_with_config(&config)?;

let obj = rayrust::put(&42i32);
let val: i32 = rayrust::get(&obj)?;
rayrust::shutdown();
```

### Async (tokio)

```rust
#[tokio::main]
async fn main() -> Result<(), RayError> {
    rayrust::init("192.168.42.141:6379")?;

    // Async put / get — zero threads blocked
    let obj = rayrust::put_async(42i32).await?;
    let val: i32 = obj.get_async().await?;

    // Concurrent tasks with tokio::join!
    #[rayrust::remote]
    fn add(a: i32, b: i32) -> i32 { a + b }

    let (r1, r2) = tokio::join!(
        add_remote_async(1, 2),
        add_remote_async(3, 4),
    );
    let (v1, v2) = tokio::join!(r1?.get_async(), r2?.get_async());

    rayrust::shutdown();
    Ok(())
}
```

### Cross-language (Python)

```rust
// Call Python function
let arg = rayrust::serialize(&5i64)?;
let ref = rayrust::task_call_python("my_module", "add", &[&arg])?;
let val: i64 = ref.cast().get_async().await?;  // auto xlang deserialization

// Create Python actor
let actor = rayrust::actor_create_python("my_module", "Counter", &[&arg])?;
let ref = rayrust::actor_call_python(actor.id(), "increment", &[&arg])?;
let val: i64 = ref.cast().get_async().await?;
```

## Workspace structure

```
rayrust/
├── crates/
│   ├── rayrust-sys/            # FFI + C ABI wrapper
│   │   ├── wrapper/ray_c.h     # C ABI header
│   │   ├── wrapper/ray_c.cc    # C ABI implementation
│   │   ├── build.rs            # cc + link libray_api.so
│   │   └── src/lib.rs          # extern "C" + RAII guards
│   ├── rayrust-macros/         # #[remote] proc macro (sync + async fn)
│   ├── rayrust/                # Safe Rust API
│   │   ├── examples/
│   │   │   ├── full_test.rs    # Comprehensive cluster test
│   │   │   ├── async_demo.rs   # Async concurrent tasks demo
│   │   │   ├── cluster_remote_task.rs
│   │   │   └── test_local.rs   # Local mode test (sync + async + actor)
│   │   └── src/
│   │       ├── lib.rs          # Re-exports + convenience functions
│   │       ├── error.rs        # RayError
│   │       ├── object_ref.rs   # ObjectRef<T> (sync + async + xlang)
│   │       ├── runtime.rs      # init/put/get/wait/task/actor/cancel/etc
│   │       └── serialize.rs   # msgpack bridge + xlang header stripping
│   └── rayrust-example-worker/ # cdylib worker template
│       └── src/lib.rs          # add/greet/multiply + Counter actor
├── tests/python/rayrust_test.py # Python helper for xlang tests
└── Cargo.toml
```

## Known limitations

1. **Cluster node stability**: Actor creation may fail if cluster nodes lose heartbeats. This is a Ray cluster infrastructure issue, not a code issue.
2. **Python result deserialization**: Simple types (int, string) work via xlang header stripping. Complex types (lists, dicts with pickle) need additional work.
3. **`#[remote]` async fn in cluster mode**: The callback creates a new tokio runtime per call. For high-throughput scenarios, a persistent runtime would be better.

## License

Apache-2.0 (same as Ray)
