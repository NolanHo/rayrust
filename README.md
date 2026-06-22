# rayrust

A Rust SDK for [Ray](https://ray.io) — the distributed computing framework for scaling AI and Python applications.

rayrust wraps the Ray C++ SDK (`libray_api.so`) via a C ABI layer, providing idiomatic Rust APIs for Ray's core distributed primitives: object store, remote tasks, actors, placement groups, and cross-language calls.

## Features

| Feature | Local Mode | Cluster Mode |
|---|:---:|:---:|
| `init` / `shutdown` | ✅ | ✅ |
| `put` / `get` / `wait` | ✅ | ✅ |
| `get_many` (batch get) | ✅ | ✅ |
| `get_namespace` | ✅ | ✅ |
| `#[remote]` sync task | ✅ | ✅ |
| `#[remote]` async task | ✅ | ✅ |
| `get_async` (non-blocking) | ✅ | ✅ |
| Rust Actor (factory + methods) | ✅ | ✅ |
| Python task (cross-language) | ✅ | ✅ |
| Python actor (cross-language) | ✅ | ✅ |
| `get_actor` (named actor) | ✅ | ✅ |
| `cancel` | ✅ | ✅ |
| `kill` | ✅ | ✅ |
| PlacementGroup | ✅ | ✅ |
| ObjectRef as task argument | ✅ | ✅ |
| `runtime_env` | ✅ | ✅ |
| `log_dir` | ✅ | ✅ |

## Quick Start

### Prerequisites

```bash
# Install Ray with C++ SDK
pip install "ray[cpp]"

# Find the ray[cpp] directory
export RAY_CPP_DIR=$(python3 -c "
import ray, os, sys
for p in sys.path:
    c = os.path.join(p, 'ray', 'cpp')
    if os.path.exists(os.path.join(c, 'lib', 'libray_api.so')):
        print(c); break
")
```

### Build

```bash
git clone https://github.com/NolanHo/rayrust.git
cd rayrust

# Build the worker .so (for cluster mode)
cargo build --release -p rayrust-example-worker

# Build examples
cargo build --example full_test
```

### Run in local mode (no cluster needed)

```bash
cargo run --example test_local
```

Output:
```
Ray initialized (local mode)
add(3,4)=7
async_sum(10,20)=30 (async fn)
Rust Counter actor created
Counter.increment(5)=105
Counter.get()=105
killed
done
```

### Run against a Ray cluster

```bash
# Start a Ray cluster (or use an existing one)
ray start --head --port=6379

# On the worker node, start a Ray node
ray start --address='<head-node-ip>:6379'

# Run the driver
export RAY_ADDRESS='<head-node-ip>:6379'
export RAY_NODE_IP='<this-node-ip>'
export RAY_WORKER_SO="$(pwd)/target/release/librayrust_worker.so"
export LD_LIBRARY_PATH="$RAY_CPP_DIR/lib:$LD_LIBRARY_PATH"

cargo run --example full_test
```

Output:
```
Ray initialized
wait: 2 ready, 0 unready
add(3, 4) = 7
get_many: [100, 200, 300]
Python add(5, 3) = 8 (auto xlang deserialization)
Python Counter actor created
Counter.increment(5) = 15 (auto xlang)
PlacementGroup created
PlacementGroup removed
Ray shutdown
```

## Usage

### Sync API

```rust
use rayrust::prelude::*;

fn main() -> Result<(), RayError> {
    rayrust::init("127.0.0.1:6379")?;

    let obj = rayrust::put(&42i32);
    let val: i32 = rayrust::get(&obj)?;

    rayrust::shutdown();
    Ok(())
}
```

### Async API (tokio)

`get_async()` uses a polling thread + eventfd + `tokio::io::AsyncFd` pattern — **zero tokio threads blocked** while waiting for results.

```rust
use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[tokio::main]
async fn main() -> Result<(), RayError> {
    rayrust::init("127.0.0.1:6379")?;

    // Concurrent task submission
    let (r1, r2) = tokio::join!(
        add_remote_async(1, 2),
        add_remote_async(3, 4),
    );

    // Concurrent result gathering — no threads blocked
    let (v1, v2) = tokio::join!(
        r1?.get_async(),
        r2?.get_async(),
    );

    println!("{} {}", v1?, v2?); // 3 7

    rayrust::shutdown();
    Ok(())
}
```

### Remote tasks (`#[remote]`)

Supports both sync and `async fn`:

```rust
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[rayrust::remote]
async fn fetch_data(url: String) -> Vec<u8> {
    // async fn uses a tokio runtime in the worker callback
    http::get(&url).await
}

// Submit
let obj_ref = add_remote(1, 2);
let result: i32 = obj_ref.get()?;

// Or async
let obj_ref = add_remote_async(1, 2).await?;
let result: i32 = obj_ref.get_async().await?;
```

In cluster mode, compile remote functions into a `cdylib`:
```bash
cargo build --release -p rayrust-example-worker
# Output: target/release/librayrust_worker.so
```

The driver passes the `.so` path via `code_search_path`:
```rust
let config = RayConfig::new("127.0.0.1:6379")
    .code_search_path(vec!["/path/to/librayrust_worker.so".to_string()]);
rayrust::init_with_config(&config)?;
```

### Actors

Rust actors use a factory + member function pattern:

```rust
struct Counter { value: i64 }

// Factory: returns Box<Counter> as a raw pointer (u64)
#[no_mangle]
extern "C" fn counter_factory(args: *const RayBytes, n: usize) -> RayBytes { ... }

// Member function: receives actor pointer + args
#[no_mangle]
extern "C" fn counter_increment(ptr: u64, args: *const RayBytes, n: usize) -> RayBytes { ... }

// Register
rayrust::register_function("counter_factory", counter_factory);
rayrust::register_member_function("counter_factory::increment", counter_increment);

// Use
let actor = rayrust::actor_create("counter_factory", &[&arg], &[])?;
let obj = rayrust::actor_call(actor.id(), "counter_factory::increment", &[&n])?;
let val: i64 = obj.get_async().await?;
```

### Cross-language (Python)

Call Python functions and actors from Rust. Results are automatically deserialized (xlang header stripping is built-in):

```rust
// Call a Python function
let arg = rayrust::serialize(&5i64)?;
let obj = rayrust::task_call_python("my_module", "add", &[&arg])?;
let val: i64 = obj.cast().get_async().await?; // auto xlang deserialization

// Create and call a Python actor
let actor = rayrust::actor_create_python("my_module", "Counter", &[&arg])?;
let obj = rayrust::actor_call_python(actor.id(), "increment", &[&n])?;
let val: i64 = obj.cast().get_async().await?;
```

### Placement Groups

```rust
let pg_id = rayrust::placement_group_create(
    "my_pg",
    r#"[{"CPU": 1}, {"CPU": 1}]"#,
    0, // PACK strategy
)?;
rayrust::placement_group_remove(&pg_id);
```

### Configuration

```rust
let config = RayConfig::new("127.0.0.1:6379")
    .node_ip("10.0.0.1")              // explicit node IP (multi-NIC)
    .code_search_path(vec![so_path])   // worker .so path
    .runtime_env(r#"{"pip": ["numpy"]}"#) // runtime environment
    .log_dir("/tmp/ray-logs");         // log directory
rayrust::init_with_config(&config)?;
```

## Architecture

```
Rust application code
    |
    v
rayrust (safe Rust API)
    - ObjectRef<T>, ActorHandle
    - #[remote] proc macro (sync + async)
    - serialize/deserialize (rmp-serde, msgpack)
    - get_async (eventfd + AsyncFd)
    |
    v
rayrust-sys (FFI bindings)
    - extern "C" declarations
    - build.rs (cc + link libray_api.so)
    |
    v
ray_c.h / ray_c.cc (C ABI wrapper)
    - Type-erased C interface
    - Binary-safe object IDs (ptr + len)
    - Cross-language arg wrapping
    - CoreWorker forward declarations
    |
    v
libray_api.so (Ray C++ SDK)
    |
    v
Ray Core (raylet / GCS / object store)
```

## Workspace Structure

```
rayrust/
├── crates/
│   ├── rayrust-sys/             # FFI + C ABI wrapper
│   │   ├── wrapper/
│   │   │   ├── ray_c.h          # C ABI header
│   │   │   └── ray_c.cc         # C ABI implementation
│   │   ├── build.rs             # cc + link libray_api.so
│   │   └── src/lib.rs           # extern "C" + RAII guards
│   ├── rayrust-macros/          # #[remote] proc macro
│   │   └── src/lib.rs           # sync + async fn support
│   ├── rayrust/                 # Safe Rust API
│   │   ├── examples/
│   │   │   ├── full_test.rs     # Comprehensive test
│   │   │   ├── async_demo.rs    # Async concurrent tasks
│   │   │   ├── cluster_remote_task.rs
│   │   │   └── test_local.rs    # Local mode test
│   │   └── src/
│   │       ├── lib.rs           # Re-exports + convenience
│   │       ├── error.rs         # RayError
│   │       ├── object_ref.rs    # ObjectRef<T> (sync + async)
│   │       ├── runtime.rs       # init/put/get/task/actor
│   │       └── serialize.rs     # msgpack + xlang bridge
│   └── rayrust-example-worker/  # cdylib worker template
│       └── src/lib.rs           # add/greet/multiply + Counter
├── tests/
│   └── python/rayrust_test.py   # Python helper for xlang tests
└── Cargo.toml
```

## How It Works

### Cluster mode remote tasks

1. Compile Rust remote functions into a `cdylib` (`.so`)
2. `#[remote]` generates a `#[ctor]` that auto-registers functions when the `.so` is loaded
3. The driver passes the `.so` path via `code_search_path`
4. The Ray worker process `dlopen`s the `.so`, `#[ctor]` fires, functions are registered in `FunctionManager`
5. Worker calls `GetRemoteFunctions()` — finds the Rust functions — executes them

### Async get (non-blocking)

- C++ polling thread: `Get(timeout=100ms)` loop, signals via `eventfd`
- Rust side: `tokio::io::AsyncFd` polls the eventfd — **zero threads blocked**
- After eventfd fires: fast `Get()` (instant, object is local) + deserialize

### Cross-language results

Python task results are wrapped with a 9-byte XLANG header. `ObjectRef<T>` carries an `is_xlang` flag — when set, `get()` / `get_async()` automatically strip the header before deserialization.

### Key design decisions

| Decision | Reason |
|---|---|
| Wrap C++ SDK, not native rewrite | `libray_api.so` ships with `pip install ray[cpp]` — no compilation needed |
| C ABI wrapper (`ray_c.h/cc`) | C++ templates have no stable ABI; C interface type-erases them |
| Binary-safe IDs (`ptr + len`) | Ray ObjectIDs may contain null bytes |
| `_GLIBCXX_USE_CXX11_ABI=0` | Matches Bazel-built `libray_api.so` |
| `GetFunctionManager()` not `Instance()` | Avoids singleton split between translation units |
| `#[ctor]` auto-registration | Functions registered at `.so` load time, before `GetRemoteFunctions()` |
| `--no-as-needed` linker flag | Forces `libray_api.so` into cdylib's NEEDED list |
| CoreWorker forward declarations | Calls `CancelTask` / `GetAsync` without including `core_worker.h` (avoids protobuf/gRPC/absl deps) |

## Limitations

1. **Python complex types**: Simple types (int, string) deserialize automatically. Complex types (lists, dicts with pickle) need additional work.
2. **Ray Serve / Train / Tune / RLlib**: These are Python ML libraries, not part of the C++ SDK. Not supported.
3. **`#[remote]` async fn in cluster mode**: Creates a tokio runtime per call. A persistent runtime would be better for high throughput.

## License

Apache-2.0 (same as Ray)
