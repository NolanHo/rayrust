# rayrust

[English](README.md) | [中文](README_CN.md)

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
pip install "ray[cpp]"

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
cargo build --release -p rayrust-example-worker
cargo build --example full_test
```

### Run in local mode (no cluster needed)

```bash
cargo run --example test_local
```

### Run against a Ray cluster

```bash
export RAY_ADDRESS='<head-node-ip>:6379'
export RAY_NODE_IP='<this-node-ip>'
export RAY_WORKER_SO="$(pwd)/target/release/librayrust_worker.so"
export LD_LIBRARY_PATH="$RAY_CPP_DIR/lib:$LD_LIBRARY_PATH"

cargo run --example full_test
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

`get_async()` uses a polling thread + eventfd + `tokio::io::AsyncFd` — **zero tokio threads blocked**.

```rust
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[tokio::main]
async fn main() -> Result<(), RayError> {
    rayrust::init("127.0.0.1:6379")?;

    let (r1, r2) = tokio::join!(add_remote_async(1, 2), add_remote_async(3, 4));
    let (v1, v2) = tokio::join!(r1?.get_async(), r2?.get_async());
    println!("{} {}", v1?, v2?);

    rayrust::shutdown();
    Ok(())
}
```

### Remote tasks (`#[remote]`)

Supports sync and `async fn`. In cluster mode, compile into a `cdylib` and pass via `code_search_path`.

```rust
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[rayrust::remote]
async fn fetch(url: String) -> Vec<u8> { /* ... */ }
```

### Actors

Factory + member function pattern:

```rust
let actor = rayrust::actor_create("counter_factory", &[&arg], &[])?;
let obj = rayrust::actor_call(actor.id(), "counter_factory::increment", &[&n])?;
let val: i64 = obj.get_async().await?;
```

### Cross-language (Python)

```rust
let obj = rayrust::task_call_python("my_module", "add", &[&arg])?;
let val: i64 = obj.cast().get_async().await?; // auto xlang deserialization
```

### Configuration

```rust
let config = RayConfig::new("127.0.0.1:6379")
    .node_ip("10.0.0.1")
    .code_search_path(vec!["/path/to/librayrust_worker.so".to_string()])
    .runtime_env(r#"{"pip": ["numpy"]}"#)
    .log_dir("/tmp/ray-logs");
rayrust::init_with_config(&config)?;
```

## Architecture

```
Rust application code
    |
    v
rayrust (safe Rust API)
    ObjectRef<T>, ActorHandle, #[remote] macro, get_async (eventfd + AsyncFd)
    |
    v
rayrust-sys (FFI bindings)
    extern "C" declarations, build.rs (cc + link libray_api.so)
    |
    v
ray_c.h / ray_c.cc (C ABI wrapper)
    Type-erased C interface, binary-safe IDs, cross-language wrapping
    |
    v
libray_api.so (Ray C++ SDK)  ->  Ray Core (raylet / GCS / object store)
```

## Key Design Decisions

| Decision | Reason |
|---|---|
| Wrap C++ SDK, not native rewrite | `libray_api.so` ships with `pip install ray[cpp]` |
| C ABI wrapper | C++ templates have no stable ABI |
| Binary-safe IDs (`ptr + len`) | Ray ObjectIDs may contain null bytes |
| `_GLIBCXX_USE_CXX11_ABI=0` | Matches Bazel-built `libray_api.so` |
| `GetFunctionManager()` not `Instance()` | Avoids singleton split between translation units |
| `#[ctor]` auto-registration | Functions registered at `.so` load time |
| `--no-as-needed` linker flag | Forces `libray_api.so` into cdylib's NEEDED list |
| CoreWorker forward declarations | Calls `CancelTask`/`GetAsync` without `core_worker.h` |

## Limitations

- **Python complex types**: Simple types (int, string) deserialize automatically; complex types need additional work.
- **Ray Serve / Train / Tune / RLlib**: Python ML libraries, not part of the C++ SDK.
- **`#[remote]` async fn in cluster mode**: Creates a tokio runtime per call.

## License

Apache-2.0 (same as Ray)
