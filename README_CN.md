# rayrust

[English](README.md) | [中文](README_CN.md)

Ray 的 Rust SDK —— 通过 FFI 封装 Ray C++ SDK，提供地道的 Rust API 来使用 Ray 的核心分布式原语：对象存储、远程任务、Actor、Placement Group 和跨语言调用。

## 特性

| 特性 | 本地模式 | 集群模式 |
|---|:---:|:---:|
| `init` / `shutdown` | ✅ | ✅ |
| `put` / `get` / `wait` | ✅ | ✅ |
| `get_many`（批量获取） | ✅ | ✅ |
| `get_namespace` | ✅ | ✅ |
| `#[remote]` 同步任务 | ✅ | ✅ |
| `#[remote]` 异步任务 | ✅ | ✅ |
| `get_async`（非阻塞） | ✅ | ✅ |
| Rust Actor（工厂 + 方法） | ✅ | ✅ |
| Python 任务（跨语言） | ✅ | ✅ |
| Python Actor（跨语言） | ✅ | ✅ |
| `get_actor`（命名 Actor） | ✅ | ✅ |
| `cancel`（取消任务） | ✅ | ✅ |
| `kill`（终止 Actor） | ✅ | ✅ |
| PlacementGroup | ✅ | ✅ |
| ObjectRef 作为任务参数 | ✅ | ✅ |
| `runtime_env`（运行环境） | ✅ | ✅ |
| `log_dir`（日志目录） | ✅ | ✅ |

## 快速开始

### 前置条件

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

### 编译

```bash
git clone https://github.com/NolanHo/rayrust.git
cd rayrust
cargo build --release -p rayrust-example-worker
cargo build --example full_test
```

### 本地模式运行（无需集群）

```bash
cargo run --example test_local
```

输出：
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

### 集群模式运行

```bash
export RAY_ADDRESS='<head节点IP>:6379'
export RAY_NODE_IP='<本机IP>'
export RAY_WORKER_SO="$(pwd)/target/release/librayrust_worker.so"
export LD_LIBRARY_PATH="$RAY_CPP_DIR/lib:$LD_LIBRARY_PATH"

cargo run --example full_test
```

输出：
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

## 使用方式

### 同步 API

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

### 异步 API（tokio）

`get_async()` 使用轮询线程 + eventfd + `tokio::io::AsyncFd` 模式 —— **不阻塞任何 tokio 线程**。

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

### 远程任务（`#[remote]`）

支持同步和 `async fn`。集群模式下编译为 `cdylib`，通过 `code_search_path` 传入。

```rust
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 { a + b }

#[rayrust::remote]
async fn fetch(url: String) -> Vec<u8> { /* ... */ }
```

### Actor

工厂 + 成员函数模式：

```rust
let actor = rayrust::actor_create("counter_factory", &[&arg], &[])?;
let obj = rayrust::actor_call(actor.id(), "counter_factory::increment", &[&n])?;
let val: i64 = obj.get_async().await?;
```

### 跨语言调用（Python）

```rust
let obj = rayrust::task_call_python("my_module", "add", &[&arg])?;
let val: i64 = obj.cast().get_async().await?; // 自动 xlang 反序列化
```

### 配置

```rust
let config = RayConfig::new("127.0.0.1:6379")
    .node_ip("10.0.0.1")                          // 指定节点 IP（多网卡场景）
    .code_search_path(vec!["/path/to/librayrust_worker.so".to_string()])  // worker .so 路径
    .runtime_env(r#"{"pip": ["numpy"]}"#)          // 运行环境
    .log_dir("/tmp/ray-logs");                     // 日志目录
rayrust::init_with_config(&config)?;
```

## 架构

```
Rust 应用代码
    |
    v
rayrust（安全 Rust API）
    ObjectRef<T>, ActorHandle, #[remote] 宏, get_async (eventfd + AsyncFd)
    |
    v
rayrust-sys（FFI 绑定）
    extern "C" 声明, build.rs (cc + link libray_api.so)
    |
    v
ray_c.h / ray_c.cc（C ABI 封装层）
    类型擦除的 C 接口, 二进制安全的 ID, 跨语言参数包装
    |
    v
libray_api.so（Ray C++ SDK）  ->  Ray Core (raylet / GCS / object store)
```

## 关键设计决策

| 决策 | 原因 |
|---|---|
| 封装 C++ SDK，非原生重写 | `libray_api.so` 随 `pip install ray[cpp]` 发布，无需编译 |
| C ABI 封装层 | C++ 模板无稳定 ABI，用 C 接口做类型擦除 |
| 二进制安全 ID（`ptr + len`） | Ray ObjectID 可能包含 null 字节 |
| `_GLIBCXX_USE_CXX11_ABI=0` | 匹配 Bazel 编译的 `libray_api.so` |
| 用 `GetFunctionManager()` 而非 `Instance()` | 避免翻译单元间的单例分裂 |
| `#[ctor]` 自动注册 | `.so` 加载时自动注册函数 |
| `--no-as-needed` 链接选项 | 强制 `libray_api.so` 进入 cdylib 的 NEEDED 列表 |
| CoreWorker 前向声明 | 不 include `core_worker.h` 即可调用 `CancelTask`/`GetAsync` |

## 已知限制

- **Python 复杂类型**：简单类型（int、string）可自动反序列化；复杂类型（list、dict with pickle）需要额外处理。
- **Ray Serve / Train / Tune / RLlib**：这些是 Python ML 生态库，不属于 C++ SDK。
- **`#[remote]` async fn 集群模式**：每次调用创建一个 tokio runtime，高频场景下应考虑持久化 runtime。

## License

Apache-2.0（与 Ray 一致）
