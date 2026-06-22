//! Example: Connect to a Ray cluster and run put/get + remote task.
//!
//! Run:
//! ```bash
//! RAY_CPP_DIR=/path/to/ray/cpp cargo run --example hello_ray
//! ```

use rayrust::prelude::*;

/// A simple remote function.
#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());

    println!("Connecting to Ray cluster at {} (node_ip={}) ...", address, node_ip);
    let config = RayConfig::new(&address).node_ip(&node_ip);
    match rayrust::init_with_config(&config) {
        Ok(()) => println!("✓ Ray initialized"),
        Err(e) => {
            eprintln!("✗ Failed to init Ray: {}", e);
            eprintln!("  Trying local mode...");
            rayrust::init_with_config(&RayConfig::local()).expect("local mode init failed");
            println!("✓ Ray initialized (local mode)");
        }
    }

    // ── Put / Get ──────────────────────────────────────────
    println!("\n--- Put / Get ---");
    let obj = rayrust::put(&42i32);
    println!("Put 42i32 → ObjectRef(id_len={})", obj.id().len());

    match rayrust::get(&obj) {
        Ok(val) => println!("Get → {} ✓", val),
        Err(e) => println!("Get failed: {}", e),
    }

    // ── Remote Task ────────────────────────────────────────
    // NOTE: Remote tasks require the worker process to have the function
    // registered via RAY_REMOTE. In a Rust binary, functions are not
    // automatically registered with Ray's FunctionManager. To use remote
    // tasks, you need to build a C++ shared library with RAY_REMOTE and
    // set it as code_search_path. This will be addressed in a future
    // iteration with a Rust-native registration mechanism.
    println!("\n--- Remote Task ---");
    println!("(Skipped: requires C++ worker with RAY_REMOTE registration)");

    // ── Put/Get with String ───────────────────────────────
    println!("\n--- Put / Get String ---");
    let obj2 = rayrust::put(&"hello ray from rust!".to_string());
    match rayrust::get(&obj2) {
        Ok(val) => println!("Get String → {} ✓", val),
        Err(e) => println!("Get String failed: {}", e),
    }

    // ── Namespace ─────────────────────────────────────────
    println!("\n--- Namespace ---");
    match rayrust::get_namespace() {
        Ok(ns) => println!("Namespace: {}", ns),
        Err(e) => println!("Get namespace failed: {}", e),
    }

    // ── Shutdown ──────────────────────────────────────────
    println!("\n--- Shutdown ---");
    rayrust::shutdown();
    println!("✓ Ray shutdown");
}
