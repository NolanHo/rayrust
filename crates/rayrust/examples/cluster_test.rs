//! Example: Ray remote task + PlacementGroup in cluster mode.
//!
//! Run:
//! ```bash
//! RAY_CPP_DIR=/path/to/ray/cpp \
//! RAY_ADDRESS=192.168.42.141:6379 \
//! RAY_NODE_IP=192.168.42.106 \
//! cargo run --example cluster_test
//! ```

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[rayrust::remote]
fn greet(name: String) -> String {
    format!("Hello, {}!", name)
}

fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());

    // Register remote functions BEFORE init
    println!("Registering remote functions...");
    add_register();
    greet_register();
    println!("✓ Functions registered");

    println!("\nConnecting to Ray cluster at {} (node_ip={}) ...", address, node_ip);
    let config = RayConfig::new(&address).node_ip(&node_ip);
    match rayrust::init_with_config(&config) {
        Ok(()) => println!("✓ Ray initialized (cluster mode)"),
        Err(e) => {
            eprintln!("✗ Failed to init Ray: {}", e);
            std::process::exit(1);
        }
    }

    // ── Put / Get ──────────────────────────────────────────
    println!("\n--- Put / Get ---");
    let obj = rayrust::put(&42i32);
    match rayrust::get(&obj) {
        Ok(val) => println!("Put/Get i32 → {} ✓", val),
        Err(e) => println!("Put/Get i32 failed: {}", e),
    }

    let obj2 = rayrust::put(&"hello from cluster!".to_string());
    match rayrust::get(&obj2) {
        Ok(val) => println!("Put/Get String → {} ✓", val),
        Err(e) => println!("Put/Get String failed: {}", e),
    }

    // ── Remote Task (cluster mode) ─────────────────────────
    println!("\n--- Remote Task (cluster mode) ---");
    let obj_ref = add_remote(10, 32);
    println!("Task 'add(10, 32)' submitted");
    match obj_ref.get() {
        Ok(val) => println!("Task result: {} ✓", val),
        Err(e) => println!("Task result get failed: {}", e),
    }

    let obj_ref2 = greet_remote("Ray Cluster".to_string());
    match obj_ref2.get() {
        Ok(val) => println!("Greet result: {} ✓", val),
        Err(e) => println!("Greet result get failed: {}", e),
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
