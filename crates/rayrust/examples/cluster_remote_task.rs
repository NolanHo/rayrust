//! Example: Ray remote task in cluster mode with Rust cdylib worker.
//!
//! This driver connects to a Ray cluster and submits tasks that execute
//! in the Rust cdylib worker (.so).
//!
//! Prerequisites:
//! 1. Build the worker: `cargo build --release -p rayrust-example-worker`
//! 2. The resulting .so must be accessible from the worker node.
//!    Set RAY_WORKER_SO to the absolute path of librayrust_worker.so.
//!
//! Run:
//! ```bash
//! RAY_CPP_DIR=/path/to/ray/cpp \
//! RAY_ADDRESS=192.168.42.141:6379 \
//! RAY_NODE_IP=192.168.42.106 \
//! RAY_WORKER_SO=/root/code/rayrust/target/release/librayrust_worker.so \
//! cargo run --example cluster_remote_task
//! ```

use rayrust::prelude::*;

fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO")
        .unwrap_or_else(|_| {
            eprintln!("ERROR: RAY_WORKER_SO not set. Build the worker first:");
            eprintln!("  cargo build --release -p rayrust-example-worker");
            eprintln!("Then set RAY_WORKER_SO to the path of librayrust_worker.so");
            std::process::exit(1);
        });

    println!("=== Ray Cluster Remote Task ===");
    println!("Address: {}", address);
    println!("Node IP: {}", node_ip);
    println!("Worker .so: {}", worker_so);

    // Connect to cluster with code_search_path pointing to the worker .so
    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);

    println!("\nConnecting to Ray cluster...");
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

    // ── Remote Task (cluster mode) ─────────────────────────
    // These function names must match the ones in the cdylib worker.
    println!("\n--- Remote Task (cluster mode) ---");

    // add(10, 32)
    let arg1 = rayrust::serialize(&10i32).unwrap();
    let arg2 = rayrust::serialize(&32i32).unwrap();
    let args: Vec<&[u8]> = vec![&arg1, &arg2];

    match rayrust::task_call("add", &args) {
        Ok(obj_ref) => {
            println!("Task 'add(10, 32)' submitted");
            let obj_ref: ObjectRef<i32> = obj_ref.cast();
            match obj_ref.get() {
                Ok(val) => println!("Task result: {} ✓", val),
                Err(e) => println!("Task result get failed: {}", e),
            }
        }
        Err(e) => println!("Task call failed: {}", e),
    }

    // greet("Ray Cluster")
    let arg_name = rayrust::serialize(&"Ray Cluster".to_string()).unwrap();
    let args_greet: Vec<&[u8]> = vec![&arg_name];

    match rayrust::task_call("greet", &args_greet) {
        Ok(obj_ref) => {
            println!("Task 'greet(\"Ray Cluster\")' submitted");
            let obj_ref: ObjectRef<String> = obj_ref.cast();
            match obj_ref.get() {
                Ok(val) => println!("Greet result: {} ✓", val),
                Err(e) => println!("Greet result get failed: {}", e),
            }
        }
        Err(e) => println!("Task call failed: {}", e),
    }

    // multiply(7, 6)
    let arg_a = rayrust::serialize(&7i64).unwrap();
    let arg_b = rayrust::serialize(&6i64).unwrap();
    let args_mul: Vec<&[u8]> = vec![&arg_a, &arg_b];

    match rayrust::task_call("multiply", &args_mul) {
        Ok(obj_ref) => {
            println!("Task 'multiply(7, 6)' submitted");
            let obj_ref: ObjectRef<i64> = obj_ref.cast();
            match obj_ref.get() {
                Ok(val) => println!("Multiply result: {} ✓", val),
                Err(e) => println!("Multiply result get failed: {}", e),
            }
        }
        Err(e) => println!("Task call failed: {}", e),
    }

    // ── Shutdown ──────────────────────────────────────────
    println!("\n--- Shutdown ---");
    rayrust::shutdown();
    println!("✓ Ray shutdown");
}
