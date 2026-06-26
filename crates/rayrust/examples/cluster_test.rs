//! Example: Ray remote task + PlacementGroup in cluster mode.

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
    let ray = match Ray::connect(&config) {
        Ok(ray) => {
            println!("✓ Ray initialized (cluster mode)");
            ray
        }
        Err(e) => {
            eprintln!("✗ Failed to init Ray: {}", e);
            std::process::exit(1);
        }
    };

    // ── Put / Get ──────────────────────────────────────────
    println!("\n--- Put / Get ---");
    let obj = ray.put(&42i32).unwrap();
    match obj.get() {
        Ok(val) => println!("Put/Get i32 → {} ✓", val),
        Err(e) => println!("Put/Get i32 failed: {}", e),
    }

    let obj2 = ray.put(&"hello from cluster!".to_string()).unwrap();
    match obj2.get() {
        Ok(val) => println!("Put/Get String → {} ✓", val),
        Err(e) => println!("Put/Get String failed: {}", e),
    }

    // ── Remote Task (cluster mode) ─────────────────────────
    println!("\n--- Remote Task (cluster mode) ---");
    let obj_ref = add_remote(&ray, 10, 32);
    println!("Task 'add(10, 32)' submitted");
    match obj_ref.get() {
        Ok(val) => println!("Task result: {} ✓", val),
        Err(e) => println!("Task result get failed: {}", e),
    }

    let obj_ref2 = greet_remote(&ray, "Ray Cluster".to_string());
    match obj_ref2.get() {
        Ok(val) => println!("Greet result: {} ✓", val),
        Err(e) => println!("Greet result get failed: {}", e),
    }

    // ── Namespace ─────────────────────────────────────────
    println!("\n--- Namespace ---");
    match ray.namespace() {
        Ok(ns) => println!("Namespace: {}", ns),
        Err(e) => println!("Get namespace failed: {}", e),
    }

    // ── Shutdown (automatic on drop) ──────────────────────
    println!("\n--- Shutdown ---");
    drop(ray);
    println!("✓ Ray shutdown");
}
