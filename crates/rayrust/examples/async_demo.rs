//! Example: Async remote tasks with tokio + Ray cluster.
//!
//! Uses `CoreWorker::GetAsync` + eventfd for non-blocking async gets.
//! Put/Get still uses sync API (Put goes to plasma store, not memory store).
//! Remote task results arrive via CoreWorker's task receiver into memory store,
//! where GetAsync's callback fires.

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[rayrust::remote]
fn greet(name: String) -> String {
    format!("Hello, {} from async Rust!", name)
}

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO")
        .unwrap_or_else(|_| {
            eprintln!("ERROR: RAY_WORKER_SO not set.");
            std::process::exit(1);
        });

    println!("=== Ray Async Demo (GetAsync + eventfd) ===");
    println!("Address: {}, Node: {}", address, node_ip);

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);

    rayrust::init_with_config(&config).expect("init failed");
    println!("✓ Ray initialized\n");

    // ── Concurrent Remote Tasks (tokio::join!) ──────────────
    println!("--- Concurrent Remote Tasks ---");

    let (r1, r2, r3) = tokio::join!(
        add_remote_async(10, 32),
        add_remote_async(100, 200),
        greet_remote_async("Tokio".to_string()),
    );

    let r1: ObjectRef<i32> = r1.expect("add(10,32) failed");
    let r2: ObjectRef<i32> = r2.expect("add(100,200) failed");
    let r3: ObjectRef<String> = r3.expect("greet failed");

    // get_async uses CoreWorker::GetAsync + eventfd — zero threads blocked
    let (v1, v2, v3) = tokio::join!(
        r1.get_async(),
        r2.get_async(),
        r3.get_async(),
    );

    println!("add(10, 32) = {} ✓", v1.unwrap());
    println!("add(100, 200) = {} ✓", v2.unwrap());
    println!("greet(\"Tokio\") = {} ✓", v3.unwrap());

    // ── Batch Concurrent Tasks (10 tasks) ───────────────────
    println!("\n--- Batch Concurrent (10 tasks) ---");

    let mut submit_futs = Vec::new();
    for i in 0..10i32 {
        submit_futs.push(add_remote_async(i, i * 2));
    }

    let obj_refs: Vec<ObjectRef<i32>> = join_all(submit_futs).await
        .into_iter().map(|r| r.expect("task failed")).collect();

    let mut get_futs = Vec::new();
    for obj_ref in obj_refs {
        get_futs.push(async move { obj_ref.get_async().await });
    }
    let results: Vec<i32> = join_all(get_futs).await
        .into_iter().map(|r| r.unwrap_or(0)).collect();

    let mut sum = 0i32;
    for (i, val) in results.iter().enumerate() {
        sum += val;
        if i < 3 || i >= results.len() - 1 {
            println!("  task[{}] add({}, {}) = {}", i, i, i * 2, val);
        } else if i == 3 {
            println!("  ...");
        }
    }
    println!("Sum of all results: {} ✓", sum);

    // ── Shutdown ────────────────────────────────────────────
    println!("\n--- Shutdown ---");
    rayrust::shutdown();
    println!("✓ Ray shutdown");
}

async fn join_all<F, T>(futs: Vec<F>) -> Vec<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut set = tokio::task::JoinSet::new();
    for f in futs {
        set.spawn(f);
    }
    let mut results = Vec::with_capacity(set.len());
    while let Some(res) = set.join_next().await {
        results.push(res.unwrap());
    }
    results
}
