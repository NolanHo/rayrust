//! Rust Actor e2e test using #[rayrust::actor] macro.

use rayrust::prelude::*;

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| "/tmp/librayrust_worker.so".to_string());

    println!("=== Rust Actor E2E Test ===\n");

    let local = std::env::var("RAY_LOCAL").unwrap_or_default();
    let config = if local == "1" || address.is_empty() || address == "local" {
        println!("Using LOCAL mode");
        // In local mode, Ray C++ SDK doesn't auto-load the worker .so.
        // We must dlopen it ourselves to trigger #[ctor] registration.
        unsafe {
            let c_path = std::ffi::CString::new(worker_so.as_str()).unwrap();
            let handle = libc::dlopen(c_path.as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL);
            if handle.is_null() {
                eprintln!("Failed to load worker .so: {}", worker_so);
                std::process::exit(1);
            }
            // Keep the handle alive (don't dlclose)
            std::mem::forget(handle);
        }
        println!("Worker .so loaded: {}", worker_so);
        RayConfig::local().code_search_path(vec![worker_so.clone()])
    } else {
        RayConfig::new(&address)
            .node_ip(&node_ip)
            .code_search_path(vec![worker_so.clone()])
    };
    rayrust::init_with_config(&config).expect("init failed");
    println!("Ray initialized\n");

    let factory = "__rayrust_actor_factory_counter";
    let inc = format!("{}::increment", factory);
    let get = format!("{}::get", factory);
    let reset = format!("{}::reset", factory);

    // 1. Create actor
    println!("-- 1. Create actor --");
    let arg = rayrust::serialize(&100i64).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    let handle = rayrust::actor_create(&factory, &args, &[]).expect("actor create failed");
    println!("   Counter created (id_len={})\n", handle.id().len());

    // 2. Call methods
    println!("-- 2. Call methods --");
    let arg = rayrust::serialize(&5i64).unwrap();
    let r = rayrust::actor_call_async(handle.id(), &inc, vec![arg]).await.expect("inc failed");
    // Debug: get raw bytes
    let raw = r.get_raw_bytes().expect("raw get failed");
    println!("   Debug raw bytes ({}): {:02x?}", raw.len(), raw);
    let r = r.cast::<i64>();
    let v = r.get_async().await.expect("get failed");
    println!("   increment(5) = {} (expect 105)", v);
    assert_eq!(v, 105);

    let arg = rayrust::serialize(&10i64).unwrap();
    let r = rayrust::actor_call_async(handle.id(), &inc, vec![arg]).await.expect("inc failed").cast::<i64>();
    let v = r.get_async().await.expect("get failed");
    println!("   increment(10) = {} (expect 115)", v);
    assert_eq!(v, 115);

    let r = rayrust::actor_call_async(handle.id(), &get, vec![]).await.expect("get failed").cast::<i64>();
    let v = r.get_async().await.expect("get result failed");
    println!("   get() = {} (expect 115)", v);
    assert_eq!(v, 115);
    println!();

    // 3. State isolation
    println!("-- 3. State isolation --");
    let arg = rayrust::serialize(&0i64).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    let handle2 = rayrust::actor_create(&factory, &args, &[]).expect("actor2 create failed");
    let arg1 = rayrust::serialize(&1i64).unwrap();
    let arg2 = rayrust::serialize(&1i64).unwrap();
    let r1 = rayrust::actor_call_async(handle.id(), &inc, vec![arg1]).await.expect("inc1 failed").cast::<i64>();
    let r2 = rayrust::actor_call_async(handle2.id(), &inc, vec![arg2]).await.expect("inc2 failed").cast::<i64>();
    let v1 = r1.get_async().await.expect("get1 failed");
    let v2 = r2.get_async().await.expect("get2 failed");
    println!("   actor1.increment(1) = {} (expect 116)", v1);
    println!("   actor2.increment(1) = {} (expect 1)", v2);
    assert_eq!(v1, 116);
    assert_eq!(v2, 1);
    println!("   State isolated\n");

    // 4. Concurrent calls
    println!("-- 4. Concurrent calls --");
    let arg = rayrust::serialize(&0i64).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    let handle3 = rayrust::actor_create(&factory, &args, &[]).expect("actor3 create failed");
    let t0 = std::time::Instant::now();
    let mut futs = Vec::new();
    for _ in 0..50 {
        let arg = rayrust::serialize(&1i64).unwrap();
        futs.push(rayrust::actor_call_async(handle3.id(), &inc, vec![arg]));
    }
    let mut refs = Vec::new();
    for f in futs { refs.push(f.await.expect("inc failed").cast::<i64>()); }
    let mut set = tokio::task::JoinSet::new();
    for r in refs { set.spawn(async move { r.get_async().await.expect("get failed") }); }
    let mut sum = 0i64;
    while let Some(res) = set.join_next().await { sum += res.unwrap(); }
    println!("   50 concurrent increments: sum={} in {:?}", sum, t0.elapsed());
    println!();

    // 5. Reset
    println!("-- 5. Reset --");
    let r = rayrust::actor_call_async(handle.id(), &reset, vec![]).await.expect("reset failed");
    let _: () = r.get_async().await.expect("get reset failed");
    let r = rayrust::actor_call_async(handle.id(), &get, vec![]).await.expect("get failed").cast::<i64>();
    let v = r.get_async().await.expect("get result failed");
    println!("   After reset: get() = {} (expect 0)", v);
    assert_eq!(v, 0);
    println!();

    // 6. Resource scheduling
    println!("-- 6. Resource scheduling --");
    let arg = rayrust::serialize(&42i64).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    match rayrust::actor_create_with_resources(&factory, &args, &[("CPU", 1.0)]) {
        Ok(h) => {
            let r = rayrust::actor_call_async(h.id(), &get, vec![]).await.expect("call failed").cast::<i64>();
            let v = r.get_async().await.expect("get failed");
            println!("   Actor with CPU=1: get() = {} (expect 42)", v);
            assert_eq!(v, 42);
            h.kill(true);
            println!("   Resource scheduling OK\n");
        }
        Err(e) => println!("   Resource scheduling failed: {}\n", e),
    }

    handle.kill(true);
    handle2.kill(true);
    handle3.kill(true);
    println!("-- All actors killed --");
    println!("\n=== All Rust actor e2e tests passed ===");
    rayrust::shutdown();
}
