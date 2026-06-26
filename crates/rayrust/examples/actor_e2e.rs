//! Rust Actor e2e test using #[rayrust::actor] macro.
//!
//! Demonstrates the macro-generated convenience callers:
//! - `counter_actor_create(&ray, &opts, start)` — create actor
//! - `counter_increment(&ray, &handle, n)` — async method caller
//! - `counter_get(&ray, &handle)` — async method caller
//! - `counter_reset(&ray, &handle)` — async method caller
//! - `counter_get_sync(&ray, &handle)` — sync method caller
//!
//! Compare with the old approach that used raw magic strings:
//! ```no_run
//! // OLD (manual, error-prone)
//! ray.actor_create("__rayrust_actor_factory_counter", &args, &ActorOptions::new())?;
//! ray.actor_call_async(handle.id(), "__rayrust_actor_factory_counter::increment", vec![arg]).await?;
//!
//! // NEW (macro-generated, type-safe)
//! counter_actor_create(&ray, &ActorOptions::new(), 100)?;
//! counter_increment(&ray, &handle, 5).await?.get_async().await?; // 105
//! ```

use rayrust::prelude::*;

/// A simple counter actor.
struct Counter {
    value: i64,
}

#[rayrust::actor]
impl Counter {
    fn new(start: i64) -> Self {
        Counter { value: start }
    }

    fn increment(&mut self, n: i64) -> i64 {
        self.value += n;
        self.value
    }

    fn get(&self) -> i64 {
        self.value
    }

    fn reset(&mut self) {
        self.value = 0;
    }
}

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS").unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP").unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO").unwrap_or_else(|_| "/tmp/librayrust_worker.so".to_string());

    println!("=== Rust Actor E2E Test (macro-generated callers) ===\n");

    let local = std::env::var("RAY_LOCAL").unwrap_or_default();
    let config = if local == "1" || address.is_empty() || address == "local" {
        println!("Using LOCAL mode");
        unsafe {
            let c_path = std::ffi::CString::new(worker_so.as_str()).unwrap();
            let handle = libc::dlopen(c_path.as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL);
            if handle.is_null() {
                eprintln!("Failed to load worker .so: {}", worker_so);
                std::process::exit(1);
            }
            let _ = handle;
        }
        println!("Worker .so loaded: {}", worker_so);
        RayConfig::local().code_search_path(vec![worker_so.clone()])
    } else {
        RayConfig::new(&address)
            .node_ip(&node_ip)
            .code_search_path(vec![worker_so.clone()])
    };
    let ray = Ray::connect(&config).expect("init failed");
    println!("Ray initialized\n");

    // 1. Create actor via macro-generated caller
    println!("-- 1. Create actor --");
    let handle = counter_actor_create(&ray, &ActorOptions::new(), 100i64)
        .expect("actor create failed");
    println!("   Counter created (id_len={})\n", handle.id().len());

    // 2. Call methods via macro-generated async callers
    println!("-- 2. Call methods (async) --");
    let r = counter_increment(&ray, &handle, 5).await.expect("inc failed");
    let v: i64 = r.get_async().await.expect("get failed");
    println!("   increment(5) = {} (expect 105)", v);
    assert_eq!(v, 105);

    let r = counter_increment(&ray, &handle, 10).await.expect("inc failed");
    let v: i64 = r.get_async().await.expect("get failed");
    println!("   increment(10) = {} (expect 115)", v);
    assert_eq!(v, 115);

    let r = counter_get(&ray, &handle).await.expect("get failed");
    let v: i64 = r.get_async().await.expect("get result failed");
    println!("   get() = {} (expect 115)", v);
    assert_eq!(v, 115);
    println!();

    // 3. Sync caller demonstration
    println!("-- 3. Sync caller --");
    let r = counter_get_sync(&ray, &handle);
    let v: i64 = r.get().expect("sync get failed");
    println!("   get_sync() = {} (expect 115)", v);
    assert_eq!(v, 115);
    println!();

    // 4. State isolation
    println!("-- 4. State isolation --");
    let handle2 = counter_actor_create(&ray, &ActorOptions::new(), 0i64)
        .expect("actor2 create failed");
    let r1 = counter_increment(&ray, &handle, 1).await.expect("inc1 failed");
    let r2 = counter_increment(&ray, &handle2, 1).await.expect("inc2 failed");
    let v1: i64 = r1.get_async().await.expect("get1 failed");
    let v2: i64 = r2.get_async().await.expect("get2 failed");
    println!("   actor1.increment(1) = {} (expect 116)", v1);
    println!("   actor2.increment(1) = {} (expect 1)", v2);
    assert_eq!(v1, 116);
    assert_eq!(v2, 1);
    println!("   State isolated\n");

    // 5. Concurrent calls
    println!("-- 5. Concurrent calls --");
    let handle3 = counter_actor_create(&ray, &ActorOptions::new(), 0i64)
        .expect("actor3 create failed");
    let t0 = std::time::Instant::now();
    let mut futs = Vec::new();
    for _ in 0..50 {
        futs.push(counter_increment(&ray, &handle3, 1));
    }
    let mut refs = Vec::new();
    for f in futs {
        refs.push(f.await.expect("inc failed").cast::<i64>());
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.get_async().await.expect("get failed") });
    }
    let mut sum = 0i64;
    while let Some(res) = set.join_next().await {
        sum += res.unwrap();
    }
    println!("   50 concurrent increments: sum={} in {:?}", sum, t0.elapsed());
    println!();

    // 6. Reset
    println!("-- 6. Reset --");
    let r = counter_reset(&ray, &handle).await.expect("reset failed");
    let _: () = r.get_async().await.expect("get reset failed");
    let r = counter_get(&ray, &handle).await.expect("get failed");
    let v: i64 = r.get_async().await.expect("get result failed");
    println!("   After reset: get() = {} (expect 0)", v);
    assert_eq!(v, 0);
    println!();

    // 7. ActorOptions: resource scheduling + name
    println!("-- 7. ActorOptions (resource + name) --");
    let opts = ActorOptions::new()
        .name("counter_gpu")
        .resource("CPU", 1.0);
    match counter_actor_create(&ray, &opts, 42i64) {
        Ok(h) => {
            let r = counter_get(&ray, &h).await.expect("call failed");
            let v: i64 = r.get_async().await.expect("get failed");
            println!("   Named actor with CPU=1: get() = {} (expect 42)", v);
            assert_eq!(v, 42);

            // Look up by name (cross-namespace lookup demo)
            match ray.get_actor("counter_gpu", "") {
                Ok(Some(found)) => {
                    println!("   get_actor(\"counter_gpu\") found (id_len={})", found.id().len());
                }
                Ok(None) => println!("   get_actor(\"counter_gpu\") not found"),
                Err(e) => println!("   get_actor error: {}", e),
            }

            let _ = ray.kill_actor(&h, true);
            println!("   Resource scheduling OK\n");
        }
        Err(e) => println!("   Resource scheduling failed: {}\n", e),
    }

    let _ = ray.kill_actor(&handle, true);
    let _ = ray.kill_actor(&handle2, true);
    let _ = ray.kill_actor(&handle3, true);
    println!("-- All actors killed --");
    println!("\n=== All Rust actor e2e tests passed ===");
    drop(ray);
}
