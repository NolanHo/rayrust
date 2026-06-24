//! Benchmark: Rust vs Python Ray task throughput and latency.
//!
//! Measures:
//! 1. Rust → Rust sync tasks (sequential, measures FFI + serialize overhead)
//! 2. Rust → Rust async tasks (concurrent, measures persistent runtime)
//! 3. Rust → Python tasks (concurrent, measures xlang overhead)
//! 4. Latency: single-task round-trip (median of 100)
//! 5. Async runtime: 100 tasks × 50ms sleep (parallel vs serial)
//! 6. Compute-intensive: sum(0..1M) × 10 (Rust vs Python)
//! 7. Complex type serialization overhead
//!
//! Run on mint-dev:
//!   PYTHONPATH=/tmp LD_LIBRARY_PATH=/tmp \
//!   RAY_ADDRESS=192.168.42.141:6379 RAY_NODE_IP=192.168.42.106 \
//!   RAY_WORKER_SO=/tmp/librayrust_worker.so \
//!   /tmp/raybench

use rayrust::prelude::*;
use std::time::{Duration, Instant};

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[rayrust::remote]
async fn async_sum(a: i64, b: i64) -> i64 {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    a + b
}

/// CPU-intensive: sum of 0..n.
#[rayrust::remote]
fn compute(n: i64) -> i64 {
    (0..n).sum()
}

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO")
        .unwrap_or_else(|_| "/tmp/librayrust_worker.so".to_string());

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║         Rayrust Benchmark: Rust vs Python                ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);
    rayrust::init_with_config(&config).expect("init failed");
    println!("✓ Ray initialized\n");

    let warmup_arg_a = rayrust::serialize(&1i64).unwrap();
    let warmup_arg_b = rayrust::serialize(&1i64).unwrap();
    let warmup_args: Vec<&[u8]> = vec![&warmup_arg_a, &warmup_arg_b];
    let _ = rayrust::task_call_python("rayrust_test", "add", &warmup_args, &[]);
    let _ = add_remote(1, 1);
    println!("✓ Warmup done\n");

    // ── 1. Rust → Rust: Sync throughput (sequential) ────────────
    println!("── 1. Rust → Rust sync (sequential, N=500) ──");
    let n = 500;
    let t0 = Instant::now();
    let mut sum = 0i64;
    for i in 0..n {
        let r = add_remote(i, 1);
        let v: i32 = r.get().expect("get failed");
        sum += v as i64;
    }
    let elapsed = t0.elapsed();
    println!("   {} tasks in {:.2?} → {:.0} tasks/sec, {:.2?}/task",
            n, elapsed, n as f64 / elapsed.as_secs_f64(), elapsed / n as u32);
    println!("   checksum: {}\n", sum);

    // ── 2. Rust → Rust: Async throughput (concurrent) ──────────
    println!("── 2. Rust → Rust async (concurrent, N=500) ──");
    let n = 500;
    let t0 = Instant::now();
    let mut futs = Vec::new();
    for i in 0..n {
        futs.push(add_remote_async(i, 1));
    }
    let mut refs = Vec::new();
    for f in futs {
        refs.push(f.await.expect("submit failed"));
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.get_async().await.expect("get failed") });
    }
    let mut sum = 0i32;
    while let Some(res) = set.join_next().await {
        sum += res.unwrap();
    }
    let elapsed = t0.elapsed();
    println!("   {} tasks in {:.2?} → {:.0} tasks/sec, {:.2?}/task",
            n, elapsed, n as f64 / elapsed.as_secs_f64(), elapsed / n as u32);
    println!("   checksum: {}\n", sum);

    // ── 3. Rust → Python: Async throughput (concurrent) ─────────
    println!("── 3. Rust → Python async (concurrent, N=500) ──");
    let n = 500;
    let t0 = Instant::now();
    // task_call_python is sync (returns ObjectRef immediately)
    let mut refs = Vec::new();
    for i in 0..n {
        let arg_a = rayrust::serialize(&(i as i64)).unwrap();
        let arg_b = rayrust::serialize(&1i64).unwrap();
        let args: Vec<&[u8]> = vec![&arg_a, &arg_b];
        let r = rayrust::task_call_python("rayrust_test", "add", &args, &[])
            .expect("python task failed");
        refs.push(r);
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.cast::<i64>().get_async().await.expect("get failed") });
    }
    let mut sum = 0i64;
    while let Some(res) = set.join_next().await {
        sum += res.unwrap();
    }
    let elapsed = t0.elapsed();
    println!("   {} tasks in {:.2?} → {:.0} tasks/sec, {:.2?}/task",
            n, elapsed, n as f64 / elapsed.as_secs_f64(), elapsed / n as u32);
    println!("   checksum: {}\n", sum);

    // ── 4. Latency: single-task round-trip (median of 100) ─────
    println!("── 4. Latency: single-task round-trip (median of 100) ──");

    let mut times_rust_sync = Vec::new();
    for _ in 0..100 {
        let t0 = Instant::now();
        let r = add_remote(1, 2);
        let _v: i32 = r.get().unwrap();
        times_rust_sync.push(t0.elapsed());
    }
    times_rust_sync.sort();

    let mut times_rust_async = Vec::new();
    for _ in 0..100 {
        let t0 = Instant::now();
        let r = add_remote_async(1, 2).await.unwrap();
        let _v: i32 = r.get_async().await.unwrap();
        times_rust_async.push(t0.elapsed());
    }
    times_rust_async.sort();

    let mut times_python = Vec::new();
    for _ in 0..100 {
        let t0 = Instant::now();
        let arg_a = rayrust::serialize(&1i64).unwrap();
        let arg_b = rayrust::serialize(&2i64).unwrap();
        let args: Vec<&[u8]> = vec![&arg_a, &arg_b];
        let r = rayrust::task_call_python("rayrust_test", "add", &args, &[]).unwrap();
        let _v: i64 = r.cast().get_async().await.unwrap();
        times_python.push(t0.elapsed());
    }
    times_python.sort();

    let median = |v: &[Duration]| v[v.len() / 2];
    let p99 = |v: &[Duration]| v[v.len() * 99 / 100];
    println!("   Rust sync:    median {:>6.2?}  p99 {:>6.2?}", median(&times_rust_sync), p99(&times_rust_sync));
    println!("   Rust async:   median {:>6.2?}  p99 {:>6.2?}", median(&times_rust_async), p99(&times_rust_async));
    println!("   Python xlang: median {:>6.2?}  p99 {:>6.2?}", median(&times_python), p99(&times_python));
    println!();

    // ── 5. Async runtime: 100 tasks × 50ms sleep (parallel test) ─
    println!("── 5. Async runtime: 100 tasks × 50ms sleep ──");
    let n = 100;
    let t0 = Instant::now();
    let mut futs = Vec::new();
    for i in 0..n {
        futs.push(async_sum_remote_async(i, 0));
    }
    let mut refs = Vec::new();
    for f in futs {
        refs.push(f.await.expect("submit failed"));
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.get_async().await.expect("get failed") });
    }
    while set.join_next().await.is_some() {}
    let elapsed = t0.elapsed();
    let serial_expected = Duration::from_millis(50 * n as u64);
    let speedup = serial_expected.as_secs_f64() / elapsed.as_secs_f64();
    println!("   {} tasks × 50ms in {:.2?} (serial would be ~{:?})", n, elapsed, serial_expected);
    println!("   parallel speedup: {:.1}x\n", speedup);

    // ── 6. Compute-intensive: sum(0..1M) × 10 ──────────────────
    println!("── 6. Compute: sum(0..1_000_000) × 10 tasks ──");
    let n_tasks = 10;
    let n_compute: i64 = 1_000_000;

    // Rust
    let t0 = Instant::now();
    let mut futs = Vec::new();
    for _ in 0..n_tasks {
        futs.push(compute_remote_async(n_compute));
    }
    let mut refs = Vec::new();
    for f in futs {
        refs.push(f.await.expect("submit failed"));
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.get_async().await.expect("get failed") });
    }
    while set.join_next().await.is_some() {}
    let rust_elapsed = t0.elapsed();

    // Python
    let t0 = Instant::now();
    let mut refs = Vec::new();
    for _ in 0..n_tasks {
        let arg = rayrust::serialize(&n_compute).unwrap();
        let args: Vec<&[u8]> = vec![&arg];
        let r = rayrust::task_call_python("rayrust_test", "compute", &args, &[])
            .expect("python task failed");
        refs.push(r);
    }
    let mut set = tokio::task::JoinSet::new();
    for r in refs {
        set.spawn(async move { r.cast::<i64>().get_async().await.expect("get failed") });
    }
    while set.join_next().await.is_some() {}
    let python_elapsed = t0.elapsed();

    println!("   Rust:   {:>6.2?} ({:.0} tasks/sec)", rust_elapsed, n_tasks as f64 / rust_elapsed.as_secs_f64());
    println!("   Python: {:>6.2?} ({:.0} tasks/sec)", python_elapsed, n_tasks as f64 / python_elapsed.as_secs_f64());
    println!("   Rust is {:.1}x faster for compute\n", python_elapsed.as_secs_f64() / rust_elapsed.as_secs_f64());

    // ── 7. Complex type serialization overhead ─────────────────
    println!("── 7. Serialization breakdown (100×100 nested Vec, 30KB) ──");
    let big_list: Vec<Vec<i64>> = (0..100).map(|i| (0..100).map(|j| i * 100 + j).collect()).collect();
    let n_iter = 50;

    // 7a. Pure Rust serialize+deserialize (no Ray, no network)
    let t0 = Instant::now();
    for _ in 0..n_iter {
        let bytes = rayrust::serialize(&big_list).unwrap();
        let _back: Vec<Vec<i64>> = rayrust::deserialize(&bytes).unwrap();
    }
    let pure_serde = t0.elapsed();

    // 7b. Rust put only (serialize + object store)
    let t0 = Instant::now();
    for _ in 0..n_iter {
        let _obj = rayrust::put(&big_list);
    }
    let put_elapsed = t0.elapsed();

    // 7c. Full Rust→Python echo (serialize + xlang + Ray + Python + return)
    let t0 = Instant::now();
    for _ in 0..n_iter {
        let arg = rayrust::serialize(&big_list).unwrap();
        let args: Vec<&[u8]> = vec![&arg];
        let r = rayrust::task_call_python("rayrust_test", "echo_list", &args, &[]).unwrap();
        let _v = r.get_value_async().await.unwrap();
    }
    let full_xlang = t0.elapsed();

    let ser_per = pure_serde / n_iter as u32;
    let put_per = put_elapsed / n_iter as u32;
    let full_per = full_xlang / n_iter as u32;
    let ray_overhead = full_per - put_per;

    let ser_pct = pure_serde.as_micros() * 100 / full_xlang.as_micros();
    let ray_pct = (full_xlang - put_elapsed).as_micros() * 100 / full_xlang.as_micros();

    println!("   Pure serde (no Ray):     {:>6.2?}/iter  ← msgpack encode+decode", ser_per);
    println!("   Rust put (serialize):    {:>6.2?}/iter  ← serialize + object store", put_per);
    println!("   Full Rust→Python echo:   {:>6.2?}/iter  ← serialize + xlang + Ray + Python + return", full_per);
    println!("   ─────────────────────────────────────");
    println!("   Serialization cost:      {:>6.2?}/iter  ({}% of total)", ser_per, ser_pct);
    println!("   Ray + xlang overhead:    {:>6.2?}/iter  ({}% of total)", ray_overhead, ray_pct);
    println!();

    // ── Summary ────────────────────────────────────────────────
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║ Summary                                                  ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║ Rust sync latency:   median {:>6.2?}                     ║", median(&times_rust_sync));
    println!("║ Rust async latency:  median {:>6.2?}                     ║", median(&times_rust_async));
    println!("║ Python xlang latency: median {:>6.2?}                     ║", median(&times_python));
    println!("║ Async parallel speedup: {:.1}x (vs serial 5s)            ║", speedup);
    println!("║ Compute speedup:      {:.1}x (Rust vs Python)            ║", python_elapsed.as_secs_f64() / rust_elapsed.as_secs_f64());
    println!("║ Serialization share: {}% of xlang round-trip            ║", ser_pct);
    println!("╚══════════════════════════════════════════════════════════╝");

    rayrust::shutdown();
}
