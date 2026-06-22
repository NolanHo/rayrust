//! Comprehensive test: all features.

use rayrust::prelude::*;

#[rayrust::remote]
fn add(a: i32, b: i32) -> i32 {
    a + b
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

    println!("=== Comprehensive Feature Test ===\n");

    let config = RayConfig::new(&address)
        .node_ip(&node_ip)
        .code_search_path(vec![worker_so.clone()]);
    rayrust::init_with_config(&config).expect("init failed");
    println!("✓ Ray initialized\n");

    // ── 1. Wait ──────────────────────────────────────────────
    println!("--- 1. Wait ---");
    let obj1 = rayrust::put(&10i32);
    let obj2 = rayrust::put(&20i32);
    let (ready, unready) = rayrust::wait(&[obj1, obj2], 2, 5000)
        .expect("wait failed");
    println!("wait: {} ready, {} unready ✓", ready.len(), unready.len());

    // ── 2. Remote Task (Rust → Rust) ─────────────────────────
    println!("\n--- 2. Remote Task (Rust) ---");
    let r = add_remote(3, 4);
    let v: i32 = r.get_async().await.expect("add failed");
    println!("add(3, 4) = {} ✓", v);

    // ── 3. Batch Get (get_many) ─────────────────────────────
    println!("\n--- 3. Batch Get ---");
    let r1 = rayrust::put(&100i32);
    let r2 = rayrust::put(&200i32);
    let r3 = rayrust::put(&300i32);
    let vals = rayrust::get_many(&[r1, r2, r3]).expect("get_many failed");
    println!("get_many: {:?} ✓", vals);

    // ── 4. Cross-language: Python Task ──────────────────────
    println!("\n--- 4. Cross-language: Python Task ---");
    let arg_a = rayrust::serialize(&5i64).unwrap();
    let arg_b = rayrust::serialize(&3i64).unwrap();
    let args: Vec<&[u8]> = vec![&arg_a, &arg_b];

    match rayrust::task_call_python("rayrust_test", "add", &args) {
        Ok(obj_ref) => {
            println!("Python add(5, 3) task submitted");
            let obj_ref: ObjectRef<i64> = obj_ref.cast();
            match obj_ref.get_async().await {
                Ok(val) => println!("Python add(5, 3) = {} ✓ (auto xlang deserialization)", val),
                Err(e) => println!("Python add result failed: {}", e),
            }
        }
        Err(e) => println!("Python task_call failed: {}", e),
    }

    // ── 5. Cross-language: Python Actor ─────────────────────
    println!("\n--- 5. Cross-language: Python Actor ---");
    let arg_start = rayrust::serialize(&10i64).unwrap();
    let args_actor: Vec<&[u8]> = vec![&arg_start];

    match rayrust::actor_create_python("rayrust_test", "Counter", &args_actor) {
        Ok(actor) => {
            println!("Python Counter actor created ✓");

            let arg_n = rayrust::serialize(&5i64).unwrap();
            let args_inc: Vec<&[u8]> = vec![&arg_n];
            match rayrust::actor_call_python(actor.id(), "increment", &args_inc) {
                Ok(obj_ref) => {
                    let obj_ref: ObjectRef<i64> = obj_ref.cast();
                    match obj_ref.get_async().await {
                        Ok(val) => println!("Counter.increment(5) = {} ✓ (auto xlang)", val),
                        Err(e) => println!("Counter.increment result failed: {}", e),
                    }
                }
                Err(e) => println!("Counter.increment failed: {}", e),
            }

            actor.kill(true);
            println!("Counter killed ✓");
        }
        Err(e) => println!("Python actor_create failed: {}", e),
    }

    // ── 6. Placement Group ──────────────────────────────────
    println!("\n--- 6. Placement Group ---");
    let bundles_json = r#"[{"CPU": 1}, {"CPU": 1}]"#;
    match rayrust::placement_group_create("test_pg", bundles_json, 0) {
        Ok(pg_id) => {
            println!("PlacementGroup created ✓ (id_len={})", pg_id.len());
            rayrust::placement_group_remove(&pg_id);
            println!("PlacementGroup removed ✓");
        }
        Err(e) => println!("PlacementGroup create failed: {}", e),
    }

    // ── 7. id_hex ───────────────────────────────────────────
    println!("\n--- 7. Debug helpers ---");
    let obj = rayrust::put(&42i32);
    println!("ObjectRef id_hex: {} ✓", obj.id_hex());
    println!("is_initialized: {} ✓", rayrust::is_initialized());

    // ── Shutdown ────────────────────────────────────────────
    println!("\n--- Shutdown ---");
    rayrust::shutdown();
    println!("✓ Ray shutdown");
}
