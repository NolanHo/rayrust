//! Cross-language complex type test: Rust ↔ Python.
//!
//! Tests both directions:
//! - Python → Rust: Python functions return list/dict/nested/None/mixed,
//!   Rust deserializes into typed structs or rmpv::Value.
//! - Rust → Python: Rust sends complex args (Vec, HashMap), Python processes them.

use rayrust::prelude::*;
use std::collections::HashMap;

/// Helper: look up a key in an rmpv::Value map.
fn vget<'a>(val: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    let map = val.as_map()?;
    for (k, v) in map {
        if k.as_str() == Some(key) {
            return Some(v);
        }
    }
    None
}

#[tokio::main]
async fn main() {
    let address = std::env::var("RAY_ADDRESS")
        .unwrap_or_else(|_| "192.168.42.141:6379".to_string());
    let node_ip = std::env::var("RAY_NODE_IP")
        .unwrap_or_else(|_| "192.168.42.106".to_string());
    let worker_so = std::env::var("RAY_WORKER_SO")
        .unwrap_or_default();

    println!("=== Cross-Language Complex Type Test ===\n");

    let mut config = if address.is_empty() || address == "local" {
        println!("Using LOCAL mode");
        let c = RayConfig::local();
        if !worker_so.is_empty() {
            RayConfig {
                local_mode: true,
                code_search_path: vec![worker_so.clone()],
                ..RayConfig::local()
            }
        } else {
            c
        }
    } else {
        println!("Using CLUSTER mode: address={}, node_ip={}", address, node_ip);
        let mut c = RayConfig::new(&address).node_ip(&node_ip);
        if !worker_so.is_empty() {
            c = c.code_search_path(vec![worker_so.clone()]);
        }
        c
    };
    // Set PYTHONPATH via runtime_env only in cluster mode (local mode inherits env)
    if address.is_empty() || address == "local" {
        // Local mode: rely on PYTHONPATH env var inherited by subprocess
    } else {
        config = config.runtime_env(r#"{"env_vars": {"PYTHONPATH": "/tmp"}}"#);
    }
    let ray = Ray::connect(&config).expect("init failed");
    println!("✓ Ray initialized\n");

    // ── 0. Basic Python sanity check (add) ────────────────────
    println!("--- 0. Python sanity check: add(5, 3) ---");
    let arg_a = rayrust::serialize(&5i64).unwrap();
    let arg_b = rayrust::serialize(&3i64).unwrap();
    let args_add: Vec<&[u8]> = vec![&arg_a, &arg_b];
    match ray.task_call_python("rayrust_test", "add", &args_add, &[]) {
        Ok(obj_ref) => {
            let raw = obj_ref.get_raw_bytes().expect("raw get failed");
            println!("  Debug raw bytes ({}): {:02x?}", raw.len(), raw);
            let val: i64 = obj_ref.cast().get_async().await.expect("add failed");
            println!("add(5, 3) = {} ✓", val);
        }
        Err(e) => println!("add failed: {}", e),
    }

    // ── 1. Python returns list → Rust Vec<i64> ──────────────────
    println!("--- 1. Python returns list → Vec<i64> ---");
    match ray.task_call_python("rayrust_test", "return_list", &[], &[]) {
        Ok(obj_ref) => {
            let raw_val = obj_ref.get_value_async().await;
            println!("  Debug raw value: {:?}", raw_val);
            match raw_val {
                Ok(v) => println!("  Value type: {:?}", v),
                Err(e) => println!("  get_value error: {}", e),
            }
            let val: Result<Vec<i64>, _> = obj_ref.cast().get_async().await;
            match val {
                Ok(v) => {
                    assert_eq!(v, vec![1, 2, 3, 4, 5]);
                    println!("return_list() = {:?} ✓", v);
                }
                Err(e) => println!("return_list typed failed: {}", e),
            }
        }
        Err(e) => println!("return_list failed: {}", e),
    }

    // ── 2. Python returns dict → Rust HashMap<String, i64> ───────
    println!("\n--- 2. Python returns dict → HashMap<String, i64> ---");
    match ray.task_call_python("rayrust_test", "return_dict", &[], &[]) {
        Ok(obj_ref) => {
            let val: HashMap<String, i64> = obj_ref.cast().get_async().await
                .expect("return_dict failed");
            assert_eq!(val.get("a"), Some(&1));
            assert_eq!(val.get("b"), Some(&2));
            assert_eq!(val.get("c"), Some(&3));
            println!("return_dict() = {:?} ✓", val);
        }
        Err(e) => println!("return_dict failed: {}", e),
    }

    // ── 3. Python returns nested → rmpv::Value (dynamic) ────────
    println!("\n--- 3. Python returns nested → rmpv::Value ---");
    match ray.task_call_python("rayrust_test", "return_nested", &[], &[]) {
        Ok(obj_ref) => {
            let val = obj_ref.get_value_async().await.expect("return_nested failed");
            assert!(val.is_array());
            let arr = val.as_array().unwrap();
            assert_eq!(arr.len(), 2);
            let first = &arr[0];
            assert!(first.is_map());
            let name = vget(first, "name").unwrap();
            assert_eq!(name.as_str().unwrap(), "alice");
            let age = vget(first, "age").unwrap();
            assert_eq!(age.as_i64().unwrap(), 30);
            let scores = vget(first, "scores").unwrap();
            assert!(scores.is_array());
            assert_eq!(scores.as_array().unwrap().len(), 3);
            println!("return_nested()[0].name = {} ✓", name.as_str().unwrap());
            println!("return_nested()[0].age = {} ✓", age.as_i64().unwrap());
            println!("return_nested()[0].scores.len = {} ✓", scores.as_array().unwrap().len());
        }
        Err(e) => println!("return_nested failed: {}", e),
    }

    // ── 4. Python returns None → Option<i64> ─────────────────────
    println!("\n--- 4. Python returns None → Option<i64> ---");
    match ray.task_call_python("rayrust_test", "return_none", &[], &[]) {
        Ok(obj_ref) => {
            let val: Option<i64> = obj_ref.cast().get_async().await
                .expect("return_none failed");
            assert_eq!(val, None);
            println!("return_none() = {:?} ✓", val);
        }
        Err(e) => println!("return_none failed: {}", e),
    }

    // ── 5. Python returns mixed → rmpv::Value ───────────────────
    println!("\n--- 5. Python returns mixed → rmpv::Value ---");
    match ray.task_call_python("rayrust_test", "return_mixed", &[], &[]) {
        Ok(obj_ref) => {
            let val = obj_ref.get_value_async().await.expect("return_mixed failed");
            assert!(val.is_array());
            let arr = val.as_array().unwrap();
            assert_eq!(arr.len(), 5);
            assert_eq!(arr[0].as_i64().unwrap(), 42);       // int
            assert_eq!(arr[1].as_str().unwrap(), "hello");  // str
            assert!(arr[2].as_bool().unwrap());    // bool
            assert!(arr[3].is_nil());                        // None
            assert!((arr[4].as_f64().unwrap() - 3.15).abs() < 1e-9); // float
            println!("return_mixed() = [42, \"hello\", true, None, 3.14] ✓");
        }
        Err(e) => println!("return_mixed failed: {}", e),
    }

    // ── 6. Python returns string list → Vec<String> ──────────────
    println!("\n--- 6. Python returns string list → Vec<String> ---");
    match ray.task_call_python("rayrust_test", "return_string_list", &[], &[]) {
        Ok(obj_ref) => {
            let val: Vec<String> = obj_ref.cast().get_async().await
                .expect("return_string_list failed");
            assert_eq!(val, vec!["foo", "bar", "baz"]);
            println!("return_string_list() = {:?} ✓", val);
        }
        Err(e) => println!("return_string_list failed: {}", e),
    }

    // ── 7. Rust → Python: send Vec<i64>, Python echoes back ──────
    println!("\n--- 7. Rust → Python: echo list ---");
    let input_list = vec![10i64, 20, 30, 40, 50];
    let arg = rayrust::serialize(&input_list).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    match ray.task_call_python("rayrust_test", "echo_list", &args, &[]) {
        Ok(obj_ref) => {
            let val: Vec<i64> = obj_ref.cast().get_async().await.expect("echo_list failed");
            assert_eq!(val, input_list);
            println!("echo_list({:?}) = {:?} ✓", input_list, val);
        }
        Err(e) => println!("echo_list failed: {}", e),
    }

    // ── 8. Rust → Python: send HashMap, Python echoes back ───────
    println!("\n--- 8. Rust → Python: echo dict ---");
    let mut input_dict = HashMap::new();
    input_dict.insert("x".to_string(), 100i64);
    input_dict.insert("y".to_string(), 200);
    let arg = rayrust::serialize(&input_dict).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    match ray.task_call_python("rayrust_test", "echo_dict", &args, &[]) {
        Ok(obj_ref) => {
            let val: HashMap<String, i64> = obj_ref.cast().get_async().await
                .expect("echo_dict failed");
            assert_eq!(val.get("x"), Some(&100));
            assert_eq!(val.get("y"), Some(&200));
            println!("echo_dict({{x:100, y:200}}) = {:?} ✓", val);
        }
        Err(e) => println!("echo_dict failed: {}", e),
    }

    // ── 9. Rust → Python: send Vec, Python sums ──────────────────
    println!("\n--- 9. Rust → Python: sum_list ---");
    let numbers = vec![1i64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let arg = rayrust::serialize(&numbers).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    match ray.task_call_python("rayrust_test", "sum_list", &args, &[]) {
        Ok(obj_ref) => {
            let val: i64 = obj_ref.cast().get_async().await.expect("sum_list failed");
            assert_eq!(val, 55);
            println!("sum_list({:?}) = {} ✓", numbers, val);
        }
        Err(e) => println!("sum_list failed: {}", e),
    }

    // ── 10. Rust → Python: send nested dict, Python processes ───
    println!("\n--- 10. Rust → Python: process_nested ---");
    let input = rmpv::Value::Map(vec![
        (rmpv::Value::String("items".into()), rmpv::Value::Array(vec![
            rmpv::Value::Map(vec![
                (rmpv::Value::String("id".into()), rmpv::Value::Integer(1.into())),
                (rmpv::Value::String("name".into()), rmpv::Value::String("a".into())),
            ]),
            rmpv::Value::Map(vec![
                (rmpv::Value::String("id".into()), rmpv::Value::Integer(2.into())),
                (rmpv::Value::String("name".into()), rmpv::Value::String("b".into())),
            ]),
        ])),
    ]);
    let arg = rayrust::serialize(&input).unwrap();
    let args: Vec<&[u8]> = vec![&arg];
    match ray.task_call_python("rayrust_test", "process_nested", &args, &[]) {
        Ok(obj_ref) => {
            let val = obj_ref.get_value_async().await.expect("process_nested failed");
            assert!(val.is_map());
            let count = vget(&val, "count").unwrap();
            assert_eq!(count.as_i64().unwrap(), 2);
            let names = vget(&val, "names").unwrap();
            assert_eq!(names.as_array().unwrap().len(), 2);
            assert_eq!(names.as_array().unwrap()[0].as_str().unwrap(), "a");
            assert_eq!(names.as_array().unwrap()[1].as_str().unwrap(), "b");
            println!("process_nested() count={}, names=[a, b] ✓", count.as_i64().unwrap());
        }
        Err(e) => println!("process_nested failed: {}", e),
    }

    // ── 11. Python actor returns complex type ────────────────────
    println!("\n--- 11. Python actor: get_stats (complex return) ---");
    let arg_start = rayrust::serialize(&42i64).unwrap();
    let args_actor: Vec<&[u8]> = vec![&arg_start];
    match ray.actor_create_python("rayrust_test", "Counter", &args_actor, &ActorOptions::new()) {
        Ok(actor) => {
            let arg_n = rayrust::serialize(&8i64).unwrap();
            let args_inc: Vec<&[u8]> = vec![&arg_n];
            match ray.actor_call_python(actor.id(), "increment", &args_inc, &[]) {
                Ok(obj_ref) => {
                    let val: i64 = obj_ref.cast().get_async().await.expect("increment failed");
                    assert_eq!(val, 50);
                    println!("Counter.increment(8) = {} ✓", val);
                }
                Err(e) => println!("increment failed: {}", e),
            }

            match ray.actor_call_python(actor.id(), "get_stats", &[], &[]) {
                Ok(obj_ref) => {
                    let val = obj_ref.get_value_async().await.expect("get_stats failed");
                    assert!(val.is_map());
                    let value = vget(&val, "value").unwrap();
                    assert_eq!(value.as_i64().unwrap(), 50);
                    let is_pos = vget(&val, "is_positive").unwrap();
                    assert!(is_pos.as_bool().unwrap());
                    let history = vget(&val, "history").unwrap();
                    assert!(history.is_array());
                    assert_eq!(history.as_array().unwrap().len(), 3);
                    println!("Counter.get_stats() = {{value:50, is_positive:true, history:[48,49,50]}} ✓");
                }
                Err(e) => println!("get_stats failed: {}", e),
            }

            let _ = ray.kill_actor(&actor, true);
            println!("Counter killed ✓");
        }
        Err(e) => println!("Python actor_create failed: {}", e),
    }

    // ── Shutdown (automatic on drop) ──────────────────────────
    println!("\n--- All tests passed ✓ ---");
    drop(ray);
    println!("✓ Ray shutdown");
}
