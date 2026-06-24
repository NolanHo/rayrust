#!/usr/bin/env python3
"""Python baseline benchmark for comparison with raybench.

Run on mint-dev:
  PYTHONPATH=/tmp python3 /tmp/pybench.py
"""

import time
import ray

ray.init(address="192.168.42.141:6379", _node_ip_address="192.168.42.106")
print("✓ Ray initialized\n")

# Must import after ray.init for the @ray.remote to register
import rayrust_test


@ray.remote
def add(a, b):
    return a + b


@ray.remote
def async_sum(a, b):
    import asyncio
    asyncio.sleep(0.05)
    return a + b


@ray.remote
def compute(n):
    return sum(range(n))


# Warmup
ray.get(add.remote(1, 1))
print("✓ Warmup done\n")

# ── 1. Python sync throughput (sequential) ──
print("── 1. Python sync (sequential, N=500) ──")
N = 500
t0 = time.monotonic()
s = 0
for i in range(N):
    v = ray.get(add.remote(i, 1))
    s += v
elapsed = time.monotonic() - t0
print(f"   {N} tasks in {elapsed:.3f}s → {N/elapsed:.0f} tasks/sec, {elapsed/N*1000:.2f}ms/task")
print(f"   checksum: {s}\n")

# ── 2. Python async throughput (concurrent) ──
print("── 2. Python async (concurrent, N=500) ──")
N = 500
t0 = time.monotonic()
refs = [add.remote(i, 1) for i in range(N)]
results = ray.get(refs)
s = sum(results)
elapsed = time.monotonic() - t0
print(f"   {N} tasks in {elapsed:.3f}s → {N/elapsed:.0f} tasks/sec, {elapsed/N*1000:.2f}ms/task")
print(f"   checksum: {s}\n")

# ── 3. Latency: single-task round-trip (median of 100) ──
print("── 3. Latency: single-task round-trip (median of 100) ──")
times = []
for _ in range(100):
    t0 = time.monotonic()
    ray.get(add.remote(1, 2))
    times.append(time.monotonic() - t0)
times.sort()
median = times[len(times)//2]
p99 = times[len(times)*99//100]
print(f"   Python→Python: median {median*1000:.2f}ms  p99 {p99*1000:.2f}ms")
print()

# ── 4. Compute-intensive: sum(0..1M) × 10 ──
print("── 4. Compute: sum(0..1_000_000) × 10 tasks ──")
N_TASKS = 10
N_COMPUTE = 1_000_000

t0 = time.monotonic()
refs = [compute.remote(N_COMPUTE) for _ in range(N_TASKS)]
ray.get(refs)
py_elapsed = time.monotonic() - t0
print(f"   Python: {py_elapsed:.3f}s ({N_TASKS/py_elapsed:.0f} tasks/sec)")
print()

# ── 5. Complex type round-trip ──
print("── 5. Complex type round-trip ──")
big_list = [[i * 100 + j for j in range(100)] for i in range(100)]
N_ITER = 50

t0 = time.monotonic()
for _ in range(N_ITER):
    ref = rayrust_test.echo_list.remote(big_list)
    ray.get(ref)
elapsed = time.monotonic() - t0
print(f"   100×100 nested list, {N_ITER} iterations: {elapsed:.3f}s ({elapsed/N_ITER*1000:.2f}ms/iter)")
print()

# ── Summary ──
print("╔══════════════════════════════════════════════════════════╗")
print("║ Python Baseline Summary                                  ║")
print("╠══════════════════════════════════════════════════════════╣")
print(f"║ Python→Python latency: median {median*1000:.2f}ms               ║")
print(f"║ Python compute: {py_elapsed:.3f}s ({N_TASKS/py_elapsed:.0f} tasks/sec)        ║")
print("╚══════════════════════════════════════════════════════════╝")

ray.shutdown()
