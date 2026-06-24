import ray, time, json, subprocess

ray.init(
    address="192.168.42.141:6379",
    _node_ip_address="192.168.42.106",
    runtime_env={"env_vars": {"LD_LIBRARY_PATH": "/tmp:/usr/lib:/usr/local/lib"}}
)

# Clean dead nodes
for n in ray.nodes():
    if not n["Alive"]:
        print("Dead: " + n['NodeID'][:16])
        try: ray.kill(n["NodeID"])
        except: pass
time.sleep(1)

alive = [n for n in ray.nodes() if n["Alive"]]
print("Alive nodes: " + str(len(alive)))
for n in alive:
    print("  " + n['NodeManagerAddress'])

# Test Python actor
@ray.remote
class PyCounter:
    def __init__(self, v=0): self.v = v
    def inc(self, n=1): self.v += n; return self.v
    def get(self): return self.v

a = PyCounter.remote(100)
print("Py actor inc(5) = " + str(ray.get(a.inc.remote(5))))
print("Py actor get() = " + str(ray.get(a.get.remote())))
a.kill()
print("Python actor OK")

# Check .so deps on all nodes
for n in alive:
    ip = n['NodeManagerAddress']
    try:
        r = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=3",
             ip, "ldd /tmp/librayrust_worker.so 2>&1 | grep 'not found'"],
            capture_output=True, text=True, timeout=5
        )
        missing = r.stdout.strip()
        if missing:
            print(ip + ": MISSING: " + missing)
        else:
            print(ip + ": all deps OK")
    except Exception as e:
        print(ip + ": unreachable (" + str(e) + ")")

ray.shutdown()
