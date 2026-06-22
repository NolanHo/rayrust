"""Test module for rayrust cross-language calls.

This module is imported by the Ray Python worker when a Rust driver
calls rayrust::task_call_python("rayrust_test", "add", ...) or
rayrust::actor_create_python("rayrust_test", "Counter", ...).
"""

import ray


@ray.remote
def add(a, b):
    return a + b


@ray.remote
def greet(name):
    return f"Hello, {name} from Python!"


@ray.remote
class Counter:
    def __init__(self, start=0):
        self.value = start

    def increment(self, n=1):
        self.value += n
        return self.value

    def get(self):
        return self.value

    def reset(self):
        self.value = 0
