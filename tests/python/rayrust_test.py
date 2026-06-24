"""Test module for rayrust cross-language calls.

This module is imported by the Ray Python worker when a Rust driver
calls rayrust::task_call_python("rayrust_test", "add", ...) or
rayrust::actor_create_python("rayrust_test", "Counter", ...).

Includes functions that return complex types (lists, dicts, nested
structures) for cross-language serialization testing.
"""

import ray


@ray.remote
def add(a, b):
    return a + b


@ray.remote
def greet(name):
    return f"Hello, {name} from Python!"


@ray.remote
def compute(n):
    """CPU-intensive: sum of 0..n."""
    return sum(range(n))


# ── Complex type return functions ────────────────────────────────

@ray.remote
def return_list():
    """Return a list of integers."""
    return [1, 2, 3, 4, 5]


@ray.remote
def return_dict():
    """Return a dictionary with string keys and integer values."""
    return {"a": 1, "b": 2, "c": 3}


@ray.remote
def return_nested():
    """Return a nested structure: list of dicts."""
    return [
        {"name": "alice", "age": 30, "scores": [90, 85, 92]},
        {"name": "bob", "age": 25, "scores": [78, 88, 95]},
    ]


@ray.remote
def return_none():
    """Return None (msgpack nil)."""
    return None


@ray.remote
def return_mixed():
    """Return a list with mixed types (int, str, bool, None)."""
    return [42, "hello", True, None, 3.14]


@ray.remote
def return_string_list():
    """Return a list of strings."""
    return ["foo", "bar", "baz"]


# ── Complex type argument functions (Rust → Python) ──────────────

@ray.remote
def echo_list(lst):
    """Accept a list and return it unchanged."""
    return lst


@ray.remote
def echo_dict(d):
    """Accept a dict and return it unchanged."""
    return d


@ray.remote
def sum_list(numbers):
    """Accept a list of numbers and return their sum."""
    return sum(numbers)


@ray.remote
def count_words(word_list):
    """Accept a list of strings, return a dict mapping word → count."""
    from collections import Counter
    return dict(Counter(word_list))


@ray.remote
def process_nested(data):
    """Accept a dict with nested structure, return a summary.

    Expected input: {"items": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}
    Returns: {"count": 2, "names": ["a", "b"]}
    """
    items = data["items"]
    return {
        "count": len(items),
        "names": [item["name"] for item in items],
    }


# ── Actor ─────────────────────────────────────────────────────────

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

    def get_stats(self):
        """Return a dict with complex structure."""
        return {
            "value": self.value,
            "is_positive": self.value > 0,
            "history": [self.value - 2, self.value - 1, self.value],
        }
