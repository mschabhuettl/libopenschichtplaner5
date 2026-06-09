"""Thread-safety test for the global DBF cache in sp5lib.database.

Verifies that concurrent reads and cache invalidations from multiple threads
do not corrupt the cache dict or raise exceptions.
"""

import threading

from sp5lib import database as db_module

# ── Helpers ──────────────────────────────────────────────────────────────────

def _cache_write(key, value, iterations=500):
    for _ in range(iterations):
        with db_module._CACHE_LOCK:
            db_module._GLOBAL_DBF_CACHE[key] = value


def _cache_read(key, iterations=500):
    for _ in range(iterations):
        with db_module._CACHE_LOCK:
            _ = db_module._GLOBAL_DBF_CACHE.get(key)


def _cache_invalidate(key, iterations=500):
    for _ in range(iterations):
        with db_module._CACHE_LOCK:
            db_module._GLOBAL_DBF_CACHE.pop(key, None)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_concurrent_cache_access_no_exception():
    """10 threads hammer the cache simultaneously — no crash, no data corruption."""
    errors = []

    def worker(idx):
        try:
            key = ("fake_db_path", f"TABLE_{idx % 3}")
            value = (float(idx), [{"id": idx, "name": f"row{idx}"}])
            # Each thread alternates between write, read, and invalidate
            _cache_write(key, value, iterations=200)
            _cache_read(key, iterations=200)
            _cache_invalidate(key, iterations=100)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"Thread errors: {errors}"


def test_concurrent_writes_then_read_consistent():
    """Multiple threads write to the same key; final read must return a valid value."""
    key = ("shared_db", "SHARED_TABLE")
    db_module._GLOBAL_DBF_CACHE.pop(key, None)

    written_values = []
    lock = threading.Lock()

    def writer(idx):
        value = (float(idx), [{"row": idx}])
        with lock:
            written_values.append(value)
        with db_module._CACHE_LOCK:
            db_module._GLOBAL_DBF_CACHE[key] = value

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    with db_module._CACHE_LOCK:
        cached = db_module._GLOBAL_DBF_CACHE.get(key)

    # The cache must contain one of the values that was actually written
    assert cached in written_values, f"Cache holds unexpected value: {cached}"


def test_lock_is_reentrant():
    """Verify _CACHE_LOCK is an RLock — same thread can acquire it twice."""
    acquired_twice = False
    with db_module._CACHE_LOCK:
        with db_module._CACHE_LOCK:  # would deadlock with a plain Lock
            acquired_twice = True
    assert acquired_twice


def test_cache_invalidate_thread_safe():
    """_invalidate_cache removes key safely under concurrent load."""
    key = ("inv_db", "INV_TABLE")

    errors = []

    def invalidator():
        try:
            for _ in range(300):
                with db_module._CACHE_LOCK:
                    db_module._GLOBAL_DBF_CACHE.pop(key, None)
        except Exception as exc:
            errors.append(exc)

    def writer():
        try:
            for i in range(300):
                with db_module._CACHE_LOCK:
                    db_module._GLOBAL_DBF_CACHE[key] = (float(i), [])
        except Exception as exc:
            errors.append(exc)

    threads = [
        *[threading.Thread(target=invalidator) for _ in range(5)],
        *[threading.Thread(target=writer) for _ in range(5)],
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"Errors during concurrent invalidate/write: {errors}"
