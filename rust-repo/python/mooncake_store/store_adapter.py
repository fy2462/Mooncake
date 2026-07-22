"""Synchronous compatibility adapters for the Rust Python bindings."""

from __future__ import annotations

import asyncio
import ctypes
import inspect
import threading
from typing import Any


class RustStoreAdapter:
    """Expose the Rust client's awaitable CRUD methods as blocking calls.

    Structured-object encoding is intentionally synchronous.  Rust's PyO3
    client owns one mutable client internally, so calls are serialized onto a
    dedicated event loop rather than creating a new loop for every chunk.
    """

    _MUTATIONS = frozenset({"put", "put_batch", "remove"})

    def __init__(self, store: Any) -> None:
        self._store = store
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._call_lock = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run_loop,
            name="mooncake-rust-store-adapter",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()
        self._loop.close()

    def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("Rust Store adapter is closed")
        method = getattr(self._store, name)
        with self._call_lock:
            result = method(*args, **kwargs)
            if inspect.isawaitable(result):
                result = asyncio.run_coroutine_threadsafe(result, self._loop).result()
        if name in self._MUTATIONS and result is None:
            return 0
        return result

    def put(self, key: str, value: Any, config: Any = None) -> int:
        return self._call("put", key, bytes(value), config)

    def get(self, key: str) -> bytes:
        return self._call("get", key)

    def remove(self, key: str, force: bool = False) -> int:
        return self._call("remove", key, force)

    def put_batch(self, keys: list[str], values: list[bytes], config: Any = None) -> Any:
        return self._call("put_batch", keys, values, config)

    def batch_remove(self, keys: list[str], force: bool = False) -> Any:
        result = self._call("batch_remove", keys, force)
        return [0] * len(keys) if result is None else result

    def batch_put(self, keys: list[str], values: list[bytes], config: Any = None) -> Any:
        result = self._call("batch_put", keys, values, config)
        return [0] * len(keys) if result is None else result

    def put_from(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("put_from", *args, **kwargs)

    def batch_put_from(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("batch_put_from", *args, **kwargs)

    def get_into(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("get_into", *args, **kwargs)

    def batch_get_into(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("batch_get_into", *args, **kwargs)

    def get_into_ranges(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("get_into_ranges", *args, **kwargs)

    def register_buffer(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("register_buffer", *args, **kwargs)

    def unregister_buffer(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("unregister_buffer", *args, **kwargs)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)

    def __enter__(self) -> "RustStoreAdapter":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class RustBufferLease:
    """C++ BufferPool-compatible lease over a Rust-owned bytearray."""

    def __init__(self, pool: Any, owner: bytearray, size: int) -> None:
        self._pool = pool
        self.owner = owner
        self.buffer = memoryview(owner)[:size]
        self.ptr = ctypes.addressof(ctypes.c_ubyte.from_buffer(owner))
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self.buffer.release()
        self._pool.release(self.owner)

    def __enter__(self) -> "RustBufferLease":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            self.release()
        except Exception:
            pass


class RustBufferPoolAdapter:
    """Convert Rust BufferPool bytearrays into pointer-bearing leases."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    def acquire(
        self, size: int, block: bool = True, timeout: float | None = None
    ) -> RustBufferLease:
        owner = self._pool.acquire(size, block, timeout)
        return RustBufferLease(self._pool, owner, size)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pool, name)


def _is_rust_binding(value: Any) -> bool:
    return type(value).__module__.startswith("mooncake_store._mooncake_store")


def adapt_store(store: Any) -> Any:
    """Wrap only clients provided by this package's Rust extension."""
    if isinstance(store, RustStoreAdapter) or not _is_rust_binding(store):
        return store
    return RustStoreAdapter(store)


def adapt_buffer_pool(pool: Any) -> Any:
    """Wrap only buffer pools provided by this package's Rust extension."""
    if pool is None or isinstance(pool, RustBufferPoolAdapter) or not _is_rust_binding(pool):
        return pool
    return RustBufferPoolAdapter(pool)
