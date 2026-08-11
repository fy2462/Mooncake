"""Rust Store compatibility surface mirroring the C++ wheel's ``mooncake.store``.

The Rust pyo3 client exposes an async ``MooncakeClient.create`` and async data
operations.  This facade keeps the wheel-style synchronous surface
(``MooncakeDistributedStore()`` + ``setup(...)`` + synchronous KV/tensor calls)
on a private event-loop thread, so existing callers and parity tests keep
working unchanged against the Rust store.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from typing import Any, Callable

from mooncake_store import (
    BufferLease,
    BufferPool,
    EngramStore,
    EngramStoreConfig,
    MooncakeClient,
    P2pStore,
    RegisteredBufferLease,
    RegisteredBufferPool,
    ReplicateConfig,
    RemoteSourceConfig,
    S3Config,
    StoreError,
    _deserialize_tensor,
    _serialize_tensor,
    _tensor_metadata_size,
)

__all__ = [
    "MooncakeDistributedStore",
    "ReplicateConfig",
    "StoreError",
    "EngramStore",
    "EngramStoreConfig",
    "P2pStore",
    "S3Config",
    "RemoteSourceConfig",
    "BufferPool",
    "RegisteredBufferPool",
    "BufferLease",
    "RegisteredBufferLease",
    "_serialize_tensor",
    "_deserialize_tensor",
    "_tensor_metadata_size",
    "enable_te_debug",
    "setup_te_debug_logging",
]

_logger = logging.getLogger("mooncake.store")


class MooncakeDistributedStore:
    """Rust-backed synchronous facade matching the C++ wheel client surface."""

    def __init__(self) -> None:
        self._store: MooncakeClient | None = None
        self._lifecycle_lock = threading.RLock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        # Start the private loop lazily on the first submitted operation. A
        # merely constructed facade therefore owns no background thread.

    def _start_loop(self) -> asyncio.AbstractEventLoop:
        with self._lifecycle_lock:
            if self._loop is not None:
                return self._loop
            loop = asyncio.new_event_loop()
            loop_ready = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.call_soon(loop_ready.set)
                loop.run_forever()

            loop_thread = threading.Thread(
                target=run_loop,
                name="mooncake-store-compat-loop",
                daemon=True,
            )
            loop_thread.start()
            loop_ready.wait()
            self._loop = loop
            self._loop_thread = loop_thread
            return loop

    def _stop_loop(self) -> None:
        with self._lifecycle_lock:
            loop, self._loop = self._loop, None
            loop_thread, self._loop_thread = self._loop_thread, None
        if loop is None:
            return
        loop.call_soon_threadsafe(loop.stop)
        if loop_thread is not None:
            loop_thread.join()
        loop.close()

    def setup(
        self,
        local_hostname: str,
        metadata_server: str,
        global_size: int,
        local_size: int,
        protocol: str,
        device_name: str,
        master_server: str,
    ) -> int:
        """Initialize the Rust client; returns zero on success, nonzero on failure."""

        with self._lifecycle_lock:
            try:
                client = self._submit(
                    lambda: MooncakeClient.create(
                        local_hostname,
                        metadata_server,
                        master_server,
                        protocol,
                        device_name,
                        int(global_size),
                        int(local_size),
                    )
                )
            except Exception:
                _logger.exception("MooncakeDistributedStore setup failed")
                self._stop_loop()
                return -1
            self._store = client
            return 0

    def close(self) -> int:
        with self._lifecycle_lock:
            client = self._store
            try:
                status = (
                    0 if client is None else int(self._submit(lambda: client.close()))
                )
            except Exception:
                _logger.exception("MooncakeDistributedStore close failed")
                return -1
            if status == 0:
                self._store = None
                self._stop_loop()
            return status

    def get_tensor_into(self, *args: Any, **kwargs: Any) -> Any:
        return self._sync("get_tensor_into")(*args, **kwargs)

    def put(self, key: str, value: Any, config: Any = None) -> Any:
        """Store a value, converting buffer-protocol inputs to bytes."""
        if not isinstance(value, bytes):
            value = bytes(value)
        result = self._sync("put")(key, value, config)
        # The C++ wheel contract reports success as 0; the Rust pyo3 method
        # returns None/() on success.  Normalize so callers checking status work.
        return 0 if result in (None, ()) else result

    def _sync(self, name: str) -> Callable[..., Any]:
        def call(*args: Any, **kwargs: Any) -> Any:
            with self._lifecycle_lock:
                store = self._store
                if store is None:
                    raise RuntimeError("MooncakeDistributedStore is not set up")
                method = getattr(store, name)
                return self._submit(lambda: method(*args, **kwargs))

        return call

    def _submit(self, fn: Callable[[], Any]) -> Any:
        """Run *fn* on the persistent loop and block for its result."""

        async def run() -> Any:
            result = fn()
            if inspect.isawaitable(result):
                result = await result
            return result

        with self._lifecycle_lock:
            loop = self._start_loop()
            return asyncio.run_coroutine_threadsafe(run(), loop).result()

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return self._sync(name)

    def __del__(self) -> None:
        # Finalization cannot safely await an async client close, but it must
        # not leave the compatibility loop thread running forever.
        try:
            self._stop_loop()
        except Exception:
            pass


def setup_te_debug_logging(level: int = logging.DEBUG) -> None:
    """Enable detailed transfer-engine operation traces (Rust + Python layers)."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[PY_TE_DEBUG] %(asctime)s %(thread)d %(name)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    _logger.addHandler(handler)
    _logger.setLevel(level)
    try:
        from mooncake_store import _mooncake_store

        _mooncake_store.enable_te_debug_tracing()
    except Exception:
        pass  # function may not exist in older builds


def enable_te_debug() -> None:
    """Enable all TE debug logging (Rust + Python layers)."""
    setup_te_debug_logging()
