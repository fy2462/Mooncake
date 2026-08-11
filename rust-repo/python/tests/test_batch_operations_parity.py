"""Wheel parity tests for single-store batch/zero-copy operations.

The reference oracle is mooncake-wheel/tests/test_distributed_object_store.py
(TestDistributedObjectStoreSingleStore + TestZeroLocalBufferSize). These tests
exercise the same API surface against a real Rust client and Rust master.
"""

from __future__ import annotations

import asyncio
import ctypes
import os
import threading
from collections.abc import Iterator

import pytest

import mooncake.store as ms

_real_stores: list[object] = []


def real_store(local_buffer_size: int = 4 * 1024 * 1024) -> ms.MooncakeDistributedStore:
    store = ms.MooncakeDistributedStore()
    rc = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:55052/metadata"),
        64 * 1024 * 1024,
        local_buffer_size,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if rc != 0:
        pytest.skip(f"MooncakeDistributedStore setup failed: {rc}")
    _real_stores.append(store)
    return store


@pytest.fixture(autouse=True)
def _close_real_stores_after_each_test() -> Iterator[None]:
    yield
    while _real_stores:
        store = _real_stores.pop()
        try:
            store.close()
        except Exception:
            pass


def test_cpp_parity_replicate_config_creation_and_properties() -> None:
    # Wheel test_replicate_config_creation_and_properties.
    from mooncake.store import ReplicateConfig

    config = ReplicateConfig()
    assert config.replica_num == 1
    assert config.with_soft_pin is False
    assert config.preferred_segment == ""

    config.replica_num = 3
    config.with_soft_pin = True
    config.preferred_segment = "node1:12345"
    assert config.replica_num == 3
    assert config.with_soft_pin is True
    assert config.preferred_segment == "node1:12345"

    config_str = str(config)
    assert isinstance(config_str, str)
    assert "3" in config_str


def test_cpp_parity_basic_put_get_exist_operations() -> None:
    # Wheel test_basic_put_get_exist_operations.
    store = real_store()
    test_data = b"Hello, World!"
    key = "test_basic_key"

    assert store.put(key, test_data) == 0
    assert store.get_size(key) == len(test_data)
    assert store.get(key) == test_data

    # Duplicate same-key/same-value put succeeds.
    assert store.put(key, test_data) == 0
    assert store.is_exist(key) == 1
    store.remove(key, force=True)


def test_cpp_parity_batch_is_exist_operations() -> None:
    # Wheel test_batch_is_exist_operations: ten of twenty keys exist.
    store = real_store()
    batch_size = 20
    test_data = b"Hello, Batch World!"
    keys = [f"test_batch_exist_key_{i}" for i in range(batch_size)]
    for key in keys[: batch_size // 2]:
        assert store.put(key, test_data) == 0

    results = store.batch_is_exist(keys)
    assert len(results) == batch_size
    assert results[: batch_size // 2] == [1] * (batch_size // 2)
    assert results[batch_size // 2 :] == [0] * (batch_size // 2)

    assert store.batch_is_exist([]) == []
    assert store.batch_is_exist([keys[0]]) == [1]
    assert store.batch_is_exist(["missing_single"]) == [0]

    for key in keys[: batch_size // 2]:
        store.remove(key, force=True)


def test_cpp_parity_batch_get_buffer_operations() -> None:
    # Wheel test_batch_get_buffer_operations.
    store = real_store()
    test_data = [
        b"Batch Buffer Data 1! " * 100,
        b"Batch Buffer Data 2! " * 200,
    ]
    keys = ["test_batch_get_buffer_key1", "test_batch_get_buffer_key2"]
    for key, data in zip(keys, test_data):
        assert store.put(key, data) == 0

    results = store.batch_get_buffer(keys)
    assert len(results) == len(keys)
    for expected_data, buffer in zip(test_data, results):
        assert buffer is not None
        assert len(buffer) == len(expected_data)
        assert bytes(buffer) == expected_data

    mixed = store.batch_get_buffer([keys[0], "non_existent_key"])
    assert len(mixed) == 2
    assert mixed[0] is not None
    assert bytes(mixed[0]) == test_data[0]
    assert mixed[1] is None

    assert store.batch_get_buffer([]) == []
    single = store.batch_get_buffer([keys[0]])
    assert len(single) == 1 and single[0] is not None
    missing = store.batch_get_buffer(["missing_only"])
    assert len(missing) == 1 and missing[0] is None

    for key in keys:
        store.remove(key, force=True)


def test_cpp_parity_client_tear_down() -> None:
    # Wheel test_client_tear_down: close returns zero, reinitialization loses
    # the old key, and the same Store object works again.
    store = real_store()
    test_data = b"Hello, World!"
    key = "test_teardown_key"

    assert store.put(key, test_data) == 0
    assert store.close() == 0

    rc = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:55052/metadata"),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    assert rc == 0
    _real_stores.append(store)

    assert store.get(key) == b""
    assert store.put(key, test_data) == 0
    assert store.get(key) == test_data
    store.remove(key, force=True)


def test_compat_store_without_setup_owns_no_background_loop() -> None:
    store = ms.MooncakeDistributedStore()
    assert store._loop_thread is None

    assert store.close() == 0

    assert store._loop_thread is None


def test_compat_store_failed_setup_stops_background_loop(monkeypatch) -> None:
    class FailingClient:
        @staticmethod
        def create(*args, **kwargs):
            raise RuntimeError("intentional setup failure")

    monkeypatch.setattr(ms, "MooncakeClient", FailingClient)
    store = ms.MooncakeDistributedStore()

    assert store.setup("host", "metadata", 1, 1, "tcp", "", "master") == -1
    assert store._loop is None
    assert store._loop_thread is None


def test_compat_store_close_waits_for_inflight_submission(monkeypatch) -> None:
    operation_started = threading.Event()
    release_operation = threading.Event()

    class FakeClient:
        @staticmethod
        async def create(*args, **kwargs):
            return FakeClient()

        async def get(self, key):
            operation_started.set()
            await asyncio.to_thread(release_operation.wait)
            return b"value"

        async def close(self):
            return 0

    monkeypatch.setattr(ms, "MooncakeClient", FakeClient)
    store = ms.MooncakeDistributedStore()
    assert store.setup("host", "metadata", 1, 1, "tcp", "", "master") == 0
    values = []
    worker = threading.Thread(target=lambda: values.append(store.get("key")))
    closer = threading.Thread(target=store.close)

    worker.start()
    assert operation_started.wait(timeout=1)
    closer.start()
    assert closer.is_alive()
    release_operation.set()
    worker.join(timeout=1)
    closer.join(timeout=1)

    assert not worker.is_alive()
    assert not closer.is_alive()
    assert values == [b"value"]
    assert store._loop is None


def test_cpp_parity_basic_get_hostname() -> None:
    # Wheel test_basic_get_hostname: auto endpoint in localhost:12300..14300.
    store = real_store()
    hostname = store.get_hostname()
    assert hostname.startswith("localhost:")
    port = int(hostname.split(":")[1])
    assert 12300 <= port <= 14300


def _register(store: ms.MooncakeDistributedStore, ptr: int, size: int) -> None:
    assert store.register_buffer(ptr, size) == 0


def test_cpp_parity_batch_put_from_operations() -> None:
    # Wheel test_batch_put_from_operations with spaced registered sources.
    store = real_store()
    batch_size = 3
    test_data = [
        b"Batch Put Data 1! " * 100,
        b"Batch Put Data 2! " * 200,
        b"Batch Put Data 3! " * 150,
    ]
    keys = [f"test_batch_put_from_key_{i}" for i in range(batch_size)]
    buffer_spacing = 1024 * 1024
    large_buffer = (ctypes.c_ubyte * (buffer_spacing * batch_size))()
    large_buffer_ptr = ctypes.addressof(large_buffer)
    _register(store, large_buffer_ptr, buffer_spacing * batch_size)
    try:
        buffer_ptrs = []
        buffer_sizes = []
        for i, data in enumerate(test_data):
            buffer_ptr = large_buffer_ptr + i * buffer_spacing
            ctypes.memmove(ctypes.c_void_p(buffer_ptr), data, len(data))
            buffer_ptrs.append(buffer_ptr)
            buffer_sizes.append(len(data))

        results = store.batch_put_from(keys, buffer_ptrs, buffer_sizes)
        assert len(results) == batch_size
        assert results == [0] * batch_size
        for key, expected_data in zip(keys, test_data):
            assert store.get(key) == expected_data

        mismatch = store.batch_put_from(keys[:2], buffer_ptrs[:3], buffer_sizes[:3])
        assert len(mismatch) == 2
        assert all(result < 0 for result in mismatch)
        # Empty arrays produce empty results.
        assert store.batch_put_from([], [], []) == []
    finally:
        store.unregister_buffer(large_buffer_ptr)
        for key in keys:
            store.remove(key, force=True)


def test_cpp_parity_batch_get_into_operations() -> None:
    # Wheel test_batch_get_into_operations with spaced registered destinations.
    store = real_store()
    batch_size = 3
    test_data = [
        b"Hello, Batch World 1! " * 100,
        b"Hello, Batch World 2! " * 200,
        b"Hello, Batch World 3! " * 150,
    ]
    keys = [f"test_batch_get_into_key_{i}" for i in range(batch_size)]
    for key, data in zip(keys, test_data):
        assert store.put(key, data) == 0

    buffer_spacing = 1024 * 1024
    large_buffer = (ctypes.c_ubyte * (buffer_spacing * batch_size))()
    large_buffer_ptr = ctypes.addressof(large_buffer)
    _register(store, large_buffer_ptr, buffer_spacing * batch_size)
    try:
        buffer_ptrs = [large_buffer_ptr + i * buffer_spacing for i in range(batch_size)]
        buffer_sizes = [buffer_spacing] * batch_size
        results = store.batch_get_into(keys, buffer_ptrs, buffer_sizes)
        assert len(results) == batch_size
        for i, (key, expected_data) in enumerate(zip(keys, test_data)):
            assert results[i] == len(expected_data)
            offset = i * buffer_spacing
            assert (
                bytes(large_buffer[offset : offset + len(expected_data)])
                == expected_data
            )

        mismatch = store.batch_get_into(keys[:2], buffer_ptrs[:3], buffer_sizes[:3])
        assert len(mismatch) == 2
        assert all(result < 0 for result in mismatch)
        assert store.batch_get_into([], [], []) == []
    finally:
        store.unregister_buffer(large_buffer_ptr)
        for key in keys:
            store.remove(key, force=True)


def test_cpp_parity_zero_copy_operations() -> None:
    # Wheel test_zero_copy_operations: registered put_from + get_into round-trip
    # and a registered half-size destination returns negative.
    store = real_store()
    test_data = b"Hello, Zero-Copy World! " * 1000
    key = "test_zero_copy_key"

    buffer_size = len(test_data) + 1024
    buffer = (ctypes.c_ubyte * buffer_size)()
    buffer_ptr = ctypes.addressof(buffer)
    _register(store, buffer_ptr, buffer_size)

    small_size = len(test_data) // 2
    small_buffer = (ctypes.c_ubyte * small_size)()
    small_buffer_ptr = ctypes.addressof(small_buffer)
    _register(store, small_buffer_ptr, small_size)
    try:
        ctypes.memmove(buffer, test_data, len(test_data))
        assert store.put_from(key, buffer_ptr, len(test_data)) == 0
        assert store.get(key) == test_data

        ctypes.memset(buffer, 0, buffer_size)
        bytes_read = store.get_into(key, buffer_ptr, buffer_size)
        assert bytes_read == len(test_data)
        assert bytes(buffer[:bytes_read]) == test_data

        bytes_read = store.get_into(key, small_buffer_ptr, small_size)
        assert bytes_read < 0
    finally:
        store.unregister_buffer(buffer_ptr)
        store.unregister_buffer(small_buffer_ptr)
        store.remove(key, force=True)


def test_cpp_parity_get_into_ranges_operations() -> None:
    # Wheel test_get_into_ranges_operations: buffer-major multi-key range reads
    # into registered buffers with mismatch/overflow/missing-key continuation.
    store = real_store()
    key1 = "test_get_into_ranges_key_1"
    key2 = "test_get_into_ranges_key_2"
    data1 = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    data2 = b"abcdefghijklmnopqrstuvwxyz0123456789"
    buffer_size = 32
    assert store.put(key1, data1) == 0
    assert store.put(key2, data2) == 0

    buffer0 = (ctypes.c_ubyte * buffer_size)()
    buffer1 = (ctypes.c_ubyte * buffer_size)()
    buffer_ptr0 = ctypes.addressof(buffer0)
    buffer_ptr1 = ctypes.addressof(buffer1)
    _register(store, buffer_ptr0, buffer_size)
    _register(store, buffer_ptr1, buffer_size)
    try:
        ctypes.memset(buffer0, ord("_"), buffer_size)
        ctypes.memset(buffer1, ord("_"), buffer_size)

        results = store.get_into_ranges(
            [buffer_ptr0, buffer_ptr1],
            [[key1, key2], [key2, key1]],
            [[[0, 20], [8]], [[4], [16]]],
            [[[1, 30], [2]], [[0], [10]]],
            [[[4, 3], [5]], [[6], [4]]],
        )
        assert results == [[[4, 3], [5]], [[6], [4]]]
        assert bytes(buffer0[0:4]) == data1[1:5]
        assert bytes(buffer0[8:13]) == data2[2:7]
        assert bytes(buffer0[20:23]) == data1[30:33]
        assert bytes(buffer1[4:10]) == data2[0:6]
        assert bytes(buffer1[16:20]) == data1[10:14]

        mismatch = store.get_into_ranges(
            [buffer_ptr0], [[key1, key2]], [[[0], []]], [[[0, 1], []]], [[[4, 4], []]]
        )
        assert len(mismatch) == 1
        assert len(mismatch[0]) == 2
        assert mismatch[0][0][0] < 0
        assert len(mismatch[0][1]) == 0

        source_overflow = store.get_into_ranges(
            [buffer_ptr0], [[key1]], [[[0]]], [[[len(data1) - 1]]], [[[4]]]
        )
        assert source_overflow[0][0][0] < 0

        destination_overflow = store.get_into_ranges(
            [buffer_ptr0], [[key1]], [[[buffer_size - 1]]], [[[0]]], [[[4]]]
        )
        assert destination_overflow[0][0][0] < 0

        missing_key = store.get_into_ranges(
            [buffer_ptr0],
            [["missing-key", key1]],
            [[[0], [8]]],
            [[[0], [0]]],
            [[[4], [4]]],
        )
        assert missing_key[0][0][0] < 0
        assert missing_key[0][1][0] == 4
    finally:
        store.unregister_buffer(buffer_ptr0)
        store.unregister_buffer(buffer_ptr1)
        store.remove(key1, force=True)
        store.remove(key2, force=True)
