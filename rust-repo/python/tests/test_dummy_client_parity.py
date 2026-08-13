from __future__ import annotations

import asyncio
import ctypes
import os
from collections.abc import Iterator

import pytest


def _binding():
    return pytest.importorskip("mooncake_store._mooncake_store")


async def _create_dummy(binding):
    real = await binding.MooncakeClient.create(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        16 * 1024 * 1024,
        4 * 1024 * 1024,
    )
    dummy = binding.MooncakeDummyClient.setup_dummy(
        real,
        64 * 1024 * 1024,
        4 * 1024 * 1024,
    )
    return real, dummy


@pytest.fixture
def dummy_client() -> Iterator[object]:
    binding = _binding()
    try:
        real, dummy = asyncio.run(_create_dummy(binding))
    except Exception as error:  # pragma: no cover - depends on CI TE setup
        pytest.skip(f"MooncakeDummyClient setup unavailable: {error}")
    try:
        yield dummy
    finally:
        try:
            asyncio.run(real.close())
        except Exception:  # pragma: no cover
            pass


def test_basic_put_get_exist_operations(dummy_client) -> None:
    test_data = b"Hello, World!"
    key = "test_dummy_basic_key"

    dummy_client.put(key, test_data)
    fetched = dummy_client.get(key)
    assert fetched == test_data
    assert dummy_client.exists(key) is True

    dummy_client.put(key, test_data)
    assert dummy_client.remove(key, False) is None


def test_batch_is_exist_operations(dummy_client) -> None:
    batch_size = 20
    keys = [f"test_dummy_batch_exist_key_{i}" for i in range(batch_size)]
    existing = keys[: batch_size // 2]
    for key in existing:
        dummy_client.put(key, b"Hello, Batch World!")

    results = dummy_client.batch_is_exist(keys)
    assert len(results) == len(keys)
    for i in range(batch_size // 2):
        assert results[i] is True
    for i in range(batch_size // 2, batch_size):
        assert results[i] is False

    assert dummy_client.batch_is_exist([]) == []
    assert dummy_client.batch_is_exist([existing[0]]) == [True]
    assert dummy_client.batch_is_exist(["non_existent_key"]) == [False]


def test_batch_get_into_operations(dummy_client) -> None:
    test_data = [b"Hello, Batch World 1! " * 100, b"Hello, Batch World 2! " * 200]
    keys = [f"test_dummy_get_into_key_{i}" for i in range(len(test_data))]
    for key, data in zip(keys, test_data):
        dummy_client.put(key, data)

    spacing = 1024 * 1024
    base = dummy_client.alloc_from_mem_pool(spacing * len(keys))
    addrs = [base + i * spacing for i in range(len(keys))]
    sizes = [spacing] * len(keys)

    results = dummy_client.batch_get_into(keys, addrs, sizes)
    for i, (data, result) in enumerate(zip(test_data, results)):
        assert result == len(data)
        buf = (ctypes.c_ubyte * spacing).from_address(addrs[i])
        assert bytes(buf[:result]) == data


def test_batch_put_from_operations(dummy_client) -> None:
    test_data = [b"Batch Put Data 1! " * 100, b"Batch Put Data 2! " * 200]
    keys = [f"test_dummy_put_from_key_{i}" for i in range(len(test_data))]

    spacing = 1024 * 1024
    base = dummy_client.alloc_from_mem_pool(spacing * len(keys))
    addrs = [base + i * spacing for i in range(len(keys))]
    for addr, data in zip(addrs, test_data):
        ctypes.memmove(ctypes.c_void_p(addr), data, len(data))

    results = dummy_client.batch_put_from(keys, addrs, [len(d) for d in test_data])
    for result in results:
        assert result == 0
    for key, data in zip(keys, test_data):
        assert dummy_client.get(key) == data


def test_get_into_ranges_operations(dummy_client) -> None:
    key1 = "test_dummy_ranges_key_1"
    key2 = "test_dummy_ranges_key_2"
    data1 = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    data2 = b"abcdefghijklmnopqrstuvwxyz0123456789"
    dummy_client.put(key1, data1)
    dummy_client.put(key2, data2)

    buffer_size = 64
    ptr0 = dummy_client.alloc_from_mem_pool(buffer_size)
    ptr1 = dummy_client.alloc_from_mem_pool(buffer_size)
    buffer0 = (ctypes.c_ubyte * buffer_size).from_address(ptr0)
    buffer1 = (ctypes.c_ubyte * buffer_size).from_address(ptr1)
    ctypes.memset(ptr0, ord("_"), buffer_size)
    ctypes.memset(ptr1, ord("_"), buffer_size)

    results = dummy_client.get_into_ranges(
        [ptr0, ptr1],
        [[key1, key2], [key2, key1]],
        [[[0, 20], [8]], [[4], [16]]],
        [[[2, 30], [10]], [[0], [12]]],
        [[[4, 3], [6]], [[6], [4]]],
    )
    assert results == [[[4, 3], [6]], [[6], [4]]]
    assert bytes(buffer0[0:4]) == data1[2:6]
    assert bytes(buffer0[8:14]) == data2[10:16]
    assert bytes(buffer0[20:23]) == data1[30:33]
    assert bytes(buffer1[4:10]) == data2[0:6]
    assert bytes(buffer1[16:20]) == data1[12:16]


def test_multi_dummy_clients_interaction(dummy_client) -> None:
    first = dummy_client
    binding = _binding()
    real, second = asyncio.run(_create_dummy(binding))
    try:
        first.put("dummy-interaction-key", b"payload-from-first")
        assert second.get("dummy-interaction-key") == b"payload-from-first"
    finally:
        asyncio.run(real.close())
