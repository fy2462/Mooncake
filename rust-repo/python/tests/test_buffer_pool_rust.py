from __future__ import annotations

import asyncio
import gc
import os
import threading
import time
from collections.abc import Iterator

import pytest


def _binding():
    return pytest.importorskip("mooncake_store._mooncake_store")


async def _create_client(binding):
    return await binding.MooncakeClient.create(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        16 * 1024 * 1024,
        4 * 1024 * 1024,
    )


@pytest.fixture
def rust_client() -> Iterator[object]:
    binding = _binding()
    try:
        client = asyncio.run(_create_client(binding))
    except Exception as error:
        pytest.skip(f"Rust MooncakeClient setup unavailable: {error}")
    try:
        yield client
    finally:
        asyncio.run(client.close())


def test_registered_pool_and_lease_are_public_aliases() -> None:
    binding = _binding()
    assert binding.RegisteredBufferPool is binding.BufferPool
    assert binding.RegisteredBufferLease is binding.BufferLease


def test_buffer_lease_is_aligned_writable_and_idempotent(rust_client) -> None:
    binding = _binding()
    pool = binding.BufferPool(
        rust_client,
        1024 * 1024,
        min_size_class=4096,
        alignment=4096,
    )
    lease = pool.acquire(1234)
    assert lease.size == 1234
    assert lease.ptr % 4096 == 0
    view = lease.buffer
    view[:4] = b"rust"
    assert bytes(view[:4]) == b"rust"
    view.release()
    assert pool.borrowed_bytes == 1234
    lease.release()
    lease.release()
    assert pool.borrowed_bytes == 0
    pool.close()


def test_release_rejects_exported_views(rust_client) -> None:
    binding = _binding()
    pool = binding.BufferPool(rust_client, 1024 * 1024, alignment=4096)
    lease = pool.acquire(4)
    view = lease.buffer
    with pytest.raises(RuntimeError, match="exported views"):
        lease.release()
    view.release()
    lease.release()
    pool.close()


def test_nonblocking_exhaustion_and_active_close(rust_client) -> None:
    binding = _binding()
    pool = binding.BufferPool(
        rust_client,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
        max_regions=1,
        block_on_exhaustion=False,
    )
    lease = pool.acquire(1)
    with pytest.raises(RuntimeError, match="exhausted"):
        pool.acquire(1)
    with pytest.raises(RuntimeError, match="active leases"):
        pool.close()
    lease.release()
    pool.close()


def test_blocking_acquire_releases_python_thread_and_honors_timeout(
    rust_client,
) -> None:
    binding = _binding()
    pool = binding.BufferPool(
        rust_client,
        min_size_class=4096,
        max_size_class=4096,
        alignment=4096,
        max_regions=1,
    )
    first = pool.acquire(1)
    acquired: list[object] = []

    def acquire_after_release() -> None:
        acquired.append(pool.acquire(1, timeout=1.0))

    worker = threading.Thread(target=acquire_after_release)
    worker.start()
    time.sleep(0.05)
    first.release()
    worker.join(timeout=5)
    assert not worker.is_alive()
    acquired.pop().release()

    held = pool.acquire(1)
    with pytest.raises(RuntimeError, match="timed out"):
        pool.acquire(1, timeout=0.01)
    held.release()
    pool.close()


def test_lease_destructor_unregisters_region(rust_client) -> None:
    binding = _binding()
    pool = binding.BufferPool(rust_client, 1024 * 1024, alignment=4096)
    lease = pool.acquire(1)
    assert pool.borrowed_bytes == 1
    del lease
    gc.collect()
    assert pool.borrowed_bytes == 0
    pool.close()
