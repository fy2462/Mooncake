from __future__ import annotations

import asyncio

from mooncake_store.store_adapter import RustBufferPoolAdapter, RustStoreAdapter


class AsyncStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(self, key: str, value: bytes, config=None) -> None:
        await asyncio.sleep(0)
        self.objects[key] = bytes(value)

    async def get(self, key: str) -> bytes:
        await asyncio.sleep(0)
        return self.objects[key]

    async def remove(self, key: str, force: bool = False) -> None:
        await asyncio.sleep(0)
        self.objects.pop(key, None)

    async def batch_remove(self, keys: list[str], force: bool = False) -> None:
        await asyncio.sleep(0)
        for key in keys:
            self.objects.pop(key, None)


class ByteArrayPool:
    def __init__(self) -> None:
        self.released: list[bytearray] = []

    def acquire(
        self, size: int, block: bool = True, timeout: float | None = None
    ) -> bytearray:
        del block, timeout
        return bytearray(size)

    def release(self, buffer: bytearray) -> None:
        self.released.append(buffer)


def test_store_adapter_resolves_awaitables_and_normalizes_mutations() -> None:
    raw = AsyncStore()
    store = RustStoreAdapter(raw)

    assert store.put("key", b"value") == 0
    assert store.get("key") == b"value"
    assert store.remove("key", True) == 0
    assert store.batch_remove(["one", "two"], True) == [0, 0]
    store.close()


def test_store_adapter_works_inside_an_existing_event_loop() -> None:
    async def exercise() -> None:
        with RustStoreAdapter(AsyncStore()) as store:
            assert store.put("key", b"value") == 0
            assert store.get("key") == b"value"

    asyncio.run(exercise())


def test_buffer_pool_adapter_exposes_releasable_pointer_lease() -> None:
    raw = ByteArrayPool()
    pool = RustBufferPoolAdapter(raw)

    lease = pool.acquire(16)
    assert lease.ptr > 0
    assert len(lease.buffer) == 16
    lease.buffer[:] = b"0123456789abcdef"
    lease.release()
    lease.release()

    assert raw.released == [lease.owner]
