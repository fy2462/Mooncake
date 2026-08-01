import os
import socket
import subprocess
import time
from pathlib import Path

import pytest
from mooncake_store import BufferPool, MooncakeClient

SLAB_SIZE = 1 << 24
REPO_ROOT = Path(__file__).resolve().parents[3]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _master_binary() -> Path:
    configured = os.environ.get("MOONCAKE_MASTER_BIN")
    candidates = [
        Path(configured) if configured else None,
        REPO_ROOT / "rust-repo/target/debug/mooncake-master",
        REPO_ROOT / "rust-repo/target/native-parity-classic/debug/mooncake-master",
    ]
    for candidate in candidates:
        if candidate is not None and candidate.is_file():
            return candidate
    pytest.skip("mooncake-master binary is unavailable")


@pytest.fixture
def cachelib_master():
    rpc_port = _free_port()
    metadata_port = _free_port()
    metrics_port = _free_port()
    process = subprocess.Popen(
        [
            str(_master_binary()),
            "--rpc-address",
            "127.0.0.1",
            "--rpc-port",
            str(rpc_port),
            "--http-metadata-server-host",
            "127.0.0.1",
            "--http-metadata-server-port",
            str(metadata_port),
            "--metrics-port",
            str(metrics_port),
            "--memory-allocator",
            "cachelib",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for _ in range(100):
            if process.poll() is not None:
                pytest.fail(process.stdout.read())
            try:
                with socket.create_connection(("127.0.0.1", rpc_port), timeout=0.1):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            pytest.fail("mooncake-master did not open its RPC port")
        yield rpc_port, metadata_port
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


async def _client(
    cachelib_master,
    *,
    global_segment_size: int = 0,
    local_hostname: str = "localhost",
):
    rpc_port, metadata_port = cachelib_master
    return await MooncakeClient.create(
        local_hostname=local_hostname,
        metadata_server=f"http://127.0.0.1:{metadata_port}/metadata",
        master_server_addr=f"127.0.0.1:{rpc_port}",
        protocol="tcp",
        device="",
        global_segment_size=global_segment_size,
        local_buffer_size=16 * 1024 * 1024,
    )


@pytest.mark.asyncio
async def test_allocate_and_mount_segments_rounds_and_frees(cachelib_master):
    client = await _client(cachelib_master)
    try:
        first_ids, first_size = await client.allocate_and_mount_segments(1)
        assert first_ids
        assert first_size == SLAB_SIZE
        await client.unmount_and_free_segments(first_ids)

        second_ids, second_size = await client.allocate_and_mount_segments(
            SLAB_SIZE + 1
        )
        assert second_ids
        assert second_size == 2 * SLAB_SIZE
        await client.unmount_and_free_segments(second_ids)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_allocate_and_mount_segments_rejects_overflow_without_outputs(
    cachelib_master,
):
    client = await _client(cachelib_master)
    segment_ids = []
    allocated_size = 0
    try:
        with pytest.raises(Exception, match="overflows alignment"):
            segment_ids, allocated_size = await client.allocate_and_mount_segments(
                (1 << 64) - 1
            )
        assert segment_ids == []
        assert allocated_size == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_close_then_unmount_and_free_previous_ids_fails(cachelib_master):
    client = await _client(cachelib_master)
    segment_ids, allocated_size = await client.allocate_and_mount_segments(1)
    assert segment_ids
    assert allocated_size > 0

    await client.close()
    with pytest.raises(Exception, match="client already closed"):
        await client.unmount_and_free_segments(segment_ids)


@pytest.mark.asyncio
async def test_put_get_buffer_and_exists_visible_results(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    key = "test_key_realclient"
    value = b"Hello, RealClient!"
    try:
        assert await client.put(key, value) == 0
        retrieved = await client.get_buffer(key)
        assert len(retrieved) == len(value)
        assert bytes(retrieved) == value
        assert await client.exists(key) is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_get_into_accepts_interior_registered_buffer_range(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    value = b"shared-local-buffer-read"
    pool = BufferPool(client)
    lease = pool.acquire(len(value) + 32)
    owner = lease.buffer
    destination = owner[16 : 16 + len(value)]
    try:
        assert await client.put("local_buffer_subrange_key", value) == 0
        assert client.get_into("local_buffer_subrange_key", destination) == len(value)
        assert bytes(destination) == value
    finally:
        destination.release()
        owner.release()
        lease.release()
        pool.close()
        await client.close()


@pytest.mark.asyncio
async def test_get_size_returns_exact_stored_length(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    value = b"getsize_payload_123"
    try:
        assert await client.put("getsize_key", value) == 0
        assert await client.get_size("getsize_key") == len(value)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_health_check_after_create_reports_healthy(cachelib_master):
    client = await _client(cachelib_master)
    try:
        assert await client.health_check() is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_get_hostname_returns_configured_value(cachelib_master):
    client = await _client(cachelib_master, local_hostname="localhost:17813")
    try:
        assert client.get_hostname() == "localhost:17813"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_close_is_idempotent_with_real_binding_client(cachelib_master):
    client = await _client(cachelib_master)
    assert await client.close() == 0
    assert await client.close() == 0


@pytest.mark.asyncio
async def test_duplicate_put_preserves_first_value(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    try:
        assert await client.put("duplicate_put_key", b"first_value") == 0
        assert await client.exists("duplicate_put_key") is True
        assert await client.put("duplicate_put_key", b"second_value_longer") == 0
        assert bytes(await client.get_buffer("duplicate_put_key")) == b"first_value"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_batch_exists_preserves_mixed_input_order(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    try:
        assert await client.put("exist_key_1", b"batch_exist_data") == 0
        assert await client.put("exist_key_2", b"batch_exist_data") == 0
        assert await client.batch_is_exist(
            ["exist_key_1", "missing_key", "exist_key_2", "also_missing"]
        ) == [True, False, True, False]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_remove_by_regex_removes_exact_matches(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    try:
        for key in ("prefix_alpha", "prefix_beta", "other_key"):
            assert await client.put(key, b"regex_data") == 0
        assert await client.remove_by_regex(r"^prefix_.*") == 2
        assert await client.batch_is_exist(
            ["prefix_alpha", "prefix_beta", "other_key"]
        ) == [False, False, True]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_remove_by_regex_no_match_is_zero_and_nonmutating(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    try:
        assert await client.put("some_key", b"no_match_data") == 0
        assert await client.remove_by_regex(r"^nonexistent_pattern_.*") == 0
        assert await client.exists("some_key") is True
    finally:
        await client.close()
