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


@pytest.mark.asyncio
async def test_remove_all_empty_store_returns_zero(cachelib_master):
    client = await _client(cachelib_master)
    try:
        assert await client.remove_all() == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_batch_remove_preserves_status_order_and_unselected_key(
    cachelib_master,
):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    try:
        for key in ("brm_1", "brm_2", "brm_3"):
            assert await client.put(key, b"batch_rm_data") == 0
        assert await client.batch_remove(["brm_1", "brm_3"]) == [0, 0]
        assert await client.batch_is_exist(["brm_1", "brm_2", "brm_3"]) == [
            False,
            True,
            False,
        ]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_batch_remove_missing_keys_preserves_cardinality(cachelib_master):
    client = await _client(cachelib_master)
    try:
        results = await client.batch_remove(["never_existed_1", "never_existed_2"])
        assert len(results) == 2
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_put_parts_concatenates_exact_bytes(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    try:
        assert await client.put_parts("put_parts_key", [b"Hello, ", b"Parts!"]) == 0
        value = await client.get_buffer("put_parts_key")
        assert len(value) == len(b"Hello, Parts!")
        assert bytes(value) == b"Hello, Parts!"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_put_batch_then_batch_get_buffer_preserves_order_and_owned_bytes(
    cachelib_master,
):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    keys = ["batch_kv_0", "batch_kv_1", "batch_kv_2"]
    values = [b"value_zero", b"value_one!", b"value_two!"]
    try:
        assert await client.put_batch(keys, values) == 0
        actual = await client.batch_get_buffer(keys)
        assert len(actual) == 3
        assert [bytes(value) for value in actual] == values
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_empty_batch_operation_matrix(cachelib_master):
    client = await _client(cachelib_master)
    try:
        assert await client.put_batch([], []) == 0
        assert await client.batch_get_buffer([]) == []
        assert await client.batch_is_exist([]) == []
        assert await client.batch_remove([]) == []
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_unmount_and_free_unowned_uuid_fails(cachelib_master):
    client = await _client(cachelib_master)
    try:
        with pytest.raises(Exception):
            await client.unmount_and_free_segments(
                ["00000000-0000-0000-0000-000000000000"]
            )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_missing_key_has_single_and_batch_python_error_shapes(cachelib_master):
    client = await _client(cachelib_master)
    pool = BufferPool(client)
    lease = pool.acquire(64)
    destination = lease.buffer
    try:
        assert await client.exists("nonexistent_key") is False
        with pytest.raises(Exception):
            await client.get_buffer("nonexistent_key")
        with pytest.raises(Exception):
            await client.get_size("nonexistent_key")
        with pytest.raises(Exception):
            client.get_into("nonexistent_key", destination)
        assert await client.batch_get_buffer(["no_key_1", "no_key_2", "no_key_3"]) == [
            None,
            None,
            None,
        ]
    finally:
        destination.release()
        lease.release()
        pool.close()
        await client.close()


@pytest.mark.asyncio
async def test_replica_descriptor_and_invalid_batch_mapping(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    key = "mooncake_key"
    try:
        assert (
            await client.put(key, b"It's a test data for get_allocated_buffer_desc.")
            == 0
        )

        replicas = client.get_replica_desc(key)
        assert len(replicas) == 1
        assert replicas[0]["replica_type"] == "Memory"

        batch_replicas = client.batch_get_replica_desc([key])
        assert list(batch_replicas) == [key]
        assert len(batch_replicas[key]) == 1
        assert batch_replicas[key][0]["replica_type"] == "Memory"
        assert client.batch_get_replica_desc(["test_key_1"]) == {}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_register_buffer_rejects_unknown_unregistration_and_duplicate(
    cachelib_master,
):
    client = await _client(cachelib_master)
    unknown = bytearray(64)
    registered = bytearray(1024)
    try:
        with pytest.raises(Exception):
            client.unregister_buffer(unknown)
        assert client.register_buffer(registered, len(registered)) == 0
        with pytest.raises(Exception):
            client.register_buffer(registered, len(registered))
        assert client.unregister_buffer(registered) == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_upsert_batch_returns_aggregate_success_and_exact_ordered_bytes(
    cachelib_master,
):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    keys = ["upsert_batch_0", "upsert_batch_1", "upsert_batch_2"]
    values = [b"value_for_key_0!", b"value_for_key_1!", b"value_for_key_2!"]
    try:
        assert await client.upsert_batch(keys, values) == 0
        assert [bytes(await client.get_buffer(key)) for key in keys] == values
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_upsert_create_and_same_size_replace_returns_visible_result_and_bytes(
    cachelib_master,
):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    key = "upsert_basic_key"
    try:
        assert await client.upsert(key, b"upsert_basic_v1!") == 0
        assert bytes(await client.get_buffer(key)) == b"upsert_basic_v1!"
        assert await client.upsert(key, b"upsert_basic_v2!") == 0
        assert bytes(await client.get_buffer(key)) == b"upsert_basic_v2!"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_upsert_from_replaces_registered_buffer(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    key = "upsert_from_key"
    first = bytearray(b"A" * 64)
    second = bytearray(b"B" * 64)
    try:
        assert client.register_buffer(first, len(first)) == 0
        assert client.upsert_from(key, first, len(first)) == 0
        assert bytes(await client.get_buffer(key)) == bytes(first)

        assert client.register_buffer(second, len(second)) == 0
        assert client.upsert_from(key, second, len(second)) == 0
        assert bytes(await client.get_buffer(key)) == bytes(second)
        client.unregister_buffer(first)
        client.unregister_buffer(second)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_upsert_parts_concatenates_and_replaces_same_size(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    key = "upsert_parts_key"
    try:
        assert await client.upsert_parts(key, [b"Hello, ", b"World!"]) == 0
        assert bytes(await client.get_buffer(key)) == b"Hello, World!"
        assert await client.upsert_parts(key, [b"Goodbye", b"Moon!!"]) == 0
        assert bytes(await client.get_buffer(key)) == b"GoodbyeMoon!!"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_batch_upsert_from_preserves_status_order_and_bytes(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    keys = ["batch_upsert_0", "batch_upsert_1", "batch_upsert_2"]
    buffers = [bytearray(b"X" * 32), bytearray(b"Y" * 32), bytearray(b"Z" * 32)]
    try:
        for buffer in buffers:
            assert client.register_buffer(buffer, len(buffer)) == 0
        assert client.batch_upsert_from(keys, buffers, [32, 32, 32]) == [0, 0, 0]
        assert [bytes(await client.get_buffer(key)) for key in keys] == [
            bytes(buffer) for buffer in buffers
        ]
        partial_statuses = client.batch_upsert_from(
            ["batch_upsert_valid_retry", ""],
            buffers[:2],
            [32, 32],
        )
        assert partial_statuses[0] == 0
        assert partial_statuses[1] < 0
        mismatch_statuses = client.batch_upsert_from(["one", "two"], buffers[:1], [32])
        assert len(mismatch_statuses) == 2
        assert all(status < 0 for status in mismatch_statuses)
        for buffer in buffers:
            client.unregister_buffer(buffer)
    finally:
        await client.close()
