import asyncio
import concurrent.futures
import os
import random
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
from mooncake_store import (
    BufferPool,
    ClassicTransferEngine,
    MooncakeClient,
    ReplicateConfig,
    StoreError,
)
from mooncake.store import MooncakeDistributedStore

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
def cachelib_master(request):
    rpc_port = _free_port()
    metadata_port = _free_port()
    metrics_port = _free_port()
    fixture_config = getattr(request, "param", None)
    if isinstance(fixture_config, dict):
        lease_ttl_ms = fixture_config.get("lease_ttl_ms")
        memory_allocator = fixture_config.get("memory_allocator", "cachelib")
    else:
        lease_ttl_ms = fixture_config
        memory_allocator = "cachelib"
    command = [
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
        memory_allocator,
    ]
    if lease_ttl_ms is not None:
        command.extend(["--default-kv-lease-ttl-ms", str(lease_ttl_ms)])
    process = subprocess.Popen(
        command,
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
    local_buffer_size: int = 16 * 1024 * 1024,
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
        local_buffer_size=local_buffer_size,
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
async def test_segment_unmount_apis_reject_foreign_ownership(
    cachelib_master, tmp_path, monkeypatch
):
    """Match RealClientTest.MountAndAllocateUnmountApisRejectForeignSegments."""
    backing_file = tmp_path / "ownership-segment.bin"
    backing_file.write_bytes(b"\0" * SLAB_SIZE)
    monkeypatch.setenv("MC_MAX_MR_SIZE", str(SLAB_SIZE))

    client = await _client(cachelib_master)
    try:
        owned_ids, allocated_size = await client.allocate_and_mount_segments(1)
        assert owned_ids and allocated_size == SLAB_SIZE
        # Allocator-owned UUIDs are not accepted by plain file unmount.
        assert await client.unmount_segments(owned_ids) != 0

        status, file_ids = await client.mount_file_segments(
            str(backing_file), 0, SLAB_SIZE, "tcp", ""
        )
        assert status == 0 and len(file_ids) == 1
        # File-mounted UUIDs are not accepted by allocator release.
        with pytest.raises(Exception):
            await client.unmount_and_free_segments(file_ids)

        await client.unmount_and_free_segments(owned_ids)
        assert await client.unmount_segments(file_ids) == 0
        backing_file.unlink()
        assert not backing_file.exists()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_segment_unmount_apis_accept_grace_period(
    cachelib_master, tmp_path, monkeypatch
):
    """Match RealClientTest.MountAndAllocateUnmountApisAcceptGracePeriod."""
    backing_file = tmp_path / "grace-segment.bin"
    backing_file.write_bytes(b"\0" * SLAB_SIZE)
    monkeypatch.setenv("MC_MAX_MR_SIZE", str(SLAB_SIZE))

    client = await _client(cachelib_master)
    try:
        owned_ids, _ = await client.allocate_and_mount_segments(1)
        # This binding keeps the historical millisecond parameter; one C++
        # grace second is therefore represented as 1000 ms. Success returns
        # None (the underlying StoreResult<()>), not a status integer.
        assert await client.unmount_and_free_segments(owned_ids, 1000) is None

        status, file_ids = await client.mount_file_segments(
            str(backing_file), 0, SLAB_SIZE, "tcp", ""
        )
        assert status == 0 and len(file_ids) == 1
        assert await client.unmount_segments(file_ids, 1) == 0
        backing_file.unlink()
        assert not backing_file.exists()
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


@pytest.mark.parametrize(
    "cachelib_master",
    [{"lease_ttl_ms": 20, "memory_allocator": "offset"}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_cpp_parity_put_with_config_parameter(cachelib_master):
    client1 = await _client(
        cachelib_master,
        global_segment_size=SLAB_SIZE,
        local_hostname="localhost:12345",
    )
    client2 = await _client(
        cachelib_master,
        global_segment_size=SLAB_SIZE,
        local_hostname="localhost:12346",
    )
    value = b"Hello, Config World!"
    try:
        key = "test_put_config_key"
        assert await client1.put(key=key, value=value) == 0
        assert await client1.get(key) == value
        await asyncio.sleep(0.05)
        assert await client1.remove(key) == 0

        config = ReplicateConfig(replica_num=2)
        key2 = "test_put_config_key2"
        assert await client1.put(key=key2, value=value, config=config) == 0
        assert await client1.get(key2) == value
        await asyncio.sleep(0.05)
        assert await client1.remove(key2) == 0

        with pytest.raises(TypeError):
            client1.put(key_arg_name_error=key, value=value, config=config)
        with pytest.raises(TypeError):
            client1.put(key=key, value_arg_name_error=value, config=config)
        with pytest.raises(TypeError):
            client1.put(key=key, value=value, config_arg_name_error=config)
    finally:
        await client2.close()
        await client1.close()


@pytest.mark.parametrize(
    "cachelib_master",
    [{"lease_ttl_ms": 20, "memory_allocator": "offset"}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_cpp_parity_put_batch_with_config_parameter(cachelib_master):
    client1 = await _client(
        cachelib_master,
        global_segment_size=SLAB_SIZE,
        local_hostname="localhost:12345",
    )
    client2 = await _client(
        cachelib_master,
        global_segment_size=SLAB_SIZE,
        local_hostname="localhost:12346",
    )
    values = [b"Batch Data 1", b"Batch Data 2", b"Batch Data 3"]
    try:
        keys = [
            "test_batch_config_key1",
            "test_batch_config_key2",
            "test_batch_config_key3",
        ]
        assert await client1.put_batch(keys, values) == 0
        for key, value in zip(keys, values):
            assert await client1.get(key) == value
        await asyncio.sleep(0.05)
        for key in keys:
            assert await client1.remove(key) == 0

        keys2 = [
            "test_batch_config_key4",
            "test_batch_config_key5",
            "test_batch_config_key6",
        ]
        config = ReplicateConfig(replica_num=2)
        assert await client1.put_batch(keys=keys2, values=values, config=config) == 0
        for key, value in zip(keys2, values):
            assert await client1.get(key) == value
        await asyncio.sleep(0.05)
        for key in keys2:
            assert await client1.remove(key) == 0
    finally:
        await client2.close()
        await client1.close()


@pytest.mark.parametrize(
    "cachelib_master",
    [{"lease_ttl_ms": 200, "memory_allocator": "offset"}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_cpp_parity_replication_failure_tolerance(cachelib_master):
    main_hostname = "localhost:12345"
    replica_hostname = "localhost:12346"
    client1 = await _client(
        cachelib_master,
        global_segment_size=SLAB_SIZE,
        local_hostname=main_hostname,
    )
    client2 = await _client(
        cachelib_master,
        global_segment_size=SLAB_SIZE,
        local_hostname=replica_hostname,
    )
    value = b"Replicated failure tolerance test data!"
    key = "test_replication_failure_key"
    try:
        config = ReplicateConfig(replica_num=2)
        assert await client1.put(key=key, value=value, config=config) == 0
        assert await client1.get(key) == value

        assert await client1.close() == 0
        client1 = None
        await asyncio.sleep(1)
        assert await client2.get(key) == value

        client1 = await _client(
            cachelib_master,
            global_segment_size=SLAB_SIZE,
            local_hostname=main_hostname,
        )
        assert await client1.get(key) == value
        await asyncio.sleep(0.25)
        assert await client1.remove(key) == 0
    finally:
        await client2.close()
        if client1 is not None:
            await client1.close()


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
        assert await client.health_check() == 0
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


@pytest.mark.parametrize(
    "cachelib_master",
    [{"lease_ttl_ms": 20, "memory_allocator": "offset"}],
    indirect=True,
)
def test_cpp_parity_concurrent_stress_with_barrier(cachelib_master):
    num_threads = 8
    operations_per_thread = 100
    value_size = 1024 * 1024
    rpc_port, metadata_port = cachelib_master
    store = MooncakeDistributedStore()
    assert (
        store.setup(
            "localhost",
            f"http://127.0.0.1:{metadata_port}/metadata",
            3200 * 1024 * 1024,
            512 * 1024 * 1024,
            "tcp",
            "",
            f"127.0.0.1:{rpc_port}",
        )
        == 0
    )

    start_barrier = threading.Barrier(num_threads + 1)
    put_barrier = threading.Barrier(num_threads + 1)
    get_barrier = threading.Barrier(num_threads + 1)
    thread_exceptions: list[str] = []

    def abort_barriers() -> None:
        for barrier in (start_barrier, put_barrier, get_barrier):
            try:
                barrier.abort()
            except threading.BrokenBarrierError:
                pass

    def worker(thread_id: int) -> None:
        try:
            value = os.urandom(value_size)
            keys = [
                f"key_{thread_id}_{index}" for index in range(operations_per_thread)
            ]
            start_barrier.wait(timeout=30)
            for key in keys:
                assert store.put(key, value) == 0
            put_barrier.wait(timeout=300)
            for key in keys:
                retrieved = store.get(key)
                assert len(retrieved) == value_size
                assert retrieved == value
            get_barrier.wait(timeout=300)
            time.sleep(0.05)
            for key in keys:
                assert store.remove(key) == 0
        except Exception as error:
            thread_exceptions.append(f"Thread {thread_id} failed: {error}")
            abort_barriers()

    threads = [
        threading.Thread(target=worker, args=(index,), name=f"Worker-{index}")
        for index in range(num_threads)
    ]
    try:
        for thread in threads:
            thread.start()
        for barrier in (start_barrier, put_barrier, get_barrier):
            try:
                barrier.wait(timeout=300)
            except threading.BrokenBarrierError:
                break
        for thread in threads:
            thread.join(timeout=300)
        assert all(not thread.is_alive() for thread in threads)
        assert thread_exceptions == []
    finally:
        abort_barriers()
        for thread in threads:
            thread.join(timeout=5)
        store.close()


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


@pytest.mark.asyncio
async def test_multi_buffer_batch_put_get_preserves_partition_order_and_bytes(
    cachelib_master,
):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    source = bytearray(b"1" * 1000)
    destination = bytearray(b"0" * 1000)
    source_view = memoryview(source)
    destination_view = memoryview(destination)
    source_parts = [source_view[offset : offset + 10] for offset in range(0, 1000, 10)]
    destination_parts = [
        destination_view[offset : offset + 10] for offset in range(0, 1000, 10)
    ]
    keys = [f"test_key_{index}" for index in range(10)]
    all_sizes = [[10] * 10 for _ in keys]
    try:
        assert client.register_buffer(source, len(source)) == 0
        assert client.register_buffer(destination, len(destination)) == 0
        put_statuses = client.batch_put_from_multi_buffers(
            keys,
            [source_parts[index * 10 : (index + 1) * 10] for index in range(10)],
            all_sizes,
        )
        assert put_statuses == [0] * 10
        get_statuses = client.batch_get_into_multi_buffers(
            keys,
            [destination_parts[index * 10 : (index + 1) * 10] for index in range(10)],
            all_sizes,
            True,
        )
        assert get_statuses == [100] * 10
        assert destination == source
        assert client.unregister_buffer(source) == 0
        assert client.unregister_buffer(destination) == 0
    finally:
        for part in source_parts + destination_parts:
            part.release()
        source_view.release()
        destination_view.release()
        await client.close()


@pytest.mark.parametrize("cachelib_master", [1], indirect=True)
@pytest.mark.asyncio
async def test_remove_then_get_is_absent_and_repeat_remove_is_safe(cachelib_master):
    client = await _client(cachelib_master, global_segment_size=SLAB_SIZE)
    key = "remove_then_get_key"
    try:
        assert await client.put(key, b"some_data_to_remove") == 0
        assert await client.exists(key) is True
        await asyncio.sleep(0.01)
        assert await client.remove(key) == 0
        assert await client.exists(key) is False
        with pytest.raises(Exception):
            await client.get_buffer(key)
        try:
            await client.remove(key)
        except Exception:
            pass
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_create_rejects_unreachable_master_invalid_protocol_and_empty_hostname(
    cachelib_master,
):
    rpc_port, metadata_port = cachelib_master
    base = {
        "local_hostname": "localhost",
        "metadata_server": f"http://127.0.0.1:{metadata_port}/metadata",
        "master_server_addr": f"127.0.0.1:{rpc_port}",
        "protocol": "tcp",
        "device": "",
        "global_segment_size": SLAB_SIZE,
        "local_buffer_size": SLAB_SIZE,
    }

    with pytest.raises(Exception) as unreachable_error:
        await asyncio.wait_for(
            MooncakeClient.create(**{**base, "master_server_addr": "192.0.2.1:1"}),
            timeout=10,
        )
    assert not isinstance(unreachable_error.value, asyncio.TimeoutError)
    with pytest.raises(Exception):
        await MooncakeClient.create(**{**base, "protocol": "invalid_protocol"})
    with pytest.raises(Exception):
        await MooncakeClient.create(**{**base, "local_hostname": ""})


@pytest.mark.parametrize(
    "cachelib_master",
    [{"lease_ttl_ms": 1, "memory_allocator": "offset"}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_lease_expiry_preserves_single_and_batch_failure_shapes(
    cachelib_master,
):
    data_size = 256 * 1024 * 1024
    segment_size = 512 * 1024 * 1024
    source = bytearray(b"A" * data_size)
    source_view = memoryview(source)
    client = await _client(
        cachelib_master,
        global_segment_size=segment_size,
        local_buffer_size=segment_size,
    )
    batch_views = []
    try:
        assert client.register_buffer(source, len(source)) == 0

        key = "test_key_realclient"
        assert await client.put(key, bytes(source)) == 0
        with pytest.raises(Exception):
            await client.get_buffer(key)
        with pytest.raises(Exception):
            client.get_into(key, source)
        await asyncio.sleep(0.01)
        assert await client.remove(key) == 0

        keys = [f"batch_test_key_{index}" for index in range(128)]
        slice_size = data_size // len(keys)
        batch_views = [
            source_view[offset : offset + slice_size]
            for offset in range(0, data_size, slice_size)
        ]
        values = [bytes(view) for view in batch_views]
        assert await client.put_batch(keys, values) == 0

        handles = await client.batch_get_buffer(keys)
        assert len(handles) == 128
        assert any(handle is None for handle in handles)

        bytes_read = client.batch_get_into(keys, batch_views, [slice_size] * len(keys))
        assert len(bytes_read) == 128
        assert any(result < 0 for result in bytes_read)

        await asyncio.sleep(0.01)
        assert await client.remove_all() == 128
        assert client.unregister_buffer(source) == 0
    finally:
        for view in batch_views:
            view.release()
        source_view.release()
        await client.close()


@pytest.mark.parametrize(
    "cachelib_master",
    [{"lease_ttl_ms": 1, "memory_allocator": "offset"}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_concurrent_expiring_reads_never_return_stale_bytes(cachelib_master):
    segment_size = 16 * 1024 * 1024
    num_threads = 4
    num_iterations = 100
    client = await _client(
        cachelib_master,
        global_segment_size=segment_size,
        local_buffer_size=segment_size,
    )

    def run_workers(worker):
        barrier = threading.Barrier(num_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as pool:
            futures = [
                pool.submit(worker, index, barrier) for index in range(num_threads)
            ]
            for future in futures:
                future.result(timeout=180)

    def single_worker(thread_index, barrier):
        slice_size = segment_size // num_threads + 1024
        key = f"concurrent_test_key_{thread_index}"
        put_data = bytes([ord("a") + thread_index]) * slice_size
        get_data = bytearray(slice_size)
        assert client.register_buffer(get_data, len(get_data)) == 0
        barrier.wait(timeout=30)

        async def exercise():
            rng = random.Random(0x51A9 + thread_index)
            for _ in range(num_iterations):
                try:
                    await client.put(key, put_data)
                except StoreError:
                    await asyncio.sleep(0)
                if rng.randrange(2) == 0:
                    try:
                        value = await client.get_buffer(key)
                    except StoreError:
                        value = None
                    if value is not None:
                        assert len(value) == slice_size
                        assert bytes(value) == put_data
                else:
                    try:
                        bytes_read = client.get_into(key, get_data)
                    except StoreError:
                        bytes_read = -1
                    if bytes_read > 0:
                        assert bytes_read == slice_size
                        assert bytes(get_data) == put_data
                await asyncio.sleep(0.000001)

        try:
            asyncio.run(exercise())
        finally:
            assert client.unregister_buffer(get_data) == 0

    def batch_worker(thread_index, barrier):
        num_slices = 32
        slice_size = segment_size // (num_threads * num_slices) + 1024
        data_size = num_slices * slice_size
        put_data = random.Random(0xDA7A + thread_index).randbytes(data_size)
        keys = [
            f"batch_concurrent_key_{thread_index}_{slice_index}"
            for slice_index in range(num_slices)
        ]
        values = [
            put_data[offset : offset + slice_size]
            for offset in range(0, data_size, slice_size)
        ]
        get_data = bytearray(data_size)
        assert client.register_buffer(get_data, len(get_data)) == 0
        get_views = [
            memoryview(get_data)[offset : offset + slice_size]
            for offset in range(0, data_size, slice_size)
        ]
        barrier.wait(timeout=30)

        async def exercise():
            rng = random.Random(0xBA7C + thread_index)
            for _ in range(num_iterations):
                try:
                    await client.put_batch(keys, values)
                except StoreError:
                    await asyncio.sleep(0)
                if rng.randrange(2) == 0:
                    handles = await client.batch_get_buffer(keys)
                    for index, handle in enumerate(handles):
                        if handle is not None:
                            assert len(handle) == slice_size
                            assert bytes(handle) == values[index]
                else:
                    results = client.batch_get_into(
                        keys, get_views, [slice_size] * num_slices
                    )
                    for index, bytes_read in enumerate(results):
                        if bytes_read > 0:
                            assert bytes_read == slice_size
                            assert bytes(get_views[index]) == values[index]
                await asyncio.sleep(0.000001)

        try:
            asyncio.run(exercise())
        finally:
            for view in get_views:
                view.release()
            assert client.unregister_buffer(get_data) == 0

    try:
        await asyncio.to_thread(run_workers, single_worker)
        await asyncio.sleep(0.001)
        try:
            await client.remove_all()
        except StoreError:
            await asyncio.sleep(0)

        await asyncio.to_thread(run_workers, batch_worker)
        await asyncio.sleep(0.001)
        try:
            await client.remove_all()
        except StoreError:
            await asyncio.sleep(0)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_create_accepts_initialized_external_transfer_engine(cachelib_master):
    rpc_port, _ = cachelib_master
    metadata_server = "P2PHANDSHAKE"
    local_hostname = "localhost:17813"
    engine = ClassicTransferEngine(
        metadata_server=metadata_server,
        local_hostname=local_hostname,
        protocol="tcp",
    )
    client = await MooncakeClient.create_with_transfer_engine(
        transfer_engine=engine,
        local_hostname=local_hostname,
        metadata_server=metadata_server,
        master_server_addr=f"127.0.0.1:{rpc_port}",
        protocol="tcp",
        global_segment_size=SLAB_SIZE,
        local_buffer_size=SLAB_SIZE,
    )
    try:
        assert await client.put("test_key_external_te", b"Hello, RealClient!") == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_external_transfer_engine_rejects_mismatched_identity(cachelib_master):
    rpc_port, metadata_port = cachelib_master
    metadata_server = f"http://127.0.0.1:{metadata_port}/metadata"
    engine = ClassicTransferEngine(
        metadata_server=metadata_server,
        local_hostname="localhost:17814",
        protocol="tcp",
    )

    common = dict(
        transfer_engine=engine,
        local_hostname="localhost:17814",
        metadata_server=metadata_server,
        master_server_addr=f"127.0.0.1:{rpc_port}",
        protocol="tcp",
        global_segment_size=SLAB_SIZE,
        local_buffer_size=SLAB_SIZE,
    )

    with pytest.raises(StoreError, match="external Transfer Engine local endpoint"):
        await MooncakeClient.create_with_transfer_engine(
            local_hostname="localhost:17815",
            **{key: value for key, value in common.items() if key != "local_hostname"},
        )
    with pytest.raises(StoreError, match="metadata connection"):
        await MooncakeClient.create_with_transfer_engine(
            metadata_server=f"{metadata_server}-other",
            **{key: value for key, value in common.items() if key != "metadata_server"},
        )
    with pytest.raises(StoreError, match="external Transfer Engine protocol"):
        await MooncakeClient.create_with_transfer_engine(
            protocol="rdma",
            **{key: value for key, value in common.items() if key != "protocol"},
        )


@pytest.mark.asyncio
async def test_external_transfer_engine_survives_pre_mount_setup_failure(
    cachelib_master, monkeypatch
):
    rpc_port, metadata_port = cachelib_master
    metadata_server = f"http://127.0.0.1:{metadata_port}/metadata"
    local_hostname = "localhost:17816"
    engine = ClassicTransferEngine(
        metadata_server=metadata_server,
        local_hostname=local_hostname,
        protocol="tcp",
    )
    create_args = dict(
        transfer_engine=engine,
        local_hostname=local_hostname,
        metadata_server=metadata_server,
        master_server_addr=f"127.0.0.1:{rpc_port}",
        protocol="tcp",
        global_segment_size=SLAB_SIZE,
        local_buffer_size=SLAB_SIZE,
    )

    monkeypatch.setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", str(SLAB_SIZE))
    monkeypatch.setenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1")
    with pytest.raises(StoreError, match="LOCAL_HOT_CACHE_USE_SHM"):
        await MooncakeClient.create_with_transfer_engine(**create_args)

    monkeypatch.delenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM")
    client = await MooncakeClient.create_with_transfer_engine(**create_args)
    try:
        assert await client.put("test_key_external_te_retry", b"retry") == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_external_transfer_engine_allows_only_one_live_client(cachelib_master):
    rpc_port, _ = cachelib_master
    local_hostname = "localhost:17817"
    engine = ClassicTransferEngine(
        metadata_server="P2PHANDSHAKE",
        local_hostname=local_hostname,
        protocol="tcp",
    )
    create_args = dict(
        transfer_engine=engine,
        local_hostname=local_hostname,
        metadata_server="P2PHANDSHAKE",
        master_server_addr=f"127.0.0.1:{rpc_port}",
        protocol="tcp",
        global_segment_size=0,
        local_buffer_size=SLAB_SIZE,
    )

    first = await MooncakeClient.create_with_transfer_engine(**create_args)
    try:
        with pytest.raises(StoreError, match="already bound"):
            await MooncakeClient.create_with_transfer_engine(**create_args)
    finally:
        await first.close()

    replacement = await MooncakeClient.create_with_transfer_engine(**create_args)
    await replacement.close()


@pytest.mark.asyncio
async def test_copy_move_query_task_reports_id_type_and_success(cachelib_master):
    rpc_port, _ = cachelib_master

    async def create_client(local_hostname):
        return await MooncakeClient.create(
            local_hostname=local_hostname,
            metadata_server="P2PHANDSHAKE",
            master_server_addr=f"127.0.0.1:{rpc_port}",
            protocol="tcp",
            device="",
            global_segment_size=SLAB_SIZE,
            local_buffer_size=SLAB_SIZE,
        )

    async def wait_for_task(client, task_id, expected_type):
        deadline = asyncio.get_running_loop().time() + 10
        last_result = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                last_result = await client.query_task(task_id)
            except StoreError:
                await asyncio.sleep(0.1)
                continue
            returned_id, task_type, status, message = last_result
            if status in (2, 3):
                assert returned_id == task_id
                assert task_type == expected_type
                assert status == 2, message
                return
            await asyncio.sleep(0.1)
        last_result = await client.query_task(task_id)
        returned_id, task_type, status, message = last_result
        assert returned_id == task_id
        assert task_type == expected_type
        assert (
            status == 2
        ), f"task did not finish within 10 seconds: status={status}, message={message}"

    client1_addr = "localhost:17813"
    client2_addr = "localhost:17814"
    client1 = await create_client(client1_addr)
    client2 = None
    try:
        client2 = await create_client(client2_addr)
        config = ReplicateConfig(replica_num=1, preferred_segment=client1_addr)
        assert (
            await client1.put("test_key_copymove", b"Hello, CopyMoveQueryTask!", config)
            == 0
        )

        copy_task_id = await client1.create_copy_task(
            "test_key_copymove", [client2_addr]
        )
        await wait_for_task(client1, copy_task_id, 0)

        move_task_id = await client1.create_move_task(
            "test_key_copymove", client1_addr, client2_addr
        )
        await wait_for_task(client1, move_task_id, 1)
    finally:
        try:
            if client2 is not None:
                await client2.close()
        finally:
            await client1.close()


def _config_dict(cachelib_master, local_hostname, global_size, local_size):
    rpc_port, _ = cachelib_master
    return {
        "local_hostname": local_hostname,
        "metadata_server": "P2PHANDSHAKE",
        "global_segment_size": global_size,
        "local_buffer_size": local_size,
        "protocol": "tcp",
        "rdma_devices": "",
        "master_server_addr": f"127.0.0.1:{rpc_port}",
    }


@pytest.mark.asyncio
async def test_config_dict_setup_rejects_empty_and_roundtrips(cachelib_master):
    with pytest.raises(StoreError, match="local_hostname"):
        await MooncakeClient.create_from_config({})

    client = await MooncakeClient.create_from_config(
        _config_dict(
            cachelib_master,
            "localhost:17813",
            str(SLAB_SIZE),
            str(SLAB_SIZE),
        )
    )
    try:
        value = b"Hello, ConfigDict!"
        assert await client.put("test_key_configdict", value) == 0
        buffer = await client.get_buffer("test_key_configdict")
        assert buffer is not None
        assert bytes(buffer) == value
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_config_dict_setup_accepts_human_readable_sizes(cachelib_master):
    client = await MooncakeClient.create_from_config(
        _config_dict(cachelib_master, "localhost:17814", "16MB", "16 MB")
    )
    await client.close()


@pytest.mark.asyncio
async def test_config_dict_setup_accepts_zero_sizes(cachelib_master):
    client = await MooncakeClient.create_from_config(
        _config_dict(cachelib_master, "localhost:17815", "0", "0")
    )
    await client.close()


@pytest.mark.asyncio
async def test_config_dict_large_global_size_reaches_transport_failure():
    config = {
        "local_hostname": "localhost:17816",
        "metadata_server": "P2PHANDSHAKE",
        "global_segment_size": str((1 << 40) + 1),
        "local_buffer_size": "0",
        "protocol": "tcp",
        "rdma_devices": "",
        "master_server_addr": "127.0.0.1:1",
    }
    with pytest.raises(StoreError, match="transport error") as exc_info:
        await MooncakeClient.create_from_config(config)
    assert "Invalid global_segment_size" not in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("local_hostname", "global_size", "local_size"),
    [
        ("localhost:17816", "50%", "16MB"),
        ("localhost:17817", "16MB", "16XB"),
        ("localhost:17818", "-5", "16MB"),
        ("localhost:17819", "0", str((1 << 40) + 1)),
    ],
)
async def test_config_dict_rejects_invalid_size_strings(
    cachelib_master, local_hostname, global_size, local_size
):
    with pytest.raises(StoreError):
        await MooncakeClient.create_from_config(
            _config_dict(
                cachelib_master,
                local_hostname,
                global_size,
                local_size,
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("tenant_id", "tenant-a"),
        ("ipc_socket_path", "/tmp/mooncake.sock"),
        ("ssd_offload_path", "/tmp/mooncake-ssd"),
        ("enable_ssd_offload", "true"),
        ("enable_client_http_server", "true"),
        ("client_http_port", "50052"),
    ],
)
async def test_config_dict_rejects_known_unsupported_behavioral_fields(key, value):
    config = {
        "local_hostname": "localhost:17820",
        "metadata_server": "P2PHANDSHAKE",
        "global_segment_size": "0",
        "local_buffer_size": "0",
        "protocol": "tcp",
        "rdma_devices": "",
        "master_server_addr": "127.0.0.1:1",
        key: value,
    }
    with pytest.raises(StoreError, match=rf"{key}.*not supported"):
        await MooncakeClient.create_from_config(config)


@pytest.mark.asyncio
async def test_uninitialized_client_operation_sentinels():
    client = MooncakeClient.uninitialized()
    config = ReplicateConfig(replica_num=1)

    assert client.is_closed() is False
    assert repr(client) == "MooncakeClient(uninitialized)"
    assert await client.put("before_setup_key", b"test_data", config) != 0
    assert await client.get_buffer("before_setup_key") is None
    assert await client.exists("before_setup_key") < 0
    assert await client.remove("before_setup_key") != 0
    assert await client.get_size("before_setup_key") < 0
    assert await client.remove_all() != 0
    assert await client.tear_down_all() == 0
    assert client.is_closed() is False
    assert await client.close() == 0
    assert client.is_closed() is True
    assert repr(client) == "MooncakeClient(closed)"
    with pytest.raises(StoreError, match="already closed"):
        await client.put("before_setup_key", b"test_data", config)


@pytest.mark.asyncio
async def test_live_tear_down_all_returns_integer_zero(cachelib_master):
    rpc_port, _ = cachelib_master
    client = await MooncakeClient.create(
        local_hostname="localhost:17821",
        metadata_server="P2PHANDSHAKE",
        master_server_addr=f"127.0.0.1:{rpc_port}",
        protocol="tcp",
        device="",
        global_segment_size=SLAB_SIZE,
        local_buffer_size=SLAB_SIZE,
    )
    try:
        assert await client.tear_down_all() == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_uninitialized_batch_exists_returns_two_failures():
    client = MooncakeClient.uninitialized()
    results = await client.batch_is_exist(["k1", "k2"])
    assert len(results) == 2
    assert all(status < 0 for status in results)


@pytest.mark.asyncio
async def test_uninitialized_get_size_is_failure():
    client = MooncakeClient.uninitialized()
    assert await client.get_size("any_key") < 0


@pytest.mark.asyncio
async def test_uninitialized_remove_by_regex_is_failure():
    client = MooncakeClient.uninitialized()
    assert await client.remove_by_regex(".*") < 0


@pytest.mark.asyncio
async def test_uninitialized_batch_remove_returns_two_failures():
    client = MooncakeClient.uninitialized()
    results = await client.batch_remove(["k1", "k2"])
    assert len(results) == 2
    assert all(status != 0 for status in results)


@pytest.mark.asyncio
async def test_uninitialized_put_parts_is_failure():
    client = MooncakeClient.uninitialized()
    config = ReplicateConfig(replica_num=1)
    assert await client.put_parts("key", [b"data"], config) != 0


@pytest.mark.asyncio
async def test_uninitialized_health_check_reports_not_initialized():
    client = MooncakeClient.uninitialized()
    assert await client.health_check() == 1
    assert await client.close() == 0
    with pytest.raises(StoreError, match="already closed"):
        await client.health_check()


@pytest.mark.asyncio
async def test_file_mount_missing_path_fails_without_ids(cachelib_master):
    client = await _client(cachelib_master)
    try:
        status, segment_ids = await client.mount_file_segments(
            "/tmp/mooncake_nonexistent_file_12345", 0, 4096, "tcp", ""
        )
        assert status != 0
        assert segment_ids == []
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_plain_unmount_rejects_zero_uuid(cachelib_master):
    client = await _client(cachelib_master)
    try:
        assert (
            await client.unmount_segments(["00000000-0000-0000-0000-000000000000"]) != 0
        )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_file_mount_roundtrips_plain_uuid_unmount(
    cachelib_master, tmp_path, monkeypatch
):
    backing_file = tmp_path / "mounted-segment.bin"
    with backing_file.open("wb") as file:
        file.truncate(SLAB_SIZE * 2)
    monkeypatch.setenv("MC_MAX_MR_SIZE", str(SLAB_SIZE))

    client = await _client(cachelib_master)
    try:
        status, segment_ids = await client.mount_file_segments(
            str(backing_file), 0, SLAB_SIZE * 2, "tcp", ""
        )
        assert status == 0
        assert len(segment_ids) == 2
        assert await client.unmount_segments(segment_ids) == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_plain_unmount_processes_valid_ids_among_invalid_values(
    cachelib_master, tmp_path
):
    backing_file = tmp_path / "mounted-segment.bin"
    with backing_file.open("wb") as file:
        file.truncate(SLAB_SIZE)

    client = await _client(cachelib_master)
    try:
        status, segment_ids = await client.mount_file_segments(
            str(backing_file), 0, SLAB_SIZE, "tcp", ""
        )
        assert status == 0
        assert len(segment_ids) == 1
        assert await client.unmount_segments(["invalid-uuid", segment_ids[0]]) != 0
        assert await client.unmount_segments(segment_ids) != 0
    finally:
        await client.close()
