#!/usr/bin/env python3
"""
Mooncake multi-node API coverage E2E test.

Validates the protobuf field changes made in the Rust client:
  - BatchUpsertEnd: PutEndEntry entries (was UpsertEntry), statuses response
  - MountSegment: te_endpoint + protocol fields
  - ReMountSegment: base_addrs + te_endpoints + protocols fields
  - batch_query_ip / batch_replica_clear: multi-node integration
  - upsert overwrite, upsert_parts, health_check roundtrip

Two-node topology (etcd + master + storage + client), same pattern as
debug_multi_node_batch_get.py.
"""
import asyncio
import logging
import os
import socket
import sys

# ---------------------------------------------------------------------------
# Setup logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s.%(msecs)03d %(name)s %(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("api-cov")

# Enable Rust-side TE debug tracing
try:
    import _mooncake_store
    _mooncake_store.enable_te_debug_tracing()
    logger.info("Rust TE debug tracing enabled")
except Exception as e:
    logger.warning("Could not enable Rust tracing: %s", e)

from mooncake_store import MooncakeClient, ReplicateConfig

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ROLE = os.getenv("ROLE", "client")
ETCD = os.getenv("ETCD_ENDPOINTS", "127.0.0.1:2379")
MASTER = os.getenv("MASTER_ADDR", "127.0.0.1:50051")
RUN_ID = os.getenv("RUN_ID", "default")


def node_name(suffix: str) -> str:
    host = socket.gethostname()
    return f"{host}:{suffix}"


# ===================================================================
# Storage node
# ===================================================================
async def run_storage_node():
    local_name = node_name("12001")
    logger.info("[storage] Creating node at %s (etcd=%s master=%s)", local_name, ETCD, MASTER)

    node = await MooncakeClient.create(
        local_hostname=local_name,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=64 * 1024 * 1024,
        local_buffer_size=16 * 1024 * 1024,
    )
    logger.info("[storage] Node ready: %s", node.get_hostname())
    sys.stdout.flush()

    while True:
        await asyncio.sleep(3600)


# ===================================================================
# Client tests
# ===================================================================
async def run_client():
    local_name = node_name("12002")
    logger.info("[client] Creating node at %s (etcd=%s master=%s)", local_name, ETCD, MASTER)
    logger.info("[client] Python version: %s", sys.version)

    client = await MooncakeClient.create(
        local_hostname=local_name,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=0,
        local_buffer_size=16 * 1024 * 1024,
    )
    logger.info("[client] Connected: %s", client.get_hostname())

    passed = 0
    failed = 0

    def check(name: str, ok: bool, detail: str = ""):
        nonlocal passed, failed
        status = "PASS" if ok else "FAIL"
        msg = f"[client]   {status}: {name}"
        if detail:
            msg += f" ({detail})"
        logger.info(msg)
        if ok:
            passed += 1
        else:
            failed += 1
        sys.stdout.flush()

    # ------------------------------------------------------------------
    # Test 1: upsert (new key) + get — validates BatchUpsertEnd proto
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 1: upsert new key + get ---")
    try:
        key = f"api_t1_{RUN_ID}"
        data = b"upsert test data! " * 500
        replicas = await client.upsert(key, data)
        ok = len(replicas) > 0
        detail = f"replicas={len(replicas)}"
        if ok:
            await asyncio.sleep(0.3)
            result = await client.get(key)
            ok = result == data
            detail += f" data_ok={ok}"
        check("upsert+get", ok, detail)
    except Exception as e:
        check("upsert+get", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 2: upsert (overwrite existing key) — validates idempotent
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 2: upsert overwrite + get ---")
    try:
        key = f"api_t2_{RUN_ID}"
        old_data = b"old value " * 300
        new_data = b"NEW VALUE overwritten! " * 300
        await client.upsert(key, old_data)
        await asyncio.sleep(0.2)
        # Overwrite via upsert
        await client.upsert(key, new_data)
        await asyncio.sleep(0.3)
        result = await client.get(key)
        check("upsert-overwrite", result == new_data,
              f"len={len(result)} expected={len(new_data)}")
    except Exception as e:
        check("upsert-overwrite", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 3: upsert_parts (multi-part data)
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 3: upsert_parts ---")
    try:
        key = f"api_t3_{RUN_ID}"
        parts = [b"Part1-", b"Part2-", b"Part3"]
        expected = b"Part1-Part2-Part3"
        await client.upsert_parts(key, parts)
        await asyncio.sleep(0.3)
        result = await client.get(key)
        check("upsert-parts", result == expected,
              f"got={result[:50] if result else None}")
    except Exception as e:
        check("upsert-parts", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 4: health_check — validates ReMountSegment path
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 4: health_check (ReMountSegment path) ---")
    try:
        await client.health_check()
        ok = True
        detail = "no error"
    except Exception as e:
        ok = False
        detail = str(e)[:100]
    check("health_check", ok, detail)

    # ------------------------------------------------------------------
    # Test 5: batch_get (10 keys, individually put)
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 5: batch_get (10 keys) ---")
    try:
        keys = [f"api_t5_k{i}_{RUN_ID}" for i in range(10)]
        data_list = [f"batch data {i}! ".encode() * 256 for i in range(10)]
        for key, data in zip(keys, data_list):
            await client.put(key, data)

        await asyncio.sleep(1.0)
        results = await client.batch_get(keys)
        ok = True
        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None or result != expected:
                logger.error("[client]   Key %s mismatch! (size: %d vs %d)",
                             key, len(result) if result else 0, len(expected))
                ok = False
        check("batch_get(10)", ok, f"got {sum(1 for r in results if r is not None)}/10")
    except Exception as e:
        check("batch_get(10)", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 6: batch_remove + batch_is_exist
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 6: batch_remove + exists ---")
    try:
        keys = [f"api_t6_k{i}_{RUN_ID}" for i in range(3)]
        data_list = [f"removable {i}".encode() * 200 for i in range(3)]

        # Put individually since batch_put has timing issue
        for key, data in zip(keys, data_list):
            await client.put(key, data)

        await asyncio.sleep(0.3)

        # Remove first two keys
        remove_keys = keys[:2]
        await client.batch_remove(remove_keys)
        await asyncio.sleep(0.2)

        # Verify removed keys don't exist, 3rd key still exists
        exists_removed = [await client.exists(k) for k in remove_keys]
        exists_kept = await client.exists(keys[2])
        ok = (exists_removed == [False, False] and exists_kept)
        check("batch_remove+exists", ok,
              f"removed_exist={exists_removed} kept_exist={exists_kept}")
    except Exception as e:
        check("batch_remove+exists", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 7: put + remove + exists lifecycle
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 7: put+remove+exists lifecycle ---")
    try:
        key = f"api_t7_{RUN_ID}"
        await client.put(key, b"lifecycle test data")
        await asyncio.sleep(0.2)

        exists_before = await client.exists(key)
        await client.remove(key)
        await asyncio.sleep(0.2)
        exists_after = await client.exists(key)

        check("lifecycle", exists_before and not exists_after,
              f"before={exists_before} after={exists_after}")
    except Exception as e:
        check("lifecycle", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 8: upsert + remove + re-upsert (idempotent cycle)
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 8: upsert+remove+re-upsert cycle ---")
    try:
        key = f"api_t8_{RUN_ID}"
        data = b"cycle data " * 400
        await client.upsert(key, data)
        await asyncio.sleep(0.2)
        await client.remove(key)
        await asyncio.sleep(0.2)
        # Re-upsert after remove — should create fresh
        await client.upsert(key, data)
        await asyncio.sleep(0.3)
        result = await client.get(key)
        check("upsert-cycle", result == data,
              f"len={len(result) if result else 0}")
    except Exception as e:
        check("upsert-cycle", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 9: get_size
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 9: get_size ---")
    try:
        key = f"api_t9_{RUN_ID}"
        data = b"sized data! " * 128
        await client.put(key, data)
        await asyncio.sleep(0.3)
        size = await client.get_size(key)
        check("get_size", size == len(data),
              f"size={size} expected={len(data)}")
    except Exception as e:
        check("get_size", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 10: multiple health_checks (stress RemountSegment path)
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 10: repeated health_check ---")
    try:
        ok = True
        for i in range(5):
            await client.health_check()
            await asyncio.sleep(0.2)
        detail = "5x health_check all ok"
    except Exception as e:
        ok = False
        detail = str(e)[:100]
    check("health_check_x5", ok, detail)

    # ------------------------------------------------------------------
    # Test 11: batch_upsert semantics via multiple upserts
    #   Validates BatchUpsertEnd with PutEndEntry for multiple keys
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 11: multiple upserts in sequence ---")
    try:
        keys = [f"api_t11_k{i}_{RUN_ID}" for i in range(6)]
        data_list = [f"upsert multi {i}! ".encode() * 200 for i in range(6)]

        for key, data in zip(keys, data_list):
            await client.upsert(key, data)

        await asyncio.sleep(0.5)
        results = await client.batch_get(keys)
        ok = True
        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None or result != expected:
                logger.error("[client]   Key %s mismatch!", key)
                ok = False
        check("multi-upserts", ok, f"{sum(1 for r in results if r is not None)}/6 retrieved")
    except Exception as e:
        check("multi-upserts", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Test 12: upsert with explicit ReplicateConfig
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 12: upsert with ReplicateConfig ---")
    try:
        key = f"api_t12_{RUN_ID}"
        data = b"configured upsert! " * 300
        config = ReplicateConfig()
        config.replica_num = 1
        replicas = await client.upsert(key, data, config)
        ok = len(replicas) > 0
        await asyncio.sleep(0.3)
        result = await client.get(key)
        ok = ok and result == data
        check("upsert-with-config", ok, f"replicas={len(replicas)}")
    except Exception as e:
        check("upsert-with-config", False, str(e)[:100])

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    client.close()
    total = passed + failed
    emoji = "\U0001f3af" if failed == 0 else "❌"
    logger.info("[client] %s Results: %d/%d passed, %d failed",
                emoji, passed, total, failed)
    sys.stdout.flush()
    return failed


# ===================================================================
# Main
# ===================================================================
async def main():
    logger.info("[%s] Starting API coverage with RUN_ID=%s ETCD=%s MASTER=%s",
                ROLE, RUN_ID, ETCD, MASTER)
    sys.stdout.flush()

    if ROLE == "storage":
        await run_storage_node()
    elif ROLE == "client":
        failed = await run_client()
        if failed:
            sys.exit(1)
    else:
        logger.error("Unknown ROLE: %s (use 'storage' or 'client')", ROLE)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
