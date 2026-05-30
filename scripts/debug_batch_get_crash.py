#!/usr/bin/env python3
"""
Minimal reproduction script for batch_get crash after prior TE operations.

Prerequisites:
    etcd running on 127.0.0.1:2379
    master running on 127.0.0.1:50051
    LD_LIBRARY_PATH includes build/ .so directories

Usage:
    source .venv/bin/activate
    export LD_LIBRARY_PATH=build/mooncake-common/etcd:build/mooncake-common:build/mooncake-transfer-engine/src:$LD_LIBRARY_PATH
    export MC_METADATA_SERVER=127.0.0.1:2379
    export MASTER_SERVER=127.0.0.1:50051
    python scripts/debug_batch_get_crash.py
"""

import asyncio
import logging
import os
import sys
import time
import traceback

# ---------------------------------------------------------------------------
# Setup debug logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="[PY_DEBUG] %(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("debug_batch_get_crash")

# Enable Rust-side TE debug tracing
try:
    from mooncake_store import _mooncake_store
    _mooncake_store.enable_te_debug_tracing()
    logger.info("=== Rust TE debug tracing enabled ===")
except Exception as e:
    logger.warning("Could not enable Rust tracing: %s", e)

from mooncake_store import MooncakeClient


async def create_client(local_hostname: str = "localhost:12002"):
    """Create a MooncakeClient connected to master."""
    metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
    master_server = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
    protocol = os.getenv("PROTOCOL", "tcp")
    device = os.getenv("DEVICE_NAME", "")

    logger.info(
        "Creating client: hostname=%s metadata=%s master=%s protocol=%s",
        local_hostname, metadata_server, master_server, protocol,
    )

    client = await MooncakeClient.create(
        local_hostname=local_hostname,
        metadata_server=metadata_server,
        master_server_addr=master_server,
        protocol=protocol,
        device=device,
        global_segment_size=64 * 1024 * 1024,  # 64MB
        local_buffer_size=16 * 1024 * 1024,     # 16MB
    )

    logger.info("Client created: %s", client.get_hostname())
    return client


# Unique prefix to avoid key collisions across runs
_RUN_ID = str(int(time.time() * 1000))[-8:]

# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

async def test_01_batch_get_direct(client: MooncakeClient):
    """batch_get WITHOUT prior TE operations."""
    logger.info("=== Test 1: batch_get directly (no prior writes) ===")
    keys = [f"direct_test_key_1_{_RUN_ID}", f"direct_test_key_2_{_RUN_ID}"]
    try:
        logger.info("Calling client.batch_get(%s)...", keys)
        results = await client.batch_get(keys)
        logger.info("batch_get OK: %d results", len(results))
        return True
    except Exception as e:
        logger.error("FAILED: %s", e)
        traceback.print_exc()
        return False


async def test_02_put_then_get(client: MooncakeClient):
    """put then get (single key)."""
    logger.info("=== Test 2: put then get ===")
    key = f"test_02_key_{_RUN_ID}"
    data = b"Hello from test 02! " * 1000
    try:
        logger.info("put(key=%s, data_len=%d)...", key, len(data))
        await client.put(key, data)
        logger.info("get(key=%s)...", key)
        retrieved = await client.get(key)
        assert retrieved == data, f"data mismatch: got {len(retrieved)} bytes"
        logger.info("get OK: %d bytes", len(retrieved))
        return True
    except Exception as e:
        logger.error("FAILED: %s", e)
        traceback.print_exc()
        return False


async def test_03_put_then_batch_get(client: MooncakeClient):
    """put then batch_get — THE CRASH SCENARIO."""
    logger.info("=== Test 3: put then batch_get (CRASH SCENARIO) ===")
    keys = [f"test_03_key_1_{_RUN_ID}", f"test_03_key_2_{_RUN_ID}"]
    data_list = [
        b"Batch data 1! " * 1000,
        b"Batch data 2! " * 2000,
    ]
    try:
        for i, (key, data) in enumerate(zip(keys, data_list)):
            logger.info("[put %d] key=%s len=%d", i, key, len(data))
            await client.put(key, data)
            logger.info("[put %d] OK", i)

        await asyncio.sleep(0.2)

        logger.info("[batch_get] keys=%s", keys)
        results = await client.batch_get(keys)
        logger.info("[batch_get] OK: %d results", len(results))

        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None:
                logger.error("[%d] Key %s returned None!", i, key)
                return False
            if result != expected:
                logger.error("[%d] DATA MISMATCH for key %s!", i, key)
                return False
            logger.info("[%d] Key %s: %d bytes OK", i, key, len(result))
        return True
    except Exception as e:
        logger.error("FAILED: %s", e)
        traceback.print_exc()
        return False


async def test_04_batch_put_then_batch_get(client: MooncakeClient):
    """batch_put then batch_get."""
    logger.info("=== Test 4: batch_put then batch_get ===")
    keys = [f"test_04_key_{i}_{_RUN_ID}" for i in range(1, 4)]
    data_list = [b"Batch data %d! " % i * 500 for i in range(3)]
    try:
        logger.info("batch_put(%s)...", keys)
        results = await client.batch_put(keys, data_list)
        logger.info("batch_put OK: %s", results)
        assert all(r == 0 for r in results), f"batch_put failed: {results}"

        await asyncio.sleep(0.2)

        logger.info("batch_get(%s)...", keys)
        results = await client.batch_get(keys)
        logger.info("batch_get OK: %d results", len(results))

        for i, result in enumerate(results):
            if result is None or result != data_list[i]:
                logger.error("Key %s mismatch!", keys[i])
                return False
        return True
    except Exception as e:
        logger.error("FAILED: %s", e)
        traceback.print_exc()
        return False


async def test_05_multi_put_then_batch_get(client: MooncakeClient):
    """Many sequential puts then batch_get."""
    logger.info("=== Test 5: many puts then batch_get ===")
    num = 10
    keys = [f"test_05_key_{i}_{_RUN_ID}" for i in range(num)]
    data_list = [f"Data {i}! ".encode() * 200 for i in range(num)]
    try:
        for i, (key, data) in enumerate(zip(keys, data_list)):
            await client.put(key, data)
            if i % 5 == 0:
                logger.info("  put %d/%d OK", i + 1, num)

        await asyncio.sleep(0.5)

        logger.info("batch_get(%d keys)...", num)
        results = await client.batch_get(keys)
        logger.info("batch_get OK: %d results", len(results))

        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None:
                logger.error("Key %s returned None!", key)
                return False
            if result != expected:
                logger.error("Key %s data mismatch!", key)
                return False
        return True
    except Exception as e:
        logger.error("FAILED: %s", e)
        traceback.print_exc()
        return False


async def test_06_repeated_cycles(client: MooncakeClient):
    """Repeated put+batch_get cycles (fresh keys per cycle to avoid upsert race)."""
    logger.info("=== Test 6: repeated cycles ===")
    data_list = [b"Cycle 1! " * 500, b"Cycle 2! " * 500]
    for cycle in range(5):
        try:
            logger.info("  Cycle %d/5...", cycle + 1)
            keys = [f"cycle_{cycle}_key_1_{_RUN_ID}", f"cycle_{cycle}_key_2_{_RUN_ID}"]
            for key, data in zip(keys, data_list):
                await client.put(key, data)
            await asyncio.sleep(0.2)
            results = await client.batch_get(keys)
            for i, result in enumerate(results):
                if result is None or result != data_list[i]:
                    logger.error("Cycle %d: key %s mismatch!", cycle, keys[i])
                    return False
        except Exception as e:
            logger.error("Cycle %d FAILED: %s", cycle, e)
            traceback.print_exc()
            return False
    logger.info("All 5 cycles OK")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    logger.info("=" * 60)
    logger.info("batch_get CRASH REPRODUCTION")
    logger.info("MC_METADATA_SERVER=%s MASTER_SERVER=%s",
                os.getenv("MC_METADATA_SERVER", "not set"),
                os.getenv("MASTER_SERVER", "not set"))
    logger.info("=" * 60)

    try:
        client = await create_client()
    except Exception as e:
        logger.error("Failed to create client: %s", e)
        traceback.print_exc()
        return 1

    results = []
    tests = [
        ("batch_get_direct", test_01_batch_get_direct),
        ("put_then_get", test_02_put_then_get),
        ("put_then_batch_get", test_03_put_then_batch_get),
        ("batch_put_then_batch_get", test_04_batch_put_then_batch_get),
        ("multi_put_then_batch_get", test_05_multi_put_then_batch_get),
        ("repeated_cycles", test_06_repeated_cycles),
    ]

    for name, test_fn in tests:
        logger.info("")
        result = await test_fn(client)
        results.append((name, result))
        if not result:
            logger.error("Stopping early: %s failed", name)
            break

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    passed = sum(1 for _, r in results if r)
    failed = sum(1 for _, r in results if not r)
    for name, result in results:
        logger.info("  %s: %s", name, "PASS" if result else "FAIL")
    logger.info("Total: %d passed, %d failed", passed, failed)

    client.close()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
