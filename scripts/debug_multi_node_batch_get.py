#!/usr/bin/env python3
"""
Mooncake multi-node batch_get E2E test with Python 3.14t.

Two nodes on different Docker containers share etcd + master:
  - storage node: global_segment_size=64MB (provides segment memory)
  - client node:  global_segment_size=0  (reads/writes via TE TCP)

ROLE env var controls behavior:
  - ROLE=storage: create storage node, sleep
  - ROLE=client:  create client, run get/batch tests
"""
import asyncio
import logging
import os
import socket
import sys
import traceback

# ---------------------------------------------------------------------------
# Setup logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s.%(msecs)03d %(name)s %(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("multi-node")

# Enable Rust-side TE debug tracing
try:
    import _mooncake_store
    _mooncake_store.enable_te_debug_tracing()
    logger.info("Rust TE debug tracing enabled")
except Exception as e:
    logger.warning("Could not enable Rust tracing: %s", e)

from mooncake_store import MooncakeClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ROLE = os.getenv("ROLE", "client")
ETCD = os.getenv("ETCD_ENDPOINTS", "127.0.0.1:2379")
MASTER = os.getenv("MASTER_ADDR", "127.0.0.1:50051")
RUN_ID = os.getenv("RUN_ID", "default")


def node_name(role: str, suffix: str) -> str:
    """Build unique local_hostname from container hostname + fixed port suffix."""
    host = socket.gethostname()
    return f"{host}:{suffix}"


# ===================================================================
# Storage node
# ===================================================================
async def run_storage_node():
    local_name = node_name("storage", "12001")
    logger.info("[storage] Creating node at %s (etcd=%s master=%s)", local_name, ETCD, MASTER)
    logger.info("[storage] Python version: %s", sys.version)

    node = await MooncakeClient.create(
        local_hostname=local_name,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=64 * 1024 * 1024,   # 64MB segment
        local_buffer_size=16 * 1024 * 1024,      # 16MB buffer
    )
    logger.info("[storage] Node ready: %s", node.get_hostname())
    logger.info("[storage] Waiting for client operations...")
    sys.stdout.flush()

    # Keep alive until killed
    while True:
        await asyncio.sleep(3600)


# ===================================================================
# Client tests
# ===================================================================
async def run_client():
    local_name = node_name("client", "12002")
    logger.info("[client] Creating node at %s (etcd=%s master=%s)", local_name, ETCD, MASTER)
    logger.info("[client] Python version: %s", sys.version)

    client = await MooncakeClient.create(
        local_hostname=local_name,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=0,              # pure client, no segment
        local_buffer_size=16 * 1024 * 1024,  # 16MB buffer
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
    # Test 1: put then get (single key, single value)
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 1: put then get ---")
    try:
        key = f"t1_key_{RUN_ID}"
        data = b"multi-node test 1 data! " * 500  # ~13KB
        await client.put(key, data)
        await asyncio.sleep(0.3)
        result = await client.get(key)
        check("put+get", result == data, f"size={len(result)}")
    except Exception as e:
        check("put+get", False, str(e)[:80])

    # ------------------------------------------------------------------
    # Test 2: put 2 keys then batch_get
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 2: put + batch_get (2 keys) ---")
    try:
        keys = [f"t2_k1_{RUN_ID}", f"t2_k2_{RUN_ID}"]
        data_list = [
            b"batch key 1! " * 500,
            b"batch key 2! " * 1000,
        ]
        for key, data in zip(keys, data_list):
            await client.put(key, data)

        await asyncio.sleep(0.3)
        results = await client.batch_get(keys)
        ok = True
        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None or result != expected:
                logger.error("[client]   Key %s mismatch!", key)
                ok = False
        check("batch_get(2)", ok)
    except Exception as e:
        check("batch_get(2)", False, str(e)[:80])

    # ------------------------------------------------------------------
    # Test 3: batch_put 5 keys then batch_get
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 3: batch_put + batch_get (5 keys) ---")
    try:
        keys = [f"t3_k{i}_{RUN_ID}" for i in range(5)]
        data_list = [f"batch data {i}! ".encode() * 300 for i in range(5)]
        statuses = await client.batch_put(keys, data_list)
        ok = all(s == 0 for s in statuses)

        await asyncio.sleep(0.5)
        results = await client.batch_get(keys)
        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None or result != expected:
                logger.error("[client]   Key %s mismatch!", key)
                ok = False
        check("batch_put+get(5)", ok)
    except Exception as e:
        check("batch_put+get(5)", False, str(e)[:80])

    # ------------------------------------------------------------------
    # Test 4: 10 sequential puts then batch_get all
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 4: 10 sequential puts + batch_get ---")
    try:
        keys = [f"t4_k{i}_{RUN_ID}" for i in range(10)]
        data_list = [f"data {i}! ".encode() * 200 for i in range(10)]
        for i, (key, data) in enumerate(zip(keys, data_list)):
            await client.put(key, data)

        await asyncio.sleep(0.5)
        results = await client.batch_get(keys)
        ok = True
        for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
            if result is None or result != expected:
                logger.error("[client]   Key %s mismatch!", key)
                ok = False
        check("10puts+batch_get", ok)
    except Exception as e:
        check("10puts+batch_get", False, str(e)[:80])

    # ------------------------------------------------------------------
    # Test 5: 5 cycles of put+batch_get (fresh keys each cycle)
    # ------------------------------------------------------------------
    logger.info("[client] --- Test 5: repeated cycles (5x put+batch_get) ---")
    try:
        ok = True
        for cycle in range(5):
            keys = [f"t5_c{cycle}_k{i}_{RUN_ID}" for i in range(2)]
            data_list = [f"cycle {cycle} data {i}! ".encode() * 300 for i in range(2)]
            for key, data in zip(keys, data_list):
                await client.put(key, data)

            await asyncio.sleep(0.2)
            results = await client.batch_get(keys)
            for i, (key, expected, result) in enumerate(zip(keys, data_list, results)):
                if result is None or result != expected:
                    logger.error("[client]   Cycle %d key %s mismatch!", cycle, key)
                    ok = False
        check("5cycles", ok)
    except Exception as e:
        check("5cycles", False, str(e)[:80])

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    client.close()
    total = passed + failed
    emoji = "\U0001f3af" if failed == 0 else "❌"
    logger.info(f"[client] {emoji} Results: {passed}/{total} passed, {failed} failed")
    sys.stdout.flush()
    return failed


# ===================================================================
# Main
# ===================================================================
async def main():
    logger.info(f"[{ROLE}] Starting with RUN_ID={RUN_ID} ETCD={ETCD} MASTER={MASTER}")
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
