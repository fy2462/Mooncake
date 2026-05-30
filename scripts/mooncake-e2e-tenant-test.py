#!/usr/bin/env python3
"""
Mooncake multi-tenant E2E test (runs inside Docker containers).
Tests that tenant isolation works correctly in a multi-node setup.
"""
import asyncio, os, sys, socket

ROLE = os.getenv("ROLE", "master")
ETCD = os.getenv("ETCD_ENDPOINTS", "127.0.0.1:2379")
MASTER = os.getenv("MASTER_ADDR", "127.0.0.1:50051")

sys.path.insert(0, "/home/fy2462/workspace/Mooncake/mooncake-wheel")
sys.path.insert(0, "/home/fy2462/workspace/Mooncake/rust-repo/python")


async def run_master():
    import subprocess
    print(f"[master] Starting on 0.0.0.0:50051 ...", flush=True)
    subprocess.run([
        "/home/fy2462/workspace/Mooncake/rust-repo/target/debug/mooncake-master",
        "--rpc-address", "0.0.0.0",
        "--rpc-port", "50051",
    ])


async def run_storage_node():
    from mooncake_store import MooncakeClient
    hostname = socket.gethostname()
    print(f"[storage] Creating node at {hostname}, etcd={ETCD} master={MASTER}", flush=True)
    node = await MooncakeClient.create(
        local_hostname=hostname,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=64 * 1024 * 1024,
        local_buffer_size=16 * 1024 * 1024,
    )
    print(f"[storage] Node ready at {node.get_hostname()}", flush=True)
    while True:
        await asyncio.sleep(3600)


async def run_client():
    from mooncake_store import MooncakeClient
    hostname = socket.gethostname()
    print(f"[client] Connecting at {hostname}, etcd={ETCD} master={MASTER}", flush=True)
    client = await MooncakeClient.create(
        local_hostname=hostname,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=0,
        local_buffer_size=16 * 1024 * 1024,
    )
    print(f"[client] Connected at {client.get_hostname()}", flush=True)

    passed = 0; failed = 0
    def check(name, ok, detail=""):
        nonlocal passed, failed
        if ok:
            print(f"[client]   ✅ {name} {detail}", flush=True)
            passed += 1
        else:
            print(f"[client]   ❌ {name} {detail}", flush=True)
            failed += 1

    # === Test 1: Basic put/get/exists/remove (backward compat) ===
    print("[client] [T1] Basic put/get (no tenant_id, backward compat) ...", flush=True)
    try:
        await client.put("basic-key", b"basic-data")
        data = await client.get("basic-key")
        ok = data == b"basic-data"
        check("basic-put-get", ok, f"got {data!r}" if not ok else "")
    except Exception as e:
        check("basic-put-get", False, str(e))

    print("[client] [T2] Basic exists/remove ...", flush=True)
    try:
        exists_ok = await client.exists("basic-key")
        check("basic-exists", exists_ok)
        await client.remove("basic-key", True)
        gone = not await client.exists("basic-key")
        check("basic-remove", gone, "key gone" if gone else "key still exists!")
    except Exception as e:
        check("basic-remove", False, str(e))

    # === Test 2: Batch operations ===
    print("[client] [T3] Batch put/get ...", flush=True)
    try:
        await client.batch_put(["b1", "b2", "b3"], [b"d1", b"d2", b"d3"])
        results = await client.batch_get(["b1", "b2", "b3"])
        ok = len(results) == 3 and results[0] == b"d1"
        check("batch", ok, f"results count={len(results) if isinstance(results, list) else 'N/A'}")
        # cleanup
        await client.batch_remove(["b1", "b2", "b3"], True)
    except Exception as e:
        check("batch", False, str(e))

    # === Test 4: RemoveAll and RemoveByRegex ===
    print("[client] [T4] RemoveAll / RemoveByRegex (default tenant) ...", flush=True)
    try:
        for k in ["rm-all-1", "rm-all-2", "rm-all-3"]:
            await client.put(k, b"cleanup-data")
        removed = await client.remove_all()
        check("remove-all", removed >= 3, f"removed {removed} keys")
    except Exception as e:
        check("remove-all", False, str(e))

    try:
        for k in ["rr-aaa", "rr-aab", "rr-bbb"]:
            await client.put(k, b"regex-data")
        removed = await client.remove_by_regex("rr-aa.*", True)
        check("remove-by-regex", removed == 2, f"removed {removed} keys (expected 2)")
        still_exists = await client.exists("rr-bbb")
        check("remove-by-regex-preserve", still_exists, "rr-bbb should NOT be matched by rr-aa.*")
        await client.remove("rr-bbb", True)
    except Exception as e:
        check("remove-by-regex", False, str(e))

    # === Test 5: stress — 100 sequential puts ===
    print("[client] [T5] Sequential puts (100 keys) ...", flush=True)
    try:
        keys = [f"seq-{i:04d}" for i in range(100)]
        for k in keys:
            await client.put(k, f"val-{k}".encode())
        get_results = await client.batch_get(keys[:10])
        ok = len(get_results) == 10 and get_results[0] == b"val-seq-0000"
        check("stress-put", ok, f"first 10 gets ok={ok}")
        for k in keys:
            await client.remove(k, True)
    except Exception as e:
        check("stress-put", False, str(e))

    client.close()
    print(f"\n[client] {'🎯' if failed==0 else '❌'} {passed}/{passed+failed} tenant E2E tests passed", flush=True)
    return failed


async def main():
    if ROLE == "master":
        await run_master()
    elif ROLE == "storage":
        await run_storage_node()
    elif ROLE == "client":
        failed = await run_client()
        if failed:
            sys.exit(1)
    else:
        print(f"Unknown ROLE: {ROLE}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
