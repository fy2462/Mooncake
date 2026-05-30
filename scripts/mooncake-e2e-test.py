#!/usr/bin/env python3
"""
Mooncake multi-node end-to-end test (runs inside podman containers).
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
    local_name = f"{hostname}:12001"

    print(f"[storage] Creating node at {local_name} etcd={ETCD} master={MASTER}", flush=True)
    node = await MooncakeClient.create(
        local_hostname=hostname,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=64 * 1024 * 1024,
        local_buffer_size=16 * 1024 * 1024,
    )
    print(f"[storage] ✅ Node ready at {node.get_hostname()}", flush=True)
    print(f"[storage] Waiting for client operations (infinite sleep)...", flush=True)
    while True:
        await asyncio.sleep(3600)


async def run_client():
    from mooncake_store import MooncakeClient

    hostname = socket.gethostname()
    local_name = f"{hostname}:12002"

    print(f"[client] Connecting at {local_name} etcd={ETCD} master={MASTER}", flush=True)
    client = await MooncakeClient.create(
        local_hostname=hostname,
        metadata_server=ETCD,
        master_server_addr=MASTER,
        protocol="tcp",
        device="",
        global_segment_size=0,
        local_buffer_size=16 * 1024 * 1024,
    )
    print(f"[client] ✅ Connected at {client.get_hostname()}", flush=True)

    passed = 0; failed = 0
    def check(name, ok, detail=""):
        nonlocal passed, failed
        if ok:
            print(f"[client]   ✅ {name} {detail}", flush=True)
            passed += 1
        else:
            print(f"[client]   ❌ {name} {detail}", flush=True)
            failed += 1

    # Test 1: put
    print("[client] [1/5] put('hello', ...) ...", flush=True)
    try:
        await client.put("hello", b"world from podman")
        check("put", True)
    except Exception as e:
        check("put", False, str(e))

    # Test 2: get
    print("[client] [2/5] get('hello') ...", flush=True)
    try:
        data = await client.get("hello")
        decoded = data.decode()
        ok = decoded == "world from podman"
        check("get", ok, f"got '{decoded}'" if not ok else f"'{decoded}'")
    except Exception as e:
        check("get", False, str(e))

    # Test 3: exists
    print("[client] [3/5] exists('hello') ...", flush=True)
    try:
        exists_ok = await client.exists("hello")
        check("exists", exists_ok, f"result={exists_ok}")
    except Exception as e:
        check("exists", False, str(e))

    # Test 4: remove (force=True bypasses lease TTL)
    print("[client] [4/5] remove('hello') ...", flush=True)
    try:
        await client.remove("hello", True)
        gone = not await client.exists("hello")
        check("remove", gone, "key gone after remove" if gone else "key still exists!")
    except Exception as e:
        check("remove", False, str(e))

    # Test 5: batch (batch_get returns list of Optional[bytes])
    print("[client] [5/5] batch_put/batch_get ...", flush=True)
    try:
        await client.batch_put(["b1", "b2"], [b"d1", b"d2"])
        results = await client.batch_get(["b1", "b2"])
        if isinstance(results, list) and len(results) == 2:
            r1 = results[0] if results[0] is not None else b""
            r2 = results[1] if results[1] is not None else b""
            ok = r1 == b"d1" and r2 == b"d2"
        else:
            ok = False
        check("batch", ok, f"results ok={ok}")
    except Exception as e:
        check("batch", False, str(e))

    # close is synchronous (not async) in the Rust binding
    client.close()
    print(f"\n[client] {'🎯' if failed==0 else '❌'} {passed}/{passed+failed} tests passed", flush=True)
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
