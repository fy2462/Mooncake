import importlib.util
from pathlib import Path

import pytest


SUITE = Path(__file__).parents[1]


def load_script(name, filename):
    spec = importlib.util.spec_from_file_location(name, SUITE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


STORE = load_script("store_e2e_multilevel", "store-e2e.py")


class TieredClient:
    def __init__(self, *, corrupt_fallback=False, promote=True):
        self.values = {}
        self.parts = {}
        self.states = {}
        self.closed = False
        self.corrupt_fallback = corrupt_fallback
        self.promote = promote

    async def put(self, key, value, config=None):
        self.values[key] = value
        self.states[key] = "memory"

    async def put_parts(self, key, parts, config=None):
        self.parts[key] = list(parts)
        self.values[key] = b"".join(parts)
        self.states[key] = "memory"

    async def upsert(self, key, value, config=None):
        self.values[key] = value

    async def get(self, key):
        value = self.values[key]
        if self.states.get(key) == "disk":
            if self.promote:
                self.states[key] = "promoted"
            if self.corrupt_fallback:
                return value[:-1] + bytes([value[-1] ^ 0xFF])
        return value

    def get_replica_desc(self, key):
        state = self.states.get(key, "memory")
        if state == "disk":
            return [
                {
                    "segment_name": "127.0.0.1:19001",
                    "replica_type": "LocalDisk",
                    "status": "Complete",
                    "handle_valid": True,
                }
            ]
        return [
            {
                "segment_name": "node-a",
                "replica_type": "Memory",
                "protocol": "rdma",
                "status": "Complete",
                "handle_valid": True,
            },
            {
                "segment_name": "node-b",
                "replica_type": "Memory",
                "protocol": "rdma",
                "status": "Complete",
                "handle_valid": True,
            },
        ]

    async def exists(self, key):
        return key in self.values

    async def remove(self, key, force=False):
        assert force
        self.values.pop(key, None)

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_standard_contract_exercises_sizes_parts_concurrency_and_mutation():
    primary = TieredClient()
    workers = [TieredClient(), TieredClient()]
    evidence = {}

    rc = await STORE.run_standard_scenario(
        primary,
        config=object(),
        workers=workers,
        evidence=evidence,
        include_multilevel=False,
    )

    assert rc == 0
    assert set(evidence["objects"]) == {"small", "large", "cross_slice"}
    assert evidence["objects"]["cross_slice"]["part_count"] >= 3
    assert evidence["concurrent"]["operations"] >= 4
    assert evidence["overwrite_delete"] == {"overwrite": True, "delete": True}
    assert primary.closed and all(worker.closed for worker in workers)


@pytest.mark.asyncio
async def test_fallback_is_byte_identical_and_promotion_restores_memory():
    client = TieredClient()
    key = "tiered"
    expected = STORE.payload(3 * 1024 * 1024 + 17, seed=19)
    client.values[key] = expected
    client.states[key] = "disk"

    evidence = await STORE.verify_fallback_and_promotion(
        client, key, expected, timeout=0.05, poll_interval=0
    )

    assert evidence["fallback"]["replica_type"] == "LocalDisk"
    assert evidence["fallback"]["sha256"] == evidence["expected_sha256"]
    assert evidence["promotion"]["memory_replicas"] >= 1


@pytest.mark.asyncio
async def test_corrupted_fallback_is_rejected():
    client = TieredClient(corrupt_fallback=True)
    client.values["tiered"] = b"expected-fallback"
    client.states["tiered"] = "disk"

    with pytest.raises(STORE.ScenarioFailure, match="fallback bytes"):
        await STORE.verify_fallback_and_promotion(
            client, "tiered", b"expected-fallback", timeout=0.05, poll_interval=0
        )


@pytest.mark.asyncio
async def test_missing_promotion_evidence_is_rejected():
    client = TieredClient(promote=False)
    client.values["tiered"] = b"expected-fallback"
    client.states["tiered"] = "disk"

    with pytest.raises(STORE.ScenarioFailure, match="promotion"):
        await STORE.verify_fallback_and_promotion(
            client, "tiered", b"expected-fallback", timeout=0.01, poll_interval=0
        )


def test_store_node_uses_public_rust_ssd_and_watermark_interfaces():
    source = (SUITE / "store-node.py").read_text()
    binding = (Path(__file__).parents[3] / "python/src/client.rs").read_text()

    assert "attach_local_storage_backend" in source
    assert "mount_local_disk_segment" in source
    assert "offload_objects" in source
    assert "promote_objects" in source
    assert "run_disk_watermark_eviction" in source
    assert "fn run_disk_watermark_eviction" in binding


def test_gate_enables_real_offload_and_forbids_cpp_store():
    gate = (SUITE / "run-store-gate.sh").read_text()
    assert "--enable-offload" in gate
    assert "--offload-on-evict" in gate
    assert "libmooncake_store.so" in gate
    assert "store-standard.result" in gate
