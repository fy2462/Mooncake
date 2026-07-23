import importlib.util
import asyncio
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
    def __init__(
        self,
        *,
        corrupt_fallback=False,
        promote=True,
        promoted_segments=("node-a", "node-b"),
    ):
        self.values = {}
        self.parts = {}
        self.states = {}
        self.closed = False
        self.corrupt_fallback = corrupt_fallback
        self.promote = promote
        self.promoted_segments = promoted_segments

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
        segments = (
            self.promoted_segments
            if state == "promoted"
            else ("node-a", "node-b", "node-c")
        )
        return [
            {
                "segment_name": segment,
                "replica_type": "Memory",
                "protocol": "rdma",
                "status": "Complete",
                "handle_valid": True,
            }
            for segment in segments
        ]

    async def exists(self, key):
        return key in self.values

    async def remove(self, key, force=False):
        assert force
        self.values.pop(key, None)

    def close(self):
        self.closed = True


class NonReentrantClient(TieredClient):
    def __init__(self):
        super().__init__()
        self.active = False

    async def put(self, key, value, config=None):
        if self.active:
            raise RuntimeError("client operation overlapped")
        self.active = True
        try:
            await asyncio.sleep(0)
            await super().put(key, value, config)
        finally:
            self.active = False


def test_multilevel_pressure_exceeds_the_cluster_high_watermark():
    pressure_bytes = STORE.PRESSURE_OBJECT_COUNT * STORE.PRESSURE_OBJECT_BYTES
    cluster_high_watermark = (
        STORE.STORE_NODE_COUNT
        * STORE.STORE_NODE_SEGMENT_BYTES
        * STORE.STORE_EVICTION_HIGH_WATERMARK
    )

    assert pressure_bytes > cluster_high_watermark


def test_standard_profile_requires_three_distinct_rdma_replicas():
    replicas = TieredClient().get_replica_desc("key")
    assert STORE._three_rdma_replicas(replicas) == [
        "node-a",
        "node-b",
        "node-c",
    ]

    with pytest.raises(STORE.ScenarioFailure, match="3 complete RDMA"):
        STORE._three_rdma_replicas(replicas[:2])


def test_store_client_ports_do_not_collide_with_three_store_nodes():
    worker_ports = {
        STORE.DEFAULT_CLIENT_PORT + offset for offset in range(3)
    }
    assert worker_ports.isdisjoint(STORE.STORE_NODE_PORTS)

    async def get(self, key):
        if self.active:
            raise RuntimeError("client operation overlapped")
        self.active = True
        try:
            await asyncio.sleep(0)
            return await super().get(key)
        finally:
            self.active = False


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
async def test_concurrency_is_across_clients_without_reentrant_client_use():
    workers = [NonReentrantClient(), NonReentrantClient()]

    evidence = await STORE._concurrent_case(
        workers, object(), "non-reentrant", sizes=(4096, 8192)
    )

    assert evidence["operations"] == 4


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
async def test_disk_only_candidate_is_selected_from_actual_eviction_victims():
    client = TieredClient()
    client.values.update({"partial": b"partial", "disk": b"disk"})
    client.states.update({"partial": "memory", "disk": "disk"})
    candidates = {
        "partial": (b"partial", ("node-a", "node-b")),
        "disk": (b"disk", ("node-a", "node-b")),
    }

    key, expected, segments = await STORE.wait_for_disk_only_candidate(
        client, candidates, timeout=0.05, poll_interval=0
    )

    assert (key, expected, segments) == (
        "disk",
        b"disk",
        ("node-a", "node-b"),
    )


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


@pytest.mark.asyncio
async def test_promotion_requires_two_distinct_rdma_replicas():
    client = TieredClient(promoted_segments=("node-a",))
    client.values["tiered"] = b"expected-fallback"
    client.states["tiered"] = "disk"

    with pytest.raises(STORE.ScenarioFailure, match="two complete RDMA replicas"):
        await STORE.verify_fallback_and_promotion(
            client, "tiered", b"expected-fallback", timeout=0.05, poll_interval=0
        )


@pytest.mark.asyncio
async def test_single_replica_tier_round_trip_restores_original_topology():
    client = TieredClient(promoted_segments=("node-a",))
    client.values["tiered"] = b"expected-fallback"
    client.states["tiered"] = "disk"

    evidence = await STORE.verify_fallback_and_promotion(
        client,
        "tiered",
        b"expected-fallback",
        timeout=0.05,
        poll_interval=0,
        expected_segments=("node-a",),
    )

    assert evidence["promotion"]["memory_replicas"] == 1
    assert evidence["promotion"]["remote_segments"] == ["node-a"]


@pytest.mark.asyncio
async def test_promotion_requires_consistent_memory_topology():
    client = TieredClient(promoted_segments=("node-a", "node-c"))
    client.values["tiered"] = b"expected-fallback"
    client.states["tiered"] = "disk"

    with pytest.raises(STORE.ScenarioFailure, match="topology"):
        await STORE.verify_fallback_and_promotion(
            client,
            "tiered",
            b"expected-fallback",
            timeout=0.05,
            poll_interval=0,
            expected_segments=("node-a", "node-b"),
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
