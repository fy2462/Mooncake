import asyncio
import importlib.util
import json
from pathlib import Path

import pytest


SUITE = Path(__file__).parents[1]
SCRIPT = SUITE / "store-resilience-e2e.py"


def load_script():
    spec = importlib.util.spec_from_file_location("store_resilience_e2e", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def valid_evidence(module, name):
    common = {"duration_seconds": 0.01}
    values = {
        "store_node_restart": {"restart": True, "post_restart_read": True},
        "master_etcd_restart": {
            "master_recovered": True,
            "etcd_recovered": True,
            "post_restart_read": True,
        },
        "rdma_reconnect": {
            "link_down_observed": True,
            "link_restored": True,
            "post_reconnect_read": True,
        },
        "degraded_read": {
            "initial_owners": 2,
            "unavailable_owner": "node-a",
            "remaining_owners": 1,
            "read_valid": True,
        },
        "watermark_eviction": {
            "memory_offloaded": 1,
            "ssd_evicted": 1,
            "high_ratio": 0.6,
            "low_ratio": 0.3,
        },
        "mixed_stress": {
            "operations": 24,
            "sizes": [4096, 1048576, 8388608],
            "byte_identical": True,
        },
        "second_standard": {"standard_status": "PASS", "complete": True},
    }
    return common | values[name]


def test_required_scenarios_have_strict_evidence_contracts():
    module = load_script()
    assert tuple(module.REQUIRED_SCENARIOS) == (
        "store_node_restart",
        "master_etcd_restart",
        "rdma_reconnect",
        "degraded_read",
        "watermark_eviction",
        "mixed_stress",
        "second_standard",
    )
    for name in module.REQUIRED_SCENARIOS:
        module.validate_evidence(name, valid_evidence(module, name))


@pytest.mark.parametrize(
    ("name", "change", "message"),
    [
        ("rdma_reconnect", {"link_restored": False}, "reconnect"),
        ("degraded_read", {"read_valid": False}, "degraded"),
        ("watermark_eviction", {"ssd_evicted": 0}, "SSD"),
        ("second_standard", {"complete": False}, "standard"),
    ],
)
def test_invalid_acceptance_evidence_fails(name, change, message):
    module = load_script()
    evidence = valid_evidence(module, name) | change
    with pytest.raises(module.ScenarioFailure, match=message):
        module.validate_evidence(name, evidence)


@pytest.mark.asyncio
async def test_bounded_runner_preserves_first_failure_and_restores(tmp_path):
    module = load_script()
    calls = []

    async def failed():
        calls.append("failed")
        raise module.ScenarioFailure("first failure")

    async def passed():
        calls.append("passed")
        return valid_evidence(module, "mixed_stress")

    async def restore(name):
        calls.append(f"restore:{name}")

    result = await module.run_bounded_scenarios(
        [("rdma_reconnect", failed), ("mixed_stress", passed)],
        tmp_path,
        timeout=0.1,
        restore=restore,
    )

    assert result["status"] == "FAIL"
    assert result["first_failure"] == "rdma_reconnect"
    assert result["scenarios"]["mixed_stress"]["status"] == "PASS"
    assert calls == [
        "failed",
        "restore:rdma_reconnect",
        "passed",
        "restore:mixed_stress",
    ]
    assert (
        json.loads((tmp_path / "rdma_reconnect.json").read_text())["status"] == "FAIL"
    )


@pytest.mark.asyncio
async def test_timeout_fails_scenario_and_group(tmp_path):
    module = load_script()

    async def hangs():
        await asyncio.Event().wait()

    result = await module.run_bounded_scenarios(
        [("store_node_restart", hangs)], tmp_path, timeout=0.01
    )

    assert result["status"] == "FAIL"
    assert result["first_failure"] == "store_node_restart"
    scenario = json.loads((tmp_path / "store_node_restart.json").read_text())
    assert scenario["status"] == "FAIL"
    assert scenario["reason"] == "timeout"


def test_runner_mutates_only_manifest_owned_link_without_password_plumbing():
    source = SCRIPT.read_text()
    assert "host-rdma.json" in source
    assert "mc-rdma-net-a" in source and "mc-rdma-net-b" in source
    assert "owned_veth" in source
    assert "sudo" in source
    assert "-S" not in source
    assert "SUDO_ASKPASS" not in source


def test_run_sh_defaults_to_real_resilience_gate():
    source = (SUITE / "run.sh").read_text()
    assert "STORE_GATE_MODE=resilience" in source
    assert "status=NOT_IMPLEMENTED" not in source
