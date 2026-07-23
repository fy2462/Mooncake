import asyncio
import builtins
import importlib.util
import io
import inspect
import json
import os
from pathlib import Path
import signal
import sys
import textwrap
import time
from types import SimpleNamespace

import pytest


SUITE = Path(__file__).parents[1]
SCRIPT = SUITE / "store-resilience-e2e.py"


def load_script():
    spec = importlib.util.spec_from_file_location("store_resilience_e2e", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def install_fake_docker(tmp_path, monkeypatch):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state_root = tmp_path / "fake-docker-state"
    state_root.mkdir()
    docker = fake_bin / "docker"
    docker.write_text(
        textwrap.dedent(
            r"""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root = Path(os.environ["FAKE_DOCKER_STATE"])
args = sys.argv[1:]

if args[0] == "exec":
    index = 1
    token = None
    while index < len(args) and args[index].startswith("-"):
        if args[index] == "-e":
            assignment = args[index + 1]
            if assignment.startswith("MOONCAKE_RDMA_EXEC_TOKEN="):
                token = assignment.split("=", 1)[1]
            index += 2
        else:
            index += 1
    container = args[index]
    inner = args[index + 1:]
    if len(inner) >= 4 and inner[:2] == ["python3", "-c"] and "MOONCAKE_DOCKER_STOP_V1" in inner[2]:
        stop_token = inner[3]
        pid_file = root / f"exec-{stop_token}.pid"
        deadline = time.monotonic() + 1.0
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        observed = pid_file.exists()
        absent = True
        if observed:
            pid = int(pid_file.read_text())
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.01)
            else:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                absent = True
                pid_file.unlink(missing_ok=True)
            else:
                absent = False
        print(json.dumps({"observed": observed, "absent": absent}))
        print("stopper-stderr", file=sys.stderr, flush=True)
        raise SystemExit(0 if absent else 1)
    if inner and inner[0] == "fake-delayed":
        marker = inner[1]
        worker = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import pathlib,sys,time;time.sleep(0.25);pathlib.Path(sys.argv[1]).write_text('late')",
                marker,
            ],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        owned = token or "unowned"
        (root / f"exec-{owned}.pid").write_text(str(worker.pid))
        (root / "exec-started").write_text(owned)
        print("exec-daemon-stdout", flush=True)
        print("exec-daemon-stderr", file=sys.stderr, flush=True)
        status = worker.wait()
        (root / f"exec-{owned}.pid").unlink(missing_ok=True)
        raise SystemExit(status)
    raise SystemExit(0)

if args[0] == "restart":
    state = root / "restart-state"
    state.write_text("restarting")
    subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import pathlib,sys,time;time.sleep(0.8);pathlib.Path(sys.argv[1]).write_text('running:healthy')",
            str(state),
        ],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print("restart-daemon-stderr", file=sys.stderr, flush=True)
    time.sleep(10)

if args[0] == "inspect":
    state = (root / "restart-state").read_text()
    healthy = state == "running:healthy"
    print(json.dumps({
        "Status": "running" if healthy else "restarting",
        "Running": healthy,
        "Health": {"Status": "healthy" if healthy else "starting"},
    }))
    raise SystemExit(0)

raise SystemExit(0)
"""
        )
    )
    docker.chmod(0o755)
    monkeypatch.setenv("FAKE_DOCKER_STATE", str(state_root))
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    return state_root


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
            "interruption_failed_read": True,
            "link_restored": True,
            "post_reconnect_read": True,
        },
        "degraded_read": {
            "initial_owners": 2,
            "unavailable_owner": "node-a",
            "remaining_owners": 1,
            "byte_valid": True,
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


def test_docker_stopper_does_not_signal_reused_pid(monkeypatch, capsys):
    module = load_script()
    pid = 424242
    token = "pid-reuse-probe"
    marker = f"MOONCAKE_RDMA_EXEC_TOKEN={token}".encode()
    state = {"starttime": "101", "clock": 0.0}
    signals = []
    real_open = builtins.open
    real_listdir = os.listdir
    real_exists = os.path.exists

    def stat_record():
        fields = ["S", *("0" for _ in range(49))]
        fields[19] = state["starttime"]
        return f"{pid} (worker ) with parens) {' '.join(fields)}\n".encode()

    def fake_open(path, mode="r", *args, **kwargs):
        if path == f"/proc/{pid}/environ":
            return io.BytesIO(marker + b"\0")
        if path == f"/proc/{pid}/stat":
            return io.BytesIO(stat_record())
        return real_open(path, mode, *args, **kwargs)

    def fake_listdir(path):
        if path == "/proc":
            return [str(pid)]
        return real_listdir(path)

    def fake_exists(path):
        if path == f"/proc/{pid}":
            return True
        return real_exists(path)

    def fake_kill(target, sent_signal):
        assert target == pid
        signals.append(sent_signal)
        if sent_signal == signal.SIGTERM:
            state["starttime"] = "202"

    def fake_monotonic():
        state["clock"] += 0.1
        return state["clock"]

    monkeypatch.setattr(builtins, "open", fake_open)
    monkeypatch.setattr(os, "listdir", fake_listdir)
    monkeypatch.setattr(os.path, "exists", fake_exists)
    monkeypatch.setattr(os, "kill", fake_kill)
    monkeypatch.setattr(time, "monotonic", fake_monotonic)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(sys, "argv", ["stopper", token, "0.3"])

    with pytest.raises(SystemExit) as exit_info:
        exec(module.STOP_DOCKER_EXEC, {})

    assert exit_info.value.code == 0
    assert signals == [signal.SIGTERM]
    stopper_state = json.loads(capsys.readouterr().out)
    assert stopper_state == {"observed": True, "absent": True, "remaining": []}


def test_required_scenarios_have_strict_evidence_contracts():
    module = load_script()
    assert tuple(module.REQUIRED_SCENARIOS) == (
        "second_standard",
        "mixed_stress",
        "watermark_eviction",
        "degraded_read",
        "store_node_restart",
        "master_etcd_restart",
        "rdma_reconnect",
    )
    for name in module.REQUIRED_SCENARIOS:
        module.validate_evidence(name, valid_evidence(module, name))


def test_rxe_reconnect_restarts_affected_store_before_validation_read():
    module = load_script()
    source = inspect.getsource(module.DockerOrchestrator.scenario_rdma_reconnect)
    restart = source.index("await self.restart_node(self.args.node_c)")
    read = source.index('"rdma-reconnected"')
    assert restart < read


@pytest.mark.parametrize(
    ("name", "change", "message"),
    [
        ("rdma_reconnect", {"link_restored": False}, "reconnect"),
        ("rdma_reconnect", {"interruption_failed_read": False}, "failed read"),
        ("degraded_read", {"remaining_owners": 2}, "exactly one"),
        ("degraded_read", {"byte_valid": False}, "degraded"),
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


@pytest.mark.asyncio
async def test_cancelled_command_kills_process_group_before_returning(tmp_path):
    module = load_script()
    master_commands = tmp_path / "master.json"
    node_commands = tmp_path / "nodes.json"
    master_commands.write_text("[]\n")
    node_commands.write_text("{}\n")
    args = SimpleNamespace(
        artifact_root=str(tmp_path),
        master_command_file=str(master_commands),
        node_command_file=str(node_commands),
    )
    orchestrator = module.DockerOrchestrator(args)
    delayed_marker = tmp_path / "delayed-mutation"
    command = [
        sys.executable,
        "-c",
        (
            "import pathlib,subprocess,time,sys;"
            "subprocess.Popen([sys.executable,'-c',"
            f'"import time,pathlib;time.sleep(0.15);pathlib.Path({str(delayed_marker)!r}).touch()"]);'
            "time.sleep(10)"
        ),
    ]

    task = asyncio.create_task(orchestrator.command(command, timeout=5))
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.25)

    assert (
        not delayed_marker.exists()
    ), "cancelled child mutated state after restoration began"


@pytest.mark.asyncio
async def test_command_timeout_kills_process_group_before_returning(tmp_path):
    module = load_script()
    master_commands = tmp_path / "master.json"
    node_commands = tmp_path / "nodes.json"
    master_commands.write_text("[]\n")
    node_commands.write_text("{}\n")
    orchestrator = module.DockerOrchestrator(
        SimpleNamespace(
            artifact_root=str(tmp_path),
            master_command_file=str(master_commands),
            node_command_file=str(node_commands),
        )
    )
    delayed_marker = tmp_path / "timeout-mutation"
    command = [
        sys.executable,
        "-c",
        (
            "import subprocess,time,sys;"
            "subprocess.Popen([sys.executable,'-c',"
            f'"import time,pathlib;time.sleep(0.15);pathlib.Path({str(delayed_marker)!r}).touch()"]);'
            "time.sleep(10)"
        ),
    ]

    with pytest.raises(module.ScenarioFailure, match="timed out"):
        await orchestrator.command(command, timeout=0.03)
    await asyncio.sleep(0.25)

    assert (
        not delayed_marker.exists()
    ), "timed-out child mutated state after restoration began"


@pytest.mark.parametrize("termination", ["cancel", "timeout"])
@pytest.mark.asyncio
async def test_docker_exec_is_absent_before_cancel_or_timeout_returns(
    tmp_path, monkeypatch, termination
):
    module = load_script()
    state_root = install_fake_docker(tmp_path, monkeypatch)
    master_commands = tmp_path / "master.json"
    node_commands = tmp_path / "nodes.json"
    master_commands.write_text("[]\n")
    node_commands.write_text("{}\n")
    orchestrator = module.DockerOrchestrator(
        SimpleNamespace(
            artifact_root=str(tmp_path),
            master_command_file=str(master_commands),
            node_command_file=str(node_commands),
        )
    )
    command_log = tmp_path / f"docker-exec-{termination}.log"
    orchestrator.current_log = command_log
    delayed_marker = tmp_path / f"docker-{termination}-late"

    if termination == "cancel":
        task = asyncio.create_task(
            orchestrator.command(
                [
                    "docker",
                    "exec",
                    "fake-container",
                    "fake-delayed",
                    str(delayed_marker),
                ],
                timeout=5,
            )
        )
        deadline = asyncio.get_running_loop().time() + 1
        while not (state_root / "exec-started").exists():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(module.ScenarioFailure, match="timed out"):
            await orchestrator.command(
                [
                    "docker",
                    "exec",
                    "fake-container",
                    "fake-delayed",
                    str(delayed_marker),
                ],
                timeout=0.05,
            )

    await asyncio.sleep(0.3)
    assert (
        not delayed_marker.exists()
    ), "Docker exec mutated state after command returned"
    assert not list(state_root.glob("exec-*.pid")), "owned Docker exec PID remained"
    log = command_log.read_text()
    assert "exec-daemon-stderr" in log
    assert "stopper-stderr" in log


@pytest.mark.parametrize("termination", ["cancel", "timeout"])
@pytest.mark.asyncio
async def test_docker_restart_is_settled_before_cancel_or_timeout_returns(
    tmp_path, monkeypatch, termination
):
    module = load_script()
    state_root = install_fake_docker(tmp_path, monkeypatch)
    master_commands = tmp_path / "master.json"
    node_commands = tmp_path / "nodes.json"
    master_commands.write_text("[]\n")
    node_commands.write_text("{}\n")
    orchestrator = module.DockerOrchestrator(
        SimpleNamespace(
            artifact_root=str(tmp_path),
            master_command_file=str(master_commands),
            node_command_file=str(node_commands),
        )
    )
    command_log = tmp_path / f"docker-restart-{termination}.log"
    orchestrator.current_log = command_log

    if termination == "cancel":
        task = asyncio.create_task(
            orchestrator.command(["docker", "restart", "fake-container"], timeout=5)
        )
        deadline = asyncio.get_running_loop().time() + 1
        while not (state_root / "restart-state").exists():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(module.ScenarioFailure, match="timed out"):
            await orchestrator.command(
                ["docker", "restart", "fake-container"], timeout=0.05
            )

    assert (state_root / "restart-state").read_text() == "running:healthy"
    assert "restart-daemon-stderr" in command_log.read_text()


@pytest.mark.asyncio
async def test_restore_failure_is_recorded_and_gates_later_scenarios(tmp_path):
    module = load_script()
    calls = []
    restore_attempts = 0

    async def first():
        calls.append("first")
        raise module.ScenarioFailure("scenario failed")

    async def second():
        calls.append("second")
        return valid_evidence(module, "mixed_stress")

    async def third():
        calls.append("third")
        return valid_evidence(module, "second_standard")

    async def restore(_name):
        nonlocal restore_attempts
        restore_attempts += 1
        if restore_attempts <= 2:
            raise module.ScenarioFailure(f"restore attempt {restore_attempts} failed")

    result = await module.run_bounded_scenarios(
        [
            ("rdma_reconnect", first),
            ("mixed_stress", second),
            ("second_standard", third),
        ],
        tmp_path,
        timeout=0.1,
        restore=restore,
    )

    assert calls == ["first", "third"]
    assert result["status"] == "FAIL"
    assert result["first_failure"] == "rdma_reconnect"
    assert result["scenarios"]["rdma_reconnect"]["restore"]["status"] == "FAIL"
    assert result["scenarios"]["mixed_stress"]["status"] == "BLOCKED"
    assert result["scenarios"]["second_standard"]["status"] == "PASS"
    first_log = (tmp_path / "rdma_reconnect.log").read_text()
    assert "scenario failed" in first_log
    assert "restore attempt 1 failed" in first_log


@pytest.mark.asyncio
async def test_restore_verifies_all_service_health_even_when_processes_exist():
    module = load_script()
    orchestrator = object.__new__(module.DockerOrchestrator)
    orchestrator.link_down = False
    orchestrator.args = SimpleNamespace(
        etcd="etcd",
        master_container="master",
        node_a="node-a",
        node_b="node-b",
        node_c="node-c",
    )
    calls = []

    async def command(*_args, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="true\n")

    async def process_running(container, pattern):
        calls.append(("running", container, pattern))
        return True

    async def wait_etcd():
        calls.append(("healthy", "etcd"))

    async def wait_master():
        calls.append(("healthy", "master"))

    async def wait_node_healthy(container):
        calls.append(("healthy", container))

    orchestrator.command = command
    orchestrator.process_running = process_running
    orchestrator.wait_etcd = wait_etcd
    orchestrator.wait_master = wait_master
    orchestrator.wait_node_healthy = wait_node_healthy

    await orchestrator.restore("probe")

    assert ("healthy", "etcd") in calls
    assert ("healthy", "master") in calls
    assert ("healthy", "node-a") in calls
    assert ("healthy", "node-b") in calls
    assert ("healthy", "node-c") in calls


@pytest.mark.asyncio
async def test_node_health_accepts_completed_storage_cycle_with_transient_backend_error(
    tmp_path,
):
    module = load_script()
    orchestrator = object.__new__(module.DockerOrchestrator)
    orchestrator.artifacts = tmp_path
    orchestrator.node_commands = {
        "node-a": [
            "python3",
            "/tmp/store-node.py",
            "--ready",
            "/artifacts/node.ready",
            "--stats",
            "/artifacts/node.stats.json",
        ]
    }
    (tmp_path / "node.ready").write_text(
        json.dumps(
            {
                "storage_backend": "RustFilePerKey",
                "cpp_store_loaded": False,
                "pid": 42,
            }
        )
    )
    stats_path = tmp_path / "node.stats.json"
    stats_path.write_text(
        json.dumps({"cycles": 4, "last_error": "local disk segment not found"})
    )

    async def process_running(_container, _pattern):
        return True

    async def wait_until(predicate, _description, **_options):
        assert await predicate() is True

    orchestrator.process_running = process_running
    orchestrator.wait_until = wait_until

    await orchestrator.wait_node_healthy("node-a")


def test_runner_mutates_only_manifest_owned_link_without_password_plumbing():
    source = SCRIPT.read_text()
    assert "host-rdma.json" in source
    assert 'f"mc-rdma-net-{node_name}"' in source
    assert 'f"mc-rdma-peer-{node_name}"' in source
    assert "owned_veth" in source
    assert "sudo" in source
    assert "-S" not in source
    assert "SUDO_ASKPASS" not in source


def test_runner_covers_three_nodes_and_two_replica_degraded_reads():
    source = SCRIPT.read_text()
    assert 'parser.add_argument("--node-c"' in source
    assert "self.args.node_c" in source
    assert '"store-c.storage.json"' in source
    assert '"store-c.command.json"' in source
    assert "replica_num=2" in source
    assert 'set_link(False, "c")' in source


def test_run_sh_defaults_to_real_resilience_gate():
    source = (SUITE / "run.sh").read_text()
    assert "STORE_GATE_MODE=resilience" in source
    assert "status=NOT_IMPLEMENTED" not in source
