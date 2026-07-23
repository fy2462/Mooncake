#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import time


REQUIRED_SCENARIOS = (
    "store_node_restart",
    "master_etcd_restart",
    "rdma_reconnect",
    "degraded_read",
    "watermark_eviction",
    "mixed_stress",
    "second_standard",
)


class ScenarioFailure(RuntimeError):
    pass


PROCESS_PIDS = r"""import os,sys
needle=sys.argv[1]
matches=[]
for entry in os.listdir("/proc"):
    if entry.isdigit() and int(entry) != os.getpid():
        try:
            command=open(f"/proc/{entry}/cmdline", "rb").read().replace(b"\0", b" ").decode()
            if needle in command:
                matches.append(entry)
        except (FileNotFoundError, PermissionError):
            pass
print("\n".join(matches))
raise SystemExit(0 if matches else 1)"""


STOP_PROCESSES = r"""import os,signal,sys
needle=sys.argv[1]
for entry in os.listdir("/proc"):
    if entry.isdigit() and int(entry) != os.getpid():
        try:
            command=open(f"/proc/{entry}/cmdline", "rb").read().replace(b"\0", b" ").decode()
            if needle in command:
                os.kill(int(entry), signal.SIGTERM)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass"""


def _require(condition, message):
    if not condition:
        raise ScenarioFailure(message)


def validate_evidence(name, evidence):
    _require(evidence.get("duration_seconds", 0) >= 0, "missing scenario duration")
    if name == "store_node_restart":
        _require(evidence.get("restart") is True, "Store node did not restart")
        _require(evidence.get("post_restart_read") is True, "post-restart read failed")
    elif name == "master_etcd_restart":
        _require(evidence.get("master_recovered") is True, "Master did not recover")
        _require(evidence.get("etcd_recovered") is True, "etcd did not recover")
        _require(evidence.get("post_restart_read") is True, "post-restart read failed")
    elif name == "rdma_reconnect":
        _require(
            evidence.get("link_down_observed") is True, "RDMA interruption not observed"
        )
        _require(evidence.get("link_restored") is True, "RDMA reconnect failed")
        _require(
            evidence.get("post_reconnect_read") is True, "post-reconnect read failed"
        )
    elif name == "degraded_read":
        _require(
            evidence.get("initial_owners", 0) >= 2, "degraded read lacked two owners"
        )
        _require(
            bool(evidence.get("unavailable_owner")), "unavailable owner not recorded"
        )
        _require(evidence.get("remaining_owners", 0) >= 1, "no remaining replica owner")
        _require(evidence.get("read_valid") is True, "invalid degraded read")
    elif name == "watermark_eviction":
        _require(
            evidence.get("memory_offloaded", 0) > 0, "memory watermark did not offload"
        )
        _require(evidence.get("ssd_evicted", 0) > 0, "SSD watermark did not evict")
        high = evidence.get("high_ratio", 0)
        low = evidence.get("low_ratio", 0)
        _require(0 < low < high <= 1, "invalid SSD watermark ratios")
    elif name == "mixed_stress":
        _require(evidence.get("operations", 0) > 0, "mixed stress ran no operations")
        _require(
            len(set(evidence.get("sizes", []))) >= 3,
            "mixed stress lacked size diversity",
        )
        _require(evidence.get("byte_identical") is True, "mixed stress bytes differ")
    elif name == "second_standard":
        _require(
            evidence.get("standard_status") == "PASS", "second standard run failed"
        )
        _require(evidence.get("complete") is True, "second standard run was incomplete")
    else:
        raise ScenarioFailure(f"unknown scenario: {name}")


def _atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


async def run_bounded_scenarios(scenarios, artifact_root, timeout, restore=None):
    artifact_root = Path(artifact_root)
    artifact_root.mkdir(parents=True, exist_ok=True)
    results = {}
    first_failure = None
    for name, scenario in scenarios:
        started = time.monotonic()
        record = None
        try:
            evidence = await asyncio.wait_for(scenario(), timeout=timeout)
            evidence["duration_seconds"] = round(time.monotonic() - started, 6)
            validate_evidence(name, evidence)
            record = {"status": "PASS", "scenario": name, "evidence": evidence}
        except asyncio.TimeoutError:
            record = {"status": "FAIL", "scenario": name, "reason": "timeout"}
        except Exception as error:
            record = {
                "status": "FAIL",
                "scenario": name,
                "reason": "scenario-failure",
                "error": str(error),
            }
        finally:
            if restore is not None:
                try:
                    await restore(name)
                except Exception as error:
                    if record is None or record.get("status") == "PASS":
                        record = {
                            "status": "FAIL",
                            "scenario": name,
                            "reason": "restore-failure",
                            "error": str(error),
                        }
        if record["status"] != "PASS" and first_failure is None:
            first_failure = name
        results[name] = record
        _atomic_json(artifact_root / f"{name}.json", record)
        with (artifact_root / f"{name}.log").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
    return {
        "status": "PASS" if first_failure is None else "FAIL",
        "first_failure": first_failure,
        "scenarios": results,
    }


class DockerOrchestrator:
    def __init__(self, args):
        self.args = args
        self.artifacts = Path(args.artifact_root)
        self.current_log = None
        self.link_down = False
        self.master_command = json.loads(Path(args.master_command_file).read_text())
        self.node_commands = json.loads(Path(args.node_command_file).read_text())
        self.seed_counter = 0

    async def command(self, command, timeout=30, check=True):
        def execute():
            return subprocess.run(
                command,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                check=False,
            )

        try:
            result = await asyncio.to_thread(execute)
        except subprocess.TimeoutExpired as error:
            raise ScenarioFailure(f"command timed out: {command!r}") from error
        if self.current_log is not None:
            with self.current_log.open("a", encoding="utf-8") as stream:
                stream.write(f"$ {command!r}\n{result.stdout}")
        if check and result.returncode != 0:
            raise ScenarioFailure(
                f"command failed rc={result.returncode}: {command!r}: {result.stdout[-1000:]}"
            )
        return result

    async def wait_until(self, predicate, description, timeout=30, interval=0.2):
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() <= deadline:
            try:
                value = await predicate()
                if value:
                    return value
            except Exception as error:
                last_error = error
            await asyncio.sleep(interval)
        raise ScenarioFailure(f"timed out waiting for {description}: {last_error}")

    async def client_op(self, label, mode, **options):
        result_path = self.artifacts / f"client-{label}.json"
        command = [
            "docker",
            "exec",
            self.args.client,
            "timeout",
            str(int(self.args.scenario_timeout)),
            "python3",
            "/tmp/store-e2e.py",
            "--device",
            self.args.device,
            "--metadata",
            self.args.metadata,
            "--master",
            self.args.master,
            "--result",
            f"/artifacts/{result_path.name}",
            "--mode",
            mode,
            "--client-port",
            str(13000 + self.seed_counter * 10),
        ]
        self.seed_counter += 1
        for key, value in options.items():
            command.extend((f"--{key.replace('_', '-')}", str(value)))
        process = await self.command(
            command, timeout=self.args.scenario_timeout + 10, check=False
        )
        _require(result_path.exists(), f"client operation {label} produced no result")
        result = json.loads(result_path.read_text())
        if process.returncode != 0 or result.get("status") != "PASS":
            raise ScenarioFailure(f"client operation {label} failed: {result}")
        return result

    async def seed(self, label, size=2 * 1024 * 1024, seed=71):
        key = f"resilience-{label}-{self.seed_counter}"
        result = await self.client_op(label, "seed", key=key, size=size, seed=seed)
        owners = [
            replica.get("segment_name")
            for replica in result["replicas"]
            if replica.get("replica_type") == "Memory"
            and replica.get("status") == "Complete"
            and replica.get("handle_valid") is True
        ]
        _require(len(set(owners)) >= 2, f"seed {key} did not have two memory owners")
        return key, result["sha256"], sorted(set(owners))

    async def read_valid(self, label, key, expected_hash):
        result = await self.client_op(label, "get", key=key)
        _require(
            result.get("sha256") == expected_hash,
            "read hash differs from seeded object",
        )
        return True

    async def describe(self, label, key):
        return (await self.client_op(label, "describe", key=key))["replicas"]

    async def process_running(self, container, pattern):
        result = await self.command(
            ["docker", "exec", container, "python3", "-c", PROCESS_PIDS, pattern],
            check=False,
        )
        return result.returncode == 0

    async def start_detached(self, container, command):
        await self.command(["docker", "exec", "-d", container, *command])

    async def stop_process(self, container, pattern):
        await self.command(
            ["docker", "exec", container, "python3", "-c", STOP_PROCESSES, pattern],
            check=False,
        )
        await self.wait_until(
            lambda: self._process_stopped(container, pattern),
            f"{pattern} to stop in {container}",
            timeout=10,
        )

    async def _process_stopped(self, container, pattern):
        return not await self.process_running(container, pattern)

    async def restart_node(self, container):
        command = self.node_commands[container]
        ready_name = command[command.index("--ready") + 1].removeprefix("/artifacts/")
        ready_path = self.artifacts / ready_name
        await self.stop_process(container, "/tmp/store-node.py")
        ready_path.unlink(missing_ok=True)
        await self.start_detached(container, command)
        await self.wait_until(
            lambda: asyncio.to_thread(ready_path.exists),
            f"{container} readiness after restart",
            timeout=30,
        )
        return await self.process_running(container, "/tmp/store-node.py")

    async def restart_master(self):
        await self.stop_process(self.args.master_container, "mooncake-master")
        await self.start_detached(self.args.master_container, self.master_command)
        await self.wait_master()
        return True

    async def wait_master(self):
        async def ready():
            result = await self.command(
                [
                    "docker",
                    "exec",
                    self.args.client,
                    "python3",
                    "-c",
                    "import socket;s=socket.create_connection(('127.0.0.1',50051),1);s.close()",
                ],
                timeout=3,
                check=False,
            )
            return result.returncode == 0

        await self.wait_until(ready, "Master TCP service", timeout=45)

    def owned_link(self):
        manifest = json.loads(Path(self.args.host_rdma_manifest).read_text())
        _require(
            manifest
            == {
                "device": "mc-rdma-rxe",
                "veth": "mc-rdma-net-a",
                "peer_veth": "mc-rdma-net-b",
                "address": "10.90.0.1/30",
                "gid": manifest.get("gid"),
                "owned_rxe": True,
                "owned_veth": True,
            },
            "RDMA link interruption requires the exact owned manifest",
        )
        _require(bool(manifest.get("gid")), "owned RDMA manifest has no GID")
        return manifest["veth"], manifest["peer_veth"]

    async def set_link(self, up):
        veth, peer = self.owned_link()
        state = "up" if up else "down"
        await self.command(["sudo", "ip", "link", "set", "dev", veth, state])
        if up:
            await self.command(["sudo", "ip", "link", "set", "dev", peer, "up"])
        self.link_down = not up
        result = await self.command(["ip", "-j", "link", "show", "dev", veth])
        flags = json.loads(result.stdout)[0].get("flags", [])
        return ("UP" in flags) is up

    async def node_stats(self):
        values = []
        for name in ("store-a.storage.json", "store-b.storage.json"):
            path = self.artifacts / name
            values.append(json.loads(path.read_text()) if path.exists() else {})
        return values

    async def request_watermark(self, high, low):
        request_id = f"watermark-{time.monotonic_ns()}"
        for name in ("store-a.command.json", "store-b.command.json"):
            _atomic_json(
                self.artifacts / name,
                {"id": request_id, "action": "watermark", "high": high, "low": low},
            )

        async def completed():
            stats = await self.node_stats()
            return (
                stats
                if all(s.get("last_command", {}).get("id") == request_id for s in stats)
                else None
            )

        return await self.wait_until(completed, "SSD watermark commands", timeout=30)

    async def scenario_store_node_restart(self):
        key, expected, owners = await self.seed("node-restart")
        unavailable = "127.0.0.1:12401"
        await self.stop_process(self.args.node_a, "/tmp/store-node.py")

        async def owner_removed():
            replicas = await self.describe("node-restart-describe", key)
            current = {
                replica.get("segment_name")
                for replica in replicas
                if replica.get("replica_type") == "Memory"
                and replica.get("status") == "Complete"
            }
            return unavailable not in current and bool(current)

        await self.wait_until(owner_removed, "stopped Store owner removal", timeout=20)
        restarted = await self.restart_node(self.args.node_a)
        return {
            "restart": restarted,
            "initial_owners": len(owners),
            "post_restart_read": await self.read_valid(
                "node-restart-read", key, expected
            ),
        }

    async def scenario_master_etcd_restart(self):
        key, expected, _ = await self.seed("master-etcd")
        await self.command(["docker", "restart", self.args.etcd], timeout=30)

        async def etcd_ready():
            result = await self.command(
                [
                    "docker",
                    "exec",
                    self.args.etcd,
                    "etcdctl",
                    "endpoint",
                    "health",
                    "--endpoints=http://127.0.0.1:2379",
                ],
                timeout=15,
                check=False,
            )
            return result.returncode == 0

        await self.wait_until(etcd_ready, "etcd health after restart", timeout=30)
        etcd_recovered = True
        master_recovered = await self.restart_master()
        return {
            "master_recovered": master_recovered,
            "etcd_recovered": etcd_recovered,
            "post_restart_read": await self.read_valid(
                "master-etcd-read", key, expected
            ),
        }

    async def scenario_rdma_reconnect(self):
        key, expected, _ = await self.seed("rdma-reconnect")
        link_down_observed = await self.set_link(False)
        interrupted = await self.command(
            [
                "docker",
                "exec",
                self.args.client,
                "timeout",
                "5",
                "python3",
                "/tmp/store-e2e.py",
                "--device",
                self.args.device,
                "--metadata",
                self.args.metadata,
                "--master",
                self.args.master,
                "--result",
                "/artifacts/client-link-down.json",
                "--mode",
                "get",
                "--key",
                key,
            ],
            timeout=10,
            check=False,
        )
        _require(
            interrupted.returncode != 0,
            "RDMA read unexpectedly succeeded while link was down",
        )
        link_restored = await self.set_link(True)
        return {
            "link_down_observed": link_down_observed,
            "interruption_failed_read": True,
            "link_restored": link_restored,
            "post_reconnect_read": await self.read_valid(
                "rdma-reconnected", key, expected
            ),
        }

    async def scenario_degraded_read(self):
        key, expected, owners = await self.seed("degraded")
        unavailable = "127.0.0.1:12401"
        await self.stop_process(self.args.node_a, "/tmp/store-node.py")

        async def one_owner():
            replicas = await self.describe("degraded-describe", key)
            current = {
                replica.get("segment_name")
                for replica in replicas
                if replica.get("replica_type") == "Memory"
                and replica.get("status") == "Complete"
                and replica.get("handle_valid") is True
            }
            return current if unavailable not in current and len(current) == 1 else None

        remaining = await self.wait_until(
            one_owner, "one-owner degraded metadata", timeout=20
        )
        valid = await self.read_valid("degraded-read", key, expected)
        await self.restart_node(self.args.node_a)
        return {
            "initial_owners": len(owners),
            "unavailable_owner": unavailable,
            "remaining_owners": len(remaining),
            "read_valid": valid,
        }

    async def scenario_watermark_eviction(self):
        before = await self.node_stats()
        await self.client_op(
            "watermark-fill", "standard", prefix="watermark", tier_timeout=45
        )
        after_fill = await self.node_stats()
        memory_offloaded = sum(s.get("offloaded", 0) for s in after_fill) - sum(
            s.get("offloaded", 0) for s in before
        )
        after_evict = await self.request_watermark(0.10, 0.05)
        ssd_evicted = sum(
            s.get("last_command", {}).get("evicted", 0) for s in after_evict
        )
        return {
            "memory_offloaded": memory_offloaded,
            "ssd_evicted": ssd_evicted,
            "high_ratio": 0.10,
            "low_ratio": 0.05,
        }

    async def scenario_mixed_stress(self):
        result = await self.client_op("mixed-stress", "stress", prefix="mixed-stress")
        return {
            "operations": result["operations"],
            "sizes": result["sizes"],
            "byte_identical": result["byte_identical"],
        }

    async def scenario_second_standard(self):
        result = await self.client_op(
            "second-standard", "standard", prefix="second-standard", tier_timeout=45
        )
        evidence = result.get("evidence", {})
        complete = all(
            name in evidence
            for name in ("objects", "concurrent", "overwrite_delete", "multilevel")
        )
        return {"standard_status": result["status"], "complete": complete}

    async def restore(self, _name):
        if self.link_down:
            await self.set_link(True)
        inspect = await self.command(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.args.etcd],
            check=False,
        )
        if inspect.returncode != 0 or inspect.stdout.strip() != "true":
            await self.command(["docker", "start", self.args.etcd])
        if not await self.process_running(
            self.args.master_container, "mooncake-master"
        ):
            await self.start_detached(self.args.master_container, self.master_command)
            await self.wait_master()
        for node in (self.args.node_a, self.args.node_b):
            if not await self.process_running(node, "/tmp/store-node.py"):
                await self.restart_node(node)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--master-command-file", required=True)
    parser.add_argument("--node-command-file", required=True)
    parser.add_argument(
        "--host-rdma-manifest",
        default="/home/fy2462/workspace/tmp/mooncake/rdma-multinode/host-rdma.json",
    )
    parser.add_argument("--master-container", default="mc-rdma-rust-master")
    parser.add_argument("--node-a", default="mc-rdma-store-node-a")
    parser.add_argument("--node-b", default="mc-rdma-store-node-b")
    parser.add_argument("--client", default="mc-rdma-store-test-client")
    parser.add_argument("--etcd", default="mc-rdma-etcd")
    parser.add_argument("--device", default="mc-rdma-rxe")
    parser.add_argument("--metadata", default="127.0.0.1:2379")
    parser.add_argument("--master", default="127.0.0.1:50051")
    parser.add_argument("--scenario-timeout", type=float, default=120)
    args = parser.parse_args()
    orchestrator = DockerOrchestrator(args)
    scenarios = [
        (name, getattr(orchestrator, f"scenario_{name}")) for name in REQUIRED_SCENARIOS
    ]
    wrapped = []
    for name, scenario in scenarios:

        async def run_with_log(scenario=scenario, name=name):
            orchestrator.current_log = Path(args.artifact_root) / f"{name}.log"
            return await scenario()

        wrapped.append((name, run_with_log))
    result = await run_bounded_scenarios(
        wrapped,
        args.artifact_root,
        args.scenario_timeout,
        restore=orchestrator.restore,
    )
    _require(
        set(result["scenarios"]) == set(REQUIRED_SCENARIOS),
        "missing resilience scenario",
    )
    _atomic_json(args.result, result)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
