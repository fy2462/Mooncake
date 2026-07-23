#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
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


STOP_DOCKER_EXEC = r"""# MOONCAKE_DOCKER_STOP_V1
import json,os,signal,sys,time
token=sys.argv[1]
marker=f"MOONCAKE_RDMA_EXEC_TOKEN={token}".encode()
deadline=time.monotonic()+float(sys.argv[2])
observed=False
owned={}
retired=set()

def starttime(pid):
    try:
        record=open(f"/proc/{pid}/stat","rb").read()
    except (FileNotFoundError,ProcessLookupError,PermissionError):
        return None
    close=record.rfind(b")")
    fields=record[close+1:].split() if close>=0 else []
    return fields[19] if len(fields)>19 else None

def has_token(pid):
    try:
        values=open(f"/proc/{pid}/environ","rb").read().split(b"\0")
        return marker in values
    except (FileNotFoundError,ProcessLookupError,PermissionError):
        return False

def snapshot(pid):
    before=starttime(pid)
    if before is None or not has_token(pid):
        return None
    return before if starttime(pid)==before else None

def matches_identity(pid,original_starttime):
    before=starttime(pid)
    return (
        before==original_starttime
        and has_token(pid)
        and starttime(pid)==original_starttime
    )

def discover():
    found=[]
    for entry in os.listdir("/proc"):
        if not entry.isdigit() or int(entry)==os.getpid():
            continue
        pid=int(entry)
        identity=snapshot(pid)
        if identity is not None:
            found.append((pid,identity))
    return found

def refresh():
    global observed
    for pid,identity in discover():
        observed=True
        if pid not in owned:
            owned[pid]=identity

def original_is_live(pid,identity):
    if pid in retired:
        return False
    if matches_identity(pid,identity):
        return True
    retired.add(pid)
    return False

def remaining_originals():
    return [pid for pid,identity in owned.items() if original_is_live(pid,identity)]

while time.monotonic()<deadline:
    refresh()
    if owned:
        break
    time.sleep(0.02)

for sig,grace in ((signal.SIGTERM,0.5),(signal.SIGKILL,0.5)):
    refresh()
    for pid,identity in owned.items():
        if original_is_live(pid,identity):
            try:
                os.kill(pid,sig)
            except ProcessLookupError:
                pass
    until=time.monotonic()+grace
    remaining=remaining_originals()
    while time.monotonic()<until:
        refresh()
        remaining=remaining_originals()
        if not remaining:
            break
        time.sleep(0.02)
    if not remaining:
        break

refresh()
remaining=remaining_originals()
print(json.dumps({"observed":observed,"absent":not remaining,"remaining":remaining}))
raise SystemExit(0 if not remaining else 1)"""


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
        _require(
            evidence.get("interruption_failed_read") is True,
            "RDMA interruption did not prove a failed read",
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
        _require(
            evidence.get("remaining_owners") == 1,
            "degraded read did not retain exactly one owner",
        )
        _require(evidence.get("byte_valid") is True, "invalid degraded read bytes")
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
    restoration_healthy = True

    def restore_failure(error):
        return {
            "status": "FAIL",
            "error": str(error) or error.__class__.__name__,
        }

    def publish(name, record):
        _atomic_json(artifact_root / f"{name}.json", record)
        with (artifact_root / f"{name}.log").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")

    for name, scenario in scenarios:
        if restore is not None and not restoration_healthy:
            try:
                await asyncio.wait_for(restore(name), timeout=timeout)
                restoration_healthy = True
            except Exception as error:
                record = {
                    "status": "BLOCKED",
                    "scenario": name,
                    "reason": "restore-gate",
                    "restore": restore_failure(error),
                }
                if first_failure is None:
                    first_failure = name
                results[name] = record
                publish(name, record)
                continue

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
                    await asyncio.wait_for(restore(name), timeout=timeout)
                except Exception as error:
                    restoration_healthy = False
                    record["restore"] = restore_failure(error)
                    if record.get("status") == "PASS":
                        record["status"] = "FAIL"
                        record["reason"] = "restore-failure"
        if record["status"] != "PASS" and first_failure is None:
            first_failure = name
        results[name] = record
        publish(name, record)
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
        docker_exec = self._docker_exec_details(command)
        docker_state_container = self._docker_state_container(command)
        exec_token = None
        executed_command = list(command)
        if docker_exec is not None:
            container, _detached = docker_exec
            exec_token = f"{os.getpid()}-{time.monotonic_ns()}"
            executed_command = [
                command[0],
                "exec",
                "-e",
                f"MOONCAKE_RDMA_EXEC_TOKEN={exec_token}",
                *command[2:],
            ]
        process = await asyncio.create_subprocess_exec(
            *executed_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
        )
        communicate = asyncio.create_task(process.communicate())
        try:
            stdout, _ = await asyncio.wait_for(
                asyncio.shield(communicate), timeout=timeout
            )
        except asyncio.TimeoutError as error:
            await self._reconcile_and_log(
                command,
                process,
                communicate,
                docker_exec,
                exec_token,
                docker_state_container,
            )
            raise ScenarioFailure(f"command timed out: {command!r}") from error
        except asyncio.CancelledError:
            await self._reconcile_and_log(
                command,
                process,
                communicate,
                docker_exec,
                exec_token,
                docker_state_container,
            )
            raise
        output = stdout.decode(errors="replace")
        result = subprocess.CompletedProcess(command, process.returncode, output)
        self._write_command_log(command, output)
        if check and result.returncode != 0:
            raise ScenarioFailure(
                f"command failed rc={result.returncode}: {command!r}: {result.stdout[-1000:]}"
            )
        return result

    def _docker_exec_details(self, command):
        if (
            len(command) < 4
            or Path(command[0]).name != "docker"
            or command[1] != "exec"
        ):
            return None
        index = 2
        detached = False
        options_with_values = {"-e", "--env", "-u", "--user", "-w", "--workdir"}
        while index < len(command) and command[index].startswith("-"):
            option = command[index]
            detached = detached or option in {"-d", "--detach"}
            index += 2 if option in options_with_values else 1
        _require(index < len(command), f"Docker exec has no container: {command!r}")
        return command[index], detached

    def _docker_state_container(self, command):
        if (
            len(command) >= 3
            and Path(command[0]).name == "docker"
            and command[1] in {"restart", "start"}
        ):
            return command[-1]
        return None

    async def _reconcile_and_log(
        self,
        command,
        process,
        communicate,
        docker_exec,
        exec_token,
        docker_state_container,
    ):
        try:
            output, reconciliation = await self._reconcile_interrupted_docker(
                command,
                process,
                communicate,
                docker_exec,
                exec_token,
                docker_state_container,
            )
        except Exception as error:
            try:
                output, _forced = await self._settle_local_cli(process, communicate)
            except Exception as settle_error:
                output = f"failed to settle local Docker CLI: {settle_error}\n"
            self._write_command_log(
                command,
                output,
                f"Docker reconciliation failed: {error}\n",
            )
            raise
        self._write_command_log(command, output, reconciliation)

    async def _reconcile_interrupted_docker(
        self,
        command,
        process,
        communicate,
        docker_exec,
        exec_token,
        docker_state_container,
    ):
        reconciliation = []
        if docker_exec is not None:
            container, _detached = docker_exec
            completed_before_stop = communicate.done()
            stop = await self._raw_command(
                [
                    command[0],
                    "exec",
                    container,
                    "python3",
                    "-c",
                    STOP_DOCKER_EXEC,
                    exec_token,
                    "2.0",
                ],
                timeout=4,
            )
            reconciliation.append(stop.stdout)
            state = self._parse_stopper_state(stop.stdout)
            output, forced_local_stop = await self._settle_local_cli(
                process, communicate
            )
            if stop.returncode != 0 or not state.get("absent"):
                raise ScenarioFailure(
                    f"Docker exec token {exec_token} was not absent after interruption: {stop.stdout}"
                )
            if (
                not state.get("observed")
                and not completed_before_stop
                and forced_local_stop
            ):
                raise ScenarioFailure(
                    f"Docker exec token {exec_token} never reached a settled daemon state"
                )
            return output, "".join(reconciliation)
        if docker_state_container is not None:
            state_output = await self._wait_container_settled(
                command[0], docker_state_container
            )
            reconciliation.append(state_output)
            output, _forced = await self._settle_local_cli(process, communicate)
            return output, "".join(reconciliation)
        output = await self._terminate_process_group(process, communicate)
        return output.decode(errors="replace"), ""

    async def _raw_command(self, command, timeout):
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
        )
        communicate = asyncio.create_task(process.communicate())
        try:
            stdout, _ = await asyncio.wait_for(
                asyncio.shield(communicate), timeout=timeout
            )
        except asyncio.TimeoutError as error:
            stdout = await self._terminate_process_group(process, communicate)
            raise ScenarioFailure(
                f"Docker reconciliation command timed out: {command!r}: "
                f"{stdout.decode(errors='replace')}"
            ) from error
        return subprocess.CompletedProcess(
            command, process.returncode, stdout.decode(errors="replace")
        )

    def _parse_stopper_state(self, output):
        for line in output.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and "absent" in value:
                return value
        return {}

    async def _wait_container_settled(self, docker, container):
        deadline = time.monotonic() + 4
        evidence = []
        while time.monotonic() < deadline:
            inspect = await self._raw_command(
                [docker, "inspect", "-f", "{{json .State}}", container],
                timeout=1,
            )
            evidence.append(inspect.stdout)
            if inspect.returncode == 0:
                try:
                    state = json.loads(inspect.stdout.strip())
                except ValueError:
                    state = {}
                health = state.get("Health", {}).get("Status")
                if (
                    state.get("Running") is True
                    and state.get("Status") == "running"
                    and health
                    in {
                        None,
                        "healthy",
                    }
                ):
                    return "".join(evidence)
            await asyncio.sleep(0.05)
        raise ScenarioFailure(
            f"Docker container {container} did not settle running/healthy: {''.join(evidence)}"
        )

    async def _settle_local_cli(self, process, communicate):
        try:
            stdout, _ = await asyncio.wait_for(asyncio.shield(communicate), timeout=0.5)
            forced = False
        except asyncio.TimeoutError:
            stdout = await self._terminate_process_group(process, communicate)
            forced = True
        return stdout.decode(errors="replace"), forced

    def _write_command_log(self, command, output, reconciliation=""):
        if self.current_log is not None:
            with self.current_log.open("a", encoding="utf-8") as stream:
                stream.write(f"$ {command!r}\n{output}")
                if reconciliation:
                    stream.write(f"\n[reconciliation]\n{reconciliation}")

    async def _terminate_process_group(self, process, communicate):
        if process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                stdout, _ = await asyncio.wait_for(
                    asyncio.shield(communicate), timeout=2
                )
            except asyncio.TimeoutError:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                stdout, _ = await asyncio.wait_for(
                    asyncio.shield(communicate), timeout=2
                )
        else:
            stdout, _ = await communicate
        return stdout

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

    async def seed(self, label, size=2 * 1024 * 1024, seed=71, replica_num=3):
        key = f"resilience-{label}-{self.seed_counter}"
        result = await self.client_op(
            label,
            "seed",
            key=key,
            size=size,
            seed=seed,
            replica_num=replica_num,
        )
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

    async def seed_on_owner(self, label, required_owner, replica_num):
        for attempt in range(10):
            seeded = await self.seed(
                f"{label}-{attempt}", replica_num=replica_num
            )
            if required_owner in seeded[2]:
                return seeded
        raise ScenarioFailure(
            f"could not place {replica_num}-replica object on {required_owner}"
        )

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
        ready_path = self._node_artifact_path(container, "--ready")
        stats_path = self._node_artifact_path(container, "--stats")
        await self.stop_process(container, "/tmp/store-node.py")
        ready_path.unlink(missing_ok=True)
        stats_path.unlink(missing_ok=True)
        await self.start_detached(container, command)
        await self.wait_node_healthy(container)
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

    async def wait_etcd(self):
        async def ready():
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

        await self.wait_until(ready, "etcd endpoint health", timeout=30)

    def _node_artifact_path(self, container, option):
        command = self.node_commands[container]
        value = command[command.index(option) + 1]
        prefix = "/artifacts/"
        _require(value.startswith(prefix), f"{container} {option} is outside artifacts")
        return self.artifacts / value.removeprefix(prefix)

    async def wait_node_healthy(self, container):
        ready_path = self._node_artifact_path(container, "--ready")
        stats_path = self._node_artifact_path(container, "--stats")
        initial_cycles = -1
        if stats_path.exists():
            try:
                initial_cycles = int(
                    json.loads(stats_path.read_text()).get("cycles", -1)
                )
            except (OSError, ValueError, json.JSONDecodeError):
                pass

        async def healthy():
            if not await self.process_running(container, "/tmp/store-node.py"):
                return False
            try:
                ready = json.loads(ready_path.read_text())
                stats = json.loads(stats_path.read_text())
            except (FileNotFoundError, ValueError, json.JSONDecodeError):
                return False
            return (
                ready.get("storage_backend") == "RustFilePerKey"
                and ready.get("cpp_store_loaded") is False
                and int(ready.get("pid", 0)) > 0
                and stats.get("last_error", "") == ""
                and int(stats.get("cycles", -1)) > initial_cycles
            )

        await self.wait_until(
            healthy,
            f"{container} ready storage cycle",
            timeout=30,
            interval=0.1,
        )

    def owned_link(self, node_name):
        manifest = json.loads(Path(self.args.host_rdma_manifest).read_text())
        nodes = manifest.get("nodes") if isinstance(manifest, dict) else None
        _require(
            isinstance(nodes, list) and len(nodes) == 3,
            "RDMA link interruption requires a three-node owned manifest",
        )
        matches = [node for node in nodes if node.get("name") == node_name]
        _require(len(matches) == 1, f"owned RDMA node {node_name} is missing")
        node = matches[0]
        expected = {
            "name": node_name,
            "device": f"mc-rdma-rxe-{node_name}",
            "veth": f"mc-rdma-net-{node_name}",
            "peer_veth": f"mc-rdma-peer-{node_name}",
            "address": f"10.90.{ord(node_name) - ord('a') + 1}.1/30",
            "gid": node.get("gid"),
            "owned_rxe": True,
            "owned_veth": True,
        }
        _require(node == expected, "RDMA link interruption requires exact ownership")
        _require(bool(node.get("gid")), "owned RDMA manifest has no GID")
        return node["veth"], node["peer_veth"]

    async def set_link(self, up, node_name="c"):
        veth, peer = self.owned_link(node_name)
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
        for name in (
            "store-a.storage.json",
            "store-b.storage.json",
            "store-c.storage.json",
        ):
            path = self.artifacts / name
            values.append(json.loads(path.read_text()) if path.exists() else {})
        return values

    async def request_watermark(self, high, low):
        request_id = f"watermark-{time.monotonic_ns()}"
        for name in (
            "store-a.command.json",
            "store-b.command.json",
            "store-c.command.json",
        ):
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
            "affected_node": "a",
            "initial_owners": len(owners),
            "post_restart_read": await self.read_valid(
                "node-restart-read", key, expected
            ),
        }

    async def scenario_master_etcd_restart(self):
        key, expected, _ = await self.seed("master-etcd")
        await self.command(["docker", "restart", self.args.etcd], timeout=30)
        await self.wait_etcd()
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
        link_down_observed = await self.set_link(False, "c")
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
        link_restored = await self.set_link(True, "c")
        return {
            "link_down_observed": link_down_observed,
            "affected_node": "c",
            "interruption_failed_read": True,
            "link_restored": link_restored,
            "post_reconnect_read": await self.read_valid(
                "rdma-reconnected", key, expected
            ),
        }

    async def scenario_degraded_read(self):
        key, expected, owners = await self.seed_on_owner(
            "degraded", "127.0.0.1:12402", replica_num=2
        )
        unavailable = "127.0.0.1:12402"
        await self.stop_process(self.args.node_b, "/tmp/store-node.py")

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
        await self.restart_node(self.args.node_b)
        return {
            "initial_owners": len(owners),
            "affected_node": "b",
            "unavailable_owner": unavailable,
            "remaining_owners": len(remaining),
            "byte_valid": valid,
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
        await self.wait_etcd()
        if not await self.process_running(
            self.args.master_container, "mooncake-master"
        ):
            await self.start_detached(self.args.master_container, self.master_command)
        await self.wait_master()
        for node in (self.args.node_a, self.args.node_b, self.args.node_c):
            if not await self.process_running(node, "/tmp/store-node.py"):
                await self.restart_node(node)
            else:
                await self.wait_node_healthy(node)


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
    parser.add_argument("--node-c", default="mc-rdma-store-node-c")
    parser.add_argument("--client", default="mc-rdma-store-test-client")
    parser.add_argument("--etcd", default="mc-rdma-etcd")
    parser.add_argument("--device", default="mc-rdma-rxe-c")
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
