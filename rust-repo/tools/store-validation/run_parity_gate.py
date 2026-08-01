#!/usr/bin/env python3
"""Plan and execute the reviewed Rust Store parity test manifest."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import subprocess
import sys
import time
from typing import Any, Sequence

from inventory import TestRef, discover_python_tests, discover_rust_tests
from result import build_gate_result, utc_now, write_json_atomic
from suites import SUITES, discover_suite_tests
from validate_parity import validate_manifest


@dataclass(frozen=True)
class PlannedCommand:
    name: str
    argv: list[str]
    rust: dict[str, Any]
    references: list[dict[str, str]]


@dataclass(frozen=True)
class ParityPlan:
    status: str
    commands: list[PlannedCommand]
    blocked: list[dict[str, Any]]


def _command_for(rust: dict[str, Any]) -> tuple[str, list[str]]:
    path = PurePosixPath(rust["file"])
    parts = path.parts
    test_name = rust["test"]
    if len(parts) >= 3 and parts[:2] == ("python", "tests"):
        if path.suffix != ".py":
            raise ValueError(f"Python reference is not a test module: {rust['file']}")
        pytest_node = "::".join(test_name.split("."))
        safe_name = f"python-{path.stem}-{test_name}".replace("_", "-")
        return safe_name, [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            f"{path}::{pytest_node}",
        ]
    if len(parts) < 3 or parts[0] == "tools" or parts[1] not in {"src", "tests"}:
        raise ValueError(f"Rust reference is outside Rust crates: {rust['file']}")
    package = parts[0]
    features = (
        ["link-native"]
        if package in {"mooncake-store-client", "mooncake-p2p-store"}
        else []
    )
    features.extend(rust.get("features", []))
    features = list(dict.fromkeys(features))
    feature_args = ["--features", ",".join(features)] if features else []
    if parts[1] == "tests":
        target = PurePosixPath(parts[2]).stem
        module_names = [PurePosixPath(part).stem for part in parts[3:]]
        if module_name := rust.get("module"):
            module_names.append(module_name)
        test_filter = "::".join([*module_names, test_name])
        argv = [
            "cargo",
            "test",
            "-p",
            package,
            *feature_args,
            "--test",
            target,
            test_filter,
            "--",
            "--exact",
        ]
    else:
        argv = [
            "cargo",
            "test",
            "-p",
            package,
            *feature_args,
            test_name,
            "--lib",
        ]
    safe_name = f"{package}-{path.stem}-{test_name}".replace("_", "-")
    return safe_name, argv


def plan_parity_run(
    manifest: dict[str, Any], rust_inventory: set[TestRef]
) -> ParityPlan:
    discovered = {(item.file, item.name) for item in rust_inventory}
    blocked: list[dict[str, Any]] = []
    by_test: dict[tuple[str, str], PlannedCommand] = {}

    for entry in manifest.get("entries", []):
        status = entry.get("status")
        if status in {"missing", "blocked"}:
            blocked.append(entry)
            continue
        if status == "not-applicable":
            continue
        if status != "covered":
            blocked.append(entry)
            continue
        for rust in entry.get("rust", []):
            key = (rust.get("file", ""), rust.get("test", ""))
            if key not in discovered:
                blocked.append(entry)
                continue
            name, argv = _command_for(rust)
            existing = by_test.get(key)
            reference = dict(entry["reference"])
            if existing is None:
                by_test[key] = PlannedCommand(
                    name=name,
                    argv=argv,
                    rust=dict(rust),
                    references=[reference],
                )
            elif reference not in existing.references:
                existing.references.append(reference)

    commands = sorted(
        by_test.values(), key=lambda item: (item.rust["file"], item.rust["test"])
    )
    return ParityPlan(
        status="BLOCKED" if blocked else "READY",
        commands=commands,
        blocked=blocked,
    )


def plan_parity_runs(
    manifests: list[dict[str, Any]],
    rust_inventory: set[TestRef],
) -> ParityPlan:
    entries: list[dict[str, Any]] = []
    for manifest in manifests:
        suite_value = manifest.get("suite")
        suite_id = suite_value.get("id") if isinstance(suite_value, dict) else "invalid"
        for entry in manifest.get("entries", []):
            if not isinstance(entry, dict):
                entries.append(entry)
                continue
            enriched = dict(entry)
            reference = dict(entry.get("reference", {}))
            reference["suite"] = suite_id
            enriched["reference"] = reference
            entries.append(enriched)
    return plan_parity_run({"entries": entries}, rust_inventory)


def _version(argv: Sequence[str]) -> str:
    try:
        output = subprocess.run(
            argv,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout
        return output.splitlines()[0]
    except (OSError, IndexError):
        return "unavailable"


def execute_plan(
    plan: ParityPlan, rust_root: Path, artifact_root: Path
) -> dict[str, Any]:
    logs_dir = artifact_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()
    started = time.monotonic()
    records: list[dict[str, Any]] = []
    command_env = os.environ.copy()
    python_root = str(rust_root / "python")
    inherited_python_path = command_env.get("PYTHONPATH")
    command_env["PYTHONPATH"] = (
        f"{python_root}{os.pathsep}{inherited_python_path}"
        if inherited_python_path
        else python_root
    )
    for index, command in enumerate(plan.commands):
        log = logs_dir / f"{index:04d}-{command.name}.log"
        command_started = time.monotonic()
        with log.open("w", encoding="utf-8") as stream:
            completed = subprocess.run(
                command.argv,
                cwd=rust_root,
                env=command_env,
                check=False,
                text=True,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
        zero_tests = bool(
            re.search(r"(?m)^running 0 tests$", log.read_text(encoding="utf-8"))
        )
        exit_code = completed.returncode if not zero_tests else 3
        records.append(
            {
                "name": command.name,
                "argv": command.argv,
                "status": "PASS" if exit_code == 0 else "FAIL",
                "exit_code": exit_code,
                "duration_seconds": round(time.monotonic() - command_started, 6),
                "log": str(log),
                "rust": command.rust,
                "references": command.references,
                "zero_tests_executed": zero_tests,
            }
        )
    for index, entry in enumerate(plan.blocked):
        records.append(
            {
                "name": f"blocked-{index:04d}",
                "argv": [],
                "status": "BLOCKED",
                "exit_code": None,
                "duration_seconds": 0.0,
                "log": "",
                "reference": entry.get("reference", {}),
                "prerequisite": entry.get("reason", "invalid parity disposition"),
            }
        )
    finished_at = utc_now()
    return build_gate_result(
        "parity",
        records,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=time.monotonic() - started,
        environment={
            "uname": platform.platform(),
            "rustc": _version(["rustc", "--version"]),
            "cargo": _version(["cargo", "--version"]),
            "python": _version([sys.executable, "--version"]),
            "git_commit": _version(
                ["git", "-C", str(rust_root.parent), "rev-parse", "HEAD"]
            ),
        },
    )


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    repo_root = args.repo_root.resolve()
    rust_root = repo_root / "rust-repo"
    try:
        manifests = [
            json.loads(path.read_text(encoding="utf-8")) for path in args.manifest
        ]
        rust_inventory = set(discover_rust_tests(rust_root / "crates"))
        python_root = rust_root / "python"
        if python_root.is_dir():
            rust_inventory.update(
                TestRef(item.framework, f"python/{item.file}", item.name)
                for item in discover_python_tests(python_root)
            )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        print(f"parity gate error: {error}", file=sys.stderr)
        return 2

    suite_ids = [
        value.get("id") if isinstance(value, dict) else None
        for manifest in manifests
        for value in [manifest.get("suite")]
    ]
    if len(suite_ids) != len(set(suite_ids)):
        print("parity gate error: duplicate suite manifest", file=sys.stderr)
        return 2

    has_findings = False
    for manifest in manifests:
        suite_value = manifest.get("suite")
        suite_id = suite_value.get("id") if isinstance(suite_value, dict) else None
        suite = SUITES.get(suite_id) if isinstance(suite_id, str) else None
        reference_inventory = (
            set(discover_suite_tests(repo_root, suite)) if suite is not None else set()
        )
        findings = validate_manifest(manifest, reference_inventory, rust_inventory)
        has_findings |= bool(findings)
        for finding in findings:
            print(
                f"ERROR suite={suite_id or 'invalid'} {finding.code} "
                f"{finding.reference}: {finding.message}",
                file=sys.stderr,
            )
    if has_findings:
        return 2

    plan = plan_parity_runs(manifests, rust_inventory)
    result = execute_plan(plan, rust_root, args.artifact_root.resolve())
    output = args.artifact_root.resolve() / "parity.result.json"
    write_json_atomic(output, result)
    print(f"parity gate: {result['status']} ({output})")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
