#!/usr/bin/env python3
"""Stable result records shared by Store validation gates."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Sequence


STATUSES = {"PASS", "FAIL", "SKIP", "BLOCKED"}


def aggregate_status(statuses: Sequence[str]) -> str:
    """Return the conservative roll-up for required validation stages."""
    if not statuses:
        return "BLOCKED"
    unknown = set(statuses) - STATUSES
    if unknown:
        raise ValueError(f"unknown result statuses: {sorted(unknown)}")
    for status in ("FAIL", "BLOCKED", "SKIP"):
        if status in statuses:
            return status
    return "PASS"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_gate_result(
    gate: str,
    commands: Sequence[dict[str, Any]],
    *,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    environment: dict[str, str],
) -> dict[str, Any]:
    command_list = list(commands)
    result: dict[str, Any] = {
        "schema_version": 1,
        "gate": gate,
        "status": aggregate_status([item["status"] for item in command_list]),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 6),
        "environment": dict(sorted(environment.items())),
        "commands": command_list,
    }
    first_failure = next(
        (item for item in command_list if item["status"] != "PASS"), None
    )
    if first_failure is not None:
        result["first_failure"] = {
            key: first_failure[key]
            for key in ("name", "argv", "status", "exit_code", "log", "prerequisite")
            if key in first_failure
        }
    return result


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
