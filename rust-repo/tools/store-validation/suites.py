"""Trusted reference-suite definitions for Rust parity validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from inventory import TestRef, discover_cpp_tests, discover_python_tests


@dataclass(frozen=True)
class SuiteDefinition:
    id: str
    framework: str
    reference_root: str
    excluded_files: frozenset[str]


SUITES = {
    "store-cpp": SuiteDefinition(
        "store-cpp", "gtest", "mooncake-store/tests", frozenset()
    ),
    "transfer-engine-cpp": SuiteDefinition(
        "transfer-engine-cpp",
        "gtest",
        "mooncake-transfer-engine/tests",
        frozenset(),
    ),
    "tent-cpp": SuiteDefinition(
        "tent-cpp",
        "gtest",
        "mooncake-transfer-engine/tent/tests",
        frozenset(),
    ),
    "wheel-store-python": SuiteDefinition(
        "wheel-store-python",
        "python",
        "mooncake-wheel/tests",
        frozenset({"test_release_wheel_tags.py"}),
    ),
}


def discover_suite_tests(
    repo_root: Path,
    suite: SuiteDefinition,
) -> list[TestRef]:
    root = repo_root / suite.reference_root
    if suite.framework == "gtest":
        return discover_cpp_tests(root)
    if suite.framework == "python":
        return discover_python_tests(root, suite.excluded_files)
    raise ValueError(f"unsupported reference framework: {suite.framework}")
