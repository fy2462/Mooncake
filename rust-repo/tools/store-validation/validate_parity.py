#!/usr/bin/env python3
"""Validate the C++ behavioral-reference to Rust-test parity manifest."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import sys
from typing import Any

from inventory import TestRef, discover_cpp_tests, discover_rust_tests


ALLOWED_STATUSES = {"covered", "missing", "not-applicable", "blocked"}


@dataclass(frozen=True, order=True)
class Finding:
    code: str
    reference: str
    message: str


def _finding(code: str, reference: str, message: str) -> Finding:
    return Finding(code=code, reference=reference, message=message)


def _cpp_key(entry: dict[str, Any]) -> tuple[str, str] | None:
    cpp = entry.get("cpp")
    if not isinstance(cpp, dict):
        return None
    file_name = cpp.get("file")
    test_name = cpp.get("test")
    if not isinstance(file_name, str) or not isinstance(test_name, str):
        return None
    if not file_name.strip() or not test_name.strip():
        return None
    return file_name, test_name


def _rust_key(value: Any) -> tuple[str, str] | None:
    if not isinstance(value, dict):
        return None
    file_name = value.get("file")
    test_name = value.get("test")
    if not isinstance(file_name, str) or not isinstance(test_name, str):
        return None
    if not file_name.strip() or not test_name.strip():
        return None
    return file_name, test_name


def validate_manifest(
    manifest: dict[str, Any],
    cpp_refs: set[TestRef],
    rust_refs: set[TestRef],
) -> list[Finding]:
    findings: list[Finding] = []
    if manifest.get("schema_version") != 1:
        findings.append(
            _finding("invalid-schema-version", "manifest", "schema_version must be 1")
        )

    entries = manifest.get("entries")
    if not isinstance(entries, list):
        return findings + [
            _finding("invalid-entries", "manifest", "entries must be a JSON array")
        ]

    cpp_inventory = {(ref.file, ref.name) for ref in cpp_refs}
    rust_inventory = {(ref.file, ref.name) for ref in rust_refs}
    entry_keys = [
        _cpp_key(entry) if isinstance(entry, dict) else None for entry in entries
    ]
    counts = Counter(key for key in entry_keys if key is not None)

    for key in sorted(cpp_inventory - set(counts)):
        findings.append(
            _finding(
                "unmapped-cpp-test",
                f"{key[0]}:{key[1]}",
                "discovered C++ behavioral reference has no manifest entry",
            )
        )

    for key, count in sorted(counts.items()):
        reference = f"{key[0]}:{key[1]}"
        if count > 1:
            findings.append(
                _finding(
                    "duplicate-cpp-reference",
                    reference,
                    f"C++ reference appears {count} times",
                )
            )
        if key not in cpp_inventory:
            findings.append(
                _finding(
                    "stale-cpp-reference",
                    reference,
                    "manifest entry does not match a discovered C++ test",
                )
            )

    for index, raw_entry in enumerate(entries):
        if not isinstance(raw_entry, dict):
            findings.append(
                _finding(
                    "invalid-entry", f"entries[{index}]", "entry must be an object"
                )
            )
            continue
        key = _cpp_key(raw_entry)
        reference = f"{key[0]}:{key[1]}" if key is not None else f"entries[{index}]"
        if key is None:
            findings.append(
                _finding(
                    "invalid-cpp-reference",
                    reference,
                    "cpp.file and cpp.test must be nonempty strings",
                )
            )

        behavior = raw_entry.get("behavior")
        if not isinstance(behavior, str) or not behavior.strip():
            findings.append(
                _finding(
                    "missing-behavior",
                    reference,
                    "entry must describe the observable C++ behavior",
                )
            )

        boundary = raw_entry.get("boundary")
        if (
            not isinstance(boundary, list)
            or not boundary
            or any(not isinstance(item, str) or not item.strip() for item in boundary)
        ):
            findings.append(
                _finding(
                    "invalid-boundary",
                    reference,
                    "boundary must be a nonempty array of nonempty strings",
                )
            )

        status = raw_entry.get("status")
        if status not in ALLOWED_STATUSES:
            findings.append(
                _finding(
                    "invalid-status",
                    reference,
                    f"status must be one of {sorted(ALLOWED_STATUSES)}",
                )
            )

        rust_values = raw_entry.get("rust")
        if not isinstance(rust_values, list):
            findings.append(
                _finding("invalid-rust-list", reference, "rust must be an array")
            )
            rust_values = []
        rust_keys: list[tuple[str, str]] = []
        for rust_index, value in enumerate(rust_values):
            rust_key = _rust_key(value)
            if rust_key is None:
                findings.append(
                    _finding(
                        "invalid-rust-reference",
                        f"{reference}:rust[{rust_index}]",
                        "rust.file and rust.test must be nonempty strings",
                    )
                )
                continue
            rust_keys.append(rust_key)
            if rust_key not in rust_inventory:
                findings.append(
                    _finding(
                        "missing-rust-test",
                        f"{rust_key[0]}:{rust_key[1]}",
                        f"Rust test referenced by {reference} is not discoverable",
                    )
                )

        reason = raw_entry.get("reason")
        if not isinstance(reason, str):
            findings.append(
                _finding("invalid-reason", reference, "reason must be a string")
            )
            reason = ""
        if status == "covered" and not rust_keys:
            findings.append(
                _finding(
                    "covered-without-rust-test",
                    reference,
                    "covered entries require at least one Rust test",
                )
            )
        if status != "covered" and rust_keys:
            findings.append(
                _finding(
                    "noncovered-with-rust-test",
                    reference,
                    f"{status} entries must not claim executable Rust coverage",
                )
            )
        if status in {"missing", "not-applicable", "blocked"} and not reason.strip():
            findings.append(
                _finding(
                    "missing-disposition-reason",
                    reference,
                    f"{status} entries require a concrete reason",
                )
            )

        review = raw_entry.get("review")
        if not isinstance(review, dict):
            findings.append(
                _finding("invalid-review", reference, "review must be an object")
            )
        else:
            oracle = review.get("oracle")
            if not isinstance(oracle, str) or not oracle.strip():
                findings.append(
                    _finding(
                        "missing-review-oracle",
                        reference,
                        "review.oracle must identify the inspected C++ path",
                    )
                )
            if review.get("reviewed") is not True:
                findings.append(
                    _finding(
                        "unreviewed-entry",
                        reference,
                        "review.reviewed must be true",
                    )
                )

    return sorted(set(findings))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cpp-root", type=Path, required=True)
    parser.add_argument("--rust-root", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument(
        "--list-status", choices=sorted(ALLOWED_STATUSES), action="append", default=[]
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        cpp_refs = set(discover_cpp_tests(args.cpp_root))
        rust_refs = set(discover_rust_tests(args.rust_root))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        print(f"parity validation error: {error}", file=sys.stderr)
        return 2

    findings = validate_manifest(manifest, cpp_refs, rust_refs)
    for finding in findings:
        print(f"ERROR {finding.code} {finding.reference}: {finding.message}")

    entries = manifest.get("entries", [])
    statuses = Counter(
        entry.get("status") for entry in entries if isinstance(entry, dict)
    )
    print(
        "summary "
        + " ".join(
            f"{status}={statuses.get(status, 0)}" for status in sorted(ALLOWED_STATUSES)
        )
    )
    for status in args.list_status:
        for entry in entries:
            if isinstance(entry, dict) and entry.get("status") == status:
                cpp = entry.get("cpp", {})
                print(
                    f"{status} {cpp.get('file', '?')}:{cpp.get('test', '?')}: "
                    f"{entry.get('behavior', '')} -- {entry.get('reason', '')}"
                )

    if findings:
        return 1
    if args.require_complete and (
        statuses.get("missing", 0) or statuses.get("blocked", 0)
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
