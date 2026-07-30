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
ALLOWED_NA_CATEGORIES = frozenset(
    {
        "language-unrepresentable",
        "absent-rust-product-boundary",
        "cpp-build-or-abi",
        "noncanonical-duplicate-source",
        "excluded-component-internal",
        "no-executable-cpp-oracle",
        "excluded-performance-scope",
    }
)


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


def summarize_manifest(manifest: dict[str, Any]) -> dict[str, int]:
    entries = [
        entry for entry in manifest.get("entries", []) if isinstance(entry, dict)
    ]
    statuses = Counter(entry.get("status") for entry in entries)
    primary_owners: Counter[tuple[str, str]] = Counter()
    for entry in entries:
        if entry.get("status") not in {"covered", "blocked"}:
            continue
        rust_keys = {
            rust_key
            for value in entry.get("rust", [])
            if (rust_key := _rust_key(value)) is not None
        }
        primary_owners.update(rust_keys)
    return {
        "cpp_total": len(entries),
        "applicable": sum(
            statuses.get(status, 0) for status in ("covered", "missing", "blocked")
        ),
        "unique_primary": len(primary_owners),
        "duplicate_primary": sum(count > 1 for count in primary_owners.values()),
        "covered": statuses.get("covered", 0),
        "missing": statuses.get("missing", 0),
        "blocked": statuses.get("blocked", 0),
        "not_applicable": statuses.get("not-applicable", 0),
    }


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
    primary_owners: dict[tuple[str, str], list[str]] = {}

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

        if status in {"covered", "blocked"}:
            for rust_key in set(rust_keys):
                primary_owners.setdefault(rust_key, []).append(reference)

        reason = raw_entry.get("reason")
        if not isinstance(reason, str):
            findings.append(
                _finding("invalid-reason", reference, "reason must be a string")
            )
            reason = ""
        if status == "covered":
            if not rust_keys:
                findings.append(
                    _finding(
                        "covered-without-rust-test",
                        reference,
                        "covered entries require exactly one Rust primary test",
                    )
                )
            elif len(rust_keys) > 1:
                findings.append(
                    _finding(
                        "multiple-primary-rust-tests",
                        reference,
                        "covered entries require exactly one Rust primary test",
                    )
                )
        elif status == "blocked":
            if not rust_keys:
                findings.append(
                    _finding(
                        "blocked-without-rust-test",
                        reference,
                        "blocked entries require exactly one existing Rust primary test",
                    )
                )
            elif len(rust_keys) > 1:
                findings.append(
                    _finding(
                        "multiple-primary-rust-tests",
                        reference,
                        "blocked entries require exactly one Rust primary test",
                    )
                )
            prerequisite = raw_entry.get("prerequisite")
            if not isinstance(prerequisite, str) or not prerequisite.strip():
                findings.append(
                    _finding(
                        "missing-blocked-prerequisite",
                        reference,
                        "blocked entries require a named external prerequisite",
                    )
                )
        elif status in {"missing", "not-applicable"} and rust_keys:
            findings.append(
                _finding(
                    "noncovered-with-rust-test",
                    reference,
                    f"{status} entries must not claim a Rust primary test",
                )
            )
        if status != "blocked" and "prerequisite" in raw_entry:
            findings.append(
                _finding(
                    "unexpected-blocked-prerequisite",
                    reference,
                    "only blocked entries may set prerequisite",
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
            na_category = review.get("na_category")
            if status == "not-applicable":
                if not isinstance(na_category, str) or not na_category.strip():
                    findings.append(
                        _finding(
                            "missing-na-category",
                            reference,
                            "not-applicable entries require review.na_category",
                        )
                    )
                elif na_category not in ALLOWED_NA_CATEGORIES:
                    findings.append(
                        _finding(
                            "invalid-na-category",
                            reference,
                            "review.na_category must be one of "
                            f"{sorted(ALLOWED_NA_CATEGORIES)}",
                        )
                    )
            elif "na_category" in review:
                findings.append(
                    _finding(
                        "unexpected-na-category",
                        reference,
                        "only not-applicable entries may set review.na_category",
                    )
                )

    for rust_key, owners in sorted(primary_owners.items()):
        if len(owners) > 1:
            findings.append(
                _finding(
                    "duplicate-primary-rust-test",
                    f"{rust_key[0]}:{rust_key[1]}",
                    "Rust primary test is reused by " + ", ".join(sorted(owners)),
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
    summary = summarize_manifest(manifest)
    print(
        "summary "
        + " ".join(
            f"{label}={summary[key]}"
            for label, key in (
                ("cpp-total", "cpp_total"),
                ("applicable", "applicable"),
                ("unique-primary", "unique_primary"),
                ("duplicate-primary", "duplicate_primary"),
                ("covered", "covered"),
                ("missing", "missing"),
                ("blocked", "blocked"),
                ("not-applicable", "not_applicable"),
            )
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
    if args.require_complete and (summary["missing"] or summary["blocked"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
