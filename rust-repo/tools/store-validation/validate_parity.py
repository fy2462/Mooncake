#!/usr/bin/env python3
"""Validate reference-test to Rust-test parity manifests."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path, PurePosixPath
import sys
from typing import Any

from inventory import TestRef, discover_python_tests, discover_rust_tests
from suites import SUITES, discover_suite_tests


ALLOWED_STATUSES = {"covered", "missing", "not-applicable", "blocked"}
ALLOWED_RUST_PACKAGES = frozenset(
    {
        "mooncake-store-client",
        "mooncake-store-core",
        "mooncake-store-master",
        "transfer-engine-ffi",
    }
)
ALLOWED_PYTHON_TEST_ROOT = ("python", "tests")
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


def _reference_key(entry: dict[str, Any]) -> tuple[str, str] | None:
    reference = entry.get("reference")
    if not isinstance(reference, dict):
        return None
    file_name = reference.get("file")
    test_name = reference.get("test")
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


def _is_approved_test_path(file_name: str) -> bool:
    parts = PurePosixPath(file_name).parts
    return bool(
        parts
        and (
            parts[0] in ALLOWED_RUST_PACKAGES
            or parts[: len(ALLOWED_PYTHON_TEST_ROOT)] == ALLOWED_PYTHON_TEST_ROOT
        )
    )


def _suite_is_trusted(manifest: dict[str, Any]) -> bool:
    value = manifest.get("suite")
    if not isinstance(value, dict):
        return False
    suite_id = value.get("id")
    expected = SUITES.get(suite_id) if isinstance(suite_id, str) else None
    return bool(
        expected
        and value.get("framework") == expected.framework
        and value.get("reference_root") == expected.reference_root
    )


def summarize_manifest(manifest: dict[str, Any]) -> dict[str, int]:
    entries = [
        entry for entry in manifest.get("entries", []) if isinstance(entry, dict)
    ]
    statuses = Counter(entry.get("status") for entry in entries)
    rust_references: list[tuple[str, str]] = []
    owners: Counter[tuple[str, str]] = Counter()
    multi_test_rows = 0
    for entry in entries:
        if entry.get("status") not in {"covered", "blocked"}:
            continue
        rust_keys = [
            rust_key
            for value in entry.get("rust", [])
            if (rust_key := _rust_key(value)) is not None
        ]
        rust_references.extend(rust_keys)
        owners.update(set(rust_keys))
        multi_test_rows += len(set(rust_keys)) > 1
    return {
        "reference_total": len(entries),
        "applicable": sum(
            statuses.get(status, 0) for status in ("covered", "missing", "blocked")
        ),
        "rust_references": len(rust_references),
        "unique_rust_tests": len(owners),
        "shared_rust_tests": sum(count > 1 for count in owners.values()),
        "multi_test_rows": multi_test_rows,
        "covered": statuses.get("covered", 0),
        "missing": statuses.get("missing", 0),
        "blocked": statuses.get("blocked", 0),
        "not_applicable": statuses.get("not-applicable", 0),
    }


def summarize_manifests(manifests: list[dict[str, Any]]) -> dict[str, int]:
    return summarize_manifest(
        {
            "entries": [
                entry
                for manifest in manifests
                for entry in manifest.get("entries", [])
                if isinstance(entry, dict)
            ]
        }
    )


def validate_manifest(
    manifest: dict[str, Any],
    reference_refs: set[TestRef],
    rust_refs: set[TestRef],
) -> list[Finding]:
    findings: list[Finding] = []
    if manifest.get("schema_version") != 2:
        findings.append(
            _finding("invalid-schema-version", "manifest", "schema_version must be 2")
        )
    if not _suite_is_trusted(manifest):
        findings.append(
            _finding(
                "invalid-suite",
                "manifest",
                "suite must match one trusted reference-suite definition",
            )
        )

    entries = manifest.get("entries")
    if not isinstance(entries, list):
        return findings + [
            _finding("invalid-entries", "manifest", "entries must be a JSON array")
        ]

    reference_inventory = {(ref.file, ref.name) for ref in reference_refs}
    rust_inventory = {(ref.file, ref.name) for ref in rust_refs}
    entry_keys = [
        _reference_key(entry) if isinstance(entry, dict) else None for entry in entries
    ]
    counts = Counter(key for key in entry_keys if key is not None)

    for key in sorted(reference_inventory - set(counts)):
        findings.append(
            _finding(
                "unmapped-reference-test",
                f"{key[0]}:{key[1]}",
                "discovered reference test has no manifest entry",
            )
        )

    for key, count in sorted(counts.items()):
        reference = f"{key[0]}:{key[1]}"
        if count > 1:
            findings.append(
                _finding(
                    "duplicate-reference",
                    reference,
                    f"reference appears {count} times",
                )
            )
        if key not in reference_inventory:
            findings.append(
                _finding(
                    "stale-reference",
                    reference,
                    "manifest entry does not match a discovered reference test",
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
        key = _reference_key(raw_entry)
        reference = f"{key[0]}:{key[1]}" if key is not None else f"entries[{index}]"
        if key is None:
            findings.append(
                _finding(
                    "invalid-reference",
                    reference,
                    "reference.file and reference.test must be nonempty strings",
                )
            )

        behavior = raw_entry.get("behavior")
        if not isinstance(behavior, str) or not behavior.strip():
            findings.append(
                _finding(
                    "missing-behavior",
                    reference,
                    "entry must describe the observable reference behavior",
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
            if not _is_approved_test_path(rust_key[0]):
                findings.append(
                    _finding(
                        "rust-test-outside-approved-packages",
                        f"{rust_key[0]}:{rust_key[1]}",
                        "Test evidence must belong to an approved Store-facing package",
                    )
                )
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
                    "covered entries require at least one Rust evidence test",
                )
            )
        elif status == "blocked":
            if not rust_keys:
                findings.append(
                    _finding(
                        "blocked-without-rust-test",
                        reference,
                        "blocked entries require at least one existing Rust evidence test",
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
                    f"{status} entries must not claim complete Rust evidence",
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
            continue
        oracle = review.get("oracle")
        if not isinstance(oracle, str) or not oracle.strip():
            findings.append(
                _finding(
                    "missing-review-oracle",
                    reference,
                    "review.oracle must identify the inspected reference path",
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

    return sorted(set(findings))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument(
        "--list-status", choices=sorted(ALLOWED_STATUSES), action="append", default=[]
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        manifests = [
            json.loads(path.read_text(encoding="utf-8")) for path in args.manifest
        ]
        repo_root = args.repo_root.resolve()
        rust_refs = set(discover_rust_tests(repo_root / "rust-repo/crates"))
        python_root = repo_root / "rust-repo/python"
        if python_root.is_dir():
            rust_refs.update(
                TestRef(ref.framework, f"python/{ref.file}", ref.name)
                for ref in discover_python_tests(python_root)
            )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        print(f"parity validation error: {error}", file=sys.stderr)
        return 2

    suite_ids = [
        value.get("id") if isinstance(value, dict) else None
        for manifest in manifests
        for value in [manifest.get("suite")]
    ]
    duplicate_suites = sorted(
        suite_id
        for suite_id, count in Counter(suite_ids).items()
        if suite_id is not None and count > 1
    )
    if duplicate_suites:
        print(
            "ERROR duplicate-suite-manifest: " + ", ".join(duplicate_suites),
            file=sys.stderr,
        )
        return 2

    findings: list[Finding] = []
    for manifest in manifests:
        suite_value = manifest.get("suite")
        suite_id = suite_value.get("id") if isinstance(suite_value, dict) else None
        suite = SUITES.get(suite_id) if isinstance(suite_id, str) else None
        reference_refs = (
            set(discover_suite_tests(repo_root, suite)) if suite is not None else set()
        )
        suite_findings = validate_manifest(manifest, reference_refs, rust_refs)
        findings.extend(suite_findings)
        for finding in suite_findings:
            print(
                f"ERROR suite={suite_id or 'invalid'} {finding.code} "
                f"{finding.reference}: {finding.message}"
            )
        summary = summarize_manifest(manifest)
        print(
            f"summary suite={suite_id or 'invalid'} "
            + " ".join(
                f"{key.replace('_', '-')}={value}" for key, value in summary.items()
            )
        )

    summary = summarize_manifests(manifests)
    print(
        "summary aggregate "
        + " ".join(f"{key.replace('_', '-')}={value}" for key, value in summary.items())
    )

    requested_statuses = set(args.list_status)
    for manifest in manifests:
        suite_value = manifest.get("suite")
        suite_id = suite_value.get("id") if isinstance(suite_value, dict) else "invalid"
        entries = manifest.get("entries", [])
        for entry in entries if isinstance(entries, list) else []:
            if (
                not isinstance(entry, dict)
                or entry.get("status") not in requested_statuses
            ):
                continue
            reference = entry.get("reference", {})
            print(
                f"{entry.get('status')} suite={suite_id} "
                f"{reference.get('file', '')}:{reference.get('test', '')}: "
                f"{entry.get('reason', '')}"
            )

    if findings:
        return 2
    if args.require_complete and (summary["missing"] or summary["blocked"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
