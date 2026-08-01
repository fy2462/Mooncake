import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from inventory import TestRef
import validate_parity
from validate_parity import validate_manifest


REFERENCE_REF = TestRef("gtest", "allocator_test.cpp", "AllocatorTest.Reuse")
SECOND_REFERENCE_REF = TestRef("gtest", "client_test.cpp", "ClientTest.Timeout")
THIRD_REFERENCE_REF = TestRef("gtest", "master_test.cpp", "MasterTest.Recovery")
RUST_REF = TestRef(
    "rust", "mooncake-store-master/tests/test_allocator.rs", "reuse_freed_range"
)
RUST_REF_2 = TestRef(
    "rust", "transfer-engine-ffi/tests/test_engine.rs", "reuse_adjacent_range"
)
PYTHON_REF = TestRef(
    "python", "python/tests/test_pybind_client_parity.py", "test_round_trip"
)
STORE_SUITE = {
    "id": "store-cpp",
    "framework": "gtest",
    "reference_root": "mooncake-store/tests",
}
TRANSFER_ENGINE_SUITE = {
    "id": "transfer-engine-cpp",
    "framework": "gtest",
    "reference_root": "mooncake-transfer-engine/tests",
}
TOOL_ROOT = Path(__file__).parents[1]


def rust_value(reference: TestRef) -> dict[str, str]:
    return {"file": reference.file, "test": reference.name}


def manifest(entries: list[dict], *, suite: dict | None = None) -> dict:
    return {
        "schema_version": 2,
        "suite": dict(STORE_SUITE if suite is None else suite),
        "entries": entries,
    }


def finding_codes(
    candidate: dict,
    rust_refs: set[TestRef],
    *,
    reference_refs: set[TestRef] | None = None,
) -> set[str]:
    findings = validate_manifest(
        manifest([candidate]),
        {REFERENCE_REF} if reference_refs is None else reference_refs,
        rust_refs,
    )
    return {item.code for item in findings}


def entry(
    *,
    status: str = "covered",
    rust: list[dict[str, str]] | None = None,
    reason: str = "",
    reference: TestRef = REFERENCE_REF,
) -> dict:
    return {
        "reference": {"file": reference.file, "test": reference.name},
        "behavior": "A freed allocator range can be reused without losing capacity.",
        "boundary": ["master", "allocator"],
        "status": status,
        "rust": (
            [{"file": RUST_REF.file, "test": RUST_REF.name}] if rust is None else rust
        ),
        "reason": reason,
        "review": {
            "oracle": "allocator_test.cpp and allocation_strategy.cpp",
            "reviewed": True,
        },
    }


class ValidateParityTest(unittest.TestCase):
    def test_requires_schema_version_two_and_a_trusted_suite(self):
        old = manifest([entry()])
        old["schema_version"] = 1
        findings = validate_manifest(old, {REFERENCE_REF}, {RUST_REF})
        self.assertIn("invalid-schema-version", {item.code for item in findings})

        unknown = manifest(
            [entry()],
            suite={
                "id": "unknown",
                "framework": "gtest",
                "reference_root": "unknown/tests",
            },
        )
        findings = validate_manifest(unknown, {REFERENCE_REF}, {RUST_REF})
        self.assertIn("invalid-suite", {item.code for item in findings})

    def test_rejects_unmapped_reference_test(self):
        findings = validate_manifest(manifest([]), {REFERENCE_REF}, set())
        self.assertIn("unmapped-reference-test", {item.code for item in findings})

    def test_covered_entry_requires_discoverable_rust_test(self):
        findings = validate_manifest(
            manifest([entry()]),
            {REFERENCE_REF},
            set(),
        )
        self.assertIn("missing-rust-test", {item.code for item in findings})

    def test_covered_entry_accepts_multiple_rust_evidence_tests(self):
        covered = entry(rust=[rust_value(RUST_REF), rust_value(RUST_REF_2)])
        self.assertEqual(
            validate_manifest(
                manifest([covered]),
                {REFERENCE_REF},
                {RUST_REF, RUST_REF_2},
            ),
            [],
        )

    def test_rust_evidence_test_can_serve_multiple_reference_rows(self):
        second = entry(reference=SECOND_REFERENCE_REF)
        self.assertEqual(
            validate_manifest(
                manifest([entry(), second]),
                {REFERENCE_REF, SECOND_REFERENCE_REF},
                {RUST_REF},
            ),
            [],
        )

    def test_transfer_engine_ffi_is_allowed_but_unrelated_package_is_rejected(self):
        ffi = entry(rust=[rust_value(RUST_REF_2)])
        self.assertEqual(
            validate_manifest(
                manifest([ffi]),
                {REFERENCE_REF},
                {RUST_REF_2},
            ),
            [],
        )

        unrelated_ref = TestRef(
            "rust", "mooncake-conductor/tests/test_router.rs", "routes_request"
        )
        unrelated = entry(rust=[rust_value(unrelated_ref)])
        codes = finding_codes(unrelated, {unrelated_ref})
        self.assertIn("rust-test-outside-approved-packages", codes)

    def test_python_binding_test_is_accepted_as_store_facing_evidence(self):
        python_evidence = entry(rust=[rust_value(PYTHON_REF)])
        self.assertEqual(
            validate_manifest(
                manifest([python_evidence]),
                {REFERENCE_REF},
                {PYTHON_REF},
            ),
            [],
        )

    def test_not_applicable_requires_nonempty_reviewed_reason(self):
        candidate = entry(status="not-applicable", rust=[], reason="")
        candidate["review"]["na_category"] = "absent-rust-product-boundary"
        findings = validate_manifest(manifest([candidate]), {REFERENCE_REF}, set())
        self.assertIn("missing-disposition-reason", {item.code for item in findings})

    def test_not_applicable_requires_allowed_na_category(self):
        candidate = entry(
            status="not-applicable",
            rust=[],
            reason="The C++ helper has no Rust product boundary.",
        )
        findings = validate_manifest(manifest([candidate]), {REFERENCE_REF}, set())
        self.assertIn("missing-na-category", {item.code for item in findings})

        candidate["review"]["na_category"] = "too-expensive"
        findings = validate_manifest(manifest([candidate]), {REFERENCE_REF}, set())
        self.assertIn("invalid-na-category", {item.code for item in findings})

    def test_non_na_row_rejects_stale_na_category(self):
        covered = entry()
        covered["review"]["na_category"] = "absent-rust-product-boundary"
        codes = finding_codes(covered, {RUST_REF})
        self.assertIn("unexpected-na-category", codes)

    def test_blocked_entry_requires_evidence_and_prerequisite(self):
        blocked = entry(status="blocked", rust=[], reason="GPU execution is blocked.")
        codes = finding_codes(blocked, set())
        self.assertIn("blocked-without-rust-test", codes)
        self.assertIn("missing-blocked-prerequisite", codes)

    def test_blocked_entry_accepts_aggregate_evidence_and_prerequisite(self):
        blocked = entry(
            status="blocked",
            rust=[rust_value(RUST_REF), rust_value(RUST_REF_2)],
            reason="The parity evidence exists but cannot execute on this host.",
        )
        blocked["prerequisite"] = "CUDA-capable GPU"
        findings = validate_manifest(
            manifest([blocked]),
            {REFERENCE_REF},
            {RUST_REF, RUST_REF_2},
        )
        self.assertEqual(findings, [])

    def test_missing_and_not_applicable_entries_reject_rust_evidence(self):
        for status in ("missing", "not-applicable"):
            candidate = entry(
                status=status,
                reason="This disposition must not claim complete Rust evidence.",
            )
            if status == "not-applicable":
                candidate["review"]["na_category"] = "absent-rust-product-boundary"
            with self.subTest(status=status):
                self.assertIn(
                    "noncovered-with-rust-test",
                    finding_codes(candidate, {RUST_REF}),
                )

    def test_summary_reports_reuse_and_aggregate_evidence(self):
        first = entry()
        second = entry(reference=SECOND_REFERENCE_REF)
        third = entry(
            reference=THIRD_REFERENCE_REF,
            rust=[rust_value(RUST_REF), rust_value(RUST_REF_2)],
        )
        self.assertEqual(
            validate_parity.summarize_manifest(manifest([first, second, third])),
            {
                "reference_total": 3,
                "applicable": 3,
                "rust_references": 4,
                "unique_rust_tests": 2,
                "shared_rust_tests": 1,
                "multi_test_rows": 1,
                "covered": 3,
                "missing": 0,
                "blocked": 0,
                "not_applicable": 0,
            },
        )

    def test_rejects_duplicate_and_stale_reference_rows(self):
        duplicate = entry()
        stale_ref = TestRef("gtest", "removed_test.cpp", "Old.Test")
        stale = entry(reference=stale_ref)
        findings = validate_manifest(
            manifest([duplicate, duplicate.copy(), stale]),
            {REFERENCE_REF},
            {RUST_REF},
        )
        codes = {item.code for item in findings}
        self.assertIn("duplicate-reference", codes)
        self.assertIn("stale-reference", codes)

    def test_rejects_unknown_status_and_missing_behavior(self):
        invalid = entry()
        invalid["status"] = "done"
        invalid["behavior"] = ""
        findings = validate_manifest(
            manifest([invalid]),
            {REFERENCE_REF},
            {RUST_REF},
        )
        codes = {item.code for item in findings}
        self.assertIn("invalid-status", codes)
        self.assertIn("missing-behavior", codes)

    def test_accepts_valid_mixed_manifest(self):
        missing = entry(
            status="missing",
            rust=[],
            reason="No Rust test currently asserts timeout cleanup.",
            reference=SECOND_REFERENCE_REF,
        )
        missing["behavior"] = "A timed-out request releases registered memory."
        findings = validate_manifest(
            manifest([entry(), missing]),
            {REFERENCE_REF, SECOND_REFERENCE_REF},
            {RUST_REF},
        )
        self.assertEqual(findings, [])

    def test_cli_validates_multiple_suite_manifests_and_reports_aggregate(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            store_root = repo / "mooncake-store/tests"
            transfer_root = repo / "mooncake-transfer-engine/tests"
            rust_root = repo / "rust-repo/crates/mooncake-store-core/src"
            store_root.mkdir(parents=True)
            transfer_root.mkdir(parents=True)
            rust_root.mkdir(parents=True)
            (store_root / "store_test.cpp").write_text(
                "TEST(StoreTest, Put) {}\n", encoding="utf-8"
            )
            (transfer_root / "transfer_test.cpp").write_text(
                "TEST(TransferTest, Submit) {}\n", encoding="utf-8"
            )
            (rust_root / "lib.rs").write_text(
                "#[test]\nfn shared_evidence() {}\n", encoding="utf-8"
            )
            rust = {
                "file": "mooncake-store-core/src/lib.rs",
                "test": "shared_evidence",
            }
            store_manifest = manifest(
                [
                    entry(
                        reference=TestRef("gtest", "store_test.cpp", "StoreTest.Put"),
                        rust=[rust],
                    )
                ]
            )
            transfer_manifest = manifest(
                [
                    entry(
                        reference=TestRef(
                            "gtest", "transfer_test.cpp", "TransferTest.Submit"
                        ),
                        rust=[rust],
                    )
                ],
                suite=TRANSFER_ENGINE_SUITE,
            )
            store_path = repo / "store.json"
            transfer_path = repo / "transfer.json"
            store_path.write_text(json.dumps(store_manifest), encoding="utf-8")
            transfer_path.write_text(json.dumps(transfer_manifest), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(TOOL_ROOT / "validate_parity.py"),
                    "--manifest",
                    str(store_path),
                    "--manifest",
                    str(transfer_path),
                    "--repo-root",
                    str(repo),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("summary suite=store-cpp reference-total=1", result.stdout)
        self.assertIn(
            "summary suite=transfer-engine-cpp reference-total=1", result.stdout
        )
        self.assertIn("summary aggregate reference-total=2", result.stdout)
        self.assertIn("unique-rust-tests=1", result.stdout)

    def test_cli_rejects_duplicate_suite_manifests(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            (repo / "mooncake-store/tests").mkdir(parents=True)
            (repo / "rust-repo/crates").mkdir(parents=True)
            path = repo / "store.json"
            path.write_text(json.dumps(manifest([])), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(TOOL_ROOT / "validate_parity.py"),
                    "--manifest",
                    str(path),
                    "--manifest",
                    str(path),
                    "--repo-root",
                    str(repo),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate-suite-manifest", result.stdout + result.stderr)

    def test_cli_reports_a_missing_requested_manifest_as_input_error(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [
                    sys.executable,
                    str(TOOL_ROOT / "validate_parity.py"),
                    "--manifest",
                    str(Path(directory) / "missing.json"),
                    "--repo-root",
                    directory,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("parity validation error", result.stderr)


if __name__ == "__main__":
    unittest.main()
