import unittest

from inventory import TestRef
import validate_parity
from validate_parity import validate_manifest


CPP_REF = TestRef("gtest", "allocator_test.cpp", "AllocatorTest.Reuse")
RUST_REF = TestRef(
    "rust", "mooncake-store-master/tests/test_allocator.rs", "reuse_freed_range"
)
RUST_REF_2 = TestRef(
    "rust", "mooncake-store-master/tests/test_allocator.rs", "reuse_adjacent_range"
)


def rust_value(reference: TestRef) -> dict[str, str]:
    return {"file": reference.file, "test": reference.name}


def finding_codes(candidate: dict, rust_refs: set[TestRef]) -> set[str]:
    findings = validate_manifest(
        {"schema_version": 1, "entries": [candidate]}, {CPP_REF}, rust_refs
    )
    return {item.code for item in findings}


def entry(
    *,
    status: str = "covered",
    rust: list[dict[str, str]] | None = None,
    reason: str = "",
) -> dict:
    review = {
        "oracle": "allocator_test.cpp and allocation_strategy.cpp",
        "reviewed": True,
    }
    if status in {"covered", "blocked"}:
        review["primary_reviewed"] = True
    return {
        "cpp": {"file": CPP_REF.file, "test": CPP_REF.name},
        "behavior": "A freed allocator range can be reused without losing capacity.",
        "boundary": ["master", "allocator"],
        "status": status,
        "rust": (
            [{"file": RUST_REF.file, "test": RUST_REF.name}] if rust is None else rust
        ),
        "reason": reason,
        "review": review,
    }


class ValidateParityTest(unittest.TestCase):
    def test_covered_and_blocked_require_primary_review(self):
        for status in ("covered", "blocked"):
            candidate = entry(
                status=status,
                reason=(
                    "The existing primary is externally blocked."
                    if status == "blocked"
                    else ""
                ),
            )
            if status == "blocked":
                candidate["prerequisite"] = "CUDA-capable GPU"
            candidate["review"].pop("primary_reviewed")
            with self.subTest(status=status):
                self.assertIn(
                    "missing-primary-review",
                    finding_codes(candidate, {RUST_REF}),
                )

    def test_missing_row_rejects_stale_primary_review(self):
        missing = entry(
            status="missing",
            rust=[],
            reason="No unique primary exists yet.",
        )
        missing["review"]["primary_reviewed"] = True
        self.assertIn(
            "unexpected-primary-review",
            finding_codes(missing, set()),
        )

    def test_rejects_unmapped_cpp_test(self):
        findings = validate_manifest(
            {"schema_version": 1, "entries": []}, {CPP_REF}, set()
        )
        self.assertIn("unmapped-cpp-test", {item.code for item in findings})

    def test_covered_entry_requires_discoverable_rust_test(self):
        findings = validate_manifest(
            {"schema_version": 1, "entries": [entry()]}, {CPP_REF}, set()
        )
        self.assertIn("missing-rust-test", {item.code for item in findings})

    def test_not_applicable_requires_nonempty_reviewed_reason(self):
        not_applicable = entry(status="not-applicable", rust=[], reason="")
        not_applicable["review"]["na_category"] = "absent-rust-product-boundary"
        manifest = {
            "schema_version": 1,
            "entries": [not_applicable],
        }
        findings = validate_manifest(manifest, {CPP_REF}, set())
        self.assertIn("missing-disposition-reason", {item.code for item in findings})

    def test_not_applicable_requires_allowed_na_category(self):
        missing = entry(
            status="not-applicable",
            rust=[],
            reason="The C++ helper has no Rust product boundary.",
        )
        findings = validate_manifest(
            {"schema_version": 1, "entries": [missing]}, {CPP_REF}, set()
        )
        self.assertIn("missing-na-category", {item.code for item in findings})

        invalid = entry(
            status="not-applicable",
            rust=[],
            reason="The C++ helper has no Rust product boundary.",
        )
        invalid["review"]["na_category"] = "too-expensive"
        findings = validate_manifest(
            {"schema_version": 1, "entries": [invalid]}, {CPP_REF}, set()
        )
        self.assertIn("invalid-na-category", {item.code for item in findings})

    def test_non_na_row_rejects_stale_na_category(self):
        covered = entry()
        covered["review"]["na_category"] = "absent-rust-product-boundary"
        findings = validate_manifest(
            {"schema_version": 1, "entries": [covered]}, {CPP_REF}, {RUST_REF}
        )
        self.assertIn("unexpected-na-category", {item.code for item in findings})

    def test_covered_entry_rejects_multiple_primary_tests(self):
        covered = entry(rust=[rust_value(RUST_REF), rust_value(RUST_REF_2)])
        codes = finding_codes(covered, {RUST_REF, RUST_REF_2})
        self.assertIn("multiple-primary-rust-tests", codes)

    def test_blocked_entry_requires_one_discoverable_primary_and_prerequisite(self):
        blocked = entry(status="blocked", rust=[], reason="GPU execution is blocked.")
        codes = finding_codes(blocked, set())
        self.assertIn("blocked-without-rust-test", codes)
        self.assertIn("missing-blocked-prerequisite", codes)

    def test_blocked_entry_accepts_one_primary_and_explicit_prerequisite(self):
        blocked = entry(
            status="blocked",
            reason="The parity test exists but cannot execute on this host.",
        )
        blocked["prerequisite"] = "CUDA-capable GPU"
        findings = validate_manifest(
            {"schema_version": 1, "entries": [blocked]}, {CPP_REF}, {RUST_REF}
        )
        self.assertEqual(findings, [])

    def test_missing_and_not_applicable_entries_reject_primary_tests(self):
        for status in ("missing", "not-applicable"):
            candidate = entry(
                status=status,
                reason="This disposition must not claim a primary Rust test.",
            )
            if status == "not-applicable":
                candidate["review"]["na_category"] = "absent-rust-product-boundary"
            with self.subTest(status=status):
                self.assertIn(
                    "noncovered-with-rust-test",
                    finding_codes(candidate, {RUST_REF}),
                )

    def test_rejects_primary_reused_by_multiple_cpp_rows(self):
        second_cpp_ref = TestRef("gtest", "client_test.cpp", "ClientTest.Timeout")
        second = entry()
        second["cpp"] = {"file": second_cpp_ref.file, "test": second_cpp_ref.name}
        findings = validate_manifest(
            {"schema_version": 1, "entries": [entry(), second]},
            {CPP_REF, second_cpp_ref},
            {RUST_REF},
        )
        self.assertIn("duplicate-primary-rust-test", {item.code for item in findings})

    def test_summary_reports_one_to_one_counts(self):
        second_covered = entry()
        second_covered["cpp"] = {
            "file": "client_test.cpp",
            "test": "ClientTest.Timeout",
        }
        missing = entry(
            status="missing",
            rust=[],
            reason="No unique Rust primary test exists.",
        )
        missing["cpp"] = {
            "file": "master_test.cpp",
            "test": "MasterTest.Missing",
        }
        not_applicable = entry(
            status="not-applicable",
            rust=[],
            reason="The C++ helper has no Rust product boundary.",
        )
        not_applicable["cpp"] = {
            "file": "wrapper_test.cpp",
            "test": "WrapperTest.MoveOnly",
        }
        not_applicable["review"]["na_category"] = "absent-rust-product-boundary"
        manifest = {
            "schema_version": 1,
            "entries": [entry(), second_covered, missing, not_applicable],
        }
        self.assertEqual(
            validate_parity.summarize_manifest(manifest),
            {
                "cpp_total": 4,
                "applicable": 3,
                "unique_primary": 1,
                "duplicate_primary": 1,
                "covered": 2,
                "missing": 1,
                "blocked": 0,
                "not_applicable": 1,
            },
        )

    def test_rejects_duplicate_and_stale_cpp_references(self):
        duplicate = entry()
        stale = entry()
        stale["cpp"] = {"file": "removed_test.cpp", "test": "Old.Test"}
        findings = validate_manifest(
            {
                "schema_version": 1,
                "entries": [duplicate, duplicate.copy(), stale],
            },
            {CPP_REF},
            {RUST_REF},
        )
        codes = {item.code for item in findings}
        self.assertIn("duplicate-cpp-reference", codes)
        self.assertIn("stale-cpp-reference", codes)

    def test_rejects_unknown_status_and_missing_behavior(self):
        invalid = entry()
        invalid["status"] = "done"
        invalid["behavior"] = ""
        findings = validate_manifest(
            {"schema_version": 1, "entries": [invalid]}, {CPP_REF}, {RUST_REF}
        )
        codes = {item.code for item in findings}
        self.assertIn("invalid-status", codes)
        self.assertIn("missing-behavior", codes)

    def test_accepts_valid_mixed_manifest(self):
        covered = entry()
        missing_ref = TestRef("gtest", "client_test.cpp", "ClientTest.Timeout")
        missing = entry(
            status="missing",
            rust=[],
            reason="No Rust test currently asserts timeout cleanup.",
        )
        missing["cpp"] = {"file": missing_ref.file, "test": missing_ref.name}
        missing["behavior"] = "A timed-out request releases its registered memory."
        findings = validate_manifest(
            {"schema_version": 1, "entries": [covered, missing]},
            {CPP_REF, missing_ref},
            {RUST_REF},
        )
        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()
