import unittest

from inventory import TestRef
from validate_parity import validate_manifest


CPP_REF = TestRef("gtest", "allocator_test.cpp", "AllocatorTest.Reuse")
RUST_REF = TestRef(
    "rust", "mooncake-store-master/tests/test_allocator.rs", "reuse_freed_range"
)


def entry(
    *,
    status: str = "covered",
    rust: list[dict[str, str]] | None = None,
    reason: str = "",
) -> dict:
    return {
        "cpp": {"file": CPP_REF.file, "test": CPP_REF.name},
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
        manifest = {
            "schema_version": 1,
            "entries": [entry(status="not-applicable", rust=[], reason="")],
        }
        findings = validate_manifest(manifest, {CPP_REF}, set())
        self.assertIn("missing-disposition-reason", {item.code for item in findings})

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
