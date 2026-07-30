from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import inventory
from inventory import discover_cpp_tests, discover_rust_tests


FIXTURES = Path(__file__).parent / "fixtures"
TOOL_ROOT = Path(__file__).parents[1]


class InventoryTest(unittest.TestCase):
    def test_discovers_google_test_macros_and_ignores_comments_and_strings(self):
        refs = discover_cpp_tests(FIXTURES)
        self.assertEqual(
            [(ref.file, ref.name) for ref in refs],
            [
                ("cpp_tests.cpp", "AllocatorTest.ReusesFreedRange"),
                ("cpp_tests.cpp", "ReplicaParamTest.DegradedRead"),
            ],
        )

    def test_discovers_google_tests_after_cpp_digit_separator(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "digit_separator_test.cpp"
            fixture.write_text(
                "constexpr auto kDeadlineNs = 1'000;\n"
                "TEST(DeadlineTest, DiscoveredAfterLiteral) {}\n"
                "constexpr char kLetter = u8'a';\n"
                "TEST(DeadlineTest, DiscoveredAfterPrefixedCharacter) {}\n",
                encoding="utf-8",
            )

            refs = discover_cpp_tests(Path(directory))

        self.assertEqual(
            [(ref.file, ref.name) for ref in refs],
            [
                (
                    "digit_separator_test.cpp",
                    "DeadlineTest.DiscoveredAfterLiteral",
                ),
                (
                    "digit_separator_test.cpp",
                    "DeadlineTest.DiscoveredAfterPrefixedCharacter",
                ),
            ],
        )

    def test_discovers_rust_test_functions_and_ignores_helpers(self):
        refs = discover_rust_tests(FIXTURES)
        self.assertEqual(
            [(ref.file, ref.name) for ref in refs],
            [
                ("rust_tests.rs", "degraded_read_uses_remaining_replica"),
                ("rust_tests.rs", "test_after_lifetime_is_discovered"),
                ("rust_tests.rs", "tokio_restart_recovers_catalog"),
            ],
        )

    def test_discovers_python_test_declarations_without_importing_modules(self):
        refs = inventory.discover_python_tests(
            FIXTURES,
            excluded_files={"test_release_wheel_tags.py"},
        )
        self.assertIn(
            inventory.TestRef("python", "python_tests.py", "test_function"),
            refs,
        )
        self.assertIn(
            inventory.TestRef("python", "python_tests.py", "TestClient.test_method"),
            refs,
        )
        self.assertNotIn("test_nested", {ref.name for ref in refs})
        self.assertNotIn(
            "HelperClient.test_method_on_non_test_class",
            {ref.name for ref in refs},
        )

    def test_python_discovery_honors_explicit_file_exclusions(self):
        included = inventory.discover_python_tests(FIXTURES)
        excluded = inventory.discover_python_tests(
            FIXTURES,
            excluded_files={"test_release_wheel_tags.py"},
        )
        release_ref = inventory.TestRef(
            "python",
            "test_release_wheel_tags.py",
            "test_release_packaging_only",
        )
        self.assertIn(release_ref, included)
        self.assertNotIn(release_ref, excluded)

    def test_cli_returns_two_for_an_unreadable_root(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [
                    sys.executable,
                    str(TOOL_ROOT / "inventory.py"),
                    "--cpp-root",
                    str(Path(directory) / "missing"),
                    "--rust-root",
                    str(FIXTURES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("inventory error", result.stderr)


if __name__ == "__main__":
    unittest.main()
