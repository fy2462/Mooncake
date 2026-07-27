from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

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

    def test_discovers_rust_test_functions_and_ignores_helpers(self):
        refs = discover_rust_tests(FIXTURES)
        self.assertEqual(
            [(ref.file, ref.name) for ref in refs],
            [
                ("rust_tests.rs", "degraded_read_uses_remaining_replica"),
                ("rust_tests.rs", "tokio_restart_recovers_catalog"),
            ],
        )

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
