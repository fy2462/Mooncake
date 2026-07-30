from pathlib import Path
import unittest

from inventory import TestRef
from suites import SUITES, SuiteDefinition, discover_suite_tests


TOOL_ROOT = Path(__file__).parents[1]


class SuiteDefinitionsTest(unittest.TestCase):
    def test_defines_the_four_approved_reference_suites(self):
        self.assertEqual(
            {
                suite_id: (
                    suite.framework,
                    suite.reference_root,
                    suite.excluded_files,
                )
                for suite_id, suite in SUITES.items()
            },
            {
                "store-cpp": ("gtest", "mooncake-store/tests", frozenset()),
                "transfer-engine-cpp": (
                    "gtest",
                    "mooncake-transfer-engine/tests",
                    frozenset(),
                ),
                "tent-cpp": (
                    "gtest",
                    "mooncake-transfer-engine/tent/tests",
                    frozenset(),
                ),
                "wheel-store-python": (
                    "python",
                    "mooncake-wheel/tests",
                    frozenset({"test_release_wheel_tags.py"}),
                ),
            },
        )

    def test_routes_gtest_and_python_suites_to_source_discovery(self):
        gtest = discover_suite_tests(
            TOOL_ROOT,
            SuiteDefinition("fixture-cpp", "gtest", "tests/fixtures", frozenset()),
        )
        python = discover_suite_tests(
            TOOL_ROOT,
            SuiteDefinition(
                "fixture-python",
                "python",
                "tests/fixtures",
                frozenset({"test_release_wheel_tags.py"}),
            ),
        )
        self.assertIn(
            TestRef("gtest", "cpp_tests.cpp", "AllocatorTest.ReusesFreedRange"),
            gtest,
        )
        self.assertIn(
            TestRef("python", "python_tests.py", "test_function"),
            python,
        )
        self.assertNotIn(
            "test_release_packaging_only",
            {reference.name for reference in python},
        )

    def test_rejects_an_unknown_reference_framework(self):
        with self.assertRaisesRegex(ValueError, "unsupported reference framework"):
            discover_suite_tests(
                TOOL_ROOT,
                SuiteDefinition(
                    "fixture-unknown",
                    "unknown",
                    "tests/fixtures",
                    frozenset(),
                ),
            )


if __name__ == "__main__":
    unittest.main()
