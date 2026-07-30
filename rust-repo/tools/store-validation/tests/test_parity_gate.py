from pathlib import Path
import sys
import tempfile
import unittest

from inventory import TestRef
from run_parity_gate import (
    ParityPlan,
    PlannedCommand,
    execute_plan,
    plan_parity_run,
)


def entry(
    *,
    status="covered",
    cpp_test="AllocatorTest.Allocate",
    rust_file="mooncake-store-master/tests/test_allocator.rs",
    rust_test="allocate_consumes_capacity",
):
    return {
        "cpp": {"file": "allocation_strategy_test.cpp", "test": cpp_test},
        "behavior": "A successful allocation consumes visible capacity.",
        "boundary": ["master", "allocator"],
        "status": status,
        "rust": (
            [{"file": rust_file, "test": rust_test}] if status == "covered" else []
        ),
        "reason": "Uncovered required behavior." if status != "covered" else "",
        "review": {"oracle": "allocation_strategy_test.cpp", "reviewed": True},
    }


class ParityGateTest(unittest.TestCase):
    def setUp(self):
        self.inventory = {
            TestRef(
                "rust",
                "mooncake-store-master/tests/test_allocator.rs",
                "allocate_consumes_capacity",
            )
        }

    def test_missing_row_blocks_execution_pass(self):
        plan = plan_parity_run(
            {"schema_version": 1, "entries": [entry(status="missing")]},
            self.inventory,
        )
        self.assertEqual(plan.status, "BLOCKED")
        self.assertEqual(plan.commands, [])
        self.assertEqual(plan.blocked[0]["cpp"]["test"], "AllocatorTest.Allocate")

    def test_groups_discoverable_integration_test_by_cargo_target(self):
        plan = plan_parity_run(
            {"schema_version": 1, "entries": [entry()]}, self.inventory
        )
        self.assertEqual(plan.status, "READY")
        self.assertEqual(
            [command.argv for command in plan.commands],
            [
                [
                    "cargo",
                    "test",
                    "-p",
                    "mooncake-store-master",
                    "--test",
                    "test_allocator",
                    "allocate_consumes_capacity",
                    "--",
                    "--exact",
                ]
            ],
        )

    def test_duplicate_primary_is_rejected_even_when_planner_is_called_directly(self):
        manifest = {
            "schema_version": 1,
            "entries": [entry(), entry(cpp_test="AllocatorTest.Reallocate")],
        }
        with self.assertRaisesRegex(ValueError, "reused by"):
            plan_parity_run(manifest, self.inventory)

    def test_unit_test_under_src_uses_lib_filter(self):
        inventory = {
            TestRef(
                "rust", "mooncake-store-client/src/client/remove.rs", "remove_missing"
            )
        }
        manifest = {
            "schema_version": 1,
            "entries": [
                entry(
                    rust_file="mooncake-store-client/src/client/remove.rs",
                    rust_test="remove_missing",
                )
            ],
        }
        plan = plan_parity_run(manifest, inventory)
        self.assertEqual(
            plan.commands[0].argv,
            [
                "cargo",
                "test",
                "-p",
                "mooncake-store-client",
                "--features",
                "link-native",
                "remove_missing",
                "--lib",
            ],
        )

    def test_rejects_reference_outside_crates(self):
        inventory = {TestRef("rust", "python/tests/test_client.py", "test_put")}
        manifest = {
            "schema_version": 1,
            "entries": [
                entry(rust_file="python/tests/test_client.py", rust_test="test_put")
            ],
        }
        with self.assertRaisesRegex(ValueError, "outside Rust crates"):
            plan_parity_run(manifest, inventory)

    def test_execution_failure_retains_owning_cpp_behavior(self):
        command = PlannedCommand(
            name="failing-parity-test",
            argv=[sys.executable, "-c", "raise SystemExit(7)"],
            rust={"file": "sample/tests/test_case.rs", "test": "case"},
            cpp_references=[
                {"file": "reference_test.cpp", "test": "ReferenceTest.Case"}
            ],
        )
        with tempfile.TemporaryDirectory() as directory:
            result = execute_plan(
                ParityPlan(status="READY", commands=[command], blocked=[]),
                Path(__file__).parents[3],
                Path(directory),
            )
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["first_failure"]["name"], "failing-parity-test")
        self.assertEqual(
            result["commands"][0]["cpp_references"][0]["test"],
            "ReferenceTest.Case",
        )

    def test_zero_executed_tests_is_a_failure_even_when_process_exits_zero(self):
        command = PlannedCommand(
            name="stale-filter",
            argv=[sys.executable, "-c", "print('running 0 tests')"],
            rust={"file": "sample/src/lib.rs", "test": "missing"},
            cpp_references=[{"file": "reference.cpp", "test": "Suite.Missing"}],
        )
        with tempfile.TemporaryDirectory() as directory:
            result = execute_plan(
                ParityPlan(status="READY", commands=[command], blocked=[]),
                Path(__file__).parents[3],
                Path(directory),
            )
        self.assertEqual(result["status"], "FAIL")
        self.assertTrue(result["commands"][0]["zero_tests_executed"])


if __name__ == "__main__":
    unittest.main()
