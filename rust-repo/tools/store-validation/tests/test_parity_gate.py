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
    plan_parity_runs,
)


STORE_SUITE = {
    "id": "store-cpp",
    "framework": "gtest",
    "reference_root": "mooncake-store/tests",
}


def manifest(entries):
    return {"schema_version": 2, "suite": dict(STORE_SUITE), "entries": entries}


def entry(
    *,
    status="covered",
    reference_test="AllocatorTest.Allocate",
    rust=None,
):
    return {
        "reference": {
            "file": "allocation_strategy_test.cpp",
            "test": reference_test,
        },
        "behavior": "A successful allocation consumes visible capacity.",
        "boundary": ["master", "allocator"],
        "status": status,
        "rust": (
            [
                {
                    "file": "mooncake-store-master/tests/test_allocator.rs",
                    "test": "allocate_consumes_capacity",
                }
            ]
            if rust is None and status == "covered"
            else ([] if rust is None else rust)
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
        plan = plan_parity_run(manifest([entry(status="missing")]), self.inventory)
        self.assertEqual(plan.status, "BLOCKED")
        self.assertEqual(plan.commands, [])
        self.assertEqual(
            plan.blocked[0]["reference"]["test"],
            "AllocatorTest.Allocate",
        )

    def test_groups_discoverable_integration_test_by_cargo_target(self):
        plan = plan_parity_run(manifest([entry()]), self.inventory)
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

    def test_nested_integration_module_uses_root_target_and_qualified_filter(self):
        rust = {
            "file": "mooncake-store-master/tests/test_allocator/cachelib.rs",
            "test": "test_cachelib_like_allocator_reuses_freed_slot",
        }
        inventory = {TestRef("rust", rust["file"], rust["test"])}

        plan = plan_parity_run(manifest([entry(rust=[rust])]), inventory)

        self.assertEqual(
            plan.commands[0].argv,
            [
                "cargo",
                "test",
                "-p",
                "mooncake-store-master",
                "--test",
                "test_allocator",
                "cachelib::test_cachelib_like_allocator_reuses_freed_slot",
                "--",
                "--exact",
            ],
        )

    def test_shared_evidence_runs_once_and_retains_all_references(self):
        plan = plan_parity_run(
            manifest([entry(), entry(reference_test="AllocatorTest.Reallocate")]),
            self.inventory,
        )
        self.assertEqual(len(plan.commands), 1)
        self.assertEqual(
            [reference["test"] for reference in plan.commands[0].references],
            ["AllocatorTest.Allocate", "AllocatorTest.Reallocate"],
        )

    def test_shared_evidence_across_manifests_runs_once_with_suite_context(self):
        transfer_manifest = manifest([entry(reference_test="TransferTest.Submit")])
        transfer_manifest["suite"] = {
            "id": "transfer-engine-cpp",
            "framework": "gtest",
            "reference_root": "mooncake-transfer-engine/tests",
        }
        plan = plan_parity_runs(
            [manifest([entry()]), transfer_manifest],
            self.inventory,
        )
        self.assertEqual(len(plan.commands), 1)
        self.assertEqual(
            [reference["suite"] for reference in plan.commands[0].references],
            ["store-cpp", "transfer-engine-cpp"],
        )

    def test_aggregate_evidence_produces_one_command_per_rust_test(self):
        second = {
            "file": "transfer-engine-ffi/tests/test_engine.rs",
            "test": "allocate_via_ffi",
        }
        inventory = self.inventory | {TestRef("rust", second["file"], second["test"])}
        first = {
            "file": "mooncake-store-master/tests/test_allocator.rs",
            "test": "allocate_consumes_capacity",
        }
        plan = plan_parity_run(manifest([entry(rust=[first, second])]), inventory)
        self.assertEqual(len(plan.commands), 2)
        self.assertEqual(
            {command.references[0]["test"] for command in plan.commands},
            {"AllocatorTest.Allocate"},
        )

    def test_transfer_engine_ffi_integration_test_uses_own_package(self):
        rust = {
            "file": "transfer-engine-ffi/tests/test_engine.rs",
            "test": "test_submit",
        }
        inventory = {TestRef("rust", rust["file"], rust["test"])}
        plan = plan_parity_run(manifest([entry(rust=[rust])]), inventory)
        self.assertEqual(
            plan.commands[0].argv,
            [
                "cargo",
                "test",
                "-p",
                "transfer-engine-ffi",
                "--test",
                "test_engine",
                "test_submit",
                "--",
                "--exact",
            ],
        )

    def test_unit_test_under_src_uses_lib_filter(self):
        rust = {
            "file": "mooncake-store-client/src/client/remove.rs",
            "test": "remove_missing",
        }
        inventory = {TestRef("rust", rust["file"], rust["test"])}
        plan = plan_parity_run(manifest([entry(rust=[rust])]), inventory)
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

    def test_rust_evidence_can_request_an_additional_cargo_feature(self):
        rust = {
            "file": "mooncake-store-client/tests/test_s3_source.rs",
            "test": "test_miss_handler_batch_fetch_with_s3_hot_cache",
            "features": ["s3"],
            "module": "s3_tests",
        }
        inventory = {TestRef("rust", rust["file"], rust["test"])}

        plan = plan_parity_run(manifest([entry(rust=[rust])]), inventory)

        self.assertEqual(
            plan.commands[0].argv,
            [
                "cargo",
                "test",
                "-p",
                "mooncake-store-client",
                "--features",
                "link-native,s3",
                "--test",
                "test_s3_source",
                "s3_tests::test_miss_handler_batch_fetch_with_s3_hot_cache",
                "--",
                "--exact",
            ],
        )

    def test_rejects_reference_outside_crates(self):
        rust = {"file": "python/tests/test_client.py", "test": "test_put"}
        inventory = {TestRef("rust", rust["file"], rust["test"])}
        with self.assertRaisesRegex(ValueError, "outside Rust crates"):
            plan_parity_run(manifest([entry(rust=[rust])]), inventory)

    def test_execution_failure_retains_owning_reference_behavior(self):
        command = PlannedCommand(
            name="failing-parity-test",
            argv=[sys.executable, "-c", "raise SystemExit(7)"],
            rust={"file": "sample/tests/test_case.rs", "test": "case"},
            references=[{"file": "reference_test.cpp", "test": "ReferenceTest.Case"}],
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
            result["commands"][0]["references"][0]["test"],
            "ReferenceTest.Case",
        )

    def test_zero_executed_tests_is_a_failure_even_when_process_exits_zero(self):
        command = PlannedCommand(
            name="stale-filter",
            argv=[sys.executable, "-c", "print('running 0 tests')"],
            rust={"file": "sample/src/lib.rs", "test": "missing"},
            references=[{"file": "reference.cpp", "test": "Suite.Missing"}],
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
