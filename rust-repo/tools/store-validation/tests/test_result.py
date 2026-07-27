from pathlib import Path
import json
import tempfile
import unittest

from result import aggregate_status, build_gate_result, write_json_atomic


class ResultTest(unittest.TestCase):
    def test_required_skip_or_block_cannot_pass(self):
        self.assertEqual(aggregate_status(["PASS", "SKIP"]), "SKIP")
        self.assertEqual(aggregate_status(["PASS", "BLOCKED"]), "BLOCKED")

    def test_failure_has_priority_and_is_retained(self):
        commands = [
            {
                "name": "first",
                "argv": ["false"],
                "status": "FAIL",
                "exit_code": 1,
                "log": "first.log",
            },
            {
                "name": "second",
                "argv": ["true"],
                "status": "PASS",
                "exit_code": 0,
                "log": "second.log",
            },
        ]
        result = build_gate_result(
            "module",
            commands,
            started_at="start",
            finished_at="finish",
            duration_seconds=1.25,
            environment={},
        )
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["first_failure"]["name"], "first")

    def test_atomic_writer_leaves_valid_json_without_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            write_json_atomic(path, {"status": "PASS"})
            self.assertEqual(json.loads(path.read_text()), {"status": "PASS"})
            self.assertFalse(path.with_name("result.json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
