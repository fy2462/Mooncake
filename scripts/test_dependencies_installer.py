from pathlib import Path
import unittest


class DependenciesInstallerTest(unittest.TestCase):
    def test_spdk_version_has_one_v26_source_of_truth(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertEqual(script.count("SPDK_VERSION=v26.01"), 1)
        self.assertIn('git checkout "$SPDK_VERSION"', script)
        self.assertIn('SPDK ($SPDK_VERSION)', script)
        self.assertNotIn("v23.01.1", script)


if __name__ == "__main__":
    unittest.main()
