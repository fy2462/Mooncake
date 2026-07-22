from pathlib import Path
import unittest


class DependenciesInstallerTest(unittest.TestCase):
    def test_spdk_uses_pinned_openebs_system_checkout(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertEqual(script.count("SPDK_REPOSITORY=openebs/spdk"), 1)
        self.assertEqual(
            script.count(
                "SPDK_COMMIT=bc57f3ea7933b0965c09e9d751c21a3968c6cc11"
            ),
            1,
        )
        self.assertEqual(script.count("SPDK_ROOT_DIR=/opt/mooncake/spdk"), 1)
        self.assertIn('${GITHUB_PROXY}/${SPDK_REPOSITORY}.git', script)
        self.assertIn(
            'git -C "$SPDK_ROOT_DIR" checkout --detach "$SPDK_COMMIT"', script
        )
        self.assertIn(
            'git -C "$SPDK_ROOT_DIR" status --porcelain --untracked-files=no',
            script,
        )
        self.assertIn('export SPDK_ROOT_DIR="$SPDK_ROOT_DIR"', script)
        for option in (
            "--without-shared",
            "--with-uring",
            "--without-uring-zns",
            "--without-nvme-cuse",
            "--without-fuse",
            "--disable-unit-tests",
            "--disable-tests",
            "--with-rdma",
            "--with-crypto",
            "--max-lcores=256",
        ):
            self.assertIn(option, script)

    def test_spdk_install_does_not_mutate_repository_checkout(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertNotIn('cd "${REPO_ROOT}/extern"', script)
        self.assertNotIn("rm -rf spdk", script)
        self.assertNotIn("SPDK_VERSION=v26.01", script)


if __name__ == "__main__":
    unittest.main()
