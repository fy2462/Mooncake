import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class DependenciesInstallerTest(unittest.TestCase):
    installer = Path(__file__).parents[1].joinpath("dependencies.sh")

    def run_deploy(
        self, staging: Path, prefix: Path, source: Path
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "INSTALLER": str(self.installer),
                "MOONCAKE_DEPENDENCIES_LIBRARY_ONLY": "1",
                "SPDK_INSTALL_PREFIX": str(prefix),
                "SPDK_INSTALL_MANIFEST": str(
                    prefix.joinpath("share/mooncake/spdk-25.05.manifest")
                ),
                "SPDK_SOURCE_DIR": str(source),
                "STAGING": str(staging),
            }
        )
        return subprocess.run(
            ["bash", "-c", 'source "$INSTALLER"; deploy_spdk_sdk "$STAGING"'],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    @staticmethod
    def make_staging(root: Path) -> Path:
        staging = root.joinpath("staging")
        staging.joinpath("include/spdk").mkdir(parents=True)
        staging.joinpath("include/spdk/version.h").write_text("#define SPDK 1\n")
        staging.joinpath("lib/pkgconfig").mkdir(parents=True)
        return staging

    def test_spdk_uses_v2_11_compatible_stack_from_shared_storage(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertEqual(script.count("SPDK_REPOSITORY=openebs/spdk"), 1)
        self.assertEqual(script.count("SPDK_RS_REPOSITORY=openebs/spdk-rs"), 1)
        self.assertEqual(
            script.count("SPDK_COMMIT=cc090cd2b64775545eb38022bb0ec8f37f4741a6"),
            1,
        )
        self.assertEqual(
            script.count("SPDK_RS_COMMIT=78d6018af041e80a42e222165b86070bae631821"),
            1,
        )
        self.assertEqual(
            script.count("DPDK_COMMIT=cf36799c473a686fa14fde9af97f917a2125d3d5"),
            1,
        )
        self.assertIn(
            'SHARED_BUILD_ROOT=${SHARED_BUILD_ROOT:-"$INSTALL_HOME/workspace/tmp/mooncake"}',
            script,
        )
        self.assertIn(
            'SPDK_SOURCE_DIR=${SPDK_SOURCE_DIR:-"$SHARED_BUILD_ROOT/spdk-25.05"}',
            script,
        )
        self.assertIn(
            'SPDK_RS_SOURCE_DIR=${SPDK_RS_SOURCE_DIR:-"$SHARED_BUILD_ROOT/spdk-rs-v2.11.0"}',
            script,
        )
        self.assertIn(
            'SPDK_STAGING_DIR=${SPDK_STAGING_DIR:-"$SHARED_BUILD_ROOT/spdk-sdk-25.05"}',
            script,
        )
        self.assertIn("${GITHUB_PROXY}/${SPDK_REPOSITORY}.git", script)
        self.assertIn(
            'git -C "$SPDK_SOURCE_DIR" checkout --detach "$SPDK_COMMIT"', script
        )
        self.assertIn(
            'git -C "$SPDK_SOURCE_DIR" status --porcelain --untracked-files=no',
            script,
        )
        self.assertIn('export SPDK_ROOT_DIR="$SPDK_INSTALL_PREFIX"', script)
        self.assertIn(
            'test "$(git -C "$SPDK_SOURCE_DIR/dpdk" rev-parse HEAD)" = "$DPDK_COMMIT"',
            script,
        )
        self.assertIn(
            'SPDK_BUILD_SCRIPT="$SPDK_RS_SOURCE_DIR/build_scripts/build_spdk.sh"',
            script,
        )
        self.assertIn('"$SPDK_BUILD_SCRIPT"', script)
        for command in ("configure", "make", 'install "$SPDK_STAGING_DIR"'):
            self.assertIn(command, script)

    def test_spdk_install_does_not_mutate_repository_checkout(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertNotIn('cd "${REPO_ROOT}/extern"', script)
        self.assertNotIn("rm -rf spdk", script)
        self.assertNotIn("SPDK_VERSION=v26.01", script)

    def test_spdk_install_is_manifest_controlled_under_usr_local(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertIn("SPDK_INSTALL_PREFIX=${SPDK_INSTALL_PREFIX:-/usr/local}", script)
        self.assertIn(
            'SPDK_INSTALL_MANIFEST=${SPDK_INSTALL_MANIFEST:-"$SPDK_INSTALL_PREFIX/share/mooncake/spdk-25.05.manifest"}',
            script,
        )
        self.assertIn("deploy_spdk_sdk", script)
        self.assertIn('deploy_spdk_sdk "$SPDK_STAGING_DIR"', script)
        self.assertIn('realpath -ms "$installed_path"', script)
        self.assertIn("assert_safe_spdk_parent", script)
        self.assertIn('owned_paths["$installed_path"]', script)
        self.assertIn("Refusing to overwrite unmanaged path", script)
        self.assertNotIn('cp -a "$staging_dir/."', script)
        self.assertIn("Installed SPDK pkg-config metadata still references", script)
        self.assertIn("SPDK deployment journal", script)
        self.assertIn('export SPDK_ROOT_DIR="$SPDK_INSTALL_PREFIX"', script)
        self.assertNotIn("rm -rf /usr/local", script)

    def test_spdk_static_link_dependencies_are_installed(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertIn("libjitterentropy3-dev", script)

    def test_spdk_reinstall_accepts_only_the_pinned_build_helper_patch(self) -> None:
        script = Path(__file__).parents[1].joinpath("dependencies.sh").read_text()
        self.assertIn("is_expected_spdk_build_patch", script)
        self.assertIn(
            "SPDK_ISAL_CRYPTO_CONFIGURE_SHA256=",
            script,
        )
        self.assertIn(
            "if ! is_expected_spdk_build_patch; then",
            script,
        )
        self.assertIn(
            'git -C "$SPDK_SOURCE_DIR/isa-l-crypto" status --porcelain --untracked-files=no',
            script,
        )

    def test_spdk_deploy_rewrites_metadata_and_records_owned_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prefix = root.joinpath("prefix")
            source = root.joinpath("source")
            staging = self.make_staging(root)
            pc_file = staging.joinpath("lib/pkgconfig/spdk-test.pc")
            pc_file.write_text(f"prefix={staging}\nCflags: -I{source}/build/include\n")

            result = self.run_deploy(staging, prefix, source)

            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            installed_pc = prefix.joinpath("lib/pkgconfig/spdk-test.pc").read_text()
            self.assertIn(f"prefix={prefix}", installed_pc)
            self.assertIn(f"-I{prefix}/include", installed_pc)
            manifest = prefix.joinpath("share/mooncake/spdk-25.05.manifest").read_text()
            self.assertIn(str(prefix.joinpath("include/spdk/version.h")), manifest)
            self.assertIn(str(prefix.joinpath("lib/pkgconfig/spdk-test.pc")), manifest)

    def test_spdk_deploy_rejects_unmanaged_collision(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prefix = root.joinpath("prefix")
            source = root.joinpath("source")
            staging = self.make_staging(root)
            collision = prefix.joinpath("include/spdk/version.h")
            collision.parent.mkdir(parents=True)
            collision.write_text("unmanaged\n")

            result = self.run_deploy(staging, prefix, source)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Refusing to overwrite unmanaged path", result.stdout)
            self.assertEqual(collision.read_text(), "unmanaged\n")

    def test_spdk_deploy_rejects_symlink_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prefix = root.joinpath("prefix")
            source = root.joinpath("source")
            staging = self.make_staging(root)
            outside = root.joinpath("outside")
            outside.mkdir()
            prefix.mkdir()
            prefix.joinpath("include").symlink_to(outside, target_is_directory=True)

            result = self.run_deploy(staging, prefix, source)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Symlink parent is unsafe", result.stdout)
            self.assertEqual(list(outside.iterdir()), [])

    def test_spdk_deploy_recovers_from_published_partial_journal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prefix = root.joinpath("prefix")
            source = root.joinpath("source")
            staging = self.make_staging(root)
            staging.joinpath("include/spdk/second.h").write_text("second\n")
            installed = prefix.joinpath("include/spdk/version.h")
            installed.parent.mkdir(parents=True)
            installed.write_text("partial\n")
            manifest = prefix.joinpath("share/mooncake/spdk-25.05.manifest")
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                f"{installed}\n{prefix.joinpath('include/spdk/second.h')}\n"
            )

            result = self.run_deploy(staging, prefix, source)

            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertEqual(installed.read_text(), "#define SPDK 1\n")
            self.assertEqual(
                prefix.joinpath("include/spdk/second.h").read_text(), "second\n"
            )


if __name__ == "__main__":
    unittest.main()
