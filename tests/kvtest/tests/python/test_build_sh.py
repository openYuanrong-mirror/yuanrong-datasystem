#!/usr/bin/env python3
"""Behavior tests for the standalone kvtest build entrypoint."""

import os
import pathlib
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch


KVTEST_DIR = pathlib.Path(__file__).resolve().parents[2]


class TestKvtestBuildScript(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.script_dir = self.root / "tests" / "kvtest"
        self.script_dir.mkdir(parents=True)
        shutil.copy2(KVTEST_DIR / "build.sh", self.script_dir / "build.sh")
        shutil.copy2(KVTEST_DIR / "VERSION", self.script_dir / "VERSION")

        self.bin_dir = self.root / "fake-bin"
        self.bin_dir.mkdir()
        self.bazel_log = self.root / "bazel.log"
        self._write_executable(
            "bazel",
            """#!/bin/sh
printf '%s\n' "$*" >> "$KVTEST_BAZEL_LOG"
if [ "$1" = "build" ]; then
    mkdir -p "$PWD/bazel-bin/tests/kvtest"
    for name in kvtest coordinator_test worker_test; do
        if [ "$name" != "$KVTEST_MISSING_BINARY" ]; then
            printf '#!/bin/sh\n' > "$PWD/bazel-bin/tests/kvtest/$name"
            chmod +x "$PWD/bazel-bin/tests/kvtest/$name"
        fi
    done
    mkdir -p "$PWD/bazel-bin/yr/datasystem/lib"
    case " $* " in
        *" --config=jeprof "*) runtime='profiling runtime' ;;
        *) runtime='ordinary runtime' ;;
    esac
    printf '%s' "$runtime" > "$PWD/bazel-bin/yr/datasystem/lib/libjemalloc.so.2"
    mkdir -p "$PWD/fake-exec/external/local_urma/yr/datasystem/lib"
    printf 'urma runtime' > "$PWD/fake-exec/external/local_urma/yr/datasystem/lib/liburma.so"
fi
if [ "$1" = "info" ] && [ "$2" = "execution_root" ]; then
    printf '%s/fake-exec\n' "$PWD"
fi
""",
        )
        self._write_executable("gcc", "#!/bin/sh\nexit 1\n")
        self._write_executable("ldconfig", "#!/bin/sh\nexit 1\n")
        self._write_executable("make", "#!/bin/sh\nexit 0\n")
        self._write_executable("readelf", "#!/bin/sh\necho 'NEEDED libjemalloc.so.2'\n")

    def tearDown(self):
        self._tmp.cleanup()

    def _write_executable(self, name, content):
        path = self.bin_dir / name
        path.write_text(content, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    def _run(self, *args):
        env = os.environ.copy()
        env["PATH"] = f"{self.bin_dir}:{env['PATH']}"
        env["KVTEST_BAZEL_LOG"] = str(self.bazel_log)
        env["JOBS"] = "2"
        return subprocess.run(
            ["bash", str(self.script_dir / "build.sh"), *args],
            cwd=self.script_dir,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def _bazel_commands(self):
        if not self.bazel_log.exists():
            return []
        return self.bazel_log.read_text(encoding="utf-8").splitlines()

    def test_urma_option_controls_bazel_config(self):
        cases = [
            (["-b", "bazel", "-c", "-M", "on"], True),
            (["-b", "bazel", "-M", "off"], False),
            (["-b", "bazel"], False),
        ]
        for args, expect_urma in cases:
            with self.subTest(args=args):
                # unlink(missing_ok=...) is 3.8+; this repo's tests run on
                # 3.7 too (see test_deploy_coordinator's 3.7 note), so guard
                # the unlink explicitly instead of relying on missing_ok.
                if self.bazel_log.exists():
                    self.bazel_log.unlink()
                result = self._run(*args)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                build_commands = [cmd for cmd in self._bazel_commands() if cmd.startswith("build ")]
                self.assertEqual(len(build_commands), 3 if expect_urma else 2, self._bazel_commands())
                self.assertEqual("--config=urma" in build_commands[0], expect_urma)
                self.assertEqual((self.script_dir / "build/lib/libjemalloc.so.2").read_text(),
                                 "ordinary runtime")

    def test_rejects_invalid_urma_option(self):
        result = self._run("-b", "bazel", "-M", "invalid")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("choose from on or off", result.stdout + result.stderr)

    def test_rejects_missing_urma_option(self):
        result = self._run("-b", "bazel", "-M")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("-M requires on or off", result.stdout + result.stderr)

    def test_rejects_urma_option_for_cmake_sdk_build(self):
        result = self._run("-b", "cmake", "-M", "on")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("-M on is supported only with -b bazel", result.stdout + result.stderr)

    def test_jemalloc_profile_build_and_disable_replace_runtime(self):
        result = self._run("-b", "bazel", "-x", "on")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = self._bazel_commands()
        self.assertEqual(len(commands), 2, commands)
        self.assertTrue(all("--config=jeprof" in cmd for cmd in commands))
        self.assertIn("//:libjemalloc_shared_file", commands[1])
        runtime = self.script_dir / "build/lib/libjemalloc.so.2"
        self.assertEqual(runtime.read_text(), "profiling runtime")
        packaged = self.script_dir / "output/lib/libjemalloc.so.2"
        packaged.parent.mkdir(parents=True)
        packaged.write_text("stale")
        result = self._run("-b", "bazel", "-x", "off")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(runtime.read_text(), "ordinary runtime")
        self.assertFalse(packaged.exists())
        self.assertIn("--define=enable_jemalloc_prof=false", self._bazel_commands()[-1])

    def test_rejects_invalid_or_missing_jemalloc_option(self):
        for args, message in [
            (["-x"], "-x requires on or off"),
            (["-x", "bad"], "choose from on or off"),
            (["-b", "cmake", "-x", "on"], "-x on is supported only with -b bazel"),
        ]:
            with self.subTest(args=args):
                result = self._run(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stdout + result.stderr)

    def test_profile_build_rejects_missing_link(self):
        for name in ["kvtest", "coordinator_test", "worker_test"]:
            self._write_executable(
                "readelf", '#!/bin/sh\ncase "$2" in\n'
                f'    */{name}) echo "NEEDED libc.so.6" ;;\n'
                '    *) echo "NEEDED libjemalloc.so.2" ;;\nesac\n')
            for enabled in ["on", "off"]:
                with self.subTest(name=name, enabled=enabled):
                    result = self._run("-x", enabled)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(f"{name} is not linked", result.stdout)

    def test_missing_bazel_output_rejects_stale_staged_binary(self):
        for name in ["coordinator_test", "worker_test"]:
            with self.subTest(name=name):
                stale = self.script_dir / "build" / name
                stale.parent.mkdir(parents=True, exist_ok=True)
                stale.write_text("stale")
                output = self.root / "bazel-bin/tests/kvtest" / name
                if output.exists():
                    output.unlink()
                with patch.dict(os.environ, {"KVTEST_MISSING_BINARY": name}):
                    result = self._run("-M", "off")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"{name} binary not found", result.stdout)

    def test_help_lists_profiling_option(self):
        result = self._run("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("-x on|off", result.stdout)


class TestKvtestPackage(unittest.TestCase):
    def test_runtime_and_launcher_are_packaged_without_stale_profile_library(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            for directory in ['build/lib', 'tools', 'config', 'tests', 'src']:
                (root / directory).mkdir(parents=True)
            for name in ['kvtest', 'coordinator_test', 'worker_test']:
                (root / 'build' / name).write_text('binary')
            for name in ['deploy_client.py', 'deploy_common.py', 'deploy_pods.py', 'deploy_jf.py',
                         'parse_resource.py', 'tools/procmon.py',
                         'tools/standalone_launcher.py', 'VERSION',
                         'config/config.json.example', 'config/deploy.json.example']:
                shutil.copy2(KVTEST_DIR / name, root / name)
            runtime = root / 'build/lib/libjemalloc.so.2'
            runtime.write_text('profiling runtime')
            for enabled in [True, False]:
                if not enabled:
                    runtime.unlink()
                    for name in ['kvtest', 'coordinator_test', 'worker_test']:
                        (root / 'output' / name).chmod(0o555)
                        (root / 'build' / name).write_text('replacement')
                result = subprocess.run(
                    ['make', '-f', str(KVTEST_DIR / 'Makefile'), 'package'],
                    cwd=root, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual((root / 'output/lib/libjemalloc.so.2').exists(), enabled)
                for name in ['kvtest', 'coordinator_test', 'worker_test']:
                    self.assertEqual((root / 'output' / name).read_text(),
                                     'binary' if enabled else 'replacement')
                self.assertTrue((root / 'output/standalone_launcher.py').is_file())
                result = subprocess.run(
                    ['python3', str(root / 'output/deploy_client.py'), 'deploy', '--help'],
                    cwd=root, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('--jemalloc_prof_conf', result.stdout)


if __name__ == "__main__":
    unittest.main()
