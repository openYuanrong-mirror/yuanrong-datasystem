import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from deploy_client import Deployer

KVTEST_DIR = pathlib.Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which('cc') and sys.platform.startswith('linux'), 'requires Linux C compiler')
class TestJemallocRuntime(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.package = self.root / 'package'
        self.lib = self.package / 'lib'
        self.lib.mkdir(parents=True)
        self.allocator = self.lib / 'libjemalloc.so.2'
        self.compile('int mallctl(void) { return 0; }', self.allocator,
                     '-shared', '-fPIC', '-Wl,-soname,libjemalloc.so.2')

    def compile(self, source, output, *args):
        subprocess.run(['cc', '-x', 'c', '-', '-x', 'none', '-o', str(output), *args],
                       input=source, text=True, check=True, capture_output=True)

    def build_binary(self, sdk_lib=None):
        sdk_decl = 'extern int sdk_version(void);' if sdk_lib else ''
        sdk_print = 'printf("sdk=%d\\n", sdk_version());' if sdk_lib else ''
        source = (
            '#include <stdio.h>\n#include <stdlib.h>\nextern int mallctl(void);\n'
            + sdk_decl + '\nint main(void) { void *p = malloc(32); free(p); '
            'puts("jemalloc_prof_supported=false"); ' + sdk_print + ' return mallctl(); }')
        args = ['-fno-builtin', '-Wl,--enable-new-dtags,-rpath,$ORIGIN/lib', str(self.allocator)]
        if sdk_lib:
            args.append(str(sdk_lib))
        binary = self.package / 'kvtest'
        self.compile(source, binary, *args)
        return binary

    def test_needed_jemalloc_without_malloc_interposition_is_rejected(self):
        binary = self.build_binary()
        env = dict(os.environ, TEST_TMPDIR=str(self.root / 'relocated'))
        result = subprocess.run(
            ['bash', str(KVTEST_DIR / 'tests/jemalloc_prof_test.sh'),
             str(binary), 'false', str(self.allocator)],
            env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('jemalloc_prof_supported=false', result.stdout)
        self.assertIn('libjemalloc.so.2', result.stdout)
        self.assertIn('malloc is not provided by the bundled jemalloc', result.stderr)

    @unittest.skipUnless(os.environ.get('JEMALLOC_TEST_RUNTIME'), 'requires profiling jemalloc runtime')
    def test_known_allocation_is_present_in_profile(self):
        shutil.copy2(os.environ['JEMALLOC_TEST_RUNTIME'], self.allocator)
        source = (
            '#include <stdbool.h>\n#include <stdio.h>\n#include <stdlib.h>\n'
            'extern int mallctl(const char *, void *, size_t *, void *, size_t);\n'
            'void *sample;\n'
            '__attribute__((noinline)) void AllocateSample(void) { sample = malloc(4194304); }\n'
            'int main(void) { bool supported = false; size_t size = sizeof(supported); '
            'if (mallctl("config.prof", &supported, &size, NULL, 0)) return 1; '
            'printf("jemalloc_prof_supported=%s\\n", supported ? "true" : "false"); '
            'void *p = malloc(32); free(p); AllocateSample(); return sample ? 0 : 1; }')
        binary = self.package / 'allocation_probe'
        self.compile(source, binary, '-fno-builtin', '-Wl,-rpath,$ORIGIN/lib', str(self.allocator))
        stage = self.root / 'relocated'
        subprocess.run(
            ['bash', str(KVTEST_DIR / 'tests/jemalloc_prof_test.sh'),
             str(binary), 'true', str(self.allocator)],
            env=dict(os.environ, TEST_TMPDIR=str(stage)), check=True, capture_output=True, text=True)
        heaps = list(stage.glob('*/logs/jemalloc/*.heap'))
        self.assertTrue(heaps)
        totals = [int(value) for heap in heaps
                  for value in re.findall(r'^[ \t]*t\*: \d+: (\d+)', heap.read_text(), re.MULTILINE)]
        self.assertTrue(any(total >= 4194304 for total in totals), totals)

    def test_old_runtime_directory_cannot_override_selected_sdk(self):
        remote = self.root / 'remote'
        old_lib = remote / 'lib'
        old_lib.mkdir(parents=True)
        sdk = self.root / 'sdk'
        sdk.mkdir()
        for directory, version in [(old_lib, 1), (sdk, 2)]:
            self.compile(f'int sdk_version(void) {{ return {version}; }}',
                         directory / 'libfixture.so', '-shared', '-fPIC', '-Wl,-soname,libfixture.so')
        binary = self.build_binary(sdk / 'libfixture.so')
        deploy = self.root / 'deploy.json'
        node = {'host': 'localhost', 'host_ip': '192.0.2.1', 'instance_id': 0,
                'remote_sdk_dir': str(sdk)}
        deploy.write_text(json.dumps({'nodes': [node], 'remote_work_dir': str(remote)}))
        config = self.root / 'config.json'
        config.write_text('{}')
        d = Deployer(str(deploy), str(config))
        d.binary_path = str(binary)
        outputs = []

        def execute(node, command, **kwargs):
            if command == 'pgrep -x kvtest':
                return subprocess.CompletedProcess([], 1, '', '')
            if 'nohup ./kvtest' in command or '--binary' in command:
                words = shlex.split(command)
                if '--lib-path' in words:
                    lib_path = words[words.index('--lib-path') + 1]
                else:
                    assignment = next(word for word in words if word.startswith('LD_LIBRARY_PATH='))
                    lib_path = assignment.split('=', 1)[1].replace('${LD_LIBRARY_PATH:-}', '')
                env = dict(os.environ, LD_LIBRARY_PATH=lib_path)
                output = subprocess.check_output([str(remote / 'kvtest'), '--version'], env=env, text=True)
                self.assertIn('sdk=2', output)
                loaded = subprocess.check_output(['ldd', str(remote / 'kvtest')], env=env, text=True)
                self.assertIn(str(sdk / 'libfixture.so'), loaded)
                self.assertNotIn(str(old_lib / 'libfixture.so'), loaded)
                outputs.append(loaded)
                return subprocess.CompletedProcess([], 0, '123\n', '')
            return subprocess.run(command, shell=True, check=kwargs.get('check', True),
                                  capture_output=True, text=True)

        for launcher in (False, True):
            with self.subTest(launcher=launcher):
                allocator_dir = remote / 'allocator_lib'
                allocator_dir.mkdir(exist_ok=True)
                (allocator_dir / 'libfixture.so').write_text('stale')
                if launcher:
                    (self.package / 'standalone_launcher.py').write_text('launcher')
                with patch('deploy_client.__file__', str(self.package / 'deploy_client.py')), \
                        patch('deploy_client.time.sleep'), \
                        patch.object(d, 'run_on', side_effect=execute), \
                        patch.object(d, 'scp_to', side_effect=lambda node, src, dst: shutil.copy2(src, dst)):
                    self.assertTrue(d.deploy_node(node)[0])
                self.assertEqual([path.name for path in allocator_dir.iterdir()], ['libjemalloc.so.2'])
                self.assertTrue((old_lib / 'libfixture.so').exists())
        self.assertEqual(len(outputs), 2)


if __name__ == '__main__':
    unittest.main()
