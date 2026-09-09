import json
import pathlib
import shlex
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
import deploy_common as common
import deploy_coordinator as coordinator
import deploy_worker as worker


class TestStandaloneProfile(unittest.TestCase):
    def setUp(self):
        self.pod = {'name': 'pod-0', 'ip': '192.0.2.1'}
        process_check = patch.object(common, 'check_process', return_value=(None, 'dead', None))
        process_check.start()
        self.addCleanup(process_check.stop)

    def start(self, binary='worker_test', conf='prof_final:true', config=None):
        return common.start_service_standalone(
            self.pod, 'default', binary, '/work', '/work/config.json', 'jf:9000', 'service',
            config=config, enable_procmon=False, port=31501, process_name=binary,
            jemalloc_prof_conf=conf)

    def test_both_roles_and_launch_paths_receive_profile_environment(self):
        for binary in ('worker_test', 'coordinator_test'):
            for launcher in (None, '/launcher.py'):
                with self.subTest(binary=binary, launcher=launcher), \
                        patch.object(common, 'kubectl_exec') as remote, \
                        patch.object(common, 'kubectl_cp_to'), \
                        patch.object(common, 'upload_launcher', return_value=launcher), \
                        patch.object(common, 'find_pid_by_port', return_value='123'), \
                        patch.object(common.time, 'sleep'), \
                        patch.object(common.subprocess, 'run') as run:
                    remote.return_value = subprocess.CompletedProcess(
                        [], 0, 'jemalloc_prof_supported=true\n', '')
                    run.return_value = subprocess.CompletedProcess([], 0, '123 0.1\n', '')
                    config = {'log_dir': {'value': "/logs with ' quote"}}
                    self.assertTrue(self.start(binary, config=config))
                    expected = (f"MALLOC_CONF=prof_final:true,prof:true,"
                                f"prof_prefix:/logs with ' quote/jemalloc/{binary}_31501")
                    launch = run.call_args[0][0]
                    if launcher:
                        self.assertIn(expected, launch)
                        self.assertIn('env', launch)
                    else:
                        self.assertIn(expected, shlex.split(launch[-1]))
                    probe = remote.call_args_list[0][0][2]
                    self.assertIn('MALLOC_CONF= ', probe)
                    self.assertIn('LD_LIBRARY_PATH=./lib:', probe)
                    self.assertIn(f'./{binary} --version', probe)
                    directory = remote.call_args_list[1][0][2]
                    self.assertIn(shlex.quote("/logs with ' quote/jemalloc"), directory)

    def test_bad_config_is_rejected_before_remote_commands(self):
        for conf in ('', 'prof:false', 'prof:true,prof:true', 'broken'):
            with self.subTest(conf=conf), patch.object(common, 'kubectl_exec') as remote:
                self.assertFalse(self.start(conf=conf, config={'log_dir': '/logs'}))
                remote.assert_not_called()

    def test_missing_log_dir_requires_explicit_prefix(self):
        with patch.object(common, 'kubectl_exec') as remote:
            self.assertFalse(self.start())
            remote.assert_not_called()
        with patch.object(common, 'prepare_standalone_jemalloc_prof') as prepare, \
                patch.object(common, 'upload_launcher', return_value='/launcher.py'), \
                patch.object(common, '_launch_via_launcher', return_value=('123', 0.1)):
            self.assertTrue(self.start(conf='prof_prefix:relative/custom'))
            self.assertEqual(prepare.call_args[0][4], 'relative')

    def test_unsupported_or_unwritable_prevents_launch(self):
        for failure in ('unsupported', 'unwritable', 'load_failure'):
            with self.subTest(failure=failure), \
                    patch.object(common, 'kubectl_exec') as remote, \
                    patch.object(common, 'upload_launcher') as upload:
                if failure == 'unwritable':
                    remote.side_effect = [
                        subprocess.CompletedProcess([], 0, 'jemalloc_prof_supported=true\n', ''),
                        subprocess.CalledProcessError(1, 'mkdir', stderr='Permission denied'),
                    ]
                else:
                    remote.return_value = subprocess.CompletedProcess(
                        [], 1 if failure == 'load_failure' else 0,
                        'jemalloc_prof_supported=false\n', '')
                self.assertFalse(self.start(config={'log_dir': '/logs'}))
                upload.assert_not_called()

    def test_running_service_rejects_profile_configuration(self):
        for binary in ('worker_test', 'coordinator_test'):
            with self.subTest(binary=binary), \
                    patch.object(common, 'check_process', return_value=('123', 'alive', None)), \
                    patch.object(common, 'prepare_standalone_jemalloc_prof') as prepare, \
                    patch.object(common, 'upload_launcher') as launch:
                self.assertFalse(self.start(binary, config={'log_dir': '/logs'}))
                prepare.assert_not_called()
                launch.assert_not_called()

    def test_failed_preflight_is_aggregated_with_healthy_pod(self):
        for binary in ('worker_test', 'coordinator_test'):
            for failure in ('unsupported', 'unwritable', 'timeout'):
                attempted = []

                def remote(name, namespace, command, **kwargs):
                    if '--version' in command:
                        attempted.append(name)
                        if name == 'bad' and failure == 'timeout':
                            raise subprocess.TimeoutExpired(command, 10)
                        supported = name != 'bad' or failure != 'unsupported'
                        return subprocess.CompletedProcess(
                            [], 0, f'jemalloc_prof_supported={str(supported).lower()}\n', '')
                    if name == 'bad' and failure == 'unwritable':
                        raise subprocess.CalledProcessError(1, command, stderr='Permission denied')
                    return subprocess.CompletedProcess([], 0, '', '')

                def start(pod):
                    return common.start_service_standalone(
                        pod, 'default', binary, '/work', '/work/config', 'jf:9000', 'service',
                        config={'log_dir': '/logs'}, enable_procmon=False, port=31501,
                        jemalloc_prof_conf='prof:true')

                with self.subTest(binary=binary, failure=failure), \
                        patch.object(common, 'kubectl_exec', side_effect=remote), \
                        patch.object(common, 'kubectl_cp_to'), \
                        patch.object(common, 'upload_launcher', return_value='/launcher.py'), \
                        patch.object(common, '_launch_via_launcher', return_value=('123', 0.1)), \
                        patch.object(common, 'log_info') as log:
                    pods = [{'name': name, 'ip': '192.0.2.1'} for name in ('bad', 'good')]
                    self.assertEqual(common.do_for_all_pods(pods, start, 'Starting'), 1)
                    self.assertCountEqual(attempted, ['bad', 'good'])
                    self.assertTrue(any('1/2 succeeded' in str(call) for call in log.call_args_list))

    def test_no_option_does_not_probe_or_set_environment(self):
        with patch.object(common, 'kubectl_exec') as remote, \
                patch.object(common, 'upload_launcher', return_value='/launcher.py'), \
                patch.object(common, '_launch_via_launcher', return_value=('123', 0.1)) as launch:
            self.assertTrue(self.start(conf=None))
            remote.assert_not_called()
            self.assertEqual(launch.call_args[1]['env'], {})

    def test_directory_is_resolved_on_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            binary = root / 'worker_test'
            binary.write_text('#!/bin/sh\necho jemalloc_prof_supported=true\n')
            binary.chmod(0o755)

            def execute(pod, namespace, command, **kwargs):
                return subprocess.run(command, shell=True, text=True, capture_output=True,
                                      check=kwargs.get('check', True))

            with patch.object(common, 'kubectl_exec', side_effect=execute):
                common.prepare_standalone_jemalloc_prof(
                    self.pod, 'default', 'worker_test', str(root), "logs with ' quote/jemalloc", 10)
            self.assertTrue((root / "logs with ' quote/jemalloc").is_dir())


class TestServiceProfileCli(unittest.TestCase):
    def test_cli_options_and_worker_alias_are_forwarded(self):
        for module in (worker, coordinator):
            options = ['--jemalloc_prof_conf']
            if module is worker:
                options.append('--jemalloc-prof-options')
            for action in ('start', 'deploy'):
                for option in options:
                    with self.subTest(module=module.__name__, action=action, option=option):
                        argv = [module.__file__, action, '-p', 'test', '-c', 'service.config',
                                '-S', '--jf', 'jf:9000',
                                option, 'prof_final:true']
                        with patch.object(sys, 'argv', argv), \
                                patch.object(module, 'get_pods', return_value=[
                                    {'name': 'pod-0', 'ip': '192.0.2.1'}]), \
                                patch.object(module, 'cmd_' + action, return_value=0) as cmd:
                            self.assertEqual(module.main(), 0)
                            args = cmd.call_args[0][0]
                            attr = 'jemalloc_prof_options' if module is worker else 'jemalloc_prof_conf'
                            self.assertEqual(getattr(args, attr), 'prof_final:true')

    def test_invalid_cli_conf_fails_before_pod_discovery(self):
        for module in (worker, coordinator):
            for action in ('start', 'deploy'):
                argv = [module.__file__, action, '-p', 'test', '-c', 'service.config',
                        '--jemalloc_prof_conf', 'prof_active:false']
                with self.subTest(module=module.__name__, action=action), \
                        patch.object(sys, 'argv', argv), \
                        patch.object(module, 'get_pods') as discover:
                    with self.assertRaises(SystemExit) as error:
                        module.main()
                    self.assertEqual(error.exception.code, 2)
                    discover.assert_not_called()

    def test_role_start_forwards_config_and_profile_conf(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = pathlib.Path(tmp) / 'config.json'
            config.write_text(json.dumps({'log_dir': {'value': '/logs'}}))
            args = SimpleNamespace(
                config=str(config), jf='jf:9000', service='service', set=['log_dir=/newlogs'],
                remote_dir='/work', remote_config='/work/config.json', namespace='default',
                port=31501, enable_procmon=False, procmon_dir=None, timeout=10,
                ttl=30, expected_member_count=1, jemalloc_prof_conf='prof_final:true',
                jemalloc_prof_options='prof_final:true')
            for module in (worker, coordinator):
                with self.subTest(module=module.__name__), \
                        patch.object(coordinator, 'check_process', return_value=(None, 'dead', None)), \
                        patch.object(module, 'start_service_standalone', return_value=True) as start:
                    self.assertEqual(module.cmd_start_standalone(
                        args, [{'name': 'pod-0', 'ip': '192.0.2.1'}]), 0)
                    self.assertEqual(start.call_args[1]['jemalloc_prof_conf'], 'prof_final:true')
                    self.assertEqual(start.call_args[1]['config']['log_dir']['value'], '/newlogs')

    def test_coordinator_dscli_rejected_before_install(self):
        args = SimpleNamespace(
            prefixes=['test'], image=None, standalone=False, jemalloc_prof_conf='prof:true')
        with patch.object(coordinator, 'cmd_install_impl') as install:
            self.assertEqual(coordinator.cmd_start(args, []), 1)
            self.assertEqual(coordinator.cmd_deploy(args, []), 1)
            install.assert_not_called()

    def test_running_coordinator_is_not_skipped_with_profile_option(self):
        args = SimpleNamespace(jf='jf:9000', config='unused', set=[], remote_dir='/work',
                               remote_config='/work/config', ttl=30, expected_member_count=1,
                               port=31511, namespace='default', timeout=10,
                               jemalloc_prof_conf='prof:true')
        with patch('builtins.open', unittest.mock.mock_open(read_data='{}')), \
                patch.object(coordinator, 'check_process', return_value=('123', 'alive', None)), \
                patch.object(coordinator, 'start_service_standalone') as start:
            self.assertEqual(coordinator.cmd_start_standalone(
                args, [{'name': 'pod-0', 'ip': '192.0.2.1'}]), 1)
            start.assert_not_called()


if __name__ == '__main__':
    unittest.main()
