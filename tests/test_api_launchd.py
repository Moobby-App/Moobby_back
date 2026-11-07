"""API/tunnel setup tests. No subprocess, HTTP, database, or ngrok invocation."""
from io import StringIO
import json
from pathlib import Path
import plistlib
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
import launchd_api

REF = 'abcdefghijklmnopqrst'
URL = f'postgresql://mooby_app_api_login_dev:PRIVATE_DATABASE_PASSWORD@db.{REF}.supabase.co/postgres'
TOKEN = 'PRIVATE_BEARER_' + 'x'*32


class ApiLaunchdTests(unittest.TestCase):
    def test_api_reads_and_forwards_only_two_api_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            envfile = Path(tmp)/'.env'
            envfile.write_text(f'APP_API_DATABASE_URL={URL}\nAPP_API_BEARER_TOKEN={TOKEN}\n'
                'DATABASE_URL=PRIVATE_ADMIN\nWORKER_DATABASE_URL=PRIVATE_WORKER\n'
                'NGROK_AUTHTOKEN=PRIVATE_NGROK\nAPP_API_SYNTHETIC_FIXTURE_MODE=development\n')
            with patch.dict('os.environ', {'PGPASSWORD':'PRIVATE', 'NGROK_AUTHTOKEN':'PRIVATE',
                                         'APP_API_DATABASE_URL':'PRIVATE_ADMIN'}), \
                 patch('sys.stdout', new_callable=StringIO) as out, \
                 patch('sys.stderr', new_callable=StringIO) as err:
                env = launchd_api.api_environment({'expected_project_ref':REF}, envfile)
            self.assertEqual(out.getvalue()+err.getvalue(), '')
            self.assertEqual(env['APP_API_DATABASE_URL'], URL)
            self.assertEqual(env['APP_API_BEARER_TOKEN'], TOKEN)
            for key in ['DATABASE_URL','WORKER_DATABASE_URL','NGROK_AUTHTOKEN','PGPASSWORD',
                        'APP_API_SYNTHETIC_FIXTURE_MODE']:
                self.assertNotIn(key, env)
            for role in ['postgres','mooby_nlp_worker_login_dev']:
                envfile.write_text(f'APP_API_DATABASE_URL={URL.replace("mooby_app_api_login_dev",role)}\n'
                                   f'APP_API_BEARER_TOKEN={TOKEN}')
                with self.assertRaises(launchd_api.ConfigurationBlocked):
                    launchd_api.api_environment({'expected_project_ref':REF}, envfile)

    def test_api_command_binds_localhost_and_has_no_credentials(self):
        command = launchd_api.service_command('api', {})
        self.assertIn('mooby.api.app:app', command)
        self.assertEqual(command[command.index('--host')+1], '127.0.0.1')
        self.assertEqual(command[command.index('--port')+1], '8000')
        self.assertIn('--no-access-log', command)
        self.assertNotIn('PRIVATE', str(command))
        self.assertNotIn('--env-file', command)

    def test_existing_ngrok_auth_without_stable_domain_is_a_blocker(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp)/'ngrok.yml'
            cfg.write_text('version: "3"\nagent:\n  authtoken: PRIVATE_NGROK\n')
            with patch.object(launchd_api, 'NGROK', Path(sys.executable)):
                with self.assertRaises(launchd_api.ConfigurationBlocked) as caught:
                    launchd_api.tunnel_configuration({'ngrok_config_file':str(cfg),'stable_domain':None})
            self.assertEqual(caught.exception.reason, 'stable_domain_not_configured')

    def test_existing_stable_api_domain_is_reused_without_copying_auth_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp)/'ngrok.yml'
            cfg.write_text('version: "3"\nagent:\n  authtoken: PRIVATE_NGROK\n'
                'endpoints:\n  - name: api\n    url: https://reserved.example.test\n'
                '    upstream:\n      url: http://127.0.0.1:8000\n')
            settings = {'ngrok_config_file':str(cfg),'stable_domain':None}
            with patch.object(launchd_api, 'NGROK', Path(sys.executable)):
                command = launchd_api.service_command('tunnel', settings)
            self.assertEqual(command[command.index('--url')+1], 'https://reserved.example.test')
            self.assertIn('--inspect=false', command)
            self.assertIn('--log=false', command)
            self.assertNotIn('PRIVATE', str(command))
            self.assertNotIn('APP_API_BEARER_TOKEN', launchd_api.base_environment())
            self.assertNotIn('NGROK_AUTHTOKEN', launchd_api.base_environment())

    def test_unrelated_endpoint_and_invalid_domains_are_not_used(self):
        self.assertEqual(launchd_api.configured_domains({'endpoints':[
            {'url':'https://other.example.test', 'upstream':{'url':'http://127.0.0.1:3000'}}]}), set())
        for value in [None, 'https://user:PRIVATE@example.test', 'example.test/path',
                      'example.test:443', '*.example.test', 'REPLACE_WITH_DOMAIN']:
            with self.assertRaises(launchd_api.ConfigurationBlocked): launchd_api.domain_name(value)

    def test_children_output_is_discarded_and_only_lifecycle_records_are_logged(self):
        class Child:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def poll(self): return 1
            def wait(self, **kwargs): return 1
        logs = []
        with patch.object(launchd_api.subprocess, 'Popen', return_value=Child()) as popen, \
             patch.object(launchd_api.signal, 'signal'):
            result = launchd_api.supervise(['test-command'], {'APP_API_BEARER_TOKEN':TOKEN}, 'api', logs.append)
        self.assertEqual(result, 1)
        self.assertEqual(popen.call_args.kwargs['stdout'], launchd_api.subprocess.DEVNULL)
        self.assertEqual(popen.call_args.kwargs['stderr'], launchd_api.subprocess.DEVNULL)
        self.assertEqual(logs, [{'event':'service_started','service':'api'},
                                {'event':'service_exited','service':'api','exit_code':1}])
        self.assertNotIn('PRIVATE', str(logs))

    def test_configuration_blocker_never_starts_child_or_prints_secret(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(launchd_api, 'ROOT', Path(tmp)), \
             patch.object(launchd_api, 'read_settings', side_effect=ValueError('PRIVATE URL TOKEN')), \
             patch('sys.argv', ['launchd_api.py','--service','tunnel','--check-config']), \
             patch.object(launchd_api.subprocess, 'Popen') as popen, \
             patch('sys.stdout', new_callable=StringIO) as output:
            self.assertEqual(launchd_api.main(), 1)
            self.assertNotIn('PRIVATE', output.getvalue())
            self.assertEqual(json.loads(output.getvalue())['reason'], 'invalid_local_configuration')
        popen.assert_not_called()

    def test_plists_and_example_contain_no_credentials_and_are_not_live_settings(self):
        for label, service in [('dev.mooby.api','api'),('dev.mooby.ngrok','tunnel')]:
            data = plistlib.loads((ROOT/'ops/launchd'/f'{label}.plist').read_bytes())
            self.assertEqual(data['Label'], label)
            self.assertEqual(data['ProgramArguments'][-2:], ['--service',service])
            self.assertNotIn('EnvironmentVariables', data)
            self.assertEqual(data['StandardOutPath'], '/dev/null')
            self.assertEqual(data['StandardErrorPath'], '/dev/null')
        example = json.loads((ROOT/'ops/launchd/development-api.json.example').read_text())
        self.assertIsNone(example['stable_domain'])
        self.assertNotIn('authtoken', str(example))


if __name__ == '__main__':
    unittest.main()
