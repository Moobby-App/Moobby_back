"""API/tunnel setup tests. No subprocess, HTTP, database, or ngrok invocation."""
from io import StringIO
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import plistlib
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
import launchd_api
from mooby.api.config import ApiConfigurationError, ApiSettings
from mooby.api import repository

REF = 'abcdefghijklmnopqrst'
URL = f'postgresql://mooby_app_api_login_dev:PRIVATE_DATABASE_PASSWORD@db.{REF}.supabase.co/postgres'
TOKEN = 'PRIVATE_BEARER_' + 'x'*32


class ApiLaunchdTests(unittest.TestCase):
    def timeout_environment(self, entries=(), settings=None):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'.env'
            path.write_text(f'APP_API_DATABASE_URL={URL}\nAPP_API_BEARER_TOKEN={TOKEN}\n'
                            + '\n'.join(entries) + '\n')
            return launchd_api.api_environment({'expected_project_ref':REF, **(settings or {})}, path)

    def parse_child(self, env):
        with patch.dict('os.environ', env, clear=True):
            return ApiSettings.from_env()

    def test_custom_connection_timeout_is_forwarded_and_consumed(self):
        key = 'APP_API_CONNECT_TIMEOUT_SECONDS'
        env = self.timeout_environment([f'{key}=8'])
        self.assertEqual(env[key], '8')
        self.assertNotIn('APP_API_STATEMENT_TIMEOUT_MS', env)
        settings = self.parse_child(env)
        self.assertEqual((settings.connect_timeout_seconds, settings.statement_timeout_ms), (8,3000))

    def test_custom_statement_timeout_is_forwarded_and_consumed(self):
        key = 'APP_API_STATEMENT_TIMEOUT_MS'
        env = self.timeout_environment([f'{key}=4500'])
        self.assertEqual(env[key], '4500')
        self.assertNotIn('APP_API_CONNECT_TIMEOUT_SECONDS', env)
        settings = self.parse_child(env)
        self.assertEqual((settings.connect_timeout_seconds, settings.statement_timeout_ms), (3,4500))

    def test_missing_timeouts_keep_defaults_and_do_not_inherit_parent_values(self):
        with patch.dict('os.environ', {'APP_API_CONNECT_TIMEOUT_SECONDS':'15',
                                     'APP_API_STATEMENT_TIMEOUT_MS':'10000', 'PGOPTIONS':'PRIVATE',
                                     'DATABASE_URL':'PRIVATE', 'WORKER_DATABASE_URL':'PRIVATE'}):
            before = dict(os.environ)
            env = self.timeout_environment()
            self.assertEqual(dict(os.environ), before)
        for key in (*launchd_api.API_TIMEOUT_KEYS, 'PGOPTIONS','DATABASE_URL','WORKER_DATABASE_URL'):
            self.assertNotIn(key, env)
        settings = self.parse_child(env)
        self.assertEqual((settings.connect_timeout_seconds, settings.statement_timeout_ms), (3,3000))

    def test_minimum_and_maximum_timeout_pairs_preserve_credentials(self):
        for connection, statement in [(1,100),(15,10000)]:
            with self.subTest(connection=connection,statement=statement):
                env = self.timeout_environment([f'APP_API_CONNECT_TIMEOUT_SECONDS={connection}',
                                               f'APP_API_STATEMENT_TIMEOUT_MS={statement}'])
                self.assertEqual(env['APP_API_DATABASE_URL'], URL)
                self.assertEqual(env['APP_API_BEARER_TOKEN'], TOKEN)
                settings = self.parse_child(env)
                self.assertEqual((settings.connect_timeout_seconds, settings.statement_timeout_ms),
                                 (connection,statement))

    def test_quoted_whitespace_is_forwarded_verbatim_with_existing_integer_parsing(self):
        env = self.timeout_environment(['APP_API_CONNECT_TIMEOUT_SECONDS="  +5  "',
                                       'APP_API_STATEMENT_TIMEOUT_MS=" 04000 "'])
        self.assertEqual(env['APP_API_CONNECT_TIMEOUT_SECONDS'], '  +5  ')
        self.assertEqual(env['APP_API_STATEMENT_TIMEOUT_MS'], ' 04000 ')
        settings = self.parse_child(env)
        self.assertEqual((settings.connect_timeout_seconds,settings.statement_timeout_ms), (5,4000))

    def test_invalid_numeric_formats_blank_and_bare_values_are_blocked(self):
        for key in launchd_api.API_TIMEOUT_KEYS:
            for suffix in ('=bad','=3.0','=1e3','=NaN','=Infinity','=true','=',
                           '="   "','="${PARENT_TIMEOUT}"','="$(touch NEVER_EXECUTE)"',''):
                with self.subTest(key=key,suffix=suffix), \
                     patch('sys.stdout',new_callable=StringIO) as out, \
                     patch('sys.stderr',new_callable=StringIO) as err:
                    with self.assertRaises(launchd_api.ConfigurationBlocked) as caught:
                        self.timeout_environment([key+suffix])
                    self.assertEqual(caught.exception.reason,'invalid_api_timeout_configuration')
                    self.assertEqual(out.getvalue()+err.getvalue(),'')
                    self.assertNotIn('PRIVATE',str(caught.exception))

    def test_out_of_range_values_are_blocked(self):
        for key, values in [('APP_API_CONNECT_TIMEOUT_SECONDS',(-1,0,16)),
                            ('APP_API_STATEMENT_TIMEOUT_MS',(-1,0,99,10001))]:
            for value in values:
                with self.subTest(key=key,value=value):
                    with self.assertRaises(launchd_api.ConfigurationBlocked) as caught:
                        self.timeout_environment([f'{key}={value}'])
                    self.assertEqual(caught.exception.reason,'invalid_api_timeout_configuration')

    def test_local_timeouts_override_parent_without_forwarding_unrelated_values(self):
        with patch.dict('os.environ', {'APP_API_CONNECT_TIMEOUT_SECONDS':'bad',
                                     'APP_API_STATEMENT_TIMEOUT_MS':'bad',
                                     'APP_API_UNKNOWN_TIMEOUT':'PRIVATE','PGPASSWORD':'PRIVATE'}):
            env = self.timeout_environment(['APP_API_CONNECT_TIMEOUT_SECONDS=7',
                                           'APP_API_STATEMENT_TIMEOUT_MS=2500',
                                           'APP_API_UNKNOWN_TIMEOUT=PRIVATE','PGPASSWORD=PRIVATE'])
        self.assertEqual(env['APP_API_CONNECT_TIMEOUT_SECONDS'],'7')
        self.assertEqual(env['APP_API_STATEMENT_TIMEOUT_MS'],'2500')
        self.assertNotIn('APP_API_UNKNOWN_TIMEOUT',env)
        self.assertNotIn('PGPASSWORD',env)

    def test_timeout_and_diagnostic_opt_ins_remain_independent(self):
        env = self.timeout_environment(['APP_API_CONNECT_TIMEOUT_SECONDS=6'],
                                      {'diagnostics':{'enabled':True,'capacity':32}})
        self.assertEqual(env['MOOBY_DIAGNOSTIC_ENABLED'],'1')
        self.assertEqual(env['MOOBY_DIAGNOSTIC_CAPACITY'],'32')
        self.assertEqual(self.parse_child(env).connect_timeout_seconds,6)
        env = self.timeout_environment(['APP_API_STATEMENT_TIMEOUT_MS=1200'])
        self.assertNotIn('MOOBY_DIAGNOSTIC_ENABLED',env)
        self.assertEqual(self.parse_child(env).statement_timeout_ms,1200)

    def test_shared_mapping_validator_and_environment_parser_are_equivalent(self):
        env = {'APP_API_DATABASE_URL':URL,'APP_API_BEARER_TOKEN':TOKEN,
               'APP_API_CONNECT_TIMEOUT_SECONDS':'9','APP_API_STATEMENT_TIMEOUT_MS':'1700'}
        self.assertEqual(ApiSettings.from_mapping(env), self.parse_child(env))
        for key in launchd_api.API_TIMEOUT_KEYS:
            invalid = {**env,key:'3.5'}
            with self.assertRaises(ApiConfigurationError): ApiSettings.from_mapping(invalid)
            with self.assertRaises(ApiConfigurationError): self.parse_child(invalid)

    def test_all_repository_connections_consume_forwarded_timeouts_without_extra_queries(self):
        settings = self.parse_child(self.timeout_environment([
            'APP_API_CONNECT_TIMEOUT_SECONDS=8','APP_API_STATEMENT_TIMEOUT_MS=4500']))
        for operation, count in [(repository.fetch_disruptions,2),
                                 (repository.check_database,1),(repository.check_app_api_projection,1)]:
            conn = MagicMock()
            conn.__enter__.return_value = conn
            conn.execute.return_value.fetchone.return_value = {'generated_at':datetime.now(timezone.utc)}
            conn.execute.return_value.fetchall.return_value = []
            kwargs = dict(database_url=settings.database_url,
                          connect_timeout_seconds=settings.connect_timeout_seconds,
                          statement_timeout_ms=settings.statement_timeout_ms)
            if operation is repository.fetch_disruptions:
                kwargs.update(station_ids=['open-krl:SRP'],limit=20,offset=0)
            with patch.object(repository.psycopg,'connect',return_value=conn) as connect:
                operation(**kwargs)
            self.assertEqual(connect.call_args.args,(URL,))
            self.assertEqual(connect.call_args.kwargs['connect_timeout'],8)
            self.assertEqual(connect.call_args.kwargs['options'],'-c statement_timeout=4500')
            self.assertEqual(conn.execute.call_count,count)

    def test_invalid_timeout_check_config_reports_only_safe_reason_and_starts_no_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'.env').write_text(f'APP_API_DATABASE_URL={URL}\nAPP_API_BEARER_TOKEN={TOKEN}\n'
                                    'APP_API_CONNECT_TIMEOUT_SECONDS=PRIVATE_INVALID\n')
            with patch.object(launchd_api,'ROOT',root), \
                 patch.object(launchd_api,'read_settings',return_value={'expected_project_ref':REF}), \
                 patch('sys.argv',['launchd_api.py','--service','api','--check-config']), \
                 patch.object(launchd_api.subprocess,'Popen') as popen, \
                 patch('sys.stdout',new_callable=StringIO) as out:
                # Default env_file is bound at definition time; explicitly supply
                # this temporary fixture while exercising the real validator.
                environment = launchd_api.api_environment
                with patch.object(launchd_api,'api_environment',side_effect=lambda settings:
                                  environment(settings,root/'.env')):
                    self.assertEqual(launchd_api.main(),1)
            popen.assert_not_called()
            self.assertEqual(json.loads(out.getvalue())['reason'],'invalid_api_timeout_configuration')
            self.assertNotIn('PRIVATE',out.getvalue())
            for path in (root/'.local/services').glob('*.log'):
                self.assertNotIn('PRIVATE',path.read_text())

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

    def test_mock_opt_in_is_explicit_local_json_with_bounded_ttl(self):
        with tempfile.TemporaryDirectory() as tmp:
            envfile = Path(tmp)/'.env'
            envfile.write_text(f'APP_API_DATABASE_URL={URL}\nAPP_API_BEARER_TOKEN={TOKEN}\n')
            envfile.write_text(envfile.read_text()+'MOOBY_RELEVANCE_THRESHOLD=0.8\n'
                'MOOBY_SEVERITY_THRESHOLD=0.7\nMOOBY_TRUSTED_PUBLISHED_AT_BASES=page_timestamp\n'
                'WORKER_DATABASE_URL=PRIVATE_WORKER\n')
            settings = {'expected_project_ref':REF, 'mock_enabled':True, 'mock_ttl_minutes':60}
            env = launchd_api.api_environment(settings, envfile)
            self.assertEqual(env['MOOBY_RELEVANCE_THRESHOLD'], '0.8')
            self.assertEqual(env['MOOBY_SEVERITY_THRESHOLD'], '0.7')
            self.assertEqual(env['MOOBY_TRUSTED_PUBLISHED_AT_BASES'], 'page_timestamp')
            self.assertNotIn('WORKER_DATABASE_URL', env)
            disabled = launchd_api.api_environment({'expected_project_ref':REF}, envfile)
            self.assertNotIn('MOOBY_RELEVANCE_THRESHOLD', disabled)
            self.assertEqual(env['APP_API_ENVIRONMENT'], 'development')
            self.assertEqual(env['APP_API_SYNTHETIC_FIXTURE_MODE'], 'development')
            self.assertEqual(env['APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES'], '60')
            for extra in [{'mock_enabled':'true'}, {'mock_ttl_minutes':0}, {'mock_ttl_minutes':241}]:
                with self.assertRaises(launchd_api.ConfigurationBlocked):
                    launchd_api.api_environment({**settings, **extra}, envfile)

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
