"""Local-only polling/supervision checks: fake connections, no worker is started."""
from contextlib import contextmanager
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
import launchd_worker
import run_worker
from worker_runtime import AlreadyRunning, WorkerConfigurationError, single_instance, validate_development_url

REF = 'abcdefghijklmnopqrst'
URL = f'postgresql://mooby_nlp_worker_login_dev:TEST_ONLY@db.{REF}.supabase.co/postgres'


class LaunchdTests(unittest.TestCase):
    def test_exact_project_and_worker_principal_are_required(self):
        validate_development_url(URL, REF)
        validate_development_url(
            f'postgresql://mooby_nlp_worker_login_dev.{REF}:TEST_ONLY@aws-0-example.pooler.supabase.com/postgres', REF)
        for wrong in [URL.replace(REF, 'z'*20), URL.replace('mooby_nlp_worker_login_dev', 'postgres'),
                      URL.replace(f'db.{REF}.supabase.co', 'untrusted.example'),
                      URL+'?host=other', URL+'?user=postgres', URL+'?options=role%3Dpostgres',
                      URL+'?hostaddr=127.0.0.1', URL+'?service=admin', URL+'?sslmode=require&sslmode=disable',
                      f'postgresql://postgres.{REF}@aws-0-example.pooler.supabase.com/postgres']:
            with self.subTest(url_type='redacted'), self.assertRaises(WorkerConfigurationError):
                validate_development_url(wrong, REF)

    def test_dotenv_is_data_only_and_never_forwards_generic_admin_or_pg_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp)/'config.json'
            config.write_text(json.dumps(dict(expected_project_ref=REF, pipeline_version='dev-v1', worker_id='mac-worker')))
            envfile = Path(tmp)/'.env'
            envfile.write_text(f'WORKER_DATABASE_URL={URL}\nDATABASE_URL=PRIVATE_ADMIN\n'
                'MOOBY_RELEVANCE_THRESHOLD=0.8\nMOOBY_SEVERITY_THRESHOLD=0.8\n'
                'MOOBY_TRUSTED_PUBLISHED_AT_BASES=page_timestamp\n'
                'ADMIN_DATABASE_URL=PRIVATE_ADMIN\nPGPASSWORD=PRIVATE_PASSWORD\n')
            with patch.dict('os.environ', {'DATABASE_URL':'PRIVATE', 'PGHOSTADDR':'untrusted',
                                         'PGSERVICE':'admin', 'PYTHONPATH':'untrusted'}), \
                 patch('sys.stdout', new_callable=StringIO) as out, \
                 patch('sys.stderr', new_callable=StringIO) as err:
                actual, env = launchd_worker.load_configuration(config, envfile)
            self.assertEqual(out.getvalue()+err.getvalue(), '')
            self.assertEqual(env['WORKER_DATABASE_URL'], URL)
            for key in ['DATABASE_URL','ADMIN_DATABASE_URL','PGPASSWORD','PGHOSTADDR','PGSERVICE','PYTHONPATH']:
                self.assertNotIn(key, env)
            self.assertEqual(env['HF_HUB_OFFLINE'], '1')
            envfile.write_text(envfile.read_text().replace('MOOBY_RELEVANCE_THRESHOLD=0.8',
                                                          'MOOBY_RELEVANCE_THRESHOLD=invalid'))
            with self.assertRaises(ValueError): launchd_worker.load_configuration(config, envfile)
            envfile.write_text('DATABASE_URL='+URL)
            with self.assertRaises(ValueError): launchd_worker.load_configuration(config, envfile)

    def test_lock_prevents_duplicate_and_releases_without_deleting_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'instance.lock'
            with single_instance(path):
                with self.assertRaises(AlreadyRunning):
                    with single_instance(path): self.fail('duplicate lock accepted')
            self.assertTrue(path.exists())
            with single_instance(path): pass

    def test_supervisor_logs_only_allowlisted_counters(self):
        for line in ['PRIVATE SQL password raw text', 'postgresql://PRIVATE',
                     json.dumps({'event':'unrecognized', 'message':'PRIVATE'})]:
            self.assertIsNone(launchd_worker.filtered_record(line))
        record = launchd_worker.filtered_record(json.dumps({
            'event':'poll', 'processed_count':1, 'raw_text':'PRIVATE', 'quote':'PRIVATE',
            'author':'PRIVATE', 'run_id':'PRIVATE', 'password':'PRIVATE', 'sql':'PRIVATE'}))
        self.assertEqual(record, {'event':'poll', 'processed_count':1})
        self.assertNotIn('PRIVATE', str(record))

    def test_supervisor_reuses_cli_and_forwards_only_sanitized_records(self):
        class Child:
            stdout = StringIO('PRIVATE RAW ERROR\n'+json.dumps({'event':'poll','processed_count':1,'author':'PRIVATE'})+'\n')
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def wait(self, **kwargs): return 1
            def poll(self): return 1
        logs = []
        config = dict(expected_project_ref=REF, pipeline_version='stable-dev-v1', worker_id='stable-mac')
        with patch.object(launchd_worker.subprocess, 'Popen', return_value=Child()) as popen, \
             patch.object(launchd_worker.signal, 'signal'):
            code = launchd_worker.supervise(config, {}, logs.append)
        self.assertEqual(code, 1)
        command = popen.call_args.args[0]
        self.assertIn(str(ROOT/'scripts/run_worker.py'), command)
        self.assertNotIn('--once', command)
        self.assertEqual(command[command.index('--max-items')+1], '1')
        self.assertNotIn('PRIVATE', str(logs))
        self.assertEqual(logs[0], {'event':'poll','processed_count':1})
        self.assertEqual(popen.call_args.kwargs['stderr'], launchd_worker.subprocess.DEVNULL)

    def test_stop_signal_is_forwarded_without_spawning_another_processing_path(self):
        handlers = {}
        class Pipe:
            called = False
            def readline(self, limit):
                if not self.called:
                    self.called = True
                    handlers[launchd_worker.signal.SIGTERM](launchd_worker.signal.SIGTERM, None)
                return ''
        class Child:
            pid = 12345  # Test double only; killpg is mocked below.
            stdout = Pipe()
            stopped = False
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def poll(self): return -15 if self.stopped else None
            def wait(self, **kwargs):
                self.stopped = True
                return -15
        def install(signum, handler):
            handlers[signum] = handler
            return None
        child = Child()
        with patch.object(launchd_worker.subprocess, 'Popen', return_value=child),              patch.object(launchd_worker.signal, 'signal', side_effect=install),              patch.object(launchd_worker.os, 'killpg') as kill:
            self.assertEqual(launchd_worker.supervise(
                dict(expected_project_ref=REF, pipeline_version='stable-dev-v1', worker_id='stable-mac'),
                {}, lambda record: None), 0)
        kill.assert_called_once_with(12345, launchd_worker.signal.SIGTERM)

    def test_continuous_cli_discovers_and_processes_multiple_polls_through_existing_helpers(self):
        @contextmanager
        def connection(): yield object()
        args = SimpleNamespace(once=False, pipeline_version='stable-dev-v1', worker_id='stable-mac',
            lookback_seconds=3600, discovery_batch_size=5, snapshot_id=None, max_items=1, poll_seconds=5)
        with patch.object(run_worker.psycopg, 'connect', return_value=connection()) as connect, \
             patch.object(run_worker, 'enqueue_recent_snapshots', side_effect=[['test-id'], []]) as enqueue, \
             patch.object(run_worker, 'run_available', return_value=[]) as process, \
             patch.object(run_worker.time, 'sleep', side_effect=[None, KeyboardInterrupt]), \
             patch('sys.stdout', new_callable=StringIO):
            with self.assertRaises(KeyboardInterrupt): run_worker._poll(args, URL)
        self.assertEqual(enqueue.call_count, 2)
        self.assertEqual(process.call_count, 2)
        self.assertEqual(enqueue.call_args.kwargs['pipeline_version'], 'stable-dev-v1')
        self.assertEqual(process.call_args.kwargs['max_items'], 1)
        connect.assert_called_once()

    def test_cli_error_boundary_never_logs_exception_message(self):
        from psycopg.errors import InsufficientPrivilege
        with patch.object(run_worker, 'main', side_effect=InsufficientPrivilege('PRIVATE SQL QUOTE URL')), \
             patch('sys.stdout', new_callable=StringIO) as out:
            self.assertEqual(run_worker.entrypoint(), 1)
        self.assertEqual(json.loads(out.getvalue()), {'event':'worker_error','sqlstate':'42501'})
        self.assertNotIn('PRIVATE', out.getvalue())

    def test_duplicate_cli_instance_never_connects(self):
        args = SimpleNamespace(expected_project_ref=REF)
        @contextmanager
        def locked(*args):
            raise AlreadyRunning()
            yield
        with patch.object(run_worker, '_arguments', return_value=args), \
             patch.dict('os.environ', {'WORKER_DATABASE_URL':URL}), \
             patch.object(run_worker, 'single_instance', locked), \
             patch.object(run_worker.psycopg, 'connect') as connect, \
             patch('sys.stdout', new_callable=StringIO) as out:
            self.assertEqual(run_worker.entrypoint(), 0)
        connect.assert_not_called()
        self.assertEqual(json.loads(out.getvalue()), {'event':'duplicate_instance'})

    def test_plist_is_opt_in_and_uses_repo_interpreter_without_credentials(self):
        plist = plistlib.loads((ROOT/'ops/launchd/dev.mooby.worker.plist').read_bytes())
        self.assertEqual(plist['Label'], 'dev.mooby.worker')
        self.assertEqual(plist['ProgramArguments'][0], str(ROOT/'.venv/bin/python'))
        self.assertEqual(plist['WorkingDirectory'], str(ROOT))
        self.assertEqual(plist['KeepAlive'], {'SuccessfulExit':False})
        self.assertNotIn('EnvironmentVariables', plist)
        self.assertEqual(plist['StandardErrorPath'], '/dev/null')
        self.assertEqual(plist['StandardOutPath'], '/dev/null')
        self.assertEqual(plist['ThrottleInterval'], 30)


if __name__ == '__main__':
    unittest.main()
