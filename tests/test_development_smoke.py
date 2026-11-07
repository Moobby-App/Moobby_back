"""Local rollback/target guard tests only: no DB connections or model loading."""
import copy
from contextlib import contextmanager
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from smoke_one_item_development import (
    SmokeFailure, rollback_only, validate_target, validate_session_identity, main,
    StagedConnection, database_error_report, row_counts)
from psycopg.pq import TransactionStatus


class TransactionConnection:
    """Nested savepoints can succeed, but force_rollback restores the outer state."""
    def __init__(self):
        self.autocommit = True
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.rows = {k: ['existing-user-row'] for k in
                     ('queue', 'runs', 'events', 'versions', 'sources', 'evidence')}
        self.transactions = []
        self.statements = []
        self.depth = 0

    @contextmanager
    def transaction(self, force_rollback=False):
        self.transactions.append(force_rollback)
        before = copy.deepcopy(self.rows)
        self.depth += 1
        self.info.transaction_status = TransactionStatus.INTRANS
        try:
            yield
        except BaseException:
            self.rows = before
            raise
        else:
            if force_rollback:
                self.rows = before
        finally:
            self.depth -= 1
            self.info.transaction_status = (TransactionStatus.INTRANS if self.depth else TransactionStatus.IDLE)

    def execute(self, statement):
        self.statements.append(statement)


class DevelopmentSmokeTests(unittest.TestCase):
    def setUp(self):
        self.conn = TransactionConnection()
        self.before = copy.deepcopy(self.conn.rows)

    def absence(self):
        self.assertEqual(self.conn.statements[-1], 'SET TRANSACTION READ ONLY')
        self.assertEqual(self.conn.rows, self.before)
        return {k: sum(row == 'smoke' for row in rows) for k, rows in self.conn.rows.items()}

    def write_all(self):
        # Models finish_attempt's nested savepoint succeeding, not an outer commit.
        with self.conn.transaction():
            for rows in self.conn.rows.values():
                rows.append('smoke')
        return {'verified': True, 'published': True, 'embedding_dimensions': 384}

    def test_success_still_rolls_back_queue_run_event_vector_source_evidence(self):
        report = rollback_only(self.conn, self.write_all, self.absence)
        self.assertEqual(report['status'], 'passed')
        self.assertTrue(report['rollback_verified'])
        self.assertTrue(all(n == 0 for n in report['remaining_rows'].values()))
        self.assertEqual(self.conn.rows, self.before)
        self.assertEqual(self.conn.transactions, [True, False, True])

    def test_partial_failure_rolls_back_and_performs_absence_check(self):
        def fail():
            self.write_all()
            raise RuntimeError('private DB detail must never be printed')
        with self.assertRaises(SmokeFailure) as caught:
            rollback_only(self.conn, fail, self.absence)
        self.assertTrue(caught.exception.report['rollback_verified'])
        self.assertIsNone(caught.exception.report['sqlstate'])
        self.assertNotIn('error_type', caught.exception.report)
        self.assertNotIn('private', str(caught.exception.report))
        self.assertEqual(self.conn.rows, self.before)

    def test_failed_worker_assertion_still_rolls_back(self):
        def fail_worker_check():
            self.write_all()
            raise SmokeFailure({'status': 'failed', 'stage': 'worker_persistence_insert_not_confirmed'})
        with self.assertRaises(SmokeFailure) as caught:
            rollback_only(self.conn, fail_worker_check, self.absence)
        self.assertEqual(caught.exception.report['stage'], 'worker_persistence_insert_not_confirmed')
        self.assertTrue(caught.exception.report['rollback_verified'])
        self.assertEqual(self.conn.rows, self.before)

    def test_interrupt_also_rolls_back(self):
        def interrupt():
            self.write_all()
            raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            rollback_only(self.conn, interrupt, self.absence)
        self.assertEqual(self.conn.rows, self.before)
        self.assertEqual(self.conn.info.transaction_status, TransactionStatus.IDLE)

    def test_failed_post_rollback_verification_never_reports_success(self):
        with self.assertRaises(SmokeFailure) as caught:
            rollback_only(self.conn, self.write_all, lambda: {'events': 1})
        self.assertFalse(caught.exception.report['rollback_verified'])
        self.assertEqual(caught.exception.report['stage'], 'rollback_residue_detected')

    def test_requires_a_fresh_outer_transaction(self):
        for attr, value in [('autocommit', False)]:
            setattr(self.conn, attr, value)
            with self.assertRaises(SmokeFailure):
                rollback_only(self.conn, self.write_all, self.absence)
        self.assertEqual(self.conn.rows, self.before)
        self.assertEqual(self.conn.transactions, [])

    def test_explicit_development_identity_and_no_indirect_dsn(self):
        target = dict(host='reviewed-dev.example', database='dev', user='smoke_dev')
        valid = 'host=reviewed-dev.example dbname=dev user=smoke_dev'
        validate_target(valid, **target)
        for invalid in ['host=other.example dbname=dev user=smoke_dev',
                        'host=reviewed-dev.example dbname=prod user=smoke_dev',
                        valid+' hostaddr=127.0.0.1', valid+' service=other', '']:
            with self.subTest(dsn=invalid), self.assertRaises(SmokeFailure):
                validate_target(invalid, **target)

    def test_pooler_connection_username_may_differ_from_both_database_roles(self):
        validate_target('host=dev.example dbname=dev user=smoke_dev.project',
                        host='dev.example', database='dev', user='smoke_dev.project')
        conn = SimpleNamespace(execute=lambda sql: SimpleNamespace(fetchone=lambda: {
            'database': 'dev', 'current_role': 'smoke_dev', 'session_role': 'smoke_dev'}))
        validate_session_identity(conn, database='dev', session_user='smoke_dev')
        with self.assertRaises(SmokeFailure):
            validate_target('host=dev.example dbname=dev user=wrong.project',
                            host='dev.example', database='dev', user='smoke_dev.project')

    def test_database_current_user_and_session_user_mismatches_fail_closed(self):
        correct = {'database': 'dev', 'current_role': 'smoke_dev', 'session_role': 'smoke_dev'}
        for field in correct:
            row = {**correct, field: 'unexpected'}
            conn = SimpleNamespace(execute=lambda sql: SimpleNamespace(fetchone=lambda: row))
            with self.subTest(field=field), self.assertRaises(SmokeFailure):
                validate_session_identity(conn, database='dev', session_user='smoke_dev')

    def test_cli_missing_dedicated_dsn_never_connects(self):
        argv = ['smoke', '--snapshot-id', '00000000-0000-0000-0000-000000000001',
                '--expected-dev-host', 'dev.example', '--expected-dev-database', 'dev',
                '--expected-dev-user', 'smoke_dev.project',
                '--expected-dev-session-user', 'smoke_dev',
                '--expected-dev-inspection-user', 'inspector.project',
                '--expected-dev-inspection-session-user', 'inspector', '--confirm-development-rollback']
        with patch('sys.argv', argv), patch.dict('os.environ', {}, clear=True), \
             patch('smoke_one_item_development.psycopg.connect') as connect, \
             patch('builtins.print'):
            self.assertEqual(main(), 1)
        connect.assert_not_called()


class HarnessFlowConnection(TransactionConnection):
    """Worker-rights double: real queue/processor/finish code, no DB SQL executed."""
    def __init__(self, snapshot, fail_insert=False):
        super().__init__()
        from uuid import uuid4
        self.snapshot = snapshot
        self.fail_insert = fail_insert
        self.rows = {k: [] for k in ('queue', 'runs', 'notes', 'events', 'versions', 'sources', 'evidence')}
        self.unrelated = dict(source_snapshot_id=uuid4(), pipeline_version='unrelated-live-work',
                              status='queued', attempt_count=0, lease_owner=None)
        self.rows['queue'].append(self.unrelated)
        self.smoke_pipeline = None
        self.inserted = []
        self.vector_param = None
        self.claimed_snapshots = []

    def execute(self, sql, params=()):
        from test_automatic_publication import NOW
        from psycopg.errors import InsufficientPrivilege
        self.statements.append(sql)
        self.assert_worker_query(sql)
        row, rows, rowcount, description = None, [], 1, []
        if 'AS row_count' in sql:
            row = {'row_count': sum(r['pipeline_version'] == params[0] for r in self.rows['queue'])}
        elif 'current_database()' in sql:
            row = dict(database='dev', current_role='mooby_nlp_worker_login_dev', session_role='mooby_nlp_worker_login_dev')
        elif sql.strip().startswith('SELECT clock_timestamp()'):
            row = {'clock_timestamp': NOW}
        elif 'WITH picked AS' in sql:
            candidates = [r for r in self.rows['queue'] if
                          r['pipeline_version'] == params['pipeline_version'] and r['status'] == 'queued']
            for record in candidates[:params['batch_size']]:
                record.update(status='claimed', attempt_count=record['attempt_count']+1,
                              claimed_at=NOW, lease_until=NOW, lease_owner=params['worker_id'])
                rows.append(dict(record))
                self.claimed_snapshots.append(record['source_snapshot_id'])
            description = [SimpleNamespace(name=k) for k in
                           ('source_snapshot_id', 'pipeline_version', 'attempt_count',
                            'claimed_at', 'lease_until', 'lease_owner')]
        elif 'INSERT INTO nlp.processing_queue' in sql:
            # The real dispatcher INSERT only uses the two granted columns.
            normalized = ' '.join(sql.split())
            if 'INSERT INTO nlp.processing_queue (source_snapshot_id, pipeline_version)' not in normalized:
                raise AssertionError('non-least-privilege enqueue')
            if params[1] is None or params[1] != params[2] or params[4] != 1:
                raise AssertionError('enqueue is not exactly snapshot-scoped')
            self.smoke_pipeline = params[3]
            record = dict(source_snapshot_id=self.snapshot.snapshot_id,
                          pipeline_version=self.smoke_pipeline, status='queued',
                          attempt_count=0, lease_owner=None)
            self.rows['queue'].append(record)
            rows = [{'source_snapshot_id': record['source_snapshot_id']}]
        elif 'UPDATE nlp.processing_queue' in sql:
            matching = [r for r in self.rows['queue'] if
                        r['pipeline_version'] == params[4] and r['source_snapshot_id'] == params[3]
                        and r['lease_owner'] == params[5]]
            if matching:
                matching[0].update(status=params[0], lease_owner=None)
                row = {'source_snapshot_id': self.snapshot.snapshot_id}
        elif 'INSERT INTO nlp.' in sql:
            for table, key in [('processing_runs', 'runs'), ('processing_notes', 'notes'),
                               ('events', 'events'), ('event_versions', 'versions'),
                               ('event_sources', 'sources'), ('event_evidence', 'evidence')]:
                if f'INSERT INTO nlp.{table} ' in sql:
                    if table == 'event_versions' and self.fail_insert:
                        raise InsufficientPrivilege('PRIVATE MESSAGE')
                    self.rows[key].append({'pipeline_version': self.smoke_pipeline})
                    self.inserted.append(key)
                    if table == 'event_versions': self.vector_param = params[-2]
                    break
        elif 'SELECT source_snapshot_id, status' in sql:
            rows = [dict(r) for r in self.rows['queue'] if r['pipeline_version'] == params[0]]
        return SimpleNamespace(fetchone=lambda: row, fetchall=lambda: rows,
                               rowcount=rowcount, description=description)

    def assert_worker_query(self, sql):
        if 'app_api.' in sql:
            raise AssertionError('worker must never inspect app_api')
        if sql.lstrip().startswith('SELECT') and any('FROM nlp.'+table in sql for table in
                ('processing_runs', 'processing_notes', 'events', 'event_versions',
                 'event_sources', 'event_evidence')):
            raise AssertionError('worker has no inspection SELECT grants')


class HarnessFlowTests(unittest.TestCase):
    def run_flow(self, fail_insert=False):
        import smoke_one_item_development as harness
        from dataclasses import replace
        from datetime import timedelta
        from test_automatic_publication import AutomaticPublicationTests, Classifier, TEST_EMBEDDING, NOW
        fixture = AutomaticPublicationTests(); fixture.setUp()
        original = replace(fixture.snapshot, captured_at=NOW-timedelta(days=30))
        conn = HarnessFlowConnection(original, fail_insert)
        before = copy.deepcopy(conn.rows)
        inspector_calls = []
        def residue(pipeline, event_id):
            # No observer is used until the outer worker transaction is gone.
            self.assertEqual(conn.rows, before)
            self.assertEqual(conn.rows['queue'][0]['status'], 'queued')
            inspector_calls.append((pipeline, event_id))
            return {k: sum(r.get('pipeline_version') == pipeline for r in v)
                    for k, v in conn.rows.items()}
        with patch.object(harness, 'load_snapshot', return_value=original),              patch.object(harness, 'load_station_catalog', return_value=fixture.catalog),              patch.object(harness, 'embedding_runtime'),              patch.object(harness.ClassifierAdapter, 'load', return_value=Classifier()),              patch.object(harness.WorkerSettings, 'from_env', return_value=fixture.settings),              patch('mooby.worker.processor.embed_event', return_value=TEST_EMBEDDING),              patch.object(harness, 'enqueue_recent_snapshots', wraps=harness.enqueue_recent_snapshots) as enqueue,              patch.object(harness, 'claim_batch', wraps=harness.claim_batch) as claim:
            if not fail_insert:
                report = harness.smoke_one(conn, original.snapshot_id,
                    expected_database='dev', expected_session_user='mooby_nlp_worker_login_dev', check_residue=residue)
                self.assertEqual(report['status'], 'passed')
                self.assertTrue(report['worker_persistence_checked'])
                self.assertTrue(report['rollback_verified'])
                self.assertNotIn('projection_visible', report)
            else:
                with self.assertRaises(SmokeFailure) as caught:
                    harness.smoke_one(conn, original.snapshot_id,
                        expected_database='dev', expected_session_user='mooby_nlp_worker_login_dev', check_residue=residue)
                self.assertEqual(caught.exception.report['stage'], 'insert_event_version')
                self.assertTrue(caught.exception.report['rollback_verified'])
            enqueue.assert_called_once()
            claim.assert_called_once()
            kwargs = enqueue.call_args.kwargs
            self.assertEqual(kwargs['snapshot_id'], str(original.snapshot_id))
            self.assertEqual(kwargs['batch_size'], 1)
            self.assertGreaterEqual(kwargs['lookback_seconds'], 30*24*60*60)
            self.assertEqual(claim.call_args.kwargs['pipeline_version'], kwargs['pipeline_version'])
            self.assertNotEqual(kwargs['pipeline_version'], 'unrelated-live-work')
            self.assertEqual(claim.call_args.kwargs['batch_size'], 1)
        self.assertEqual(conn.claimed_snapshots, [original.snapshot_id])
        self.assertEqual(len(inspector_calls), 1)
        self.assertEqual(conn.rows, before)
        self.assertEqual(original.captured_at, NOW-timedelta(days=30))
        if not fail_insert:
            self.assertEqual(conn.vector_param, TEST_EMBEDDING.pgvector())
            self.assertEqual(conn.inserted, ['runs', 'events', 'versions', 'sources', 'evidence'])
        self.assertFalse(any('app_api.' in s or 'UPDATE collection.' in s for s in conn.statements))

    def test_empty_or_wrong_snapshot_enqueue_never_claims_queue_work(self):
        import smoke_one_item_development as harness
        from test_automatic_publication import AutomaticPublicationTests, Classifier
        fixture = AutomaticPublicationTests(); fixture.setUp()
        for result in [[], ['00000000-0000-0000-0000-000000000099']]:
            conn = HarnessFlowConnection(fixture.snapshot)
            before = copy.deepcopy(conn.rows)
            with self.subTest(enqueued=result),                  patch.object(harness, 'load_snapshot', return_value=fixture.snapshot),                  patch.object(harness, 'embedding_runtime'),                  patch.object(harness.ClassifierAdapter, 'load', return_value=Classifier()),                  patch.object(harness.WorkerSettings, 'from_env', return_value=fixture.settings),                  patch.object(harness, 'enqueue_recent_snapshots', return_value=result),                  patch.object(harness, 'claim_batch') as claim:
                with self.assertRaises(SmokeFailure) as caught:
                    harness.smoke_one(conn, fixture.snapshot.snapshot_id,
                        expected_database='dev', expected_session_user='mooby_nlp_worker_login_dev',
                        check_residue=lambda *_: {k: 0 for k in before})
                self.assertEqual(caught.exception.report['stage'], 'exact_snapshot_not_enqueued')
                self.assertTrue(caught.exception.report['rollback_verified'])
                claim.assert_not_called()
                self.assertEqual(conn.rows, before)

    def test_real_enqueue_claim_finish_and_persistence_rollback_on_success(self):
        self.run_flow()

    def test_real_enqueue_claim_finish_and_persistence_rollback_on_failure(self):
        self.run_flow(fail_insert=True)


class StageReportingTests(unittest.TestCase):
    def permission_error(self, with_names=True):
        from psycopg.errors import InsufficientPrivilege
        class Denied(InsufficientPrivilege):
            @property
            def diag(self):
                return SimpleNamespace(schema_name='nlp' if with_names else None,
                    table_name='event_versions' if with_names else None,
                    column_name='embedding' if with_names else None,
                    message_detail='PRIVATE DETAIL', context='PRIVATE QUOTE',
                    message_primary='PRIVATE SQL')
        return Denied('PRIVATE SQL PARAMETERS CONNECTION URL RAW TEXT')

    def test_diagnostics_are_allowlisted_with_optional_names(self):
        report = database_error_report('insert_event_version', self.permission_error())
        self.assertEqual(report, dict(status='failed', stage='insert_event_version',
            sqlstate='42501', schema='nlp', table='event_versions', column='embedding'))
        self.assertNotIn('PRIVATE', str(report))
        unnamed = database_error_report('insert_event', self.permission_error(False))
        self.assertEqual(set(unnamed), {'status', 'stage', 'sqlstate'})

    def test_each_nested_write_and_read_gets_a_precise_stage(self):
        samples = [
            ('UPDATE nlp.processing_queue SET status = %s', 'finish_queue_update'),
            ('INSERT INTO nlp.processing_queue (x) VALUES (%s)', 'insert_smoke_queue'),
            ('INSERT INTO nlp.processing_runs (x) VALUES (%s)', 'insert_processing_run'),
            ('INSERT INTO nlp.processing_notes (x) VALUES (%s)', 'insert_processing_note'),
            ('INSERT INTO nlp.events (x) VALUES (%s)', 'insert_event'),
            ('INSERT INTO nlp.event_versions (x) VALUES (%s)', 'insert_event_version'),
            ('INSERT INTO nlp.event_sources (x) VALUES (%s)', 'insert_event_source'),
            ('INSERT INTO nlp.event_evidence (x) VALUES (%s)', 'insert_event_evidence'),
            ('SELECT id FROM collection.raw_snapshots', 'load_source_snapshot'),
            ('SELECT station_id FROM reference.krl_stations', 'load_station_catalog'),
            ('SELECT path_id FROM nlp.line_paths', 'load_confirmed_paths'),
            ('SELECT clock_timestamp()', 'read_database_clock')]
        for sql, label in samples:
            def fail(*args): raise self.permission_error()
            conn = StagedConnection(SimpleNamespace(execute=fail))
            with self.subTest(stage=label), self.assertRaises(Exception):
                conn.execute(sql, ('PRIVATE PARAMETER',))
            self.assertEqual(conn.first_error['stage'], label)
            self.assertEqual(conn.first_error['sqlstate'], '42501')
            self.assertNotIn('PRIVATE', str(conn.first_error))

    def test_row_count_permission_error_identifies_the_exact_table(self):
        def fail(*args): raise self.permission_error(False)
        conn = StagedConnection(SimpleNamespace(execute=fail))
        conn.stage = 'preflight_row_counts'
        with self.assertRaises(Exception): row_counts(conn, 'namespace', 'event-id')
        self.assertEqual(conn.first_error, {'status': 'failed',
            'stage': 'preflight_row_counts.nlp.processing_queue', 'sqlstate': '42501'})

    def test_fetch_error_retains_its_original_stage(self):
        def fail(): raise self.permission_error()
        conn = StagedConnection(SimpleNamespace(execute=lambda *args:
            SimpleNamespace(fetchone=fail)))
        conn.stage = 'inspect_event_version_and_vector'
        cursor = conn.execute('SELECT allowed_fields')
        conn.stage = 'other_operation'
        with self.assertRaises(Exception): cursor.fetchone()
        self.assertEqual(conn.first_error['stage'], 'inspect_event_version_and_vector')

    def test_permission_error_rolls_back_and_keeps_first_stage(self):
        raw = TransactionConnection()
        before = copy.deepcopy(raw.rows)
        conn = StagedConnection(raw)
        original_execute = raw.execute
        def execute(sql, params=()):
            if 'INSERT INTO nlp.event_versions' in sql:
                raise self.permission_error()
            return original_execute(sql)
        raw.execute = execute
        def exercise():
            raw.rows['queue'].append('smoke')
            conn.execute('INSERT INTO nlp.event_versions (x) VALUES (%s)', ('PRIVATE',))
        with self.assertRaises(SmokeFailure) as caught:
            rollback_only(conn, exercise, lambda: {'queue': len(raw.rows['queue'])-1})
        report = caught.exception.report
        self.assertEqual(report['stage'], 'insert_event_version')
        self.assertEqual(report['sqlstate'], '42501')
        self.assertTrue(report['rollback_verified'])
        self.assertEqual(raw.rows, before)
        self.assertNotIn('PRIVATE', str(report))

    def test_post_rollback_permission_failure_preserves_original_diagnostics(self):
        raw = TransactionConnection()
        conn = StagedConnection(raw)
        original_execute = raw.execute
        def execute(sql, params=()):
            if 'INSERT INTO' in sql:
                raise self.permission_error()
            if 'SELECT count' in sql:
                raise self.permission_error(False)
            return original_execute(sql)
        raw.execute = execute
        def exercise():
            conn.execute('INSERT INTO nlp.event_versions (x) VALUES (%s)', ('PRIVATE',))
        def absent():
            conn.stage = 'post_rollback_row_counts'
            return row_counts(conn, 'namespace', 'event-id')
        with self.assertRaises(SmokeFailure) as caught:
            rollback_only(conn, exercise, absent)
        report = caught.exception.report
        self.assertEqual(report['stage'], 'insert_event_version')
        self.assertEqual(report['sqlstate'], '42501')
        self.assertFalse(report['rollback_verified'])
        self.assertEqual(report['rollback_check_error']['stage'],
                         'post_rollback_row_counts.nlp.processing_queue')
        self.assertNotIn('PRIVATE', str(report))

    def test_swallowed_driver_error_can_be_reported_without_message(self):
        from psycopg.errors import InFailedSqlTransaction
        conn = StagedConnection(SimpleNamespace())
        conn.record('load_station_catalog', self.permission_error())
        conn.record('finish_queue_update', InFailedSqlTransaction('PRIVATE'))
        with self.assertRaises(SmokeFailure) as caught:
            conn.raise_recorded_error()
        self.assertEqual(caught.exception.report['stage'], 'load_station_catalog')
        self.assertEqual(caught.exception.report['sqlstate'], '42501')


class PostRollbackInspectorTests(unittest.TestCase):
    def test_inspector_uses_only_read_only_nlp_checks_and_closes(self):
        import smoke_one_item_development as harness
        raw = TransactionConnection()
        raw.info.host = 'dev.example'
        raw.closed = False
        raw.close = lambda: setattr(raw, 'closed', True)
        def execute(sql, params=()):
            self.assertGreater(raw.depth, 0)
            self.assertNotIn('app_api.', sql)
            self.assertTrue(sql.startswith(('SELECT', 'SET TRANSACTION READ ONLY')))
            raw.statements.append(sql)
            if 'current_database()' in sql:
                row = dict(database='dev', current_role='reader', session_role='reader')
            else:
                row = {'row_count': 0}
            return SimpleNamespace(fetchone=lambda: row)
        raw.execute = execute
        with patch.object(harness.psycopg, 'connect', return_value=raw) as connect:
            result = harness.inspect_residue('host=dev.example dbname=dev user=reader.project',
                host='dev.example', database='dev', user='reader.project', session_user='reader',
                pipeline='smoke-test', event_id='test-event')
        self.assertEqual(set(result), {'queue', 'runs', 'notes', 'events', 'versions', 'sources', 'evidence'})
        self.assertTrue(all(v == 0 for v in result.values()))
        self.assertEqual(connect.call_args.kwargs['options'], '-c default_transaction_read_only=on')
        self.assertEqual(raw.statements[0], 'SET TRANSACTION READ ONLY')
        self.assertEqual(raw.transactions, [True])
        self.assertTrue(raw.closed)

    def test_missing_inspection_credentials_fail_before_worker_connects(self):
        argv = ['smoke', '--snapshot-id', '00000000-0000-0000-0000-000000000001',
                '--expected-dev-host', 'dev.example', '--expected-dev-database', 'dev',
                '--expected-dev-user', 'worker.project', '--expected-dev-session-user', 'mooby_nlp_worker_login_dev',
                '--expected-dev-inspection-user', 'reader.project',
                '--expected-dev-inspection-session-user', 'reader', '--confirm-development-rollback']
        with patch('sys.argv', argv),              patch.dict('os.environ', {'SMOKE_DEV_DATABASE_URL':
                        'host=dev.example dbname=dev user=worker.project'}, clear=True),              patch('smoke_one_item_development.psycopg.connect') as connect, patch('builtins.print'):
            self.assertEqual(main(), 1)
        connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
