"""Local read-only diagnostic checks using fake driver connections only."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
import diagnose_api_feed as diagnostic
from mooby.api.config import ApiSettings
from mooby.api.repository import _SEARCH_SQL
from psycopg.errors import OperationalError, UndefinedColumn


class FakeConnection:
    def __init__(self, fail_readiness=False):
        self.queries = []
        self.closed = False
        self.rollbacks = []
        self.fail_readiness = fail_readiness

    @contextmanager
    def transaction(self, force_rollback=False):
        self.rollbacks.append(force_rollback)
        yield

    def execute(self, sql, params=()):
        self.queries.append((sql, params))
        if self.fail_readiness and sql == 'SELECT public_event_id FROM app_api.published_disruptions LIMIT 1':
            raise UndefinedColumn('PRIVATE SQL ROW PARAMETER CREDENTIAL')
        if 'AS generated_at' in sql:
            row = {'generated_at': datetime(2026, 11, 8, tzinfo=timezone.utc)}
        elif sql == 'SELECT public_event_id FROM app_api.published_disruptions LIMIT 1':
            row = None
        else:
            row = {'count': 0}
        return SimpleNamespace(fetchone=lambda: row, fetchall=lambda: [])

    def close(self):
        self.closed = True


class ApiDiagnosticTests(unittest.TestCase):
    def settings(self):
        return ApiSettings('postgresql://PRIVATE_UNUSED_DSN', 'PRIVATE_BEARER_'+'x'*32)

    def test_empty_view_is_success_and_query_parameters_are_never_reported(self):
        conns = [FakeConnection(), FakeConnection()]
        emitted = []
        with patch.object(diagnostic.psycopg, 'connect', side_effect=conns) as connect:
            self.assertTrue(diagnostic.diagnose(self.settings(), ['open-krl:BOO'], emitted.append))
        self.assertNotIn('options', connect.call_args_list[0].kwargs)
        self.assertEqual(connect.call_args_list[1].kwargs['options'], '-c statement_timeout=3000')
        for conn in conns:
            self.assertEqual(conn.queries[0], ('SET TRANSACTION READ ONLY', ()))
            self.assertIn((_SEARCH_SQL, (['open-krl:BOO'], 21, 0)), conn.queries)
            self.assertTrue(conn.closed)
            self.assertTrue(all(conn.rollbacks))
        for row in emitted:
            self.assertEqual(set(row), {'stage','exception_type','sqlstate'})
            self.assertIsNone(row['exception_type'])
        self.assertNotIn('PRIVATE', json.dumps(emitted))
        self.assertNotIn('open-krl:BOO', json.dumps(emitted))

    def test_startup_option_failure_is_distinguished_from_manual_baseline(self):
        emitted = []
        def connect(*args, **kwargs):
            if 'options' in kwargs:
                raise OperationalError('PRIVATE CONNECTION URL')
            return FakeConnection()
        with patch.object(diagnostic.psycopg, 'connect', side_effect=connect):
            self.assertFalse(diagnostic.diagnose(self.settings(), ['open-krl:BOO'], emitted.append))
        failure = next(row for row in emitted if row['stage'] == 'api_options.connect')
        self.assertEqual(failure['exception_type'], 'OperationalError')
        self.assertNotIn('PRIVATE', json.dumps(emitted))

    def test_projection_column_failure_has_type_and_sqlstate_without_message(self):
        emitted = []
        with patch.object(diagnostic.psycopg, 'connect', side_effect=[FakeConnection(True), FakeConnection(True)]):
            self.assertFalse(diagnostic.diagnose(self.settings(), ['open-krl:BOO'], emitted.append))
        failures = [r for r in emitted if r['stage'].endswith('.projection_readiness')]
        self.assertEqual(len(failures), 2)
        for row in failures:
            self.assertEqual(row['exception_type'], 'UndefinedColumn')
            self.assertEqual(row['sqlstate'], '42703')
        self.assertTrue(any(r['stage'] == 'api_options.feed_query' for r in emitted))
        self.assertNotIn('PRIVATE', json.dumps(emitted))

    def test_query_view_columns_match_migration_contract(self):
        import re
        expected = {'public_event_id','event_kind','title','summary','original_text','severity','event_at',
                    'event_time_certainty','certainty_label','freshness_anchor_at','line_name',
                    'station_name','station_id','start_station_name','start_station_id',
                    'end_station_name','end_station_id','direction','affected_station_ids',
                    'source_display_name','source_url','source_published_at'}
        self.assertEqual(set(re.findall(r'\bv\.(\w+)', _SEARCH_SQL)), expected)
        view = (Path(__file__).resolve().parents[1]/'migrations/018_app_api_original_text.sql').read_text()
        for column in expected:
            self.assertRegex(view, rf'\b{column}\b')
        self.assertIn('WHERE v.affected_station_ids && %s::text[]', _SEARCH_SQL)
        self.assertIn('ORDER BY v.freshness_anchor_at DESC, v.public_event_id', _SEARCH_SQL)
