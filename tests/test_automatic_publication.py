"""Offline processor/transaction and API contract checks; no SQL is executed."""
import copy
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from mooby.api.app import create_app
from mooby.api.config import ApiSettings
from mooby.api.repository import FeedPage, _SEARCH_SQL
from mooby.db.source_reader import SourceSnapshot, StationReference
from mooby.worker.classifier import ModelArtifactError, ModelPrediction
from mooby.worker.processor import WorkerSettings, process_snapshot
from mooby.worker.queue import finish_attempt

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)


class Classifier:
    artifact_manifest = {}
    label = 'disruption'
    confidence = .99
    fail = False
    severity_confidence = .99

    def predict_relevance(self, text):
        if self.fail:
            raise ModelArtifactError('test failure')
        other = 'not_disruption' if self.label == 'disruption' else 'disruption'
        return ModelPrediction(self.label, {self.label: self.confidence,
                               other: 1-self.confidence}, self.confidence, 'test', 'a'*64)

    def predict_severity(self, text):
        rest = (1-self.severity_confidence)/2
        return ModelPrediction('light', {'light': self.severity_confidence,
                               'moderate': rest, 'severe': rest},
                               self.severity_confidence, 'test', 'a'*64)


class RecordingConnection:
    """Records SQL intent only; transaction fake models rollback on any exception."""
    def __init__(self, fail_on=None, lease=True):
        self.calls = []
        self.fail_on = fail_on
        self.lease = lease

    @contextmanager
    def transaction(self):
        before = copy.copy(self.calls)
        try:
            yield
        except Exception:
            self.calls = before
            raise

    def execute(self, sql, params=()):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError('simulated write/permission failure')
        self.calls.append((sql, params))
        return self

    def fetchone(self):
        return {'source_snapshot_id': 'claimed'} if self.lease else None


class AutomaticPublicationTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = SourceSnapshot(uuid4(), uuid4(), 'a'*64,
            'Gangguan sinyal di Stasiun Bogor.', 'post_text', 'complete',
            NOW, 'page_timestamp', NOW, 'x', 'https://example.com/post')
        self.settings = WorkerSettings.from_env({
            'MOOBY_RELEVANCE_THRESHOLD': '.8', 'MOOBY_SEVERITY_THRESHOLD': '.8',
            'MOOBY_TRUSTED_PUBLISHED_AT_BASES': 'page_timestamp'})
        self.catalog = [StationReference('open-krl:BOO', 'Bogor', ())]

    def process(self, snapshot=None, classifier=None, **kw):
        return process_snapshot(snapshot or self.snapshot, pipeline_version='test-v1',
            database_now=NOW, classifier=classifier or Classifier(),
            settings=self.settings, station_catalog=self.catalog, **kw)

    def finish(self, outcome, conn=None):
        conn = conn or RecordingConnection()
        finish_attempt(conn, {'attempt_count': 1, 'source_snapshot_id': self.snapshot.snapshot_id,
            'pipeline_version': 'test-v1', 'claimed_at': NOW}, worker_id='test-worker',
            result_status=outcome.result_status, code_version='test', rules_version='test',
            source_digest='a'*64, max_attempts=1, reason_code=outcome.reason_code,
            result_metadata=outcome.result_metadata, draft_event=outcome.draft_event)
        return conn

    def test_accepted_event_insert_satisfies_publication_contract(self):
        outcome = self.process()
        self.assertEqual(outcome.result_status, 'succeeded')
        self.assertIsNotNone(outcome.draft_event)
        self.assertEqual(outcome.result_metadata['event_draft_persistence']['verification_status'], 'verified')
        conn = self.finish(outcome)
        sql, params = next(c for c in conn.calls if 'INSERT INTO nlp.event_versions' in c[0])
        self.assertEqual(sql.count('%s'), len(params))
        normalized = ' '.join(sql.split())
        self.assertIn('verification_status, publication_status, verified_at, published_at', normalized)
        self.assertIn("'verified', 'published', transaction_timestamp(), transaction_timestamp()", normalized)
        self.assertIn('ON CONFLICT DO NOTHING', normalized)
        self.assertNotIn('DO UPDATE', normalized)
        self.assertEqual(sum('INSERT INTO' in s for s, _ in conn.calls), 5)
        # Migration 003 requires a timestamp iff each corresponding status is set.
        schema = (ROOT/'migrations/003_event_core.sql').read_text()
        for status, stamp, value in [('verification_status', 'verified_at', 'verified'),
                                      ('publication_status', 'published_at', 'published')]:
            self.assertIn(f"({status} = '{value}')\n        = ({stamp} IS NOT NULL)", schema)

    def test_nonaccepted_results_never_insert_events(self):
        irrelevant = Classifier(); irrelevant.label = 'not_disruption'
        below = Classifier(); below.confidence = .6
        broken = Classifier(); broken.fail = True
        severity_below = Classifier(); severity_below.severity_confidence = .6
        outcomes = [self.process(replace(self.snapshot, raw_text='Selamat pagi Bogor.')),
            self.process(replace(self.snapshot, raw_text='Gangguan di tempat asing.')),
            self.process(classifier=irrelevant), self.process(classifier=below),
            self.process(classifier=broken),
            self.process(classifier=severity_below),
            self.process(replace(self.snapshot, published_at=None))]
        # Explicit rejection/format error paths must also never persist a draft.
        from mooby.worker.processor import ProcessingOutcome
        outcomes += [ProcessingOutcome('policy_rejection', reason_code='test_rejection'),
                     ProcessingOutcome('format_failure', reason_code='test_format')]
        for outcome in outcomes:
            with self.subTest(outcome=outcome.reason_code):
                self.assertIsNone(outcome.draft_event)
                conn = self.finish(outcome)
                self.assertFalse(any('INSERT INTO nlp.event_' in s for s, _ in conn.calls))

    def test_succeeded_without_valid_draft_does_not_publish(self):
        # Classifiers accept, but invalid source provenance blocks the draft.
        outcome = self.process(replace(self.snapshot, canonical_url='http://example.com/post'))
        self.assertEqual(outcome.result_status, 'succeeded')
        self.assertIsNone(outcome.draft_event)
        self.assertFalse(any('INSERT INTO nlp.event_' in s for s, _ in self.finish(outcome).calls))

    def test_unresolved_segment_never_publishes_despite_classification_success(self):
        self.catalog.append(StationReference('open-krl:THB', 'Tanah Abang', ()))
        outcome = self.process(replace(self.snapshot,
            raw_text='Gangguan sinyal lintas Bogor sampai Tanah Abang.'))
        self.assertIsNone(outcome.draft_event)
        self.assertFalse(any('INSERT INTO nlp.event_' in s for s, _ in self.finish(outcome).calls))

    def test_draft_cannot_bypass_failed_result_or_identity(self):
        good = self.process()
        for bad in [replace(good, result_status='error', reason_code='test_error'),
                    replace(good, draft_event=replace(good.draft_event, pipeline_version='other'))]:
            conn = RecordingConnection()
            with self.assertRaises(ValueError): self.finish(bad, conn)
            self.assertEqual(conn.calls, [])

    def test_evidence_and_write_failures_roll_back_entire_attempt(self):
        good = self.process()
        bad = replace(good, draft_event=replace(good.draft_event, quote_start_char=999))
        conn = RecordingConnection()
        with self.assertRaises(ValueError): self.finish(bad, conn)
        self.assertEqual(conn.calls, [])
        for table in ['nlp.event_versions', 'nlp.event_sources', 'nlp.event_evidence']:
            conn = RecordingConnection(fail_on='INSERT INTO '+table)
            with self.assertRaises(RuntimeError): self.finish(good, conn)
            self.assertEqual(conn.calls, [])
        conn = RecordingConnection(lease=False)
        with self.assertRaises(RuntimeError): self.finish(good, conn)
        self.assertEqual(conn.calls, [])

    def test_existing_projection_and_api_allowlist(self):
        view = (ROOT/'migrations/014_app_api_read_contract.sql').read_text()
        self.assertIn("WHERE verification_status = 'verified'", view)
        self.assertIn("AND publication_status = 'published'", view)
        self.assertIn("interval '24 hours'", view)
        self.assertIn('AS affected_station_ids', view)
        self.assertNotIn('event_cluster', view)
        self.assertIn('v.affected_station_ids && %s::text[]', _SEARCH_SQL)
        # Exercise the actual endpoint/serialization with a mocked projection row.
        draft = self.process().draft_event
        row = dict(public_event_id=draft.event_id, event_kind='station', title=draft.title,
            summary=draft.summary, severity='light', station_name='Bogor',
            station_id='open-krl:BOO', affected_station_ids=['open-krl:BOO'],
            event_at=None, event_time_certainty='unknown', freshness='fresh',
            certainty_label=None, source_display_name='X', source_url=draft.canonical_url,
            source_published_at=NOW, raw_text='PRIVATE SENTINEL', evidence_quote='PRIVATE SENTINEL',
            author='PRIVATE SENTINEL', source_snapshot_id=str(self.snapshot.snapshot_id),
            result_metadata={'private': True})
        app = create_app()
        endpoint = next(r.endpoint for r in app.routes if r.path == '/api/v1/disruptions')
        with patch('mooby.api.app._synthetic_fixture_mode_enabled', return_value=False), \
             patch('mooby.api.app.fetch_disruptions', return_value=FeedPage(NOW, [row])):
            result = endpoint(ApiSettings('postgresql://unused', 'x'*32), ['open-krl:BOO'], 20, 0)
        payload = result.model_dump(mode='json')
        self.assertEqual(payload['availability'], 'results')
        item = payload['items'][0]
        self.assertEqual(item['station_or_segment']['affected_station_ids'], ['open-krl:BOO'])
        self.assertEqual(set(item), {'public_event_id','title','summary','station_or_segment',
            'severity','event_time','event_time_certainty','freshness','certainty','source'})
        self.assertEqual(set(item['source']), {'display_name', 'url', 'published_at'})
        for private in ['PRIVATE SENTINEL', 'raw_text', 'evidence_quote', 'author',
                        'source_snapshot_id', 'result_metadata']:
            self.assertNotIn(private, result.model_dump_json())

    def test_optional_embedding_migration_and_access_blocker(self):
        migration = (ROOT/'migrations/015_optional_event_embeddings.sql').read_text()
        self.assertIn('ALTER COLUMN embedding DROP NOT NULL', migration)
        self.assertIn('ALTER COLUMN embedding_input_version DROP NOT NULL', migration)
        self.assertIn('(embedding IS NULL) = (embedding_input_version IS NULL)', migration)
        self.assertNotRegex(migration, r'(?im)^\s*(GRANT|UPDATE|INSERT|CREATE ROLE)\b')
        grants = (ROOT/'migrations/012_worker_draft_event_access.sql').read_text()
        # Deployment is explicitly blocked by existing column-level permissions.
        for column in ['verification_status', 'publication_status', 'verified_at', 'published_at']:
            self.assertNotRegex(grants, rf'\b{column}\b')


if __name__ == '__main__':
    unittest.main()
