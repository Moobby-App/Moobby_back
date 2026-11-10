"""Approved original text and forward view compatibility, without a database."""
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import re
import unittest

from mooby.api.app import _item
from mooby.api.synthetic_fixtures import _item_from_draft
import test_automatic_publication as fixtures

ROOT = Path(__file__).resolve().parents[1]


class OriginalTextTests(unittest.TestCase):
    def setUp(self):
        f = fixtures.AutomaticPublicationTests(); f.setUp()
        self.fixture = f
        self.draft = f.process().draft_event
        self.original = '  '+self.draft.raw_text+'\n🚉\n'
        self.draft = replace(self.draft, raw_text=self.original,
                             quote_start_char=self.draft.quote_start_char+2)

    def row(self):
        return dict(public_event_id=self.draft.event_id, event_kind='station',
                    title=self.draft.title, summary=self.draft.summary,
                    station_name='Bogor', station_id='open-krl:BOO',
                    affected_station_ids=['open-krl:BOO'], severity='light',
                    event_at=None, event_time_certainty='unknown', freshness='fresh',
                    certainty_label=None, source_display_name='X',
                    source_url='https://example.com/post', source_published_at=None)

    def test_original_text_is_optional_for_legacy_rows_and_exact_for_new_rows(self):
        row = self.row()
        self.assertIsNone(_item(row).original_text)
        row['original_text'] = self.original
        payload = _item(row).model_dump(mode='json')
        self.assertEqual(payload['original_text'], self.original)
        self.assertNotIn('evidence_quote', payload)
        self.assertNotIn('source_snapshot_id', payload)
        self.assertNotIn('author', payload)

    def test_mock_uses_original_synthetic_input_without_rewriting(self):
        stations = {s.station_id:s for s in self.fixture.catalog}
        record = _item_from_draft(self.draft, datetime.now(timezone.utc), stations)
        self.assertEqual(record.item.original_text, self.original)
        self.assertTrue(record.item.title.startswith('SIMULASI'))
        self.assertEqual(record.item.station_or_segment.affected_station_ids, ['open-krl:BOO'])

    def test_persistence_copies_unmodified_text_and_preserves_embedding_bindings(self):
        outcome = replace(self.fixture.process(), draft_event=self.draft)
        conn = self.fixture.finish(outcome)
        sql, params = next(c for c in conn.calls if 'INSERT INTO nlp.event_versions' in c[0])
        self.assertIn('original_text', sql)
        self.assertEqual(params[-3], self.original)
        self.assertEqual(params[-1], fixtures.TEST_EMBEDDING.input_version)
        self.assertEqual(sql.count('%s'), len(params))

    def test_forward_migration_only_extends_existing_view_output_and_new_text_column(self):
        old = (ROOT/'migrations/014_app_api_read_contract.sql').read_text()
        new = (ROOT/'migrations/018_app_api_original_text.sql').read_text()
        def view(sql, prefix):
            start = sql.index(prefix+' app_api.published_disruptions')
            return sql[start:sql.index('\nCOMMENT ON VIEW',start)].strip()
        original = view(old,'CREATE VIEW')
        extended = view(new,'CREATE OR REPLACE VIEW')
        restored = extended.replace('CREATE OR REPLACE VIEW','CREATE VIEW',1)
        restored = restored.replace('        ev.summary,\n        ev.original_text\n', '        ev.summary\n',1)
        restored = restored.replace('    src.source_published_at,\n    ev.original_text\n', '    src.source_published_at\n',1)
        self.assertEqual(restored,original)
        self.assertIn('ADD COLUMN original_text text;', new)
        self.assertIn('GRANT INSERT (original_text) ON TABLE nlp.event_versions TO mooby_nlp_worker_dev;',new)
        self.assertIn('GRANT SELECT (original_text) ON TABLE nlp.event_versions TO mooby_app_api_owner_dev;',new)
        sql = re.sub(r'--[^\n]*','',new)
        self.assertNotRegex(sql, r'(?im)^\s*(UPDATE|DELETE|INSERT INTO)\b')
        self.assertNotIn('collection.',sql)
        self.assertIn("AND grantor = 'postgres'::regrole", sql)
        self.assertIn("IF session_user <> 'postgres' OR current_user <> 'postgres'", sql)
        self.assertIn('AND admin_option', sql)
        self.assertIn('WITH INHERIT FALSE, SET TRUE, ADMIN FALSE\n    GRANTED BY postgres;', sql)
        revokes = re.findall(r'(?im)^\s*REVOKE[^;]+;', sql)
        self.assertEqual([r.strip() for r in revokes], [
            'REVOKE mooby_app_api_owner_dev FROM postgres GRANTED BY postgres RESTRICT;'])
        self.assertNotIn('GRANTED BY supabase_admin', sql)
        self.assertLess(sql.index('DO $preflight$'), sql.index('ALTER TABLE'))
        self.assertLess(sql.index('GRANT mooby_app_api_owner_dev'),
                        sql.index('SET LOCAL ROLE mooby_app_api_owner_dev;'))
        self.assertIn('SET LOCAL ROLE mooby_app_api_owner_dev;', sql)
