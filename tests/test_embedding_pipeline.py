"""Test doubles exercise the provider and worker; no download or database."""
from dataclasses import replace
from pathlib import Path
import json
import tempfile
import types
import unittest
from unittest.mock import patch

from mooby.worker.embedding import (
    EventEmbedding, EmbeddingUnavailable, INPUT_VERSION, _runtime, embed_event)
from mooby.worker.bundle_manifest import build_manifest, manifest_digest, DIGEST_METHOD
from mooby.worker.processor import process_and_finish_attempt
from mooby.worker.queue import finish_attempt
import test_automatic_publication as fixtures
from test_automatic_publication import (
    Classifier, RecordingConnection, TEST_EMBEDDING, NOW)


class EmbeddingTests(unittest.TestCase):
    def test_schema_dimension_and_runtime_lock_match(self):
        root = Path(__file__).resolve().parents[1]
        schema = (root/'migrations/004_event_embeddings.sql').read_text()
        lock = json.loads((root/'evaluation/embedding_model.lock.json').read_text())
        self.assertIn('embedding extensions.vector(384) NOT NULL', schema)
        self.assertIn('embedding_input_version text NOT NULL', schema)
        self.assertEqual(lock['embedding_dimension'], 384)
        self.assertEqual(lock['model_id'], 'intfloat/multilingual-e5-small')

    def test_contract_rejects_invalid_vectors_and_versions(self):
        TEST_EMBEDDING.validate()
        for bad in [replace(TEST_EMBEDDING, values=(1.,)*383),
                    replace(TEST_EMBEDDING, values=(0.,)*384),
                    replace(TEST_EMBEDDING, values=(float('nan'),)*384),
                    replace(TEST_EMBEDDING, values=(float('inf'),)*384),
                    replace(TEST_EMBEDDING, input_version='wrong'),
                    replace(TEST_EMBEDDING, model_digest='invalid')]:
            with self.subTest(bad=bad):
                with self.assertRaises(EmbeddingUnavailable): bad.validate()

    def test_real_adapter_uses_derived_text_passage_prefix_and_normalization(self):
        class Encoder:
            def encode(self, inputs, **kw):
                self.inputs, self.kw = inputs, kw
                return [TEST_EMBEDDING.values]
        encoder = Encoder()
        with patch('mooby.worker.embedding._runtime', return_value=(encoder, 'pinned@revision', 'b'*64)):
            result = embed_event('gangguan sinyal di bogor.')
        self.assertEqual(encoder.inputs, ['passage: gangguan sinyal di bogor.'])
        self.assertTrue(encoder.kw['normalize_embeddings'])
        self.assertEqual(len(result.values), 384)
        self.assertEqual(result.input_version, INPUT_VERSION)
        self.assertEqual(result.model_version, 'pinned@revision')
        self.assertEqual(result.model_digest, 'b'*64)

    def test_runtime_is_local_only_checks_artifacts_and_dimension(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)/'snapshot'; folder.mkdir()
            (folder/'weights').write_bytes(b'unit test artifact, not a real model')
            lock = Path(tmp)/'lock.json'
            manifest = build_manifest(folder, 'intfloat/multilingual-e5-small', 'pinned')
            (Path(tmp)/'manifest.json').write_text(json.dumps(manifest))
            data = {'model_id': 'intfloat/multilingual-e5-small', 'revision': 'pinned',
                    'embedding_dimension':384, 'artifact_sha256': manifest_digest(manifest),
                    'digest_method':DIGEST_METHOD, 'manifest_file':'manifest.json', 'file_count':1}
            lock.write_text(json.dumps(data))
            class Model:
                def __init__(self, path, **kw):
                    self.kw = kw
                    assert kw['local_files_only'] and not kw['trust_remote_code']
                def get_sentence_embedding_dimension(self): return 384
            def download(model_id, **kw):
                self.assertEqual(kw, {'revision':'pinned','local_files_only':True})
                return str(folder)
            with patch('mooby.worker.embedding.LOCK_PATH', lock), \
                 patch.dict('os.environ', {'MOOBY_EMBEDDING_MODEL_DIR': ''}), \
                 patch.dict('sys.modules', {
                    'sentence_transformers': types.SimpleNamespace(SentenceTransformer=Model),
                    'huggingface_hub': types.SimpleNamespace(snapshot_download=download)}):
                _runtime.cache_clear()
                model, version, digest = _runtime()
                self.assertEqual(version, 'intfloat/multilingual-e5-small@pinned')
                self.assertEqual(digest, data['artifact_sha256'])
                (folder/'weights').write_bytes(b'changed')
                _runtime.cache_clear()
                with self.assertRaises(ValueError): _runtime()
            _runtime.cache_clear()


class WorkerEmbeddingTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.AutomaticPublicationTests(); fixture.setUp()
        self.fixture = fixture
        self.item = {'attempt_count':1, 'source_snapshot_id':fixture.snapshot.snapshot_id,
                     'pipeline_version':'test-v1', 'claimed_at':NOW}

    def run_worker(self, conn, **kw):
        f = self.fixture
        with patch('mooby.worker.processor.database_clock_now', return_value=NOW):
            return process_and_finish_attempt(conn, self.item, kw.pop('snapshot', f.snapshot),
                worker_id='test', classifier=kw.pop('classifier', Classifier()),
                settings=f.settings, max_attempts=3, transient_retry_after_seconds=10,
                station_catalog=f.catalog, **kw)

    def test_accepted_embeds_before_atomic_publication_and_records_provenance(self):
        conn = RecordingConnection()
        def embedding(text):
            self.assertFalse(conn.calls)  # no queue completion/event inserts before embedding
            self.assertEqual(text, 'gangguan sinyal di stasiun bogor.')
            return TEST_EMBEDDING
        with patch('mooby.worker.processor.embed_event', side_effect=embedding) as provider:
            outcome, _ = self.run_worker(conn)
        provider.assert_called_once()
        self.assertEqual(outcome.result_status, 'succeeded')
        self.assertEqual(outcome.result_metadata['embedding']['dimension'], 384)
        sql, params = next(c for c in conn.calls if 'INSERT INTO nlp.event_versions' in c[0])
        self.assertEqual(sql.count('%s'), len(params))
        self.assertEqual(params[-2:], (TEST_EMBEDDING.pgvector(), INPUT_VERSION))
        self.assertIn("'verified', 'published', transaction_timestamp(), transaction_timestamp()", sql)
        run_sql, run = next(c for c in conn.calls if 'INSERT INTO nlp.processing_runs' in c[0])
        self.assertEqual(run_sql.count('%s'), len(run))
        self.assertEqual(run[8], TEST_EMBEDDING.model_version)
        self.assertEqual(run[-1], TEST_EMBEDDING.model_digest)

    def test_embedding_failure_records_retry_without_any_event(self):
        for result in [EmbeddingUnavailable('test'), replace(TEST_EMBEDDING, values=())]:
            conn = RecordingConnection()
            args = {'side_effect':result} if isinstance(result, Exception) else {'return_value':result}
            with patch('mooby.worker.processor.embed_event', **args):
                outcome, _ = self.run_worker(conn)
            self.assertEqual(outcome.reason_code, 'transient_embedding_error')
            self.assertIsNone(outcome.draft_event)
            self.assertEqual(outcome.retry_after_seconds, 10)
            self.assertFalse(any('INSERT INTO nlp.event' in s for s, _ in conn.calls))
            queue = next(p for s, p in conn.calls if 'UPDATE nlp.processing_queue' in s)
            self.assertEqual(queue[0], 'queued')
            self.assertTrue(any('INSERT INTO nlp.processing_runs' in s for s, _ in conn.calls))
            self.assertTrue(any('INSERT INTO nlp.processing_notes' in s for s, _ in conn.calls))

    def test_embedding_is_never_called_for_failed_gates_or_no_draft(self):
        broken = Classifier(); broken.fail = True
        irrelevant = Classifier(); irrelevant.label = 'not_disruption'
        for kwargs in [dict(snapshot=replace(self.fixture.snapshot, raw_text='Pagi Bogor.')),
                       dict(classifier=broken), dict(classifier=irrelevant),
                       dict(snapshot=replace(self.fixture.snapshot, canonical_url='invalid'))]:
            with patch('mooby.worker.processor.embed_event') as provider:
                self.run_worker(RecordingConnection(), **kwargs)
            provider.assert_not_called()

    def test_write_failure_rolls_back_embedding_evidence_run_queue(self):
        for target in ['nlp.event_versions','nlp.event_sources','nlp.event_evidence']:
            conn = RecordingConnection(fail_on='INSERT INTO '+target)
            with patch('mooby.worker.processor.embed_event', return_value=TEST_EMBEDDING):
                with self.assertRaises(RuntimeError): self.run_worker(conn)
            self.assertEqual(conn.calls, [])

    def test_missing_embedding_cannot_bypass_persistence(self):
        good = self.fixture.process()
        conn = RecordingConnection()
        with self.assertRaises(ValueError):
            finish_attempt(conn, self.item, worker_id='test', result_status='succeeded',
                code_version='test', rules_version='test', source_digest='a'*64,
                max_attempts=3, draft_event=good.draft_event)
        self.assertEqual(conn.calls, [])
