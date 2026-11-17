"""Final package/reference equivalence and isolated worker/DB compatibility."""
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import joblib

from mooby.worker.classifier import ClassifierAdapter, ModelArtifactError, normalize_model_text
from mooby.worker.model_config import ACTIVE_CLASSIFIER_ARTIFACT_DIR
from mooby.worker.processor import _process_runtime, process_snapshot, WorkerSettings
import test_automatic_publication as publication_fixture
from test_automatic_publication import NOW, RecordingConnection, TEST_EMBEDDING
from mooby.worker.queue import finish_attempt

ROOT = Path(__file__).resolve().parents[1]
TEXTS = [
    'KRL lancar hari ini, selamat pagi!',
    'KRL di Stasiun Serpong terlambat 10 menit karena antre masuk stasiun.',
    'Gangguan persinyalan di Stasiun Serpong, perjalanan KRL tertahan.',
    'KRL tabrakan dan jalur ditutup total, perjalanan dibatalkan di Stasiun Serpong.',
    '  #KRL\t\tDELAY 😭\n@CommuterLine https://example.invalid/berita  ',
    'ＫＲＬ gangguan di Stasiun Bogor',
    'Info: Perjalanan Commuter Line mengalami keterlambatan akibat gangguan listrik.',
]


class HybridClassifierTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.classifier = ClassifierAdapter.load()

    def test_all_final_stages_loaded_and_no_probabilities(self):
        self.assertTrue(all(v['status']=='loaded' for v in self.classifier.artifact_manifest.values()))
        for method in (self.classifier.predict_relevance,self.classifier.predict_severity):
            prediction=method(TEXTS[3])
            self.assertIsNone(prediction.confidence)
            self.assertIsNone(prediction.probabilities)

    def test_only_whitespace_is_changed(self):
        text='  ＫＲＬ\tDELAY 😭\n@A https://EXAMPLE.invalid  '
        self.assertEqual(normalize_model_text(text),'ＫＲＬ DELAY 😭 @A https://EXAMPLE.invalid')

    def test_invalid_and_empty_input_and_batch(self):
        for text in (None,1,[],{}):
            with self.assertRaises(TypeError):self.classifier.predict(text)
        for text in ('','  \n\t'):
            with self.assertRaises(ValueError):self.classifier.predict(text)
        self.assertEqual(self.classifier.predict_many([]),[])
        with self.assertRaises(TypeError):self.classifier.predict_many('not a batch')
        with self.assertRaises(TypeError):self.classifier.predict_many(['valid',None])

    def test_packaged_inference_matches_supplied_reference(self):
        reference=ROOT/'krl_hybrid_final_v1/predictor.py'
        spec=importlib.util.spec_from_file_location('supplied_hybrid_reference',reference)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        predictor=module.KRLClassifier(reference.parent)
        self.assertEqual(self.classifier.predict_many(TEXTS),[predictor.predict(t) for t in TEXTS])
        self.assertTrue(all(module.KRLClassifier.preprocess(t)==normalize_model_text(t) for t in TEXTS))

    def test_non_disruption_short_circuits_stage2(self):
        with patch.object(self.classifier.stage2.estimator,'predict',side_effect=AssertionError('stage2 forbidden')):
            self.assertEqual(self.classifier.predict(TEXTS[0]),{'prediction':'not_disruption','is_disruption':False})

    def test_heavy_is_preserved_in_model_output_and_mapped_only_for_storage(self):
        self.assertEqual(self.classifier.predict(TEXTS[3])['prediction'],'heavy')
        prediction=self.classifier.predict_severity(TEXTS[3])
        self.assertEqual((prediction.model_label,prediction.label),('heavy','severe'))

    def test_joblib_load_only_twice_and_never_in_inference(self):
        with patch('joblib.load',wraps=joblib.load) as loader:
            adapter=ClassifierAdapter.load()
            self.assertEqual(loader.call_count,2)
            adapter.predict_many(TEXTS)
            adapter.predict_many(TEXTS)
            self.assertEqual(loader.call_count,2)

    def test_worker_runtime_cached_and_no_probability_settings_needed(self):
        _process_runtime.cache_clear()
        self.addCleanup(_process_runtime.cache_clear)
        with patch.dict('os.environ',{'MOOBY_TRUSTED_PUBLISHED_AT_BASES':'page_timestamp'},clear=True), \
             patch('joblib.load',wraps=joblib.load) as loader:
            first=_process_runtime();second=_process_runtime()
            self.assertIs(first[0],second[0])
            self.assertEqual(loader.call_count,2)

    def test_corruption_rejected_without_fallback_or_deserialization(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'model';shutil.copytree(ACTIVE_CLASSIFIER_ARTIFACT_DIR,path)
            (path/'stage2.joblib').write_bytes(b'corrupt')
            with patch('joblib.load',side_effect=AssertionError('no deserialize')):
                adapter=ClassifierAdapter.load(path)
            self.assertTrue(all(v['status']=='unavailable' for v in adapter.artifact_manifest.values()))
            with self.assertRaises(ModelArtifactError):adapter.predict(TEXTS[0])

    def test_wrong_runtime_dependency_is_rejected(self):
        with patch('mooby.worker.classifier.importlib.metadata.version',return_value='mismatch'):
            adapter=ClassifierAdapter.load()
        self.assertEqual(adapter.artifact_manifest['relevance']['error_type'],'RuntimeError')


class HybridWorkerIntegrationTests(unittest.TestCase):
    def setUp(self):
        fixture=publication_fixture.AutomaticPublicationTests();fixture.setUp()
        self.snapshot=fixture.snapshot
        self.catalog=fixture.catalog
        self.classifier=ClassifierAdapter.load()

    def test_worker_receives_original_case_and_stores_severe_without_probabilities(self):
        text='  KRL mengalami GANGGUAN akibat TABRAKAN di Stasiun Bogor, jalur ditutup total dan perjalanan dibatalkan 😭  '
        snapshot=replace(self.snapshot,raw_text=text)
        settings=WorkerSettings.from_env({'MOOBY_TRUSTED_PUBLISHED_AT_BASES':'page_timestamp'})
        expected=self.classifier.predict(text)
        self.assertEqual(expected['prediction'],'heavy')
        with patch.object(self.classifier.stage1.estimator,'predict',wraps=self.classifier.stage1.estimator.predict) as stage1:
            result=process_snapshot(snapshot,pipeline_version='test-hybrid-v1',database_now=NOW,
                classifier=self.classifier,settings=settings,station_catalog=self.catalog)
        self.assertEqual(stage1.call_args.args[0],[normalize_model_text(text)])
        self.assertEqual(result.result_status,'succeeded')
        self.assertEqual(result.draft_event.severity,'severe')
        self.assertEqual(result.draft_event.raw_text,text)
        self.assertIsNone(result.result_metadata['thresholds'])
        self.assertIsNone(result.result_metadata['severity_prediction']['confidence'])
        self.assertEqual(result.result_metadata['severity_prediction']['model_label'],'heavy')
        conn=RecordingConnection()
        finish_attempt(conn,{'attempt_count':1,'source_snapshot_id':snapshot.snapshot_id,
            'pipeline_version':'test-hybrid-v1','claimed_at':NOW},worker_id='test',
            result_status=result.result_status,code_version='test',rules_version='test',
            source_digest='a'*64,max_attempts=1,reason_code=result.reason_code,
            result_metadata=result.result_metadata,draft_event=result.draft_event,
            event_embedding=TEST_EMBEDDING,embedding_model_version=TEST_EMBEDDING.model_version,
            embedding_model_digest=TEST_EMBEDDING.model_digest)
        sql,params=next(c for c in conn.calls if 'INSERT INTO nlp.event_versions' in c[0])
        self.assertIn('severe',params)
        self.assertNotIn('heavy',params)

    def test_non_disruption_does_not_run_stage2_or_make_draft(self):
        snapshot=replace(self.snapshot,raw_text='KRL di Stasiun Bogor lancar, tidak ada gangguan hari ini.')
        settings=WorkerSettings.from_env({'MOOBY_TRUSTED_PUBLISHED_AT_BASES':'page_timestamp'})
        with patch.object(self.classifier.stage2.estimator,'predict',side_effect=AssertionError('forbidden')):
            result=process_snapshot(snapshot,pipeline_version='test',database_now=NOW,
                classifier=self.classifier,settings=settings,station_catalog=self.catalog)
        self.assertEqual(result.reason_code,'non_disruption')
        self.assertIsNone(result.draft_event)
