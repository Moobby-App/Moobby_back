"""Configured artifact loading and legacy package compatibility, without services."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import joblib
import sklearn

from mooby.worker.legacy_classifier import ClassifierAdapter


class StubEstimator:
    def __init__(self, classes):
        self.classes_ = classes

    def predict(self, texts):
        return [self.classes_[0] for _ in texts]

    def predict_proba(self, texts):
        return [[1.0] + [0.0] * (len(self.classes_) - 1) for _ in texts]


class ClassifierConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        for name, classes in [
            ('relevance_tfidf_logreg.joblib', ['disruption', 'not_disruption']),
            ('severity_tfidf_logreg.joblib', ['light', 'moderate', 'severe']),
        ]:
            joblib.dump(StubEstimator(classes), self.directory / name)
        self.metrics = {'experiment': 'legacy', 'scikit_learn_version': sklearn.__version__}

    def write_metrics(self):
        (self.directory / 'metrics.json').write_text(json.dumps(self.metrics))

    def test_explicit_legacy_directory_is_compatible(self):
        self.write_metrics()
        adapter = ClassifierAdapter.load(self.directory)
        self.assertEqual(adapter.predict_relevance('sample').model_version, 'legacy:relevance')
        self.assertEqual(adapter.predict_severity('sample').label, 'light')

    def test_default_directory_uses_configured_svm_filename_and_versions(self):
        target = self.directory / 'relevance_tfidf_svm_sigmoid.joblib'
        (self.directory / 'relevance_tfidf_logreg.joblib').rename(target)
        self.metrics.update({
            'model_files': {'relevance': target.name},
            'model_versions': {'relevance': 'svm:v1', 'severity': 'original:severity'},
            'artifact_sha256': {'relevance': hashlib.sha256(target.read_bytes()).hexdigest()},
        })
        self.write_metrics()
        with patch('mooby.worker.legacy_classifier.ACTIVE_CLASSIFIER_ARTIFACT_DIR', self.directory):
            adapter = ClassifierAdapter.load()
        self.assertEqual(adapter.predict_relevance('sample').model_version, 'svm:v1')
        self.assertEqual(adapter.predict_severity('sample').model_version, 'original:severity')

    def test_checksum_mismatch_rejects_package(self):
        self.metrics['artifact_sha256'] = {'relevance': 'incorrect'}
        self.write_metrics()
        with patch.object(joblib, 'load', side_effect=AssertionError('must not deserialize')):
            adapter = ClassifierAdapter.load(self.directory)
        self.assertEqual(adapter.artifact_manifest['relevance']['status'], 'unavailable')

    def test_runtime_version_mismatch_rejects_package(self):
        self.metrics['scikit_learn_version'] = 'wrong-version'
        self.write_metrics()
        adapter = ClassifierAdapter.load(self.directory)
        self.assertEqual(adapter.artifact_manifest['relevance']['error_type'], 'RuntimeError')

    def test_filename_escape_rejected(self):
        self.metrics['model_files'] = {'relevance': '../outside.joblib'}
        self.write_metrics()
        adapter = ClassifierAdapter.load(self.directory)
        self.assertEqual(adapter.artifact_manifest['relevance']['status'], 'unavailable')


if __name__ == '__main__':
    unittest.main()
