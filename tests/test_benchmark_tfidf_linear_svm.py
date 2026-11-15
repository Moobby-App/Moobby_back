"""Readiness safeguards: no estimator fitting and ID-based assignment stability."""
import unittest
from unittest.mock import patch
from evaluation import benchmark_tfidf_linear_svm as benchmark


class BenchmarkReadinessTests(unittest.TestCase):
    def test_preparation_cannot_fit_estimators(self):
        with patch.object(benchmark.TfidfVectorizer, 'fit', side_effect=AssertionError('fit forbidden')), \
             patch.object(benchmark.TfidfVectorizer, 'fit_transform', side_effect=AssertionError('fit_transform forbidden')), \
             patch.object(benchmark.LinearSVC, 'fit', side_effect=AssertionError('fit forbidden')):
            split, _, report = benchmark.load_data()
            for config in benchmark.experiment_configs():
                estimator = benchmark.build_pipeline(config)
                self.assertFalse(hasattr(estimator['tfidf'], 'vocabulary_'))
                self.assertFalse(hasattr(estimator['svm'], 'classes_'))
        self.assertEqual(report['unique_tweet_ids'], 1311)
        self.assertEqual(sum(len(d) for d in split.values()), 1311)

    def test_assignment_row_order_does_not_change_membership(self):
        baseline, _, _ = benchmark.load_data()
        original_read = benchmark.pd.read_csv
        def reversed_assignment(path, **kwargs):
            result = original_read(path, **kwargs)
            if str(path).endswith('final_split_assignments.csv'):
                return result.iloc[::-1].reset_index(drop=True)
            return result
        with patch.object(benchmark.pd, 'read_csv', side_effect=reversed_assignment):
            reordered, _, _ = benchmark.load_data()
        for partition in benchmark.PARTITIONS:
            self.assertEqual(set(baseline[partition]['tweet_id']), set(reordered[partition]['tweet_id']))

    def test_duplicate_assignment_id_is_rejected(self):
        original_read = benchmark.pd.read_csv
        def invalid_assignment(path, **kwargs):
            result = original_read(path, **kwargs)
            if str(path).endswith('final_split_assignments.csv'):
                result.loc[1, 'tweet_id'] = result.loc[0, 'tweet_id']
            return result
        with patch.object(benchmark.pd, 'read_csv', side_effect=invalid_assignment):
            with self.assertRaisesRegex(ValueError, 'Duplicate assignment IDs'):
                benchmark.load_data()

    def test_historical_model_text_drift_is_rejected(self):
        original_read = benchmark.pd.read_csv
        def altered_source(path, **kwargs):
            result = original_read(path, **kwargs)
            if str(path).endswith('krl_tweets_clean.csv'):
                result.loc[0, 'full_text'] += ' changed test fixture'
            return result
        with patch.object(benchmark.pd, 'read_csv', side_effect=altered_source):
            with self.assertRaisesRegex(ValueError, 'Historical mismatch'):
                benchmark.load_data()


if __name__ == '__main__':
    unittest.main()
