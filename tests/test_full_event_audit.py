"""Full snapshot audit: identity safeguards, date validation, reproducibility."""
from copy import deepcopy
from pathlib import Path
import hashlib
import json
import sys
import tempfile
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'evaluation'))
import full_event_audit as audit


class RuleTests(unittest.TestCase):
    def test_known_snowflake(self):
        self.assertEqual(audit.snowflake('1501259319806889986').isoformat(), '2022-03-08T18:11:28.548000+00:00')
        self.assertIsNone(audit.snowflake('invalid'))

    def test_substation_is_not_two_incident_locations(self):
        import re
        rules = [('Bekasi', re.compile(r'\bbekasi\b')), ('Bekasi Timur', re.compile(r'\bbekasi timur\b'))]
        record = dict(tweet_id='1501259319806889986', date='Mar 9, 2022', partition='train',
                      source_url='https://x.com/example/status/1501259319806889986',
                      full_text='KRL tertahan karena gangguan sinyal di Stasiun Bekasi Timur saat ini.')
        features, _ = audit.enrich([record], rules)
        self.assertEqual(features[0]['locations'], ['Bekasi Timur'])
        self.assertEqual(features[0]['incident_locations'], ['Bekasi Timur'])

    def test_stable_id_order_invariant(self):
        self.assertEqual(audit.stable_id('AUTO_', ['b','a']), audit.stable_id('AUTO_', ['a','b']))

    def test_generic_signal_wait_not_confirmed(self):
        record = dict(tweet_id='1501259319806889986', date='Mar 9, 2022', partition='train',
                      source_url='https://x.com/example/status/1501259319806889986',
                      full_text='Mohon maaf kereta menunggu sinyal aman masuk stasiun dan dalam antrean.')
        features, _ = audit.enrich([record,record], [])
        relation, _, _ = audit.pair_relation(*features)
        self.assertNotEqual(relation,'SAME_EVENT')

    def test_different_train_numbers_can_share_one_incident(self):
        rules=[('Bojong Gede',audit.re.compile('bojong gede')),('Citayam',audit.re.compile('citayam'))]
        base=dict(tweet_id='1501259319806889986',date='Mar 9, 2022',partition='train',
                  source_url='https://x.com/example/status/1501259319806889986')
        records=[dict(base,full_text=f'KA {number} tertahan akibat banjir genangan air di antara Bojong Gede dan Citayam saat ini.')
                 for number in ('1234','5678')]
        features,_=audit.enrich(records,rules)
        self.assertEqual(audit.pair_relation(*features)[0],'SAME_EVENT')


class FullSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / 'evaluation/private/leakage_audit/existing_audit_state_8d1f8f26cb784880b1af72e392bb2d58.json'
        if not cls.path.exists():raise unittest.SkipTest('Private snapshot unavailable')
        cls.sha = hashlib.sha256(cls.path.read_bytes()).hexdigest()
        cls.state = audit.load_snapshot(cls.path)
        cls.frames = (cls.state.df_model.copy(deep=True), cls.state.audit_manifest.copy(deep=True))
        cls.rules = audit.load_location_rules(ROOT / 'evaluation/reference/station_alias_review.csv')
        cls.first = audit.run_audit(cls.state, cls.rules)
        cls.second = audit.run_audit(cls.state, cls.rules)

    def test_all_records_order_labels_and_partitions(self):
        manifest = self.first[0]; df = self.state.df_model
        self.assertEqual(len(manifest),1311)
        self.assertTrue(manifest.tweet_id.is_unique)
        self.assertEqual(manifest.tweet_id.tolist(),df.tweet_id.tolist())
        for a,b in [('original_partition','partition'),('original_relevance','disruption_label'),('original_severity','severity')]:
            self.assertEqual(manifest[a].tolist(),df[b].tolist())
        self.assertEqual(self.first[-1]['all_record_pairs_considered'],858705)

    def test_manual_assignments_preserved(self):
        m=self.first[0].set_index('tweet_id')
        for r in self.state.audit_manifest.to_dict('records'):
            self.assertEqual(m.loc[r['tweet_id'],'event_id'],r['event_id'])
            self.assertEqual(m.loc[r['tweet_id'],'event_identity_confidence'],'prior_human_verified')

    def test_no_input_mutations(self):
        pd.testing.assert_frame_equal(self.frames[0],self.state.df_model)
        pd.testing.assert_frame_equal(self.frames[1],self.state.audit_manifest)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(),self.sha)

    def test_deterministic(self):
        for i in range(3):pd.testing.assert_frame_equal(self.first[i],self.second[i])
        self.assertEqual(self.first[3:],self.second[3:])

    def test_unresolved_not_assigned_event_or_verified(self):
        subset=self.first[0].loc[self.first[0].event_identity_status.eq('UNRESOLVED')]
        self.assertTrue(subset.event_id.eq('').all())
        self.assertTrue(subset.event_identity_confidence.eq('unresolved').all())

    def test_prior_uncertain_cluster_retained(self):
        subset=self.first[0].loc[self.first[0].prior_reviewed_uncertain]
        self.assertEqual(len(subset),7)
        self.assertTrue(subset.event_identity_status.eq('UNRESOLVED').all())
        self.assertTrue(subset.event_id.eq('').all())

    def test_all_cross_partition_event_ids_reported_once(self):
        manifest,leaks=self.first[:2]
        ids=manifest.loc[manifest.event_id.ne('')].groupby('event_id').original_partition.nunique()
        self.assertEqual(set(ids[ids>1].index),set(leaks.event_id))
        self.assertTrue(leaks.event_id.is_unique)

    def test_dates_and_conflicting_search_metadata(self):
        validation=self.first[-1]['date_validation']
        self.assertEqual(validation['complete_date_matches_wib'],433)
        self.assertEqual(validation['month_day_matches'],424)
        self.assertEqual(validation['month_conflicts'],189)
        manifest=self.first[0]
        conflicts=manifest.date_resolution_method.eq('snowflake_url_id_conflicting_search_month')
        self.assertEqual(int(conflicts.sum()),189)
        self.assertTrue(manifest.loc[conflicts,'original_date'].eq('Nov 2026 (X search display)').all())

    def test_exports_and_proposal_do_not_resplit(self):
        with tempfile.TemporaryDirectory() as tmp:
            output,summary=audit.save_audit(self.state,deepcopy(self.first),tmp)
            for name in ['full_event_identity_manifest.csv','event_leakage_report.csv','template_overlap_report.csv',
                         'event_audit_summary.md','event_aware_split_proposal.md']:
                self.assertTrue((output/name).is_file())
            saved=pd.read_csv(output/'full_event_identity_manifest.csv',dtype=str,keep_default_na=False)
            self.assertEqual(saved.original_partition.tolist(),self.state.df_model.partition.tolist())
            pd.testing.assert_frame_equal(self.frames[0],self.state.df_model)


if __name__=='__main__':unittest.main()
