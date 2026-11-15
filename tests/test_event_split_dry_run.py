"""Dry-run allocation checks; never run original split code or train models."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'evaluation'))
import event_split_dry_run as dry


class DryRunTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory=ROOT/'evaluation/private/leakage_audit/full_event_audit_642696a4953c443dbb5f9fb16609d48b'
        cls.snapshot=ROOT/'evaluation/private/leakage_audit/existing_audit_state_8d1f8f26cb784880b1af72e392bb2d58.json'
        if not cls.directory.exists():raise unittest.SkipTest('Private completed audit unavailable')
        cls.inputs=dry.load_inputs(cls.directory,cls.snapshot)
        cls.original=cls.inputs.manifest.copy(deep=True)
        cls.original_manual=cls.inputs.original_manifest.copy(deep=True)
        cls.original_relations=deepcopy(cls.inputs.relations)
        cls.candidates,cls.comparison=dry.propose(cls.inputs)

    def test_all_records_and_baseline_preserved(self):
        self.assertEqual(len(self.original),1311)
        self.assertTrue(self.original.tweet_id.is_unique)
        pd.testing.assert_frame_equal(self.inputs.manifest,self.original)
        pd.testing.assert_frame_equal(self.inputs.original_manifest,self.original_manual)
        self.assertEqual(self.inputs.relations,self.original_relations)
        for p,h in self.inputs.hashes.items():self.assertEqual(dry.file_hash(p),h)

    def test_all_eight_overlaps_eliminated_without_new_group_leaks(self):
        self.assertEqual(len(self.candidates),5)
        for values in self.candidates.values():
            frame=self.original.assign(proposed=values)
            self.assertTrue(frame.loc[frame.event_id.ne('')].groupby('event_id').proposed.nunique().eq(1).all())
            self.assertTrue(frame.groupby('original_similarity_group').proposed.nunique().eq(1).all())
            self.assertEqual(len(values),1311)
            self.assertEqual(set(values),set(dry.PARTS))

    def test_balanced_candidates_exact_original_class_counts(self):
        classes,expected=dry.targets(self.inputs)
        for name,values in self.candidates.items():
            if name.startswith(('C2','C3','C5')):
                actual=np.array([[sum((values==p)&(classes==c)) for c in dry.CLASSES] for p in dry.PARTS])
                np.testing.assert_array_equal(actual,expected)

    def test_severe_event_coverage_guard(self):
        for name,values in self.candidates.items():
            if not name.startswith(('C3','C5')):continue
            frame=self.original.assign(proposed=values)
            for p in dry.PARTS:
                selected=frame.loc[frame.proposed.eq(p)&frame.original_severity.eq('severe')&frame.event_id.ne('')]
                self.assertGreaterEqual(selected.event_id.nunique(),1)

    def test_specific_and_all_risk_blocks_keep_edges_without_new_event_ids(self):
        positions={t:i for i,t in enumerate(self.original.tweet_id)}
        self.assertEqual(len(dry.unresolved_edges(self.inputs,'specific')),73)
        self.assertEqual(len(dry.unresolved_edges(self.inputs,'all')),1090)
        for name,tier in [('C3_SPECIFIC_RISK_AND_SEVERE_COVERAGE','specific'),('C4_ALL_UNRESOLVED_ENVELOPE','all')]:
            values=self.candidates[name]
            for r in dry.unresolved_edges(self.inputs,tier):
                self.assertEqual(values[positions[r['tweet_id_a']]],values[positions[r['tweet_id_b']]])
        pd.testing.assert_frame_equal(self.inputs.manifest,self.original)

    def test_minimum_movement_lower_bound(self):
        blocks=dry.allocation_blocks(self.inputs,'none')
        lower=sum(len(b)-self.original.iloc[b].original_partition.value_counts().max() for b in blocks)
        value=self.comparison.set_index('candidate').loc['C1_MINIMUM_MOVEMENT','moved_records']
        self.assertEqual(value,lower)

    def test_reproducible_solver_and_candidate_assignments(self):
        again,comparison=dry.propose(self.inputs)
        for name in self.candidates:np.testing.assert_array_equal(self.candidates[name],again[name])
        pd.testing.assert_frame_equal(self.comparison,comparison)

    def test_reported_unresolved_and_new_risks_are_real_counts(self):
        for name,values in self.candidates.items():
            mapping=dict(zip(self.original.tweet_id,values))
            actual=sum(r['relation']=='UNRESOLVED' and mapping[r['tweet_id_a']]!=mapping[r['tweet_id_b']]
                       for r in self.inputs.relations)
            row=self.comparison.set_index('candidate').loc[name]
            self.assertEqual(row['unresolved_crossing_edges'],actual)

    def test_exports_are_dry_run_and_non_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            one=dry.write_proposal(self.inputs,self.candidates,self.comparison,directory)
            baseline_bytes=(one/'DRY_RUN_candidate_assignments.csv').read_bytes()
            two=dry.write_proposal(self.inputs,self.candidates,self.comparison,directory)
            self.assertNotEqual(one,two)
            self.assertEqual((one/'DRY_RUN_candidate_assignments.csv').read_bytes(),baseline_bytes)
            saved=pd.read_csv(one/'DRY_RUN_candidate_assignments.csv',dtype=str,keep_default_na=False)
            self.assertEqual(saved.original_partition.tolist(),self.original.original_partition.tolist())
            self.assertEqual(saved.original_relevance.tolist(),self.original.original_relevance.tolist())
            self.assertEqual(saved.original_severity.tolist(),self.original.original_severity.tolist())
            self.assertEqual(saved.event_id.tolist(),self.original.event_id.tolist())
            self.assertEqual(saved.event_identity_confidence.tolist(),self.original.event_identity_confidence.tolist())


if __name__=='__main__':unittest.main()
