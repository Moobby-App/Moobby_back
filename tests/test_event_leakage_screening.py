"""Local rule tests and actual immutable snapshot verification; no model fitting."""
import ast
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "evaluation"))
import event_leakage_screening as audit


def example(text, raw_date="Oct 23, 2026", tweet_id="a"):
    return audit.extract_evidence({"tweet_id": tweet_id, "partition": "train",
                                   "full_text": text, "date": raw_date}, (
        ("Tanjung Barat", audit.re.compile(r"tanjung barat")),
        ("Tambun", audit.re.compile(r"tambun")),
    ))


class RuleTests(unittest.TestCase):
    def test_complete_date(self):
        self.assertEqual(audit.parse_audit_date("Oct 23, 2026"),
                         dict(precision="day", year=2026, month=10, day=23))

    def test_partial_date_never_gets_year(self):
        self.assertIsNone(audit.parse_audit_date("May 12")["year"])
        self.assertEqual(audit.date_relation([audit.parse_audit_date("May 12")] * 2),
                         "partial_dates_compatible_year_or_day_unproven")

    def test_month_year_not_fabricated_day(self):
        value = audit.parse_audit_date("Nov 2026 (X search display)")
        self.assertEqual((value["precision"], value["day"]), ("month", None))

    def test_relative_dates_not_anchored(self):
        for value in ("2 hours ago", "5h", "47 minutes ago", "2 jam lalu"):
            with self.subTest(value=value):
                self.assertEqual(audit.parse_audit_date(value)["precision"], "relative")

    def test_unknown_and_invalid_dates_remain_unknown(self):
        for value in ("not shown in timeline", "", "Feb 30, 2026", "2026-13-01"):
            with self.subTest(value=value):
                self.assertEqual(audit.parse_audit_date(value)["precision"], "unknown")

    def test_calendar_conflict_and_partial_compatibility(self):
        self.assertEqual(audit.date_relation([audit.parse_audit_date("May 12"),
                                             audit.parse_audit_date("May 19")]),
                         "conflicting_calendar_dates")
        self.assertNotEqual(audit.date_relation([audit.parse_audit_date("May 12"),
                                                audit.parse_audit_date("May 12, 2026")]),
                            "same_full_date")

    def test_train_number_location_cause_and_excerpt(self):
        e = example("Mohon maaf KA 1382 mengalami gangguan sarana di Stasiun Tanjung Barat.")
        self.assertEqual(e["train_numbers"], ["1382"])
        self.assertIn("Tanjung Barat", e["incident_locations"])
        self.assertIn("rolling_stock", e["causes"])
        self.assertLessEqual(len(e["excerpt"]), 160)

    def test_similarity_not_an_identity_input(self):
        e = [example("Mohon maaf perjalanan kereta mengalami keterlambatan hari ini.")] * 2
        category, *_ = audit.classify_evidence(e, ["disruption"], ["light"], ["train", "test"])
        self.assertNotEqual(category, "A")

    def test_strong_independent_signals_only_propose_A(self):
        e = [example("KA 1382 mengalami gangguan sarana di Stasiun Tanjung Barat dan dalam penanganan petugas saat ini.")] * 2
        category, flags, *_ = audit.classify_evidence(e, ["disruption"], ["light"], ["train", "test"])
        self.assertEqual(category, "A")
        self.assertIn("high_priority", flags)
        self.assertNotIn("confirmed", category.lower())

    def test_incomplete_dates_prevent_A(self):
        text = "KA 1382 mengalami gangguan sarana di Stasiun Tanjung Barat dan dalam penanganan petugas saat ini."
        e = [example(text, "Oct 23")] * 2
        category, flags, *_ = audit.classify_evidence(e, ["disruption"], ["light"], ["train", "validation"])
        self.assertEqual(category, "C")
        self.assertIn("ambiguous_or_conflicting_dates", flags)

    def test_template_and_differing_details_B_plus_D(self):
        e = [example("Selamat pagi mohon maaf KA 1382 mengalami gangguan di Tanjung Barat.", "May 12"),
             example("Selamat pagi mohon maaf KA 1507 mengalami gangguan di Tambun.", "May 19")]
        category, flags, *_ = audit.classify_evidence(e, ["disruption"], ["moderate"], ["train", "test"])
        self.assertEqual(category, "B")
        self.assertIn("different_train_numbers", flags)
        self.assertIn("high_priority", flags)

    def test_labels_are_risk_flags_not_auto_corrections(self):
        e = [example("KA 1382 mengalami gangguan sarana di Stasiun Tanjung Barat dan dalam penanganan petugas saat ini.")] * 2
        _, flags, *_ = audit.classify_evidence(e, ["disruption", "not_disruption"],
                                              ["light", "severe"], ["train", "validation"])
        self.assertIn("mixed_relevance_labels", flags)
        self.assertIn("mixed_disruption_severity_labels", flags)
        self.assertIn("includes_severe", flags)

    def test_no_fitting_splitting_clustering_or_external_services(self):
        tree = ast.parse(Path(audit.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                self.assertFalse(any(x.name.startswith(("sklearn", "requests", "multiprocessing")) for x in node.names))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                # str.split() is whitespace inspection, not dataset splitting.
                self.assertNotIn(node.func.attr, {"fit", "fit_transform", "drop_duplicates", "connect"})
                if node.func.attr == "split" and isinstance(node.func.value, ast.Name):
                    self.assertNotIn(node.func.value.id, {"splitter", "cv", "model"})


class SnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "evaluation/private/leakage_audit/existing_audit_state_8d1f8f26cb784880b1af72e392bb2d58.json"
        if not cls.path.is_file():
            raise unittest.SkipTest("Private audit snapshot not available")
        cls.sha = hashlib.sha256(cls.path.read_bytes()).hexdigest()
        cls.state = audit.load_snapshot(cls.path)
        cls.locations = audit.load_location_rules(ROOT / "evaluation/reference/station_alias_review.csv")

    def test_actual_snapshot_full_accounting(self):
        audit.validate_state(self.state)
        self.assertEqual(self.state.df_model.index.tolist(), list(range(1311)))
        self.assertEqual(self.state.df_model.tweet_id.iloc[0], "1683656326646497281")

    def test_restored_record_values_and_order_equal_original_json(self):
        raw = json.loads(self.path.read_text())
        for field, frame in (("df_model", self.state.df_model), ("audit_manifest", self.state.audit_manifest)):
            for original, restored in zip(raw[field]["data"], frame.to_dict("records")):
                for column in frame.columns:
                    if column != "parsed_date":
                        self.assertEqual(original[column], restored[column])
                    elif original[column] is None:
                        self.assertTrue(pd.isna(restored[column]))
                    else:
                        self.assertEqual(pd.Timestamp(original[column]), restored[column])

    def test_actual_reproducibility_and_all_input_immutability(self):
        state = self.state
        frames = [f.copy(deep=True) for f in (state.df_model, state.cluster_summary, state.audit_manifest)]
        edges, clusters = deepcopy(state.event_candidates), deepcopy(state.candidate_clusters)
        first = audit.screen_state(state, self.locations)
        second = audit.screen_state(state, self.locations)
        for left, right in zip(first[:3], second[:3]):
            pd.testing.assert_frame_equal(left, right)
        self.assertEqual(first[3], second[3])
        for original, after in zip(frames, (state.df_model, state.cluster_summary, state.audit_manifest)):
            pd.testing.assert_frame_equal(original, after)
        self.assertEqual(edges, state.event_candidates)
        self.assertEqual(clusters, state.candidate_clusters)
        self.assertEqual(self.sha, hashlib.sha256(self.path.read_bytes()).hexdigest())

    def test_confirmations_and_prior_uncertainty_not_overwritten(self):
        report, queue, decisions, summary = audit.screen_state(self.state, self.locations)
        self.assertEqual((summary["confirmed_reviewed_events"], summary["confirmed_reviewed_posts"]), (6, 43))
        self.assertEqual(summary["never_manually_reviewed_clusters"], 28)
        self.assertEqual(summary["clusters_without_confirmed_identity"], 29)
        row = report.set_index("cluster_key").loc[audit.UNCERTAIN_KEY]
        self.assertEqual((row.review_status, row.screening_category), ("manually_reviewed_event_uncertain", "C"))
        self.assertTrue(decisions.proposed_event_identity.isna().all())
        self.assertEqual(len(decisions), 29)
        self.assertFalse(queue.review_status.eq("same_event_high_confidence").any())

    def test_invalid_pair_or_duplicate_pair_rejected(self):
        pairs = list(self.state.event_candidates)
        pairs[0] = (0, 99999, .8)
        with self.assertRaises(ValueError):
            audit.validate_state(replace(self.state, event_candidates=tuple(pairs)))
        pairs[0] = pairs[1]
        with self.assertRaises(ValueError):
            audit.validate_state(replace(self.state, event_candidates=tuple(pairs)))

    def test_reordered_membership_rejected(self):
        changed = self.state.df_model.iloc[::-1].copy()
        with self.assertRaises(ValueError):
            audit.validate_state(replace(self.state, df_model=changed))

    def test_changed_labels_partitions_or_manifest_rejected(self):
        for column, value in (("partition", "test"), ("disruption_label", "not_disruption")):
            frame = self.state.df_model.copy(deep=True)
            frame.loc[0, column] = value
            with self.assertRaises(ValueError):
                audit.validate_state(replace(self.state, df_model=frame))
        manifest = self.state.audit_manifest.copy(deep=True)
        manifest.loc[0, "event_id"] = "CONTRADICTORY_EVENT"
        with self.assertRaises(ValueError):
            audit.validate_state(replace(self.state, audit_manifest=manifest))

    def test_exclusive_outputs_never_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            result = audit.audit_snapshot(self.path)
            first = audit.write_results(result, directory)
            before = (first / "summary.json").read_bytes()
            second = audit.write_results(result, directory)
            self.assertNotEqual(first, second)
            self.assertEqual(before, (first / "summary.json").read_bytes())

    def test_new_notebook_section_uses_frozen_state_without_running_old_cells(self):
        notebook = json.loads((ROOT / "evaluation/retrain.ipynb").read_text())
        cells = notebook["cells"][91:]
        self.assertTrue(cells)
        namespace = {
            "df_model": self.state.df_model.copy(deep=True),
            "audit_manifest": self.state.audit_manifest.copy(deep=True),
            "event_candidates": list(self.state.event_candidates),
            "candidate_clusters": deepcopy(self.state.candidate_clusters),
            "cluster_summary": self.state.cluster_summary.copy(deep=True),
            "display": lambda value: None,
        }
        with patch.object(audit, "write_results", return_value=Path("isolated-test-output")), patch("builtins.print"):
            for cell in cells:
                if cell["cell_type"] == "code":
                    exec(compile("".join(cell["source"]), "<new-audit-section>", "exec"), namespace)
        self.assertEqual(len(namespace["leakage_screening_report"]), 35)
        self.assertEqual(len(namespace["candidate_annotation_table"]), 29)
        self.assertEqual(len(namespace["audit_manifest"]), 43)
        pd.testing.assert_frame_equal(namespace["df_model"], self.state.df_model)
        pd.testing.assert_frame_equal(namespace["audit_manifest"], self.state.audit_manifest)


if __name__ == "__main__":
    unittest.main()
