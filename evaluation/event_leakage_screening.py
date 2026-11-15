"""Deterministic audit of existing state; no fitting, splitting or clustering.

All automated categories are hypotheses. Only the supplied manual manifest can
confer confirmed status. Inputs are copied/read, never annotated in place.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from dataclasses import dataclass
from datetime import date
import hashlib
import io
import json
import math
from pathlib import Path
import re
import unicodedata
from uuid import uuid4

import pandas as pd


RULE_VERSION = "moobby-event-screening-v1"
UNCERTAIN_KEY = "2054010080199352486"
CAUSE_RULES = {
    "laa": r"\blaa\b|listrik aliran atas",
    "flooding": r"\bbanjir\b|genangan air",
    "signal": r"\bsinyal\b|persinyalan",
    "power": r"gangguan listrik|pasokan listrik",
    "switch": r"\bwesel\b",
    "switch_over": r"switch[\s-]*over|peralihan prasarana",
    "infrastructure": r"kerusakan (?:prasarana|rel|jembatan)|perbaikan (?:rel|jembatan)",
    "vehicle_obstruction": r"\btruk\b|\bmobil\b|kendaraan.*(?:mogok|melintang)",
    "collision": r"tertemper|tertabrak|menabrak|tabrakan",
    "rolling_stock": r"gangguan (?:pada )?(?:sarana|rangkaian|ka\b)|tukar rangkaian",
    "crowd": r"kerumunan|keramaian|massa di|warga.*(?:rel|jalur)",
    "fire": r"kebakaran",
}
TEMPLATE_PHRASES = (
    "selamat pagi", "selamat siang", "selamat sore", "selamat malam",
    "mohon maaf", "ketidaknyamanannya", "informasi terbaru", "cek secara berkala",
    "timeline media sosial", "aplikasi c-access", "tks", "kami informasikan",
)
MONTHS = {v: i for i, names in enumerate((
    ("jan", "january"), ("feb", "february"), ("mar", "march"),
    ("apr", "april"), ("may", "mei"), ("jun", "june"),
    ("jul", "july"), ("aug", "august", "agu"),
    ("sep", "september"), ("oct", "october", "okt"),
    ("nov", "november"), ("dec", "december", "des"),
), 1) for v in names}
EXPECTED_EVENTS = {
    "E001_LAA_BOJONGGEDE_CITAYAM": {"train": 4, "test": 7},
    "E002_BANJIR_KAMPUNGBANDAN": {"train": 4, "validation": 5, "test": 1},
    "E003_TRUK_TAMBUN_BEKASITIMUR": {"validation": 4, "test": 1},
    "E004_WESEL_JATINEGARA": {"train": 2, "validation": 4},
    "E005_SWITCHOVER_TAMBUN": {"train": 4, "test": 1},
    "E006_KA1382_TANJUNGBARAT": {"train": 5, "validation": 1},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def normalized(value):
    return " ".join(unicodedata.normalize("NFKC", str(value)).casefold().split())


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False).encode()).hexdigest()


def frame_digest(frame):
    return hashlib.sha256(frame.to_json(orient="table", date_format="iso").encode()).hexdigest()


@dataclass(frozen=True)
class AuditState:
    df_model: pd.DataFrame
    event_candidates: tuple
    candidate_clusters: dict
    cluster_summary: pd.DataFrame
    audit_manifest: pd.DataFrame
    snapshot_sha256: str


def load_snapshot(path):
    """Restore pandas table schema/order; never derive or reassign membership."""
    payload = Path(path).read_bytes()
    raw = json.loads(payload)
    require(raw.get("snapshot_version") == 1, "Unsupported snapshot version")
    frames = {}
    for name in ("df_model", "cluster_summary", "audit_manifest"):
        table = raw[name]
        frame = pd.read_json(io.StringIO(json.dumps(table)), orient="table")
        require(len(frame) == len(table["data"]), f"Lost rows restoring {name}")
        # Critical: keep positional indices, schema and ordered IDs, not sorted IDs.
        if name != "cluster_summary":
            require(frame["tweet_id"].tolist() == [r["tweet_id"] for r in table["data"]],
                    f"Record order changed restoring {name}")
        frames[name] = frame
    state = AuditState(frames["df_model"], tuple(tuple(p) for p in raw["event_candidates"]),
                       {str(k): tuple(v) for k, v in raw["candidate_clusters"].items()},
                       frames["cluster_summary"], frames["audit_manifest"],
                       hashlib.sha256(payload).hexdigest())
    validate_state(state)
    return state


def validate_state(state):
    df, manifest = state.df_model, state.audit_manifest
    require(len(df) == 1311 and df["tweet_id"].is_unique, "Expected 1311 unique records")
    require(df[["tweet_id", "partition", "similarity_group", "full_text", "model_text",
                "disruption_label", "severity"]].notna().all().all(), "Missing essential snapshot values")
    require(df.tweet_id.map(lambda value: isinstance(value, str) and bool(value.strip())).all(),
            "Record IDs must be nonempty strings")
    require(df.loc[df.disruption_label.eq("not_disruption"), "severity"].eq("").all(),
            "Non-disruption severity should remain not applicable")
    require(df.index.tolist() == list(range(len(df))), "Unexpected original row index")
    require(df["partition"].value_counts().to_dict() ==
            {"train": 786, "validation": 262, "test": 263}, "Partition totals mismatch")
    require(df["disruption_label"].value_counts().to_dict() ==
            {"disruption": 559, "not_disruption": 752}, "Relevance totals mismatch")
    require(df.loc[df.disruption_label.eq("disruption"), "severity"].value_counts().to_dict()
            == {"light": 342, "moderate": 151, "severe": 66}, "Severity totals mismatch")
    require(df["model_text"].is_unique, "Existing normalized texts not unique")
    require(df.similarity_group.nunique() == 1231 and
            df.groupby("similarity_group").partition.nunique().eq(1).all(),
            "Existing similarity group separation mismatch")
    require(len(state.event_candidates) == 124, "Expected 124 original pairs")
    owners, members = {}, set()
    summaries = state.cluster_summary.set_index("cluster_key")
    require(len(summaries) == 35 and summaries.index.is_unique, "Expected 35 summaries")
    require(len(state.candidate_clusters) == 35, "Expected 35 clusters")
    for root, indices in state.candidate_clusters.items():
        require(len(indices) >= 2 and len(indices) == len(set(indices)), "Invalid cluster members")
        for pos in indices:
            require(type(pos) is int and 0 <= pos < len(df), "Out-of-bounds cluster position")
            require(pos not in owners, "Post belongs to multiple candidate clusters")
            owners[pos] = root
            members.add(pos)
        key = min(df.iloc[list(indices)].tweet_id)
        require(key in summaries.index and int(summaries.loc[key, "posts"]) == len(indices),
                "Cluster identity/count differs from saved summary")
    require(len(members) == 120, "Expected 120 original involved posts")
    seen, edge_counts, graph = set(), Counter(), {i: set() for i in members}
    for i, j, score in state.event_candidates:
        require(type(i) is int and type(j) is int and 0 <= i < j < len(df), "Invalid pair positions")
        require(type(score) in (int, float) and math.isfinite(score) and .75 <= score <= 1.0000001,
                "Invalid saved pair score")
        require((i, j) not in seen, "Duplicate candidate edge")
        seen.add((i, j))
        require(i in owners and j in owners and owners[i] == owners[j], "Pair/cluster mismatch")
        require(df.iloc[i].partition != df.iloc[j].partition and
                df.iloc[i].similarity_group != df.iloc[j].similarity_group, "Invalid cross-partition pair")
        edge_counts[owners[i]] += 1
        graph[i].add(j); graph[j].add(i)
    for root, indices in state.candidate_clusters.items():
        # Verify the supplied component, without deriving replacement clusters.
        visited, pending = set(), [indices[0]]
        while pending:
            pos = pending.pop()
            if pos not in visited:
                visited.add(pos); pending.extend(graph[pos] - visited)
        require(visited == set(indices), "Saved cluster not connected by saved edges")
        key = min(df.iloc[list(indices)].tweet_id)
        require(edge_counts[root] == int(summaries.loc[key, "candidate_pairs"]), "Edge total mismatch")
    require(len(manifest) == 43 and manifest.tweet_id.is_unique, "Expected 43 unique reviewed posts")
    require(set(manifest.event_id) == set(EXPECTED_EVENTS), "Confirmed event IDs changed")
    indexed = df.set_index("tweet_id")
    for record in manifest.to_dict("records"):
        require(record["tweet_id"] in indexed.index, "Unknown confirmed record")
        for column in manifest.columns.difference(["tweet_id", "event_id", "review_status"]):
            require(record[column] == indexed.loc[record["tweet_id"], column],
                    f"Manifest/source disagreement: {record['tweet_id']} {column}")
        require(record["review_status"] == "same_event_high_confidence", "Unexpected confirmed status")
    for event_id, expected in EXPECTED_EVENTS.items():
        require(manifest.loc[manifest.event_id.eq(event_id)].partition.value_counts().to_dict()
                == expected, "Confirmed event partition counts changed")
    require(UNCERTAIN_KEY in summaries.index and int(summaries.loc[UNCERTAIN_KEY, "posts"]) == 7,
            "Known reviewed-uncertain cluster missing")


def parse_audit_date(raw):
    """Calendar compatibility only. Never resolve relative time or infer a year."""
    value = normalized(raw)
    match = re.fullmatch(r"([a-z]+) (\d{1,2})(?:,? (\d{4}))?", value)
    if match and match[1] in MONTHS:
        month, day, year = MONTHS[match[1]], int(match[2]), int(match[3]) if match[3] else None
        try:
            date(year if year is not None else 2000, month, day)  # validation only; year stays None
            return {"precision": "day" if year is not None else "month_day",
                    "year": year, "month": month, "day": day}
        except ValueError:
            pass
    match = re.fullmatch(r"([a-z]+) (\d{4})(?: \(x search display\))?", value)
    if match and match[1] in MONTHS:
        return {"precision": "month", "year": int(match[2]), "month": MONTHS[match[1]], "day": None}
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})(?:[t ].*)?", value)
    if match:
        try:
            parsed = date(*map(int, match.groups()))
            return {"precision": "day", "year": parsed.year, "month": parsed.month, "day": parsed.day}
        except ValueError:
            pass
    precision = "relative" if re.search(r"\bago\b|\d+\s*[hm]\b|\d+\s*(?:jam|menit)\b", value) else "unknown"
    return {"precision": precision, "year": None, "month": None, "day": None}


def date_relation(dates):
    conflict = any(any(a[k] is not None and b[k] is not None and a[k] != b[k]
                       for k in ("year", "month", "day"))
                   for i, a in enumerate(dates) for b in dates[i + 1:])
    if conflict:
        return "conflicting_calendar_dates"
    if all(d["precision"] == "day" for d in dates):
        return "same_full_date"
    if any(d["precision"] in {"relative", "unknown"} for d in dates):
        return "insufficient_date_evidence"
    return "partial_dates_compatible_year_or_day_unproven"


def load_location_rules(path):
    """Local reference lexicon only; no geocoding/database/model call."""
    terms = {}
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            canonical = row["canonical_name"]
            aliases = [canonical]
            for field in ("aliases", "curated_aliases"):
                aliases.extend(json.loads(row.get(field) or "[]"))
            for alias in aliases:
                term = normalized(alias)
                if len(term) >= 4:
                    terms[term] = canonical
    return tuple((canonical, re.compile(r"(?<!\w)" + r"\s*".join(map(re.escape, term.split())) + r"(?!\w)"))
                 for term, canonical in sorted(terms.items()))


def extract_evidence(record, location_rules):
    text = normalized(record["full_text"])
    cause_matches = [(name, m) for name, pattern in CAUSE_RULES.items() for m in re.finditer(pattern, text)]
    locations, incident_locations, positions = set(), set(), []
    for name, pattern in location_rules:
        for match in pattern.finditer(text):
            locations.add(name); positions.append(match.start())
            before = text[max(0, match.start() - 40):match.start()]
            if (re.search(r"(?:antara|di stasiun|di lintas)\s*(?:stasiun\s*)?$", before)
                    or any(abs(match.start() - m.start()) <= 80 for _, m in cause_matches)):
                incident_locations.add(name)
    positions.extend(m.start() for _, m in cause_matches)
    start = max(0, min(positions, default=0) - 45)
    return {
        "tweet_id": str(record["tweet_id"]), "partition": record["partition"],
        "raw_date": record["date"], "date_evidence": parse_audit_date(record["date"]),
        "locations": sorted(locations), "incident_locations": sorted(incident_locations),
        "train_numbers": sorted(set(re.findall(r"\bka\s*(?:no\.?\s*)?(\d{3,5}[a-z]?)\b", text))),
        "causes": sorted({name for name, _ in cause_matches}),
        "template_phrases": [phrase for phrase in TEMPLATE_PHRASES if phrase in text],
        "word_count": len(text.split()), "excerpt": text[start:start + 160],
    }


def intersection(evidence, field):
    return sorted(set.intersection(*(set(e[field]) for e in evidence)))


def classify_evidence(evidence, relevance_labels, severity_labels, partitions):
    """Conservative, versioned heuristics; D is orthogonal priority, not identity."""
    relation = date_relation([e["date_evidence"] for e in evidence])
    flags = []
    common_locations = intersection(evidence, "incident_locations")
    common_causes = intersection(evidence, "causes")
    common_trains = intersection(evidence, "train_numbers")
    all_trains = {t for e in evidence for t in e["train_numbers"]}
    generic = sum(len(e["template_phrases"]) >= 2 for e in evidence) / len(evidence) >= .8
    if generic: flags.append("generic_response_template")
    if relation != "same_full_date": flags.append("ambiguous_or_conflicting_dates")
    if relation == "conflicting_calendar_dates": flags.append("calendar_date_conflict")
    if len(all_trains) > 1 and not common_trains: flags.append("different_train_numbers")
    if not common_locations: flags.append("no_shared_incident_location")
    if not common_causes: flags.append("no_shared_cause")
    if min(e["word_count"] for e in evidence) < 10: flags.append("sparse_text")
    if len(relevance_labels) > 1: flags.append("mixed_relevance_labels")
    if len(severity_labels) > 1: flags.append("mixed_disruption_severity_labels")
    if "severe" in severity_labels: flags.append("includes_severe")
    if {"train", "test"}.issubset(partitions): flags.append("train_test_overlap")
    strong = (relation == "same_full_date" and common_locations and common_causes
              and (common_trains or len(common_locations) >= 2)
              and "different_train_numbers" not in flags and "sparse_text" not in flags)
    if strong:
        category = "A"
    elif generic and (relation == "conflicting_calendar_dates" or "different_train_numbers" in flags
                      or (not common_locations and not common_causes)):
        category = "B"
    else:
        category = "C"
    high = any(f in flags for f in (
        "ambiguous_or_conflicting_dates", "mixed_relevance_labels",
        "mixed_disruption_severity_labels", "includes_severe", "train_test_overlap"))
    if high: flags.append("high_priority")
    return category, flags, relation, common_locations, common_causes, common_trains


def screen_state(state, location_rules):
    """Create new reports, preserving every input frame, edge and annotation."""
    before = [frame_digest(f) for f in (state.df_model, state.cluster_summary, state.audit_manifest)]
    graph_before = digest([state.event_candidates, state.candidate_clusters])
    df, manifest = state.df_model, state.audit_manifest
    manual = manifest.set_index("tweet_id")
    rows = []
    for indices in state.candidate_clusters.values():
        subset = df.iloc[sorted(indices)]
        ids = subset.tweet_id.tolist()
        key = min(ids)
        evidence = [extract_evidence(r, location_rules) for r in subset.to_dict("records")]
        relevance = sorted(set(subset.disruption_label))
        severity = sorted(set(subset.loc[subset.disruption_label.eq("disruption"), "severity"]) - {""})
        partitions = sorted(set(subset.partition))
        category, flags, relation, locs, causes, trains = classify_evidence(evidence, relevance, severity, partitions)
        auto_category = category
        covered = [tid for tid in ids if tid in manual.index]
        event_ids = sorted(set(manual.loc[covered, "event_id"])) if covered else []
        confirmed = len(covered) == len(ids) and len(event_ids) == 1
        if confirmed:
            status, category = "same_event_high_confidence", "MANUALLY_CONFIRMED"
        elif covered:
            status = "partially_manually_reviewed"
            flags.extend(["partial_manifest_coverage", "high_priority"])
        elif key == UNCERTAIN_KEY:
            status, category = "manually_reviewed_event_uncertain", "C"
            flags.extend(["prior_manual_template_uncertain", "high_priority"])
        else:
            status = "not_manually_reviewed"
        if len(event_ids) > 1:
            flags.extend(["multiple_confirmed_events_in_cluster", "high_priority"])
        flags = sorted(set(flags))
        edges = [float(score) for i, j, score in state.event_candidates if i in indices and j in indices]
        requires_review = not confirmed and (category != "B" or "high_priority" in flags)
        reasons = [f"calendar={relation}", f"shared incident locations={','.join(locs) or 'none'}",
                   f"shared causes={','.join(causes) or 'none'}", f"shared KA={','.join(trains) or 'none'}"]
        if confirmed: reasons.append("Identity retained from supplied human manifest, not automated evidence")
        if key == UNCERTAIN_KEY: reasons.append("Prior human decision: same template / event uncertain; May 12 vs May 19")
        rows.append({
            "cluster_key": key, "post_count": len(ids), "tweet_ids": ids,
            "partitions": partitions, "raw_dates": sorted(set(subset.date)),
            "date_compatibility": relation,
            "detected_locations": sorted({x for e in evidence for x in e["locations"]}),
            "detected_train_numbers": sorted({x for e in evidence for x in e["train_numbers"]}),
            "detected_causes": sorted({x for e in evidence for x in e["causes"]}),
            "relevance_labels": relevance, "severity_labels": severity,
            "screening_category": category, "automated_category": auto_category,
            "screening_flags": ([category, "D"] if "high_priority" in flags and not confirmed else [category]),
            "risk_flags": flags, "evidence_summary": "; ".join(reasons),
            "requires_manual_review": requires_review, "review_status": status,
            "confirmed_event_ids": event_ids, "reviewed_post_count": len(covered),
            "screened_pair_count": len(edges), "screened_edge_min": min(edges),
            "screened_edge_max": max(edges), "screened_edge_mean": sum(edges) / len(edges),
            "screened_cross_partition_edge_density": len(edges) / (len(ids) * (len(ids) - 1) / 2),
            "post_evidence": evidence,
            "priority_score": (100 * ("includes_severe" in flags) + 60 * ("train_test_overlap" in flags)
                               + 40 * ("mixed_relevance_labels" in flags)
                               + 30 * ("mixed_disruption_severity_labels" in flags)
                               + 20 * ("ambiguous_or_conflicting_dates" in flags) + len(ids)),
        })
    report = pd.DataFrame(rows).sort_values("cluster_key").reset_index(drop=True)
    queue = report.loc[report.requires_manual_review].sort_values(
        ["priority_score", "post_count", "cluster_key"], ascending=[False, False, True]
    ).reset_index(drop=True)
    decisions = report.loc[report.review_status.ne("same_event_high_confidence"),
                           ["cluster_key", "tweet_ids", "screening_category", "evidence_summary", "review_status"]].copy()
    decisions["proposed_event_identity"] = None
    decisions["confidence_or_uncertainty"] = "heuristic_only_not_validated"
    decisions["human_decision"] = decisions.cluster_key.map(
        lambda key: "same_template_event_uncertain" if key == UNCERTAIN_KEY else "not_reviewed")
    decisions["human_evidence_note"] = decisions.cluster_key.map(
        lambda key: "User supplied manual review: May 12/19, differing train numbers" if key == UNCERTAIN_KEY else "")
    require(before == [frame_digest(f) for f in (df, state.cluster_summary, manifest)], "Input frames mutated")
    require(graph_before == digest([state.event_candidates, state.candidate_clusters]), "Pair/cluster input mutated")
    require(report.post_count.sum() == 120 and report.screened_pair_count.sum() == 124, "Output accounting failed")
    summary = {
        "rule_version": RULE_VERSION, "snapshot_sha256": state.snapshot_sha256,
        "dataset_records": len(df), "candidate_pairs": 124, "candidate_clusters": len(report),
        "candidate_posts": int(report.post_count.sum()),
        "confirmed_reviewed_events": int(manifest.event_id.nunique()), "confirmed_reviewed_posts": len(manifest),
        "confirmed_clusters": int(report.screening_category.eq("MANUALLY_CONFIRMED").sum()),
        "clusters_without_confirmed_identity": int(report.screening_category.ne("MANUALLY_CONFIRMED").sum()),
        "previously_reviewed_uncertain_clusters": int(report.review_status.eq("manually_reviewed_event_uncertain").sum()),
        "never_manually_reviewed_clusters": int(report.review_status.eq("not_manually_reviewed").sum()),
        "category_counts": {key: int(report.screening_category.eq(key).sum())
                            for key in ("A", "B", "C", "MANUALLY_CONFIRMED")},
        "high_priority_unconfirmed_clusters": sum("D" in v for v in report.screening_flags),
        "requires_manual_review": len(queue), "review_queue_posts": int(queue.post_count.sum()),
        "uncertain_identity_clusters": int(report.screening_category.eq("C").sum()),
        "unconfirmed_candidate_posts": int(report.loc[report.screening_category.ne("MANUALLY_CONFIRMED"), "post_count"].sum()),
        "input_frames_and_memberships_unchanged": True,
        "ordered_records_sha256": frame_digest(df),
        "partition_assignments_sha256": frame_digest(df[["tweet_id", "partition"]]),
        "confirmed_manifest_sha256": frame_digest(manifest),
        "candidate_pairs_sha256": digest(state.event_candidates),
        "candidate_clusters_sha256": digest(state.candidate_clusters),
    }
    return report, queue, decisions, summary


def audit_snapshot(snapshot_path, location_path=None):
    state = load_snapshot(snapshot_path)
    location_path = location_path or Path(__file__).parent / "reference/station_alias_review.csv"
    report, queue, decisions, summary = screen_state(state, load_location_rules(location_path))
    summary["location_catalog_sha256"] = hashlib.sha256(Path(location_path).read_bytes()).hexdigest()
    summary["screening_code_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    summary["manual_uncertain_decision_provenance"] = "User task brief; cluster 2054010080199352486"
    return {"leakage_screening_report": report, "review_queue": queue,
            "candidate_annotation_table": decisions, "summary": summary}


def write_results(result, directory):
    """New exclusive run directory; never overwrite source or previous output."""
    output = Path(directory) / ("screening_" + uuid4().hex)
    output.mkdir(parents=True, exist_ok=False)
    for name in ("leakage_screening_report", "review_queue", "candidate_annotation_table"):
        with (output / (name + ".json")).open("x", encoding="utf-8") as target:
            target.write(result[name].to_json(orient="records", indent=2, force_ascii=False))
    with (output / "summary.json").open("x", encoding="utf-8") as target:
        json.dump(result["summary"], target, ensure_ascii=False, indent=2)
    summary = result["summary"]
    lines = ["# Phase 2 — Snapshot screening results", "",
             f"Rule version: {RULE_VERSION}",
             f"Snapshot SHA-256: {summary['snapshot_sha256']}", "",
             "Automated categories are hypotheses; original human confirmations are preserved.", "",
             "| Measure | Count |", "|---|---:|"]
    for key in ("dataset_records", "candidate_pairs", "candidate_clusters", "candidate_posts",
                "confirmed_reviewed_events", "confirmed_reviewed_posts",
                "clusters_without_confirmed_identity", "previously_reviewed_uncertain_clusters",
                "never_manually_reviewed_clusters", "requires_manual_review", "review_queue_posts"):
        lines.append(f"| {key} | {summary[key]} |")
    lines.extend(["", "## Category counts", "", "A/B/C are identity hypotheses; D is an overlapping priority flag.", ""])
    for key in ("A", "B", "C", "MANUALLY_CONFIRMED"):
        lines.append(f"- {key}: {summary['category_counts'].get(key, 0)}")
    lines.append(f"- D (unconfirmed priority cases): {summary['high_priority_unconfirmed_clusters']}")
    lines.extend(["", "## Prioritized shortlist", "",
                  "Full evidence/IDs and bounded excerpts are in the JSON files.", "",
                  "| Cluster | Posts | Category | Priority | Reasons |", "|---|---:|---|---:|---|"])
    for record in result["review_queue"].head(12).to_dict("records"):
        lines.append(f"| {record['cluster_key']} | {record['post_count']} | {record['screening_category']} "
                     f"| {record['priority_score']} | {', '.join(record['risk_flags'])} |")
    lines.extend(["", "## Interpretation and safeguards", "",
                  f"{summary['requires_manual_review']} of {summary['clusters_without_confirmed_identity']} unconfirmed clusters require review; deferral is never proof of no leakage.",
                  f"Across unconfirmed clusters: {summary['previously_reviewed_uncertain_clusters']} previously reviewed uncertain and {summary['never_manually_reviewed_clusters']} never reviewed.",
                  f"{summary['category_counts'].get('B', 0)} B proposals suggest templates, but do not prove absence of event overlap.",
                  f"{summary['category_counts'].get('A', 0)} A proposals passed the complete-date and independent-detail requirements; none are automatic confirmations.",
                  "Category/priority rules are not validated research accuracy or calibrated probabilities.",
                  "Input hashes, unchanged frame checks and existing membership/edge checks passed.",
                  "No training, TF-IDF fitting, new components, label change or resplitting occurred.",
                  "Discovery examines only the existing candidate set; undetected event leakage remains possible.",
                  "A future event-aware split requires separate approval.", ""])
    with (output / "report.md").open("x", encoding="utf-8") as target:
        target.write("\n".join(lines))
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path)
    args = parser.parse_args()
    result = audit_snapshot(args.snapshot)
    print(json.dumps(result["summary"], indent=2))
    print(result["review_queue"][["cluster_key", "post_count", "screening_category", "priority_score"]]
          .head(12).to_string(index=False))
    if args.output_directory:
        print("Output:", write_results(result, args.output_directory))
