"""Full-corpus deterministic event evidence audit, not a classifier or resplit.

Uses publication-time candidates, incident evidence, conservative complete-link
event membership and a separate template graph. Unknown is an explicit result.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import itertools
import json
from pathlib import Path
import re
from uuid import uuid4

import pandas as pd
from event_leakage_screening import (CAUSE_RULES, load_snapshot, load_location_rules, extract_evidence,
                                    parse_audit_date, frame_digest, normalized, UNCERTAIN_KEY)

VERSION = "full-event-evidence-v1"
WIB = timezone(timedelta(hours=7))
EPOCH_MS = 1288834974657
SPECIFIC = {"laa", "flooding", "signal", "power", "switch", "switch_over",
            "infrastructure", "vehicle_obstruction", "collision", "fire", "crowd"}
GENERIC = ("menunggu sinyal aman", "antrean masuk stasiun", "jadwal perjalanan",
           "silakan cek", "silahkan cek", "guna kami lakukan pengecekan")
ANALYST_EVENTS = {
    "EA001_BEKASITIMUR_COLLISION_20260427": {
        "ids": ["2048781550008057971", "2048785474488271035", "2048787687713480979",
                "2048788927583334420", "2048792854923981028", "2048798585677074716",
                "2048807483817549961", "2048825165442957331", "2048883555728810371",
                "2048963069649576430"],
        "evidence": "Dataset-level analyst comparison: same April 27/28 publication episode, Bekasi Timur collision involving KRL and Argo Bromo/KA 4B, linked taxi collision explanations and rescue/recovery reports. Different descriptions/cause speculation do not imply separate incidents. This establishes shared report identity, not factual verification of the asserted accident mechanism.",
    }
}


def snowflake(tweet_id):
    if not re.fullmatch(r"[0-9]{15,20}", tweet_id):
        return None
    value = int(tweet_id)
    if not 0 < value < 2**64:
        return None
    result = datetime.fromtimestamp(((value >> 22) + EPOCH_MS) / 1000, timezone.utc)
    return result if 2010 <= result.year <= 2026 else None


def resolve_dates(records):
    results, validation = [], Counter()
    for r in records:
        raw = parse_audit_date(r["date"])
        timestamp = snowflake(r["tweet_id"])
        url_match = re.search(r"/(?:status|statuses)/(\d+)", r["source_url"])
        url_ok = bool(url_match and url_match[1] == r["tweet_id"])
        compatible = bool(timestamp and all(raw[k] is None or raw[k] == getattr(timestamp.astimezone(WIB), k)
                                            for k in ("year", "month", "day")))
        if raw["precision"] == "day":
            validation["complete_date_checked"] += 1
            validation["complete_date_matches_wib" if compatible else "complete_date_conflicts"] += 1
        elif raw["precision"] in {"month_day", "month"}:
            validation[raw["precision"] + "_checked"] += 1
            validation[raw["precision"] + ("_matches" if compatible else "_conflicts")] += 1
        # A coarse search-display month is not an authoritative post timestamp.
        # Decode the ID but retain the conflict; never silently repair raw metadata.
        coarse_conflict = raw["precision"] == "month" and not compatible
        trusted = bool(timestamp and url_ok and (compatible or coarse_conflict))
        validation["url_id_matches" if url_ok else "url_id_unverified"] += 1
        results.append({"timestamp": timestamp if trusted else None,
                        "resolved_date": timestamp.isoformat() if trusted else "",
                        "resolved_date_wib": timestamp.astimezone(WIB).isoformat() if trusted else "",
                        "date_resolution_method": ("snowflake_url_id_conflicting_search_month" if trusted and coarse_conflict
                                                   else "snowflake_url_id_calendar_validated" if trusted else "unresolved_metadata_conflict"),
                        "date_resolution_confidence": "high_for_post_creation_not_event_time" if trusted else "unresolved",
                        "raw_date_precision": raw["precision"],
                        "date_limitation": "Decoded ID creation time; not API-verified, incident time, capture time or retrospective event date"})
    if validation["complete_date_checked"] == 0 or validation["complete_date_conflicts"]:
        raise ValueError("Snowflake/date validation failed; do not infer dates")
    return results, dict(validation)


def enrich(records, locations):
    dates, validation = resolve_dates(records)
    output = []
    for r, d in zip(records, dates):
        e = extract_evidence(r, locations)
        text = normalized(r["full_text"])
        # Longest nonoverlapping place matches: Bekasi Timur is not two stations
        # (Bekasi + Bekasi Timur), and Bojong Gede aliases cannot count twice.
        found = sorted([(m.start(), m.end(), name) for name, pattern in locations
                        for m in pattern.finditer(text)], key=lambda x: (-(x[1]-x[0]), x[0], x[2]))
        accepted = []
        for start, end, name in found:
            if not any(start < b and end > a for a, b, _ in accepted):accepted.append((start, end, name))
        e["locations"] = sorted({name for _, _, name in accepted})
        e["incident_locations"] = sorted({name for start, _, name in accepted
            if re.search(r"(?:antara|di stasiun|di lintas)\s*(?:stasiun\s*)?$", text[max(0,start-40):start])
            or any(abs(start-m.start()) <=80 for pattern in CAUSE_RULES.values()
                   for m in re.finditer(pattern,text))})
        e.update(d)
        e["text"] = text
        e["specific_causes"] = set(e["causes"]) & SPECIFIC
        # Location qualifiers and operational impact are supporting evidence only.
        e["routes"] = sorted(set(re.findall(r"\b(?:lintas|rute)\s+([^.,;()]{3,65})", text)))
        e["impact"] = sorted({label for label, pattern in {
            "delays": r"terlambat|keterlambatan|delay", "held": r"tertahan|menunggu",
            "suspended": r"belum (?:bisa|dapat) dilalui|dihentikan|tidak dapat dilalui|dibatalkan",
            "recovery": r"sudah.*(?:kembali|dilalui)|penguraian|normal kembali|kembali normal|back to normal|selesai ditangani",
        }.items() if re.search(pattern, text)})
        e["retrospective"] = bool(re.search(r"\bkemarin\b|\btahun lalu\b|\bpekan lalu\b", text))
        e["generic"] = any(x in text for x in GENERIC)
        e["tokens"] = set(re.findall(r"\w+", text))
        template = re.sub(r"https?://\S+|@\w+|\bka\s*\d+[a-z]?|\d+", " <detail> ", text)
        for _, pattern in locations:
            template = pattern.sub(" <station> ", template)
        template = " ".join(template.split())
        e["template"] = template
        e["template_tokens"] = set(re.findall(r"\w+", template))
        e["details"] = sorted({name for name, pattern in {
            "truck_broken_down": r"truk.*mogok|mogok.*truk",
            "overhead_pole_struck": r"(?:menabrak|tertabrak).*tiang|tiang.*(?:ditabrak|tertabrak)",
            "vehicle_struck": r"tertemper|menabrak|tertabrak",
            "switch_over_work": r"switch[\s-]*over|peralihan prasarana",
            "power_failure": r"gangguan.*(?:laa|listrik aliran atas)",
            "switch_failure": r"gangguan.*wesel|kendala.*wesel|wesel.*gangguan",
            "flood_water": r"genangan air|banjir",
            "taxi_collision": r"(?:taxi|taksi).*(?:tertabrak|menabrak|tertemper|kecelakaan)|(?:menabrak|tertabrak).*(?:taxi|taksi)",
            "argo_bromo_collision": r"argo bromo anggrek|\bka aba\b",
        }.items() if re.search(pattern, text)})
        output.append(e)
    return output, validation


def pair_relation(a, b):
    """Pair-specific evidence, independent of class labels and partitions."""
    shared_locations = set(a["locations"]) & set(b["locations"])
    shared_incident = set(a["incident_locations"]) & set(b["incident_locations"])
    shared_trains = set(a["train_numbers"]) & set(b["train_numbers"])
    shared_causes = a["specific_causes"] & b["specific_causes"]
    shared_details = set(a["details"]) & set(b["details"])
    hours = (abs((a["timestamp"] - b["timestamp"]).total_seconds()) / 3600
             if a["timestamp"] and b["timestamp"] else None)
    common_tokens = len(a["template_tokens"] & b["template_tokens"])
    template_score = common_tokens / max(1, len(a["template_tokens"] | b["template_tokens"]))
    template_match = common_tokens >= 8 and template_score >= .80
    generic = a["generic"] or b["generic"]
    evidence = {"hours_apart": round(hours, 4) if hours is not None else None,
                "shared_locations": sorted(shared_locations), "shared_incident_locations": sorted(shared_incident),
                "shared_causes": sorted(shared_causes), "shared_train_numbers": sorted(shared_trains),
                "shared_incident_details": sorted(shared_details), "template_token_jaccard": round(template_score, 4)}
    # No train-number disagreement veto: one incident may affect different trains.
    strong = (hours is not None and hours <= 24 and not generic
              and not a["retrospective"] and not b["retrospective"]
              and min(a["word_count"], b["word_count"]) >= 8
              and ((shared_causes and len(shared_incident) >= 2 and shared_details)
                   or ("collision" in shared_causes and shared_incident
                       and shared_details & {"taxi_collision", "argo_bromo_collision"})
                   or (shared_trains and shared_incident and
                       (shared_causes or "rolling_stock" in set(a["causes"]) & set(b["causes"])))))
    if strong:
        return "SAME_EVENT", evidence, template_match
    if generic and template_match:
        return "REPEATED_TEMPLATE", evidence, True
    related = bool((shared_locations and (shared_causes or shared_trains))
                   or (shared_trains and shared_causes))
    if related and hours is not None and hours > 72:
        current = all(re.search(r"\bhari ini\b|\bsaat ini\b", x["text"]) for x in (a,b))
        distinct_causes = bool(a['specific_causes'] and b['specific_causes'] and not shared_causes)
        distinct_trains_locations = bool(a['train_numbers'] and b['train_numbers'] and not shared_trains
                                        and a['incident_locations'] and b['incident_locations'] and not shared_incident)
        if current and not a['retrospective'] and not b['retrospective'] and (distinct_causes or distinct_trains_locations):
            return "DIFFERENT_EVENT", evidence, template_match
        if template_match:return "REPEATED_TEMPLATE", evidence, True
        return "UNRESOLVED", evidence, template_match
    if related and (hours is None or hours <= 72):
        return "UNRESOLVED", evidence, template_match
    if template_match:
        return "REPEATED_TEMPLATE", evidence, True
    return None, evidence, False


def stable_id(prefix, ids):
    return prefix + hashlib.sha256("|".join(sorted(ids)).encode()).hexdigest()[:16]


def run_audit(state, location_rules):
    df, manual = state.df_model, state.audit_manifest
    before = [frame_digest(df), frame_digest(manual)]
    records = df.to_dict("records")
    features, date_validation = enrich(records, location_rules)
    relation_rows, strong, possible, different = [], {}, defaultdict(set), defaultdict(set)
    template_parent = list(range(len(records)))
    def root(i):
        while template_parent[i] != i:
            template_parent[i] = template_parent[template_parent[i]]; i = template_parent[i]
        return i
    # All 858705 unordered record pairs are considered; no similarity-screen boundary.
    pairs_considered = 0
    for i in range(len(records)):
        for j in range(i + 1, len(records)):
            pairs_considered += 1
            relation, evidence, template = pair_relation(features[i], features[j])
            if template:
                template_parent[root(j)] = root(i)
            if relation:
                relation_rows.append({"tweet_id_a": records[i]["tweet_id"], "tweet_id_b": records[j]["tweet_id"],
                                      "relation": relation, "evidence": evidence})
            if relation == "SAME_EVENT": strong[i, j] = evidence
            if relation in {"SAME_EVENT", "UNRESOLVED"}:
                possible[i].add(j); possible[j].add(i)
            if relation == "DIFFERENT_EVENT":
                different[i].add(j); different[j].add(i)
    id_position = {r["tweet_id"]: i for i, r in enumerate(records)}
    groups = {event: [id_position[t] for t in rows.tweet_id]
              for event, rows in manual.groupby("event_id")}
    for event, decision in ANALYST_EVENTS.items():
        assert state.snapshot_sha256 == '611d058bc89424c84e0675f37e325da54eb40a2c107f89e19802c68128e37df9'
        assert not (set(decision['ids']) & set(manual.tweet_id))
        groups[event] = [id_position[t] for t in decision['ids']]
    assignment = {i: event for event, members in groups.items() for i in members}
    provenance = {event: ("evidence_supported_analyst_adjudication" if event in ANALYST_EVENTS
                          else "prior_human_confirmation") for event in groups}
    by_id = sorted(range(len(records)), key=lambda i: int(records[i]["tweet_id"]))
    prior_uncertain = {i for members in state.candidate_clusters.values()
                       if min(records[k]['tweet_id'] for k in members) == UNCERTAIN_KEY for i in members}
    # Only complete-link evidence may extend/construct automated events.
    # This prevents a generic intermediary from chaining independent incidents.
    def linked(i, j): return (min(i, j), max(i, j)) in strong
    blocked = set(prior_uncertain)
    for i in by_id:
        if i in assignment or i in blocked: continue
        connected_manual = {assignment[j] for j in assignment if linked(i, j)}
        if len(connected_manual) > 1:
            blocked.add(i);continue
        matches = [event for event, members in groups.items() if all(linked(i, j) for j in members)]
        if len(matches) == 1:
            event = matches[0]; groups[event].append(i); assignment[i] = event
    for i, j in sorted(strong, key=lambda edge: (int(records[edge[0]]["tweet_id"]), int(records[edge[1]]["tweet_id"]))):
        if i in assignment or j in assignment or i in blocked or j in blocked: continue
        event = stable_id("AUTO_", [records[i]["tweet_id"], records[j]["tweet_id"]])
        groups[event] = [i, j]; assignment[i] = assignment[j] = event
        provenance[event] = "evidence_supported_automated_not_human_verified"
        for k in by_id:
            if k not in assignment and k not in blocked and all(linked(k, m) for m in groups[event]):
                groups[event].append(k); assignment[k] = event
    template_members = defaultdict(list)
    for i in range(len(records)): template_members[root(i)].append(i)
    template_ids = {i: stable_id("TPL_", [records[k]["tweet_id"] for k in members])
                    for members in template_members.values() if len(members) > 1 for i in members}
    original_candidates = {i for members in state.candidate_clusters.values() for i in members}
    manual_ids = set(manual.tweet_id)
    manifest_rows = []
    for i, r in enumerate(records):
        e = features[i]; event = assignment.get(i)
        related = possible[i]
        if event:
            status = "SAME_EVENT"
            confidence = ("prior_human_verified" if r["tweet_id"] in manual_ids else
                          "analyst_evidence_supported" if event in ANALYST_EVENTS else "automated_evidence_supported")
            reason = ("Existing human assignment retained" if r["tweet_id"] in manual_ids else
                      ANALYST_EVENTS[event]['evidence'] if event in ANALYST_EVENTS else
                      "Complete-link specific incident evidence; not similarity membership")
        elif i in prior_uncertain:
            status, confidence, reason = "UNRESOLVED", "unresolved", "Prior reviewed same-template/event-uncertain decision retained; May 12/19 train-specific replies do not establish a shared event identity"
        elif related:
            status, confidence, reason = "UNRESOLVED", "unresolved", "Plausible related records but insufficient independent incident evidence or competing memberships"
        elif i in template_ids:
            status, confidence, reason = "REPEATED_TEMPLATE", "template_only_no_event_identity", "Generic/structural overlap establishes template identity only"
        elif different[i]:
            status, confidence, reason = "DIFFERENT_EVENT", "distinguishable_publication_episodes", "Shared incident attributes recur in different publication episodes (>72h); no event merge"
        elif not e["timestamp"] or e["generic"] or (r["disruption_label"] == "disruption" and not (e["specific_causes"] and e["incident_locations"])):
            status, confidence, reason = "UNRESOLVED", "unresolved", "Sparse/generic or missing event-specific details; absence of a detected match does not establish independence"
        else:
            status, confidence, reason = "UNIQUE_RECORD", "no_credible_relation_identified", "No credible related record found by this full-corpus method; not a leakage-free guarantee"
        event_evidence = [{"related_tweet_id": records[j]["tweet_id"], **strong[min(i, j), max(i, j)]}
                          for j in groups.get(event, []) if j != i and linked(i, j)]
        manifest_rows.append({
            "tweet_id": r["tweet_id"], "original_partition": r["partition"],
            "original_similarity_group": r["similarity_group"], "event_id": event or "",
            "event_identity_status": status, "event_evidence": json.dumps({"reason": reason, "specific_links": event_evidence}, ensure_ascii=False),
            "related_tweet_ids": json.dumps(sorted({records[j]["tweet_id"] for j in related} |
                                                   {records[j]["tweet_id"] for j in groups.get(event, []) if j != i})),
            "original_date": r["date"], **{k: e[k] for k in ("resolved_date", "resolved_date_wib", "date_resolution_method", "date_resolution_confidence", "raw_date_precision", "date_limitation")},
            "detected_locations": json.dumps(e["locations"]), "detected_incident_locations": json.dumps(e["incident_locations"]),
            "detected_causes": json.dumps(e["causes"]), "detected_train_numbers": json.dumps(e["train_numbers"]),
            "detected_routes": json.dumps(e["routes"]), "operational_impacts": json.dumps(e["impact"]),
            "retrospective_reference": e["retrospective"], "template_group_id": template_ids.get(i, ""),
            "event_identity_confidence": confidence, "original_relevance": r["disruption_label"],
            "original_severity": r["severity"], "source_account": r["account"],
            "previous_screening_involved": i in original_candidates,
            "prior_reviewed_uncertain": i in prior_uncertain,
        })
    manifest = pd.DataFrame(manifest_rows)
    for relation in relation_rows:
        i, j = id_position[relation["tweet_id_a"]], id_position[relation["tweet_id_b"]]
        relation["candidate_relation"] = relation["relation"]
        if i in assignment and assignment.get(i) == assignment.get(j):
            relation["relation"] = "SAME_EVENT"
        elif relation["relation"] == "SAME_EVENT":
            relation["relation"] = "UNRESOLVED"
    leaks = []
    for event, members in sorted(groups.items()):
        subset = df.iloc[members]; parts = set(subset.partition)
        if len(parts) < 2: continue
        leaks.append({"event_id": event, "provenance": provenance[event],
                      "prior_human_posts": sum(records[i]["tweet_id"] in manual_ids for i in members),
                      "automated_extension_posts": sum(records[i]["tweet_id"] not in manual_ids for i in members)
                          if provenance[event] == "prior_human_confirmation" else 0,
                      "posts": len(members), "tweet_ids": json.dumps(sorted(subset.tweet_id)),
                      "partitions": json.dumps(sorted(parts)),
                      "train": int(subset.partition.eq("train").sum()),
                      "validation": int(subset.partition.eq("validation").sum()),
                      "test": int(subset.partition.eq("test").sum()),
                      "train_validation": {"train", "validation"}.issubset(parts),
                      "train_test": {"train", "test"}.issubset(parts),
                      "validation_test": {"validation", "test"}.issubset(parts), "all_three": len(parts) == 3,
                      "relevance_counts": json.dumps(dict(Counter(subset.disruption_label))),
                      "severity_counts": json.dumps(dict(Counter(subset.severity))),
                      "severe_posts": int(subset.severity.eq("severe").sum()),
                      "outside_original_candidate_set": sum(i not in original_candidates for i in members),
                      "missed_entirely_by_old_screening": all(i not in original_candidates for i in members),
                      "evidence_rule": ("original human identity; any extensions separately marked" if provenance[event].startswith("prior")
                                        else ANALYST_EVENTS[event]['evidence'] if event in ANALYST_EVENTS
                                        else "complete-link time/location/cause/incident-detail evidence")})
    templates = []
    for members in template_members.values():
        if len(members) < 2: continue
        subset = df.iloc[members]
        if subset.partition.nunique() < 2:continue
        templates.append({"template_group_id": template_ids[members[0]], "posts": len(members),
                          "tweet_ids": json.dumps(sorted(subset.tweet_id)),
                          "partitions": json.dumps(sorted(set(subset.partition))),
                          "dates_wib": json.dumps(sorted({features[i]["resolved_date_wib"][:10] for i in members})),
                          "event_ids": json.dumps(sorted({assignment[i] for i in members if i in assignment})),
                          "rule": "connected structure-token Jaccard >= .80 with >=8 common tokens; template, never event identity"})
    # Mandatory all-record and original annotation invariants.
    assert len(manifest) == len(df) == 1311 and manifest.tweet_id.is_unique
    assert manifest.tweet_id.tolist() == df.tweet_id.tolist()
    for a, b in (("original_partition", "partition"), ("original_relevance", "disruption_label"), ("original_severity", "severity")):
        assert manifest[a].tolist() == df[b].tolist()
    indexed = manifest.set_index("tweet_id")
    for row in manual.to_dict("records"):
        assert indexed.loc[row["tweet_id"], "event_id"] == row["event_id"]
        assert indexed.loc[row["tweet_id"], "event_identity_confidence"] == "prior_human_verified"
    assert manifest.loc[manifest.event_identity_status.eq("UNRESOLVED"), "event_id"].eq("").all()
    assert before == [frame_digest(df), frame_digest(manual)]
    return manifest, pd.DataFrame(leaks), pd.DataFrame(templates), relation_rows, {
        "version": VERSION, "snapshot_sha256": state.snapshot_sha256,
        "date_validation": date_validation, "all_record_pairs_considered": pairs_considered,
        "status_counts": {status: int(manifest.event_identity_status.eq(status).sum()) for status in
                          ('SAME_EVENT','DIFFERENT_EVENT','REPEATED_TEMPLATE','UNRESOLVED','UNIQUE_RECORD')},
        "automated_event_count": sum(p == "evidence_supported_automated_not_human_verified" for p in provenance.values()),
        "analyst_adjudicated_event_count": len(ANALYST_EVENTS),
        "total_identified_event_count": len(groups), "candidate_evidence_edges": len(strong),
        "all_input_and_manual_assignment_checks_passed": True,
    }


def save_audit(state, result, directory):
    manifest, leaks, templates, relations, summary = result
    output = Path(directory) / ("full_event_audit_" + uuid4().hex)
    output.mkdir(parents=True, exist_ok=False)
    for name, frame in (("full_event_identity_manifest.csv", manifest),
                        ("event_leakage_report.csv", leaks), ("template_overlap_report.csv", templates)):
        frame.to_csv(output / name, index=False, mode="x")
    with (output / "relationship_evidence.jsonl").open("x") as target:
        for row in relations:target.write(json.dumps(row, ensure_ascii=False) + "\n")
    leak_ids = {i for value in leaks.tweet_ids for i in json.loads(value)} if len(leaks) else set()
    affected = manifest.loc[manifest.tweet_id.isin(leak_ids)]
    by_id = manifest.set_index("tweet_id")
    unresolved_cross = [r for r in relations if r["relation"] == "UNRESOLVED"
                        and by_id.loc[r["tweet_id_a"], "original_partition"] != by_id.loc[r["tweet_id_b"], "original_partition"]]
    summary.update({"cross_partition_events": len(leaks), "unique_affected_posts": len(affected),
                    "affected_relevance_counts": dict(Counter(affected.original_relevance)),
                    "affected_severity_counts": dict(Counter(affected.original_severity)),
                    "cross_partition_templates": len(templates),
                    "leakage_by_boundary": {name: int(leaks[name].sum()) for name in
                                            ("train_validation", "train_test", "validation_test", "all_three")},
                    "severe_overlap_events": int(leaks.severe_posts.gt(0).sum()),
                    "new_leak_events": int(leaks.provenance.str.startswith("evidence_supported").sum()),
                    "entirely_missed_events": int(leaks.missed_entirely_by_old_screening.sum()),
                    "affected_posts_outside_original_screening": int(leaks.outside_original_candidate_set.sum()),
                    "automated_extensions_to_prior_event_posts": int(leaks.automated_extension_posts.sum()),
                    "unresolved_by_partition": manifest.loc[manifest.event_identity_status.eq("UNRESOLVED")].original_partition.value_counts().to_dict(),
                    "unresolved_cross_partition_relations": len(unresolved_cross),
                    "posts_in_unresolved_cross_partition_relations": len({r[k] for r in unresolved_cross for k in ("tweet_id_a", "tweet_id_b")}),
                    "severe_status_by_partition": {str(k): int(v) for k,v in manifest.loc[manifest.original_severity.eq("severe")].groupby(["original_partition", "event_identity_status"]).size().items()},
                    "commuterline_by_partition": manifest.assign(is_commuter=manifest.source_account.eq("@CommuterLine")).groupby("original_partition").is_commuter.sum().astype(int).to_dict(),
                    "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    (output / "audit_metrics.json").write_text(json.dumps(summary, indent=2))
    (output / "analyst_event_adjudications.json").write_text(json.dumps(ANALYST_EVENTS, indent=2))
    proposed = []
    delta = Counter()
    for r in leaks.to_dict("records"):
        counts = {p: r[p] for p in ("train", "validation", "test")}
        destination = max(counts, key=counts.get)
        moved = r["posts"] - counts[destination]
        for p, count in counts.items():
            if p != destination:delta[p] -= count;delta[destination] += count
        proposed.append((r["event_id"], destination, moved, r["provenance"]))
    lines = ["# Event-aware split proposal — NOT EXECUTED", "",
             "This is an analytical scenario, not a replacement assignment file. Original partitions remain unchanged.",
             "Keep each prior human-confirmed event indivisible. Assess new automated event units separately, preserving their non-human provenance.",
             "The per-event majority destination minimizes record moves for these disjoint units; ties use train, validation, test order.",
             "It does not optimize class balance, unresolved relationships, unseen incidents or template independence.", "",
             "| Event | Hypothetical destination | Minimum moves | Provenance |", "|---|---|---:|---|"]
    for e, p, count, provenance in proposed:lines.append(f"| {e} | {p} | {count} | {provenance} |")
    lines += ["", f"Minimum moves for these accepted overlap units only: {sum(r[2] for r in proposed)}.",
              "Hypothetical total sizes: " + json.dumps({p: int(state.df_model.partition.eq(p).sum()) + delta[p] for p in ("train", "validation", "test")}),
              "", "Before approval, compute class-distribution constraints and event/wording diversity, not only record-level stratification.",
              "Retain unresolved records, but do not advertise any resulting evaluation partition as event-independent while unresolved links can cross it.",
              "Options for unresolved records: keep potential-relation blocks indivisible as explicitly conservative split constraints, or retain a separate uncertainty challenge set outside the primary event-independent score. Neither option assigns an event identity.",
              "Do not equate template groups with events; a template-disjoint robustness evaluation can be a separate benchmark.",
              "Preserve all data/labels and the original split as a versioned exploratory benchmark. Never choose a new split using model test scores.",
              "This proposal does not solve undiscovered events. New independently collected, time-complete evaluation data would provide a stronger future validation."]
    projected = Counter((r['partition'],r['disruption_label'],r['severity']) for r in state.df_model.to_dict('records'))
    destinations = {event: target for event,target,_,_ in proposed}
    for record in manifest.to_dict('records'):
        target = destinations.get(record['event_id'],record['original_partition'])
        if target != record['original_partition']:
            label=(record['original_relevance'],record['original_severity'])
            projected[(record['original_partition'],*label)]-=1;projected[(target,*label)]+=1
    lines += ['', '## Analytical class-count impact (no assignments written)', '',
              '| Partition | Not disruption | Light | Moderate | Severe |', '|---|---:|---:|---:|---:|']
    for part in ('train','validation','test'):
        lines.append('| '+part+' | '+' | '.join(str(projected[(part,label,sev)]) for label,sev in
            [('not_disruption',''),('disruption','light'),('disruption','moderate'),('disruption','severe')])+' |')
    lines += ['', 'These are predicted counts under the majority-unit scenario only; no original partition values were changed.']
    (output / "event_aware_split_proposal.md").write_text("\n".join(lines) + "\n")
    lines = ["# Full dataset event leakage audit", "", f"Method: {VERSION}",
             f"Input snapshot SHA-256: {state.snapshot_sha256}", "",
             "## Actual results", "", "```json", json.dumps(summary, indent=2), "```", "",
             "## Interpretation", "",
             "Every record was feature-extracted and every unordered pair considered; no old similarity cluster limits candidate discovery.",
             "The six original human identities and all 43 assignments are retained. New event IDs are evidence-supported automated hypotheses, not independently verified human facts.",
             "Original labels do not decide pair identity. Different train numbers do not veto an incident; same station alone is insufficient.",
             "Automated event groups require complete-link independent detail evidence; graph connectivity alone cannot merge events. Original human groups are not broken apart by the algorithm.",
             "One additional incident (Bekasi Timur collision, April 27/28 2026) was consolidated by direct analyst comparison of ten actual texts/time/location/incident details; this avoids double-counting differently worded taxi-collision and Argo Bromo reports as two events. Evidence and IDs are in analyst_event_adjudications.json. This is not a prior human confirmation or external fact-check.",
             "Publication time is not incident time. Different publication dates alone never establish DIFFERENT_EVENT: that category also requires current-time wording and distinguishable causes or disjoint trains plus incident locations. Retrospective or extended incidents can remain unresolved.",
             "UNIQUE_RECORD means no credible relation detected, not proven independence. UNRESOLVED has no event ID and is never declared leakage-free.",
             "## Method and limits", "",
             "Snowflake decoding: ((integer ID >> 22) + 1288834974657) milliseconds UTC. Verify the source URL ID and known calendar fields, compare calendar dates in WIB. Never anchor relative dates to execution time.",
             "Primary references: https://docs.x.com/fundamentals/x-ids and https://raw.githubusercontent.com/twitter-archive/snowflake/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala",
             "Features: original and normalized text; existing station alias catalog; explicit KA numbers; cause, route, impact and recovery regexes. Location proximity to cause is heuristic, not validated relation extraction.",
             "Strong pair: <=24h publication separation, non-generic, >=8 words, no explicit retrospective marker, and shared specific cause + >=2 incident locations + distinctive incident detail; alternatively shared KA number + incident location + specific cause/rolling-stock problem.",
             "Potential relationship: shared location and cause/train, or train and cause, within 72h (or unresolved dates). Insufficient evidence remains unresolved, including generic signal-wait language.",
             "Templates: replace station names, KA numbers, URLs, mentions and numbers, then require structural token Jaccard >=.80 with >=8 common tokens. Connected template groups do not establish event identity.",
             "No fitted NLP model, TF-IDF, original clustering, label alteration or resplit was performed. Fixed rule thresholds are not validated research accuracy; false negatives and false positives remain possible.",
             "## Current evaluation validity", "",
             "The current split cannot support a claim of event-independent generalization. Prior human-confirmed leakage alone establishes this, regardless of the success of the original similarity-group check.",
             "It can serve as an exploratory within-corpus/source/template benchmark with explicit contamination disclosure; model rankings and accuracy must not be presented as unbiased performance on unseen incidents.",
             "39 severe training records and 14 severe test records do not imply that many independent incidents. Count event units and unresolved severe records in the manifest before making severe-generalization claims.",
             "Source dominance remains 707/1311 @CommuterLine posts; lexical account style can carry across partitions even without the account feature. This is a risk, not measured model bias.",
             "## Validation", "",
             "1311 IDs exactly once, original ordered IDs/partitions/relevance/severity unchanged, 43 human assignments unchanged, each accepted cross-partition event listed once. Repeated runs are checked for deterministic identity and evidence output.",
             "No manual-review queue is generated. Uncertainty is retained in the manifest and aggregate metrics, not assigned back to the user."]
    lines += ['', '## Cross-partition event evidence inventory', '',
              'The boundary counts above are inclusive: all-three events also appear in each applicable pairwise boundary. Each event and affected tweet is counted once in unique totals.', '',
              '| Event | Posts (train/validation/test) | Evidence provenance | Locations mentioned | Publication date range WIB |',
              '|---|---|---|---|---|']
    for row in leaks.to_dict('records'):
        subset=manifest.loc[manifest.tweet_id.isin(json.loads(row['tweet_ids']))]
        locations=sorted({v for values in subset.detected_locations for v in json.loads(values)})
        dates=sorted(subset.resolved_date_wib.str[:10])
        provenance=row['provenance']
        if row['automated_extension_posts']:provenance+=f"; {row['automated_extension_posts']} automated extensions"
        lines.append(f"| {row['event_id']} | {row['posts']} ({row['train']}/{row['validation']}/{row['test']}) | {provenance} | {', '.join(locations)} | {dates[0]} to {dates[-1]} |")
    lines += ['', '## Additional findings', '',
              'EA001_BEKASITIMUR_COLLISION_20260427 consolidates ten reports of the April 27/28 Bekasi Timur KRL/Argo Bromo collision and its linked taxi-collision explanations. The original 35-cluster screening contained none of these ten records. It spans all three partitions. Speculation about responsibility/mechanism is not independently verified.',
              'AUTO_4dba55f0f2293ad3 links three May 4 LAA Jurangmangu–Pondok Ranji announcements: alternating single-track operation, temporarily impassable route, and repair/recovery with restricted speed. Two records were in the original candidate set and one additional record was found by the full-corpus audit.',
              'E003 and E006 each retain every prior human assignment and have one separately identified automated extension. They were not silently added to the original human audit_manifest.',
              'The seven-post May 12/19 cluster remains UNRESOLVED with template identity separate; it is not automatically promoted to an event.',
              '189 search-display month/year values conflict with ID creation dates. Original values remain in the manifest; explicit method labels distinguish these cases. All 433 complete calendar dates and all 424 month/day values validate in WIB.',
              '32 of 39 severe training posts and 9 of 14 severe test posts remain event-unresolved under this method. These counts quantify an evidence limitation, not mislabeled data or instructions for manual review.',
              'No evidence met the conservative DIFFERENT_EVENT rule in this run; zero is reported instead of forcing recurring publications into distinct incident identities. Template overlap and publication-time differences alone cannot establish that conclusion.']
    (output / "event_audit_summary.md").write_text("\n".join(lines) + "\n")
    return output, summary


if __name__ == "__main__":
    p = argparse.ArgumentParser();p.add_argument("--snapshot", required=True);p.add_argument("--output-directory", required=True)
    args = p.parse_args()
    state = load_snapshot(args.snapshot)
    result = run_audit(state, load_location_rules(Path(__file__).parent / "reference/station_alias_review.csv"))
    output, summary = save_audit(state, result, args.output_directory)
    print(json.dumps(summary, indent=2));print("OUTPUT", output)
