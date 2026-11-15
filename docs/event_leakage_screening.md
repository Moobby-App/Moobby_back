# Phase 2: semi-automated event-leakage screening

The workflow consumes the user's existing JSON state, never recreates the
dataset, deduplication, partition assignments, TF-IDF vectors or clusters.
`evaluation/event_leakage_screening.py` restores pandas `orient="table"`
structures, preserving row order, string IDs, typed dates and the original index.
`event_candidates` positions remain positional references to that restored frame.
The CLI and appended notebook section invoke the same reusable functions.

## Input checks and provenance

Before screening, validation requires 1311 unique records, existing 786/262/263
partitions, 559/752 relevance counts, 342/151/66 severity counts, 1231 separated
similarity groups, 124 unique bounded cross-partition edges, 35 saved clusters,
and 120 distinct involved positions. It verifies membership, saved cluster keys,
edge totals and connectivity of *supplied* clusters, without constructing new
components. A graph traversal is only an integrity assertion.

The original human manifest must retain exactly its six event names, 43 unique
tweet-to-event assignments and per-event partition counts. Its shared columns
must agree with the source records. No report updates that manifest.

The manually reviewed uncertain cluster `2054010080199352486` is recorded from
the user's task brief as `same_template_event_uncertain`, with seven posts. This
decision is not inferred from similarity and was not present as a separate
annotation field in the supplied snapshot. Reports explicitly record provenance.
There are 29 clusters without confirmed identity, including that one previously
reviewed cluster, and 28 never manually reviewed clusters.

## Deterministic extraction, rule version moobby-event-screening-v1

- Original raw date strings and extracted date precision are retained. Calendar
  dates, month/day without year, month/year, relative time and unknown values are
  distinguished. A missing year/day is never supplied. Relative times are never
  anchored to execution time. Leap-year 2000 is used only to validate whether a
  month/day is possible; it is never assigned to the source date.
- Calendar comparison intersects explicitly known year/month/day fields.
  Matching partial dates are only compatible, not proof of same date/event.
  Different calendar dates can still belong to a long-running event; the flag is
  evidence for review, not an event-identity rule.
- Station names and aliases come from the local existing
  `evaluation/reference/station_alias_review.csv`; its SHA-256 is reported.
  Matches use NFKC, casefold and word boundaries. References to stations can
  describe a route rather than an incident. A separate heuristic incident-location
  flag uses a nearby cause within 80 normalized characters or explicit location
  context (`antara`, `di stasiun`, `di lintas`). This is not validated relation
  extraction and can miss unlisted names or link a broad route to a cause.
- Train numbers are explicit `KA` followed by 3–5 digits and optional letter,
  optionally `No.`. Numbers alone, dates and rolling-stock identifiers are not
  treated as train numbers. Different trains can be affected by one event.
- Causes use documented source regexes for LAA, flooding, signals, power, switches,
  switch-over, infrastructure, vehicles/collisions, rolling stock, crowds and fire.
  Multiple causes may coexist. Missing matches mean undetected, not absent.
- Repeated boilerplate is flagged when at least 80% of posts each contain at
  least two phrases from the explicit greeting/apology/information phrase list.
- Sparse text means fewer than ten whitespace-separated units for this screening
  flag only; no record is removed or relabeled.
- Score summaries use only the 124 *existing cross-partition screening edges*.
  Min/max/mean and density do not represent every within-cluster pair: missing
  edges may be unrecorded or below the prior threshold. No scores are recomputed.
- Each evidence record includes tweet ID, raw date/precision, extracted locations,
  trains, causes, boilerplate phrases and a maximum 160-character excerpt. The
  report does not include credentials or hundreds of complete source posts.

## Categories and orthogonal priority

`MANUALLY_CONFIRMED` is reserved for clusters fully covered by one original
confirmed human event. Automated evidence never creates that status.

For other clusters:

- **A — likely same event:** all posts have the same complete calendar date,
  share an incident-location match and cause, and share either a train number
  or at least two incident-location matches. Sparse text and disjoint train
  numbers prevent A. These are proposals, never automatic confirmations.
- **B — likely template repetition:** boilerplate repetition plus differing
  calendar dates, disjoint train numbers, or no shared incident location/cause.
  This is not proof of no event overlap.
- **C — uncertain:** evidence does not meet A/B, or the prior human-uncertain
  decision applies. A previous uncertain decision is not upgraded automatically.
- **D — high-priority review:** an *additional* flag for unconfirmed clusters
  involving severe labels, mixed relevance/severity labels, incomplete/conflicting
  dates, or train–test overlap. Partial manifest coverage or multiple confirmed
  event IDs also require priority review. A/B/C and D can coexist.

Mixed labels are not automatically annotation errors; updates can describe
different operational states. Source account and predicted labels do not establish
event identity. Only displayed label diversity/severe coverage affect priority.

All A/C and all D cases require human review. A B case without D may be deferred,
but its identity remains unconfirmed and its human status is not changed.
The priority ordering is a transparent heuristic, not a probability or measured
risk: severe +100, train/test +60, mixed relevance +40, mixed severity +30,
ambiguous dates +20, plus post count; tie-break by size and cluster key.

## Results and future decisions

`audit_snapshot(path)` returns four objects: `leakage_screening_report`,
`review_queue`, `candidate_annotation_table`, and `summary`. The annotation table
has null proposed event identities, hypothesis categories, uncertainty, human
status and separate human evidence notes. Do not write proposed IDs into the
original manifest. `automated_category` preserves rule output even when a prior
manual decision controls the report status.

Repeated identical inputs produce equal frames and summaries. UUID output paths
are intentionally different; they do not affect report ordering or categories.
Frame, row-order, partition, manifest, pair/cluster, source snapshot and lexicon
fingerprints support traceability. Before/after checks cover all input frames
and candidate memberships. CLI outputs are written only into new exclusive run
directories under the requested local private output directory.

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B evaluation/event_leakage_screening.py \
  --snapshot evaluation/private/leakage_audit/existing_audit_state_8d1f8f26cb784880b1af72e392bb2d58.json \
  --output-directory evaluation/private/leakage_audit

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests .venv/bin/python -B -m unittest test_event_leakage_screening -v
```

## Research limitations and rollback

The candidate set is a similarity-selected subset, not a census of all leakage.
Disconnected descriptions of the same event can be missed. Conversely, shared
templates can connect unrelated events, including transitively. Screening accuracy
has not been validated. Partial dates, label ambiguity, source style, multilingual
text and missing context remain limitations. No missing data is treated as an
automatic confirmation. No human review or final identity is fabricated.

Reading test text is confined to leakage auditing. Do not use these reports to
choose model features, tune model thresholds or select the best test score. Known
event overlap means the present test is not proven event-independent; a future
event-aware split requires separate approval. No training or resplitting occurs.

Rollback consists of removing the appended audit-only notebook section and this
module/tests/documentation after review. Original cells and their outputs are
preserved. Generated private run directories can be retained as provenance.
Source snapshot, CSVs and confirmed human manifest are never overwritten.
