-- 012_worker_draft_event_access.sql
-- Development only. Enables confirmed-path reads and atomic draft inserts.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

GRANT SELECT (
    path_id,
    path_version,
    line_id,
    review_status,
    effective_since,
    effective_until
)
ON TABLE nlp.line_paths
TO mooby_nlp_worker_dev;

GRANT SELECT (
    path_id,
    path_version,
    direction,
    seq,
    station_id,
    effective_since,
    effective_until
)
ON TABLE nlp.line_path_stations
TO mooby_nlp_worker_dev;

GRANT INSERT (event_id)
ON TABLE nlp.events
TO mooby_nlp_worker_dev;

GRANT INSERT (
    event_id,
    event_version,
    source_snapshot_id,
    raw_content_id,
    pipeline_version,
    processing_run_id,
    source_platform,
    canonical_url,
    source_digest,
    source_published_at,
    source_published_at_basis,
    captured_at
)
ON TABLE nlp.event_sources
TO mooby_nlp_worker_dev;

GRANT INSERT (
    evidence_id,
    event_id,
    event_version,
    source_snapshot_id,
    evidence_quote,
    quote_start_char,
    quote_end_char,
    validator_version,
    exact_match_validated_at
)
ON TABLE nlp.event_evidence
TO mooby_nlp_worker_dev;

GRANT INSERT (
    event_id,
    event_version,
    source_snapshot_id,
    pipeline_version,
    processing_run_id,
    event_kind,
    line_id,
    station_id,
    start_station_id,
    end_station_id,
    direction,
    path_id,
    path_version,
    event_at,
    event_time_raw_phrase,
    event_time_method,
    event_time_certainty,
    title,
    summary
)
ON TABLE nlp.event_versions
TO mooby_nlp_worker_dev;

COMMIT;
