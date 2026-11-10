-- 018_app_api_original_text.sql
-- Development only. Explicitly approved original post text for eligible events.
-- No collection access/grant/data changes, backfill, or promotion of old drafts.
BEGIN;

-- The reported Development membership is from supabase_admin, ADMIN TRUE,
-- INHERIT FALSE, SET FALSE. Never overwrite/revoke that existing grant.
-- Abort if a postgres-granted membership already exists: it is not ours to remove.
DO $preflight$
BEGIN
    IF session_user <> 'postgres' OR current_user <> 'postgres' THEN
        RAISE EXCEPTION '018 requires the authorized postgres migration session';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_auth_members
        WHERE roleid = 'mooby_app_api_owner_dev'::regrole
          AND member = 'postgres'::regrole
          AND grantor = 'postgres'::regrole
    ) THEN
        RAISE EXCEPTION '018 found an existing postgres-granted owner membership; review before applying';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_auth_members
        WHERE roleid = 'mooby_app_api_owner_dev'::regrole
          AND member = 'postgres'::regrole
          AND admin_option
    ) THEN
        RAISE EXCEPTION '018 requires the existing owner-role ADMIN option';
    END IF;
END;
$preflight$;

SET LOCAL ROLE mooby_nlp_owner_dev;

ALTER TABLE nlp.event_versions ADD COLUMN original_text text;
COMMENT ON COLUMN nlp.event_versions.original_text IS
    'Unmodified source text copied at new event insert for app display; null on legacy versions. Not the internal evidence_quote.';
GRANT INSERT (original_text) ON TABLE nlp.event_versions TO mooby_nlp_worker_dev;
GRANT SELECT (original_text) ON TABLE nlp.event_versions TO mooby_app_api_owner_dev;

RESET ROLE;

-- Separate temporary grant using the existing ADMIN option. Explicit grantor
-- makes cleanup affect only this new grant, not the supabase_admin membership.
GRANT mooby_app_api_owner_dev TO postgres
    WITH INHERIT FALSE, SET TRUE, ADMIN FALSE
    GRANTED BY postgres;
SET LOCAL ROLE mooby_app_api_owner_dev;

CREATE OR REPLACE VIEW app_api.published_disruptions
WITH (security_barrier = true)
AS
WITH latest_versions AS (
    SELECT DISTINCT ON (ev.event_id)
        ev.event_id,
        ev.event_version,
        ev.source_snapshot_id,
        ev.event_kind,
        ev.line_id,
        ev.station_id,
        ev.start_station_id,
        ev.end_station_id,
        ev.direction,
        ev.path_id,
        ev.path_version,
        ev.event_at,
        ev.event_time_certainty,
        ev.freshness_anchor_at,
        ev.first_seen_at,
        ev.certainty_label,
        ev.verification_status,
        ev.publication_status,
        ev.severity,
        ev.title,
        ev.summary,
        ev.original_text
    FROM nlp.event_versions AS ev
    ORDER BY ev.event_id, ev.event_version DESC
), eligible AS (
    SELECT *
    FROM latest_versions
    WHERE verification_status = 'verified'
      AND publication_status = 'published'
)
SELECT
    ev.event_id AS public_event_id,
    ev.event_kind,
    ev.title,
    ev.summary,
    ev.severity,
    ev.event_at,
    ev.event_time_certainty,
    ev.certainty_label,
    COALESCE(ev.event_at, ev.freshness_anchor_at, ev.first_seen_at)
        AS freshness_anchor_at,
    line.canonical_name AS line_name,
    ev.station_id,
    station.canonical_name AS station_name,
    ev.start_station_id,
    start_station.canonical_name AS start_station_name,
    ev.end_station_id,
    end_station.canonical_name AS end_station_name,
    ev.direction,
    CASE
        WHEN ev.event_kind = 'station' THEN ARRAY[ev.station_id]
        WHEN path_range.station_ids IS NOT NULL THEN path_range.station_ids
        ELSE ARRAY[ev.start_station_id, ev.end_station_id]
    END AS affected_station_ids,
    CASE src.source_platform
        WHEN 'x' THEN 'X'
        WHEN 'threads' THEN 'Threads'
        WHEN 'rss' THEN 'RSS'
        WHEN 'web' THEN 'Web'
        ELSE 'Source'
    END AS source_display_name,
    src.canonical_url AS source_url,
    src.source_published_at,
    ev.original_text
FROM eligible AS ev
JOIN LATERAL (
    SELECT s.source_platform, s.canonical_url, s.source_published_at
    FROM nlp.event_sources AS s
    WHERE s.event_id = ev.event_id
      AND s.event_version = ev.event_version
      AND s.source_snapshot_id = ev.source_snapshot_id
    ORDER BY s.source_published_at DESC NULLS LAST
    LIMIT 1
) AS src ON true
LEFT JOIN nlp.transit_lines AS line
    ON line.line_id = ev.line_id
LEFT JOIN nlp.line_paths AS path
    ON path.path_id = ev.path_id
   AND path.path_version = ev.path_version
   AND path.line_id = ev.line_id
LEFT JOIN reference.krl_stations AS station
    ON station.station_id = ev.station_id
LEFT JOIN reference.krl_stations AS start_station
    ON start_station.station_id = ev.start_station_id
LEFT JOIN reference.krl_stations AS end_station
    ON end_station.station_id = ev.end_station_id
LEFT JOIN LATERAL (
    WITH active_stations AS MATERIALIZED (
        SELECT pst.station_id, pst.seq
        FROM nlp.line_path_stations AS pst
        WHERE ev.event_kind = 'segment'
          AND ev.path_id IS NOT NULL
          AND pst.path_id = ev.path_id
          AND pst.path_version = ev.path_version
          AND pst.direction = ev.direction
          AND (pst.effective_since IS NULL OR pst.effective_since <=
                COALESCE(ev.event_at, src.source_published_at, ev.first_seen_at)::date)
          AND (pst.effective_until IS NULL OR pst.effective_until >=
                COALESCE(ev.event_at, src.source_published_at, ev.first_seen_at)::date)
    ), endpoint_bounds AS (
        SELECT
            MIN(seq) FILTER (WHERE station_id = ev.start_station_id) AS start_seq,
            MIN(seq) FILTER (WHERE station_id = ev.end_station_id) AS end_seq
        FROM active_stations
    )
    SELECT array_agg(active.station_id ORDER BY active.seq) AS station_ids
    FROM active_stations AS active
    CROSS JOIN endpoint_bounds AS bounds
    WHERE active.seq BETWEEN LEAST(bounds.start_seq, bounds.end_seq)
                          AND GREATEST(bounds.start_seq, bounds.end_seq)
) AS path_range ON true
WHERE COALESCE(ev.event_at, ev.freshness_anchor_at, ev.first_seen_at)
        >= statement_timestamp() - interval '24 hours'
  AND COALESCE(ev.event_at, ev.freshness_anchor_at, ev.first_seen_at)
        <= statement_timestamp()
  AND (
      ev.event_kind = 'station'
      OR (
          ev.path_id IS NOT NULL
          AND path.review_status = 'confirmed'
          AND (path.effective_since IS NULL OR path.effective_since <=
                COALESCE(ev.event_at, src.source_published_at, ev.first_seen_at)::date)
          AND (path.effective_until IS NULL OR path.effective_until >=
                COALESCE(ev.event_at, src.source_published_at, ev.first_seen_at)::date)
      )
  );

COMMENT ON VIEW app_api.published_disruptions IS
    'Latest verified/published versions within 24h; includes explicitly approved original_text, excludes internal quotes and author metadata.';

RESET ROLE;
REVOKE mooby_app_api_owner_dev FROM postgres GRANTED BY postgres RESTRICT;
COMMIT;
