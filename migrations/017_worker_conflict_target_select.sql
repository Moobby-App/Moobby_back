-- 017_worker_conflict_target_select.sql
-- Development only. Records the narrow ON CONFLICT target SELECT grants
-- already applied manually in Development; no other permissions or objects change.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

GRANT SELECT (event_id)
ON TABLE nlp.events
TO mooby_nlp_worker_dev;

GRANT SELECT (event_id, event_version, source_snapshot_id)
ON TABLE nlp.event_sources
TO mooby_nlp_worker_dev;

GRANT SELECT (evidence_id)
ON TABLE nlp.event_evidence
TO mooby_nlp_worker_dev;

COMMIT;
