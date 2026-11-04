-- 007_nlp_worker_queue_grants.sql
-- Development only. Grants access only to nlp queue objects.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

GRANT USAGE ON SCHEMA nlp
    TO mooby_nlp_worker_dev;

GRANT SELECT ON TABLE nlp.processing_queue
    TO mooby_nlp_worker_dev;

GRANT UPDATE (
    status,
    attempt_count,
    available_at,
    claimed_at,
    lease_until,
    lease_owner,
    updated_at
)
ON TABLE nlp.processing_queue
TO mooby_nlp_worker_dev;

COMMIT;