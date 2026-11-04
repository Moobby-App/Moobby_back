-- 006_nlp_worker_queue_role.sql
-- Development only. Grants queue access in nlp; does not touch collection.

BEGIN;

CREATE ROLE mooby_nlp_worker_dev NOLOGIN;

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