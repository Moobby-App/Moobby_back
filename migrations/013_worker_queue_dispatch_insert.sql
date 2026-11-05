-- 013_worker_queue_dispatch_insert.sql
-- Development only. Allows the worker to enqueue recent source snapshot IDs.
-- Does not change collection objects, data, roles, or Collector behavior.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

GRANT INSERT (source_snapshot_id, pipeline_version)
ON TABLE nlp.processing_queue
TO mooby_nlp_worker_dev;

COMMIT;
