BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

GRANT INSERT ON TABLE
    nlp.processing_runs,
    nlp.processing_notes
TO mooby_nlp_worker_dev;

COMMIT;