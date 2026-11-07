-- Development only; prepared, never applied by local tests.
-- Safe after 014 or 015. Existing null embeddings block this migration rather
-- than being fabricated, deleted, or promoted. No collection/access-policy edits.
BEGIN;
SET LOCAL ROLE mooby_nlp_owner_dev;

ALTER TABLE nlp.event_versions
    ALTER COLUMN embedding SET NOT NULL,
    ALTER COLUMN embedding_input_version SET NOT NULL;

GRANT INSERT (
    embedding, embedding_input_version,
    verification_status, verified_at, publication_status, published_at
)
ON TABLE nlp.event_versions TO mooby_nlp_worker_dev;
COMMIT;
