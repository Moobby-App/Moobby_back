-- Prepared only; do not apply as part of local verification.
-- NLP acceptance does not depend on retrieval/correlation embeddings.
-- No collection, access-policy, role, or grant changes; no draft promotion.
BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

ALTER TABLE nlp.event_versions
    ALTER COLUMN embedding_input_version DROP NOT NULL,
    ALTER COLUMN embedding DROP NOT NULL,
    ADD CONSTRAINT event_versions_embedding_pair_check
        CHECK ((embedding IS NULL) = (embedding_input_version IS NULL));

COMMIT;
