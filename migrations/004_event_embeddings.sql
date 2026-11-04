-- 004_event_embeddings.sql
-- Development only. Does not modify collection objects, data, roles, or ownership.

BEGIN;

-- Keep extension setup in the ordered migration history.
-- The extension is already enabled in the development project.
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;

-- Required by the nlp owner to use the vector type.
GRANT USAGE ON SCHEMA extensions TO mooby_nlp_owner_dev;

SET LOCAL ROLE mooby_nlp_owner_dev;

ALTER TABLE nlp.event_versions
    ADD COLUMN embedding_input_version text NOT NULL
        CHECK (btrim(embedding_input_version) <> ''),
    ADD COLUMN embedding extensions.vector(384) NOT NULL;

CREATE INDEX event_versions_embedding_hnsw_idx
    ON nlp.event_versions
    USING hnsw (embedding extensions.vector_cosine_ops);

COMMIT;