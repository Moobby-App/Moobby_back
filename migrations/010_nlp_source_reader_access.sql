BEGIN;

CREATE ROLE mooby_nlp_source_reader_dev
    NOLOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    NOBYPASSRLS;

GRANT USAGE ON SCHEMA collection
TO mooby_nlp_source_reader_dev;

GRANT SELECT (
    id,
    raw_content_id,
    capture_hash,
    raw_text,
    text_kind,
    text_completeness,
    published_at,
    published_at_basis,
    captured_at
)
ON collection.raw_snapshots
TO mooby_nlp_source_reader_dev;

GRANT SELECT (
    id,
    platform,
    canonical_url
)
ON collection.raw_contents
TO mooby_nlp_source_reader_dev;

CREATE POLICY nlp_source_reader_snapshots_select
ON collection.raw_snapshots
FOR SELECT
TO mooby_nlp_source_reader_dev
USING (true);

CREATE POLICY nlp_source_reader_contents_select
ON collection.raw_contents
FOR SELECT
TO mooby_nlp_source_reader_dev
USING (true);

GRANT mooby_nlp_source_reader_dev
TO mooby_nlp_worker_login_dev
WITH INHERIT TRUE, SET FALSE, ADMIN FALSE;

COMMIT;