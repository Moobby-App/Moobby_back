-- 011_event_category_nullable.sql
-- Development only. Incident-category taxonomy is not defined yet.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

ALTER TABLE nlp.event_versions
    ALTER COLUMN category DROP NOT NULL;

COMMENT ON COLUMN nlp.event_versions.category IS
    'Nullable until an incident-category taxonomy or approved deterministic mapping is available; do not use for severity.';

COMMIT;
