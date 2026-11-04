-- 008_nlp_worker_login.sql
-- Development only. Adds a login for the nlp queue worker.

BEGIN;

CREATE ROLE mooby_nlp_worker_login_dev LOGIN;

GRANT mooby_nlp_worker_dev TO mooby_nlp_worker_login_dev
WITH INHERIT TRUE, SET FALSE, ADMIN FALSE;

COMMIT;