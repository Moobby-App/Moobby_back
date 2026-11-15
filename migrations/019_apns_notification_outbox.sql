-- PREPARATION ONLY: apply explicitly as an authorized migration administrator.
-- No existing event backfill. No existing API/worker login gains push permissions.
BEGIN;
-- Match the existing Development ownership boundary from migration 018.
DO $preflight$
BEGIN
    IF session_user <> 'postgres' OR current_user <> 'postgres' THEN
        RAISE EXCEPTION '019 requires the authorized postgres migration session';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members
        WHERE roleid='mooby_app_api_owner_dev'::regrole
          AND member='postgres'::regrole AND grantor='postgres'::regrole) THEN
        RAISE EXCEPTION '019 found an existing postgres-granted owner membership';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_auth_members
        WHERE roleid='mooby_app_api_owner_dev'::regrole
          AND member='postgres'::regrole AND admin_option) THEN
        RAISE EXCEPTION '019 requires the existing API-owner ADMIN option';
    END IF;
END;
$preflight$;
DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='mooby_notifications_api_dev') THEN
        CREATE ROLE mooby_notifications_api_dev NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='mooby_notifications_sender_dev') THEN
        CREATE ROLE mooby_notifications_sender_dev NOLOGIN;
    END IF;
END;
$roles$;

CREATE SCHEMA notifications AUTHORIZATION mooby_nlp_owner_dev;
REVOKE ALL ON SCHEMA notifications FROM PUBLIC;
SET LOCAL ROLE mooby_nlp_owner_dev;

CREATE TABLE notifications.devices (
    installation_id uuid PRIMARY KEY,
    secret_digest text NOT NULL CHECK(secret_digest ~ '^[0-9a-f]{64}$'),
    device_token text NOT NULL CHECK(device_token ~ '^[0-9a-f]+$'
        AND length(device_token) BETWEEN 2 AND 1024 AND length(device_token)%2=0),
    environment text NOT NULL CHECK(environment IN ('sandbox','production')),
    topic text NOT NULL,
    station_ids text[] NOT NULL CHECK(cardinality(station_ids) BETWEEN 1 AND 30),
    severities text[] NOT NULL CHECK(cardinality(severities) BETWEEN 1 AND 3
        AND severities <@ ARRAY['light','moderate','severe']::text[]),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    disabled_at timestamptz,
    UNIQUE(environment,topic,device_token)
);

CREATE TABLE notifications.outbox (
    event_id uuid PRIMARY KEY REFERENCES nlp.events(event_id),
    initial_event_version integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    fanned_out_at timestamptz
);

CREATE TABLE notifications.deliveries (
    notification_id uuid PRIMARY KEY,
    event_id uuid NOT NULL REFERENCES notifications.outbox(event_id),
    installation_id uuid NOT NULL REFERENCES notifications.devices(installation_id),
    environment text NOT NULL,
    topic text NOT NULL,
    expires_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK(status IN
        ('pending','sending','accepted','expired','invalid_device','rejected','cancelled')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    lease_until timestamptz,
    claim_token uuid,
    accepted_at timestamptz,
    UNIQUE(event_id,installation_id)
);
CREATE INDEX ON notifications.deliveries(next_attempt_at) WHERE status IN ('pending','sending');
CREATE INDEX ON notifications.outbox(created_at) WHERE fanned_out_at IS NULL;

CREATE FUNCTION notifications.enqueue_published_event() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $enqueue$
BEGIN
    IF NEW.verification_status='verified' AND NEW.publication_status='published' THEN
        INSERT INTO notifications.outbox(event_id,initial_event_version)
        VALUES(NEW.event_id,NEW.event_version) ON CONFLICT(event_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$enqueue$;
REVOKE ALL ON FUNCTION notifications.enqueue_published_event() FROM PUBLIC;
CREATE TRIGGER mooby_push_publication_outbox
AFTER INSERT OR UPDATE OF verification_status,publication_status ON nlp.event_versions
FOR EACH ROW EXECUTE FUNCTION notifications.enqueue_published_event();

GRANT USAGE ON SCHEMA notifications TO mooby_notifications_api_dev,mooby_notifications_sender_dev;
GRANT SELECT,INSERT,UPDATE ON notifications.devices TO mooby_notifications_api_dev;
GRANT SELECT,UPDATE ON notifications.devices TO mooby_notifications_sender_dev;
GRANT SELECT,UPDATE ON notifications.outbox TO mooby_notifications_sender_dev;
GRANT SELECT,INSERT,UPDATE ON notifications.deliveries TO mooby_notifications_sender_dev;
RESET ROLE;

GRANT mooby_app_api_owner_dev TO postgres
    WITH INHERIT FALSE, SET TRUE, ADMIN FALSE GRANTED BY postgres;
SET LOCAL ROLE mooby_app_api_owner_dev;
GRANT USAGE ON SCHEMA app_api TO mooby_notifications_sender_dev;
GRANT SELECT ON app_api.published_disruptions TO mooby_notifications_sender_dev;
RESET ROLE;
REVOKE mooby_app_api_owner_dev FROM postgres GRANTED BY postgres RESTRICT;
COMMIT;
