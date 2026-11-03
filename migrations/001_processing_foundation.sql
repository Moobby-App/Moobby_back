-- 001_processing_foundation.sql
-- Applied manually to the Supabase dev project on 2026-11-03.
-- Requires the pre-provisioned NOLOGIN role mooby_nlp_owner_dev.
-- Does not reference or modify the collection schema.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

CREATE SCHEMA nlp AUTHORIZATION mooby_nlp_owner_dev;

CREATE TABLE nlp.processing_queue (
    source_snapshot_id uuid NOT NULL,
    pipeline_version text NOT NULL
        CHECK (btrim(pipeline_version) <> ''),
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'claimed', 'succeeded', 'skipped', 'rejected', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0
        CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    lease_until timestamptz,
    lease_owner text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (source_snapshot_id, pipeline_version),

    CHECK (
        (status = 'claimed'
            AND claimed_at IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_owner IS NOT NULL
            AND btrim(lease_owner) <> '')
        OR
        (status <> 'claimed'
            AND claimed_at IS NULL
            AND lease_until IS NULL
            AND lease_owner IS NULL)
    ),
    CHECK (lease_until IS NULL OR lease_until > claimed_at)
);

CREATE TABLE nlp.processing_runs (
    run_id uuid PRIMARY KEY,
    source_snapshot_id uuid NOT NULL,
    pipeline_version text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    result_status text NOT NULL
        CHECK (result_status IN ('succeeded', 'skipped', 'policy_rejection', 'format_failure', 'error')),
    code_version text NOT NULL CHECK (btrim(code_version) <> ''),
    rules_version text NOT NULL CHECK (btrim(rules_version) <> ''),
    classifier_model_version text,
    embedding_model_version text,
    source_digest text NOT NULL
        CHECK (source_digest ~ '^[0-9a-f]{64}$'),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    result_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(result_metadata) = 'object'),

    UNIQUE (source_snapshot_id, pipeline_version, attempt_no),
    UNIQUE (run_id, source_snapshot_id, pipeline_version),

    FOREIGN KEY (source_snapshot_id, pipeline_version)
        REFERENCES nlp.processing_queue (source_snapshot_id, pipeline_version),

    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE TABLE nlp.processing_notes (
    note_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    source_snapshot_id uuid NOT NULL,
    pipeline_version text NOT NULL,
    note_type text NOT NULL
        CHECK (note_type IN ('skip', 'error', 'format_failure', 'policy_rejection')),
    reason_code text NOT NULL CHECK (btrim(reason_code) <> ''),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(detail) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),

    FOREIGN KEY (run_id, source_snapshot_id, pipeline_version)
        REFERENCES nlp.processing_runs (run_id, source_snapshot_id, pipeline_version)
);

CREATE INDEX processing_queue_ready_idx
    ON nlp.processing_queue (available_at, created_at)
    WHERE status = 'queued';

CREATE INDEX processing_queue_expired_lease_idx
    ON nlp.processing_queue (lease_until)
    WHERE status = 'claimed';

CREATE INDEX processing_runs_snapshot_idx
    ON nlp.processing_runs (source_snapshot_id, pipeline_version, attempt_no DESC);

CREATE INDEX processing_notes_run_idx
    ON nlp.processing_notes (run_id, created_at);

COMMIT;