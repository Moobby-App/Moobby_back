-- 003_event_core.sql
-- Development only. Does not modify collection objects, data, roles, or grants.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

ALTER TABLE nlp.processing_runs
    ADD COLUMN classifier_model_digest text
        CHECK (
            classifier_model_digest IS NULL
            OR classifier_model_digest ~ '^[0-9a-f]{64}$'
        ),
    ADD COLUMN embedding_model_digest text
        CHECK (
            embedding_model_digest IS NULL
            OR embedding_model_digest ~ '^[0-9a-f]{64}$'
        );

ALTER TABLE nlp.line_paths
    ADD CONSTRAINT line_paths_path_version_line_key
    UNIQUE (path_id, path_version, line_id);

CREATE TABLE nlp.events (
    event_id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE nlp.event_versions (
    event_id uuid NOT NULL
        REFERENCES nlp.events (event_id),
    event_version integer NOT NULL
        CHECK (event_version > 0),

    source_snapshot_id uuid NOT NULL,
    pipeline_version text NOT NULL
        CHECK (btrim(pipeline_version) <> ''),
    processing_run_id uuid NOT NULL,

    event_kind text NOT NULL
        CHECK (event_kind IN ('station', 'segment')),
    category text NOT NULL
        CHECK (btrim(category) <> ''),

    line_id text
        REFERENCES nlp.transit_lines (line_id),
    station_id text
        REFERENCES reference.krl_stations (station_id),
    start_station_id text
        REFERENCES reference.krl_stations (station_id),
    end_station_id text
        REFERENCES reference.krl_stations (station_id),
    direction text,
    path_id text,
    path_version integer,

    event_at timestamptz,
    event_time_raw_phrase text,
    event_time_timezone text NOT NULL DEFAULT 'Asia/Jakarta'
        CHECK (btrim(event_time_timezone) <> ''),
    event_time_method text
        CHECK (
            event_time_method IS NULL
            OR event_time_method IN (
                'explicit',
                'relative_phrase',
                'unknown'
            )
        ),
    event_time_certainty text
        CHECK (
            event_time_certainty IS NULL
            OR event_time_certainty IN ('low', 'medium', 'high', 'unknown')
        ),

    first_seen_at timestamptz NOT NULL DEFAULT now(),
    freshness_anchor_at timestamptz,
    last_observed_at timestamptz,

    certainty_score numeric(4,3)
        CHECK (
            certainty_score IS NULL
            OR certainty_score BETWEEN 0 AND 1
        ),
    certainty_label text
        CHECK (
            certainty_label IS NULL
            OR certainty_label IN ('low', 'medium', 'high', 'unknown')
        ),

    verification_status text NOT NULL DEFAULT 'unverified'
        CHECK (
            verification_status IN (
                'unverified',
                'needs_review',
                'verified',
                'rejected'
            )
        ),
    verified_at timestamptz,

    publication_status text NOT NULL DEFAULT 'not_eligible'
        CHECK (
            publication_status IN (
                'not_eligible',
                'eligible',
                'published',
                'withdrawn'
            )
        ),
    published_at timestamptz,

    title text NOT NULL
        CHECK (btrim(title) <> ''),
    summary text NOT NULL
        CHECK (btrim(summary) <> ''),

    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (event_id, event_version),

    UNIQUE (event_id, source_snapshot_id, pipeline_version),

    FOREIGN KEY (
        processing_run_id,
        source_snapshot_id,
        pipeline_version
    )
        REFERENCES nlp.processing_runs (
            run_id,
            source_snapshot_id,
            pipeline_version
        ),

    FOREIGN KEY (path_id, path_version, line_id)
        REFERENCES nlp.line_paths (path_id, path_version, line_id),

    CHECK (
        (path_id IS NULL AND path_version IS NULL)
        OR
        (path_id IS NOT NULL AND path_version IS NOT NULL
            AND line_id IS NOT NULL
            AND event_kind = 'segment')
    ),

    CHECK (
        (
            event_kind = 'station'
            AND station_id IS NOT NULL
            AND start_station_id IS NULL
            AND end_station_id IS NULL
            AND direction IS NULL
            AND path_id IS NULL
            AND path_version IS NULL
        )
        OR
        (
            event_kind = 'segment'
            AND station_id IS NULL
            AND line_id IS NOT NULL
            AND start_station_id IS NOT NULL
            AND end_station_id IS NOT NULL
            AND start_station_id <> end_station_id
            AND direction IS NOT NULL
            AND btrim(direction) <> ''
        )
    ),

    CHECK (
        (verification_status = 'verified')
        = (verified_at IS NOT NULL)
    ),
    CHECK (
        (publication_status = 'published')
        = (published_at IS NOT NULL)
    ),
    CHECK (last_observed_at IS NULL OR last_observed_at >= first_seen_at)
);

CREATE TABLE nlp.event_sources (
    event_id uuid NOT NULL,
    event_version integer NOT NULL,

    source_snapshot_id uuid NOT NULL,
    raw_content_id uuid NOT NULL,
    pipeline_version text NOT NULL
        CHECK (btrim(pipeline_version) <> ''),
    processing_run_id uuid NOT NULL,

    source_platform text NOT NULL
        CHECK (
            source_platform IN ('x', 'threads', 'rss', 'web')
        ),
    canonical_url text NOT NULL
        CHECK (canonical_url LIKE 'https://%'),
    source_digest text NOT NULL
        CHECK (source_digest ~ '^[0-9a-f]{64}$'),
    source_published_at timestamptz,
    source_published_at_basis text NOT NULL
        CHECK (
            source_published_at_basis IN (
                'dom_datetime',
                'feed_timestamp',
                'page_timestamp',
                'unknown'
            )
        ),
    CHECK (
        (source_published_at IS NULL)
        = (source_published_at_basis = 'unknown')
    ),
    captured_at timestamptz NOT NULL,

    PRIMARY KEY (event_id, event_version, source_snapshot_id),

    FOREIGN KEY (event_id, event_version)
        REFERENCES nlp.event_versions (event_id, event_version),

    FOREIGN KEY (
        processing_run_id,
        source_snapshot_id,
        pipeline_version
    )
        REFERENCES nlp.processing_runs (
            run_id,
            source_snapshot_id,
            pipeline_version
        )
);

CREATE TABLE nlp.event_evidence (
    evidence_id uuid PRIMARY KEY,
    event_id uuid NOT NULL,
    event_version integer NOT NULL,
    source_snapshot_id uuid NOT NULL,

    evidence_quote text NOT NULL
        CHECK (char_length(evidence_quote) > 0),
    quote_start_char integer NOT NULL
        CHECK (quote_start_char >= 0),
    quote_end_char integer NOT NULL,
    validator_version text NOT NULL
        CHECK (btrim(validator_version) <> ''),
    exact_match_validated_at timestamptz NOT NULL,

    created_at timestamptz NOT NULL DEFAULT now(),

    FOREIGN KEY (event_id, event_version, source_snapshot_id)
        REFERENCES nlp.event_sources (
            event_id,
            event_version,
            source_snapshot_id
        ),

    CHECK (quote_end_char > quote_start_char),
    CHECK (
        quote_end_char - quote_start_char = char_length(evidence_quote)
    )
);

CREATE INDEX event_versions_station_time_idx
    ON nlp.event_versions (station_id, event_at DESC)
    WHERE event_kind = 'station';

CREATE INDEX event_versions_segment_time_idx
    ON nlp.event_versions (
        line_id,
        start_station_id,
        end_station_id,
        event_at DESC
    )
    WHERE event_kind = 'segment';

CREATE INDEX event_versions_eligibility_idx
    ON nlp.event_versions (
        verification_status,
        publication_status,
        event_at DESC
    );

CREATE INDEX event_sources_snapshot_idx
    ON nlp.event_sources (source_snapshot_id, pipeline_version);

CREATE INDEX event_evidence_source_idx
    ON nlp.event_evidence (
        event_id,
        event_version,
        source_snapshot_id
    );

COMMIT;
