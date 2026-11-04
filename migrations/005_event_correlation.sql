-- 005_event_correlation.sql
-- Development only. Does not modify collection or reference objects/data.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

CREATE TABLE nlp.event_clusters (
    cluster_id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE nlp.event_cluster_versions (
    cluster_id uuid NOT NULL
        REFERENCES nlp.event_clusters (cluster_id),
    cluster_version integer NOT NULL
        CHECK (cluster_version > 0),
    ruleset_version text NOT NULL
        CHECK (btrim(ruleset_version) <> ''),
    is_current boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (cluster_id, cluster_version),
    UNIQUE (cluster_id, cluster_version, ruleset_version)
);

CREATE UNIQUE INDEX event_cluster_versions_one_current_idx
    ON nlp.event_cluster_versions (cluster_id)
    WHERE is_current;

CREATE TABLE nlp.event_cluster_members (
    cluster_id uuid NOT NULL,
    cluster_version integer NOT NULL,
    event_id uuid NOT NULL,
    event_version integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (
        cluster_id,
        cluster_version,
        event_id,
        event_version
    ),

    FOREIGN KEY (cluster_id, cluster_version)
        REFERENCES nlp.event_cluster_versions (
            cluster_id,
            cluster_version
        ),

    FOREIGN KEY (event_id, event_version)
        REFERENCES nlp.event_versions (event_id, event_version)
);

CREATE INDEX event_cluster_members_event_idx
    ON nlp.event_cluster_members (event_id, event_version);

-- Current assignment pointer. Historical memberships remain in
-- event_cluster_members under their original cluster versions.
CREATE TABLE nlp.event_cluster_current_memberships (
    event_id uuid NOT NULL,
    event_version integer NOT NULL,
    cluster_id uuid NOT NULL,
    cluster_version integer NOT NULL,
    assigned_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (event_id, event_version),

    FOREIGN KEY (
        cluster_id,
        cluster_version,
        event_id,
        event_version
    )
        REFERENCES nlp.event_cluster_members (
            cluster_id,
            cluster_version,
            event_id,
            event_version
        )
);

CREATE INDEX event_cluster_current_by_cluster_idx
    ON nlp.event_cluster_current_memberships (cluster_id, cluster_version);

-- A pair is stored once in canonical event-version order.
CREATE TABLE nlp.event_correlation_pairs (
    pair_id uuid PRIMARY KEY,
    left_event_id uuid NOT NULL,
    left_event_version integer NOT NULL,
    right_event_id uuid NOT NULL,
    right_event_version integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    UNIQUE (
        left_event_id,
        left_event_version,
        right_event_id,
        right_event_version
    ),

    UNIQUE (
        pair_id,
        left_event_id,
        left_event_version,
        right_event_id,
        right_event_version
    ),

    CHECK (
        (left_event_id, left_event_version)
        <
        (right_event_id, right_event_version)
    ),

    FOREIGN KEY (left_event_id, left_event_version)
        REFERENCES nlp.event_versions (event_id, event_version),

    FOREIGN KEY (right_event_id, right_event_version)
        REFERENCES nlp.event_versions (event_id, event_version)
);

CREATE TABLE nlp.event_correlation_assessments (
    assessment_id uuid PRIMARY KEY,
    pair_id uuid NOT NULL,
    left_event_id uuid NOT NULL,
    left_event_version integer NOT NULL,
    right_event_id uuid NOT NULL,
    right_event_version integer NOT NULL,
    assessment_version integer NOT NULL
        CHECK (assessment_version > 0),

    candidate_generation_version text
        CHECK (
            candidate_generation_version IS NULL
            OR btrim(candidate_generation_version) <> ''
        ),
    embedding_model_digest text
        CHECK (
            embedding_model_digest IS NULL
            OR embedding_model_digest ~ '^[0-9a-f]{64}$'
        ),
    embedding_input_version text
        CHECK (
            embedding_input_version IS NULL
            OR btrim(embedding_input_version) <> ''
        ),
    similarity_score numeric(6,5)
        CHECK (
            similarity_score IS NULL
            OR similarity_score BETWEEN -1 AND 1
        ),

    location_compatibility text NOT NULL
        CHECK (
            location_compatibility IN (
                'compatible',
                'incompatible',
                'unresolved'
            )
        ),
    time_compatibility text NOT NULL
        CHECK (
            time_compatibility IN (
                'compatible',
                'incompatible',
                'unresolved'
            )
        ),
    category_compatibility text NOT NULL
        CHECK (
            category_compatibility IN (
                'compatible',
                'incompatible',
                'unresolved'
            )
        ),
    path_compatibility text NOT NULL
        CHECK (
            path_compatibility IN (
                'compatible',
                'incompatible',
                'unresolved',
                'not_applicable'
            )
        ),

    time_delta_seconds bigint
        CHECK (time_delta_seconds IS NULL OR time_delta_seconds >= 0),
    time_window_seconds bigint
        CHECK (time_window_seconds IS NULL OR time_window_seconds > 0),

    ruleset_version text NOT NULL
        CHECK (btrim(ruleset_version) <> ''),
    decision_status text NOT NULL
        CHECK (
            decision_status IN (
                'candidate',
                'same_event',
                'different_event',
                'needs_review'
            )
        ),
    decision_reason_code text NOT NULL
        CHECK (btrim(decision_reason_code) <> ''),
    decision_facts jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(decision_facts) = 'object'),

    result_cluster_id uuid,
    result_cluster_version integer,
    is_current boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    decided_at timestamptz,

    UNIQUE (pair_id, assessment_version),

    FOREIGN KEY (
        pair_id,
        left_event_id,
        left_event_version,
        right_event_id,
        right_event_version
    )
        REFERENCES nlp.event_correlation_pairs (
            pair_id,
            left_event_id,
            left_event_version,
            right_event_id,
            right_event_version
        ),

    FOREIGN KEY (
        result_cluster_id,
        result_cluster_version,
        ruleset_version
    )
        REFERENCES nlp.event_cluster_versions (
            cluster_id,
            cluster_version,
            ruleset_version
        ),

    FOREIGN KEY (
        result_cluster_id,
        result_cluster_version,
        left_event_id,
        left_event_version
    )
        REFERENCES nlp.event_cluster_members (
            cluster_id,
            cluster_version,
            event_id,
            event_version
        ),

    FOREIGN KEY (
        result_cluster_id,
        result_cluster_version,
        right_event_id,
        right_event_version
    )
        REFERENCES nlp.event_cluster_members (
            cluster_id,
            cluster_version,
            event_id,
            event_version
        ),

    CHECK (
        (similarity_score IS NULL)
        = (embedding_model_digest IS NULL)
    ),

    CHECK (
        similarity_score IS NULL
        OR (
            candidate_generation_version IS NOT NULL
            AND embedding_input_version IS NOT NULL
        )
    ),

    CHECK (
        time_compatibility <> 'compatible'
        OR (
            time_delta_seconds IS NOT NULL
            AND time_window_seconds IS NOT NULL
            AND time_delta_seconds <= time_window_seconds
        )
    ),

    CHECK (
        (
            decision_status = 'candidate'
            AND similarity_score IS NOT NULL
            AND result_cluster_id IS NULL
            AND result_cluster_version IS NULL
            AND location_compatibility = 'compatible'
            AND time_compatibility = 'compatible'
            AND category_compatibility = 'compatible'
            AND path_compatibility IN ('compatible', 'not_applicable')
            AND decided_at IS NULL
        )
        OR
        (
            decision_status = 'same_event'
            AND similarity_score IS NOT NULL
            AND location_compatibility = 'compatible'
            AND time_compatibility = 'compatible'
            AND category_compatibility = 'compatible'
            AND path_compatibility IN ('compatible', 'not_applicable')
            AND result_cluster_id IS NOT NULL
            AND result_cluster_version IS NOT NULL
            AND decided_at IS NOT NULL
        )
        OR
        (
            decision_status IN ('different_event', 'needs_review')
            AND result_cluster_id IS NULL
            AND result_cluster_version IS NULL
            AND decided_at IS NOT NULL
        )
    ),

    CHECK (
        (result_cluster_id IS NULL)
        = (result_cluster_version IS NULL)
    )
);

CREATE UNIQUE INDEX event_correlation_assessments_one_current_idx
    ON nlp.event_correlation_assessments (pair_id)
    WHERE is_current;

CREATE INDEX event_correlation_assessments_score_idx
    ON nlp.event_correlation_assessments (
        embedding_model_digest,
        similarity_score DESC
    )
    WHERE similarity_score IS NOT NULL;

CREATE INDEX event_correlation_assessments_status_idx
    ON nlp.event_correlation_assessments (
        decision_status,
        created_at DESC
    );

COMMIT;
