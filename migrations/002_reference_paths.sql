-- 002_reference_paths.sql
-- Development only. Does not modify collection or reference data.

BEGIN;

SET LOCAL ROLE mooby_nlp_owner_dev;

CREATE TABLE nlp.transit_lines (
    line_id text PRIMARY KEY
        CHECK (btrim(line_id) <> ''),
    canonical_name text NOT NULL
        CHECK (btrim(canonical_name) <> ''),
    source_route_id text NOT NULL UNIQUE
        CHECK (btrim(source_route_id) <> ''),
    reference_membership_name text NOT NULL UNIQUE
        CHECK (btrim(reference_membership_name) <> '')
);

INSERT INTO nlp.transit_lines (
    line_id,
    canonical_name,
    source_route_id,
    reference_membership_name
) VALUES
    ('BOGOR', 'Bogor', 'BOGOR', 'Bogor'),
    ('CIKARANG', 'Cikarang', 'CIKARANG', 'Cikarang'),
    ('RANGKASBITUNG', 'Rangkasbitung', 'RANGKASBITUNG', 'Rangkasbitung'),
    ('TANGERANG', 'Tangerang', 'TANGERANG', 'Tangerang'),
    ('TANJUNGPRIUK', 'Tanjung Priok', 'TANJUNGPRIUK', 'Tanjung Priok');

CREATE TABLE nlp.line_paths (
    path_id text NOT NULL
        CHECK (btrim(path_id) <> ''),
    path_version integer NOT NULL DEFAULT 1
        CHECK (path_version > 0),
    line_id text NOT NULL
        REFERENCES nlp.transit_lines (line_id),
    review_status text NOT NULL DEFAULT 'pending'
        CHECK (review_status IN ('pending', 'confirmed', 'rejected', 'superseded')),
    source_feed text NOT NULL
        CHECK (btrim(source_feed) <> ''),
    source_feed_sha256 text NOT NULL
        CHECK (source_feed_sha256 ~ '^[0-9a-f]{64}$'),
    station_master_sha256 text NOT NULL
        CHECK (station_master_sha256 ~ '^[0-9a-f]{64}$'),
    source_pattern_ids text[] NOT NULL
        CHECK (cardinality(source_pattern_ids) > 0),
    effective_since date,
    effective_until date,
    evidence_note text NOT NULL
        CHECK (btrim(evidence_note) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (path_id, path_version),
    CHECK (effective_until IS NULL OR effective_since IS NOT NULL),
    CHECK (effective_until IS NULL OR effective_until >= effective_since)
);

CREATE TABLE nlp.line_path_stations (
    path_id text NOT NULL,
    path_version integer NOT NULL,
    direction text NOT NULL
        CHECK (btrim(direction) <> ''),
    seq integer NOT NULL
        CHECK (seq > 0),
    station_id text NOT NULL
        REFERENCES reference.krl_stations (station_id),
    source_pattern_id text NOT NULL
        CHECK (btrim(source_pattern_id) <> ''),
    source_sha256 text NOT NULL
        CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    effective_since date,
    effective_until date,

    PRIMARY KEY (path_id, path_version, direction, seq),
    FOREIGN KEY (path_id, path_version)
        REFERENCES nlp.line_paths (path_id, path_version),
    CHECK (effective_until IS NULL OR effective_since IS NOT NULL),
    CHECK (effective_until IS NULL OR effective_until >= effective_since)
);

CREATE INDEX line_paths_line_status_idx
    ON nlp.line_paths (line_id, review_status);

CREATE INDEX line_path_stations_station_idx
    ON nlp.line_path_stations (station_id, path_id, path_version);

COMMIT;