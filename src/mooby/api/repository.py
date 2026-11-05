"""Parameterized read access to the restricted app_api projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row


@dataclass(frozen=True)
class FeedPage:
    generated_at: datetime
    rows: list[dict[str, Any]]


_SEARCH_SQL = """
SELECT
    v.public_event_id,
    v.event_kind,
    v.title,
    v.summary,
    v.severity,
    v.event_at,
    v.event_time_certainty,
    v.certainty_label,
    v.freshness_anchor_at,
    CASE
        WHEN v.freshness_anchor_at >= transaction_timestamp() - interval '8 hours'
            THEN 'fresh'
        ELSE 'stale'
    END AS freshness,
    v.line_name,
    v.station_name,
    v.start_station_name,
    v.end_station_name,
    v.direction,
    v.source_display_name,
    v.source_url,
    v.source_published_at
FROM app_api.published_disruptions AS v
WHERE v.affected_station_ids && %s::text[]
ORDER BY v.freshness_anchor_at DESC, v.public_event_id
LIMIT %s OFFSET %s
"""


def fetch_disruptions(
    *,
    database_url: str,
    station_ids: list[str],
    limit: int,
    offset: int,
    connect_timeout_seconds: int,
    statement_timeout_ms: int,
) -> FeedPage:
    with psycopg.connect(
        database_url,
        connect_timeout=connect_timeout_seconds,
        row_factory=dict_row,
        options=f"-c statement_timeout={statement_timeout_ms}",
    ) as conn:
        with conn.transaction():
            generated_at = conn.execute(
                "SELECT transaction_timestamp() AS generated_at"
            ).fetchone()["generated_at"]
            rows = conn.execute(
                _SEARCH_SQL,
                (station_ids, limit + 1, offset),
            ).fetchall()
    return FeedPage(generated_at=generated_at, rows=[dict(row) for row in rows])


def check_database(
    *, database_url: str, connect_timeout_seconds: int, statement_timeout_ms: int
) -> bool:
    try:
        with psycopg.connect(
            database_url,
            connect_timeout=connect_timeout_seconds,
            row_factory=dict_row,
            options=f"-c statement_timeout={statement_timeout_ms}",
        ) as conn:
            conn.execute("SELECT transaction_timestamp()").fetchone()
        return True
    except psycopg.Error:
        return False


def check_app_api_projection(
    *, database_url: str, connect_timeout_seconds: int, statement_timeout_ms: int
) -> bool:
    try:
        with psycopg.connect(
            database_url,
            connect_timeout=connect_timeout_seconds,
            row_factory=dict_row,
            options=f"-c statement_timeout={statement_timeout_ms}",
        ) as conn:
            conn.execute(
                "SELECT public_event_id FROM app_api.published_disruptions LIMIT 1"
            ).fetchone()
        return True
    except psycopg.Error:
        return False
