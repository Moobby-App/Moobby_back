"""Atomic persistence for non-published, unverified event drafts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid5

from psycopg import Connection

from mooby.worker.event_fields import validate_exact_evidence_quote


@dataclass(frozen=True)
class DraftEvent:
    event_id: UUID
    source_snapshot_id: UUID
    raw_content_id: UUID
    pipeline_version: str
    event_kind: Literal["station", "segment"]
    station_id: str | None
    line_id: str | None
    start_station_id: str | None
    end_station_id: str | None
    direction: str | None
    path_id: str | None
    path_version: int | None
    event_at: datetime | None
    event_time_raw_phrase: str | None
    event_time_method: Literal["explicit", "relative_phrase", "unknown"]
    event_time_certainty: Literal["low", "medium", "high", "unknown"]
    severity: Literal["light", "moderate", "severe"]
    title: str
    summary: str
    source_platform: str
    canonical_url: str
    source_digest: str
    source_published_at: datetime
    source_published_at_basis: str
    captured_at: datetime
    raw_text: str = field(repr=False)
    evidence_quote: str = field(repr=False)
    quote_start_char: int
    validator_version: str


def persist_draft_event(
    conn: Connection[Any],
    *,
    draft: DraftEvent,
    run_id: UUID,
) -> None:
    """Insert event, provenance, and exact quote in caller's transaction."""
    if draft.event_kind not in {"station", "segment"}:
        raise ValueError("unsupported draft event kind")
    if not draft.pipeline_version.strip() or not draft.title.strip() or not draft.summary.strip():
        raise ValueError("draft event required fields are empty")
    validate_exact_evidence_quote(draft.raw_text, draft.evidence_quote)
    if (
        draft.quote_start_char < 0
        or draft.raw_text[
            draft.quote_start_char : draft.quote_start_char + len(draft.evidence_quote)
        ]
        != draft.evidence_quote
    ):
        raise ValueError("evidence quote offsets do not match source text")

    conn.execute(
        "INSERT INTO nlp.events (event_id) VALUES (%s) ON CONFLICT (event_id) DO NOTHING",
        (draft.event_id,),
    )
    conn.execute(
        """
        INSERT INTO nlp.event_versions (
            event_id, event_version, source_snapshot_id, pipeline_version,
            processing_run_id, event_kind, line_id, station_id,
            start_station_id, end_station_id, direction, path_id, path_version,
            event_at, event_time_raw_phrase, event_time_method,
            event_time_certainty, severity, title, summary
        )
        VALUES (
            %s, 1, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT DO NOTHING
        """,
        (
            draft.event_id,
            draft.source_snapshot_id,
            draft.pipeline_version,
            run_id,
            draft.event_kind,
            draft.line_id,
            draft.station_id,
            draft.start_station_id,
            draft.end_station_id,
            draft.direction,
            draft.path_id,
            draft.path_version,
            draft.event_at,
            draft.event_time_raw_phrase,
            draft.event_time_method,
            draft.event_time_certainty,
            draft.severity,
            draft.title,
            draft.summary,
        ),
    )
    conn.execute(
        """
        INSERT INTO nlp.event_sources (
            event_id, event_version, source_snapshot_id, raw_content_id,
            pipeline_version, processing_run_id, source_platform,
            canonical_url, source_digest, source_published_at,
            source_published_at_basis, captured_at
        )
        VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id, event_version, source_snapshot_id) DO NOTHING
        """,
        (
            draft.event_id,
            draft.source_snapshot_id,
            draft.raw_content_id,
            draft.pipeline_version,
            run_id,
            draft.source_platform,
            draft.canonical_url,
            draft.source_digest,
            draft.source_published_at,
            draft.source_published_at_basis,
            draft.captured_at,
        ),
    )
    quote_end = draft.quote_start_char + len(draft.evidence_quote)
    conn.execute(
        """
        INSERT INTO nlp.event_evidence (
            evidence_id, event_id, event_version, source_snapshot_id,
            evidence_quote, quote_start_char, quote_end_char,
            validator_version, exact_match_validated_at
        )
        VALUES (%s, %s, 1, %s, %s, %s, %s, %s, clock_timestamp())
        ON CONFLICT (evidence_id) DO NOTHING
        """,
        (
            uuid5(
                draft.event_id,
                f"event-evidence:{draft.source_snapshot_id}:{draft.validator_version}",
            ),
            draft.event_id,
            draft.source_snapshot_id,
            draft.evidence_quote,
            draft.quote_start_char,
            quote_end,
            draft.validator_version,
        ),
    )
