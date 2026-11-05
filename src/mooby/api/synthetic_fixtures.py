"""Development-only app fixtures processed from synthetic text in memory."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Sequence
from uuid import NAMESPACE_URL, uuid5

from mooby.api.models import AppSource, DisruptionItem, StationOrSegment
from mooby.db.source_reader import SourceSnapshot, StationReference
from mooby.worker.classifier import ClassifierAdapter
from mooby.worker.event_persistence import DraftEvent
from mooby.worker.processor import WorkerSettings, process_snapshot


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SYNTHETIC_CSV = PROJECT_ROOT / "evaluation/private/synthetic_training_v1.csv"
STATION_REFERENCE_CSV = PROJECT_ROOT / "evaluation/reference/station_alias_review.csv"
PIPELINE_VERSION = "synthetic-app-fixture-v1"


class SyntheticFixtureUnavailable(RuntimeError):
    """Synthetic examples cannot be loaded or processed safely."""


@dataclass(frozen=True)
class SyntheticFixtureRecord:
    item: DisruptionItem
    station_ids: tuple[str, ...]


@dataclass(frozen=True)
class SyntheticFixturePage:
    generated_at: datetime
    items: list[SyntheticFixtureRecord]


def _read_station_catalog() -> list[StationReference]:
    try:
        with STATION_REFERENCE_CSV.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            required = {"station_id", "canonical_name", "curated_aliases"}
            if not required.issubset(reader.fieldnames or ()):
                raise ValueError("station reference columns missing")
            stations = []
            for row in reader:
                aliases = json.loads(row.get("curated_aliases") or "[]")
                if not isinstance(aliases, list) or not all(
                    isinstance(alias, str) for alias in aliases
                ):
                    raise ValueError("station aliases invalid")
                stations.append(
                    StationReference(
                        station_id=row["station_id"],
                        canonical_name=row["canonical_name"],
                        aliases=tuple(aliases),
                    )
                )
            return stations
    except Exception as exc:
        raise SyntheticFixtureUnavailable("station reference unavailable") from exc


def _synthetic_snapshot(
    row: dict[str, str], row_number: int, raw_text: str, generated_at: datetime
) -> SourceSnapshot:
    # IDs and provenance are explicitly synthetic and are never persisted.
    row_key = (row.get("record_id") or "").strip() or f"row-{row_number}"
    snapshot_id = uuid5(
        NAMESPACE_URL, f"mooby-app-synthetic-snapshot:{row_key}:{row_number}"
    )
    raw_content_id = uuid5(
        NAMESPACE_URL, f"mooby-app-synthetic-content:{row_key}:{row_number}"
    )
    return SourceSnapshot(
        snapshot_id=snapshot_id,
        raw_content_id=raw_content_id,
        capture_hash=hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
        raw_text=raw_text,
        text_kind="post_text",
        text_completeness="synthetic_fixture",
        published_at=generated_at,
        published_at_basis="page_timestamp",
        captured_at=generated_at,
        platform="x",
        canonical_url=f"https://example.invalid/synthetic/{snapshot_id}",
    )


def _item_from_draft(
    draft: DraftEvent,
    generated_at: datetime,
    stations_by_id: dict[str, StationReference],
) -> SyntheticFixtureRecord:
    event_kind = draft.event_kind
    if event_kind == "station":
        station = stations_by_id.get(draft.station_id or "")
        if station is None:
            raise ValueError("synthetic station reference missing")
        location = StationOrSegment(kind="station", station=station.canonical_name)
        station_ids = (station.station_id,)
    else:
        start = stations_by_id.get(draft.start_station_id or "")
        end = stations_by_id.get(draft.end_station_id or "")
        if start is None or end is None:
            raise ValueError("synthetic segment station reference missing")
        location = StationOrSegment(
            kind="segment",
            line=draft.line_id,
            start_station=start.canonical_name,
            end_station=end.canonical_name,
            direction=draft.direction,
        )
        station_ids = (start.station_id, end.station_id)
    return SyntheticFixtureRecord(
        item=DisruptionItem(
            public_event_id=draft.event_id,
            title=f"SIMULASI — {draft.title}",
            summary=draft.summary,
            station_or_segment=location,
            severity=draft.severity,
            event_time=draft.event_at,
            event_time_certainty=draft.event_time_certainty,
            freshness="fresh",
            certainty="unknown",
            source=AppSource(
                display_name="SIMULASI · dataset sintetis",
                url=f"https://example.invalid/synthetic/{draft.event_id}",
                published_at=generated_at,
            ),
        ),
        station_ids=station_ids,
    )


@lru_cache(maxsize=1)
def _build_fixture_feed() -> SyntheticFixturePage:
    generated_at = datetime.now(timezone.utc)
    if not SYNTHETIC_CSV.is_file():
        raise SyntheticFixtureUnavailable("synthetic dataset unavailable")
    try:
        classifier = ClassifierAdapter.load()
        settings = WorkerSettings.from_env()
        stations = _read_station_catalog()
        stations_by_id = {station.station_id: station for station in stations}
        items: list[SyntheticFixtureRecord] = []
        with SYNTHETIC_CSV.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if "raw_text" not in (reader.fieldnames or ()):
                raise ValueError("raw_text column missing")
            for row_number, row in enumerate(reader, start=2):
                raw_text = row.get("raw_text") or ""
                if not raw_text.strip():
                    continue
                snapshot = _synthetic_snapshot(
                    row, row_number, raw_text, generated_at
                )
                outcome = process_snapshot(
                    snapshot,
                    pipeline_version=PIPELINE_VERSION,
                    database_now=generated_at,
                    classifier=classifier,
                    settings=settings,
                    station_catalog=stations,
                )
                draft = outcome.draft_event
                if draft is not None:
                    items.append(_item_from_draft(draft, generated_at, stations_by_id))
        return SyntheticFixturePage(generated_at=generated_at, items=items)
    except SyntheticFixtureUnavailable:
        raise
    except Exception as exc:
        raise SyntheticFixtureUnavailable("synthetic processing unavailable") from exc


def synthetic_fixture_page(
    *, station_ids: Sequence[str], limit: int, offset: int
) -> SyntheticFixturePage:
    page = _build_fixture_feed()
    requested = set(station_ids)

    selected = [
        record
        for record in page.items
        if requested.intersection(record.station_ids)
    ]
    return SyntheticFixturePage(
        generated_at=page.generated_at,
        items=selected[offset : offset + limit + 1],
    )
