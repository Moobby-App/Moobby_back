"""Synchronous, HTTP-independent feed/readiness preparation.

Data inputs/results contain no credentials or live resources. Repository and
instrumentation callables are explicit local dependencies, never wire payloads.
Original exceptions propagate unless the established readiness boundary handles
READ_ERRORS. No environment lookup, connection creation, logging or timers here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Protocol

from mooby.api.models import AppSource, DisruptionFeed, DisruptionItem, Readiness, StationOrSegment
from mooby.api.repository import FeedPage, READ_ERRORS
from mooby.api.messages import FEED_RESULTS, FEED_NO_MATCH


@dataclass(frozen=True)
class FeedQuery:
    """Already validated route values; preserve order and duplicate station IDs."""

    station_ids: tuple[str, ...]
    limit: int = 20
    offset: int = 0


class FeedReader(Protocol):
    def __call__(self, *, station_ids: list[str], limit: int, offset: int) -> FeedPage: ...


class FeedBuilder(Protocol):
    def __call__(self, **values: Any) -> DisruptionFeed: ...


class ReadinessProbe(Protocol):
    def __call__(self) -> bool: ...


class FailureObserver(Protocol):
    def __call__(self, stage: str, error: BaseException) -> None: ...


ReadinessFailureStage = Literal["database_readiness", "projection_readiness"]


@dataclass(frozen=True)
class ReadinessStatus:
    database_available: bool
    projection_available: bool
    failure_stage: ReadinessFailureStage | None = None


def load_feed(query: FeedQuery, *, reader: FeedReader) -> FeedPage:
    """Delegate once; repository, not preparation, performs limit + 1 retrieval."""
    return reader(station_ids=list(query.station_ids), limit=query.limit, offset=query.offset)


def prepare_item(row: dict[str, Any]) -> DisruptionItem:
    event_kind = row["event_kind"]
    if event_kind not in {"station", "segment"}:
        raise ValueError("unsupported event kind in app_api projection")
    location = (
        StationOrSegment(
            kind="station",
            station=row["station_name"],
            station_id=row["station_id"],
            affected_station_ids=row["affected_station_ids"],
        )
        if event_kind == "station"
        else StationOrSegment(
            kind="segment",
            line=row["line_name"],
            start_station=row["start_station_name"],
            start_station_id=row["start_station_id"],
            end_station=row["end_station_name"],
            end_station_id=row["end_station_id"],
            direction=row["direction"],
            affected_station_ids=row["affected_station_ids"],
        )
    )
    return DisruptionItem(
        public_event_id=row["public_event_id"],
        title=row["title"],
        summary=row["summary"],
        original_text=row.get("original_text"),
        station_or_segment=location,
        severity=row["severity"],
        expires_at=row.get("cache_expires_at"),
        event_time=row["event_at"],
        event_time_certainty=row["event_time_certainty"],
        freshness=row["freshness"],
        certainty=row["certainty_label"],
        source=AppSource(
            display_name=row["source_display_name"],
            url=row["source_url"],
            published_at=row["source_published_at"],
        ),
    )


def prepare_feed(
    query: FeedQuery,
    page: FeedPage,
    *,
    item_mapper: Callable[[dict[str, Any]], DisruptionItem] = prepare_item,
    feed_builder: FeedBuilder = DisruptionFeed,
) -> DisruptionFeed:
    """Preserve ordering, map only the requested page, then validate the envelope.

    Callables allow the HTTP adapter to retain existing instrumentation wrappers.
    Mapping/envelope exceptions are not caught or replaced with empty feeds.
    """
    has_more = len(page.rows) > query.limit
    items = [item_mapper(row) for row in page.rows[:query.limit]]
    return feed_builder(
        availability="results" if items else "no_match",
        generated_at=page.generated_at,
        message=FEED_RESULTS if items else FEED_NO_MATCH,
        items=items, limit=query.limit, offset=query.offset,
        next_offset=query.offset + query.limit if has_more else None,
    )


def probe_readiness(
    *,
    database_probe: ReadinessProbe,
    projection_probe: ReadinessProbe,
    on_failure: FailureObserver | None = None,
) -> ReadinessStatus:
    """Existing ordered probes/short circuit and R03 exception boundary.

    Only escaped READ_ERRORS notify the observer, as in the original route.
    Probes returning False already have their own repository diagnostics.
    Exceptions are passed transiently to the observer, never stored in results.
    """
    database_available = projection_available = False
    try:
        database_available = database_probe()
        projection_available = database_available and projection_probe()
    except READ_ERRORS as error:
        stage = "projection_readiness" if database_available else "database_readiness"
        if on_failure is not None:
            on_failure(stage, error)
    failure_stage = ("database_readiness" if not database_available else
                     "projection_readiness" if not projection_available else None)
    return ReadinessStatus(database_available, projection_available, failure_stage)


def prepare_readiness(state: ReadinessStatus) -> Readiness:
    """Model construction remains outside the probe exception boundary."""
    return Readiness(database="available" if state.database_available else "unavailable",
                     app_api_projection="available" if state.projection_available else "unavailable")
