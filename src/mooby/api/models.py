"""Typed, app-safe disruption response contract."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


Availability = Literal["results", "no_match", "source_unavailable"]
Severity = Literal["light", "moderate", "severe"]
Freshness = Literal["fresh", "stale"]
Certainty = Literal["low", "medium", "high", "unknown"]


class StationOrSegment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["station", "segment"]
    line: str | None = None
    station: str | None = None
    station_id: str | None = None
    start_station: str | None = None
    start_station_id: str | None = None
    end_station: str | None = None
    end_station_id: str | None = None
    direction: str | None = None
    affected_station_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_location_shape(self) -> "StationOrSegment":
        if self.kind == "station":
            if not self.station or self.line or self.start_station or self.end_station:
                raise ValueError("station location fields are inconsistent")
        elif not self.line or not self.start_station or not self.end_station or self.station:
            raise ValueError("segment location fields are inconsistent")
        return self


class AppSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    display_name: str
    url: HttpUrl
    published_at: datetime | None = None

    @model_validator(mode="after")
    def require_https(self) -> "AppSource":
        if self.url.scheme != "https":
            raise ValueError("source URL must use HTTPS")
        return self


class DisruptionItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    public_event_id: UUID
    title: str
    summary: str
    station_or_segment: StationOrSegment
    severity: Severity | None = None
    event_time: datetime | None = None
    event_time_certainty: Certainty | None = None
    freshness: Freshness
    certainty: Certainty | None = None
    source: AppSource


class DisruptionFeed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    availability: Availability
    generated_at: datetime | None
    window_hours: Literal[24] = 24
    message: str
    items: list[DisruptionItem] = Field(default_factory=list)
    limit: int = Field(ge=1, le=50)
    offset: int = Field(ge=0, le=1000)
    next_offset: int | None = None


class Readiness(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    api: Literal["available"] = "available"
    database: Literal["available", "unavailable"]
    app_api_projection: Literal["available", "unavailable"]
