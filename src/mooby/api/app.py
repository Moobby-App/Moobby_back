"""Authenticated FastAPI gateway for the app-safe Path A disruption feed."""

from __future__ import annotations

import hmac
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import psycopg
from pydantic import ValidationError

from mooby.api.config import ApiConfigurationError, ApiSettings
from mooby.api.models import (
    AppSource,
    DisruptionFeed,
    DisruptionItem,
    Readiness,
    StationOrSegment,
)
from mooby.api.repository import (
    check_app_api_projection,
    check_database,
    fetch_disruptions,
)
from mooby.api.synthetic_fixtures import (
    SyntheticFixtureUnavailable,
    synthetic_fixture_page,
)


logger = logging.getLogger(__name__)
_bearer = HTTPBearer(auto_error=False)
_STATION_ID = re.compile(r"^open-krl:[A-Z0-9]+$")
_PROCESS_STARTED_AT = datetime.now(timezone.utc)


class ApiUnavailable(Exception):
    pass


def _synthetic_fixture_mode_enabled() -> bool:
    mode = os.environ.get("APP_API_SYNTHETIC_FIXTURE_MODE", "disabled").strip().casefold()
    environment = os.environ.get("APP_API_ENVIRONMENT", "").strip().casefold()
    if mode == "disabled":
        return False
    if mode == "development" and environment == "development":
        try:
            ttl_minutes = int(os.environ.get("APP_API_SYNTHETIC_FIXTURE_TTL_MINUTES", ""))
        except ValueError as exc:
            raise ApiUnavailable from exc
        if not 1 <= ttl_minutes <= 240:
            raise ApiUnavailable
        return datetime.now(timezone.utc) < (
            _PROCESS_STARTED_AT + timedelta(minutes=ttl_minutes)
        )
    raise ApiUnavailable


def _settings_or_503() -> ApiSettings:
    try:
        return ApiSettings.from_env()
    except ApiConfigurationError:
        raise ApiUnavailable from None


def _require_auth(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(_bearer)
    ],
) -> ApiSettings:
    settings = _settings_or_503()
    if credentials is None or credentials.scheme.casefold() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "authentication_required"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not hmac.compare_digest(credentials.credentials, settings.bearer_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_credentials"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    return settings


def _item(row: dict[str, object]) -> DisruptionItem:
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


def create_app() -> FastAPI:
    app = FastAPI(
        title="Mooby App API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.exception_handler(ApiUnavailable)
    def unavailable(_: Request, __: ApiUnavailable) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "availability": "source_unavailable",
                "generated_at": None,
                "window_hours": 24,
                "message": "Informasi gangguan belum dapat diperiksa. Coba lagi.",
                "items": [],
                "limit": 20,
                "offset": 0,
                "next_offset": None,
            },
        )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"api": "available"}

    @app.get("/health/ready", response_model=Readiness)
    def ready(settings: Annotated[ApiSettings, Depends(_require_auth)]) -> Readiness:
        database_available = check_database(
            database_url=settings.database_url,
            connect_timeout_seconds=settings.connect_timeout_seconds,
            statement_timeout_ms=settings.statement_timeout_ms,
        )
        projection_available = database_available and check_app_api_projection(
            database_url=settings.database_url,
            connect_timeout_seconds=settings.connect_timeout_seconds,
            statement_timeout_ms=settings.statement_timeout_ms,
        )
        if not database_available or not projection_available:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "api": "available",
                    "database": "available" if database_available else "unavailable",
                    "app_api_projection": (
                        "available" if projection_available else "unavailable"
                    ),
                },
            )
        return Readiness(
            database="available",
            app_api_projection="available",
        )

    @app.get("/api/v1/mock/disruptions", response_model=DisruptionFeed)
    def mock_disruptions(
        settings: Annotated[ApiSettings, Depends(_require_auth)],
        station_ids: Annotated[list[str], Query(min_length=1, max_length=30)],
        limit: Annotated[int, Query(ge=1, le=50)] = 20,
        offset: Annotated[int, Query(ge=0, le=1000)] = 0,
    ) -> DisruptionFeed | JSONResponse:
        if any(_STATION_ID.fullmatch(value) is None for value in station_ids):
            raise HTTPException(status_code=422, detail={"code": "invalid_station_id"})
        try:
            if not _synthetic_fixture_mode_enabled():
                raise HTTPException(status_code=404, detail={"code": "mock_feed_disabled"})
            page = synthetic_fixture_page(
                station_ids=station_ids,
                limit=limit,
                offset=offset,
            )
            items = [record.item for record in page.items[:limit]]
            has_more = len(page.items) > limit
            return DisruptionFeed(
                availability="results" if items else "no_match",
                generated_at=page.generated_at,
                message=(
                    "SIMULASI: contoh sintetis, bukan laporan gangguan nyata."
                    if items
                    else "SIMULASI: tidak ada contoh yang cocok untuk rute ini."
                ),
                items=items,
                limit=limit,
                offset=offset,
                next_offset=offset + limit if has_more else None,
            )
        except (ApiUnavailable, SyntheticFixtureUnavailable) as exc:
            logger.warning("synthetic app_api feed unavailable (%s)", type(exc).__name__)
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=DisruptionFeed(
                    availability="source_unavailable",
                    generated_at=None,
                    message="Fixture simulasi tidak tersedia. Coba lagi.",
                    items=[],
                    limit=limit,
                    offset=offset,
                ).model_dump(mode="json"),
            )

    @app.get("/api/v1/disruptions", response_model=DisruptionFeed)
    def disruptions(
        settings: Annotated[ApiSettings, Depends(_require_auth)],
        station_ids: Annotated[list[str], Query(min_length=1, max_length=30)],
        limit: Annotated[int, Query(ge=1, le=50)] = 20,
        offset: Annotated[int, Query(ge=0, le=1000)] = 0,
    ) -> DisruptionFeed | JSONResponse:
        if any(_STATION_ID.fullmatch(value) is None for value in station_ids):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"code": "invalid_station_id"},
            )
        try:
            page = fetch_disruptions(
                database_url=settings.database_url,
                station_ids=station_ids,
                limit=limit,
                offset=offset,
                connect_timeout_seconds=settings.connect_timeout_seconds,
                statement_timeout_ms=settings.statement_timeout_ms,
            )
            has_more = len(page.rows) > limit
            items = [_item(row) for row in page.rows[:limit]]
        except (psycopg.Error, OSError, ValidationError, KeyError, TypeError, ValueError) as exc:
            logger.warning("app_api read failed (%s)", type(exc).__name__)
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=DisruptionFeed(
                    availability="source_unavailable",
                    generated_at=None,
                    message="Informasi gangguan belum dapat diperiksa. Coba lagi.",
                    items=[],
                    limit=limit,
                    offset=offset,
                ).model_dump(mode="json"),
            )
        if items:
            return DisruptionFeed(
                availability="results",
                generated_at=page.generated_at,
                message="Laporan gangguan yang cocok ditemukan.",
                items=items,
                limit=limit,
                offset=offset,
                next_offset=offset + limit if has_more else None,
            )
        return DisruptionFeed(
            availability="no_match",
            generated_at=page.generated_at,
            message="Tidak ditemukan laporan gangguan yang cocok dalam 24 jam terakhir.",
            items=[],
            limit=limit,
            offset=offset,
            next_offset=None,
        )

    return app


app = create_app()
