"""Authenticated FastAPI gateway for the app-safe Path A disruption feed."""

from __future__ import annotations

import hmac
import os
import re
from datetime import datetime, timedelta, timezone
from functools import partial
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import Field, TypeAdapter, ValidationError

from mooby.api.config import ApiConfigurationError, ApiSettings
from mooby.api.feed_cache import RedisFeedCache
from mooby.api.push_devices import device_router
from mooby.api.observability import (
    ObservedFastAPI, observe_sync, record_failure, set_category, timed_stage,
)
from mooby.api.models import (
    DisruptionFeed,
    DisruptionItem,
    Readiness,
)
from mooby.api.repository import (
    READ_ERRORS,
    check_app_api_projection,
    check_database,
    fetch_disruptions,
)
from mooby.api.response_preparation import (
    FeedQuery, ReadinessStatus, load_feed, prepare_feed, prepare_item,
    prepare_readiness, probe_readiness,
)
from mooby.api.synthetic_fixtures import (
    SyntheticFixtureUnavailable,
    synthetic_fixture_page,
)


_bearer = HTTPBearer(auto_error=False)
_STATION_ID = re.compile(r"^open-krl:[A-Z0-9]+$")
_PROCESS_STARTED_AT = datetime.now(timezone.utc)
_LIMIT = TypeAdapter(Annotated[int, Field(ge=1, le=50)])
_OFFSET = TypeAdapter(Annotated[int, Field(ge=0, le=1000)])


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
    except ApiConfigurationError as exc:
        record_failure("authentication", exc)
        raise ApiUnavailable from None


@observe_sync("authentication")
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


@observe_sync("row_mapping")
def _item(row: dict[str, object]) -> DisruptionItem:
    return prepare_item(row)


@observe_sync("response_validation")
def _feed(**values) -> DisruptionFeed:
    feed = DisruptionFeed(**values)
    set_category(feed.availability)
    return feed


def _unavailable_feed(limit: int, offset: int) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=_feed(
            availability="source_unavailable", generated_at=None,
            message="Informasi gangguan belum dapat diperiksa. Coba lagi.",
            items=[], limit=limit, offset=offset,
        ).model_dump(mode="json"),
    )


def _error_pagination(request: Request) -> tuple[int, int]:
    # Configuration errors occur before FastAPI's endpoint parameter validation.
    # Preserve each valid scalar value using the same Pydantic integer rules;
    # invalid values retain defaults without changing existing error precedence.
    values = []
    for name, default, adapter in (("limit", 20, _LIMIT), ("offset", 0, _OFFSET)):
        try:
            values.append(adapter.validate_python(request.query_params.get(name, default)))
        except ValidationError:
            values.append(default)
    return values[0], values[1]


def _readiness_result(database: bool, projection: bool) -> Readiness | JSONResponse:
    result = prepare_readiness(ReadinessStatus(database, projection))
    if not database or not projection:
        return JSONResponse(status_code=503, content=result.model_dump(mode="json"))
    return result


def create_app() -> FastAPI:
    feed_cache = RedisFeedCache.from_env()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            feed_cache.close()

    app = ObservedFastAPI(
        title="Mooby App API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.exception_handler(ApiUnavailable)
    def unavailable(request: Request, __: ApiUnavailable) -> JSONResponse:
        if getattr(request.scope.get("route"), "path", None) == "/health/ready":
            return _readiness_result(False, False)
        return _unavailable_feed(*_error_pagination(request))

    @app.get("/health/live")
    @observe_sync("endpoint_execution")
    def live() -> dict[str, str]:
        return {"api": "available"}

    @app.get("/health/ready", response_model=Readiness)
    @observe_sync("endpoint_execution")
    def ready(settings: Annotated[ApiSettings, Depends(_require_auth)]) -> Readiness | JSONResponse:
        state = probe_readiness(
            database_probe=partial(check_database,
                database_url=settings.database_url,
                connect_timeout_seconds=settings.connect_timeout_seconds,
                statement_timeout_ms=settings.statement_timeout_ms,
            ),
            projection_probe=partial(check_app_api_projection,
                database_url=settings.database_url,
                connect_timeout_seconds=settings.connect_timeout_seconds,
                statement_timeout_ms=settings.statement_timeout_ms,
            ),
            on_failure=record_failure,
        )
        return _readiness_result(state.database_available, state.projection_available)

    @app.get("/api/v1/mock/disruptions", response_model=DisruptionFeed)
    @observe_sync("endpoint_execution")
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
            return _feed(
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
            record_failure("fixture_read", exc)
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=_feed(
                    availability="source_unavailable",
                    generated_at=None,
                    message="Fixture simulasi tidak tersedia. Coba lagi.",
                    items=[],
                    limit=limit,
                    offset=offset,
                ).model_dump(mode="json"),
            )

    @app.get("/api/v1/disruptions", response_model=DisruptionFeed)
    @observe_sync("endpoint_execution")
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
            query = FeedQuery(tuple(station_ids), limit, offset)
            cached = feed_cache.get(settings.database_url, query)
            if cached is not None:
                return _feed(**cached.model_dump())
            reader = partial(fetch_disruptions,
                database_url=settings.database_url,
                connect_timeout_seconds=settings.connect_timeout_seconds,
                statement_timeout_ms=settings.statement_timeout_ms,
            )
            with timed_stage("feed_read"):
                page = load_feed(query, reader=reader)
            result = prepare_feed(query, page, item_mapper=_item, feed_builder=_feed)
            # Include lookahead: its expiry can change next_offset as well.
            expiry_times = [row['cache_expires_at'].timestamp()
                            for row in page.rows if row.get('cache_expires_at') is not None]
            feed_cache.put(settings.database_url, query, result,
                           valid_until=min(expiry_times) if expiry_times else None)
            return result
        except READ_ERRORS as exc:
            record_failure("feed_read", exc)
            return _unavailable_feed(limit, offset)

    app.include_router(device_router(_require_auth))
    return app


app = create_app()
